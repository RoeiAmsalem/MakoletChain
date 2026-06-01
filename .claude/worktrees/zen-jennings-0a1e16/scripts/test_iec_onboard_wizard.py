#!/usr/bin/env python3
"""Tests for IEC onboarding wizard endpoints.

Uses mocks for the VPS SSH subprocess — does NOT actually hit the IEC API.
Run: python3 scripts/test_iec_onboard_wizard.py
"""
import json
import os
import sys
import time
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Patch subprocess.Popen before importing app
_mock_wizard_responses = {}


def _make_mock_proc(responses):
    """Create a mock Popen that returns canned JSON responses to wizard commands."""
    proc = MagicMock()
    proc.stdout = MagicMock()
    proc.stdin = MagicMock()
    proc.stderr = MagicMock()
    proc.kill = MagicMock()

    call_count = [0]

    def mock_readline():
        if call_count[0] < len(responses):
            resp = responses[call_count[0]]
            call_count[0] += 1
            return json.dumps(resp) + '\n'
        return ''

    proc.stdout.readline = mock_readline
    proc.stdout.fileno = MagicMock(return_value=99)
    return proc


class TestIecOnboardWizard(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ.setdefault('SECRET_KEY', 'test-secret')
        os.environ.setdefault('IEC_SYNC_SECRET', 'test-iec-secret')

        from app import app, get_db, _iec_wizard_sessions
        cls.app = app
        cls.app.config['TESTING'] = True
        cls._sessions = _iec_wizard_sessions

    def setUp(self):
        self._sessions.clear()
        self.client = self.app.test_client()
        # Login as admin (CEO)
        with self.client.session_transaction() as sess:
            sess['user_id'] = 1
            sess['user_role'] = 'admin'
            sess['branch_id'] = 126

    def tearDown(self):
        # Kill any leftover wizard processes
        for token, s in list(self._sessions.items()):
            try:
                s['proc'].kill()
            except Exception:
                pass
        self._sessions.clear()

    # ── Permission checks ──

    def test_non_admin_cannot_onboard_other_branch(self):
        """Manager of branch 126 cannot start wizard for branch 127."""
        with self.client.session_transaction() as sess:
            sess['user_role'] = 'manager'
            sess['branch_id'] = 126

        resp = self.client.post('/api/iec/onboard/start',
                                json={'branch_id': 127, 'id_number': '123456789'})
        self.assertEqual(resp.status_code, 403)
        data = resp.get_json()
        self.assertFalse(data['ok'])

    def test_admin_can_onboard_any_branch(self):
        """Admin (CEO) can start wizard for any branch."""
        mock_proc = _make_mock_proc([{"ok": True, "factor": "SMS"}])
        with patch('subprocess.Popen', return_value=mock_proc), \
             patch('select.select', return_value=([mock_proc.stdout], [], [])):
            resp = self.client.post('/api/iec/onboard/start',
                                    json={'branch_id': 127, 'id_number': '123456789'})
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data['ok'])
        self.assertIn('session_token', data)

    def test_manager_can_onboard_own_branch(self):
        """Manager can start wizard for their own branch."""
        with self.client.session_transaction() as sess:
            sess['user_role'] = 'manager'
            sess['branch_id'] = 126

        mock_proc = _make_mock_proc([{"ok": True, "factor": "SMS"}])
        with patch('subprocess.Popen', return_value=mock_proc), \
             patch('select.select', return_value=([mock_proc.stdout], [], [])):
            resp = self.client.post('/api/iec/onboard/start',
                                    json={'branch_id': 126, 'id_number': '123456789'})
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data['ok'])

    # ── Unauthenticated ──

    def test_unauthenticated_rejected(self):
        """No login session → 401."""
        with self.client.session_transaction() as sess:
            sess.clear()
        resp = self.client.post('/api/iec/onboard/start',
                                json={'branch_id': 126, 'id_number': '123456789'},
                                content_type='application/json')
        self.assertEqual(resp.status_code, 401)

    # ── Input validation ──

    def test_invalid_id_number(self):
        """Non-numeric ID → 400."""
        resp = self.client.post('/api/iec/onboard/start',
                                json={'branch_id': 126, 'id_number': 'abc'})
        self.assertEqual(resp.status_code, 400)

    def test_missing_params(self):
        """Missing branch_id → 400."""
        resp = self.client.post('/api/iec/onboard/start',
                                json={'id_number': '123456789'})
        self.assertEqual(resp.status_code, 400)

    # ── Session expiry ──

    def test_expired_session_rejected(self):
        """After 13 minutes, the session should be expired."""
        mock_proc = _make_mock_proc([{"ok": True, "factor": "SMS"}])
        with patch('subprocess.Popen', return_value=mock_proc), \
             patch('select.select', return_value=([mock_proc.stdout], [], [])):
            resp = self.client.post('/api/iec/onboard/start',
                                    json={'branch_id': 126, 'id_number': '123456789'})
        data = resp.get_json()
        token = data['session_token']

        # Manually expire the session
        self._sessions[token]['created_at'] = time.time() - 780  # 13 min ago

        resp = self.client.post('/api/iec/onboard/verify',
                                json={'branch_id': 126, 'session_token': token, 'otp': '123456'})
        self.assertEqual(resp.status_code, 400)
        data = resp.get_json()
        self.assertIn('פג תוקף', data['error'])

    # ── Full flow ──

    @patch('app.get_db')
    def test_full_wizard_flow(self, mock_get_db):
        """Complete wizard: start → verify → save."""
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchone.return_value = {'id': 126}
        mock_get_db.return_value = mock_db

        # Step 1: Start
        mock_proc = _make_mock_proc([
            {"ok": True, "factor": "SMS"},
            {"ok": True, "contracts": [{"contract_id": "000346412955", "address": "Test St"}], "bp_number": "12345"},
            {"ok": True, "iec_user_id": "123456789", "iec_token": "refresh_tok_abc", "iec_bp_number": "12345", "iec_contract_id": "000346412955"}
        ])
        with patch('subprocess.Popen', return_value=mock_proc), \
             patch('select.select', return_value=([mock_proc.stdout], [], [])):
            resp1 = self.client.post('/api/iec/onboard/start',
                                     json={'branch_id': 126, 'id_number': '123456789'})
            data1 = resp1.get_json()
            self.assertTrue(data1['ok'])
            token = data1['session_token']

            # Step 2: Verify
            resp2 = self.client.post('/api/iec/onboard/verify',
                                     json={'branch_id': 126, 'session_token': token, 'otp': '123456'})
            data2 = resp2.get_json()
            self.assertTrue(data2['ok'])
            self.assertEqual(len(data2['contracts']), 1)
            self.assertEqual(data2['contracts'][0]['contract_id'], '000346412955')

            # Step 3: Save
            resp3 = self.client.post('/api/iec/onboard/save',
                                     json={'branch_id': 126, 'session_token': token, 'contract_id': '000346412955'})
            data3 = resp3.get_json()
            self.assertTrue(data3['ok'])

        # Session should be cleaned up
        self.assertNotIn(token, self._sessions)

    # ── Error from IEC ──

    def test_iec_rejects_id(self):
        """IEC API rejects the ID number → friendly error, no PII leak."""
        mock_proc = _make_mock_proc([{"ok": False, "error": "ID 123456789 not found in IEC"}])
        with patch('subprocess.Popen', return_value=mock_proc), \
             patch('select.select', return_value=([mock_proc.stdout], [], [])):
            resp = self.client.post('/api/iec/onboard/start',
                                    json={'branch_id': 126, 'id_number': '123456789'})
        data = resp.get_json()
        self.assertFalse(data['ok'])
        # The ID number should NOT appear in the error message
        self.assertNotIn('123456789', data['error'])

    # ── OTP timing info ──

    def test_start_returns_expires_at(self):
        """Start response includes expires_at for the timer."""
        mock_proc = _make_mock_proc([{"ok": True, "factor": "SMS"}])
        with patch('subprocess.Popen', return_value=mock_proc), \
             patch('select.select', return_value=([mock_proc.stdout], [], [])):
            resp = self.client.post('/api/iec/onboard/start',
                                    json={'branch_id': 126, 'id_number': '123456789'})
        data = resp.get_json()
        self.assertIn('expires_at', data)
        self.assertGreater(data['expires_at'], time.time())

    # ── Nonexistent session ──

    def test_verify_with_bad_token(self):
        """Verify with nonexistent session token → 400."""
        resp = self.client.post('/api/iec/onboard/verify',
                                json={'branch_id': 126, 'session_token': 'nonexistent', 'otp': '123456'})
        self.assertEqual(resp.status_code, 400)

    def test_save_with_bad_token(self):
        """Save with nonexistent session token → 400."""
        resp = self.client.post('/api/iec/onboard/save',
                                json={'branch_id': 126, 'session_token': 'nonexistent', 'contract_id': '123'})
        self.assertEqual(resp.status_code, 400)


if __name__ == '__main__':
    unittest.main(verbosity=2)

"""Tests for analytics event collection: role exclusion + branch_id stamping."""
import os
import sys
import sqlite3
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, init_db, DB_PATH, _should_track
from werkzeug.security import generate_password_hash


@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_analytics.db')

    import app as app_module
    original_db = app_module.DB_PATH
    if os.path.exists(test_db):
        os.remove(test_db)
    app_module.DB_PATH = test_db
    app_module.init_db()

    conn = sqlite3.connect(test_db, timeout=30)
    # user_events lives in migration 007 — not in schema.sql, create it inline.
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS user_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            page TEXT,
            branch_id INTEGER,
            duration_seconds INTEGER,
            user_agent TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.execute("INSERT OR REPLACE INTO branches (id, name, city, active) VALUES (126, 'איינשטיין', 'תל אביב', 1)")
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) VALUES "
        "(1, 'Admin', 'admin@test.com', ?, 'admin', 1)",
        (generate_password_hash('test123'),))
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) VALUES "
        "(2, 'Manager', 'mgr@test.com', ?, 'manager', 1)",
        (generate_password_hash('test123'),))
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) VALUES "
        "(3, 'Ceo', 'ceo@test.com', ?, 'ceo', 1)",
        (generate_password_hash('test123'),))
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (2, 126)")
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(test_db):
        os.remove(test_db)


def _login(client, email, password='test123'):
    return client.post('/login', data={'email': email, 'password': password},
                       follow_redirects=False)


def _count_events(user_id, event_type=None):
    conn = sqlite3.connect(app.config.get('DB_PATH') or
                           os.path.join(os.path.dirname(__file__), 'test_analytics.db'))
    if event_type:
        row = conn.execute(
            'SELECT COUNT(*) FROM user_events WHERE user_id=? AND event_type=?',
            (user_id, event_type)).fetchone()
    else:
        row = conn.execute(
            'SELECT COUNT(*) FROM user_events WHERE user_id=?',
            (user_id,)).fetchone()
    conn.close()
    return row[0]


def _last_event(user_id, event_type):
    conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), 'test_analytics.db'))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        'SELECT * FROM user_events WHERE user_id=? AND event_type=? '
        'ORDER BY id DESC LIMIT 1',
        (user_id, event_type)).fetchone()
    conn.close()
    return row


# ── Task 2: _should_track helper ──────────────────────────────

class TestShouldTrack:

    def test_admin_not_tracked(self):
        assert _should_track('admin') is False

    def test_ceo_not_tracked(self):
        assert _should_track('ceo') is False

    def test_manager_tracked(self):
        assert _should_track('manager') is True


# ── Task 2: end-to-end exclusion via login event ──────────────

class TestRoleExclusion:

    def test_admin_login_creates_no_event(self, client):
        _login(client, 'admin@test.com')
        assert _count_events(1) == 0

    def test_ceo_login_creates_no_event(self, client):
        _login(client, 'ceo@test.com')
        assert _count_events(3) == 0

    def test_manager_login_creates_event(self, client):
        _login(client, 'mgr@test.com')
        assert _count_events(2, 'login') == 1


# ── Task 3: heartbeat stamps branch_id ─────────────────────────

class TestHeartbeatBranchId:

    def test_heartbeat_stamps_branch_id_from_session(self, client):
        """Manager hits /api/events/heartbeat with no branch_id in payload;
        server should fall back to session['branch_id']."""
        _login(client, 'mgr@test.com')
        res = client.post('/api/events/heartbeat',
                          json={'page': '/', 'duration_seconds': 30})
        assert res.status_code == 204
        row = _last_event(2, 'heartbeat')
        assert row is not None
        assert row['branch_id'] == 126

    def test_heartbeat_uses_explicit_branch_id_when_provided(self, client):
        _login(client, 'mgr@test.com')
        res = client.post('/api/events/heartbeat',
                          json={'page': '/sales', 'branch_id': 126,
                                'duration_seconds': 45})
        assert res.status_code == 204
        row = _last_event(2, 'heartbeat')
        assert row['branch_id'] == 126

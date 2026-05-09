"""Tests for cleanup_aviv_report_pending_names.py migration script."""

import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from scripts.cleanup_aviv_report_pending_names import cleanup


def _make_db(path):
    conn = sqlite3.connect(path)
    conn.executescript('''
        CREATE TABLE employee_match_pending (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER,
            month TEXT,
            csv_name TEXT,
            suggested_employee_id INTEGER,
            confidence TEXT,
            hours REAL,
            salary REAL,
            created_at TEXT,
            resolved INTEGER DEFAULT 0,
            aviv_employee_id INTEGER,
            source TEXT DEFAULT 'csv',
            is_new_employee INTEGER DEFAULT 0,
            is_csv_only INTEGER DEFAULT 0
        );
    ''')
    return conn


class TestCleanupMigration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.tmp.close()
        self.db_path = self.tmp.name
        conn = _make_db(self.db_path)
        # 3 polluted aviv_report rows
        conn.execute(
            "INSERT INTO employee_match_pending "
            "(branch_id, month, csv_name, hours, source, is_new_employee, resolved) "
            "VALUES (127, '2026-05', '551 אגם צאצאן תיכון', 49.4, 'aviv_report', 1, 0)")
        conn.execute(
            "INSERT INTO employee_match_pending "
            "(branch_id, month, csv_name, hours, source, is_new_employee, resolved) "
            "VALUES (127, '2026-05', '732 דביר פישר תיכון', 106.7, 'aviv_report', 1, 0)")
        conn.execute(
            "INSERT INTO employee_match_pending "
            "(branch_id, month, csv_name, hours, source, is_new_employee, resolved) "
            "VALUES (127, '2026-04', '733 דוד דהן תיכון', 35.4, 'aviv_report', 1, 0)")
        # Already-clean aviv_report row — must NOT be touched
        conn.execute(
            "INSERT INTO employee_match_pending "
            "(branch_id, month, csv_name, aviv_employee_id, hours, source, is_new_employee, resolved) "
            "VALUES (127, '2026-05', 'נקי כבר', 999, 12.0, 'aviv_report', 1, 0)")
        # CSV row with digit prefix — different source, must NOT be touched
        conn.execute(
            "INSERT INTO employee_match_pending "
            "(branch_id, month, csv_name, hours, source, resolved) "
            "VALUES (126, '2026-05', '441 עידן בקון', 88.0, 'csv', 0)")
        # Resolved aviv_report row — must NOT be touched
        conn.execute(
            "INSERT INTO employee_match_pending "
            "(branch_id, month, csv_name, hours, source, resolved) "
            "VALUES (127, '2026-04', '500 ישן ופתור', 1.0, 'aviv_report', 1)")
        conn.commit()
        conn.close()

    def tearDown(self):
        os.unlink(self.db_path)

    def test_cleans_polluted_rows(self):
        res = cleanup(self.db_path)
        self.assertEqual(res['changed'], 3)

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT csv_name, aviv_employee_id FROM employee_match_pending "
            "WHERE source='aviv_report' AND resolved=0 ORDER BY id"
        ).fetchall()
        # 1: 551 stripped
        self.assertEqual(rows[0]['csv_name'], 'אגם צאצאן תיכון')
        self.assertEqual(rows[0]['aviv_employee_id'], 551)
        # 2: 732 stripped
        self.assertEqual(rows[1]['csv_name'], 'דביר פישר תיכון')
        self.assertEqual(rows[1]['aviv_employee_id'], 732)
        # 3: 733 stripped (different month)
        self.assertEqual(rows[2]['csv_name'], 'דוד דהן תיכון')
        self.assertEqual(rows[2]['aviv_employee_id'], 733)
        # 4: already-clean row left alone
        self.assertEqual(rows[3]['csv_name'], 'נקי כבר')
        self.assertEqual(rows[3]['aviv_employee_id'], 999)
        conn.close()

    def test_does_not_touch_csv_source(self):
        cleanup(self.db_path)
        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT csv_name, aviv_employee_id FROM employee_match_pending "
            "WHERE source='csv'"
        ).fetchone()
        self.assertEqual(row[0], '441 עידן בקון')
        self.assertIsNone(row[1])
        conn.close()

    def test_does_not_touch_resolved(self):
        cleanup(self.db_path)
        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT csv_name FROM employee_match_pending "
            "WHERE source='aviv_report' AND resolved=1"
        ).fetchone()
        self.assertEqual(row[0], '500 ישן ופתור')
        conn.close()

    def test_idempotent(self):
        first = cleanup(self.db_path)
        second = cleanup(self.db_path)
        self.assertEqual(first['changed'], 3)
        self.assertEqual(second['changed'], 0)

        conn = sqlite3.connect(self.db_path)
        rows = conn.execute(
            "SELECT csv_name FROM employee_match_pending "
            "WHERE source='aviv_report' AND resolved=0"
        ).fetchall()
        for r in rows:
            self.assertFalse(r[0].split()[0].isdigit(), r[0])
        conn.close()

    def test_dry_run_does_not_write(self):
        res = cleanup(self.db_path, dry_run=True)
        self.assertEqual(res['changed'], 3)
        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT csv_name FROM employee_match_pending WHERE id=1"
        ).fetchone()
        self.assertEqual(row[0], '551 אגם צאצאן תיכון')
        conn.close()


if __name__ == '__main__':
    unittest.main()

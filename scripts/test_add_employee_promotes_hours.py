"""Tests for /api/employee-match-pending/<id>/add-new — verifies the
add-as-new-employee flow creates the employee, promotes hours from ALL
matching pending rows, and rejects bad input."""

import os
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


SCHEMA = '''
CREATE TABLE branches (
    id INTEGER PRIMARY KEY,
    name TEXT, city TEXT, active INTEGER DEFAULT 1,
    aviv_user_id TEXT, aviv_password TEXT,
    bilboy_user TEXT, bilboy_pass TEXT, gmail_label TEXT,
    franchise_supplier TEXT, avg_hourly_rate REAL DEFAULT 0,
    hours_this_month REAL DEFAULT 0, hours_baseline REAL DEFAULT 0,
    hours_updated_at TEXT
);
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT, email TEXT UNIQUE, password_hash TEXT,
    role TEXT, active INTEGER DEFAULT 1
);
CREATE TABLE user_branches (user_id INTEGER, branch_id INTEGER);
CREATE TABLE employees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id INTEGER, name TEXT, role TEXT,
    hourly_rate REAL DEFAULT 0, active INTEGER DEFAULT 1,
    aviv_employee_id INTEGER
);
CREATE TABLE employee_hours (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id INTEGER, month TEXT, employee_name TEXT,
    total_hours REAL DEFAULT 0, total_salary REAL DEFAULT 0,
    source TEXT DEFAULT 'csv', verified_by_csv INTEGER DEFAULT 0,
    UNIQUE(branch_id, month, employee_name)
);
CREATE TABLE employee_match_pending (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id INTEGER, month TEXT, csv_name TEXT,
    suggested_employee_id INTEGER, confidence TEXT,
    hours REAL, salary REAL, created_at TEXT,
    resolved INTEGER DEFAULT 0, aviv_employee_id INTEGER,
    source TEXT DEFAULT 'csv',
    is_new_employee INTEGER DEFAULT 0, is_csv_only INTEGER DEFAULT 0
);
CREATE TABLE employee_aliases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id INTEGER, alias_name TEXT, branch_id INTEGER,
    created_at TEXT,
    UNIQUE(branch_id, alias_name)
);
CREATE TABLE agent_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id INTEGER, agent TEXT, started_at TEXT, finished_at TEXT,
    status TEXT, docs_count INTEGER, amount REAL, message TEXT,
    duration_seconds REAL, dismissed INTEGER DEFAULT 0
);
CREATE TABLE daily_sales (branch_id INTEGER, date TEXT, amount REAL, transactions INTEGER, source TEXT);
CREATE TABLE goods_documents (branch_id INTEGER, ref_number TEXT, supplier_id TEXT, supplier_name TEXT, doc_type INTEGER, doc_date TEXT, amount REAL, month TEXT);
CREATE TABLE fixed_expenses (branch_id INTEGER, name TEXT, amount REAL, expense_type TEXT, month TEXT);
'''


def _seed_db(path):
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.execute("INSERT INTO branches (id, name, city, active) VALUES (127, 'תיכון', 'Test', 1)")
    # Two pending rows for same person across two months — both must be promoted.
    conn.execute(
        "INSERT INTO employee_match_pending "
        "(id, branch_id, month, csv_name, hours, source, is_new_employee, resolved, aviv_employee_id) "
        "VALUES (1, 127, '2026-05', 'אגם צאצאן תיכון', 49.4, 'aviv_report', 1, 0, 551)")
    conn.execute(
        "INSERT INTO employee_match_pending "
        "(id, branch_id, month, csv_name, hours, source, is_new_employee, resolved, aviv_employee_id) "
        "VALUES (2, 127, '2026-04', 'אגם צאצאן תיכון', 80.0, 'aviv_report', 1, 0, 551)")
    # Different source — must NOT be promoted by this call.
    conn.execute(
        "INSERT INTO employee_match_pending "
        "(id, branch_id, month, csv_name, hours, source, is_new_employee, resolved) "
        "VALUES (3, 127, '2026-05', 'אגם צאצאן תיכון', 5.0, 'csv', 1, 0)")
    conn.commit()
    conn.close()


class AddEmployeeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.tmp.close()
        self.db_path = self.tmp.name
        _seed_db(self.db_path)

        # Patch app's DB_PATH to point at our temp DB before importing.
        self._patch = patch.dict(os.environ, {'SECRET_KEY': 'test'})
        self._patch.start()

        import app as app_module
        self._orig_db_path = app_module.DB_PATH
        app_module.DB_PATH = self.db_path
        self.app_module = app_module
        app_module.app.config['TESTING'] = True
        self.client = app_module.app.test_client()

        # Bypass login by setting session.
        with self.client.session_transaction() as sess:
            sess['user_id'] = 1
            sess['user_role'] = 'ceo'
            sess['user_branches'] = [127]
            sess['branch_id'] = 127

    def tearDown(self):
        self.app_module.DB_PATH = self._orig_db_path
        self._patch.stop()
        os.unlink(self.db_path)

    def _conn(self):
        c = sqlite3.connect(self.db_path)
        c.row_factory = sqlite3.Row
        return c

    def test_promotes_all_months_for_same_csv_name(self):
        res = self.client.post(
            '/api/employee-match-pending/1/add-new',
            json={'name': 'אגם צאצאן', 'hourly_rate': 40, 'role': 'ערב'})
        self.assertEqual(res.status_code, 200, res.get_data(as_text=True))
        body = res.get_json()
        self.assertTrue(body['ok'])
        self.assertEqual(sorted(body['promoted_months']), ['2026-04', '2026-05'])

        c = self._conn()
        # Employee created with aviv_employee_id propagated.
        emp = c.execute(
            "SELECT name, hourly_rate, aviv_employee_id, active FROM employees "
            "WHERE branch_id=127"
        ).fetchone()
        self.assertEqual(emp['name'], 'אגם צאצאן')
        self.assertEqual(emp['hourly_rate'], 40.0)
        self.assertEqual(emp['aviv_employee_id'], 551)
        self.assertEqual(emp['active'], 1)

        # Hours rows for both months, salary = hours * 40.
        rows = c.execute(
            "SELECT month, total_hours, total_salary, source FROM employee_hours "
            "WHERE branch_id=127 ORDER BY month"
        ).fetchall()
        self.assertEqual(len(rows), 2)
        by_month = {r['month']: r for r in rows}
        self.assertAlmostEqual(by_month['2026-05']['total_hours'], 49.4, places=1)
        self.assertAlmostEqual(by_month['2026-05']['total_salary'], 49.4 * 40, places=1)
        self.assertEqual(by_month['2026-05']['source'], 'aviv_report')
        self.assertAlmostEqual(by_month['2026-04']['total_hours'], 80.0, places=1)
        self.assertAlmostEqual(by_month['2026-04']['total_salary'], 80.0 * 40, places=1)

        # Both same-source pending rows resolved; CSV-source row left alone.
        pend = c.execute(
            "SELECT id, source, resolved FROM employee_match_pending "
            "ORDER BY id"
        ).fetchall()
        self.assertEqual(pend[0]['resolved'], 1)  # aviv_report 2026-05
        self.assertEqual(pend[1]['resolved'], 1)  # aviv_report 2026-04
        self.assertEqual(pend[2]['resolved'], 0)  # csv — different source
        c.close()

    def test_rate_zero_returns_400_no_writes(self):
        res = self.client.post(
            '/api/employee-match-pending/1/add-new',
            json={'name': 'אגם צאצאן', 'hourly_rate': 0, 'role': 'ערב'})
        self.assertEqual(res.status_code, 400)

        c = self._conn()
        n_emp = c.execute("SELECT COUNT(*) FROM employees").fetchone()[0]
        n_hours = c.execute("SELECT COUNT(*) FROM employee_hours").fetchone()[0]
        n_resolved = c.execute(
            "SELECT COUNT(*) FROM employee_match_pending WHERE resolved=1"
        ).fetchone()[0]
        self.assertEqual(n_emp, 0)
        self.assertEqual(n_hours, 0)
        self.assertEqual(n_resolved, 0)
        c.close()

    def test_rate_missing_returns_400(self):
        res = self.client.post(
            '/api/employee-match-pending/1/add-new',
            json={'name': 'אגם צאצאן', 'role': 'ערב'})
        self.assertEqual(res.status_code, 400)

    def test_name_missing_returns_400(self):
        res = self.client.post(
            '/api/employee-match-pending/1/add-new',
            json={'name': '', 'hourly_rate': 40, 'role': 'ערב'})
        self.assertEqual(res.status_code, 400)


if __name__ == '__main__':
    unittest.main()

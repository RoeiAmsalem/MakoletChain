"""Tests: /api/employees card data must surface aviv_report rows
(parallel to this morning's _calculate_salary_cost fix). Without this fix,
employees with only aviv_report hours render as 'טרם הועלה דוח שעות'.
"""

import os
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


SCHEMA = '''
CREATE TABLE branches (id INTEGER PRIMARY KEY, name TEXT, city TEXT, active INTEGER DEFAULT 1,
    aviv_user_id TEXT, aviv_password TEXT, bilboy_user TEXT, bilboy_pass TEXT, gmail_label TEXT,
    franchise_supplier TEXT, avg_hourly_rate REAL DEFAULT 0,
    hours_this_month REAL DEFAULT 0, hours_baseline REAL DEFAULT 0, hours_updated_at TEXT,
    iec_token TEXT, iec_last_sync_at TEXT);
CREATE TABLE employees (id INTEGER PRIMARY KEY AUTOINCREMENT, branch_id INTEGER, name TEXT,
    role TEXT, hourly_rate REAL DEFAULT 0, active INTEGER DEFAULT 1, aviv_employee_id INTEGER);
CREATE TABLE employee_hours (id INTEGER PRIMARY KEY AUTOINCREMENT, branch_id INTEGER,
    month TEXT, employee_name TEXT, total_hours REAL DEFAULT 0, total_salary REAL DEFAULT 0,
    source TEXT DEFAULT 'csv', verified_by_csv INTEGER DEFAULT 0,
    UNIQUE(branch_id, month, employee_name));
CREATE TABLE employee_match_pending (id INTEGER PRIMARY KEY AUTOINCREMENT, branch_id INTEGER,
    month TEXT, csv_name TEXT, suggested_employee_id INTEGER, confidence TEXT, hours REAL,
    salary REAL, created_at TEXT, resolved INTEGER DEFAULT 0, aviv_employee_id INTEGER,
    source TEXT DEFAULT 'csv', is_new_employee INTEGER DEFAULT 0, is_csv_only INTEGER DEFAULT 0);
CREATE TABLE employee_aliases (id INTEGER PRIMARY KEY AUTOINCREMENT, employee_id INTEGER,
    alias_name TEXT, branch_id INTEGER, created_at TEXT, UNIQUE(branch_id, alias_name));
CREATE TABLE agent_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, branch_id INTEGER, agent TEXT,
    started_at TEXT, finished_at TEXT, status TEXT, docs_count INTEGER, amount REAL,
    message TEXT, duration_seconds REAL, dismissed INTEGER DEFAULT 0);
CREATE TABLE daily_sales (branch_id INTEGER, date TEXT, amount REAL, transactions INTEGER, source TEXT);
CREATE TABLE goods_documents (branch_id INTEGER, ref_number TEXT, supplier_id TEXT,
    supplier_name TEXT, doc_type INTEGER, doc_date TEXT, amount REAL, month TEXT);
CREATE TABLE fixed_expenses (branch_id INTEGER, name TEXT, amount REAL, expense_type TEXT, month TEXT);
'''


def _seed(path):
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.execute("INSERT INTO branches (id, name, city, active) VALUES (127, 'תיכון', 'Test', 1)")
    # Three employees: one with aviv_api hours, one with aviv_report hours, one with no hours.
    conn.execute("INSERT INTO employees (id, branch_id, name, role, hourly_rate, active) "
                 "VALUES (1, 127, 'אגם צאצאן', 'ערב', 35, 1)")
    conn.execute("INSERT INTO employees (id, branch_id, name, role, hourly_rate, active) "
                 "VALUES (2, 127, 'שילת טולדנו', 'ערב', 40, 1)")
    conn.execute("INSERT INTO employees (id, branch_id, name, role, hourly_rate, active) "
                 "VALUES (3, 127, 'ללא שעות', 'בוקר', 38, 1)")
    conn.execute("INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, "
                 "total_salary, source) VALUES (127, '2026-05', 'אגם צאצאן', 18.7, 654.5, 'aviv_api')")
    conn.execute("INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, "
                 "total_salary, source) VALUES (127, '2026-05', 'שילת טולדנו', 45.5, 1820.0, 'aviv_report')")
    # CSV row should NOT count (CSV path retired).
    conn.execute("INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, "
                 "total_salary, source) VALUES (127, '2026-05', 'ללא שעות', 99.9, 0, 'csv')")
    conn.commit()
    conn.close()


class CardHoursAvivReportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.tmp.close()
        self.db_path = self.tmp.name
        _seed(self.db_path)

        self._env = patch.dict(os.environ, {'SECRET_KEY': 'test'})
        self._env.start()
        import app as app_module
        self._orig = app_module.DB_PATH
        app_module.DB_PATH = self.db_path
        self.app_module = app_module
        app_module.app.config['TESTING'] = True
        self.client = app_module.app.test_client()
        with self.client.session_transaction() as sess:
            sess['user_id'] = 1
            sess['user_role'] = 'admin'
            sess['user_branches'] = [127]
            sess['branch_id'] = 127

    def tearDown(self):
        self.app_module.DB_PATH = self._orig
        self._env.stop()
        os.unlink(self.db_path)

    def test_card_includes_aviv_report_hours(self):
        res = self.client.get('/api/employees?month=2026-05&branch_id=127')
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        by_name = {e['name']: e for e in body['employees']}

        # aviv_api row → hours visible (control).
        self.assertAlmostEqual(by_name['אגם צאצאן']['hours'], 18.7, places=1)
        self.assertEqual(by_name['אגם צאצאן']['hours_source'], 'aviv_api')

        # aviv_report row → hours visible (this is the fix).
        self.assertAlmostEqual(by_name['שילת טולדנו']['hours'], 45.5, places=1)
        self.assertEqual(by_name['שילת טולדנו']['hours_source'], 'aviv_report')
        self.assertGreater(by_name['שילת טולדנו']['salary'], 0)

        # CSV row → still ignored (CSV path retired).
        self.assertEqual(by_name['ללא שעות']['hours'], 0)
        self.assertEqual(by_name['ללא שעות']['hours_source'], 'none')


class OpsAvivReportAgentTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.tmp.close()
        self.db_path = self.tmp.name
        _seed(self.db_path)
        # Seed an aviv_report run for branch 127 so /ops should surface it.
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO agent_runs (branch_id, agent, started_at, finished_at, status, "
            "docs_count, amount, message, duration_seconds) "
            "VALUES (127, 'aviv_report', datetime('now','-1 hour'), datetime('now','-59 minutes'), "
            "'success', 5, 78.7, 'matched=2 unmatched=3 open_shifts=0 hours=78.7', 12.5)")
        conn.commit()
        conn.close()

        self._env = patch.dict(os.environ, {'SECRET_KEY': 'test'})
        self._env.start()
        import app as app_module
        self._orig = app_module.DB_PATH
        app_module.DB_PATH = self.db_path
        self.app_module = app_module
        app_module.app.config['TESTING'] = True
        self.client = app_module.app.test_client()
        with self.client.session_transaction() as sess:
            sess['user_id'] = 1
            sess['user_role'] = 'admin'
            sess['user_branches'] = [127]
            sess['branch_id'] = 127

    def tearDown(self):
        self.app_module.DB_PATH = self._orig
        self._env.stop()
        os.unlink(self.db_path)

    def test_ops_status_surfaces_aviv_report(self):
        res = self.client.get('/api/ops-status')
        self.assertEqual(res.status_code, 200, res.get_data(as_text=True))
        body = res.get_json()
        branch = next(b for b in body['branches'] if b['id'] == 127)
        # The per-branch tile must include aviv_report alongside the other agents.
        self.assertIn('aviv_report', branch['agents'])
        a = branch['agents']['aviv_report']
        self.assertIsNotNone(a, 'aviv_report agent_data should not be None for seeded run')
        self.assertEqual(a['status'], 'success')
        self.assertIn('matched=2', a['message'])
        # And the recent-runs feed (auto-detected) also picks it up.
        recent_agents = {r['agent'] for r in body['agent_runs']}
        self.assertIn('aviv_report', recent_agents)


if __name__ == '__main__':
    unittest.main()

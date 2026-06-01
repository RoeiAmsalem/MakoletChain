"""Tests for the Aviv employer-report agent."""

import os
import sqlite3
import sys
import unittest
from datetime import date
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

FIXTURE_PATH = os.path.join(os.path.dirname(__file__), 'fixtures',
                             'aviv_employer_report_sample.xls')


class TestImports(unittest.TestCase):
    def test_module_exposes_expected_api(self):
        from agents import aviv_employees_report as m
        for name in ('fetch_report_list', 'find_employer_report_id',
                     'fetch_employer_report', 'parse_employer_report',
                     'parse_hh_mm', 'update_employee_hours', 'run_for_branch',
                     'EMPLOYER_REPORT_ID', 'AuthExpired'):
            self.assertTrue(hasattr(m, name), f'missing: {name}')


class TestParseHHMM(unittest.TestCase):
    def test_simple(self):
        from agents.aviv_employees_report import parse_hh_mm
        self.assertAlmostEqual(parse_hh_mm('108:34'), 108 + 34/60, places=4)
        self.assertAlmostEqual(parse_hh_mm('49:26'), 49 + 26/60, places=4)
        self.assertEqual(parse_hh_mm('00:00'), 0.0)

    def test_invalid_returns_zero(self):
        from agents.aviv_employees_report import parse_hh_mm
        self.assertEqual(parse_hh_mm(''), 0.0)
        self.assertEqual(parse_hh_mm(None), 0.0)
        self.assertEqual(parse_hh_mm('abc'), 0.0)
        self.assertEqual(parse_hh_mm('1:bad'), 0.0)


class TestParseEmployerReport(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(FIXTURE_PATH, 'rb') as f:
            cls.xls_bytes = f.read()
        from agents.aviv_employees_report import parse_employer_report
        cls.parsed = parse_employer_report(cls.xls_bytes)

    def test_six_employees(self):
        self.assertEqual(len(self.parsed), 6)

    def test_first_employee_name_strips_id_prefix(self):
        self.assertEqual(self.parsed[0]['raw_name'], 'אגם צאצאן תיכון')
        self.assertEqual(self.parsed[0]['aviv_employee_id'], 551)

    def test_all_employees_have_aviv_id(self):
        ids = [p['aviv_employee_id'] for p in self.parsed]
        self.assertEqual(sorted(ids), [551, 585, 712, 732, 733, 734])
        for p in self.parsed:
            self.assertFalse(p['raw_name'].split()[0].isdigit(),
                             f"raw_name still has prefix: {p['raw_name']}")

    def test_total_hours_sum(self):
        # subtotals in fixture: 49:26 + 108:34 + 47:22 + 106:42 + 40:14 + 35:24 = 387:42 = 387.7
        total = sum(p['total_hours'] for p in self.parsed)
        self.assertAlmostEqual(total, 387.7, places=1)

    def test_open_shift_count(self):
        # אין יציאה appears at rows 43 (דביר פישר) and 57 (דוד דהן)
        total_open = sum(p['open_shift_count'] for p in self.parsed)
        self.assertEqual(total_open, 2)
        by_name = {p['raw_name']: p for p in self.parsed}
        self.assertEqual(by_name['דביר פישר תיכון']['open_shift_count'], 1)
        self.assertEqual(by_name['דוד דהן תיכון']['open_shift_count'], 1)

    def test_each_has_shift_count(self):
        for p in self.parsed:
            self.assertGreater(p['shift_count'], 0)

    def test_total_shift_count(self):
        # grand total in fixture says 62
        total_shifts = sum(p['shift_count'] for p in self.parsed)
        self.assertEqual(total_shifts, 62)

    def test_each_employee_has_shifts_list(self):
        for p in self.parsed:
            self.assertIn('shifts', p)
            self.assertIsInstance(p['shifts'], list)
            self.assertGreater(len(p['shifts']), 0)

    def test_first_shift_fields(self):
        # אגם צאצאן row 1: entry 13/04/2026 16:02:33, exit 19:01:34, 02:59:01.
        first = self.parsed[0]['shifts'][0]
        self.assertEqual(first['shift_date'], '2026-04-13')
        self.assertEqual(first['start_ts'], '2026-04-13 16:02:33')
        self.assertEqual(first['end_ts'], '2026-04-13 19:01:34')
        self.assertEqual(first['day_of_week'], 'יום ב')
        self.assertFalse(first['is_open'])
        self.assertAlmostEqual(first['hours'], 2 + 59/60 + 1/3600, places=3)

    def test_open_shift_in_shifts_list(self):
        # דביר פישר has one open shift (17/04/2026 entry, no exit/hours).
        by_name = {p['raw_name']: p for p in self.parsed}
        dvir = by_name['דביר פישר תיכון']
        opens = [s for s in dvir['shifts'] if s['is_open']]
        self.assertEqual(len(opens), 1)
        o = opens[0]
        self.assertEqual(o['start_ts'], '2026-04-17 07:28:00')
        self.assertIsNone(o['end_ts'])
        self.assertEqual(o['hours'], 0.0)
        self.assertEqual(o['shift_date'], '2026-04-17')

    def test_shift_count_matches_shifts_len_when_dated(self):
        # Every parsed shift row has at least a date (orphan exit-only rows
        # carry the exit date). Shifts list length equals the reported count.
        for p in self.parsed:
            self.assertEqual(len(p['shifts']), p['shift_count'],
                             f"{p['raw_name']}: {len(p['shifts'])} != {p['shift_count']}")


class TestFetchReportList(unittest.TestCase):
    @patch('agents.aviv_employees_report.time.sleep')
    @patch('agents.aviv_employees_report.requests.request')
    def test_404_returns_empty(self, mock_req, mock_sleep):
        # 404 retried 3x by _http_with_retry, then fetch_report_list returns [].
        mock_req.return_value = MagicMock(status_code=404)
        from agents.aviv_employees_report import fetch_report_list
        self.assertEqual(fetch_report_list(3, 'tok'), [])
        self.assertEqual(mock_req.call_count, 3)

    @patch('agents.aviv_employees_report.requests.request')
    def test_401_raises_auth_expired(self, mock_req):
        mock_req.return_value = MagicMock(status_code=401)
        from agents.aviv_employees_report import fetch_report_list, AuthExpired
        with self.assertRaises(AuthExpired):
            fetch_report_list(3, 'tok')
        self.assertEqual(mock_req.call_count, 1)

    @patch('agents.aviv_employees_report.requests.request')
    def test_200_returns_json(self, mock_req):
        resp = MagicMock(status_code=200)
        resp.json.return_value = [{'reports': [{'id': 301}]}]
        resp.raise_for_status = MagicMock()
        mock_req.return_value = resp
        from agents.aviv_employees_report import fetch_report_list
        self.assertEqual(fetch_report_list(3, 'tok'),
                         [{'reports': [{'id': 301}]}])


class TestHttpRetry(unittest.TestCase):
    """Retry-with-backoff helper: 30s sleep on 4xx (non-401) / 5xx, max 3 tries."""

    def _resp(self, status):
        m = MagicMock(status_code=status)
        m.raise_for_status = MagicMock()
        return m

    @patch('agents.aviv_employees_report.time.sleep')
    @patch('agents.aviv_employees_report.requests.request')
    def test_404_twice_then_200_succeeds(self, mock_req, mock_sleep):
        mock_req.side_effect = [self._resp(404), self._resp(404), self._resp(200)]
        from agents.aviv_employees_report import _http_with_retry
        r = _http_with_retry('GET', 'http://x')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(mock_req.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch('agents.aviv_employees_report.time.sleep')
    @patch('agents.aviv_employees_report.requests.request')
    def test_401_no_retry_fail_fast(self, mock_req, mock_sleep):
        mock_req.return_value = self._resp(401)
        from agents.aviv_employees_report import _http_with_retry
        r = _http_with_retry('GET', 'http://x')
        self.assertEqual(r.status_code, 401)
        self.assertEqual(mock_req.call_count, 1)
        self.assertEqual(mock_sleep.call_count, 0)

    @patch('agents.aviv_employees_report.time.sleep')
    @patch('agents.aviv_employees_report.requests.request')
    def test_500_three_times_returns_last_failure(self, mock_req, mock_sleep):
        mock_req.side_effect = [self._resp(500), self._resp(500), self._resp(500)]
        from agents.aviv_employees_report import _http_with_retry
        r = _http_with_retry('GET', 'http://x')
        self.assertEqual(r.status_code, 500)
        self.assertEqual(mock_req.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)


class TestEmployeesTemplateMessage(unittest.TestCase):
    """Regression: empty-hours wording should not imply upload is pending."""

    def test_new_no_hours_message_present(self):
        path = os.path.join(os.path.dirname(__file__), '..', 'templates', 'employees.html')
        with open(path, encoding='utf-8') as f:
            html = f.read()
        self.assertIn('לא דווחו שעות באביב — בדוק שהעובד מופיע באביב POS', html)
        self.assertNotIn('טרם הועלה דוח שעות', html)


class TestFindEmployerReportId(unittest.TestCase):
    def test_found(self):
        from agents.aviv_employees_report import find_employer_report_id
        reports = [{'reports': [{'id': 1}, {'id': 301, 'name': 'x'}]}]
        self.assertEqual(find_employer_report_id(reports), 301)

    def test_missing_raises(self):
        from agents.aviv_employees_report import find_employer_report_id
        with self.assertRaises(ValueError):
            find_employer_report_id([{'reports': [{'id': 1}]}])


def _make_test_db():
    conn = sqlite3.connect(':memory:')
    conn.executescript('''
        CREATE TABLE branches (
            id INTEGER PRIMARY KEY,
            name TEXT,
            aviv_user_id TEXT,
            aviv_password TEXT,
            active INTEGER DEFAULT 1
        );
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER,
            name TEXT,
            hourly_rate REAL,
            active INTEGER DEFAULT 1,
            aviv_employee_id INTEGER
        );
        CREATE TABLE employee_hours (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER,
            month TEXT,
            employee_name TEXT,
            total_hours REAL DEFAULT 0,
            total_salary REAL DEFAULT 0,
            source TEXT DEFAULT 'csv',
            created_at TEXT,
            UNIQUE(branch_id, month, employee_name)
        );
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
            -- Columns formalized by migrations/006_aviv_report_pending_columns.sql
            aviv_employee_id INTEGER,
            source TEXT DEFAULT 'csv',
            is_new_employee INTEGER DEFAULT 0,
            is_csv_only INTEGER DEFAULT 0
        );
        CREATE TABLE agent_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER,
            agent TEXT,
            status TEXT,
            started_at TEXT,
            finished_at TEXT,
            docs_count INTEGER,
            amount REAL,
            message TEXT,
            duration_seconds REAL,
            dismissed INTEGER DEFAULT 0
        );
        CREATE TABLE employee_shifts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER NOT NULL,
            month TEXT NOT NULL,
            employee_name TEXT NOT NULL,
            shift_date TEXT,
            start_ts TEXT,
            end_ts TEXT,
            hours REAL DEFAULT 0,
            day_of_week TEXT,
            is_open INTEGER NOT NULL DEFAULT 0,
            source TEXT NOT NULL DEFAULT 'aviv_report',
            created_at TEXT DEFAULT (datetime('now'))
        );
    ''')
    conn.execute("INSERT INTO branches (id, name, aviv_user_id, aviv_password, active) "
                 "VALUES (127, 'תיכון', 'Tichon123', 'Tichon123', 1)")
    conn.execute("INSERT INTO employees (branch_id, name, hourly_rate, active) "
                 "VALUES (127, 'אגם צאצאן', 35.0, 1)")
    conn.execute("INSERT INTO employees (branch_id, name, hourly_rate, active) "
                 "VALUES (127, 'דביר פישר', 40.0, 1)")
    conn.commit()
    conn.row_factory = sqlite3.Row
    return conn


class TestUpdateEmployeeHours(unittest.TestCase):
    def setUp(self):
        self.conn = _make_test_db()

    def tearDown(self):
        self.conn.close()

    def _parsed(self):
        return [
            {'raw_name': 'אגם צאצאן תיכון', 'aviv_employee_id': 551,
             'total_hours': 49.4333, 'shift_count': 8, 'open_shift_count': 0},
            {'raw_name': 'דביר פישר תיכון', 'aviv_employee_id': 732,
             'total_hours': 106.7, 'shift_count': 14, 'open_shift_count': 1},
            {'raw_name': 'רנדומלי תיכון', 'aviv_employee_id': 999,
             'total_hours': 10.0, 'shift_count': 1, 'open_shift_count': 0},
        ]

    def test_basic_apply(self):
        from agents.aviv_employees_report import update_employee_hours
        res = update_employee_hours(127, '2026-05', self._parsed(), self.conn)
        self.assertEqual(res['matched'], 2)
        self.assertEqual(res['unmatched'], 1)
        self.assertEqual(res['open_shifts_total'], 1)

        rows = self.conn.execute(
            "SELECT employee_name, total_hours, source FROM employee_hours "
            "WHERE branch_id=127 AND month='2026-05' ORDER BY employee_name"
        ).fetchall()
        self.assertEqual(len(rows), 2)
        names = sorted(r['employee_name'] for r in rows)
        self.assertEqual(names, sorted(['אגם צאצאן', 'דביר פישר']))
        for r in rows:
            self.assertEqual(r['source'], 'aviv_report')

        pend = self.conn.execute(
            "SELECT csv_name, aviv_employee_id, source FROM employee_match_pending WHERE branch_id=127"
        ).fetchall()
        self.assertEqual(len(pend), 1)
        # Branch suffix 'תיכון' is stripped before insert into pending so the
        # name matches what the matcher / manager UI displays elsewhere.
        self.assertEqual(pend[0]['csv_name'], 'רנדומלי')
        self.assertEqual(pend[0]['aviv_employee_id'], 999)
        self.assertEqual(pend[0]['source'], 'aviv_report')

    def test_unmatched_csv_name_has_suffix_stripped(self):
        """Unmatched-path must call strip_store_suffix before pending insert.

        Regression for prod pending row id=17 ('זכאי זיני תיכון', branch 127):
        the matched-path strips 'תיכון', the unmatched-path used to store the
        raw suffixed name verbatim.
        """
        from agents.aviv_employees_report import update_employee_hours
        parsed = [{'raw_name': 'זכאי זיני תיכון', 'aviv_employee_id': 871,
                   'total_hours': 12.5, 'shift_count': 2, 'open_shift_count': 0}]
        update_employee_hours(127, '2026-04', parsed, self.conn)

        row = self.conn.execute(
            "SELECT csv_name FROM employee_match_pending WHERE branch_id=127 AND month='2026-04'"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row['csv_name'], 'זכאי זיני')

    def test_matched_path_unchanged(self):
        """Matched-path still writes employees by their DB name, not the raw input."""
        from agents.aviv_employees_report import update_employee_hours
        parsed = [{'raw_name': 'אגם צאצאן תיכון', 'aviv_employee_id': 551,
                   'total_hours': 49.43, 'shift_count': 8, 'open_shift_count': 0}]
        res = update_employee_hours(127, '2026-05', parsed, self.conn)
        self.assertEqual(res['matched'], 1)
        self.assertEqual(res['unmatched'], 0)
        row = self.conn.execute(
            "SELECT employee_name FROM employee_hours "
            "WHERE branch_id=127 AND month='2026-05' AND source='aviv_report'"
        ).fetchone()
        self.assertEqual(row['employee_name'], 'אגם צאצאן')

    def test_duration_seconds_populated(self):
        """run_for_branch must write duration_seconds (was NULL/0 in prod)."""
        from agents import aviv_employees_report as m
        with open(FIXTURE_PATH, 'rb') as f:
            xls_bytes = f.read()

        with _SharedConnContext(self.conn), \
             patch.object(m, '_login', return_value=('tok', 99)), \
             patch.object(m, '_refresh', return_value='tok'), \
             patch.object(m, 'fetch_report_list',
                           return_value=[{'reports': [{'id': 301}]}]), \
             patch.object(m, 'fetch_employer_report', return_value=xls_bytes):
            res = m.run_for_branch(127, today=date(2026, 5, 9))

        self.assertTrue(res.get('ok'))
        run = self.conn.execute(
            "SELECT duration_seconds FROM agent_runs "
            "WHERE agent='aviv_report' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        # The bug was that duration_seconds was never written, leaving the
        # column NULL (rendered as 0 in /ops). With the fix the agent always
        # populates it on the final UPDATE. Mocked tests run fast enough that
        # the rounded value can be 0.0, so we only assert it is not NULL.
        self.assertIsNotNone(run['duration_seconds'])
        self.assertGreaterEqual(run['duration_seconds'], 0.0)

    def test_does_not_touch_aviv_api_rows(self):
        """aviv_api rows for OTHER employees in same month must not be deleted."""
        self.conn.execute(
            "INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, "
            "total_salary, source) VALUES (127, '2026-05', 'מישהו אחר', 12.5, 500, 'aviv_api')"
        )
        self.conn.commit()
        from agents.aviv_employees_report import update_employee_hours
        update_employee_hours(127, '2026-05', self._parsed(), self.conn)

        api_row = self.conn.execute(
            "SELECT * FROM employee_hours WHERE source='aviv_api'"
        ).fetchall()
        self.assertEqual(len(api_row), 1)
        self.assertEqual(api_row[0]['employee_name'], 'מישהו אחר')

    def test_idempotent(self):
        from agents.aviv_employees_report import update_employee_hours
        update_employee_hours(127, '2026-05', self._parsed(), self.conn)
        update_employee_hours(127, '2026-05', self._parsed(), self.conn)

        rows = self.conn.execute(
            "SELECT COUNT(*) c FROM employee_hours "
            "WHERE branch_id=127 AND month='2026-05' AND source='aviv_report'"
        ).fetchone()
        self.assertEqual(rows['c'], 2)
        pend = self.conn.execute(
            "SELECT COUNT(*) c FROM employee_match_pending WHERE branch_id=127 AND resolved=0"
        ).fetchone()
        self.assertEqual(pend['c'], 1)

    def _parsed_with_shifts(self):
        return [
            {'raw_name': 'אגם צאצאן תיכון', 'aviv_employee_id': 551,
             'total_hours': 5.0, 'shift_count': 2, 'open_shift_count': 0,
             'shifts': [
                 {'shift_date': '2026-05-03', 'start_ts': '2026-05-03 13:59:00',
                  'end_ts': '2026-05-03 23:03:04', 'hours': 9.07,
                  'day_of_week': 'יום א', 'is_open': False},
                 {'shift_date': '2026-05-06', 'start_ts': '2026-05-06 15:56:30',
                  'end_ts': None, 'hours': 0.0, 'day_of_week': 'יום ד',
                  'is_open': True},
             ]},
            {'raw_name': 'רנדומלי תיכון', 'aviv_employee_id': 999,
             'total_hours': 10.0, 'shift_count': 1, 'open_shift_count': 0,
             'shifts': [
                 {'shift_date': '2026-05-01', 'start_ts': '2026-05-01 07:00:00',
                  'end_ts': '2026-05-01 17:00:00', 'hours': 10.0,
                  'day_of_week': 'יום ה', 'is_open': False},
             ]},
        ]

    def test_shifts_written_for_matched_only(self):
        from agents.aviv_employees_report import update_employee_hours
        update_employee_hours(127, '2026-05', self._parsed_with_shifts(), self.conn)
        rows = self.conn.execute(
            "SELECT employee_name, is_open FROM employee_shifts "
            "WHERE branch_id=127 AND month='2026-05' ORDER BY id"
        ).fetchall()
        # Only the matched employee (אגם צאצאן) gets shift rows; the unmatched
        # 'רנדומלי' goes to pending and writes no shifts.
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(r['employee_name'] == 'אגם צאצאן' for r in rows))
        self.assertEqual(sum(r['is_open'] for r in rows), 1)

    def test_shifts_overwrite_cleanly_on_resync(self):
        from agents.aviv_employees_report import update_employee_hours
        update_employee_hours(127, '2026-05', self._parsed_with_shifts(), self.conn)
        update_employee_hours(127, '2026-05', self._parsed_with_shifts(), self.conn)
        c = self.conn.execute(
            "SELECT COUNT(*) c FROM employee_shifts "
            "WHERE branch_id=127 AND month='2026-05' AND source='aviv_report'"
        ).fetchone()['c']
        self.assertEqual(c, 2)


class _SharedConnContext:
    """Pretend sqlite3.connect returns the same in-memory DB and skip close()."""
    def __init__(self, conn):
        self.conn = conn
    def __enter__(self):
        from agents import aviv_employees_report as m
        real = self.conn

        class Proxy:
            def __getattr__(self, n):
                return getattr(real, n)
            def close(self):
                pass
        proxy = Proxy()
        self._patcher = patch.object(m.sqlite3, 'connect',
                                     side_effect=lambda *a, **k: proxy)
        self._patcher.start()
        return self
    def __exit__(self, *a):
        self._patcher.stop()


class TestRunForBranch(unittest.TestCase):
    def setUp(self):
        self.conn = _make_test_db()
        with open(FIXTURE_PATH, 'rb') as f:
            self.xls_bytes = f.read()

    def tearDown(self):
        self.conn.close()

    def test_one_window_when_not_include_previous(self):
        from agents import aviv_employees_report as m
        calls = []

        def fake_fetch(*args, **kwargs):
            calls.append(args)
            return self.xls_bytes

        with _SharedConnContext(self.conn), \
             patch.object(m, '_login', return_value=('tok', 99)), \
             patch.object(m, '_refresh', return_value='tok'), \
             patch.object(m, 'fetch_report_list',
                           return_value=[{'reports': [{'id': 301}]}]), \
             patch.object(m, 'fetch_employer_report', side_effect=fake_fetch):
            res = m.run_for_branch(127, include_previous_month=False,
                                    today=date(2026, 5, 9))

        self.assertTrue(res.get('ok'), f'expected ok, got {res}')
        self.assertEqual(len(calls), 1)
        self.assertGreater(res['matched'], 0)

    def test_two_windows_when_include_previous(self):
        from agents import aviv_employees_report as m
        calls = []

        def fake_fetch(*args, **kwargs):
            calls.append(args)
            return self.xls_bytes

        with _SharedConnContext(self.conn), \
             patch.object(m, '_login', return_value=('tok', 99)), \
             patch.object(m, '_refresh', return_value='tok'), \
             patch.object(m, 'fetch_report_list',
                           return_value=[{'reports': [{'id': 301}]}]), \
             patch.object(m, 'fetch_employer_report', side_effect=fake_fetch):
            res = m.run_for_branch(127, include_previous_month=True,
                                    today=date(2026, 5, 9))

        self.assertTrue(res.get('ok'))
        self.assertEqual(len(calls), 2)
        from_dates = [c[1] for c in calls]
        to_dates = [c[2] for c in calls]
        self.assertIn('2026-05-01', from_dates)
        self.assertIn('2026-05-09', to_dates)
        self.assertIn('2026-04-01', from_dates)
        self.assertIn('2026-04-30', to_dates)

    def test_pos_offline_returns_skipped(self):
        from agents import aviv_employees_report as m
        with _SharedConnContext(self.conn), \
             patch.object(m, '_login', return_value=('tok', 99)), \
             patch.object(m, '_refresh', return_value='tok'), \
             patch.object(m, 'fetch_report_list', return_value=[]):
            res = m.run_for_branch(127, today=date(2026, 5, 9))
        self.assertTrue(res.get('ok'))
        self.assertTrue(res.get('skipped'))


class TestMonthlyReconciliation(unittest.TestCase):
    """10th-of-month previous-month reconciliation: date gate + change alert."""

    def setUp(self):
        self.conn = _make_test_db()
        # Previous-month (2026-05) stored totals — the "considered final" state.
        self.conn.execute(
            "INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, "
            "total_salary, source) VALUES (127, '2026-05', 'אגם צאצאן', 100.0, 4000.0, 'aviv_report')")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_date_gate_skips_when_not_10th(self):
        from agents import aviv_employees_report as m
        with _SharedConnContext(self.conn), patch.object(m, 'USE_CHAIN_AUTH', False):
            res = m.reconcile_previous_month(today=date(2026, 6, 11), force=False)
        self.assertFalse(res['ran'])
        self.assertEqual(res['reason'], 'not_10th')

    def test_force_runs_off_the_10th(self):
        from agents import aviv_employees_report as m
        with _SharedConnContext(self.conn), \
             patch.object(m, 'USE_CHAIN_AUTH', False), \
             patch.object(m, 'run_for_branch', side_effect=lambda bid, **kw: {'ok': True}), \
             patch('utils.notify.notify'), patch.object(m.time, 'sleep'):
            res = m.reconcile_previous_month(today=date(2026, 6, 15), force=True)
        self.assertTrue(res['ran'])
        self.assertEqual(res['month'], '2026-05')  # previous month only

    def test_change_fires_alert_and_overwrites(self):
        from agents import aviv_employees_report as m

        def fake_repull(bid, **kw):
            # Simulate a corrected re-pull: last month's total moved after month-end.
            self.conn.execute(
                "UPDATE employee_hours SET total_hours=110.0, total_salary=4400.0 "
                "WHERE branch_id=127 AND month='2026-05' AND source='aviv_report'")
            self.conn.commit()
            return {'ok': True}

        with _SharedConnContext(self.conn), \
             patch.object(m, 'USE_CHAIN_AUTH', False), \
             patch.object(m, 'run_for_branch', side_effect=fake_repull), \
             patch('utils.notify.notify') as mock_notify, \
             patch.object(m.time, 'sleep'):
            res = m.reconcile_previous_month(today=date(2026, 6, 10), force=False)

        self.assertTrue(res['ran'])
        self.assertEqual(res['month'], '2026-05')
        self.assertEqual(res['changed'], 1)
        self.assertEqual(res['checked'], 1)
        # Silent overwrite happened.
        new_total = self.conn.execute(
            "SELECT total_hours FROM employee_hours WHERE branch_id=127 AND month='2026-05'"
        ).fetchone()[0]
        self.assertEqual(new_total, 110.0)
        # And a brrr alert fired naming the change.
        self.assertEqual(mock_notify.call_count, 1)
        title, message = mock_notify.call_args[0][0], mock_notify.call_args[0][1]
        self.assertIn('Hours changed', title)
        self.assertIn('100', message)
        self.assertIn('110', message)

    def test_no_change_no_alert(self):
        from agents import aviv_employees_report as m
        with _SharedConnContext(self.conn), \
             patch.object(m, 'USE_CHAIN_AUTH', False), \
             patch.object(m, 'run_for_branch', side_effect=lambda bid, **kw: {'ok': True}), \
             patch('utils.notify.notify') as mock_notify, \
             patch.object(m.time, 'sleep'):
            res = m.reconcile_previous_month(today=date(2026, 6, 10))
        self.assertTrue(res['ran'])
        self.assertEqual(res['changed'], 0)
        self.assertEqual(res['checked'], 1)
        mock_notify.assert_not_called()

    def test_rounding_within_tolerance_is_ok(self):
        from agents import aviv_employees_report as m

        def tiny_change(bid, **kw):
            # +0.3h / +₪8 — both under tolerance (0.5h / ₪10) → no alert.
            self.conn.execute(
                "UPDATE employee_hours SET total_hours=100.3, total_salary=4008.0 "
                "WHERE branch_id=127 AND month='2026-05' AND source='aviv_report'")
            self.conn.commit()
            return {'ok': True}

        with _SharedConnContext(self.conn), \
             patch.object(m, 'USE_CHAIN_AUTH', False), \
             patch.object(m, 'run_for_branch', side_effect=tiny_change), \
             patch('utils.notify.notify') as mock_notify, \
             patch.object(m.time, 'sleep'):
            res = m.reconcile_previous_month(today=date(2026, 6, 10))
        self.assertEqual(res['changed'], 0)
        mock_notify.assert_not_called()


if __name__ == '__main__':
    unittest.main()

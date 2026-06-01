"""Demo shift seeder: every classification bucket is exercised, demo-guarded."""
import os
import sqlite3
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import scripts.seed_demo_shifts as seed  # noqa: E402


def _db():
    conn = sqlite3.connect(':memory:')
    conn.executescript('''
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT, branch_id INTEGER, name TEXT,
            role TEXT, hourly_rate REAL, active INTEGER DEFAULT 1,
            salary_type TEXT DEFAULT 'hourly', global_salary REAL);
        CREATE TABLE employee_hours (
            id INTEGER PRIMARY KEY AUTOINCREMENT, branch_id INTEGER, month TEXT,
            employee_name TEXT, total_hours REAL, total_salary REAL, source TEXT,
            UNIQUE(branch_id, month, employee_name));
        CREATE TABLE employee_shifts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, branch_id INTEGER, month TEXT,
            employee_name TEXT, shift_date TEXT, start_ts TEXT, end_ts TEXT,
            hours REAL, day_of_week TEXT, is_open INTEGER DEFAULT 0, source TEXT,
            regular_hours REAL, overtime_hours REAL, shabbat_hours REAL);
        CREATE TABLE shabbat_times (
            id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, candle_lighting_ts TEXT,
            havdalah_ts TEXT, is_holiday INTEGER DEFAULT 0, label TEXT,
            geonameid INTEGER DEFAULT 294801, updated_at TEXT,
            UNIQUE(date, geonameid));
    ''')
    return conn


class TestDemoShiftSeeder(unittest.TestCase):
    def setUp(self):
        self.conn = _db()
        self.conn.execute('BEGIN')
        seed._upsert_windows(self.conn)
        seed.seed_branch(self.conn, 9999)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_refuses_non_demo_branch(self):
        with self.assertRaises(ValueError):
            seed.seed_branch(self.conn, 126)

    def test_employees_created(self):
        n = self.conn.execute(
            "SELECT COUNT(*) FROM employees WHERE branch_id=9999 AND active=1").fetchone()[0]
        self.assertEqual(n, 4)

    def test_every_bucket_present(self):
        rows = self.conn.execute(
            "SELECT employee_name, regular_hours, overtime_hours, shabbat_hours "
            "FROM employee_shifts WHERE branch_id=9999").fetchall()
        self.assertTrue(any(r[2] > 0 for r in rows), 'no overtime shift')
        self.assertTrue(any(r[3] > 0 for r in rows), 'no shabbat shift')
        self.assertTrue(any(r[2] > 0 and r[3] > 0 for r in rows), 'no both-OT-and-shabbat shift')
        self.assertTrue(any(r[1] > 0 and r[2] == 0 and r[3] == 0 for r in rows), 'no plain regular shift')

    def test_both_shift_is_daniel_saturday(self):
        r = self.conn.execute(
            "SELECT regular_hours, overtime_hours, shabbat_hours FROM employee_shifts "
            "WHERE branch_id=9999 AND employee_name='דניאל פרץ' AND shift_date='2026-05-30'"
        ).fetchone()
        self.assertEqual(r[0], 8.0)            # regular 8
        self.assertEqual(r[1], 3.0)            # overtime 3
        self.assertAlmostEqual(r[2], 11.0, places=2)  # shabbat 11 (orthogonal)

    def test_global_not_classified(self):
        rows = self.conn.execute(
            "SELECT regular_hours, overtime_hours, shabbat_hours FROM employee_shifts "
            "WHERE branch_id=9999 AND employee_name='אורי בר'").fetchall()
        # Global: plain (regular=hours, no OT/Shabbat) even for the 12h + Saturday shifts.
        for reg, ot, shab in rows:
            self.assertEqual((ot, shab), (0.0, 0.0))

    def test_salary_is_flat_hours_times_rate(self):
        # מאיה: 12+5+6+6 = 29h × ₪42 = ₪1218, NOT inflated by OT/Shabbat.
        r = self.conn.execute(
            "SELECT total_hours, total_salary FROM employee_hours "
            "WHERE branch_id=9999 AND employee_name='מאיה כהן'").fetchone()
        self.assertEqual(r[0], 29.0)
        self.assertEqual(r[1], 29.0 * 42)
        # Global אורי: employee_hours.total_salary stays 0 (flat pay lives on the row).
        g = self.conn.execute(
            "SELECT total_salary FROM employee_hours WHERE branch_id=9999 AND employee_name='אורי בר'"
        ).fetchone()
        self.assertEqual(g[0], 0.0)

    def test_idempotent(self):
        self.conn.execute('BEGIN')
        seed.seed_branch(self.conn, 9999)
        self.conn.commit()
        n_emp = self.conn.execute("SELECT COUNT(*) FROM employees WHERE branch_id=9999").fetchone()[0]
        n_sh = self.conn.execute("SELECT COUNT(*) FROM employee_shifts WHERE branch_id=9999").fetchone()[0]
        self.assertEqual(n_emp, 4)   # no duplicate employees
        self.assertEqual(n_sh, 12)   # 3+4+3+2 shifts, no duplicates


if __name__ == '__main__':
    unittest.main()

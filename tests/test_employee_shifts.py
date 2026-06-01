"""Tests for the shift-breakdown endpoints: /api/employee-shifts (drill-down)
and /api/open-shifts (urgent open-shift flag). Covers reconciliation with the
monthly total, the open-shift flag, and the per-branch visibility floor."""
import json
import os
import sqlite3
import sys

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, DB_PATH  # noqa: E402


@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_shifts.db')

    import app as app_module
    original_db = app_module.DB_PATH
    app_module.DB_PATH = test_db
    if os.path.exists(test_db):
        os.remove(test_db)
    app_module.init_db()

    conn = sqlite3.connect(test_db, timeout=30)
    for col_sql in [
        "ALTER TABLE branches ADD COLUMN hours_this_month REAL DEFAULT 0",
        "ALTER TABLE branches ADD COLUMN avg_hourly_rate REAL DEFAULT 0",
        "ALTER TABLE branches ADD COLUMN hours_updated_at TEXT",
        "ALTER TABLE branches ADD COLUMN visible_from DATE",
    ]:
        try:
            conn.execute(col_sql)
        except sqlite3.OperationalError:
            pass
    # employee_shifts (migration 022) — init_db only builds schema.sql tables.
    conn.execute('''CREATE TABLE IF NOT EXISTS employee_shifts (
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
    )''')

    # 126 = no floor; 9001 = floored at 2026-06-01.
    conn.execute("INSERT OR REPLACE INTO branches (id, name, city, active) VALUES (126, 'איינשטיין', 'תל אביב', 1)")
    conn.execute("INSERT OR REPLACE INTO branches (id, name, city, active, visible_from) VALUES (9001, 'קדיש לוז', 'נהריה', 1, '2026-06-01')")

    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) VALUES (1, 'CEO', 'makoletdashboard@gmail.com', ?, 'admin', 1)",
        (generate_password_hash('test123'),))

    # Matched employee with hours + shifts in 2026-05.
    conn.execute("INSERT INTO employees (id, branch_id, name, role, hourly_rate, active) VALUES (1, 126, 'דביר פישר', 'ערב', 40, 1)")
    conn.execute(
        "INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, total_salary, source) "
        "VALUES (126, '2026-05', 'דביר פישר', 78.33, 3133.2, 'aviv_report')")

    shifts = [
        # (date, start, end, hours, dow, is_open)
        ('2026-05-03', '2026-05-03 13:59:00', '2026-05-03 23:03:04', 9.07, 'יום א', 0),
        ('2026-05-06', '2026-05-06 15:56:30', '2026-05-06 23:03:42', 7.12, 'יום ד', 0),
        ('2026-05-13', '2026-05-13 07:12:23', None, 0.0, 'יום ד', 1),
    ]
    for d, s, e, h, dow, op in shifts:
        conn.execute(
            "INSERT INTO employee_shifts (branch_id, month, employee_name, shift_date, "
            "start_ts, end_ts, hours, day_of_week, is_open, source) "
            "VALUES (126, '2026-05', 'דביר פישר', ?, ?, ?, ?, ?, ?, 'aviv_report')",
            (d, s, e, h, dow, op))

    # Floored branch 9001: an employee + a pre-floor (May) open shift that must
    # never surface for that branch.
    conn.execute("INSERT INTO employees (id, branch_id, name, role, hourly_rate, active) VALUES (2, 9001, 'יוסי כהן', 'בוקר', 38, 1)")
    conn.execute(
        "INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, total_salary, source) "
        "VALUES (9001, '2026-05', 'יוסי כהן', 50.0, 1900.0, 'aviv_report')")
    conn.execute(
        "INSERT INTO employee_shifts (branch_id, month, employee_name, shift_date, "
        "start_ts, end_ts, hours, day_of_week, is_open, source) "
        "VALUES (9001, '2026-05', 'יוסי כהן', '2026-05-20', '2026-05-20 08:00:00', NULL, 0, 'יום ג', 1, 'aviv_report')")

    conn.commit()
    conn.close()

    with app.test_client() as c:
        c.post('/login', data={'email': 'makoletdashboard@gmail.com', 'password': 'test123'},
               follow_redirects=True)
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(test_db):
        os.remove(test_db)


def _set_branch(client, bid):
    with client.session_transaction() as s:
        s['branch_id'] = bid


class TestEmployeeShifts:
    def test_shifts_returned(self, client):
        _set_branch(client, 126)
        d = json.loads(client.get('/api/employee-shifts?month=2026-05&employee_id=1').data)
        assert len(d['shifts']) == 3
        assert d['open_count'] == 1

    def test_total_reconciles_with_monthly(self, client):
        """Total returned is the authoritative employee_hours total (drives salary),
        NOT a sum of shift rows."""
        _set_branch(client, 126)
        d = json.loads(client.get('/api/employee-shifts?month=2026-05&employee_id=1').data)
        assert d['total_hours'] == 78.33

    def test_open_shift_has_no_end(self, client):
        _set_branch(client, 126)
        d = json.loads(client.get('/api/employee-shifts?month=2026-05&employee_id=1').data)
        opens = [s for s in d['shifts'] if s['is_open']]
        assert len(opens) == 1
        assert opens[0]['end_ts'] is None
        assert opens[0]['start_ts'] == '2026-05-13 07:12:23'

    def test_unknown_employee_404(self, client):
        _set_branch(client, 126)
        assert client.get('/api/employee-shifts?month=2026-05&employee_id=999').status_code == 404

    def test_floor_blocks_pre_floor_month(self, client):
        """Floored branch 9001 must not expose its May (pre-June) shifts."""
        _set_branch(client, 9001)
        d = json.loads(client.get('/api/employee-shifts?month=2026-05&employee_id=2').data)
        assert d['shifts'] == []
        assert d['total_hours'] == 0


class TestOpenShifts:
    def test_open_flag_lists_employee(self, client):
        _set_branch(client, 126)
        d = json.loads(client.get('/api/open-shifts?month=2026-05').data)
        assert d['count'] == 1
        assert d['open_shifts'][0]['employee_name'] == 'דביר פישר'
        assert d['open_shifts'][0]['shift_date'] == '2026-05-13'

    def test_no_open_shifts_when_none(self, client):
        _set_branch(client, 126)
        d = json.loads(client.get('/api/open-shifts?month=2026-04').data)
        assert d['count'] == 0

    def test_floor_blocks_open_shifts(self, client):
        _set_branch(client, 9001)
        d = json.loads(client.get('/api/open-shifts?month=2026-05').data)
        assert d['count'] == 0

    def test_requires_login(self, client):
        # Fresh unauthenticated client.
        with app.test_client() as anon:
            assert anon.get('/api/open-shifts?month=2026-05').status_code == 401

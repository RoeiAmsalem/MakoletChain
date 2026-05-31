"""Zero-hour names from the Aviv employer report are skipped entirely.

A 0-hour employee is noise: no employee_hours row, no employee_match_pending
entry. Names with hours > 0 are unaffected.
"""
import os
import sys
import sqlite3
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import agents.aviv_employees_report as emp

MONTH = '2026-05'


def _db(tmp_path):
    """Schema matching what update_employee_hours actually reads/writes."""
    p = tmp_path / 'zero.db'
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE branches (id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER, name TEXT, role TEXT,
            hourly_rate REAL, active INTEGER DEFAULT 1
        );
        CREATE TABLE employee_hours (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER, month TEXT, employee_name TEXT,
            total_hours REAL, total_salary REAL, source TEXT,
            UNIQUE(branch_id, month, employee_name)
        );
        CREATE TABLE employee_match_pending (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER, month TEXT, csv_name TEXT,
            aviv_employee_id INTEGER, suggested_employee_id INTEGER,
            confidence TEXT, hours REAL, salary REAL, source TEXT,
            is_new_employee INTEGER DEFAULT 0, resolved INTEGER DEFAULT 0
        );
    ''')
    conn.execute("INSERT INTO branches (id, name) VALUES (126, 'Einstein')")
    # A known employee so a >0-hour name matches (exact) instead of going pending.
    conn.execute("INSERT INTO employees (branch_id, name, role, hourly_rate, active) "
                 "VALUES (126, 'דנה כהן', 'ערב', 50, 1)")
    conn.commit()
    return conn


def _counts(conn):
    h = conn.execute("SELECT COUNT(*) FROM employee_hours WHERE branch_id=126 AND month=?", (MONTH,)).fetchone()[0]
    p = conn.execute("SELECT COUNT(*) FROM employee_match_pending WHERE branch_id=126 AND month=?", (MONTH,)).fetchone()[0]
    return h, p


def test_zero_hour_names_skipped(tmp_path):
    conn = _db(tmp_path)
    parsed = [
        {'raw_name': 'דנה כהן', 'aviv_employee_id': 1, 'total_hours': 0.0, 'open_shift_count': 0},   # matched but 0h -> skip
        {'raw_name': 'רפאל לא ידוע', 'aviv_employee_id': 2, 'total_hours': 0.0, 'open_shift_count': 0},  # unmatched + 0h -> skip
        {'raw_name': 'דנה כהן', 'aviv_employee_id': 1, 'total_hours': 80.0, 'open_shift_count': 0},  # matched, >0
    ]
    # Note: the two 'דנה כהן' rows share a name; the 0h one is skipped so the
    # 80h one is the only write — proves the skip happens before the insert.
    res = emp.update_employee_hours(126, MONTH, parsed, conn)

    h, p = _counts(conn)
    assert h == 1, f"expected 1 employee_hours row, got {h}"
    assert p == 0, f"expected 0 pending rows, got {p}"

    row = conn.execute("SELECT employee_name, total_hours, total_salary FROM employee_hours "
                       "WHERE branch_id=126 AND month=?", (MONTH,)).fetchone()
    assert row['total_hours'] == 80.0
    assert row['total_salary'] == 4000.0  # 80 × 50
    assert res['matched'] == 1
    assert res['unmatched'] == 0
    assert res['total_hours'] == 80.0


def test_unmatched_with_hours_still_pending(tmp_path):
    conn = _db(tmp_path)
    parsed = [
        {'raw_name': 'אורח חדש', 'aviv_employee_id': 9, 'total_hours': 0.0, 'open_shift_count': 0},   # 0h -> skip
        {'raw_name': 'אורח חדש', 'aviv_employee_id': 9, 'total_hours': 12.5, 'open_shift_count': 0},  # >0 -> pending
    ]
    emp.update_employee_hours(126, MONTH, parsed, conn)
    h, p = _counts(conn)
    assert h == 0
    assert p == 1
    pend = conn.execute("SELECT hours FROM employee_match_pending WHERE branch_id=126 AND month=?", (MONTH,)).fetchone()
    assert pend['hours'] == 12.5


def test_all_zero_report_writes_nothing(tmp_path):
    conn = _db(tmp_path)
    parsed = [
        {'raw_name': 'דנה כהן', 'aviv_employee_id': 1, 'total_hours': 0.0, 'open_shift_count': 0},
        {'raw_name': 'מישהו', 'aviv_employee_id': 2, 'total_hours': 0.0, 'open_shift_count': 0},
    ]
    res = emp.update_employee_hours(126, MONTH, parsed, conn)
    h, p = _counts(conn)
    assert (h, p) == (0, 0)
    assert res['matched'] == 0 and res['unmatched'] == 0
    assert res['total_hours'] == 0.0


def test_reinsert_idempotent_drops_zero(tmp_path):
    """Full-month delete+reinsert: an employee who had hours then drops to 0 in
    a re-run is removed (deleted, never reinserted)."""
    conn = _db(tmp_path)
    emp.update_employee_hours(126, MONTH, [
        {'raw_name': 'דנה כהן', 'aviv_employee_id': 1, 'total_hours': 80.0, 'open_shift_count': 0},
    ], conn)
    assert _counts(conn)[0] == 1
    # Re-run with the same name now at 0 hours.
    emp.update_employee_hours(126, MONTH, [
        {'raw_name': 'דנה כהן', 'aviv_employee_id': 1, 'total_hours': 0.0, 'open_shift_count': 0},
    ], conn)
    assert _counts(conn)[0] == 0

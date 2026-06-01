#!/usr/bin/env python3
"""Seed demo employee CARDS + classified shifts for demo branches 9999 & 9998.

Makes the shift-breakdown + OT/Shabbat classification VISIBLE in the demo. Each
demo store gets a few hourly employees (and one global) with hand-placed
May-2026 shifts that exercise every classification bucket:

  1. normal (≤8h)                         → regular only
  2. 10h shift                            → regular 8 + overtime 2  (נוספות)
  3. 12h shift                            → regular 8 + overtime 4
  4. Saturday / Friday-evening shift      → 🕯 שבת hours (inside a real window)
     + a Shavuot (chag) shift             → counts the same as Shabbat
  5. long AND on Shabbat                  → both נוספות + שבת
  + one GLOBAL-salary employee            → shown plain (not classified)

Display only — salary is unaffected (hours × rate for hourly; flat for global;
classification never feeds salary).

DEMO BRANCHES ONLY (9999, 9998) — hard guarded; refuses anything else. Idempotent:
re-running deletes and re-creates the demo employees' hours + shifts. Shifts use
source='demo' and employees live only on the demo branches; agents skip demo
branches (aviv_branch_id IS NULL under chain mode), so a real report run never
overwrites these.

Self-contained: upserts the May Haifa Shabbat/chag windows it needs into
shabbat_times, so Shabbat classification works on a fresh rebuild even without
hitting Hebcal. Buckets are computed by the real agents.shift_classify logic so
the demo matches production behaviour exactly.

Usage: python scripts/seed_demo_shifts.py [path/to/makolet_chain.db]
"""
import os
import sqlite3
import sys
from datetime import datetime

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from agents.shift_classify import classify_shifts, load_shabbat_windows  # noqa: E402

DEFAULT_DB = os.path.join(REPO_ROOT, 'db', 'makolet_chain.db')
DEMO_BRANCH_IDS = (9999, 9998)   # the ONLY branches this script may write
DEMO_MONTH = '2026-05'

# Haifa (geonameid 294801) Shabbat/chag windows for May 2026 — verified Hebcal
# values. Upserted so the demo classifies even on a fresh DB with no Hebcal run.
# (date, candle_lighting_ts, havdalah_ts, is_holiday, label)
SHABBAT_WINDOWS_MAY = [
    ('2026-05-01', '2026-05-01 18:51:00', '2026-05-02 20:02:00', 0, 'שבת'),
    ('2026-05-08', '2026-05-08 18:56:00', '2026-05-09 20:08:00', 0, 'שבת'),
    ('2026-05-15', '2026-05-15 19:01:00', '2026-05-16 20:13:00', 0, 'שבת'),
    ('2026-05-21', '2026-05-21 19:06:00', '2026-05-23 20:19:00', 1, 'חג'),  # Shavuot
    ('2026-05-29', '2026-05-29 19:11:00', '2026-05-30 20:24:00', 0, 'שבת'),
]

# (name, salary_type, hourly_rate, global_salary, role)
DEMO_EMPLOYEES = [
    ('יואב לוי',  'hourly', 38.0, None,   'בוקר'),
    ('מאיה כהן',  'hourly', 42.0, None,   'ערב'),
    ('דניאל פרץ', 'hourly', 40.0, None,   'ערב'),
    # Global: hourly_rate is NOT NULL in the schema and unused for flat pay → 0.0.
    ('אורי בר',   'global', 0.0,  9000.0, 'מנהל'),
]

# name -> [(shift_date, start_hm, end_hm, day_of_week), ...]
DEMO_SHIFTS = {
    'יואב לוי': [
        ('2026-05-04', '09:00', '17:00', 'יום ב'),   # 8h  → regular only
        ('2026-05-06', '10:00', '16:00', 'יום ד'),   # 6h  → regular only
        ('2026-05-07', '08:00', '18:00', 'יום ה'),   # 10h → reg 8 + OT 2
    ],
    'מאיה כהן': [
        ('2026-05-11', '08:00', '20:00', 'יום ב'),   # 12h → reg 8 + OT 4
        ('2026-05-13', '11:00', '16:00', 'יום ד'),   # 5h  → regular only
        ('2026-05-16', '10:00', '16:00', 'יום ש'),   # 6h Saturday → Shabbat
        ('2026-05-23', '12:00', '18:00', 'יום ש'),   # 6h Shavuot (chag) → Shabbat
    ],
    'דניאל פרץ': [
        ('2026-05-26', '09:00', '15:00', 'יום ג'),   # 6h  → regular only
        ('2026-05-29', '16:00', '22:00', 'יום ו'),   # Fri eve past candle → partial Shabbat
        ('2026-05-30', '09:00', '20:00', 'יום ש'),   # 11h Saturday → reg 8 + OT 3 + Shabbat (BOTH)
    ],
    'אורי בר': [  # global — shown plain, never classified
        ('2026-05-05', '09:00', '21:00', 'יום ג'),   # 12h
        ('2026-05-09', '10:00', '18:00', 'יום ש'),   # 8h Saturday (still plain — global)
    ],
}


def _hours(start_ts, end_ts):
    a = datetime.strptime(start_ts, '%Y-%m-%d %H:%M:%S')
    b = datetime.strptime(end_ts, '%Y-%m-%d %H:%M:%S')
    return round((b - a).total_seconds() / 3600.0, 4)


def _upsert_windows(conn):
    for date, candle, havdalah, is_holiday, label in SHABBAT_WINDOWS_MAY:
        conn.execute('''
            INSERT INTO shabbat_times
            (date, candle_lighting_ts, havdalah_ts, is_holiday, label, geonameid, updated_at)
            VALUES (?, ?, ?, ?, ?, 294801, datetime('now'))
            ON CONFLICT(date, geonameid) DO UPDATE SET
                candle_lighting_ts=excluded.candle_lighting_ts,
                havdalah_ts=excluded.havdalah_ts,
                is_holiday=excluded.is_holiday,
                label=excluded.label,
                updated_at=datetime('now')
        ''', (date, candle, havdalah, is_holiday, label))


def _upsert_employee(conn, branch_id, name, salary_type, hourly_rate, global_salary, role):
    row = conn.execute(
        'SELECT id FROM employees WHERE branch_id=? AND name=?', (branch_id, name)).fetchone()
    if row:
        emp_id = row[0]
        conn.execute(
            'UPDATE employees SET role=?, hourly_rate=?, salary_type=?, global_salary=?, active=1 '
            'WHERE id=?', (role, hourly_rate, salary_type, global_salary, emp_id))
        return emp_id
    cur = conn.execute(
        'INSERT INTO employees (branch_id, name, role, hourly_rate, active, salary_type, global_salary) '
        'VALUES (?, ?, ?, ?, 1, ?, ?)',
        (branch_id, name, role, hourly_rate, salary_type, global_salary))
    return cur.lastrowid


def seed_branch(conn, branch_id):
    if branch_id not in DEMO_BRANCH_IDS:
        raise ValueError(f'refusing to seed non-demo branch {branch_id}')

    windows = load_shabbat_windows(conn)

    # Wipe this branch's prior demo employees' hours + shifts so re-runs are clean.
    names = [e[0] for e in DEMO_EMPLOYEES]
    ph = ','.join('?' for _ in names)
    conn.execute(f'DELETE FROM employee_hours WHERE branch_id=? AND month=? AND employee_name IN ({ph})',
                 (branch_id, DEMO_MONTH, *names))
    conn.execute("DELETE FROM employee_shifts WHERE branch_id=? AND month=? AND source='demo'",
                 (branch_id, DEMO_MONTH))

    n_emp = n_shifts = 0
    for name, salary_type, hourly_rate, global_salary, role in DEMO_EMPLOYEES:
        _upsert_employee(conn, branch_id, name, salary_type, hourly_rate, global_salary, role)
        n_emp += 1
        is_global = (salary_type == 'global')

        shifts = []
        for shift_date, s_hm, e_hm, dow in DEMO_SHIFTS[name]:
            start_ts = f'{shift_date} {s_hm}:00'
            end_ts = f'{shift_date} {e_hm}:00'
            shifts.append({
                'shift_date': shift_date, 'start_ts': start_ts, 'end_ts': end_ts,
                'hours': _hours(start_ts, end_ts), 'day_of_week': dow, 'is_open': False,
            })
        classify_shifts(shifts, windows, is_global=is_global)

        for s in shifts:
            conn.execute('''
                INSERT INTO employee_shifts
                (branch_id, month, employee_name, shift_date, start_ts, end_ts, hours,
                 day_of_week, is_open, source, regular_hours, overtime_hours, shabbat_hours)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 'demo', ?, ?, ?)
            ''', (branch_id, DEMO_MONTH, name, s['shift_date'], s['start_ts'], s['end_ts'],
                  s['hours'], s['day_of_week'],
                  s['regular_hours'], s['overtime_hours'], s['shabbat_hours']))
            n_shifts += 1

        total_hours = round(sum(s['hours'] for s in shifts), 2)
        # Salary: hours×rate for hourly; flat (carried on the employee row) for
        # global — employee_hours.total_salary stays 0 for globals, exactly as a
        # real report row would, so _calculate_salary_cost is unaffected.
        total_salary = 0.0 if is_global else round(total_hours * (hourly_rate or 0), 2)
        conn.execute('''
            INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, total_salary, source)
            VALUES (?, ?, ?, ?, ?, 'aviv_report')
            ON CONFLICT(branch_id, month, employee_name) DO UPDATE SET
                total_hours=excluded.total_hours, total_salary=excluded.total_salary,
                source='aviv_report'
        ''', (branch_id, DEMO_MONTH, name, total_hours, total_salary))

    print(f'[demo-shifts] branch {branch_id}: {n_emp} employees, {n_shifts} shifts for {DEMO_MONTH}')


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB
    if not os.path.exists(db_path):
        sys.exit(f'[demo-shifts] ERROR: database not found at {db_path}')
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute('BEGIN')
        _upsert_windows(conn)
        for bid in DEMO_BRANCH_IDS:
            seed_branch(conn, bid)
        conn.commit()
        print('[demo-shifts] DONE — committed.')
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()

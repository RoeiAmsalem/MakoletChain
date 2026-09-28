"""Alias matching must survive the store suffix on the raw Aviv name.

Aliases are saved from pending rows, which store strip_store_suffix(raw). The
employer report passes the RAW name ('... רמת גן') to match_employee_name, so an
exact-only alias lookup never hit and the full overwrite dropped the employee's
hours (9020 Sep: 'קוראפ עובד זר' 195.75h × ₪55 = ₪11,667.02).
"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import agents._employee_matching as m
import agents.aviv_employees_report as emp

BR = 9020
BRANCH_NAME = 'רמת גן'
MONTH = '2026-09'


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = str(tmp_path / 'm.db')
    conn = sqlite3.connect(path)
    conn.executescript('''
        CREATE TABLE branches (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY, branch_id INTEGER, name TEXT,
            hourly_rate REAL, active INTEGER DEFAULT 1, salary_type TEXT
        );
        CREATE TABLE employee_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT, employee_id INTEGER NOT NULL,
            alias_name TEXT NOT NULL, branch_id INTEGER NOT NULL,
            created_at TEXT DEFAULT (datetime('now')), UNIQUE(branch_id, alias_name)
        );
        CREATE TABLE employee_hours (
            id INTEGER PRIMARY KEY AUTOINCREMENT, branch_id INTEGER, month TEXT,
            employee_name TEXT, total_hours REAL, total_salary REAL, source TEXT,
            UNIQUE(branch_id, month, employee_name)
        );
        CREATE TABLE employee_match_pending (
            id INTEGER PRIMARY KEY AUTOINCREMENT, branch_id INTEGER, month TEXT,
            csv_name TEXT, aviv_employee_id INTEGER, suggested_employee_id INTEGER,
            confidence TEXT, hours REAL, salary REAL, source TEXT,
            is_new_employee INTEGER, shifts_json TEXT, resolved INTEGER DEFAULT 0
        );
    ''')
    conn.execute('INSERT INTO branches VALUES (?, ?)', (BR, BRANCH_NAME))
    conn.execute("INSERT INTO employees VALUES (26, ?, 'פלוני קוראפ', 55.0, 1, 'hourly')", (BR,))
    conn.commit()
    conn.close()
    monkeypatch.setattr(m, 'DB_PATH', path)
    return path


def _alias(path, alias_id, name, emp_id=26):
    conn = sqlite3.connect(path)
    conn.execute('INSERT INTO employee_aliases (id, employee_id, alias_name, branch_id) '
                 'VALUES (?, ?, ?, ?)', (alias_id, emp_id, name, BR))
    conn.commit()
    conn.close()


def _employees():
    return [{'id': 26, 'name': 'פלוני קוראפ', 'hourly_rate': 55.0}]


def _run(path, rows):
    conn = sqlite3.connect(path)
    res = emp.update_employee_hours(BR, MONTH, rows, conn)
    conn.close()
    return res


def _row(name, hours=195.75):
    return {'raw_name': name, 'total_hours': hours, 'shifts': []}


def _pending(path):
    conn = sqlite3.connect(path)
    rows = conn.execute('SELECT csv_name, resolved FROM employee_match_pending '
                        'ORDER BY id').fetchall()
    conn.close()
    return rows


def test_suffixed_raw_name_matches_alias(db):
    _alias(db, 1, 'עובד זר')
    r = m.match_employee_name('עובד זר רמת גן', _employees(), BRANCH_NAME, BR)
    assert r[0] == 26 and r[1] == 'exact'


def test_stripped_name_alone_still_matches(db):
    _alias(db, 1, 'עובד זר')
    r = m.match_employee_name('עובד זר', _employees(), BRANCH_NAME, BR)
    assert r[0] == 26 and r[1] == 'exact'


def test_foreign_suffix_not_stripped(db):
    _alias(db, 1, 'עובד זר')
    r = m.match_employee_name('עובד זר חיפה', _employees(), BRANCH_NAME, BR)
    assert r[0] is None and r[1] == 'none'


def test_prod_case_9020(db):
    # 81 = stripped Aviv name saved from the pending row, 82 = manager's final name
    _alias(db, 81, 'קוראפ עובד זר')
    _alias(db, 82, 'פלוני קוראפ')
    emp_id, conf, _name, rate = m.match_employee_name(
        'קוראפ עובד זר רמת גן', _employees(), BRANCH_NAME, BR)
    assert (emp_id, conf, rate) == (26, 'exact', 55.0)

    res = _run(db, [_row('קוראפ עובד זר רמת גן')])
    assert res['matched'] == 1 and res['unmatched'] == 0
    conn = sqlite3.connect(db)
    hrs = conn.execute('SELECT employee_name, total_hours, total_salary FROM employee_hours').fetchall()
    conn.close()
    assert hrs == [('פלוני קוראפ', 195.75, 10766.25)]
    assert _pending(db) == []


def test_no_alias_unmatched_pending_once(db):
    _run(db, [_row('זר חדש רמת גן', 10)])
    _run(db, [_row('זר חדש רמת גן', 12)])
    assert _pending(db) == [('זר חדש', 0)]


def test_resolved_name_not_reflagged(db):
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO employee_match_pending (branch_id, month, csv_name, hours, "
                 "confidence, resolved) VALUES (?, ?, 'זר נדחה', 5, 'none', 1)", (BR, MONTH))
    conn.commit()
    conn.close()
    _run(db, [_row('זר נדחה רמת גן', 5)])
    _run(db, [_row('זר נדחה רמת גן', 6)])
    assert _pending(db) == [('זר נדחה', 1)]


def test_alias_to_inactive_employee_not_reflagged(db):
    # Alias exists (manager decided) but the matcher can't use it — no new pending.
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO employees VALUES (27, ?, 'לשעבר', 40.0, 0, 'hourly')", (BR,))
    conn.commit()
    conn.close()
    _alias(db, 5, 'עובד עבר', emp_id=27)
    _run(db, [_row('עובד עבר רמת גן', 3)])
    assert _pending(db) == []

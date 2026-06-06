"""Tests for scripts/backfill_june_departments_112.py — gap-fill semantics.

All offline. Aviv HTTP (login/refresh/112 pull) is monkeypatched; the DB is a
real temp SQLite. The three invariants under test:
  1. INSERTS only for (branch, date) with NO existing dept rows.
  2. SKIPS — and never even pulls 112 for — combos that already have data.
  3. NEVER overwrites an existing row (its values are untouched after --apply).
"""
import importlib
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import agents.aviv_z_report as zr  # noqa: E402

bf = importlib.import_module('scripts.backfill_june_departments_112')


SCHEMA = '''
    CREATE TABLE branches (
        id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 1,
        aviv_branch_id INTEGER
    );
    CREATE TABLE z_department_sales (
        branch_id INTEGER NOT NULL, date TEXT NOT NULL,
        dept_code INTEGER NOT NULL, dept_name TEXT NOT NULL,
        amount REAL NOT NULL, qty REAL,
        cost_ex_vat REAL, profit REAL, profit_pct REAL, contrib_pct REAL,
        fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (branch_id, date, dept_code)
    );
'''

# Two depts the fake 112 pull "returns" for any (branch, day).
FAKE_DEPTS = [
    {'dept_code': 5, 'dept_name': 'מקרר חלב', 'qty': 10.0,
     'cost_ex_vat': 50.0, 'sale_incl_vat': 100.0, 'profit': 50.0,
     'profit_pct': 50.0, 'contrib_pct': 20.0},
    {'dept_code': 2, 'dept_name': 'ירקות', 'qty': 5.0,
     'cost_ex_vat': 30.0, 'sale_incl_vat': 60.0, 'profit': 30.0,
     'profit_pct': 50.0, 'contrib_pct': 12.0},
]


@pytest.fixture
def temp_db(tmp_path, monkeypatch):
    db = str(tmp_path / 'makolet.db')
    conn = sqlite3.connect(db)
    conn.executescript(SCHEMA)
    # 127 (aviv 8) and 9018 (aviv 3684) are real targets; 9011 (aviv 107) must
    # be skipped entirely (report 112 404s there).
    conn.executemany(
        'INSERT INTO branches (id, name, active, aviv_branch_id) VALUES (?,?,1,?)',
        [(127, 'תיכון', 8), (9018, 'דפנה', 3684), (9011, 'ויצמן', 107)])
    # Branch 127, 2026-06-01 ALREADY has dept data — must be left untouched.
    conn.execute(
        "INSERT INTO z_department_sales (branch_id, date, dept_code, dept_name, "
        "amount, qty) VALUES (127, '2026-06-01', 99, 'PRE-EXISTING', 999.99, 1)")
    conn.commit()
    conn.close()
    monkeypatch.setattr(bf, 'DB_PATH', db)
    monkeypatch.setattr(bf, '_june_days', lambda: ['2026-06-01', '2026-06-02'])
    monkeypatch.setattr(zr, '_login_chain_account', lambda: 'tok')
    monkeypatch.setattr(zr, '_refresh', lambda t: t)
    return db


def _install_fake_fetch(monkeypatch):
    """Record every (aviv_id, day) the backfill actually pulls 112 for."""
    calls = []

    def fake_fetch(aviv_id, day, token):
        calls.append((aviv_id, day))
        return list(FAKE_DEPTS)

    monkeypatch.setattr(zr, 'fetch_112_departments', fake_fetch)
    return calls


def _rows(db):
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        'SELECT branch_id, date, dept_code, amount FROM z_department_sales '
        'ORDER BY branch_id, date, dept_code').fetchall()
    conn.close()
    return rows


def test_dry_run_writes_nothing_and_skips_existing(temp_db, monkeypatch):
    calls = _install_fake_fetch(monkeypatch)
    monkeypatch.setattr(sys, 'argv', ['backfill'])  # no --apply
    bf.main()

    rows = _rows(temp_db)
    # Only the pre-existing row remains — dry-run wrote nothing.
    assert len(rows) == 1
    assert (rows[0]['branch_id'], rows[0]['date'], rows[0]['dept_code']) == \
        (127, '2026-06-01', 99)
    assert rows[0]['amount'] == 999.99

    # 112 was NOT pulled for the combo that already had data (skip-before-pull),
    # and never for the skipped branch 9011 (aviv 107).
    assert (8, '2026-06-01') not in calls
    assert all(aviv != 107 for aviv, _ in calls)
    # It WAS pulled for the genuine gaps.
    assert (8, '2026-06-02') in calls
    assert (3684, '2026-06-01') in calls
    assert (3684, '2026-06-02') in calls


def test_apply_inserts_gaps_but_never_overwrites(temp_db, monkeypatch):
    calls = _install_fake_fetch(monkeypatch)
    monkeypatch.setattr(sys, 'argv', ['backfill', '--apply'])
    bf.main()

    rows = _rows(temp_db)
    by_key = {(r['branch_id'], r['date'], r['dept_code']): r['amount']
              for r in rows}

    # Pre-existing row is UNTOUCHED — never overwritten.
    assert by_key[(127, '2026-06-01', 99)] == 999.99
    # No 112-sourced rows were inserted into the already-populated combo
    # (it kept ONLY its original dept 99).
    branch127_0601 = [r for r in rows
                      if r['branch_id'] == 127 and r['date'] == '2026-06-01']
    assert len(branch127_0601) == 1

    # Genuine gaps were filled with the 112 depts (amount == sale_incl_vat).
    assert by_key[(127, '2026-06-02', 5)] == 100.0
    assert by_key[(127, '2026-06-02', 2)] == 60.0
    assert by_key[(9018, '2026-06-01', 5)] == 100.0
    assert by_key[(9018, '2026-06-02', 5)] == 100.0

    # Branch 9011 was skipped entirely.
    assert not [r for r in rows if r['branch_id'] == 9011]
    assert all(aviv != 107 for aviv, _ in calls)


def test_apply_is_idempotent_second_run_is_all_skips(temp_db, monkeypatch):
    """After one --apply, a second --apply finds every combo populated and
    pulls nothing (proves the gap-fill guard converges)."""
    _install_fake_fetch(monkeypatch)
    monkeypatch.setattr(sys, 'argv', ['backfill', '--apply'])
    bf.main()
    first = _rows(temp_db)

    calls2 = _install_fake_fetch(monkeypatch)
    bf.main()
    second = _rows(temp_db)

    assert len(first) == len(second)        # nothing added
    assert calls2 == []                     # nothing pulled

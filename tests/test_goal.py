"""Tests for /goal — per-supplier monthly purchase-budget tracker.

Covers the run-rate math, the divide-by-zero guard (day floored at 1), the
mtd=0 case, and that summing per-supplier mtd_spend reconciles to the trusted
/goods pre-VAT MTD total for the same branch.
"""
import os
import sys
import sqlite3
from datetime import datetime

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module
from app import app, _goal_data, _goods_doc_context, IL_TZ

BRANCH = 9015
MONTH = '2026-05'
PREV = '2026-04'


@pytest.fixture
def db():
    test_db = os.path.join(os.path.dirname(__file__), 'test_goal.db')
    original = app_module.DB_PATH
    if os.path.exists(test_db):
        os.remove(test_db)
    app_module.DB_PATH = test_db
    app_module.init_db()

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
    import migrate as _migrate
    _migrate.DB_PATH = test_db
    mconn = _migrate.get_connection()
    _migrate.ensure_migrations_table(mconn)
    _migrate.cmd_apply(mconn)
    mconn.close()

    conn = sqlite3.connect(test_db, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("INSERT OR REPLACE INTO branches (id, name, city, active) VALUES (?, 'ההגנה', 'חיפה', 1)", (BRANCH,))
    # total_without_vat set explicitly → clean pre-VAT amounts, no /1.17 rounding.
    rows = [
        # (supplier, ref, amount, before_vat, date, doc_type)
        ('סופר א', 'A1', 117.0, 100.0, MONTH + '-01', 3),
        ('סופר א', 'A2', 234.0, 200.0, MONTH + '-02', 3),   # A: mtd 300
        ('סופר ב', 'B1', 585.0, 500.0, MONTH + '-03', 3),   # B: mtd 500
        ('סופר ג', 'C1', 351.0, 300.0, PREV + '-15', 3),    # C: prev month only → mtd 0
    ]
    for sup, ref, amt, bv, dt, dtype in rows:
        conn.execute(
            "INSERT INTO goods_documents (branch_id, doc_date, supplier, ref_number, amount, total_without_vat, doc_type) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (BRANCH, dt, sup, ref, amt, bv, dtype))
    # Budget-only supplier (no goods at all) + a budget on A.
    conn.execute("INSERT INTO supplier_budgets (branch_id, supplier_name, monthly_budget) VALUES (?, 'סופר א', 1000)", (BRANCH,))
    conn.execute("INSERT INTO supplier_budgets (branch_id, supplier_name, monthly_budget) VALUES (?, 'סופר ד', 800)", (BRANCH,))
    conn.commit()
    yield conn
    conn.close()
    app_module.DB_PATH = original
    if os.path.exists(test_db):
        os.remove(test_db)


def _freeze(monkeypatch, day):
    fixed = datetime(2026, 5, day, 12, 0, tzinfo=IL_TZ)
    monkeypatch.setattr(app_module, '_now_il', lambda: fixed)


def _by_name(data):
    return {s['supplier_name']: s for s in data['suppliers']}


def test_run_rate_math(db, monkeypatch):
    """projected = mtd_spend * days_in_month / days_elapsed (May = 31 days)."""
    _freeze(monkeypatch, 10)  # day 10 of 31
    data = _goal_data(BRANCH, db)
    assert data['days_elapsed'] == 10
    assert data['days_in_month'] == 31
    s = _by_name(data)
    # A: mtd 300 → 300 * 31 / 10 = 930
    assert s['סופר א']['mtd_spend'] == 300.0
    assert s['סופר א']['projected'] == 930.0
    # A has a 1000 budget → remaining = 1000 - 930 = 70 (positive)
    assert s['סופר א']['remaining'] == 70.0
    # B: mtd 500 → 1550, no budget → remaining None
    assert s['סופר ב']['projected'] == 1550.0
    assert s['סופר ב']['remaining'] is None


def test_divide_by_zero_guard(db, monkeypatch):
    """On day 1 days_elapsed is floored at 1 — no ZeroDivisionError; projected
    = mtd * days_in_month."""
    _freeze(monkeypatch, 1)
    data = _goal_data(BRANCH, db)
    assert data['days_elapsed'] == 1
    s = _by_name(data)
    assert s['סופר א']['projected'] == 300.0 * 31  # 9300


def test_mtd_zero_projects_zero(db, monkeypatch):
    """A supplier with no goods this month projects 0 (prev-month + budget-only
    suppliers still appear in the roster)."""
    _freeze(monkeypatch, 10)
    s = _by_name(_goal_data(BRANCH, db))
    # C: prev month only
    assert s['סופר ג']['mtd_spend'] == 0.0
    assert s['סופר ג']['projected'] == 0.0
    # D: budget only, never ordered → still listed, remaining = full budget
    assert s['סופר ד']['mtd_spend'] == 0.0
    assert s['סופר ד']['projected'] == 0.0
    assert s['סופר ד']['remaining'] == 800.0


def test_reconciles_to_goods_total(db, monkeypatch):
    """Σ per-supplier mtd_spend == the /goods pre-VAT MTD total for the branch."""
    _freeze(monkeypatch, 10)
    data = _goal_data(BRANCH, db)
    sum_mtd = round(sum(s['mtd_spend'] for s in data['suppliers']), 2)
    goods_total = _goods_doc_context(BRANCH, MONTH, db)['total_before_vat']
    assert sum_mtd == goods_total == 800.0  # 100 + 200 + 500

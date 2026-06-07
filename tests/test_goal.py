"""Tests for /goal — per-supplier monthly purchase-budget tracker.

Actual-spending model: יתרה = תקציב − actual MTD spend (mtd_spend), NOT a
projected run-rate. Covers the remaining math, day-independence (no run-rate),
the mtd=0 case, the exact-budget (remaining 0, neutral-color) case, that totals
are budgeted-only, and that summing per-supplier mtd_spend reconciles to the
trusted /goods pre-VAT MTD total for the same branch.
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
    # supplier_roster (migration 029): prior-month supplier ג + a roster-only
    # supplier ה (never ordered, no budget). Both must appear on the budget page
    # via the roster union, even with no current-month goods.
    conn.execute("INSERT INTO supplier_roster (branch_id, supplier_name) VALUES (?, 'סופר ג')", (BRANCH,))
    conn.execute("INSERT INTO supplier_roster (branch_id, supplier_name) VALUES (?, 'סופר ה')", (BRANCH,))
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


def test_remaining_is_budget_minus_spent(db, monkeypatch):
    """יתרה = תקציב − actual MTD spend (NOT pace). קצב is present per supplier
    but must not affect remaining."""
    _freeze(monkeypatch, 10)
    data = _goal_data(BRANCH, db)
    assert data['days_elapsed'] == 10
    assert data['days_in_month'] == 31
    s = _by_name(data)
    # A: mtd 300, budget 1000 → remaining = 1000 - 300 = 700 (positive/green).
    # projected (קצב) = 930 must NOT be what remaining is computed from.
    assert s['סופר א']['mtd_spend'] == 300.0
    assert s['סופר א']['projected'] == 930.0
    assert s['סופר א']['remaining'] == 700.0
    # B: mtd 500, no budget → remaining None
    assert s['סופר ב']['mtd_spend'] == 500.0
    assert s['סופר ב']['remaining'] is None


def test_projected_is_run_rate(db, monkeypatch):
    """קצב (projected) = mtd_spend × days_in_month / days_elapsed — informational
    pace, surfaced per supplier, NOT summed into totals."""
    _freeze(monkeypatch, 10)  # day 10 of 31
    data = _goal_data(BRANCH, db)
    s = _by_name(data)
    assert s['סופר א']['projected'] == 930.0    # 300 * 31 / 10
    assert s['סופר ב']['projected'] == 1550.0   # 500 * 31 / 10
    assert s['סופר ג']['projected'] == 0.0      # mtd 0
    assert 'projected' not in data['totals']    # informational — never a total


def test_remaining_is_day_independent_but_projected_is_not(db, monkeypatch):
    """Actual-spending יתרה has no run-rate, so spend & remaining do NOT change
    with the day. קצב (projected), being a run-rate, DOES change with the day."""
    _freeze(monkeypatch, 1)
    d1 = _by_name(_goal_data(BRANCH, db))
    _freeze(monkeypatch, 28)
    d28 = _by_name(_goal_data(BRANCH, db))
    # יתרה / spend: day-independent
    assert d1['סופר א']['mtd_spend'] == d28['סופר א']['mtd_spend'] == 300.0
    assert d1['סופר א']['remaining'] == d28['סופר א']['remaining'] == 700.0
    # קצב: day-dependent run-rate (day 1 → ×31, day 28 lower multiplier)
    assert d1['סופר א']['projected'] == 300.0 * 31         # 9300
    assert d28['סופר א']['projected'] > d28['סופר א']['mtd_spend']
    assert d1['סופר א']['projected'] != d28['סופר א']['projected']


def test_mtd_zero_remaining_is_full_budget(db, monkeypatch):
    """A supplier with no goods this month has mtd 0 (prev-month + budget-only
    suppliers still appear); a budgeted-but-unordered one shows the full budget
    as יתרה."""
    _freeze(monkeypatch, 10)
    s = _by_name(_goal_data(BRANCH, db))
    # C: prev month only → mtd 0, no budget → remaining None
    assert s['סופר ג']['mtd_spend'] == 0.0
    assert s['סופר ג']['remaining'] is None
    # D: budget 800, never ordered → mtd 0, remaining = full budget
    assert s['סופר ד']['mtd_spend'] == 0.0
    assert s['סופר ד']['remaining'] == 800.0


def test_exact_budget_gives_zero_remaining(db, monkeypatch):
    """Budget exactly equal to spend → remaining 0 — the neutral / no-color
    case the per-row + strip color rule must render without green or red."""
    _freeze(monkeypatch, 10)
    db.execute("INSERT OR REPLACE INTO supplier_budgets "
               "(branch_id, supplier_name, monthly_budget) VALUES (?, 'סופר ב', 500)",
               (BRANCH,))
    db.commit()
    s = _by_name(_goal_data(BRANCH, db))
    assert s['סופר ב']['mtd_spend'] == 500.0
    assert s['סופר ב']['remaining'] == 0.0


def test_reconciles_to_goods_total(db, monkeypatch):
    """Σ per-supplier mtd_spend == the /goods pre-VAT MTD total for the branch."""
    _freeze(monkeypatch, 10)
    data = _goal_data(BRANCH, db)
    sum_mtd = round(sum(s['mtd_spend'] for s in data['suppliers']), 2)
    goods_total = _goods_doc_context(BRANCH, MONTH, db)['total_before_vat']
    assert sum_mtd == goods_total == 800.0  # 100 + 200 + 500


def test_totals_summed_over_budgeted_only(db, monkeypatch):
    """All three totals share one basis: budgeted suppliers only. Budgets are
    set on א (1000, spent 300) and ד (800, spent 0). ב is unbudgeted (spent 500)
    and must NOT inflate Σ הוצאה / Σ יתרה."""
    _freeze(monkeypatch, 10)  # day 10 of 31
    data = _goal_data(BRANCH, db)
    t = data['totals']
    assert t['budget'] == 1800.0                 # 1000 + 800
    assert t['spent'] == 300.0                   # 300 + 0 — excludes ב's 500
    assert t['remaining'] == 1500.0              # 1800 - 300
    # קצב הזמנות is store-wide (all suppliers), kept SEPARATE from the
    # budgeted-only trio — but it is a total, surfaced under order_pace.
    assert 'order_pace' in t
    # ב is unbudgeted: its per-row הוצאה still shows but is excluded from totals.
    s = _by_name(data)
    assert s['סופר ב']['budget'] is None and s['סופר ב']['mtd_spend'] == 500.0


def test_page_list_unions_roster(db, monkeypatch):
    """Budget page = roster ∪ current-month spenders ∪ budgeted. A roster
    supplier with NO current-month orders and NO budget (ה) still appears, with
    הוצאה 0 / קצב 0 / יתרה None ("הוסף תקציב")."""
    _freeze(monkeypatch, 10)
    s = _by_name(_goal_data(BRANCH, db))
    # roster-only ה
    assert 'סופר ה' in s
    assert s['סופר ה']['mtd_spend'] == 0.0
    assert s['סופר ה']['projected'] == 0.0
    assert s['סופר ה']['remaining'] is None
    # current-month spender (א), budgeted-only (ד), roster prior-month (ג) all present
    for name in ('סופר א', 'סופר ב', 'סופר ג', 'סופר ד', 'סופר ה'):
        assert name in s


def test_page_list_falls_back_without_roster(db, monkeypatch):
    """If the roster table is empty for the branch, the page degrades to
    current-month ∪ budgeted (no breakage, no roster-only rows)."""
    _freeze(monkeypatch, 10)
    db.execute("DELETE FROM supplier_roster WHERE branch_id = ?", (BRANCH,))
    db.commit()
    s = _by_name(_goal_data(BRANCH, db))
    # current spenders + budgeted survive; roster-only ה and prior-month ג vanish
    for name in ('סופר א', 'סופר ב', 'סופר ד'):
        assert name in s
    assert 'סופר ה' not in s
    assert 'סופר ג' not in s


def test_order_pace_is_all_suppliers(db, monkeypatch):
    """קצב הזמנות (order_pace) = Σ projected over ALL suppliers (budgeted AND
    unbudgeted) — the store's whole ordering pace, and it exceeds the
    budgeted-only spend when big suppliers are unbudgeted (ב here)."""
    _freeze(monkeypatch, 10)  # day 10 of 31
    t = _goal_data(BRANCH, db)['totals']
    # א proj 930 + ב proj 1550 + ג/ד/ה 0 = 2480
    assert t['order_pace'] == 2480.0
    assert t['order_pace'] > t['spent']   # 2480 > 300 — unbudgeted ב lifts the pace

"""Tests for agents/supplier_roster — the /goods budget-page full roster build.

Covers the prior-2-calendar-month window, the franchise (זיכיונות) exclusion,
that the visible_from display floor is IGNORED (new chain stores still get their
prior-month suppliers), and replace-on-refresh semantics.
"""
import os
import sys
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module
from agents.supplier_roster import build_for_branch, prior_two_months

IL_TZ = ZoneInfo('Asia/Jerusalem')
NOW = datetime(2026, 6, 7, 12, 0, tzinfo=IL_TZ)   # June → prior 2 months = Apr + May


@pytest.fixture
def db():
    test_db = os.path.join(os.path.dirname(__file__), 'test_roster.db')
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
    yield conn
    conn.close()
    app_module.DB_PATH = original
    if os.path.exists(test_db):
        os.remove(test_db)


def _branch(conn, bid, franchise=None, visible_from=None):
    conn.execute("INSERT OR REPLACE INTO branches (id, name, active, franchise_supplier, visible_from) "
                 "VALUES (?, ?, 1, ?, ?)", (bid, f'B{bid}', franchise, visible_from))


def _goods(conn, bid, supplier, date):
    conn.execute("INSERT INTO goods_documents (branch_id, doc_date, supplier, ref_number, amount, doc_type) "
                 "VALUES (?, ?, ?, ?, 100, 3)", (bid, date, supplier, f'r{date}{supplier}'))


def _roster(conn, bid):
    return {r['supplier_name'] for r in conn.execute(
        "SELECT supplier_name FROM supplier_roster WHERE branch_id = ?", (bid,)).fetchall()}


def test_prior_two_months():
    assert prior_two_months(NOW) == ('2026-04', '2026-05')
    assert prior_two_months(datetime(2026, 1, 15, tzinfo=IL_TZ)) == ('2025-11', '2025-12')


def test_build_uses_prior_two_months_only(db):
    _branch(db, 700)
    _goods(db, 700, 'mar-sup', '2026-03-15')   # 3 months ago — excluded
    _goods(db, 700, 'apr-sup', '2026-04-10')   # prior — included
    _goods(db, 700, 'may-sup', '2026-05-20')   # prior — included
    _goods(db, 700, 'jun-sup', '2026-06-03')   # current month — excluded
    db.commit()
    n = build_for_branch(db, 700, now=NOW)
    assert n == 2
    assert _roster(db, 700) == {'apr-sup', 'may-sup'}


def test_excludes_franchise_supplier(db):
    _branch(db, 701, franchise='זיכיונות המכולת בע"מ')
    _goods(db, 701, 'זיכיונות המכולת בע"מ', '2026-05-05')   # franchise — excluded
    _goods(db, 701, 'ספק אמיתי', '2026-05-06')
    db.commit()
    build_for_branch(db, 701, now=NOW)
    r = _roster(db, 701)
    assert 'ספק אמיתי' in r
    assert 'זיכיונות המכולת בע"מ' not in r


def test_ignores_visible_from_floor(db):
    """New chain store: June floor, but its BilBoy goods exist in May (pre-floor).
    The roster build must still include those prior-month suppliers."""
    _branch(db, 702, visible_from='2026-06-01')
    _goods(db, 702, 'pre-floor-sup', '2026-05-12')
    db.commit()
    n = build_for_branch(db, 702, now=NOW)
    assert n == 1
    assert _roster(db, 702) == {'pre-floor-sup'}


def test_replace_on_refresh(db):
    _branch(db, 703)
    _goods(db, 703, 'old-sup', '2026-05-01')
    db.commit()
    build_for_branch(db, 703, now=NOW)
    assert _roster(db, 703) == {'old-sup'}
    # New month's data wipes the old roster — no stale rows.
    db.execute("DELETE FROM goods_documents WHERE branch_id = 703")
    _goods(db, 703, 'new-sup', '2026-04-09')
    db.commit()
    build_for_branch(db, 703, now=NOW)
    assert _roster(db, 703) == {'new-sup'}


def test_skips_blank_and_dash_suppliers(db):
    _branch(db, 704)
    _goods(db, 704, '—', '2026-05-01')
    _goods(db, 704, '', '2026-05-02')
    _goods(db, 704, 'real', '2026-05-03')
    db.commit()
    build_for_branch(db, 704, now=NOW)
    assert _roster(db, 704) == {'real'}

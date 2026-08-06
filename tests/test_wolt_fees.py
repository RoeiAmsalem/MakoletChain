"""Wolt commission fees → automatic system fixed-expense row (source='bilboy_wolt').

Covers: fee-line pattern extraction (incl. the audited 9020 July shape), MTD
growth across two simulated syncs, idempotent re-run, manager edit/delete
rejection, manual rows unaffected, no-Wolt branch has no row, carry-forward
ignores system rows, MTD pro-ration excludes the system row.
"""
import json
import os
import sys
import sqlite3

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from agents.bilboy import (
    extract_wolt_fee_total, is_wolt_fee_line, upsert_wolt_fee_expense,
    WOLT_EXPENSE_NAME, WOLT_EXPENSE_SOURCE,
)
from app import app, _now_il
import app as app_module


# ── extraction unit tests ─────────────────────────────────────────────────

def _doc(twv, two, items):
    return {'totalWithVat': twv, 'totalWithoutVat': two, 'items': items}


def _line(name, cat, total):
    return {'name': name, 'catalogNumber': cat, 'total': total}


def test_wolt_line_patterns():
    assert is_wolt_fee_line(_line('עמלת וולט 17% + מע"מ', '61', 100))
    assert is_wolt_fee_line(_line('וואלט + עמלה 17%+ מעמ', '63', 100))
    assert is_wolt_fee_line(_line('דמי שירות קבועים וולט + 3.8 למשלוח', '67', 100))
    assert is_wolt_fee_line(_line('קמפיין וולט', '69', 100))
    # cat code alone (the 10% service-fee line carries no וולט in the name)
    assert is_wolt_fee_line(_line('דמי שירות עמלה 10%', '62', 100))
    # name alone, unknown cat
    assert is_wolt_fee_line(_line('קמפיין וולט מיוחד', '0', 100))
    # NOT Wolt: royalty, catalog, rent, product EAN
    assert not is_wolt_fee_line(_line('תמלוגים חודשיים לחודש 07/2026', '30', 100))
    assert not is_wolt_fee_line(_line('ניהול קטלוג והקלדות מלאי', '10', 100))
    assert not is_wolt_fee_line(_line('שכר דירה', '40', 100))
    assert not is_wolt_fee_line(_line('חלב 3% קרטון מהדרין', '7290004131074', 100))


def test_extract_pure_wolt_doc_uses_incl_vat_total():
    """The audited 9020 July doc 4321 shape: pure-Wolt invoice — the row must
    carry the doc's incl-VAT total exactly (₪19,236)."""
    doc = _doc(19236.0, 16301.73, [
        _line('עמלת וולט 17% + מע"מ', '61', 8449.44),
        _line('וואלט + עמלה 17%+ מעמ', '63', 5899.03),
        _line('דמי שירות קבועים וולט + 3.8 למשלוח', '67', 672.60),
        _line('קמפיין וולט', '69', 1280.66),
    ])
    assert extract_wolt_fee_total([doc]) == 19236.0


def test_extract_mixed_doc_scales_by_vat_ratio():
    """A doc mixing a Wolt line with a non-Wolt fee contributes only the Wolt
    share, VAT-scaled."""
    doc = _doc(1180.0, 1000.0, [
        _line('עמלת וולט 17% + מע"מ', '61', 600.0),
        _line('ניהול קטלוג והקלדות מלאי', '10', 400.0),
    ])
    assert extract_wolt_fee_total([doc]) == round(600.0 * 1.18, 2)


def test_extract_no_wolt_lines_is_zero():
    doc = _doc(19135.0, 16216.10, [
        _line('תמלוגים חודשיים לחודש 06/2026', '30', 15662.60),
        _line('תמלוגים חודשיים ירקות ופירות לחודש 06/2026', '30', 553.50),
    ])
    assert extract_wolt_fee_total([doc]) == 0.0


# ── app-level fixture ─────────────────────────────────────────────────────

@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_wolt_fees.db')

    original_db = app_module.DB_PATH
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
    conn.execute("DELETE FROM branches")
    conn.execute("DELETE FROM users")
    conn.execute("DELETE FROM user_branches")
    conn.execute("INSERT INTO branches (id, name, city, active) VALUES (1, 'אלפא', 'עיר', 1)")
    pw = generate_password_hash('test123')
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, active) "
                 "VALUES (2, 'Mgr', 'mgr@t.com', ?, 'manager', 1)", (pw,))
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, active) "
                 "VALUES (3, 'Admin', 'admin@t.com', ?, 'admin', 1)", (pw,))
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (2, 1)")
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(test_db):
        os.remove(test_db)


def _db():
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _login(client, who='mgr'):
    email = 'admin@t.com' if who == 'admin' else 'mgr@t.com'
    return client.post('/login', data={'email': email, 'password': 'test123'})


def _cur_month():
    return _now_il().strftime('%Y-%m')


# ── upsert semantics ──────────────────────────────────────────────────────

def test_mtd_growth_across_two_syncs(client):
    """Simulated nightly recomputes: the row grows during the month, one row only."""
    m = _cur_month()
    conn = _db()
    assert upsert_wolt_fee_expense(conn, 1, m, 500.0) == 'upserted'
    assert upsert_wolt_fee_expense(conn, 1, m, 1961.0) == 'upserted'   # later sync, more fees
    rows = conn.execute(
        "SELECT amount, source, locked, expense_type FROM fixed_expenses "
        "WHERE branch_id=1 AND month=? AND name=?", (m, WOLT_EXPENSE_NAME)).fetchall()
    conn.close()
    assert len(rows) == 1
    assert rows[0]['amount'] == 1961.0
    assert rows[0]['source'] == WOLT_EXPENSE_SOURCE
    assert rows[0]['locked'] == 1


def test_idempotent_rerun(client):
    m = _cur_month()
    conn = _db()
    upsert_wolt_fee_expense(conn, 1, m, 19236.0)
    upsert_wolt_fee_expense(conn, 1, m, 19236.0)
    rows = conn.execute("SELECT COUNT(*) c FROM fixed_expenses WHERE branch_id=1 AND month=?",
                        (m,)).fetchone()
    assert rows['c'] == 1
    conn.close()


def test_zero_amount_removes_row_no_wolt_branch_shows_nothing(client):
    m = _cur_month()
    conn = _db()
    upsert_wolt_fee_expense(conn, 1, m, 500.0)
    assert upsert_wolt_fee_expense(conn, 1, m, 0.0) == 'deleted'
    assert upsert_wolt_fee_expense(conn, 1, m, 0.0) == 'none'
    c = conn.execute("SELECT COUNT(*) c FROM fixed_expenses WHERE branch_id=1").fetchone()['c']
    conn.close()
    assert c == 0


def test_manual_row_with_reserved_name_never_hijacked(client):
    m = _cur_month()
    conn = _db()
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount, expense_type, source) "
                 "VALUES (1, ?, ?, 777, 'monthly', 'manual')", (m, WOLT_EXPENSE_NAME))
    conn.commit()
    assert upsert_wolt_fee_expense(conn, 1, m, 19236.0) == 'blocked_by_manual_row'
    row = conn.execute("SELECT amount, source FROM fixed_expenses WHERE branch_id=1 AND month=?",
                       (m,)).fetchone()
    conn.close()
    assert row['amount'] == 777 and row['source'] == 'manual'


# ── endpoint guards ───────────────────────────────────────────────────────

def _seed_system_row(m, amount=1961.0):
    conn = _db()
    upsert_wolt_fee_expense(conn, 1, m, amount)
    rid = conn.execute("SELECT id FROM fixed_expenses WHERE branch_id=1 AND month=? AND name=?",
                       (m, WOLT_EXPENSE_NAME)).fetchone()['id']
    conn.close()
    return rid


def test_manager_cannot_edit_or_delete_system_row(client):
    m = _cur_month()
    rid = _seed_system_row(m)
    _login(client)
    r = client.put(f'/api/fixed-expenses/{rid}', json={'amount': 1})
    assert r.status_code == 403
    r = client.delete(f'/api/fixed-expenses/{rid}')
    assert r.status_code == 403
    conn = _db()
    amt = conn.execute("SELECT amount FROM fixed_expenses WHERE id=?", (rid,)).fetchone()['amount']
    conn.close()
    assert amt == 1961.0


def test_manager_cannot_create_reserved_name(client):
    _login(client)
    r = client.post('/api/fixed-expenses',
                    json={'name': WOLT_EXPENSE_NAME, 'amount': 5, 'month': _cur_month()})
    assert r.status_code == 400


def test_admin_can_delete_system_row(client):
    m = _cur_month()
    rid = _seed_system_row(m)
    _login(client, 'admin')
    r = client.delete(f'/api/fixed-expenses/{rid}')
    assert r.status_code == 200


def test_manual_rows_unaffected(client):
    """Managers still fully control their own manual rows."""
    m = _cur_month()
    _login(client)
    assert client.post('/api/fixed-expenses',
                       json={'name': 'שכירות', 'amount': 5000, 'month': m}).status_code == 200
    conn = _db()
    rid = conn.execute("SELECT id FROM fixed_expenses WHERE name='שכירות'").fetchone()['id']
    conn.close()
    assert client.put(f'/api/fixed-expenses/{rid}', json={'amount': 5500}).status_code == 200
    assert client.delete(f'/api/fixed-expenses/{rid}').status_code == 200


def test_list_exposes_source(client):
    m = _cur_month()
    _seed_system_row(m)
    _login(client)
    rows = json.loads(client.get(f'/api/fixed-expenses?month={m}').data)
    wolt = [r for r in rows if r['name'] == WOLT_EXPENSE_NAME]
    assert wolt and wolt[0]['source'] == WOLT_EXPENSE_SOURCE


# ── carry-forward + MTD semantics ─────────────────────────────────────────

def test_carry_forward_ignores_system_rows(client):
    """A system row in the new month must not block manual carry-forward, and
    system rows are never copied forward."""
    conn = _db()
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount, expense_type, source) "
                 "VALUES (1, '2026-01', 'שכירות', 5000, 'monthly', 'manual')")
    conn.commit()
    upsert_wolt_fee_expense(conn, 1, '2026-02', 800.0)   # system row lands first
    app_module._ensure_monthly_expenses(1, '2026-02', conn)
    rows = {r['name']: dict(r) for r in conn.execute(
        "SELECT name, amount, source FROM fixed_expenses WHERE branch_id=1 AND month='2026-02'")}
    conn.close()
    assert 'שכירות' in rows, 'manual carry-forward must not be blocked by the system row'
    assert rows['שכירות']['amount'] == 5000
    # and the JAN system row never existed, so only Feb's own wolt row is there
    assert rows[WOLT_EXPENSE_NAME]['source'] == WOLT_EXPENSE_SOURCE


def test_mtd_does_not_prorate_system_row(client):
    """MTD mode halves חודשי rows but must keep the Wolt row at its actual value."""
    conn = _db()
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount, expense_type, source) "
                 "VALUES (1, '2026-03', 'שכירות', 1000, 'monthly', 'manual')")
    conn.commit()
    upsert_wolt_fee_expense(conn, 1, '2026-03', 500.0)
    fx = app_module._get_fixed_total(1, '2026-03', 0, conn, mtd_factor=0.5)
    conn.close()
    assert fx['fixed_only'] == 1500.0
    # MTD: manual monthly 1000×0.5 + system wolt 500 (NOT pro-rated) = 1000
    assert fx['fixed_only_mtd'] == 1000.0

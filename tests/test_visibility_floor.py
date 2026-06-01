"""Per-branch visibility FLOOR (branches.visible_from, migration 021).

A floored branch (visible_from set) must NEVER see its own operational data
from before that date — and it is a rolling-forward FLOOR, not a single-month
window: June shows June, July shows June+July, but May is gone forever.

Branches 126/127 and the demo stores have NULL visible_from = no floor = full
history, and must stay completely unaffected.
"""
import json
import os
import sys
import sqlite3

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app

FLOORED = 9020          # gets visible_from = 2026-06-01
UNFLOORED = 126         # NULL visible_from — Shimon's store, full history


@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_visibility_floor.db')

    import app as app_module
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
    # Floored + unfloored branch. The migration's one-time UPDATE ran on the
    # empty table, so set the floor explicitly here (mirrors prod's real rows).
    conn.execute("INSERT INTO branches (id, name, city, active, visible_from) "
                 "VALUES (?, 'גוש', 'עיר', 1, '2026-06-01')", (FLOORED,))
    conn.execute("INSERT INTO branches (id, name, city, active, visible_from) "
                 "VALUES (?, 'איינשטיין', 'תל אביב', 1, NULL)", (UNFLOORED,))
    # Admin sees all branches and can switch via ?branch_id= — the floor is a
    # property of the BRANCH, so it applies regardless of who is viewing.
    pw = generate_password_hash('test123')
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, "
                 "active) VALUES (1, 'Roei', 'admin@t.com', ?, 'admin', 1)",
                 (pw,))

    for bid in (FLOORED, UNFLOORED):
        # daily_sales: May, June, July
        conn.execute("INSERT INTO daily_sales (branch_id, date, amount, transactions, source) "
                     "VALUES (?, '2026-05-15', 1000, 50, 'z_report')", (bid,))
        conn.execute("INSERT INTO daily_sales (branch_id, date, amount, transactions, source) "
                     "VALUES (?, '2026-06-15', 2000, 80, 'z_report')", (bid,))
        conn.execute("INSERT INTO daily_sales (branch_id, date, amount, transactions, source) "
                     "VALUES (?, '2026-07-15', 3000, 90, 'z_report')", (bid,))
        # goods, fixed expenses, employee hours in May (pre-floor) + June
        conn.execute("INSERT INTO goods_documents (branch_id, ref_number, supplier, "
                     "doc_type, doc_date, amount) "
                     "VALUES (?, 'r1', 'ספק', 3, '2026-05-10', 500)", (bid,))
        conn.execute("INSERT INTO goods_documents (branch_id, ref_number, supplier, "
                     "doc_type, doc_date, amount) "
                     "VALUES (?, 'r2', 'ספק', 3, '2026-06-10', 700)", (bid,))
        conn.execute("INSERT INTO fixed_expenses (branch_id, name, amount, expense_type, month) "
                     "VALUES (?, 'שכירות', 4000, 'monthly', '2026-05')", (bid,))
        conn.execute("INSERT INTO fixed_expenses (branch_id, name, amount, expense_type, month) "
                     "VALUES (?, 'שכירות', 4000, 'monthly', '2026-06')", (bid,))
        conn.execute("INSERT INTO employee_hours (branch_id, month, employee_name, "
                     "total_hours, total_salary, source) "
                     "VALUES (?, '2026-05', 'דנה', 100, 3000, 'aviv_report')", (bid,))
        conn.execute("INSERT INTO employee_hours (branch_id, month, employee_name, "
                     "total_hours, total_salary, source) "
                     "VALUES (?, '2026-06', 'דנה', 120, 3600, 'aviv_report')", (bid,))
    conn.commit()
    conn.close()

    with app.test_client() as c:
        c.post('/login', data={'email': 'admin@t.com', 'password': 'test123'})
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(test_db):
        os.remove(test_db)


def _get(client, path, branch_id, month=None):
    q = f'?branch_id={branch_id}'
    if month:
        q += f'&month={month}'
    return json.loads(client.get(path + q).data)


# ── summary KPIs ──────────────────────────────────────────────

def test_floored_summary_may_is_empty(client):
    d = _get(client, '/api/summary', FLOORED, '2026-05')
    assert d['income'] == 0
    assert d['goods'] == 0
    assert d['salary'] == 0


def test_floored_summary_june_shows_data(client):
    d = _get(client, '/api/summary', FLOORED, '2026-06')
    assert d['income'] == 2000
    assert d['goods'] == 700


def test_unfloored_summary_may_shows_data(client):
    d = _get(client, '/api/summary', UNFLOORED, '2026-05')
    assert d['income'] == 1000
    assert d['goods'] == 500


# ── sales list ────────────────────────────────────────────────

def test_floored_sales_may_empty_june_present(client):
    assert _get(client, '/api/sales', FLOORED, '2026-05')['sales'] == []
    assert _get(client, '/api/sales', FLOORED, '2026-06')['days'] == 1


def test_unfloored_sales_may_present(client):
    assert _get(client, '/api/sales', UNFLOORED, '2026-05')['days'] == 1


# ── fixed expenses ────────────────────────────────────────────

def test_floored_fixed_expenses_may_empty(client):
    assert _get(client, '/api/fixed-expenses', FLOORED, '2026-05') == []
    assert len(_get(client, '/api/fixed-expenses', FLOORED, '2026-06')) >= 1


# ── employees / hours ─────────────────────────────────────────

def test_floored_employees_may_empty(client):
    d = _get(client, '/api/employees', FLOORED, '2026-05')
    assert d['employees'] == []
    assert d['history'] == []


# ── history table: FLOOR, not a single-month window ───────────

def test_floored_history_is_rolling_floor(client):
    # Viewing July must return June AND July — but never May.
    rows = _get(client, '/api/history', FLOORED, '2026-07')
    months = [r['month'] for r in rows]
    assert '2026-05' not in months
    assert months == ['2026-06', '2026-07']


def test_unfloored_history_includes_may(client):
    rows = _get(client, '/api/history', UNFLOORED, '2026-07')
    months = [r['month'] for r in rows]
    assert '2026-05' in months


# ── month picker / navigation ─────────────────────────────────

def test_floored_page_clamps_picker_to_june(client):
    # Crafted ?month=2026-05 on a server-rendered page must land on June (יוני)
    # with the back-arrow disabled — can't navigate before the floor.
    html = client.get(f'/sales?branch_id={FLOORED}&month=2026-05').data.decode()
    assert 'יוני' in html                   # picker sits on June
    assert 'מאי' not in html                # May never shown
    assert '?month=2026-05' not in html     # no back-link to May
    assert 'month-disabled' in html         # back-arrow is disabled at the floor


def test_unfloored_page_allows_may(client):
    html = client.get(f'/sales?branch_id={UNFLOORED}&month=2026-05').data.decode()
    assert 'מאי' in html                    # May is shown
    assert '?month=2026-04' in html         # back-arrow to April still present

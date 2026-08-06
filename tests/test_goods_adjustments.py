"""Manual goods adjustments (goods_adjustments) — Option-B verified-figure carry.

Covers: adjustment flows into the goods total + גולמי; absent months untouched;
admin-only mutation (manager -> 403); supplier/document listing stays on the raw
doc sum.
"""
import json
import os
import sys
import sqlite3

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, _now_il
import app as app_module


@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_goods_adjustments.db')

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
    conn.execute("DELETE FROM goods_adjustments")
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


def _seed_income_and_goods(month, income, goods):
    """One daily_sales row (income) + one goods_documents row (goods) for branch 1."""
    d = month + '-01'
    conn = _db()
    conn.execute("INSERT INTO daily_sales (branch_id, date, amount, transactions, source) "
                 "VALUES (1, ?, ?, 100, 'z_report')", (d, income))
    conn.execute("INSERT INTO goods_documents (branch_id, doc_date, supplier, ref_number, amount, doc_type) "
                 "VALUES (1, ?, 'ספק א', 'R1', ?, 3)", (d, goods))
    conn.commit()
    conn.close()


def _add_adjustment(branch_id, month, label, amount):
    conn = _db()
    conn.execute("INSERT INTO goods_adjustments (branch_id, month, label, amount) "
                 "VALUES (?, ?, ?, ?)", (branch_id, month, label, amount))
    conn.commit()
    conn.close()


# ── unit: the goods total helper ──────────────────────────────────────────

def test_goods_total_includes_adjustment(client):
    m = _cur_month()
    _seed_income_and_goods(m, income=1000, goods=600)
    _add_adjustment(1, m, 'התאמה', 40)
    conn = _db()
    assert app_module._goods_total(1, m, conn) == 640.0
    conn.close()


def test_goods_total_absent_month_unaffected(client):
    m = _cur_month()
    _seed_income_and_goods(m, income=1000, goods=600)
    _add_adjustment(1, '2020-01', 'התאמה ישנה', 999)  # different month
    conn = _db()
    assert app_module._goods_total(1, m, conn) == 600.0  # July adj not applied
    conn.close()


# ── api_summary: גולמי reflects the adjustment ────────────────────────────

def test_summary_gross_reflects_adjustment(client):
    m = _cur_month()
    _seed_income_and_goods(m, income=1000, goods=600)
    _add_adjustment(1, m, 'סחורה דרך זיכיונות המכולת', 40)
    _login(client)
    d = json.loads(client.get('/api/summary').data)
    assert d['goods'] == 640.0                 # docs 600 + adjustment 40
    assert d['gross'] == 360.0                 # income 1000 - goods 640
    assert d['profit'] == d['income'] - d['goods'] - d['fixed'] - d['salary']


def test_summary_no_adjustment_is_plain_docs(client):
    m = _cur_month()
    _seed_income_and_goods(m, income=1000, goods=600)
    _login(client)
    d = json.loads(client.get('/api/summary').data)
    assert d['goods'] == 600.0
    assert d['gross'] == 400.0


# ── admin-only mutation ───────────────────────────────────────────────────

def test_manager_cannot_reach_admin_endpoints(client):
    _login(client, 'mgr')
    assert client.get('/admin/goods-adjustments').status_code == 403
    assert client.post('/admin/goods-adjustments/save',
                       json={'branch_id': 1, 'month': _cur_month(),
                             'label': 'x', 'amount': 5}).status_code == 403
    assert client.post('/admin/goods-adjustments/delete',
                       json={'id': 1}).status_code == 403


def test_admin_can_save_edit_delete(client):
    m = _cur_month()
    _seed_income_and_goods(m, income=1000, goods=600)
    _login(client, 'admin')
    # create
    r = client.post('/admin/goods-adjustments/save',
                    json={'branch_id': 1, 'month': m, 'label': 'זיכיונות', 'amount': 40})
    assert r.status_code == 200
    conn = _db()
    assert app_module._goods_total(1, m, conn) == 640.0
    row = conn.execute("SELECT id, created_by FROM goods_adjustments WHERE branch_id=1 AND month=?",
                       (m,)).fetchone()
    assert row['created_by'] == 3                        # audit: admin stamped
    adj_id = row['id']
    conn.close()
    # edit same natural key -> amount replaced, not duplicated
    r = client.post('/admin/goods-adjustments/save',
                    json={'branch_id': 1, 'month': m, 'label': 'זיכיונות', 'amount': 55})
    assert r.status_code == 200
    conn = _db()
    assert app_module._goods_total(1, m, conn) == 655.0
    assert conn.execute("SELECT COUNT(*) c FROM goods_adjustments").fetchone()['c'] == 1
    conn.close()
    # delete
    r = client.post('/admin/goods-adjustments/delete', json={'id': adj_id})
    assert r.status_code == 200
    conn = _db()
    assert app_module._goods_total(1, m, conn) == 600.0
    conn.close()


def test_save_rejects_bad_month(client):
    _login(client, 'admin')
    r = client.post('/admin/goods-adjustments/save',
                    json={'branch_id': 1, 'month': 'julyish', 'label': 'x', 'amount': 5})
    assert r.status_code == 400


# ── document/supplier listing stays on the raw doc sum ────────────────────

def test_goods_doc_context_keeps_raw_doc_total(client):
    m = _cur_month()
    _seed_income_and_goods(m, income=1000, goods=600)
    _add_adjustment(1, m, 'התאמה', 40)
    conn = _db()
    ctx = app_module._goods_doc_context(1, m, conn)
    conn.close()
    assert ctx['total'] == 600.0               # document table / supplier groups: raw docs
    assert ctx['adjustments_total'] == 40.0
    assert ctx['goods_total'] == 640.0         # headline (feeds גולמי)
    # supplier groups sum to the RAW doc total, not the adjusted one
    assert round(sum(g['total'] for g in ctx['groups']), 2) == 600.0

"""CEO/admin aggregate home view + role-aware APIs + demo CEO user.

Single-branch (manager) behaviour must stay byte-identical; admin/ceo get
the same JSON shapes summed across all visible branches. N-ready: helpers
loop a branch list, never a hardcoded id.
"""
import json
import os
import sys
import sqlite3

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import (
    app,
    _list_visible_branches,
    _is_aggregate_view,
    _aggregate_home_data,
)
from werkzeug.security import generate_password_hash

MONTH = '2026-03'  # past month → no live-sales interference, deterministic
_MIG_009 = os.path.join(os.path.dirname(__file__), '..',
                        'migrations', '009_demo_ceo_user.sql')

ADMIN = {'id': 1, 'role': 'admin'}
CEO = {'id': 3, 'role': 'ceo'}
MANAGER = {'id': 2, 'role': 'manager'}


@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_ceo_aggregate.db')

    import app as app_module
    original_db = app_module.DB_PATH
    if os.path.exists(test_db):
        os.remove(test_db)
    app_module.DB_PATH = test_db
    app_module.init_db()

    # Apply real migrations on top of schema.sql, exactly like deploy.sh
    # (gives us electricity_source/iec columns, user_events, migration 009).
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
    import migrate as _migrate
    _migrate.DB_PATH = test_db
    mconn = _migrate.get_connection()
    _migrate.ensure_migrations_table(mconn)
    _migrate.cmd_apply(mconn)
    mconn.close()

    conn = sqlite3.connect(test_db, timeout=30)
    # Reset seeded rows for a deterministic state: schema.sql seeds branch
    # 126; migration 009 seeds the demo user (it grabs id=1, colliding with
    # our explicit ids). The two demo-user tests re-apply 009 themselves.
    conn.execute("DELETE FROM branches")
    conn.execute("DELETE FROM users")
    conn.execute("DELETE FROM user_branches")
    # 5 active branches — b1/b2 carry data, b3-5 are zero (prove N-ready
    # without changing the b1+b2 sums the route-level tests assert).
    for bid, name in [(1, 'אלפא'), (2, 'בטא'), (3, 'גמא'),
                      (4, 'דלתא'), (5, 'אפסילון')]:
        conn.execute("INSERT INTO branches (id, name, city, active) "
                     "VALUES (?, ?, 'עיר', 1)", (bid, name))
    # Users: admin (id 1), manager (id 2 → branch 1), ceo (id 3, NO branches)
    pw = generate_password_hash('test123')
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, "
                 "active) VALUES (1, 'Admin', 'admin@t.com', ?, 'admin', 1)",
                 (pw,))
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, "
                 "active) VALUES (2, 'Mgr', 'mgr@t.com', ?, 'manager', 1)",
                 (pw,))
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, "
                 "active) VALUES (3, 'Ceo', 'ceo@t.com', ?, 'ceo', 1)",
                 (pw,))
    conn.execute("INSERT INTO user_branches (user_id, branch_id) "
                 "VALUES (2, 1)")
    # Revenue: b1=100, b2=200. Goods: b2=50. → profit b1=100, b2=150.
    conn.execute("INSERT INTO daily_sales (branch_id, date, amount, "
                 "transactions) VALUES (1, '2026-03-10', 100, 10)")
    conn.execute("INSERT INTO daily_sales (branch_id, date, amount, "
                 "transactions) VALUES (2, '2026-03-10', 200, 20)")
    conn.execute("INSERT INTO goods_documents (branch_id, doc_date, supplier, "
                 "ref_number, amount, doc_type) "
                 "VALUES (2, '2026-03-12', 'ספק', 'R1', 50, 1)")
    # Hourly sales for the sales-by-hour aggregate test.
    conn.execute("INSERT INTO hourly_sales (branch_id, date, hour, amount, "
                 "transactions) VALUES (1, '2026-03-10', 10, 60, 6)")
    conn.execute("INSERT INTO hourly_sales (branch_id, date, hour, amount, "
                 "transactions) VALUES (2, '2026-03-10', 10, 40, 4)")
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(test_db):
        os.remove(test_db)


def _apply_migration_009():
    """Run migrations/009 against the active test DB (login uses werkzeug)."""
    import app as app_module
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    with open(_MIG_009, 'r') as fh:
        conn.executescript(fh.read())
    conn.commit()
    conn.close()


def _login(client, email):
    return client.post('/login', data={'email': email, 'password': 'test123'},
                        follow_redirects=False)


# ── _list_visible_branches ────────────────────────────────────

def test_list_visible_branches_admin(client):
    with app.app_context():
        b = _list_visible_branches(ADMIN)
    assert sorted(x['id'] for x in b) == [1, 2, 3, 4, 5]


def test_list_visible_branches_ceo(client):
    with app.app_context():
        b = _list_visible_branches(CEO)
    assert sorted(x['id'] for x in b) == [1, 2, 3, 4, 5]


def test_list_visible_branches_manager(client):
    with app.app_context():
        b = _list_visible_branches(MANAGER)
    assert [x['id'] for x in b] == [1]


def test_is_aggregate_view_roles(client):
    assert _is_aggregate_view(ADMIN) is True
    assert _is_aggregate_view(CEO) is True
    assert _is_aggregate_view(MANAGER) is False


# ── _aggregate_home_data ──────────────────────────────────────

def test_aggregate_two_branches_sum(client):
    with app.app_context():
        d = _aggregate_home_data([1, 2], MONTH)
    assert d['totals']['revenue'] == 300


def test_aggregate_profit_pct_recomputed(client):
    with app.app_context():
        d = _aggregate_home_data([1, 2], MONTH)
    # profit = 300 - 50 = 250 → 250/300*100 = 83.33 (NOT the per-branch
    # average of 100% and 75% = 87.5).
    assert d['totals']['profit'] == 250
    assert d['totals']['profit_pct'] == pytest.approx(83.33, abs=0.01)
    assert d['totals']['profit_pct'] != pytest.approx(87.5, abs=0.01)


def test_aggregate_per_branch_breakdown(client):
    with app.app_context():
        d = _aggregate_home_data([1, 2], MONTH)
    assert len(d['per_branch']) == 2
    by = {p['branch_id']: p for p in d['per_branch']}
    assert by[1]['revenue'] == 100 and by[1]['goods'] == 0
    assert by[2]['revenue'] == 200 and by[2]['goods'] == 50


def test_aggregate_n_branches(client):
    with app.app_context():
        d = _aggregate_home_data([1, 2, 3, 4, 5], MONTH)
    assert len(d['per_branch']) == 5            # loop handled N
    assert d['totals']['revenue'] == 300        # b3-5 contribute 0


# ── Role-aware /api/summary ───────────────────────────────────

def test_api_summary_aggregate_sum(client):
    _login(client, 'admin@t.com')
    d = json.loads(client.get(f'/api/summary?month={MONTH}').data)
    assert d['income'] == 300
    assert d['goods'] == 50
    assert d['salary_source'] == 'aggregate'


def test_api_summary_manager_single(client):
    _login(client, 'mgr@t.com')
    d = json.loads(client.get(f'/api/summary?month={MONTH}').data)
    assert d['income'] == 100               # branch 1 only
    assert d['branch_id'] == 1


def test_api_history_aggregate_sum(client):
    _login(client, 'admin@t.com')
    rows = json.loads(client.get(f'/api/history?month={MONTH}').data)
    march = next(r for r in rows if r['month'] == MONTH)
    assert march['income'] == 300
    assert march['goods'] == 50


def test_api_history_manager_single(client):
    _login(client, 'mgr@t.com')
    rows = json.loads(client.get(f'/api/history?month={MONTH}').data)
    march = next(r for r in rows if r['month'] == MONTH)
    assert march['income'] == 100


def test_api_sales_by_hour_aggregate_sum(client):
    _login(client, 'admin@t.com')
    d = json.loads(client.get(f'/api/sales-by-hour?month={MONTH}').data)
    hour10 = next(h for h in d['hourly'] if h['hour'] == 10)
    assert hour10['total'] == 100           # 60 + 40
    assert hour10['count'] == 10            # 6 + 4


def test_api_sales_by_hour_manager_single(client):
    _login(client, 'mgr@t.com')
    d = json.loads(client.get(f'/api/sales-by-hour?month={MONTH}').data)
    hour10 = next(h for h in d['hourly'] if h['hour'] == 10)
    assert hour10['total'] == 60            # branch 1 only


# ── Home route role awareness ─────────────────────────────────

def test_home_route_admin_aggregate_view(client):
    _login(client, 'admin@t.com')
    body = client.get('/').get_data(as_text=True)
    assert 'תצוגת רשת' in body
    # KPI numbers are summed (loaded via the now role-aware /api/summary)
    d = json.loads(client.get(f'/api/summary?month={MONTH}').data)
    assert d['income'] == 300


def test_home_route_manager_single_view(client):
    _login(client, 'mgr@t.com')
    body = client.get('/').get_data(as_text=True)
    assert 'תצוגת רשת' not in body


def test_home_route_ceo_user_aggregate_view(client):
    _login(client, 'ceo@t.com')
    body = client.get('/').get_data(as_text=True)
    assert 'תצוגת רשת' in body


def test_branch_comparison_chart_renders_in_aggregate(client):
    _login(client, 'ceo@t.com')
    body = client.get('/').get_data(as_text=True)
    assert '<canvas id="branch-comparison-chart"' in body


def test_branch_comparison_chart_absent_in_single_view(client):
    _login(client, 'mgr@t.com')
    body = client.get('/').get_data(as_text=True)
    assert '<canvas id="branch-comparison-chart"' not in body


# ── /api/branch-comparison ────────────────────────────────────

def test_branch_comparison_endpoint_admin(client):
    _login(client, 'admin@t.com')
    res = client.get(f'/api/branch-comparison?month={MONTH}')
    assert res.status_code == 200
    d = json.loads(res.data)
    assert len(d['branches']) == 5
    by = {b['branch_id']: b for b in d['branches']}
    assert by[1]['revenue'] == 100 and by[2]['salary'] == 0


def test_branch_comparison_endpoint_manager(client):
    # Judgment call: scoped 200 one-branch payload (the home UI only calls
    # this in aggregate view; scoping is safer/less brittle than a 403).
    _login(client, 'mgr@t.com')
    res = client.get(f'/api/branch-comparison?month={MONTH}')
    assert res.status_code == 200
    d = json.loads(res.data)
    assert len(d['branches']) == 1
    assert d['branches'][0]['branch_id'] == 1


# ── Demo CEO user (migration 009) ─────────────────────────────

def test_demo_ceo_user_seeded(client):
    _apply_migration_009()
    import app as app_module
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    u = conn.execute("SELECT id, role FROM users WHERE "
                      "email = 'demo@makoletchain.com'").fetchone()
    assert u is not None and u['role'] == 'ceo'
    n = conn.execute("SELECT COUNT(*) FROM user_branches WHERE user_id = ?",
                     (u['id'],)).fetchone()[0]
    conn.close()
    assert n == 0                            # CEO gets all branches by role


def test_demo_ceo_user_can_login(client):
    _apply_migration_009()
    res = client.post('/login',
                       data={'email': 'demo@makoletchain.com',
                             'password': 'Demo2026'},
                       follow_redirects=False)
    assert res.status_code in (302, 303)     # werkzeug hash verified

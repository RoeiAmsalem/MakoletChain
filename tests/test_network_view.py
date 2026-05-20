"""CEO 'network' aggregate view.

Covers:
  - _list_visible_branches across the three roles
  - /api/network-overview gating, payload shape, and sums
  - leaderboard ordering
  - view-mode toggle persistence + manager 403
  - migration 009 demo CEO can log in
"""
import os
import sys
import sqlite3

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, _now_il, _list_visible_branches


TEST_DB = os.path.join(os.path.dirname(__file__), 'test_network_view.db')


@pytest.fixture
def client():
    app.config['TESTING'] = True

    import app as app_module
    original_db = app_module.DB_PATH
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    app_module.DB_PATH = TEST_DB
    app_module.init_db()

    # Apply all migrations (including 009 — needed by the demo-login test).
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
    import migrate as _migrate
    _migrate.DB_PATH = TEST_DB
    mconn = _migrate.get_connection()
    _migrate.ensure_migrations_table(mconn)
    _migrate.cmd_apply(mconn)
    mconn.close()

    conn = sqlite3.connect(TEST_DB, timeout=30)
    conn.execute("DELETE FROM user_branches")
    # Keep the demo CEO row created by migration 009 so we exercise the
    # migration end-to-end in test_demo_ceo_user_can_log_in.
    conn.execute("DELETE FROM users WHERE email != 'demo@makoletchain.com'")
    conn.execute("DELETE FROM branches")
    conn.execute(
        "INSERT INTO branches (id, name, city, active) "
        "VALUES (126, 'איינשטיין', 'תל אביב', 1)"
    )
    conn.execute(
        "INSERT INTO branches (id, name, city, active) "
        "VALUES (127, 'התיכון', 'תל אביב', 1)"
    )
    # Inactive — must not appear anywhere.
    conn.execute(
        "INSERT INTO branches (id, name, city, active) "
        "VALUES (200, 'סגור', 'תל אביב', 0)"
    )
    pw = generate_password_hash('test123')
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) "
        "VALUES (10, 'Admin', 'admin@test.com', ?, 'admin', 1)", (pw,))
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) "
        "VALUES (11, 'CEO', 'ceo@test.com', ?, 'ceo', 1)", (pw,))
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) "
        "VALUES (12, 'Manager 126', 'mgr@test.com', ?, 'manager', 1)", (pw,))
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (12, 126)")
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def _login(client, email, password='test123'):
    return client.post('/login', data={'email': email, 'password': password},
                       follow_redirects=False)


def _seed_branch_finance(branch_id, revenue, txn, goods, salary_hours, hourly_rate):
    """Seed daily_sales, goods_documents, employees + employee_hours for the
    current month so /api/network-overview has data to compute against."""
    month = _now_il().strftime('%Y-%m')
    day = month + '-01'
    conn = sqlite3.connect(TEST_DB, timeout=30)
    conn.execute(
        "INSERT INTO daily_sales (branch_id, date, amount, transactions) "
        "VALUES (?, ?, ?, ?)", (branch_id, day, revenue, txn))
    if goods > 0:
        conn.execute(
            "INSERT INTO goods_documents (branch_id, ref_number, supplier, "
            "doc_type, doc_date, amount) "
            "VALUES (?, ?, 'sup', 3, ?, ?)",
            (branch_id, f'R{branch_id}', day, goods))
    if salary_hours > 0:
        conn.execute(
            "INSERT INTO employees (branch_id, name, hourly_rate, active) "
            "VALUES (?, 'עובד', ?, 1)", (branch_id, hourly_rate))
        conn.execute(
            "INSERT INTO employee_hours (branch_id, month, employee_name, "
            "total_hours, total_salary, source) "
            "VALUES (?, ?, 'עובד', ?, ?, 'aviv_report')",
            (branch_id, month, salary_hours, salary_hours * hourly_rate))
    conn.commit()
    conn.close()


# ── _list_visible_branches ─────────────────────────────────────

class TestListVisibleBranches:

    def test_admin_returns_all_active_branches(self, client):
        # _list_visible_branches needs an app context for get_db().
        with app.test_request_context():
            visible = _list_visible_branches(10, 'admin')
        ids = [b['id'] for b in visible]
        assert 126 in ids and 127 in ids
        assert 200 not in ids  # inactive must be excluded

    def test_ceo_returns_all_active_branches(self, client):
        with app.test_request_context():
            visible = _list_visible_branches(11, 'ceo')
        ids = [b['id'] for b in visible]
        assert ids == [126, 127]

    def test_manager_returns_only_assigned_branches(self, client):
        with app.test_request_context():
            visible = _list_visible_branches(12, 'manager')
        ids = [b['id'] for b in visible]
        assert ids == [126]


# ── /api/network-overview ──────────────────────────────────────

class TestNetworkOverview:

    def test_ceo_gets_200_with_all_sections(self, client):
        _seed_branch_finance(126, revenue=10000, txn=200, goods=3000,
                             salary_hours=100, hourly_rate=40)
        _seed_branch_finance(127, revenue=5000, txn=125, goods=2000,
                             salary_hours=50, hourly_rate=40)
        _login(client, 'ceo@test.com')
        res = client.get('/api/network-overview')
        assert res.status_code == 200
        data = res.get_json()
        for key in ('branches', 'monthly_revenue', 'trend_6m',
                    'profitability', 'avg_basket', 'expense_breakdown',
                    'leaderboard'):
            assert key in data, f'missing key: {key}'
        assert len(data['branches']) == 2
        assert len(data['monthly_revenue']) == 2
        assert len(data['profitability']) == 2
        assert len(data['leaderboard']) == 2

    def test_manager_gets_403(self, client):
        _login(client, 'mgr@test.com')
        res = client.get('/api/network-overview')
        assert res.status_code == 403

    def test_monthly_revenue_sum_matches(self, client):
        _seed_branch_finance(126, revenue=10000, txn=200, goods=0,
                             salary_hours=0, hourly_rate=0)
        _seed_branch_finance(127, revenue=4500, txn=100, goods=0,
                             salary_hours=0, hourly_rate=0)
        _login(client, 'admin@test.com')
        data = client.get('/api/network-overview').get_json()
        total = sum(r['value'] for r in data['monthly_revenue'])
        assert total == 14500

    def test_leaderboard_sorted_by_profit_desc(self, client):
        # Branch 126: higher revenue + lower costs → higher profit
        _seed_branch_finance(126, revenue=20000, txn=400, goods=3000,
                             salary_hours=50, hourly_rate=40)
        # Branch 127: lower revenue → lower profit
        _seed_branch_finance(127, revenue=5000, txn=100, goods=2000,
                             salary_hours=50, hourly_rate=40)
        _login(client, 'admin@test.com')
        data = client.get('/api/network-overview').get_json()
        profits = [r['profit'] for r in data['leaderboard']]
        assert profits == sorted(profits, reverse=True)
        assert data['leaderboard'][0]['branch_id'] == 126
        assert data['leaderboard'][0]['rank'] == 1
        assert data['leaderboard'][1]['rank'] == 2


# ── /api/set-view-mode + home page dispatch ────────────────────

class TestViewModeToggle:

    def test_toggle_persists_and_renders_network_template(self, client):
        _login(client, 'ceo@test.com')
        # Default: branch view → index.html (has KPI tiles)
        res = client.get('/')
        assert res.status_code == 200
        assert 'kpi-section' in res.get_data(as_text=True)

        # Flip to network view
        toggle = client.post('/api/set-view-mode', json={'mode': 'network'})
        assert toggle.status_code == 200

        res = client.get('/')
        body = res.get_data(as_text=True)
        assert res.status_code == 200
        assert 'net-monthly-revenue' in body  # network template marker
        assert 'kpi-section' not in body      # KPI tiles must be absent

    def test_manager_cannot_set_view_mode(self, client):
        _login(client, 'mgr@test.com')
        res = client.post('/api/set-view-mode', json={'mode': 'network'})
        assert res.status_code == 403


# ── Network template has no KPI tiles ─────────────────────────

class TestNetworkTemplate:

    def test_no_kpi_tiles_in_network_template(self, client):
        _login(client, 'ceo@test.com')
        client.post('/api/set-view-mode', json={'mode': 'network'})
        body = client.get('/').get_data(as_text=True)
        # Each of these is unique to index.html's KPI cards.
        for marker in ('kpi-section', 'live-tile', 'profit-tile',
                       'fixed-tile', 'salary-value'):
            assert marker not in body, f'unexpected KPI marker: {marker}'


# ── Migration 009: demo CEO ────────────────────────────────────

class TestDemoCeoMigration:

    def test_demo_ceo_user_can_log_in(self, client):
        res = client.post('/login',
                          data={'email': 'demo@makoletchain.com',
                                'password': 'Demo2026'},
                          follow_redirects=False)
        # Successful login redirects to /
        assert res.status_code in (302, 303)
        assert res.headers.get('Location', '').endswith('/')

        # Session reflects CEO role
        with client.session_transaction() as sess:
            assert sess.get('user_role') == 'ceo'

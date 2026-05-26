"""Dedicated /network page for multi-branch accounts.

Replaces the inline expand on the home page (commit 0a0bb3b). The page reuses
/api/live-sales/network for data (already access-controlled there); this test
file pins the route's access guard, the summary bar + tile grid render, the
branch isolation, and the click-through links.
"""
import os
import sqlite3
import sys

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, _now_il  # noqa: E402


TEST_DB = os.path.join(os.path.dirname(__file__), 'test_network_page.db')


@pytest.fixture
def client():
    app.config['TESTING'] = True
    import app as app_module
    original_db = app_module.DB_PATH
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    app_module.DB_PATH = TEST_DB
    app_module.init_db()

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
    import migrate as _migrate
    _migrate.DB_PATH = TEST_DB
    mconn = _migrate.get_connection()
    _migrate.ensure_migrations_table(mconn)
    _migrate.cmd_apply(mconn)
    mconn.close()

    conn = sqlite3.connect(TEST_DB, timeout=30)
    conn.execute("DELETE FROM user_branches")
    conn.execute("DELETE FROM users WHERE email != 'demo@makoletchain.com'")
    conn.execute("DELETE FROM branches")
    conn.execute("DELETE FROM live_sales")
    conn.execute("DELETE FROM daily_sales")
    conn.execute("INSERT INTO branches (id, name, city, active) VALUES (126, 'איינשטיין', 'תל אביב', 1)")
    conn.execute("INSERT INTO branches (id, name, city, active) VALUES (127, 'התיכון', 'תל אביב', 1)")
    conn.execute("INSERT INTO branches (id, name, city, active) VALUES (128, 'אחר', 'חיפה', 1)")

    pw = generate_password_hash('test123')
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) "
        "VALUES (10, 'Admin', 'admin@test.com', ?, 'admin', 1)", (pw,)
    )
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) "
        "VALUES (11, 'Manager 1B', 'one@test.com', ?, 'manager', 1)", (pw,)
    )
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (11, 126)")
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) "
        "VALUES (12, 'Manager 2B', 'two@test.com', ?, 'manager', 1)", (pw,)
    )
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (12, 126)")
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (12, 127)")

    today = _now_il().strftime('%Y-%m-%d')
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, last_updated, fetched_at) "
        "VALUES (126, ?, 5000, 50, '12:30:00', datetime('now'))", (today,)
    )
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, last_updated, fetched_at) "
        "VALUES (127, ?, 3200, 28, '12:30:00', datetime('now'))", (today,)
    )
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def _login(client, email):
    return client.post('/login', data={'email': email, 'password': 'test123'},
                       follow_redirects=False)


def test_network_page_multi_branch(client):
    """A multi-branch account can load /network and the page renders the header,
    the 3-card summary bar, and the tile grid container."""
    _login(client, 'admin@test.com')
    res = client.get('/network')
    assert res.status_code == 200
    body = res.data.decode('utf-8')
    assert 'תצוגת רשת' in body
    assert 'כל הסניפים בזמן אמת' in body
    # Summary bar = 3 cards (total revenue, transactions, avg per branch)
    assert 'id="sum-total"' in body
    assert 'id="sum-transactions"' in body
    assert 'id="sum-avg"' in body
    # Tile grid container present
    assert 'id="net-grid"' in body
    # Sort controls present (default = revenue)
    assert 'id="sort-revenue"' in body
    assert 'id="sort-name"' in body


def test_network_page_single_branch_denied(client):
    """A manager assigned to exactly 1 branch is redirected away from /network —
    the page would be a one-tile grid and isn't meant for them."""
    _login(client, 'one@test.com')
    res = client.get('/network', follow_redirects=False)
    # Redirect (302/303) back to /
    assert res.status_code in (301, 302, 303)
    assert res.headers.get('Location', '').endswith('/')


def test_network_page_only_assigned(client):
    """A multi-store manager assigned to {126, 127} must see ONLY those two
    branches via /api/live-sales/network — never branch 128. URL params can't
    leak: the endpoint reads from session/user_branches."""
    _login(client, 'two@test.com')
    # /network page itself must load
    page = client.get('/network')
    assert page.status_code == 200
    # API behind the page returns only assigned branches
    res = client.get('/api/live-sales/network')
    assert res.status_code == 200
    data = res.get_json()
    ids = sorted(b['branch_id'] for b in data['branches'])
    assert ids == [126, 127], f'2-branch manager must see only assigned; got {ids}'
    # URL param injection must NOT leak branch 128
    res2 = client.get('/api/live-sales/network?branch_id=128')
    ids2 = sorted(b['branch_id'] for b in res2.get_json()['branches'])
    assert ids2 == [126, 127]


def test_dashboard_tile_links_to_network(client):
    """The multi-branch dashboard live tile must navigate to /network on click
    (replaces the inline expand from 0a0bb3b)."""
    _login(client, 'admin@test.com')
    res = client.get('/')
    assert res.status_code == 200
    body = res.data.decode('utf-8')
    assert 'data-multi-branch="true"' in body
    assert "location.href='/network'" in body, \
        'live tile must redirect to /network on click (inline expand replaced)'
    # The old inline expand markup MUST be gone
    assert 'id="live-network-section"' not in body
    assert 'id="live-network-grid"' not in body
    assert 'toggleLiveNetwork' not in body


def test_tile_links_to_branch(client):
    """Branch tiles on /network are anchors to '/?branch_id=<id>' so the CEO
    can drill into a single branch dashboard. The rendering is client-side, so
    the template carries the JS that builds those hrefs."""
    _login(client, 'admin@test.com')
    res = client.get('/network')
    body = res.data.decode('utf-8')
    # The renderTile JS builds tile hrefs from branch_id — pin the prefix
    assert "'/?branch_id=' + encodeURIComponent(b.branch_id)" in body, \
        'each network tile must link to /?branch_id=<id>'


def test_navbar_shows_network_link_for_multi_branch(client):
    """Multi-branch accounts get a navbar link to /network so they can reach
    it from anywhere — not only via the home-page live tile."""
    _login(client, 'two@test.com')
    res = client.get('/')
    body = res.data.decode('utf-8')
    assert 'href="/network"' in body, \
        'multi-branch account must see the /network nav link'


def test_navbar_hides_network_link_for_single_branch(client):
    """A single-branch manager must NOT see the /network nav link — they have
    no other branches to compare to."""
    _login(client, 'one@test.com')
    res = client.get('/')
    body = res.data.decode('utf-8')
    assert 'href="/network"' not in body

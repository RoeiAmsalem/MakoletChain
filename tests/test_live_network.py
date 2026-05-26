"""Live tile → per-branch network grid for multi-branch accounts.

Locks the trigger logic and access control on /api/live-sales/network and the
home-page render of the click-to-expand grid:

  - test_single_branch_unchanged   — 1-branch manager sees the plain live tile
  - test_multi_branch_shows_network — admin/ceo sees the click-to-expand grid
                                       with tile-per-branch markup present
  - test_network_only_assigned_branches — a 2-branch manager sees ONLY their
                                       assigned branches via the API
  - test_closed_branch_shows_greyed — a branch with no live row today but a
                                       past live row renders is_closed in the
                                       API payload (grid renders greyed in JS)
"""
import os
import sqlite3
import sys

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, _now_il  # noqa: E402


TEST_DB = os.path.join(os.path.dirname(__file__), 'test_live_network.db')


@pytest.fixture
def client():
    app.config['TESTING'] = True
    import app as app_module
    original_db = app_module.DB_PATH
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    app_module.DB_PATH = TEST_DB
    app_module.init_db()

    # Apply migrations so live_sales has the full schema (cancellation_total etc).
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
    conn.execute(
        "INSERT INTO branches (id, name, city, active) VALUES (126, 'איינשטיין', 'תל אביב', 1)"
    )
    conn.execute(
        "INSERT INTO branches (id, name, city, active) VALUES (127, 'התיכון', 'תל אביב', 1)"
    )
    # Third branch — proves admin/ceo see all three, while the 2-branch
    # manager seeded below sees only their two.
    conn.execute(
        "INSERT INTO branches (id, name, city, active) VALUES (128, 'אחר', 'חיפה', 1)"
    )

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

    # Live rows: today for branch 126 (fresh), yesterday for branch 127 (closed).
    today = _now_il().strftime('%Y-%m-%d')
    from datetime import datetime, timedelta
    yesterday = (datetime.strptime(today, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, last_updated, fetched_at) "
        "VALUES (126, ?, 5000, 50, '12:30:00', datetime('now'))", (today,)
    )
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, last_updated, fetched_at) "
        "VALUES (127, ?, 8000, 80, '20:00:00', datetime('now', '-1 day'))", (yesterday,)
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


def test_single_branch_unchanged(client):
    """A manager with exactly 1 assigned branch sees the normal single-tile
    layout — no expand affordance, no network section, no multi-branch attr."""
    _login(client, 'one@test.com')
    res = client.get('/')
    assert res.status_code == 200
    body = res.data.decode('utf-8')
    assert 'data-multi-branch="true"' not in body, \
        'single-branch user must NOT get the multi-branch attribute on the live tile'
    assert 'id="live-network-section"' not in body, \
        'single-branch user must NOT get the expandable network section'
    assert 'id="live-expand-hint"' not in body, \
        'single-branch user must NOT see the "click to expand" hint'
    # Original single-tile label is preserved
    assert 'היום בזמן אמת' in body


def test_multi_branch_shows_network(client):
    """An admin (multi-branch) sees the click-to-expand grid: the live tile has
    data-multi-branch="true" + click hint, and the network section + grid
    container are rendered (hidden by default)."""
    _login(client, 'admin@test.com')
    res = client.get('/')
    assert res.status_code == 200
    body = res.data.decode('utf-8')
    assert 'data-multi-branch="true"' in body
    assert 'id="live-network-section"' in body
    assert 'id="live-network-grid"' in body
    assert 'חי כרגע — כל הסניפים' in body
    assert 'לחץ להרחבה לפי סניף' in body


def test_network_only_assigned_branches(client):
    """A multi-store manager assigned to {126, 127} must see ONLY those two
    branches in /api/live-sales/network — never branch 128. URL params can't
    leak other branches: the endpoint reads from session/user_branches."""
    _login(client, 'two@test.com')
    res = client.get('/api/live-sales/network')
    assert res.status_code == 200
    data = res.get_json()
    assert data['is_multi_branch'] is True
    ids = sorted(b['branch_id'] for b in data['branches'])
    assert ids == [126, 127], \
        f'multi-store manager must see ONLY assigned branches; got {ids}'
    assert data['total_count'] == 2
    # Attempting to pass ?branch_id=128 must not leak — endpoint ignores it.
    res = client.get('/api/live-sales/network?branch_id=128')
    ids = sorted(b['branch_id'] for b in res.get_json()['branches'])
    assert ids == [126, 127], 'URL params must not change the assigned-branch set'


def test_closed_branch_shows_greyed(client):
    """A branch whose latest live row is a past day with no fresh pull today
    must come back with is_closed=true and last_amount/last_date — the grid
    JS uses these flags to render the greyed 'החנות סגורה' tile.
    Branch 127 has only a yesterday live row → must be is_closed."""
    _login(client, 'admin@test.com')
    res = client.get('/api/live-sales/network')
    data = res.get_json()
    by_id = {b['branch_id']: b for b in data['branches']}
    assert by_id[127]['is_closed'] is True, \
        'branch 127 (only yesterday live row) must be is_closed'
    assert by_id[127]['amount'] is None, \
        'closed branch must NOT surface a number — only last_amount for context'
    assert by_id[127].get('last_amount') == 8000
    assert by_id[127].get('last_date')
    # Branch 126 with a fresh row today is NOT closed
    assert by_id[126]['is_closed'] is False
    assert by_id[126]['amount'] == 5000
    # Chain total reflects only fresh branches
    assert data['chain_total'] == 5000
    assert data['active_count'] == 1

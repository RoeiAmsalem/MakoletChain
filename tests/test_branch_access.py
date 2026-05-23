"""Cross-branch access regression + store-name navbar render.

Locks the security guarantee that a single-branch manager cannot read another
branch's data by passing ?branch_id=<other> on an API call. The server's
`get_branch_id()` is the gate; this is the integration-level proof that the
gate holds end-to-end.

Also asserts that the navbar surfaces the manager's store (branch) name — via
the branch-name pill for a single-branch manager — and does NOT show the
product brand string "קופה שקופה" as visible navbar text.
"""
import os
import sqlite3
import sys

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app  # noqa: E402


REPO_ROOT = os.path.join(os.path.dirname(__file__), '..')
TEST_DB = os.path.join(os.path.dirname(__file__), 'test_branch_access.db')

OWNED_BRANCH = 126
OTHER_BRANCH = 127
OWNED_NAME = 'המכולת אינשטיין'
OTHER_NAME = 'המכולת תיכון'


@pytest.fixture
def client():
    app.config['TESTING'] = True
    import app as app_module
    original_db = app_module.DB_PATH
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    app_module.DB_PATH = TEST_DB
    app_module.init_db()

    sys.path.insert(0, os.path.join(REPO_ROOT, 'scripts'))
    import migrate as _migrate
    _migrate.DB_PATH = TEST_DB
    mconn = _migrate.get_connection()
    _migrate.ensure_migrations_table(mconn)
    _migrate.cmd_apply(mconn)
    mconn.close()

    conn = sqlite3.connect(TEST_DB, timeout=30)
    conn.execute('DELETE FROM branches')
    conn.execute('DELETE FROM users')
    conn.execute('DELETE FROM user_branches')
    conn.execute('DELETE FROM daily_sales')
    conn.execute(
        'INSERT INTO branches (id, name, city, active) VALUES (?, ?, ?, 1)',
        (OWNED_BRANCH, OWNED_NAME, 'חיפה')
    )
    conn.execute(
        'INSERT INTO branches (id, name, city, active) VALUES (?, ?, ?, 1)',
        (OTHER_BRANCH, OTHER_NAME, 'חיפה')
    )
    pw = generate_password_hash('test123')
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) "
        "VALUES (10, 'Shimon', 'shimon@test.com', ?, 'manager', 1)", (pw,)
    )
    conn.execute(
        'INSERT INTO user_branches (user_id, branch_id) VALUES (10, ?)',
        (OWNED_BRANCH,)
    )

    # Distinct income per branch — lets us prove which branch the server served
    conn.execute(
        "INSERT INTO daily_sales (branch_id, date, amount, transactions, source) "
        "VALUES (?, '2026-05-01', 1111, 10, 'z_report')", (OWNED_BRANCH,)
    )
    conn.execute(
        "INSERT INTO daily_sales (branch_id, date, amount, transactions, source) "
        "VALUES (?, '2026-05-01', 9999, 99, 'z_report')", (OTHER_BRANCH,)
    )
    conn.commit()
    conn.close()

    with app.test_client() as c:
        c.post('/login', data={'email': 'shimon@test.com', 'password': 'test123'})
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def test_single_branch_manager_cannot_access_other_branch(client):
    """A manager whose user_branches = [126] requesting /api/summary?branch_id=127
    must be served branch 126's data — the URL param is ignored when the user
    doesn't own the requested branch.

    We seed 1111 income for branch 126 and 9999 for branch 127, then assert
    the response income is 1111 even when the URL asks for 127. This is the
    proof of `get_branch_id()`'s allow-list check (app.py:469-474)."""
    res = client.get(f'/api/summary?branch_id={OTHER_BRANCH}&month=2026-05')
    assert res.status_code == 200
    data = res.get_json()
    assert data['income'] == 1111, (
        f"CRITICAL: server served branch {OTHER_BRANCH}'s data to a manager "
        f"who only owns {OWNED_BRANCH}. Got income={data['income']}, "
        f"expected 1111 (branch {OWNED_BRANCH}'s value)."
    )
    assert data['income'] != 9999


def test_navbar_shows_store_name(client):
    """The navbar surfaces the manager's branch name (via the branch-name pill
    for single-branch managers) — the user needs to know which store's data
    they're looking at."""
    res = client.get('/')
    assert res.status_code == 200
    body = res.data.decode('utf-8')
    assert 'class="branch-name-pill"' in body, \
        'single-branch manager must see the static branch-name pill'
    assert OWNED_NAME in body, \
        f"navbar must surface the store name '{OWNED_NAME}'"


def test_navbar_no_visible_brand_text(client):
    """The product brand 'קופה שקופה' must not appear as visible navbar text —
    only in <title>, alt, aria-label, and manifest. Logo is the brand mark."""
    res = client.get('/')
    body = res.data.decode('utf-8')
    # The <span class="brand-name"> wrapper was removed in commit b60c9c6
    assert 'class="brand-name"' not in body
    # Title and a11y attrs are allowed to carry the brand
    assert '<title>' in body and 'קופה שקופה' in body  # via title block default

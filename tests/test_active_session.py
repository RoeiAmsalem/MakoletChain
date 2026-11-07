"""users.active enforcement on EXISTING sessions (_enforce_active_user).

The login query already refuses active=0, but a 30-day remember-me cookie
outlived that check — a deactivated manager kept full access. The hook
re-reads users.active on every request: inactive or deleted → session
cleared, pages → /login, API → 401, no page_view written. Fail-open: a DB
error in the lookup lets the request through.
"""
import os
import sqlite3
import sys

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app  # noqa: E402
import app as app_module  # noqa: E402

REPO_ROOT = os.path.join(os.path.dirname(__file__), '..')
TEST_DB = os.path.join(os.path.dirname(__file__), 'test_active_session.db')

BRANCH = 126
U_MGR, U_GONE, U_ADMIN, U_CEO = 41, 42, 43, 44


@pytest.fixture
def client():
    app.config['TESTING'] = True
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
    for t in ('branches', 'users', 'user_branches', 'manager_billing',
              'user_events'):
        conn.execute(f'DELETE FROM {t}')
    conn.execute(
        "INSERT INTO branches (id, name, city, active) VALUES (?, 'המכולת אינשטיין', 'חיפה', 1)",
        (BRANCH,))
    pw = generate_password_hash('test123')
    for uid, email, role in [
        (U_MGR, 'mgr@test.com', 'manager'),
        (U_GONE, 'gone@test.com', 'manager'),
        (U_ADMIN, 'admin@test.com', 'admin'),
        (U_CEO, 'ceo@test.com', 'ceo'),
    ]:
        conn.execute(
            "INSERT INTO users (id, name, email, password_hash, role, active) "
            "VALUES (?, ?, ?, ?, ?, 1)", (uid, f'user{uid}', email, pw, role))
        if role == 'manager':
            conn.execute(
                'INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)',
                (uid, BRANCH))
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def _exec(sql, params=()):
    conn = sqlite3.connect(TEST_DB, timeout=30)
    conn.execute(sql, params)
    conn.commit()
    conn.close()


def _events(uid):
    conn = sqlite3.connect(TEST_DB, timeout=30)
    n = conn.execute('SELECT COUNT(*) FROM user_events WHERE user_id=?',
                     (uid,)).fetchone()[0]
    conn.close()
    return n


def _login(client, email):
    return client.post('/login', data={'email': email, 'password': 'test123'})


def _deactivate(uid):
    _exec('UPDATE users SET active=0 WHERE id=?', (uid,))


def _assert_logged_out(client):
    with client.session_transaction() as s:
        assert 'user_id' not in s


# ── active users unaffected ─────────────────────────────────────

def test_active_manager_pages_and_api(client):
    assert _login(client, 'mgr@test.com').status_code == 302
    assert client.get('/').status_code == 200
    assert client.get('/api/summary').status_code == 200
    with client.session_transaction() as s:
        assert s['user_id'] == U_MGR


@pytest.mark.parametrize('email,path', [
    ('admin@test.com', '/ops'),
    ('ceo@test.com', '/'),
])
def test_admin_and_ceo_unaffected(client, email, path):
    assert _login(client, email).status_code == 302
    assert client.get(path).status_code == 200
    assert client.get('/api/summary').status_code == 200


# ── deactivated mid-session ─────────────────────────────────────

def test_deactivated_page_redirects_to_login(client):
    _login(client, 'mgr@test.com')
    _deactivate(U_MGR)
    resp = client.get('/')
    assert resp.status_code == 302
    assert resp.headers['Location'].endswith('/login')
    _assert_logged_out(client)


def test_deactivated_api_returns_401(client):
    _login(client, 'mgr@test.com')
    _deactivate(U_MGR)
    resp = client.get('/api/summary')
    assert resp.status_code == 401
    assert resp.get_json()['redirect'] == '/login'
    _assert_logged_out(client)


def test_deactivated_json_post_returns_401(client):
    _login(client, 'mgr@test.com')
    _deactivate(U_MGR)
    resp = client.post('/api/summary', json={})
    assert resp.status_code == 401


def test_blocked_request_writes_no_page_view(client):
    _login(client, 'mgr@test.com')
    _deactivate(U_MGR)
    before = _events(U_MGR)
    client.get('/')
    client.get('/sales')
    assert _events(U_MGR) == before


def test_session_stays_dead_after_block(client):
    _login(client, 'mgr@test.com')
    _deactivate(U_MGR)
    client.get('/')
    # even after reactivation the old cookie is gone — must log in again
    _exec('UPDATE users SET active=1 WHERE id=?', (U_MGR,))
    resp = client.get('/')
    assert resp.status_code == 302
    assert resp.headers['Location'].endswith('/login')


def test_deleted_user_mid_session_logged_out(client):
    _login(client, 'gone@test.com')
    _exec('DELETE FROM user_branches WHERE user_id=?', (U_GONE,))
    _exec('DELETE FROM users WHERE id=?', (U_GONE,))
    resp = client.get('/')
    assert resp.status_code == 302
    assert resp.headers['Location'].endswith('/login')
    _assert_logged_out(client)
    # API path for a fresh session pointing at the deleted id
    with client.session_transaction() as s:
        s['user_id'] = U_GONE
        s['user_role'] = 'manager'
    assert client.get('/api/summary').status_code == 401


# ── login ────────────────────────────────────────────────────────

def test_deactivated_cannot_log_in(client):
    _deactivate(U_MGR)
    resp = _login(client, 'mgr@test.com')
    assert resp.status_code == 200          # login page re-rendered with error
    _assert_logged_out(client)


def test_reactivated_can_log_in_again(client):
    _deactivate(U_MGR)
    _exec('UPDATE users SET active=1 WHERE id=?', (U_MGR,))
    assert _login(client, 'mgr@test.com').status_code == 302
    assert client.get('/').status_code == 200


# ── exempt routes ────────────────────────────────────────────────

@pytest.mark.parametrize('path', ['/login', '/health', '/forgot-password',
                                  '/reset-password', '/static/manifest.json'])
def test_exempt_routes_reachable_for_inactive_session(client, path):
    with client.session_transaction() as s:
        s['user_id'] = U_MGR
        s['user_role'] = 'manager'
    _deactivate(U_MGR)
    resp = client.get(path)
    assert resp.status_code != 401
    # exempt paths never run the check, so they never clear the session
    with client.session_transaction() as s:
        assert s.get('user_id') == U_MGR


def test_logout_reachable(client):
    _login(client, 'mgr@test.com')
    _deactivate(U_MGR)
    resp = client.get('/logout')
    assert resp.status_code == 302
    assert resp.headers['Location'].endswith('/login')


# ── fail-open ────────────────────────────────────────────────────

class _FailingActiveLookup:
    """Proxy connection: only the active-check query raises."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, *a, **kw):
        if sql.startswith('SELECT active FROM users WHERE id'):
            raise sqlite3.OperationalError('database is locked')
        return self._conn.execute(sql, *a, **kw)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def test_db_error_in_lookup_fails_open(client, monkeypatch):
    _login(client, 'mgr@test.com')
    _deactivate(U_MGR)    # would block — but the lookup itself fails
    real_get_db = app_module.get_db
    monkeypatch.setattr(app_module, 'get_db',
                        lambda: _FailingActiveLookup(real_get_db()))
    assert client.get('/api/summary').status_code == 200
    with client.session_transaction() as s:
        assert s['user_id'] == U_MGR

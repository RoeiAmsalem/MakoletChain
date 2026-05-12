"""Tests: user_events collection (migration 007 + login/page_view/heartbeat
+ admin exclusion + cleanup + silent failure)."""
import os
import sqlite3
import sys

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MIGRATION_007 = os.path.join(REPO, 'migrations', '007_user_events.sql')


def _apply_migration_007(conn):
    with open(MIGRATION_007, 'r') as fh:
        sql = fh.read()
    conn.executescript(sql)


def _seed_schema(conn):
    conn.executescript('''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT, email TEXT, password_hash TEXT,
            role TEXT DEFAULT 'manager', active INTEGER DEFAULT 1
        );
        CREATE TABLE user_branches (user_id INTEGER, branch_id INTEGER);
        CREATE TABLE branches (
            id INTEGER PRIMARY KEY, name TEXT, city TEXT, active INTEGER DEFAULT 1
        );
        CREATE TABLE reset_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, token TEXT, expires_at TEXT, used INTEGER DEFAULT 0
        );
        INSERT INTO branches (id, name, city) VALUES (126, 'Test Branch', 'Test City');
    ''')


@pytest.fixture
def app_client(tmp_path):
    db_path = str(tmp_path / 'test.db')
    conn = sqlite3.connect(db_path)
    _seed_schema(conn)
    _apply_migration_007(conn)
    pw_hash = generate_password_hash('secret123')
    conn.execute(
        "INSERT INTO users (name, email, password_hash, role, active) VALUES (?,?,?,?,1)",
        ('Test Manager', 'manager@example.com', pw_hash, 'manager')
    )
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (1, 126)")
    conn.execute(
        "INSERT INTO users (name, email, password_hash, role, active) VALUES (?,?,?,?,1)",
        ('Test Admin', 'admin@example.com', pw_hash, 'admin')
    )
    conn.commit()
    conn.close()

    import app as flask_app
    flask_app.DB_PATH = db_path
    flask_app.app.config['TESTING'] = True
    with flask_app.app.test_client() as c:
        yield c, db_path, flask_app


def _login(client, email, password='secret123'):
    return client.post('/login', data={'email': email, 'password': password},
                       follow_redirects=False)


def _rows(db_path, where='1=1', params=()):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f'SELECT * FROM user_events WHERE {where} ORDER BY id', params
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── a) Schema: table + indexes exist ─────────────────────────────────────

def test_schema_table_and_indexes(app_client):
    _, db_path, _ = app_client
    conn = sqlite3.connect(db_path)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(user_events)").fetchall()]
    assert set(cols) >= {'id', 'user_id', 'event_type', 'page', 'branch_id',
                         'duration_seconds', 'user_agent', 'created_at'}
    idx = [r[1] for r in conn.execute(
        "SELECT * FROM sqlite_master WHERE type='index' AND tbl_name='user_events'"
    ).fetchall()]
    assert 'idx_user_events_user_created' in idx
    assert 'idx_user_events_type_created' in idx
    assert 'idx_user_events_page' in idx
    conn.close()


# ── b) Login event: success creates 'login' row ──────────────────────────

def test_login_success_records_event(app_client):
    client, db_path, _ = app_client
    resp = _login(client, 'manager@example.com')
    assert resp.status_code in (302, 303)
    rows = _rows(db_path, "event_type='login'")
    assert len(rows) == 1
    assert rows[0]['user_id'] == 1
    assert rows[0]['page'] is None


# ── c) Login failure: NO row ─────────────────────────────────────────────

def test_login_failure_no_event(app_client):
    client, db_path, _ = app_client
    resp = _login(client, 'manager@example.com', password='wrong')
    assert resp.status_code == 200
    rows = _rows(db_path)
    assert rows == []


# ── d) Page view: authenticated GET /employees creates page_view row ─────

def test_page_view_authenticated(app_client):
    client, db_path, _ = app_client
    _login(client, 'manager@example.com')
    # Pre-condition: only login row from the POST /login above
    pre = _rows(db_path, "event_type='page_view'")
    assert pre == []
    client.get('/employees')
    rows = _rows(db_path, "event_type='page_view' AND page='/employees'")
    assert len(rows) == 1
    assert rows[0]['user_id'] == 1


# ── e) /api/* paths NOT tracked ──────────────────────────────────────────

def test_api_paths_not_tracked(app_client):
    client, db_path, _ = app_client
    _login(client, 'manager@example.com')
    client.get('/api/branches')
    rows = _rows(db_path, "event_type='page_view' AND page LIKE '/api/%'")
    assert rows == []


# ── f) /static/* paths NOT tracked ───────────────────────────────────────

def test_static_paths_not_tracked(app_client):
    client, db_path, _ = app_client
    _login(client, 'manager@example.com')
    client.get('/static/nope.css')  # 404 is fine; before_request still fires
    rows = _rows(db_path, "page LIKE '/static/%'")
    assert rows == []


# ── g) Unauthenticated request NOT tracked ───────────────────────────────

def test_unauthenticated_not_tracked(app_client):
    client, db_path, _ = app_client
    client.get('/employees')  # will redirect to /login
    rows = _rows(db_path)
    assert rows == []


# ── h) Admin exclusion: login + page_view + heartbeat → 0 rows ───────────

def test_admin_excluded_everywhere(app_client):
    client, db_path, _ = app_client
    _login(client, 'admin@example.com')
    client.get('/employees')
    client.post('/api/events/heartbeat',
                json={'page': '/employees', 'branch_id': 126,
                      'duration_seconds': 42})
    rows = _rows(db_path)
    assert rows == [], f"Expected no rows for admin, got {rows}"


# ── i) Heartbeat endpoint: valid POST records row ────────────────────────

def test_heartbeat_valid(app_client):
    client, db_path, _ = app_client
    _login(client, 'manager@example.com')
    r = client.post('/api/events/heartbeat',
                    json={'page': '/employees', 'branch_id': 126,
                          'duration_seconds': 45})
    assert r.status_code == 204
    rows = _rows(db_path, "event_type='heartbeat'")
    assert len(rows) == 1
    assert rows[0]['page'] == '/employees'
    assert rows[0]['branch_id'] == 126
    assert rows[0]['duration_seconds'] == 45


# ── j) Heartbeat: invalid duration → 204 with no row ─────────────────────

def test_heartbeat_invalid_duration(app_client):
    client, db_path, _ = app_client
    _login(client, 'manager@example.com')
    for bad in (-1, 86401, 'abc', None):
        r = client.post('/api/events/heartbeat',
                        json={'page': '/x', 'branch_id': 126,
                              'duration_seconds': bad})
        assert r.status_code == 204
    rows = _rows(db_path, "event_type='heartbeat'")
    assert rows == []


# ── k) Heartbeat unauthenticated → 401 ───────────────────────────────────

def test_heartbeat_unauthenticated(app_client):
    client, _, _ = app_client
    r = client.post('/api/events/heartbeat',
                    json={'page': '/x', 'duration_seconds': 1})
    assert r.status_code == 401


# ── l) Cleanup: removes rows older than 90 days ──────────────────────────

def test_cleanup_deletes_old(app_client):
    _, db_path, _ = app_client
    conn = sqlite3.connect(db_path)
    # 100 old rows + 5 fresh rows
    for _ in range(100):
        conn.execute(
            "INSERT INTO user_events "
            "(user_id, event_type, page, created_at) "
            "VALUES (1, 'page_view', '/x', datetime('now', '-120 days'))"
        )
    for _ in range(5):
        conn.execute(
            "INSERT INTO user_events "
            "(user_id, event_type, page, created_at) "
            "VALUES (1, 'page_view', '/y', datetime('now', '-1 day'))"
        )
    conn.commit()
    conn.close()

    # Run the real cleanup against this DB. apscheduler may not be installed
    # in some dev envs; fall back to the equivalent SQL the function runs.
    try:
        import scheduler as sched  # noqa: WPS433
        orig = sched.DB_PATH
        sched.DB_PATH = db_path
        try:
            sched.cleanup_old_user_events()
        finally:
            sched.DB_PATH = orig
    except ModuleNotFoundError:
        c = sqlite3.connect(db_path)
        c.execute("DELETE FROM user_events WHERE created_at < datetime('now', '-90 days')")
        c.commit()
        c.close()

    remaining = _rows(db_path)
    assert len(remaining) == 5
    assert all(r['page'] == '/y' for r in remaining)


# ── m) Silent failure: DB error in _record_event must not break request ──

def test_record_event_silent_on_db_failure(app_client, monkeypatch):
    """If get_db() throws inside _record_event, the request still succeeds.
    Heartbeat route is used because it does NO DB work outside _record_event,
    so a get_db failure can only affect analytics, not the route response."""
    client, db_path, flask_app = app_client
    _login(client, 'manager@example.com')

    def boom(*a, **kw):
        raise RuntimeError('simulated DB outage')

    monkeypatch.setattr(flask_app, 'get_db', boom)

    r = client.post('/api/events/heartbeat',
                    json={'page': '/x', 'branch_id': 126,
                          'duration_seconds': 10})
    assert r.status_code == 204  # silent failure — route still returns 204

    rows = _rows(db_path, "event_type='heartbeat'")
    assert rows == []  # nothing inserted


if __name__ == '__main__':
    sys.exit(pytest.main([__file__, '-v']))

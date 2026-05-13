"""Tests for the home page (/) — verifies the unknown-employees banner
partial is rendered on both / and /employees so the two pages stay in sync."""
import os
import sys
import sqlite3
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, DB_PATH

# The banner shell is server-rendered (display:none); JS toggles visibility
# from /api/employee-match-pending. Both pages must include the same partial,
# so we assert the partial's static markup is present in the HTML response.
BANNER_TEXT = 'עובדים חדשים לא מזוהים'
BANNER_ID = 'id="unknown-banner"'


@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_makolet_home.db')
    original_db = DB_PATH

    import app as app_module
    app_module.DB_PATH = test_db

    if os.path.exists(test_db):
        os.remove(test_db)
    app_module.init_db()
    conn = sqlite3.connect(test_db, timeout=30)

    for col_sql in [
        "ALTER TABLE branches ADD COLUMN hours_this_month REAL DEFAULT 0",
        "ALTER TABLE branches ADD COLUMN hours_baseline REAL DEFAULT 0",
        "ALTER TABLE branches ADD COLUMN hours_updated_at TEXT",
        "ALTER TABLE branches ADD COLUMN avg_hourly_rate REAL DEFAULT 0",
        "ALTER TABLE branches ADD COLUMN bilboy_user TEXT",
        "ALTER TABLE branches ADD COLUMN bilboy_pass TEXT",
    ]:
        try:
            conn.execute(col_sql)
        except sqlite3.OperationalError:
            pass

    conn.execute("INSERT OR REPLACE INTO branches (id, name, city, active) VALUES (126, 'איינשטיין', 'תל אביב', 1)")
    from werkzeug.security import generate_password_hash
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) VALUES (1, 'Manager', 'mgr@test.com', ?, 'manager', 1)",
        (generate_password_hash('test123'),))
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (1, 126)")
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(test_db):
        os.remove(test_db)


def _login(client, email='mgr@test.com', password='test123'):
    return client.post('/login', data={'email': email, 'password': password}, follow_redirects=True)


def test_home_includes_unknown_employees_banner_partial(client):
    """GET / as a logged-in manager renders the partial's banner shell."""
    _login(client)
    res = client.get('/')
    assert res.status_code == 200
    body = res.data.decode('utf-8')
    assert BANNER_ID in body, '/ must include the unknown-employees banner partial'
    assert BANNER_TEXT in body, '/ must contain the banner heading text'


def test_employees_still_includes_unknown_employees_banner_partial(client):
    """Refactor must not regress /employees — the partial is still included."""
    _login(client)
    res = client.get('/employees')
    assert res.status_code == 200
    body = res.data.decode('utf-8')
    assert BANNER_ID in body, '/employees must still include the partial after refactor'
    assert BANNER_TEXT in body, '/employees must still contain the banner heading text'

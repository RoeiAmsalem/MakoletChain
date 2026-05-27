"""Tests: email case-insensitive login + lookups."""
import os, sys, sqlite3, tempfile, secrets
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from werkzeug.security import generate_password_hash

@pytest.fixture
def client(tmp_path):
    db_path = str(tmp_path / 'test.db')
    conn = sqlite3.connect(db_path)
    conn.executescript('''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT, email TEXT, password_hash TEXT,
            role TEXT DEFAULT 'manager', active INTEGER DEFAULT 1
        );
        CREATE TABLE user_branches (user_id INTEGER, branch_id INTEGER);
        CREATE TABLE branches (
            id INTEGER PRIMARY KEY, name TEXT, city TEXT, active INTEGER DEFAULT 1,
            aviv_user_id TEXT DEFAULT '', aviv_password TEXT DEFAULT '',
            aviv_branch_id INTEGER,
            bilboy_user TEXT DEFAULT '', bilboy_pass TEXT DEFAULT '',
            gmail_label TEXT DEFAULT '', franchise_supplier TEXT DEFAULT '',
            iec_contract TEXT DEFAULT '', avg_hourly_rate REAL DEFAULT 0,
            hours_this_month REAL DEFAULT 0, hours_baseline REAL DEFAULT 0,
            hours_updated_at TEXT DEFAULT ''
        );
        CREATE TABLE reset_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, token TEXT, expires_at TEXT, used INTEGER DEFAULT 0
        );
        CREATE TABLE daily_sales (id INTEGER PRIMARY KEY, branch_id INTEGER, date TEXT, amount REAL, transactions INTEGER, source TEXT);
        CREATE TABLE goods_documents (id INTEGER PRIMARY KEY, branch_id INTEGER, ref_number TEXT, supplier_id TEXT, supplier_name TEXT, doc_type TEXT, doc_date TEXT, amount REAL, month TEXT);
        CREATE TABLE fixed_expenses (id INTEGER PRIMARY KEY, branch_id INTEGER, name TEXT, amount REAL, expense_type TEXT, month TEXT, pct_value REAL DEFAULT 0);
        CREATE TABLE employee_hours (id INTEGER PRIMARY KEY, branch_id INTEGER, month TEXT, employee_name TEXT, total_hours REAL, total_salary REAL, source TEXT);
        CREATE TABLE agent_runs (id INTEGER PRIMARY KEY, branch_id INTEGER, agent TEXT, started_at TEXT, finished_at TEXT, status TEXT, docs_count INTEGER, amount REAL, message TEXT, duration_seconds REAL, dismissed INTEGER DEFAULT 0);
        INSERT INTO branches (id, name, city, aviv_branch_id) VALUES (126, 'Test Branch', 'Test City', 3);
        INSERT INTO branches (id, name, city, aviv_branch_id) VALUES (9050, 'Chain Store 50', '', 50);
    ''')
    pw_hash = generate_password_hash('secret123')
    conn.execute(
        "INSERT INTO users (name, email, password_hash, role, active) VALUES (?,?,?,?,1)",
        ('Test User', 'testuser@example.com', pw_hash, 'manager')
    )
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (1, 126)")
    conn.commit()
    conn.close()

    os.environ['DATABASE_URL'] = db_path
    import app as flask_app
    flask_app.DB_PATH = db_path
    flask_app.app.config['TESTING'] = True
    flask_app.app.config['WTF_CSRF_ENABLED'] = False
    with flask_app.app.test_client() as c:
        yield c


def _login(client, email, password='secret123'):
    return client.post('/login', data={'email': email, 'password': password}, follow_redirects=False)


def test_login_lowercase(client):
    resp = _login(client, 'testuser@example.com')
    assert resp.status_code in (302, 303), f"Expected redirect, got {resp.status_code}"


def test_login_mixed_case(client):
    resp = _login(client, 'TestUser@Example.COM')
    assert resp.status_code in (302, 303), f"Expected redirect, got {resp.status_code}"


def test_login_all_caps(client):
    resp = _login(client, 'TESTUSER@EXAMPLE.COM')
    assert resp.status_code in (302, 303), f"Expected redirect, got {resp.status_code}"


def test_login_wrong_password(client):
    resp = _login(client, 'testuser@example.com', password='wrongpass')
    assert resp.status_code == 200  # stays on login page
    assert 'שגויים' in resp.data.decode('utf-8')


def test_forgot_password_mixed_case(client):
    resp = client.post('/forgot-password', data={'email': 'TestUser@EXAMPLE.com'})
    assert resp.status_code == 200
    # Should show success message regardless (no user enumeration)
    body = resp.data.decode('utf-8')
    assert 'נשלח' in body or 'אימייל' in body


def test_admin_branch_enrich_stores_email_lowercase(client, tmp_path):
    """When enriching a chain branch with a manager, email is stored lowercase."""
    import app as flask_app
    db_path = flask_app.DB_PATH
    conn = sqlite3.connect(db_path)
    # Create admin user for auth
    pw_hash = generate_password_hash('admin123')
    conn.execute(
        "INSERT INTO users (name, email, password_hash, role, active) VALUES (?,?,?,?,1)",
        ('Admin', 'makoletdashboard@gmail.com', pw_hash, 'admin')
    )
    conn.commit()
    branches_before = conn.execute("SELECT COUNT(*) FROM branches").fetchone()[0]
    conn.close()

    # Login as admin
    _login(client, 'makoletdashboard@gmail.com', 'admin123')

    # Enrich the autoseed chain store 9050 (aviv_branch_id=50) with a
    # mixed-case manager email.
    resp = client.post('/api/admin/branches', json={
        'branch_id': 9050, 'city': 'Test',
        'manager_name': 'Manager', 'manager_email': 'NewManager@EXAMPLE.COM'
    })
    assert resp.status_code == 200, resp.data
    data = resp.get_json()
    assert data.get('ok')
    assert data.get('branch_id') == 9050

    conn = sqlite3.connect(db_path)
    # Collision guard: no new branch row was inserted.
    branches_after = conn.execute("SELECT COUNT(*) FROM branches").fetchone()[0]
    assert branches_after == branches_before, "enrich must NOT create new branches row"
    # Email stored lowercase.
    row = conn.execute("SELECT email FROM users WHERE name='Manager'").fetchone()
    conn.close()
    assert row is not None, "Manager user not created"
    assert row[0] == 'newmanager@example.com', f"Email not lowercase: {row[0]}"


def test_admin_branch_enrich_rejects_unknown_branch(client):
    """POST /api/admin/branches with missing/invalid branch_id is rejected."""
    import app as flask_app
    db_path = flask_app.DB_PATH
    conn = sqlite3.connect(db_path)
    pw_hash = generate_password_hash('admin123')
    conn.execute(
        "INSERT OR IGNORE INTO users (name, email, password_hash, role, active) VALUES (?,?,?,?,1)",
        ('Admin', 'makoletdashboard@gmail.com', pw_hash, 'admin')
    )
    conn.commit()
    conn.close()
    _login(client, 'makoletdashboard@gmail.com', 'admin123')

    # No branch_id at all → 400.
    resp = client.post('/api/admin/branches', json={'city': 'Whatever'})
    assert resp.status_code == 400

    # Unknown branch_id → 404.
    resp = client.post('/api/admin/branches', json={'branch_id': 999999})
    assert resp.status_code == 404


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

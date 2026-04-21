"""Tests: Multi-branch manager — branch switcher + filtered /api/branches.

Run: python scripts/test_multi_branch_manager.py
"""
import os
import sys
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault('SECRET_KEY', 'test-secret')

# Create temp DB BEFORE importing app — app calls init_db() + seed_admin() at import
_tmpdir = tempfile.mkdtemp()
_db_path = os.path.join(_tmpdir, 'test.db')

# Patch DB_PATH in the module dict before app module body runs
import importlib
import types

# Pre-set the DB path so init_db() uses our temp DB
_proj_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_schema_path = os.path.join(_proj_root, 'db', 'schema.sql')

# We need to trick the import. Patch os.path.join result for DB_PATH.
# Simplest: just set env and patch after import.
import app as app_module
app_module.DB_PATH = _db_path
app_module.SCHEMA_PATH = _schema_path
app_module.init_db()

from app import app, get_db
from werkzeug.security import generate_password_hash

PASS = 0
FAIL = 0


def result(name, ok, detail=''):
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  PASS: {name}")
    else:
        FAIL += 1
        print(f"  FAIL: {name} — {detail}")


def setup_test_db():
    """Seed test users: admin, single-branch manager, multi-branch manager."""
    with app.app_context():
        db = get_db()

        # Clear seed data from init_db/seed_admin and re-seed with test data
        db.execute("DELETE FROM user_branches")
        db.execute("DELETE FROM users")
        db.execute("DELETE FROM branches")

        # Branches
        db.execute("INSERT INTO branches (id, name, city) VALUES (126, 'איינשטיין', 'כפר סבא')")
        db.execute("INSERT INTO branches (id, name, city) VALUES (127, 'התיכון', 'כפר סבא')")
        db.execute("INSERT INTO branches (id, name, city) VALUES (128, 'צפון', 'נתניה')")

        pw = generate_password_hash('test123')

        # Admin (CEO)
        db.execute("INSERT INTO users (id, name, email, password_hash, role) VALUES (1, 'Admin', 'admin@test.com', ?, 'admin')", (pw,))
        db.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (1, 126)")

        # Single-branch manager
        db.execute("INSERT INTO users (id, name, email, password_hash, role) VALUES (2, 'Single', 'single@test.com', ?, 'manager')", (pw,))
        db.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (2, 126)")

        # Multi-branch manager (2 branches)
        db.execute("INSERT INTO users (id, name, email, password_hash, role) VALUES (3, 'Multi', 'multi@test.com', ?, 'manager')", (pw,))
        db.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (3, 126)")
        db.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (3, 127)")

        db.commit()


def cleanup():
    if _tmpdir and os.path.exists(_tmpdir):
        shutil.rmtree(_tmpdir)


def login(client, email):
    return client.post('/login', data={'email': email, 'password': 'test123'}, follow_redirects=False)


def test_single_branch_manager_no_switcher():
    """Single-branch manager should NOT see branch-select."""
    print("\n── Single-branch manager ──")
    with app.test_client() as c:
        login(c, 'single@test.com')
        resp = c.get('/')
        html = resp.data.decode()
        result('no branch-select', 'id="branch-select"' not in html)

        resp = c.get('/api/branches')
        branches = resp.get_json()
        result('/api/branches returns 1 branch', len(branches) == 1, f'got {len(branches)}')
        result('branch is 126', branches[0]['id'] == 126 if branches else False)


def test_multi_branch_manager_has_switcher():
    """Multi-branch manager should see branch-select with their 2 branches."""
    print("\n── Multi-branch manager ──")
    with app.test_client() as c:
        login(c, 'multi@test.com')
        resp = c.get('/')
        html = resp.data.decode()
        result('branch-select present', 'id="branch-select"' in html)

        resp = c.get('/api/branches')
        branches = resp.get_json()
        result('/api/branches returns 2 branches', len(branches) == 2, f'got {len(branches)}')
        ids = [b['id'] for b in branches]
        result('branches are 126,127', ids == [126, 127], f'got {ids}')

        # Branch 128 must NOT appear
        result('branch 128 excluded', 128 not in ids)


def test_admin_sees_all():
    """Admin should see branch-select and ALL branches."""
    print("\n── Admin ──")
    with app.test_client() as c:
        login(c, 'admin@test.com')
        resp = c.get('/')
        html = resp.data.decode()
        result('branch-select present', 'id="branch-select"' in html)

        resp = c.get('/api/branches')
        branches = resp.get_json()
        result('/api/branches returns all 3', len(branches) == 3, f'got {len(branches)}')


def test_multi_branch_cannot_access_unassigned():
    """Multi-branch manager cannot switch to branch 128 (not assigned)."""
    print("\n── Branch access validation ──")
    with app.test_client() as c:
        login(c, 'multi@test.com')
        # Try to switch to unassigned branch 128
        c.get('/?branch_id=128')
        with c.session_transaction() as sess:
            result('session branch_id != 128', sess.get('branch_id') != 128, f"got {sess.get('branch_id')}")
            result('session branch_id is 126', sess.get('branch_id') == 126, f"got {sess.get('branch_id')}")


def test_multi_branch_can_switch_to_assigned():
    """Multi-branch manager CAN switch to branch 127 (assigned)."""
    print("\n── Branch switching ──")
    with app.test_client() as c:
        login(c, 'multi@test.com')
        c.get('/?branch_id=127')
        with c.session_transaction() as sess:
            result('switched to 127', sess.get('branch_id') == 127, f"got {sess.get('branch_id')}")


def test_multi_branch_ops_forbidden():
    """Multi-branch manager cannot access /ops (admin-only)."""
    print("\n── Admin-only routes ──")
    with app.test_client() as c:
        login(c, 'multi@test.com')
        resp = c.get('/ops')
        result('/ops returns 403', resp.status_code == 403, f'got {resp.status_code}')


if __name__ == '__main__':
    app.config['TESTING'] = True
    app.config['WTF_CSRF_ENABLED'] = False

    try:
        setup_test_db()
        test_single_branch_manager_no_switcher()
        test_multi_branch_manager_has_switcher()
        test_admin_sees_all()
        test_multi_branch_cannot_access_unassigned()
        test_multi_branch_can_switch_to_assigned()
        test_multi_branch_ops_forbidden()
    finally:
        cleanup()

    print(f"\n{'='*40}")
    print(f"Results: {PASS} passed, {FAIL} failed")
    if FAIL:
        sys.exit(1)
    else:
        print("All tests passed!")

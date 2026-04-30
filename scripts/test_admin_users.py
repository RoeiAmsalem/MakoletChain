"""Tests for /api/admin/users endpoints."""
import os
import sys
import tempfile
import shutil
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from werkzeug.security import generate_password_hash


def setup_test_db():
    """Create a temp copy of the DB with test data."""
    src = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'db', 'makolet_chain.db')
    tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    tmp.close()
    shutil.copy2(src, tmp.name)
    return tmp.name


def get_app(db_path):
    """Import and configure the Flask app to use a test DB."""
    os.environ['DATABASE_PATH'] = db_path
    import app as app_module
    app_module.app.config['TESTING'] = True
    app_module.app.config['SERVER_NAME'] = 'localhost'
    app_module.DB_PATH = db_path
    # Patch get_db to use test DB
    original_get_db = app_module.get_db

    def patched_get_db():
        if 'db' not in app_module.g.__dict__ and not hasattr(app_module.g, '_db_set'):
            db = sqlite3.connect(db_path)
            db.row_factory = sqlite3.Row
            app_module.g.db = db
            app_module.g._db_set = True
            return db
        return original_get_db()

    app_module.get_db = patched_get_db
    return app_module.app


def login(client, email, role='admin'):
    """Login helper — sets session directly."""
    with client.session_transaction() as sess:
        # Find user by email in DB
        sess['user_id'] = 1 if role == 'admin' else 2
        sess['user_name'] = 'Test Admin' if role == 'admin' else 'Test Manager'
        sess['user_role'] = role
        sess['user_branches'] = [126]
        sess['branch_id'] = 126


def run_tests():
    db_path = setup_test_db()
    passed = 0
    failed = 0
    errors = []

    try:
        app = get_app(db_path)

        with app.test_client() as client:
            # Ensure test data exists
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            # Ensure test data exists — create if missing
            pw_hash = generate_password_hash('test123')

            # Ensure admin user
            admin_user = conn.execute("SELECT * FROM users WHERE role='admin' LIMIT 1").fetchone()
            if not admin_user:
                conn.execute("INSERT INTO users (name, email, password_hash, role, active) VALUES (?,?,?,?,1)",
                             ('Test Admin', 'admin@test.com', pw_hash, 'admin'))
                conn.commit()
                admin_user = conn.execute("SELECT * FROM users WHERE role='admin' LIMIT 1").fetchone()

            # Ensure manager user
            manager_user = conn.execute("SELECT * FROM users WHERE role='manager' LIMIT 1").fetchone()
            if not manager_user:
                conn.execute("INSERT INTO users (name, email, password_hash, role, active) VALUES (?,?,?,?,1)",
                             ('Test Manager', 'manager@test.com', pw_hash, 'manager'))
                conn.commit()
                manager_user = conn.execute("SELECT * FROM users WHERE role='manager' LIMIT 1").fetchone()

            # Ensure at least 2 branches
            branches = conn.execute("SELECT id FROM branches ORDER BY id").fetchall()
            branch_ids = [r['id'] for r in branches]
            if len(branch_ids) < 2:
                for bid in [126, 127]:
                    conn.execute("INSERT OR IGNORE INTO branches (id, name, city, active) VALUES (?,?,?,1)",
                                 (bid, f'Test Branch {bid}', 'Test City'))
                conn.commit()
                branches = conn.execute("SELECT id FROM branches ORDER BY id").fetchall()
                branch_ids = [r['id'] for r in branches]

            # Ensure manager has at least 1 branch
            existing = conn.execute("SELECT 1 FROM user_branches WHERE user_id=?", (manager_user['id'],)).fetchone()
            if not existing:
                conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (?,?)",
                             (manager_user['id'], branch_ids[0]))
                conn.commit()

            admin_id = admin_user['id']
            manager_id = manager_user['id']

            # ── Test 1: GET /api/admin/users as admin → 200 ──
            try:
                login(client, 'admin', 'admin')
                with client.session_transaction() as s:
                    s['user_id'] = admin_id
                resp = client.get('/api/admin/users')
                assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
                data = resp.get_json()
                assert isinstance(data, list), "Expected list response"
                assert len(data) > 0, "Expected at least one user"
                assert 'branches' in data[0], "Expected branches field"
                print(f"PASS: GET /api/admin/users as admin → 200, {len(data)} users")
                passed += 1
            except Exception as e:
                print(f"FAIL: GET /api/admin/users as admin — {e}")
                errors.append(str(e))
                failed += 1

            # ── Test 2: GET /api/admin/users as manager → 403 ──
            try:
                login(client, 'manager', 'manager')
                with client.session_transaction() as s:
                    s['user_id'] = manager_id
                resp = client.get('/api/admin/users')
                assert resp.status_code == 403, f"Expected 403, got {resp.status_code}"
                print("PASS: GET /api/admin/users as manager → 403")
                passed += 1
            except Exception as e:
                print(f"FAIL: GET /api/admin/users as manager — {e}")
                errors.append(str(e))
                failed += 1

            # ── Test 3: POST add branch to manager → 201 ──
            try:
                login(client, 'admin', 'admin')
                with client.session_transaction() as s:
                    s['user_id'] = admin_id
                # Find a branch not yet assigned to manager
                assigned = conn.execute(
                    "SELECT branch_id FROM user_branches WHERE user_id=?", (manager_id,)
                ).fetchall()
                assigned_ids = {r['branch_id'] for r in assigned}
                unassigned = [bid for bid in branch_ids if bid not in assigned_ids]

                if unassigned:
                    test_branch = unassigned[0]
                    resp = client.post(f'/api/admin/users/{manager_id}/branches',
                                       json={'branch_id': test_branch},
                                       content_type='application/json')
                    assert resp.status_code == 201, f"Expected 201, got {resp.status_code}"
                    # Verify in DB
                    row = conn.execute(
                        "SELECT 1 FROM user_branches WHERE user_id=? AND branch_id=?",
                        (manager_id, test_branch)).fetchone()
                    assert row is not None, "Row not inserted in DB"
                    print(f"PASS: POST add branch {test_branch} to manager → 201")
                    passed += 1

                    # ── Test 4: POST duplicate → 409 ──
                    try:
                        resp = client.post(f'/api/admin/users/{manager_id}/branches',
                                           json={'branch_id': test_branch},
                                           content_type='application/json')
                        assert resp.status_code == 409, f"Expected 409, got {resp.status_code}"
                        print("PASS: POST duplicate branch → 409")
                        passed += 1
                    except Exception as e:
                        print(f"FAIL: POST duplicate — {e}")
                        errors.append(str(e))
                        failed += 1

                    # ── Test 5: DELETE non-last branch → 204 ──
                    try:
                        resp = client.delete(f'/api/admin/users/{manager_id}/branches/{test_branch}')
                        assert resp.status_code == 204, f"Expected 204, got {resp.status_code}"
                        print(f"PASS: DELETE non-last branch {test_branch} → 204")
                        passed += 1
                    except Exception as e:
                        print(f"FAIL: DELETE non-last branch — {e}")
                        errors.append(str(e))
                        failed += 1
                else:
                    print("SKIP: No unassigned branches to test add")
                    # Still run duplicate test on existing branch
                    existing_branch = list(assigned_ids)[0]
                    resp = client.post(f'/api/admin/users/{manager_id}/branches',
                                       json={'branch_id': existing_branch},
                                       content_type='application/json')
                    assert resp.status_code == 409
                    print("PASS: POST duplicate branch → 409")
                    passed += 1

            except Exception as e:
                print(f"FAIL: POST add branch — {e}")
                errors.append(str(e))
                failed += 1

            # ── Test 6: POST add branch to admin → 403 ──
            try:
                login(client, 'admin', 'admin')
                with client.session_transaction() as s:
                    s['user_id'] = admin_id
                resp = client.post(f'/api/admin/users/{admin_id}/branches',
                                   json={'branch_id': branch_ids[0]},
                                   content_type='application/json')
                assert resp.status_code == 403, f"Expected 403, got {resp.status_code}"
                print("PASS: POST add branch to admin user → 403")
                passed += 1
            except Exception as e:
                print(f"FAIL: POST add branch to admin — {e}")
                errors.append(str(e))
                failed += 1

            # ── Test 7: POST nonexistent user → 404 ──
            try:
                resp = client.post('/api/admin/users/99999/branches',
                                   json={'branch_id': branch_ids[0]},
                                   content_type='application/json')
                assert resp.status_code == 404, f"Expected 404, got {resp.status_code}"
                print("PASS: POST nonexistent user → 404")
                passed += 1
            except Exception as e:
                print(f"FAIL: POST nonexistent user — {e}")
                errors.append(str(e))
                failed += 1

            # ── Test 8: POST nonexistent branch → 404 ──
            try:
                resp = client.post(f'/api/admin/users/{manager_id}/branches',
                                   json={'branch_id': 99999},
                                   content_type='application/json')
                assert resp.status_code == 404, f"Expected 404, got {resp.status_code}"
                print("PASS: POST nonexistent branch → 404")
                passed += 1
            except Exception as e:
                print(f"FAIL: POST nonexistent branch — {e}")
                errors.append(str(e))
                failed += 1

            # ── Test 9: DELETE last branch of manager → 422 ──
            try:
                # Ensure manager has exactly 1 branch
                conn.execute("DELETE FROM user_branches WHERE user_id=?", (manager_id,))
                conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (?,?)",
                             (manager_id, branch_ids[0]))
                conn.commit()

                resp = client.delete(f'/api/admin/users/{manager_id}/branches/{branch_ids[0]}')
                assert resp.status_code == 422, f"Expected 422, got {resp.status_code}"
                err = resp.get_json()
                assert 'cannot leave manager' in err.get('error', ''), f"Unexpected error: {err}"
                print("PASS: DELETE last branch of manager → 422 (guardrail)")
                passed += 1
            except Exception as e:
                print(f"FAIL: DELETE last branch guardrail — {e}")
                errors.append(str(e))
                failed += 1

            # ── Test 10: GET /api/admin/branches-list → returns all branches ──
            try:
                login(client, 'admin', 'admin')
                with client.session_transaction() as s:
                    s['user_id'] = admin_id
                resp = client.get('/api/admin/branches-list')
                assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
                data = resp.get_json()
                assert isinstance(data, list)
                assert len(data) > 0
                assert 'id' in data[0] and 'name' in data[0]
                print(f"PASS: GET /api/admin/branches-list → 200, {len(data)} branches")
                passed += 1
            except Exception as e:
                print(f"FAIL: GET /api/admin/branches-list — {e}")
                errors.append(str(e))
                failed += 1

            conn.close()

    finally:
        os.unlink(db_path)

    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed")
    if errors:
        print("Errors:")
        for e in errors:
            print(f"  - {e}")
    return failed == 0


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)

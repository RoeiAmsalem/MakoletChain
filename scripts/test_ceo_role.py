"""Tests for the CEO role.

CEO sees every active branch automatically (no user_branches rows) and is blocked
from /ops + /admin/* surfaces. Admin and manager behavior must be unchanged.
"""
import os
import sys
import tempfile
import shutil
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from werkzeug.security import generate_password_hash


def setup_test_db():
    src = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'db', 'makolet_chain.db')
    tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    tmp.close()
    shutil.copy2(src, tmp.name)
    return tmp.name


def get_app(db_path):
    os.environ['DATABASE_PATH'] = db_path
    import app as app_module
    app_module.app.config['TESTING'] = True
    app_module.app.config['SERVER_NAME'] = 'localhost'
    app_module.DB_PATH = db_path
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


def login_as(client, user_id, role, user_branches=None):
    with client.session_transaction() as sess:
        sess['user_id'] = user_id
        sess['user_name'] = f'Test {role}'
        sess['user_role'] = role
        sess['user_branches'] = user_branches or []
        if user_branches:
            sess['branch_id'] = user_branches[0]


def ensure_seed(conn, role, email, name):
    row = conn.execute("SELECT id FROM users WHERE LOWER(email)=?", (email,)).fetchone()
    if row:
        return row['id']
    pw = generate_password_hash('test123')
    cur = conn.execute(
        "INSERT INTO users (name, email, password_hash, role, active) VALUES (?,?,?,?,1)",
        (name, email, pw, role))
    conn.commit()
    return cur.lastrowid


def run_tests():
    db_path = setup_test_db()
    passed = 0
    failed = 0
    failures = []

    def check(label, cond, detail=''):
        nonlocal passed, failed
        if cond:
            passed += 1
            print(f"PASS: {label}")
        else:
            failed += 1
            failures.append(f"{label} — {detail}")
            print(f"FAIL: {label} — {detail}")

    try:
        app = get_app(db_path)

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Ensure two active branches
        branches = conn.execute("SELECT id FROM branches WHERE active=1 ORDER BY id").fetchall()
        if len(branches) < 2:
            for bid in [126, 127]:
                conn.execute("INSERT OR IGNORE INTO branches (id, name, city, active) VALUES (?,?,?,1)",
                             (bid, f'Test Branch {bid}', 'Test'))
            conn.commit()
            branches = conn.execute("SELECT id FROM branches WHERE active=1 ORDER BY id").fetchall()
        branch_ids = [r['id'] for r in branches]

        admin_id = ensure_seed(conn, 'admin', 'admin-ceotest@test.com', 'Admin Tester')
        ceo_id = ensure_seed(conn, 'ceo', 'ceo-test@test.com', 'CEO Tester')
        manager_id = ensure_seed(conn, 'manager', 'mgr-ceotest@test.com', 'Manager Tester')
        # Manager gets only branch[0]
        conn.execute("DELETE FROM user_branches WHERE user_id=?", (manager_id,))
        conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (?,?)",
                     (manager_id, branch_ids[0]))
        # CEO has no user_branches rows (test (a))
        conn.execute("DELETE FROM user_branches WHERE user_id=?", (ceo_id,))
        conn.commit()

        # (a) CEO user has no user_branches rows
        rows = conn.execute("SELECT 1 FROM user_branches WHERE user_id=?", (ceo_id,)).fetchall()
        check("(a) CEO has no user_branches rows", len(rows) == 0,
              detail=f"expected 0, got {len(rows)}")

        with app.test_client() as client:
            # (b) CEO /api/branches returns all active branches
            login_as(client, ceo_id, 'ceo', user_branches=[])
            resp = client.get('/api/branches')
            check("(b) CEO /api/branches returns 200",
                  resp.status_code == 200, detail=f"status={resp.status_code}")
            data = resp.get_json() or []
            returned_ids = sorted([b['id'] for b in data])
            all_branch_ids = sorted([
                r['id'] for r in conn.execute("SELECT id FROM branches ORDER BY id").fetchall()
            ])
            check("(b) CEO sees all branches via /api/branches",
                  returned_ids == all_branch_ids,
                  detail=f"got {returned_ids}, expected {all_branch_ids}")

            # (c) Add a new branch and confirm CEO sees it on next call
            new_branch_id = max(all_branch_ids) + 1
            conn.execute("INSERT INTO branches (id, name, city, active) VALUES (?,?,?,1)",
                         (new_branch_id, 'CEO-Visibility-Test', 'TestCity'))
            conn.commit()
            check("(c) admin/test creates new branch in DB", True)

            login_as(client, ceo_id, 'ceo', user_branches=[])
            resp = client.get('/api/branches')
            data = resp.get_json() or []
            new_ids = [b['id'] for b in data]
            check("(c) CEO immediately sees newly created branch",
                  new_branch_id in new_ids,
                  detail=f"new={new_branch_id} not in {new_ids}")

            # (d) CEO hits /ops → 403
            resp = client.get('/ops')
            check("(d) CEO blocked from /ops",
                  resp.status_code in (302, 403),
                  detail=f"status={resp.status_code}")

            # (e) CEO hits /admin/users → 403
            resp = client.get('/admin/users')
            check("(e) CEO blocked from /admin/users",
                  resp.status_code in (302, 403),
                  detail=f"status={resp.status_code}")

            # (f) CEO hits /admin/branches → 403
            resp = client.get('/admin/branches')
            check("(f) CEO blocked from /admin/branches",
                  resp.status_code in (302, 403),
                  detail=f"status={resp.status_code}")

            # (g) CEO hits / (home) → 200
            resp = client.get('/')
            check("(g) CEO /home loads",
                  resp.status_code == 200, detail=f"status={resp.status_code}")

            # (h) CEO switches branch via ?branch_id for ALL branches
            ok_all = True
            for bid in all_branch_ids[:2]:
                resp = client.get(f'/?branch_id={bid}')
                if resp.status_code != 200:
                    ok_all = False
                    break
            check("(h) CEO can switch to any branch", ok_all)

            # (i) CEO accesses /employees?branch_id=X for any branch
            resp = client.get(f'/employees?branch_id={branch_ids[1]}')
            check("(i) CEO accesses /employees for any branch",
                  resp.status_code == 200, detail=f"status={resp.status_code}")

            # (j) Decorator rename: no _ceo_required orphan references
            with open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app.py')) as fh:
                src = fh.read()
            check("(j) no _ceo_required orphan refs in app.py",
                  '_ceo_required' not in src)
            check("(j) _admin_required exists in app.py",
                  'def _admin_required' in src)

            # (k) Admin behavior unchanged
            login_as(client, admin_id, 'admin', user_branches=[])
            resp = client.get('/ops')
            check("(k) admin still reaches /ops",
                  resp.status_code == 200, detail=f"status={resp.status_code}")
            resp = client.get('/admin/users')
            check("(k) admin still reaches /admin/users",
                  resp.status_code == 200, detail=f"status={resp.status_code}")
            resp = client.get('/api/branches')
            data = resp.get_json() or []
            check("(k) admin sees all branches",
                  sorted([b['id'] for b in data]) == sorted([
                      r['id'] for r in conn.execute("SELECT id FROM branches ORDER BY id").fetchall()
                  ]))

            # (l) Manager behavior unchanged
            login_as(client, manager_id, 'manager', user_branches=[branch_ids[0]])
            resp = client.get('/api/branches')
            data = resp.get_json() or []
            mgr_ids = [b['id'] for b in data]
            check("(l) manager sees only assigned branches",
                  mgr_ids == [branch_ids[0]],
                  detail=f"got {mgr_ids}, expected [{branch_ids[0]}]")
            resp = client.get('/ops')
            check("(l) manager blocked from /ops",
                  resp.status_code in (302, 403),
                  detail=f"status={resp.status_code}")

            # Cleanup created branch
            conn.execute("DELETE FROM branches WHERE id=?", (new_branch_id,))
            conn.commit()

            # Bonus: POST /api/admin/users creates a CEO user (admin only)
            login_as(client, admin_id, 'admin', user_branches=[])
            resp = client.post('/api/admin/users', json={
                'name': 'New CEO via API',
                'email': 'newceo@test.com',
                'password': 'newpass123',
                'role': 'ceo'})
            check("(bonus) admin can create CEO via /api/admin/users",
                  resp.status_code == 201, detail=f"status={resp.status_code}")
            # Cleanup
            conn.execute("DELETE FROM users WHERE LOWER(email)=?", ('newceo@test.com',))
            conn.commit()

            # CEO cannot create users (admin-only endpoint)
            login_as(client, ceo_id, 'ceo', user_branches=[])
            resp = client.post('/api/admin/users', json={
                'name': 'x', 'email': 'x@x.com', 'password': 'xxxxxx', 'role': 'manager'})
            check("(bonus) CEO blocked from POST /api/admin/users",
                  resp.status_code in (302, 403),
                  detail=f"status={resp.status_code}")

        conn.close()
    finally:
        os.unlink(db_path)

    print()
    print(f"=== {passed} passed, {failed} failed ===")
    if failures:
        for f in failures:
            print(f"  - {f}")
    sys.exit(0 if failed == 0 else 1)


if __name__ == '__main__':
    run_tests()

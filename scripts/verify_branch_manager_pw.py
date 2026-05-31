"""Staging verification for admin-typed manager password in branch-setup.

Exercises /api/admin/branches via the Flask test client with an admin session,
covering: valid create, short/missing password, duplicate email, and no-manager.
Cleans up every user/link it creates. Read-only w.r.t. pre-existing data.
"""
import sys
import app as A
from werkzeug.security import check_password_hash

BR = 9001  # קדיש לוז — eligible chain store on staging
EMAIL = 'verify_mgr_pw@example.test'

results = []


def check(label, cond, detail=''):
    results.append((label, cond, detail))
    print(f"{'PASS' if cond else 'FAIL'} — {label}: {detail}")


def admin_client():
    c = A.app.test_client()
    with A.app.app_context():
        db = A.get_db()
        row = db.execute("SELECT id FROM users WHERE role='admin' LIMIT 1").fetchone()
        admin_id = row['id']
    with c.session_transaction() as s:
        s['user_id'] = admin_id
        s['user_role'] = 'admin'
    return c


def cleanup():
    with A.app.app_context():
        db = A.get_db()
        u = db.execute('SELECT id FROM users WHERE LOWER(email)=?', (EMAIL,)).fetchone()
        if u:
            db.execute('DELETE FROM user_branches WHERE user_id=?', (u['id'],))
            db.execute('DELETE FROM users WHERE id=?', (u['id'],))
            db.commit()


def user_row():
    with A.app.app_context():
        db = A.get_db()
        return db.execute('SELECT * FROM users WHERE LOWER(email)=?', (EMAIL,)).fetchone()


def linked_to(uid, branch):
    with A.app.app_context():
        db = A.get_db()
        return db.execute('SELECT 1 FROM user_branches WHERE user_id=? AND branch_id=?',
                          (uid, branch)).fetchone() is not None


def linked(uid):
    return linked_to(uid, BR)


cleanup()
c = admin_client()

# 1. Valid create: name+email+password(>=6)
r = c.post('/api/admin/branches', json={
    'branch_id': BR, 'manager_name': 'Verify Mgr',
    'manager_email': EMAIL, 'manager_password': 'goodpass1'})
j = r.get_json()
u = user_row()
check('valid create returns ok+manager_created, no temp_password',
      r.status_code == 200 and j.get('manager_created') and 'temp_password' not in j,
      f"status={r.status_code} json={j}")
check('user created with typed password hash + linked to branch',
      u is not None and check_password_hash(u['password_hash'], 'goodpass1') and linked(u['id']),
      f"user={'yes' if u else 'no'} linked={linked(u['id']) if u else False}")

# 2. Short password
cleanup()
r = c.post('/api/admin/branches', json={
    'branch_id': BR, 'manager_name': 'Short', 'manager_email': EMAIL,
    'manager_password': '123'})
j = r.get_json()
check('short password -> 400, no user created',
      r.status_code == 400 and 'error' in j and user_row() is None,
      f"status={r.status_code} json={j} user={'yes' if user_row() else 'no'}")

# 3. Missing password
r = c.post('/api/admin/branches', json={
    'branch_id': BR, 'manager_name': 'NoPw', 'manager_email': EMAIL})
j = r.get_json()
check('missing password -> 400, no user created',
      r.status_code == 400 and 'error' in j and user_row() is None,
      f"status={r.status_code} json={j} user={'yes' if user_row() else 'no'}")

# 4. Duplicate email: create first, then re-submit with different password
cleanup()
c.post('/api/admin/branches', json={
    'branch_id': BR, 'manager_name': 'Dup', 'manager_email': EMAIL,
    'manager_password': 'firstpass'})
orig = user_row()
r = c.post('/api/admin/branches', json={
    'branch_id': 127, 'manager_name': 'Dup', 'manager_email': EMAIL,
    'manager_password': 'secondpass'})
j = r.get_json()
after = user_row()
pw_unchanged = check_password_hash(after['password_hash'], 'firstpass') and \
    not check_password_hash(after['password_hash'], 'secondpass')
check('duplicate email -> manager_existed, no temp_password, password NOT changed',
      r.status_code == 200 and j.get('manager_existed') and 'temp_password' not in j and pw_unchanged,
      f"json={j} pw_unchanged={pw_unchanged}")
check('duplicate email still links existing user to new branch (127)',
      linked_to(orig['id'], 127),
      f"linked_to_127={linked_to(orig['id'], 127)}")

# 5. No manager fields -> branch update still works
cleanup()
r = c.post('/api/admin/branches', json={'branch_id': BR})
j = r.get_json()
check('no manager fields -> ok, no user',
      r.status_code == 200 and j.get('ok') and 'temp_password' not in j and user_row() is None,
      f"status={r.status_code} json={j}")

cleanup()
print('\n' + ('ALL PASS' if all(x[1] for x in results) else 'SOME FAILED'))
sys.exit(0 if all(x[1] for x in results) else 1)

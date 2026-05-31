"""Staging verification: demo branches (9999/9998) are visible ONLY to admin
and the scoped demo account, and excluded for every CEO / aggregate / other
manager.

Seeds the demo branches + demo-store@ manager if missing (idempotent), then
exercises _list_visible_branches and /api/branches via the Flask test client
under admin / ceo / demo-manager / other-manager sessions. Cleans up only the
throwaway "other manager" it creates; leaves the demo branches + demo account
seeded (they are intended to persist on staging).
"""
import sys
import app as A

DEMO = set(A.DEMO_BRANCH_IDS)  # {9999, 9998}
results = []


def check(label, cond, detail=''):
    results.append((label, cond, detail))
    print(f"{'PASS' if cond else 'FAIL'} — {label}: {detail}")


def seed():
    with A.app.app_context():
        db = A.get_db()
        for bid, name in [(9999, 'דמו א'), (9998, 'דמו ב')]:
            if not db.execute('SELECT 1 FROM branches WHERE id=?', (bid,)).fetchone():
                db.execute('INSERT INTO branches (id, name, city, active) VALUES (?,?,?,1)',
                           (bid, name, 'דמו'))
        u = db.execute('SELECT id FROM users WHERE LOWER(email)=?',
                       (A.DEMO_ACCOUNT_EMAIL,)).fetchone()
        if not u:
            cur = db.execute(
                "INSERT INTO users (name, email, password_hash, role, active) "
                "VALUES ('Demo Store', ?, 'x', 'manager', 1)", (A.DEMO_ACCOUNT_EMAIL,))
            demo_uid = cur.lastrowid
        else:
            demo_uid = u['id']
        for bid in (9999, 9998):
            db.execute('INSERT OR IGNORE INTO user_branches (user_id, branch_id) VALUES (?,?)',
                       (demo_uid, bid))
        # a real branch to prove the exclusion only touches demo
        real = db.execute('SELECT id FROM branches WHERE active=1 AND id NOT IN (9999,9998) '
                          'ORDER BY id LIMIT 1').fetchone()['id']
        # throwaway non-demo manager assigned a real branch + 9999 (leak probe)
        cur = db.execute(
            "INSERT INTO users (name, email, password_hash, role, active) "
            "VALUES ('Throwaway', 'throwaway_demo_probe@example.test', 'x', 'manager', 1)")
        probe_uid = cur.lastrowid
        db.execute('INSERT INTO user_branches (user_id, branch_id) VALUES (?,?)', (probe_uid, real))
        db.execute('INSERT INTO user_branches (user_id, branch_id) VALUES (?,9999)', (probe_uid,))
        db.commit()
        return demo_uid, probe_uid, real


def cleanup_probe(probe_uid):
    with A.app.app_context():
        db = A.get_db()
        db.execute('DELETE FROM user_branches WHERE user_id=?', (probe_uid,))
        db.execute('DELETE FROM users WHERE id=?', (probe_uid,))
        db.commit()


def client(role, email='x@x.test', user_id=1, branches=None):
    c = A.app.test_client()
    with c.session_transaction() as s:
        s['user_id'] = user_id
        s['user_role'] = role
        s['user_email'] = email
        s['user_branches'] = branches or []
    return c


def branch_ids_from_api(c):
    r = c.get('/api/branches')
    return {b['id'] for b in r.get_json()}, r.status_code


demo_uid, probe_uid, real = seed()

with A.app.app_context():
    # --- _list_visible_branches (source of every network/aggregate view) ---
    with A.app.test_request_context():
        from flask import session
        session['user_role'] = 'admin'
        admin_ids = {b['id'] for b in A._list_visible_branches(1, 'admin')}
    with A.app.test_request_context():
        from flask import session
        session['user_role'] = 'ceo'
        session['user_email'] = 'demo@makoletchain.com'
        ceo_ids = {b['id'] for b in A._list_visible_branches(7, 'ceo')}
    with A.app.test_request_context():
        from flask import session
        session['user_role'] = 'manager'
        session['user_email'] = A.DEMO_ACCOUNT_EMAIL
        demo_ids = {b['id'] for b in A._list_visible_branches(demo_uid, 'manager')}
    with A.app.test_request_context():
        from flask import session
        session['user_role'] = 'manager'
        session['user_email'] = 'throwaway_demo_probe@example.test'
        probe_ids = {b['id'] for b in A._list_visible_branches(probe_uid, 'manager')}

check('admin _list_visible_branches INCLUDES demo 9999+9998',
      DEMO <= admin_ids, f"demo∩admin={sorted(DEMO & admin_ids)}")
check('ceo _list_visible_branches EXCLUDES demo',
      not (DEMO & ceo_ids) and real in ceo_ids,
      f"demo∩ceo={sorted(DEMO & ceo_ids)} real_present={real in ceo_ids}")
check('demo-account manager sees EXACTLY {9999,9998}',
      demo_ids == DEMO, f"demo_ids={sorted(demo_ids)}")
check('other manager (assigned 9999) does NOT see demo; keeps real branch',
      not (DEMO & probe_ids) and real in probe_ids,
      f"probe_ids={sorted(probe_ids)}")

# --- /api/branches (branch switcher) ---
admin_api, _ = branch_ids_from_api(client('admin'))
ceo_api, _ = branch_ids_from_api(client('ceo', email='demo@makoletchain.com', user_id=7))
demo_api, _ = branch_ids_from_api(
    client('manager', email=A.DEMO_ACCOUNT_EMAIL, user_id=demo_uid, branches=[9999, 9998]))

check('/api/branches admin INCLUDES demo', DEMO <= admin_api, f"demo∩admin={sorted(DEMO & admin_api)}")
check('/api/branches ceo EXCLUDES demo', not (DEMO & ceo_api) and real in ceo_api,
      f"demo∩ceo={sorted(DEMO & ceo_api)}")
check('/api/branches demo-account = {9999,9998}', demo_api == DEMO, f"={sorted(demo_api)}")

# --- aggregate endpoint: /api/network-overview (ceo-reachable) ---
r = client('ceo', email='demo@makoletchain.com', user_id=7).get('/api/network-overview')
body = r.get_data(as_text=True)
check('/api/network-overview (ceo) response has no 9999/9998',
      '9999' not in body and '9998' not in body,
      f"status={r.status_code}")

# --- sanity: exclusion is ONLY the two demo ids, nothing real dropped ---
check('no real branch dropped for ceo (count = all_active - 2)',
      ceo_api == admin_api - DEMO, f"admin\\ceo={sorted(admin_api - ceo_api)}")

cleanup_probe(probe_uid)
print('\n' + ('ALL PASS' if all(x[1] for x in results) else 'SOME FAILED'))
sys.exit(0 if all(x[1] for x in results) else 1)

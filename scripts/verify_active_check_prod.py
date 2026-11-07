"""Post-deploy check for _enforce_active_user (deactivation ends live sessions).

Runs ON the prod server, with ZERO writes to the prod DB: the prod code is
imported from a temp tree of symlinks whose db/ holds an online-backup
SNAPSHOT of the live DB (source opened read-only). Importing app runs
init_db()/seed_admin() and requests may write user_events — all of that
lands in the snapshot, never in prod. brrr and Resend are stubbed and
counted (must stay 0); BRRR_URL/RESEND_API_KEY are also blanked before
import as a second fence.

Usage: python scripts/verify_active_check_prod.py <blocked_uid> <active_mgr_uid> <ceo_uid> <ceo_branch>
"""
import os
import shutil
import sqlite3
import sys
import tempfile

from werkzeug.security import generate_password_hash

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BLOCKED, MGR, CEO, CEO_BRANCH = (int(a) for a in sys.argv[1:5])

os.environ['BRRR_URL'] = 'http://127.0.0.1:9/'
os.environ['RESEND_API_KEY'] = ''

tmp = tempfile.mkdtemp(prefix='verify_active_')
for name in os.listdir(REPO):
    if name not in ('db', '.git'):
        os.symlink(os.path.join(REPO, name), os.path.join(tmp, name))
os.makedirs(os.path.join(tmp, 'db'))
for name in os.listdir(os.path.join(REPO, 'db')):
    if not name.startswith('makolet_chain.db'):     # schema.sql etc.
        os.symlink(os.path.join(REPO, 'db', name), os.path.join(tmp, 'db', name))
snap = os.path.join(tmp, 'db', 'makolet_chain.db')
src = sqlite3.connect(f"file:{os.path.join(REPO, 'db', 'makolet_chain.db')}?mode=ro",
                      uri=True)
dst = sqlite3.connect(snap)
src.backup(dst)
src.close()
dst.close()

sys.path.insert(0, tmp)
os.chdir(tmp)
import app as app_module  # noqa: E402
import utils.notify  # noqa: E402
import resend  # noqa: E402

assert os.path.realpath(app_module.DB_PATH) == os.path.realpath(snap), app_module.DB_PATH

sent = []
utils.notify.notify = lambda *a, **k: sent.append(('brrr', a))
resend.Emails.send = lambda *a, **k: sent.append(('email', a))

app = app_module.app
app.config['TESTING'] = True
results = []


def check(label, ok, detail=''):
    results.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {label}  {detail}")


def db():
    c = sqlite3.connect(snap)
    c.row_factory = sqlite3.Row
    return c


def session_for(client, uid):
    c = db()
    u = c.execute('SELECT * FROM users WHERE id=?', (uid,)).fetchone()
    bids = [r[0] for r in c.execute(
        'SELECT branch_id FROM user_branches WHERE user_id=?', (uid,))]
    c.close()
    with client.session_transaction() as s:
        s['user_id'] = u['id']
        s['user_name'] = u['name']
        s['user_role'] = u['role']
        s['user_email'] = (u['email'] or '').strip().lower()
        s['user_branches'] = bids
        if bids:
            s['branch_id'] = bids[0]
    return u


def events(uid):
    c = db()
    n = c.execute('SELECT COUNT(*) FROM user_events WHERE user_id=?', (uid,)).fetchone()[0]
    c.close()
    return n


# 1. active manager unaffected
with app.test_client() as c:
    u = session_for(c, MGR)
    r1, r2 = c.get('/'), c.get('/api/summary')
    check(f'active manager uid {MGR} dashboard', r1.status_code == 200, r1.status_code)
    check(f'active manager uid {MGR} /api/summary', r2.status_code == 200, r2.status_code)

# 2. blocked user's existing session rejected
with app.test_client() as c:
    u = session_for(c, BLOCKED)
    check(f'uid {BLOCKED} users.active', u['active'] == 0, u['active'])
    ev0 = events(BLOCKED)
    r = c.get('/')
    check(f'uid {BLOCKED} page -> /login',
          r.status_code == 302 and r.headers.get('Location', '').endswith('/login'),
          f"{r.status_code} {r.headers.get('Location')}")
    with c.session_transaction() as s:
        check(f'uid {BLOCKED} session cleared', 'user_id' not in s)
    session_for(c, BLOCKED)
    r = c.get('/api/summary')
    check(f'uid {BLOCKED} API -> 401', r.status_code == 401, r.status_code)
    check(f'uid {BLOCKED} no page_view written', events(BLOCKED) == ev0,
          f'{ev0} -> {events(BLOCKED)}')

# 3. blocked user can't log in — even with the correct password (snapshot only)
c2 = db()
c2.execute('UPDATE users SET password_hash=? WHERE id=?',
           (generate_password_hash('VerifyOnly!1'), BLOCKED))
c2.commit()
email = c2.execute('SELECT email FROM users WHERE id=?', (BLOCKED,)).fetchone()[0]
c2.close()
with app.test_client() as c:
    r = c.post('/login', data={'email': email, 'password': 'VerifyOnly!1'})
    with c.session_transaction() as s:
        logged = 'user_id' in s
    check(f'uid {BLOCKED} login refused (correct password)',
          r.status_code == 200 and not logged, r.status_code)

# 4. CEO (only other user of the blocked user's branches) unaffected
with app.test_client() as c:
    session_for(c, CEO)
    with c.session_transaction() as s:
        s['branch_id'] = CEO_BRANCH
    r1, r2 = c.get('/'), c.get('/api/summary')
    check(f'ceo uid {CEO} on branch {CEO_BRANCH} dashboard', r1.status_code == 200, r1.status_code)
    check(f'ceo uid {CEO} /api/summary', r2.status_code == 200, r2.status_code)

check('0 emails, 0 brrr', not sent, sent)
shutil.rmtree(tmp, ignore_errors=True)
print(f"\n{sum(results)}/{len(results)} PASS")
sys.exit(0 if all(results) else 1)

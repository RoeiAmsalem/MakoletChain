"""Create the PERSISTENT staging test user for the multi-branch תקציב view.

dennis-test@makoletchain.com / manager / branches 9015 (הגנה) + 9018 (דפנה).
Idempotent: re-running resets the password and the branch assignments.
STAGING ONLY — refuses to run from the prod tree. Plain user insert via the
same users/user_branches tables the /admin UI writes; no schema change.

After creating, verifies through the real app: POST /login with the
credentials, then GET /goods?multi=1 must render the combined strip.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT.startswith('/opt/makolet-chain') and 'staging' not in ROOT:
    sys.exit('REFUSED: this script is staging-only — never run on prod.')
sys.path.insert(0, ROOT)

from werkzeug.security import generate_password_hash  # noqa: E402
from app import app, get_db  # noqa: E402

EMAIL = 'dennis-test@makoletchain.com'
PASSWORD = 'Dennis2026!'
BRANCHES = (9015, 9018)

with app.test_request_context():
    db = get_db()
    db.execute(
        "DELETE FROM user_branches WHERE user_id IN "
        "(SELECT id FROM users WHERE LOWER(email) = ?)", (EMAIL,))
    db.execute("DELETE FROM users WHERE LOWER(email) = ?", (EMAIL,))
    cur = db.execute(
        "INSERT INTO users (name, email, password_hash, role, active) "
        "VALUES (?, ?, ?, 'manager', 1)",
        ('דניס (בדיקה)', EMAIL, generate_password_hash(PASSWORD)))
    uid = cur.lastrowid
    for b in BRANCHES:
        db.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)",
                   (uid, b))
    db.commit()
    print(f"user created: uid={uid} {EMAIL} branches={BRANCHES}")

# Verify through the real login route + multi view
app.config['TESTING'] = True
client = app.test_client()
r = client.post('/login', data={'email': EMAIL, 'password': PASSWORD})
ok_login = r.status_code == 302 and r.headers.get('Location', '').endswith('/')
print(f"{'PASS' if ok_login else 'FAIL'} — login: status={r.status_code}")

r = client.get('/goods?multi=1')
html = r.get_data(as_text=True)
ok_multi = r.status_code == 200 and 'תקציב — כל הסניפים שלי' in html
n_sections = html.count('class="card gm-section"')
print(f"{'PASS' if ok_multi else 'FAIL'} — multi view renders: "
      f"status={r.status_code} sections={n_sections}")
r = client.get('/goods')
flag = 'const SHOW_ALL_MY_BRANCHES = true' in r.get_data(as_text=True)
print(f"{'PASS' if flag else 'FAIL'} — selector option flag on")

sys.exit(0 if (ok_login and ok_multi and flag) else 1)

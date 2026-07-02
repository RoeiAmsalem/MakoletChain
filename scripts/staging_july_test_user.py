"""Create the SECOND live-e2e test manager on STAGING (July real-clock test).

july-test@makoletchain.com / manager / branch 9013, with a manager_billing row
active=1, sumit_tag=str(uid), activated_at=today — so Roei's ₪1 payment through
the tagged link must join to THIS uid while dennis's tagged customer (26) also
exists in SUMIT. Idempotent; STAGING ONLY. DB-only — never touches SUMIT.
Prints the new user_id and verifies login + the rendered tagged link.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT.startswith('/opt/makolet-chain') and 'staging' not in ROOT:
    sys.exit('REFUSED: this script is staging-only — never run on prod.')
sys.path.insert(0, ROOT)

from werkzeug.security import generate_password_hash  # noqa: E402
from app import app, get_db, _now_il  # noqa: E402

EMAIL = 'july-test@makoletchain.com'
PASSWORD = 'July2026!'
BRANCH = 9013

with app.test_request_context():
    db = get_db()
    db.execute(
        "DELETE FROM manager_billing WHERE user_id IN "
        "(SELECT id FROM users WHERE LOWER(email) = ?)", (EMAIL,))
    db.execute(
        "DELETE FROM user_branches WHERE user_id IN "
        "(SELECT id FROM users WHERE LOWER(email) = ?)", (EMAIL,))
    db.execute("DELETE FROM users WHERE LOWER(email) = ?", (EMAIL,))
    cur = db.execute(
        "INSERT INTO users (name, email, password_hash, role, active) "
        "VALUES (?, ?, ?, 'manager', 1)",
        ('יולי (בדיקה)', EMAIL, generate_password_hash(PASSWORD)))
    uid = cur.lastrowid
    db.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)",
               (uid, BRANCH))
    now = _now_il()
    db.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, "
        "last_status, activated_at, updated_at) VALUES (?, ?, 179, 1, "
        "'unpaid', ?, ?)",
        (uid, str(uid), now.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d %H:%M')))
    db.commit()
    print(f"july-test manager: uid={uid} branch={BRANCH} active=1 "
          f"activated_at={now.strftime('%Y-%m-%d')}")

app.config['TESTING'] = True
client = app.test_client()
r = client.post('/login', data={'email': EMAIL, 'password': PASSWORD})
ok_login = r.status_code == 302
html = client.get('/account').get_data(as_text=True)
marker = f'customerexternalidentifier={uid}'
has_link = marker in html
print(f"{'PASS' if ok_login else 'FAIL'} — login")
print(f"{'PASS' if has_link else 'FAIL'} — /account pay link carries {marker}")
import re  # noqa: E402
m = re.search(r'href="(https://pay\.sumit\.co\.il[^"]*)"', html)
print(f"tagged link: {m.group(1) if m else None}")
sys.exit(0 if (ok_login and has_link) else 1)

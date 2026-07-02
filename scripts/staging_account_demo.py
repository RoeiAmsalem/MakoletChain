"""Put the two /account screenshot states in place on STAGING (billing stage 1).

  dennis-test@makoletchain.com    → manager_billing active=1, unpaid
                                    (status "ממתין לתשלום החודש" + pay button)
  inactive-test@makoletchain.com  → manager, NO manager_billing row
                                    (status "המנוי אינו פעיל", no button)

DB-only — never touches SUMIT. Idempotent. STAGING ONLY — refuses to run from
the prod tree. Run create_staging_test_user.py first if dennis is missing.
Verifies both states through the real app (POST /login → GET /account).
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT.startswith('/opt/makolet-chain') and 'staging' not in ROOT:
    sys.exit('REFUSED: this script is staging-only — never run on prod.')
sys.path.insert(0, ROOT)

from werkzeug.security import generate_password_hash  # noqa: E402
from app import app, get_db  # noqa: E402

ACTIVE_EMAIL = 'dennis-test@makoletchain.com'
ACTIVE_PASSWORD = 'Dennis2026!'
INACTIVE_EMAIL = 'inactive-test@makoletchain.com'
INACTIVE_PASSWORD = 'Inactive2026!'
INACTIVE_BRANCH = 9015

with app.test_request_context():
    db = get_db()
    row = db.execute("SELECT id FROM users WHERE LOWER(email) = ?",
                     (ACTIVE_EMAIL,)).fetchone()
    if not row:
        sys.exit(f'MISSING: {ACTIVE_EMAIL} — run create_staging_test_user.py first.')
    active_uid = row['id']
    db.execute(
        "INSERT OR IGNORE INTO manager_billing (user_id, sumit_tag, fee, active) "
        "VALUES (?, ?, 179, 0)", (active_uid, str(active_uid)))
    db.execute(
        "UPDATE manager_billing SET active=1, last_status='unpaid', "
        "last_paid_date=NULL WHERE user_id=?", (active_uid,))

    db.execute(
        "DELETE FROM manager_billing WHERE user_id IN "
        "(SELECT id FROM users WHERE LOWER(email) = ?)", (INACTIVE_EMAIL,))
    db.execute(
        "DELETE FROM user_branches WHERE user_id IN "
        "(SELECT id FROM users WHERE LOWER(email) = ?)", (INACTIVE_EMAIL,))
    db.execute("DELETE FROM users WHERE LOWER(email) = ?", (INACTIVE_EMAIL,))
    cur = db.execute(
        "INSERT INTO users (name, email, password_hash, role, active) "
        "VALUES (?, ?, ?, 'manager', 1)",
        ('בדיקה (ללא מנוי)', INACTIVE_EMAIL,
         generate_password_hash(INACTIVE_PASSWORD)))
    inactive_uid = cur.lastrowid
    db.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)",
               (inactive_uid, INACTIVE_BRANCH))
    db.commit()
    print(f"active manager:   uid={active_uid} {ACTIVE_EMAIL} (billing on, unpaid)")
    print(f"inactive manager: uid={inactive_uid} {INACTIVE_EMAIL} (no billing row)")

# Verify both states through the real routes
app.config['TESTING'] = True
ok = True
for email, password, expect in [
    (ACTIVE_EMAIL, ACTIVE_PASSWORD, 'ממתין לתשלום החודש'),
    (INACTIVE_EMAIL, INACTIVE_PASSWORD, 'המנוי אינו פעיל'),
]:
    client = app.test_client()
    r = client.post('/login', data={'email': email, 'password': password})
    logged_in = r.status_code == 302
    r = client.get('/account')
    html = r.get_data(as_text=True)
    hit = r.status_code == 200 and expect in html
    ok = ok and logged_in and hit
    print(f"{'PASS' if (logged_in and hit) else 'FAIL'} — {email}: "
          f"login={logged_in} /account shows «{expect}»={hit}")
    if email == ACTIVE_EMAIL:
        marker = f'customerexternalidentifier={active_uid}'
        has_link = marker in html
        ok = ok and has_link
        print(f"{'PASS' if has_link else 'FAIL'} — pay link carries own tag ({marker})")

sys.exit(0 if ok else 1)

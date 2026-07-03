"""Pre-launch billing audit + FINAL live-e2e setup on STAGING (2026-07-03).

Does, in order:
  1. SUMIT auth check (read-only ping).
  2. Creates the FINAL test manager final-test@makoletchain.com / branch 9010
     with a manager_billing row (created OFF — activation happens via the real
     admin toggle endpoint below, which also proves the toggle works and stamps
     activated_at=today through the production code path).
  3. Admin session (temp password swap on uid=1, restored in finally):
     /admin/billing renders + EVERY row's copy-link carries that row's own uid.
  4. POST /api/admin/billing/<uid> {'active': true} — live toggle proof.
  5. final-test session: /account renders the new hero design, contact links,
     and a pay button tagged with THIS uid only.

Idempotent; STAGING ONLY. DB-only — never writes to SUMIT.
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT.startswith('/opt/makolet-chain') and 'staging' not in ROOT:
    sys.exit('REFUSED: this script is staging-only — never run on prod.')
sys.path.insert(0, ROOT)

from werkzeug.security import generate_password_hash  # noqa: E402
from app import app, get_db, _now_il  # noqa: E402
from utils import sumit  # noqa: E402

EMAIL = 'final-test@makoletchain.com'
PASSWORD = 'Final2026!'
BRANCH = 9010
ADMIN_UID, ADMIN_EMAIL = 1, 'makoletdashboard@gmail.com'
ADMIN_TMP_PW = 'TmpFinalAudit2026!'

failures = []


def check(ok, label, extra=''):
    print(f"{'PASS' if ok else 'FAIL'} — {label}{(' — ' + extra) if extra else ''}")
    if not ok:
        failures.append(label)


# ── 1. SUMIT auth ─────────────────────────────────────────────
pong = sumit.ping()
check(pong['ok'], 'SUMIT auth (ping)', f"company={pong['company']!r} error={pong['error']!r}")

# ── 2. final-test manager (billing row OFF; toggled on via API below) ──
with app.test_request_context():
    db = get_db()
    for sql in (
        "DELETE FROM manager_billing WHERE user_id IN "
        "(SELECT id FROM users WHERE LOWER(email) = ?)",
        "DELETE FROM user_branches WHERE user_id IN "
        "(SELECT id FROM users WHERE LOWER(email) = ?)",
        "DELETE FROM users WHERE LOWER(email) = ?",
    ):
        db.execute(sql, (EMAIL,))
    cur = db.execute(
        "INSERT INTO users (name, email, password_hash, role, active) "
        "VALUES (?, ?, ?, 'manager', 1)",
        ('בדיקה סופית', EMAIL, generate_password_hash(PASSWORD)))
    uid = cur.lastrowid
    db.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)",
               (uid, BRANCH))
    db.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, last_status) "
        "VALUES (?, ?, 179, 0, 'unpaid')", (uid, str(uid)))
    db.commit()
    old_admin_hash = db.execute(
        "SELECT password_hash FROM users WHERE id=?", (ADMIN_UID,)).fetchone()[0]
    db.execute("UPDATE users SET password_hash=? WHERE id=?",
               (generate_password_hash(ADMIN_TMP_PW), ADMIN_UID))
    db.commit()
print(f"final-test manager created: uid={uid} branch={BRANCH}")

app.config['TESTING'] = True
try:
    # ── 3. /admin/billing renders + per-row tagged links ─────
    admin = app.test_client()
    r = admin.post('/login', data={'email': ADMIN_EMAIL, 'password': ADMIN_TMP_PW})
    check(r.status_code == 302, 'admin login')
    r = admin.get('/admin/billing')
    html = r.get_data(as_text=True)
    check(r.status_code == 200 and '<table class="billing"' in html,
          '/admin/billing renders (200 + table)')
    check('מחובר ל-SUMIT' in html, '/admin/billing shows connected badge')

    # every row's copy-link must carry that row's own uid
    rows = re.findall(r'id="row-(\d+)".*?data-link="([^"]*)"', html, re.S)
    bad = [(rid, link) for rid, link in rows
           if f'customerexternalidentifier={rid}' not in link]
    check(len(rows) > 0 and not bad,
          f'per-manager tagged links correct ({len(rows)} rows)',
          f'bad={bad[:3]}' if bad else '')
    check(any(rid == str(uid) for rid, _ in rows),
          f'final-test uid={uid} appears on /admin/billing')

    # ── 4. live toggle via the real endpoint ──────────────────
    r = admin.post(f'/api/admin/billing/{uid}', json={'active': True})
    check(r.status_code == 200 and r.get_json().get('active') is True,
          'toggle endpoint returns active=true')
    with app.test_request_context():
        row = get_db().execute(
            "SELECT active, activated_at, sumit_tag FROM manager_billing "
            "WHERE user_id=?", (uid,)).fetchone()
    today = _now_il().strftime('%Y-%m-%d')
    check(row['active'] == 1 and row['activated_at'] == today
          and row['sumit_tag'] == str(uid),
          f'DB row: active=1 activated_at={today} sumit_tag={uid}',
          f"got active={row['active']} activated_at={row['activated_at']!r}")

    # ── 5. /account as final-test ─────────────────────────────
    mgr = app.test_client()
    r = mgr.post('/login', data={'email': EMAIL, 'password': PASSWORD})
    check(r.status_code == 302, 'final-test login')
    r = mgr.get('/account')
    html = r.get_data(as_text=True)
    check(r.status_code == 200 and 'account-hero' in html and 'kpi-card' in html,
          '/account renders new hero design')
    for needle, label in (('mailto:kupashkufaa@gmail.com', 'mailto link'),
                          ('tel:0523455860', 'tel link'),
                          ('wa.me/972523455860', 'whatsapp link')):
        check(needle in html, f'/account {label}')
    m = re.search(r'class="pay-btn" href="([^"]*)"', html)
    tag = f'customerexternalidentifier={uid}'
    check(bool(m) and tag in m.group(1), f'/account pay button tagged {tag}',
          m.group(1) if m else 'no pay-btn')
    print(f"TAGGED LINK: {m.group(1) if m else '<missing>'}")
finally:
    with app.test_request_context():
        db = get_db()
        db.execute("UPDATE users SET password_hash=? WHERE id=?",
                   (old_admin_hash, ADMIN_UID))
        db.commit()
    print('admin password hash restored')

print(f"final-test uid={uid}")
sys.exit(1 if failures else 0)

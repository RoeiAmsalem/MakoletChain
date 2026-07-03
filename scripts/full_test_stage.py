"""Stage the FULL manager-experience test on STAGING (all layers live).

Creates fulltest@makoletchain.com — manager / branch 9006 / billing active,
activated_at = REAL-clock today (grace anchor stays Jul 5 under the fake
clock) — then runs one stamping sync and verifies the exact experience Roei
will walk: warning state, amber home banner, amber /account hero, tagged ₪1
pay button. Layer A (instant sync-on-return) is live — this is the real
manager journey, not a layer-isolation test.

Idempotent; STAGING ONLY. Read-only vs SUMIT.
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT.startswith('/opt/makolet-chain') and 'staging' not in ROOT:
    sys.exit('REFUSED: this script is staging-only — never run on prod.')
sys.path.insert(0, ROOT)

from werkzeug.security import generate_password_hash  # noqa: E402
from app import app, get_db, _now_il, _billing_state, _billing_today, \
    _run_billing_sync_logged  # noqa: E402

EMAIL, PASSWORD, BRANCH = 'fulltest@makoletchain.com', 'Full2026!', 9006
failures = []


def check(ok, label, extra=''):
    print(f"{'PASS' if ok else 'FAIL'} — {label}{(' — ' + extra) if extra else ''}")
    if not ok:
        failures.append(label)


check(_billing_today().isoformat() == '2026-07-06',
      'fake clock live (2026-07-06)', f'today={_billing_today()}')

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
        ('בדיקה מלאה', EMAIL, generate_password_hash(PASSWORD)))
    uid = cur.lastrowid
    db.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)",
               (uid, BRANCH))
    now = _now_il()  # REAL clock: grace anchor must stay Jul 5
    db.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, "
        "last_status, activated_at, updated_at) VALUES (?, ?, 179, 1, "
        "'unpaid', ?, ?)",
        (uid, str(uid), now.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d %H:%M')))
    db.commit()
    print(f'fulltest manager: uid={uid} branch={BRANCH} active=1 '
          f'activated_at={now.strftime("%Y-%m-%d")}')

    res = _run_billing_sync_logged(db, 'manual')   # stamping sync
    check(res.get('connected') and not res.get('error'), 'stamping sync ran',
          str(res))

    row = db.execute("SELECT * FROM manager_billing WHERE user_id=?",
                     (uid,)).fetchone()
    check(row['active'] == 1 and row['last_status'] == 'unpaid'
          and not row['last_paid_date'],
          'row: active=1, unpaid, stamped this month',
          f"updated_at={row['updated_at']!r}")

    st = _billing_state(uid, 'manager', EMAIL, db)
    check(st.get('state') == 'warning' and st.get('days_left') == 4,
          'state == warning, days_left=4', str(st))

app.config['TESTING'] = True
client = app.test_client()
r = client.post('/login', data={'email': EMAIL, 'password': PASSWORD})
check(r.status_code == 302, 'fulltest login')

home = client.get('/').get_data(as_text=True)
m = re.search(r'id="billing-warning-banner".*?<span[^>]*>(.*?)</span>', home, re.S)
banner = ' '.join(re.sub(r'<[^>]+>', ' ', m.group(1)).split()) if m else None
check(bool(banner) and 'בעוד 4 ימים' in banner,
      'home shows amber warning banner', banner or 'MISSING')

acct = client.get('/account').get_data(as_text=True)
check('ממתין לתשלום החודש' in acct and 'kpi-card--pending' in acct,
      '/account amber hero')
mm = re.search(r'class="pay-btn" href="([^"]*)"', acct)
check(bool(mm) and 'ydhez4' in mm.group(1)
      and f'customerexternalidentifier={uid}' in mm.group(1),
      f'pay button = ₪1 page tagged uid={uid}', mm.group(1) if mm else 'no pay-btn')

print(f'BANNER TEXT: {banner}')
print(f'TAGGED LINK: {mm.group(1) if mm else "<missing>"}')
print(f'CREDENTIALS: {EMAIL} / {PASSWORD}')
sys.exit(1 if failures else 0)

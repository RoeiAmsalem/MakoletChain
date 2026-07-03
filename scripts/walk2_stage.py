"""Walk #2 restage for the final journey e2e on STAGING (2026-07-03).

Walk #1 was voided (Roei paid before fixing the SUMIT redirect; that payment
mapped to uid=30, now 'paid'). This stages a fresh walker.

Modes:
  create — (re)create walk-test@makoletchain.com / manager / branch 9002 with a
           manager_billing row active=1, activated_at=REAL-clock today (not the
           fake date — the grace anchor must match walk #1's Jul-5 math).
  check  — run AFTER the sync, with BILLING_FAKE_TODAY=2026-07-06 live: assert
           walk-test is in 'warning' (4 days left), 26/29/30 are all 'ok' (no
           banner), and the rendered home/account pages show the amber banner,
           amber hero, and the ₪1 link tagged with walk-test's uid.

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
from app import app, get_db, _now_il, _billing_state, _billing_today  # noqa: E402

EMAIL, PASSWORD, BRANCH = 'walk-test@makoletchain.com', 'Walk2026!', 9002

failures = []


def check(ok, label, extra=''):
    print(f"{'PASS' if ok else 'FAIL'} — {label}{(' — ' + extra) if extra else ''}")
    if not ok:
        failures.append(label)


def create():
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
            ('הליכה שנייה (בדיקה)', EMAIL, generate_password_hash(PASSWORD)))
        uid = cur.lastrowid
        db.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)",
                   (uid, BRANCH))
        now = _now_il()  # REAL clock on purpose — see docstring
        db.execute(
            "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, "
            "last_status, activated_at, updated_at) VALUES (?, ?, 179, 1, "
            "'unpaid', ?, ?)",
            (uid, str(uid), now.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d %H:%M')))
        db.commit()
    print(f"walk-test manager: uid={uid} branch={BRANCH} active=1 "
          f"activated_at={now.strftime('%Y-%m-%d')}")


def stage_check():
    check(_billing_today().isoformat() == '2026-07-06',
          'fake clock live (BILLING_FAKE_TODAY)', f'today={_billing_today()}')

    with app.test_request_context():
        db = get_db()
        u = db.execute("SELECT id FROM users WHERE LOWER(email)=?", (EMAIL,)).fetchone()
        uid = u['id']
        row = db.execute("SELECT * FROM manager_billing WHERE user_id=?",
                         (uid,)).fetchone()
        check(row['active'] == 1 and row['last_status'] == 'unpaid'
              and not row['last_paid_date']
              and (row['activated_at'] or '')[:10] == '2026-07-03',
              f'uid={uid} row: active=1, unpaid, activated_at=2026-07-03',
              f"active={row['active']} status={row['last_status']!r} "
              f"paid={row['last_paid_date']!r} activated={row['activated_at']!r}")

        st = _billing_state(uid, 'manager', EMAIL, db)
        check(st.get('state') == 'warning' and st.get('days_left') == 4,
              f'uid={uid} state == warning, days_left=4', str(st))
        for prev in (26, 29, 30):
            pu = db.execute("SELECT role, email FROM users WHERE id=?",
                            (prev,)).fetchone()
            pst = _billing_state(prev, pu['role'], pu['email'], db)
            check(pst.get('state') == 'ok', f'uid={prev} state == ok (no banner)',
                  str(pst))

    app.config['TESTING'] = True
    client = app.test_client()
    r = client.post('/login', data={'email': EMAIL, 'password': PASSWORD})
    check(r.status_code == 302, 'walk-test login')

    home = client.get('/').get_data(as_text=True)
    m = re.search(r'id="billing-warning-banner".*?<span[^>]*>(.*?)</span>', home, re.S)
    banner = ' '.join(re.sub(r'<[^>]+>', ' ', m.group(1)).split()) if m else None
    check(bool(banner) and 'בעוד 4 ימים' in banner,
          'home banner shows 4 days remaining', banner or 'MISSING')

    acct = client.get('/account').get_data(as_text=True)
    check('ממתין לתשלום החודש' in acct and 'kpi-card--pending' in acct,
          '/account shows amber "ממתין לתשלום החודש" hero')
    mm = re.search(r'class="pay-btn" href="([^"]*)"', acct)
    check(bool(mm) and 'ydhez4' in mm.group(1)
          and f'customerexternalidentifier={uid}' in mm.group(1),
          f'pay button = ₪1 page tagged uid={uid}', mm.group(1) if mm else 'no pay-btn')
    print(f"BANNER TEXT: {banner}")
    print(f"TAGGED LINK: {mm.group(1) if mm else '<missing>'}")


if __name__ == '__main__':
    mode = sys.argv[1] if len(sys.argv) > 1 else ''
    if mode == 'create':
        create()
    elif mode == 'check':
        stage_check()
    else:
        sys.exit('usage: walk2_stage.py create|check')
    sys.exit(1 if failures else 0)

"""Verify the FINAL-e2e journey stage on STAGING (fake-clock warning state).

Run AFTER: BILLING_FAKE_TODAY=2026-07-06 is in .env (service restarted) AND
the sync (run_billing_sync_once.py) has stamped this month's rows.

Asserts, without changing anything:
  - the fake clock is live in-process (2026-07-06)
  - uid=30 (final-test): active=1, unpaid, activated_at=2026-07-03,
    _billing_state == 'warning' with the expected day counts
  - uid=26 + uid=29: _billing_state == 'ok' (paid 2026-07-02) → no banner
  - final-test's rendered home page carries the amber warning banner (exact
    text printed) and /account shows the amber 'ממתין לתשלום החודש' hero +
    the ₪1 pay link tagged customerexternalidentifier=30

STAGING ONLY. DB read-only; never touches SUMIT.
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT.startswith('/opt/makolet-chain') and 'staging' not in ROOT:
    sys.exit('REFUSED: this script is staging-only — never run on prod.')
sys.path.insert(0, ROOT)

from app import app, get_db, _billing_state, _billing_today  # noqa: E402

EMAIL, PASSWORD, UID = 'final-test@makoletchain.com', 'Final2026!', 30

failures = []


def check(ok, label, extra=''):
    print(f"{'PASS' if ok else 'FAIL'} — {label}{(' — ' + extra) if extra else ''}")
    if not ok:
        failures.append(label)


check(_billing_today().isoformat() == '2026-07-06',
      'fake clock live (BILLING_FAKE_TODAY)', f'today={_billing_today()}')

with app.test_request_context():
    db = get_db()
    row = db.execute("SELECT * FROM manager_billing WHERE user_id=?", (UID,)).fetchone()
    check(row is not None and row['active'] == 1
          and row['last_status'] == 'unpaid' and not row['last_paid_date']
          and (row['activated_at'] or '')[:10] == '2026-07-03',
          'uid=30 row: active=1, unpaid, activated_at=2026-07-03',
          f"active={row['active']} status={row['last_status']!r} "
          f"paid={row['last_paid_date']!r} activated={row['activated_at']!r}"
          if row else 'row missing')

    states = {}
    for uid in (26, 29, 30):
        u = db.execute("SELECT role, email FROM users WHERE id=?", (uid,)).fetchone()
        states[uid] = _billing_state(uid, u['role'], u['email'], db)
    check(states[30].get('state') == 'warning', 'uid=30 state == warning',
          str(states[30]))
    check(states[30].get('days_left') == 4 and states[30].get('days_unpaid') == 2,
          'uid=30 days: unpaid=2, left=4', str(states[30]))
    for uid in (26, 29):
        mb = db.execute("SELECT last_paid_date, last_status FROM manager_billing "
                        "WHERE user_id=?", (uid,)).fetchone()
        check(states[uid].get('state') == 'ok'
              and mb['last_paid_date'] == '2026-07-02' and mb['last_status'] == 'paid',
              f'uid={uid} state == ok (paid 2026-07-02, no banner)',
              f"state={states[uid]} paid={mb['last_paid_date']!r}")

app.config['TESTING'] = True
client = app.test_client()
r = client.post('/login', data={'email': EMAIL, 'password': PASSWORD})
check(r.status_code == 302, 'final-test login')

home = client.get('/').get_data(as_text=True)
m = re.search(r'id="billing-warning-banner".*?<span[^>]*>(.*?)</span>', home, re.S)
banner_text = ' '.join(re.sub(r'<[^>]+>', ' ', m.group(1)).split()) if m else None
check(bool(m), 'home page shows amber warning banner', banner_text or 'MISSING')
check(bool(banner_text) and 'בעוד 4 ימים' in banner_text,
      'banner says 4 days remaining', banner_text or '')

acct = client.get('/account').get_data(as_text=True)
check('ממתין לתשלום החודש' in acct and 'kpi-card--pending' in acct,
      '/account shows amber "ממתין לתשלום החודש" hero')
mm = re.search(r'class="pay-btn" href="([^"]*)"', acct)
check(bool(mm) and 'ydhez4' in mm.group(1)
      and f'customerexternalidentifier={UID}' in mm.group(1),
      'pay button = ₪1 page tagged uid=30', mm.group(1) if mm else 'no pay-btn')

print(f"BANNER TEXT: {banner_text}")
sys.exit(1 if failures else 0)

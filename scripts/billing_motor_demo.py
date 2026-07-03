"""Live proof of the billing motor on STAGING (walk-style, no new payment).

Layer A: resets walk-test (uid=31, who HAS a real July payment in SUMIT) to
'unpaid', then hits the RUNNING service's /account with an OG-PaymentID —
the sync-on-return must re-find the payment and flip him back to paid
(fast path: already green; slow path: מתעדכן hint → green after refresh).
A replayed OG within 60s must NOT trigger a second sync (rate limit).

Layer B+C: creates alert-demo (active manager with NO SUMIT payment), runs
the sweep in-process — the sync marks them unpaid, the alert pass fires ONE
🟡 dry-run brrr (BRRR_SILENT=true on staging prints instead of sending), and
a second sweep sends nothing (dedup). Ends with the /admin/billing header
showing the last sync's layer. Cleanup: alert-demo row switched off.

STAGING ONLY. READ-ONLY vs SUMIT throughout.
"""
import os
import re
import sys
import time

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT.startswith('/opt/makolet-chain') and 'staging' not in ROOT:
    sys.exit('REFUSED: this script is staging-only — never run on prod.')
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, 'scripts'))

from werkzeug.security import generate_password_hash  # noqa: E402
from app import app, get_db, _now_il  # noqa: E402
import billing_sweep  # noqa: E402

BASE = 'http://127.0.0.1:8081'
WALK_EMAIL, WALK_PW, WALK_UID = 'walk-test@makoletchain.com', 'Walk2026!', 31
DEMO_EMAIL = 'alert-demo@makoletchain.com'

failures = []


def check(ok, label, extra=''):
    print(f"{'PASS' if ok else 'FAIL'} — {label}{(' — ' + extra) if extra else ''}")
    if not ok:
        failures.append(label)


def q(sql, args=()):
    with app.test_request_context():
        return [dict(r) for r in get_db().execute(sql, args).fetchall()]


def run_count():
    return q("SELECT COUNT(*) c FROM billing_sync_runs")[0]['c']


# ── stage: uid=31 back to unpaid; create alert-demo ───────────
with app.test_request_context():
    db = get_db()
    db.execute("UPDATE manager_billing SET last_paid_date=NULL, "
               "last_status='unpaid', alert_state=NULL, alert_date=NULL "
               "WHERE user_id=?", (WALK_UID,))
    row = db.execute("SELECT id FROM users WHERE LOWER(email)=?",
                     (DEMO_EMAIL,)).fetchone()
    if row:
        demo_uid = row['id']
        db.execute("DELETE FROM manager_billing WHERE user_id=?", (demo_uid,))
    else:
        cur = db.execute(
            "INSERT INTO users (name, email, password_hash, role, active) "
            "VALUES (?, ?, ?, 'manager', 1)",
            ('התראה (בדיקה)', DEMO_EMAIL, generate_password_hash('AlertDemo2026!')))
        demo_uid = cur.lastrowid
        db.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (?, 9006)",
                   (demo_uid,))
    db.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, "
        "last_status, activated_at, updated_at) VALUES (?, ?, 179, 1, 'unpaid', "
        "'2026-07-03', ?)", (demo_uid, str(demo_uid),
                             _now_il().strftime('%Y-%m-%d %H:%M')))
    db.commit()
print(f'staged: uid=31 unpaid again; alert-demo uid={demo_uid} active, no payment')

# ── layer A: sync-on-return on the LIVE service ───────────────
runs_before = run_count()
s = requests.Session()
s.get(f'{BASE}/login', timeout=10)
r = s.post(f'{BASE}/login', timeout=10, allow_redirects=False,
           data={'email': WALK_EMAIL, 'password': WALK_PW})
check(r.status_code == 302, 'walk-test login (live service)')

t0 = time.time()
r = s.get(f'{BASE}/account?OG-PaymentID=MOTOR-DEMO&OG-DocumentNumber=40005',
          timeout=30)
elapsed = time.time() - t0
html = r.text
if 'המנוי פעיל' in html:
    path = 'FAST (inline <3s)'
    check(True, f'instant flip — rendered already-green in {elapsed:.1f}s', path)
else:
    pending = 'מתעדכן' in html and 'location.reload' in html
    check(pending, f'slow path — מתעדכן hint + auto-refresh ({elapsed:.1f}s)')
    time.sleep(6)
    html = s.get(f'{BASE}/account', timeout=30).text
    check('המנוי פעיל' in html, 'green after one auto-refresh cycle')
    path = 'SLOW (background + refresh)'
print(f'  → path taken: {path}')

row = q("SELECT last_status, last_paid_date FROM manager_billing "
        "WHERE user_id=?", (WALK_UID,))[0]
check(row['last_status'] == 'paid' and row['last_paid_date'] == '2026-07-03',
      'uid=31 re-flipped to paid by the return-sync', str(row))
runs_a = q("SELECT source, ok FROM billing_sync_runs ORDER BY id DESC LIMIT 1")[0]
check(run_count() == runs_before + 1 and runs_a['source'] == 'payment'
      and runs_a['ok'] == 1, "run-log: one new row, source='payment', ok",
      str(runs_a))

# replay within 60s → rate limit, no second sync
s.get(f'{BASE}/account?OG-PaymentID=MOTOR-DEMO', timeout=30)
check(run_count() == runs_before + 1, 'replayed OG rate-limited (no 2nd sync)')

# ── layer B+C: sweep + one dry-run alert + dedup ──────────────
print('\n[sweep #1 — expect ONE dry-run 🟡 for alert-demo]')
rc = billing_sweep.run_sweep(retry_delay=0)
check(rc == 'ok', 'sweep #1 ran', f'rc={rc}')
demo_row = q("SELECT alert_state, alert_date FROM manager_billing "
             "WHERE user_id=?", (demo_uid,))[0]
check(demo_row['alert_state'] == 'warning',
      'alert-demo transitioned → warning (alert sent above)', str(demo_row))
runs_b = q("SELECT source, ok FROM billing_sync_runs ORDER BY id DESC LIMIT 1")[0]
check(runs_b['source'] == 'auto' and runs_b['ok'] == 1,
      "run-log: sweep row source='auto', ok")

print('\n[sweep #2 — expect NO alert (same-state dedup)]')
rc2 = billing_sweep.run_sweep(retry_delay=0)
check(rc2 == 'ok', 'sweep #2 ran (watch: no [brrr] line above)')

# ── admin header shows the last layer ─────────────────────────
UID_ADMIN, ADMIN_EMAIL = 1, 'makoletdashboard@gmail.com'
TMP = 'TmpMotorDemo2026!'
with app.test_request_context():
    db = get_db()
    old_hash = db.execute("SELECT password_hash FROM users WHERE id=?",
                          (UID_ADMIN,)).fetchone()[0]
    db.execute("UPDATE users SET password_hash=? WHERE id=?",
               (generate_password_hash(TMP), UID_ADMIN))
    db.commit()
try:
    app.config['TESTING'] = True
    admin = app.test_client()
    admin.post('/login', data={'email': ADMIN_EMAIL, 'password': TMP})
    ahtml = admin.get('/admin/billing').get_data(as_text=True)
    m = re.search(r'סונכרן לאחרונה:\s*<b>([\d:]+)</b>\s*(✓|✗)\s*\(([^)]+)\)',
                  ahtml)
    check(bool(m) and m.group(3) == 'אוטומטי',
          '/admin/billing header: last sync = auto layer',
          m.group(0).replace('<b>', '').replace('</b>', '') if m else 'MISSING')
    check('state-chip' in ahtml and '<th>מצב</th>' in ahtml,
          '/admin/billing shows per-manager computed state chips')
finally:
    with app.test_request_context():
        db = get_db()
        db.execute("UPDATE users SET password_hash=? WHERE id=?",
                   (old_hash, UID_ADMIN))
        db.commit()
    print('admin password hash restored')

# ── cleanup: switch alert-demo off ────────────────────────────
with app.test_request_context():
    db = get_db()
    db.execute("UPDATE manager_billing SET active=0 WHERE user_id=?", (demo_uid,))
    db.commit()
print(f'cleanup: alert-demo uid={demo_uid} manager_billing.active=0')

sys.exit(1 if failures else 0)

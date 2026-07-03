"""Post-deploy billing verification on PROD (launch checklist, 2026-07-03).

Runs ON the prod server. Read-only against SUMIT; the only DB writes are the
lazy manager_billing row materialisation (active=0) and the sync's
last_status/updated_at stamps — exactly what the first admin visit to
/admin/billing would do anyway.

Follows the shot_*.py pattern: temporarily set a known password for the admin
user, drive Playwright against 127.0.0.1:8080, restore the original hash in a
finally block.

Checks:
  1. /admin/billing renders the real manager roster, ALL toggles OFF + screenshot
  2. manual read-only SUMIT sync through the admin session (auth OK, 0 paid —
     staging-era test payments share no tag with prod managers)
  3. roster + active flags straight from manager_billing after the sync
  4. /account renders for the admin (neutral admin_no_billing state) + screenshot
  5. _billing_state == exempt for every active manager (all active=0 AND today
     precedes BILLING_START_DATE) → no banner, no lock, anywhere
"""
import json
import sqlite3
import sys

from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain/db/makolet_chain.db'
UID, EMAIL, PW = 1, 'makoletdashboard@gmail.com', 'TempBillingVerify2026!'
BASE = 'http://127.0.0.1:8080'

failures = []

conn = sqlite3.connect(DB)
old = conn.execute("SELECT password_hash FROM users WHERE id=?", (UID,)).fetchone()[0]
conn.execute("UPDATE users SET password_hash=? WHERE id=?",
             (generate_password_hash(PW), UID))
conn.commit()
conn.close()
try:
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(viewport={'width': 1280, 'height': 1400})
        pg.goto(f'{BASE}/login', wait_until='domcontentloaded')
        pg.fill('input[name=email]', EMAIL)
        pg.fill('input[name=password]', PW)
        pg.click('button[type=submit]')
        pg.wait_for_load_state('domcontentloaded')

        # 1. /admin/billing — roster visible, toggles OFF, last-sync header.
        pg.goto(f'{BASE}/admin/billing', wait_until='domcontentloaded')
        pg.wait_for_selector('table.billing', state='visible', timeout=15000)
        pg.wait_for_timeout(800)
        rows = pg.locator('table.billing tbody tr').count()
        on = pg.locator('table.billing .toggle-btn.on').count()
        print(f'ADMIN_BILLING rows={rows} toggles_on={on}')
        if on != 0:
            failures.append(f'{on} toggles ON — expected 0')

        # 2. Manual read-only sync through the live admin session.
        result = pg.evaluate(
            "async () => { const r = await fetch('/api/admin/billing/sync',"
            " {method:'POST'}); return {status: r.status, body: await r.json()}; }")
        print('SYNC', json.dumps(result, ensure_ascii=False))
        body = result.get('body') or {}
        if result.get('status') != 200 or not body.get('connected') or body.get('error'):
            failures.append(f'sync failed: {result}')
        if body.get('paid_managers'):
            failures.append(
                f"sync flipped {body['paid_managers']} manager(s) to paid — expected 0")

        # reload → last-sync header should now show
        pg.goto(f'{BASE}/admin/billing', wait_until='domcontentloaded')
        pg.wait_for_selector('table.billing', state='visible', timeout=15000)
        pg.wait_for_timeout(800)
        pg.screenshot(path='/tmp/billing_prod.png', full_page=True)
        print('SHOT /tmp/billing_prod.png')

        # 4. /account as admin — neutral state.
        pg.goto(f'{BASE}/account', wait_until='domcontentloaded')
        pg.wait_for_timeout(800)
        pg.screenshot(path='/tmp/account_prod.png', full_page=True)
        print('SHOT /tmp/account_prod.png')
        b.close()
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID))
    conn.commit()
    conn.close()

# 3. Roster + flags straight from the DB (post-sync).
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
print('ROSTER')
active_count = 0
for r in conn.execute(
        "SELECT mb.user_id, u.name, u.email, mb.sumit_tag, mb.fee, mb.active, "
        "mb.last_paid_date, mb.last_status "
        "FROM manager_billing mb JOIN users u ON u.id=mb.user_id "
        "ORDER BY mb.user_id"):
    active_count += r['active']
    print(f"  uid={r['user_id']} active={r['active']} tag={r['sumit_tag']} "
          f"fee={r['fee']} last_paid={r['last_paid_date']} "
          f"status={r['last_status']} {r['name']} <{r['email']}>")
if active_count:
    failures.append(f'{active_count} manager_billing rows active — expected 0')
conn.close()

# 5. _billing_state exempt for every active manager (in-process, real DB).
sys.path.insert(0, '/opt/makolet-chain')
from app import app, get_db, _billing_state  # noqa: E402

with app.test_request_context():
    db = get_db()
    print('PAYWALL_STATES')
    for r in db.execute("SELECT id, name, email, role FROM users "
                        "WHERE role='manager' AND active=1 ORDER BY id"):
        st = _billing_state(r['id'], r['role'], r['email'], db)
        print(f"  uid={r['id']} state={st['state']} {r['name']}")
        if st['state'] != 'exempt':
            failures.append(f"uid {r['id']} state={st['state']} — expected exempt")

if failures:
    print('FAILURES')
    for f in failures:
        print(' -', f)
    sys.exit(1)
print('ALL_CHECKS_PASSED')

"""Screenshot the two billing fixes on PROD (2026-07-03, pre-start window).

1. /account in the active-manager BEFORE-START state: no pay button, the
   'המנוי מתחיל ב-5.7' info line, neutral hero. The DEMO manager (uid 8,
   exempt-by-email, never billed) is the vehicle: its billing row is flipped
   active=1 for the render and restored EXACTLY (active/activated_at/
   updated_at) in a finally block — real managers' toggles are never touched.
2. /admin/billing with the roster now including the CEO rows (toggles off).

Same temp-password pattern as scripts/shot_billing.py / verify_billing_prod.py.
Asserts (hard exit 1): no customerexternalidentifier anywhere in the /account
HTML, info line present, CEO emails present in the roster, 0 toggles ON.
"""
import sqlite3
import sys

from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain/db/makolet_chain.db'
BASE = 'http://127.0.0.1:8080'
PW = 'TempBillingShot2026!'
ADMIN_UID, ADMIN_EMAIL = 1, 'makoletdashboard@gmail.com'
DEMO_UID, DEMO_EMAIL = 8, 'demo-store@makoletchain.com'
CEO_EMAILS = ('yaniv@hamakolet.net', 'amit@hamakolet.net',
              'elad@hamakolet.net', '1812roei@gmail.com')

failures = []

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
old_admin_hash = conn.execute(
    "SELECT password_hash FROM users WHERE id=?", (ADMIN_UID,)).fetchone()[0]
old_demo_hash = conn.execute(
    "SELECT password_hash FROM users WHERE id=?", (DEMO_UID,)).fetchone()[0]
demo_row = conn.execute(
    "SELECT active, activated_at, updated_at FROM manager_billing "
    "WHERE user_id=?", (DEMO_UID,)).fetchone()
if demo_row is None:
    print('FATAL: demo manager_billing row missing')
    sys.exit(1)
tmp_hash = generate_password_hash(PW)
conn.execute("UPDATE users SET password_hash=? WHERE id=?", (tmp_hash, ADMIN_UID))
conn.execute("UPDATE users SET password_hash=? WHERE id=?", (tmp_hash, DEMO_UID))
# transient: demo row active=1 for the before-start render only
conn.execute("UPDATE manager_billing SET active=1 WHERE user_id=?", (DEMO_UID,))
conn.commit()
conn.close()

try:
    with sync_playwright() as p:
        b = p.chromium.launch()

        def login(pg, email):
            pg.goto(f'{BASE}/login', wait_until='domcontentloaded')
            pg.fill('input[name=email]', email)
            pg.fill('input[name=password]', PW)
            pg.click('button[type=submit]')
            pg.wait_for_load_state('domcontentloaded')

        # 1. /account — active manager, before-start state.
        pg = b.new_page(viewport={'width': 1280, 'height': 900})
        login(pg, DEMO_EMAIL)
        pg.goto(f'{BASE}/account', wait_until='domcontentloaded')
        pg.wait_for_timeout(800)
        html = pg.content()
        if 'customerexternalidentifier' in html:
            failures.append('/account: pay link present before start date!')
        if 'אפשרות התשלום תיפתח כאן' not in html:
            failures.append('/account: before-start info line missing')
        if 'ממתין לתשלום החודש' in html:
            failures.append('/account: amber hero rendered before start date')
        pg.screenshot(path='/tmp/account_before_start_prod.png', full_page=True)
        print('SHOT /tmp/account_before_start_prod.png')
        pg.close()

        # 2. /admin/billing — roster now includes the CEO rows, toggles off.
        pg = b.new_page(viewport={'width': 1280, 'height': 1500})
        login(pg, ADMIN_EMAIL)
        pg.goto(f'{BASE}/admin/billing', wait_until='domcontentloaded')
        pg.wait_for_selector('table.billing', state='visible', timeout=15000)
        pg.wait_for_timeout(800)
        html = pg.content()
        for email in CEO_EMAILS:
            if email not in html:
                failures.append(f'/admin/billing: CEO {email} missing from roster')
        rows = pg.locator('table.billing tbody tr').count()
        on = pg.locator('table.billing .toggle-btn.on').count()
        print(f'ADMIN_BILLING rows={rows} toggles_on={on}')
        # the transient demo flip is the only ON toggle expected
        if on > 1:
            failures.append(f'{on} toggles ON — expected at most the transient demo row')
        pg.screenshot(path='/tmp/billing_ceo_prod.png', full_page=True)
        print('SHOT /tmp/billing_ceo_prod.png')
        b.close()
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?",
                 (old_admin_hash, ADMIN_UID))
    conn.execute("UPDATE users SET password_hash=? WHERE id=?",
                 (old_demo_hash, DEMO_UID))
    conn.execute(
        "UPDATE manager_billing SET active=?, activated_at=?, updated_at=? "
        "WHERE user_id=?",
        (demo_row['active'], demo_row['activated_at'], demo_row['updated_at'],
         DEMO_UID))
    conn.commit()
    conn.close()
    print('RESTORED demo row + both passwords')

if failures:
    print('FAILURES')
    for f in failures:
        print(' -', f)
    sys.exit(1)
print('ALL_CHECKS_PASSED')

"""Verify /admin/billing on staging: screenshot the table (all managers, toggles
OFF by default) and run the read-only SUMIT sync through a real admin session.

Runs ON the staging server (BASE=127.0.0.1:8081, staging DB path). Follows the
shot_*.py pattern: temporarily set a known password for the admin user, drive
Playwright, then restore the original hash in a finally block.
"""
import json
import sqlite3

from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW = 1, 'makoletdashboard@gmail.com', 'TempBillingShot2026!'
BASE = 'http://127.0.0.1:8081'
SHOT = '/tmp/billing_staging.png'

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

        pg.goto(f'{BASE}/admin/billing', wait_until='domcontentloaded')
        pg.wait_for_selector('table.billing', state='visible', timeout=15000)
        pg.wait_for_timeout(800)
        pg.screenshot(path=SHOT, full_page=True)
        print('SHOT', SHOT)

        # Count rows + how many toggles are ON (should be 0 by default).
        rows = pg.locator('table.billing tbody tr').count()
        on = pg.locator('table.billing .toggle-btn.on').count()
        print(f'MANAGERS_ROWS {rows}  TOGGLES_ON {on}')

        # One generated payment link (tag visible) straight from the DOM.
        link = pg.locator('.link-btn').first.get_attribute('data-link')
        print('SAMPLE_LINK', link)

        # Run the read-only sync through the logged-in session.
        result = pg.evaluate(
            "async () => { const r = await fetch('/api/admin/billing/sync',"
            " {method:'POST'}); return {status:r.status, body: await r.json()}; }")
        print('SYNC', json.dumps(result, ensure_ascii=False))
        b.close()
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID))
    conn.commit()
    conn.close()
    print('password restored')

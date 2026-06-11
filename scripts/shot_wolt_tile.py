"""Screenshots of the /sales Wolt tile on staging.

1. Desktop 9001 (has Wolt June)  → 6 equal KPI tiles incl. הכנסות Wolt
2. Desktop 126  (no Wolt)        → today's exact layout, no Wolt tile
3. Mobile 390px 9001             → Wolt as one more stacked card

Logs in as the admin user (temp password, restored in finally) and uses the
admin's branch_id URL-param switching.
"""
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW = 1, 'makoletdashboard@gmail.com', 'TempShot2026!'
BASE = 'http://127.0.0.1:8081'
MONTH = '2026-06'

conn = sqlite3.connect(DB)
old = conn.execute("SELECT password_hash FROM users WHERE id=?", (UID,)).fetchone()[0]
conn.execute("UPDATE users SET password_hash=? WHERE id=?",
             (generate_password_hash(PW), UID))
conn.commit(); conn.close()


def shoot(pg, branch_id, path):
    pg.goto(f'{BASE}/sales?branch_id={branch_id}&month={MONTH}',
            wait_until='domcontentloaded')
    pg.wait_for_selector('#sales-kpis .kpi-card', timeout=15000)
    pg.wait_for_timeout(1200)
    pg.locator('#sales-kpis').screenshot(path=path)
    print('shot', path)


try:
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(viewport={'width': 1440, 'height': 900})
        pg.goto(f'{BASE}/login', wait_until='domcontentloaded')
        pg.fill('input[name=email]', EMAIL); pg.fill('input[name=password]', PW)
        pg.click('button[type=submit]'); pg.wait_for_load_state('networkidle')

        shoot(pg, 9001, '/tmp/wolt_9001_desktop.png')
        shoot(pg, 126, '/tmp/wolt_126_desktop.png')

        m = b.new_page(viewport={'width': 390, 'height': 844})
        # session cookies are per-context; reuse pg's context instead
        m.close()
        pgm = pg.context.new_page()
        pgm.set_viewport_size({'width': 390, 'height': 844})
        shoot(pgm, 9001, '/tmp/wolt_9001_mobile.png')
        b.close()
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID))
    conn.commit(); conn.close()
    print('password restored')

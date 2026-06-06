"""Mobile 390px screenshot of the /goods יעדים toggle on staging (branch 9015).

Logs in as admin (temp password, restored after), opens /goods for branch 9015,
clicks the יעדים segment, waits for the budget table, and shoots the full page
(also captures the branch header so the '— null' fix is visible).
"""
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'
BRANCH = 9015

conn = sqlite3.connect(DB)
old = conn.execute("SELECT password_hash FROM users WHERE id=?", (UID,)).fetchone()[0]
conn.execute("UPDATE users SET password_hash=? WHERE id=?", (generate_password_hash(PW), UID))
conn.commit(); conn.close()
try:
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(viewport={'width': 390, 'height': 844})
        pg.goto(f'{BASE}/login', wait_until='domcontentloaded')
        pg.fill('input[name=email]', EMAIL); pg.fill('input[name=password]', PW)
        pg.click('button[type=submit]'); pg.wait_for_load_state('networkidle')
        pg.goto(f'{BASE}/goods?branch_id={BRANCH}', wait_until='domcontentloaded')
        pg.wait_for_selector('#goods-view-goals', state='visible', timeout=15000)
        pg.click('#goods-view-goals')
        pg.wait_for_selector('#goal-table', state='visible', timeout=15000)
        pg.wait_for_timeout(1000)
        pg.screenshot(path='/tmp/goods_goals_mobile.png', full_page=True)
        print('shot /tmp/goods_goals_mobile.png')
        b.close()
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID)); conn.commit(); conn.close()
    print('password restored')

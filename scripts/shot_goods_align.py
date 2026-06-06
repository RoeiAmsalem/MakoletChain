"""Read-only screenshots of the יעדים view to verify header/cell alignment
(staging, branch 9015) at desktop (1280px) and mobile (390px). No data change.
  -> /tmp/goods_align_desktop.png, /tmp/goods_align_mobile.png
"""
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'
BRANCH = 9015


def open_goals(pg):
    pg.goto(f'{BASE}/goods?branch_id={BRANCH}', wait_until='domcontentloaded')
    pg.click('#goods-view-goals')
    pg.wait_for_selector('#goal-table', state='visible', timeout=15000)
    pg.wait_for_timeout(700)


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
        open_goals(pg)
        pg.screenshot(path='/tmp/goods_align_mobile.png', full_page=True)

        state = pg.context.storage_state()
        ctx2 = b.new_context(viewport={'width': 1280, 'height': 900}, storage_state=state)
        pg2 = ctx2.new_page()
        open_goals(pg2)
        # clip to the goals card so the wide desktop table is fully framed
        card = pg2.query_selector('#goods-goals-view')
        box = card.bounding_box()
        pg2.screenshot(path='/tmp/goods_align_desktop.png',
                       clip={'x': box['x'], 'y': box['y'],
                             'width': min(box['width'], 1280), 'height': box['height']})
        b.close()
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID)); conn.commit(); conn.close()
    print('cleanup: password restored')

print('shot /tmp/goods_align_desktop.png')
print('shot /tmp/goods_align_mobile.png')

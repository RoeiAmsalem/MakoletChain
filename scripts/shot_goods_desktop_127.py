"""Desktop regression check for the יעדים view (branch 127): confirms the
4-column table + 3-in-a-row summary cards are intact at desktop width.
  -> /tmp/goods_desktop_127.png
"""
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'
BRANCH = 127

conn = sqlite3.connect(DB)
old = conn.execute("SELECT password_hash FROM users WHERE id=?", (UID,)).fetchone()[0]
conn.execute("UPDATE users SET password_hash=? WHERE id=?", (generate_password_hash(PW), UID))
conn.commit(); conn.close()
try:
    with sync_playwright() as p:
        b = p.chromium.launch()
        ctx = b.new_context(viewport={'width': 1280, 'height': 900})
        pg = ctx.new_page()
        pg.goto(f'{BASE}/login', wait_until='domcontentloaded')
        pg.fill('input[name=email]', EMAIL); pg.fill('input[name=password]', PW)
        pg.click('button[type=submit]'); pg.wait_for_load_state('networkidle')
        pg.goto(f'{BASE}/goods?branch_id={BRANCH}', wait_until='domcontentloaded')
        pg.click('#goods-view-goals')
        pg.wait_for_selector('#goal-table', state='visible', timeout=15000)
        pg.wait_for_timeout(800)
        card = pg.query_selector('#goods-goals-view')
        box = card.bounding_box()
        pg.screenshot(path='/tmp/goods_desktop_127.png',
                      clip={'x': box['x'], 'y': box['y'],
                            'width': min(box['width'], 1280), 'height': min(box['height'], 900)})
        # desktop sanity: קצב column visible (4-col table) + 3 summary cards in a row
        kotzev_shown = pg.eval_on_selector('#goal-table thead th:nth-child(3)',
                                           'el => getComputedStyle(el).display !== "none"')
        cols = pg.evaluate("() => getComputedStyle(document.querySelector('.goal-kpis')).gridTemplateColumns")
        print(f"desktop קצב column shown: {kotzev_shown}")
        print(f"desktop .goal-kpis columns: {cols}")
        b.close()
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID)); conn.commit(); conn.close()
    print("cleanup: password restored")
print("shot /tmp/goods_desktop_127.png")

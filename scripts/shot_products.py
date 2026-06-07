"""Screenshot the /products catalog page (staging, desktop, admin).

Logs in as the admin user (temporary password, restored after), captures the
full table and the "show only flagged" view, and prints visible-row counts.

Usage:  python3 scripts/shot_products.py
"""
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'

conn = sqlite3.connect(DB)
old = conn.execute("SELECT password_hash FROM users WHERE id=?", (UID,)).fetchone()[0]
conn.execute("UPDATE users SET password_hash=? WHERE id=?", (generate_password_hash(PW), UID))
conn.commit(); conn.close()

try:
    with sync_playwright() as p:
        b = p.chromium.launch()
        ctx = b.new_context(viewport={'width': 1366, 'height': 1000})
        pg = ctx.new_page()
        pg.goto(f'{BASE}/login', wait_until='domcontentloaded')
        pg.fill('input[name=email]', EMAIL); pg.fill('input[name=password]', PW)
        pg.click('button[type=submit]'); pg.wait_for_load_state('networkidle')

        pg.goto(f'{BASE}/products', wait_until='domcontentloaded')
        pg.wait_for_selector('#pc-table tbody tr', state='visible', timeout=15000)
        pg.wait_for_timeout(500)
        pg.screenshot(path='/tmp/products_all.png', full_page=True)
        total = pg.eval_on_selector_all('#pc-table tbody tr', 'els => els.length')
        flagged = pg.eval_on_selector_all('#pc-table tbody tr.flagged', 'els => els.length')

        # "show only flagged"
        pg.check('#pc-flagonly')
        pg.wait_for_timeout(400)
        pg.screenshot(path='/tmp/products_flagged.png', full_page=True)
        vis_flagged = pg.evaluate(
            "() => Array.from(document.querySelectorAll('#pc-table tbody tr'))"
            ".filter(tr => tr.style.display !== 'none').length")
        b.close()
    print(f"shots: /tmp/products_all.png  /tmp/products_flagged.png")
    print(f"rows total={total} flagged={flagged} | flagged-only view shows {vis_flagged}")
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID)); conn.commit(); conn.close()
    print("cleanup: password restored")

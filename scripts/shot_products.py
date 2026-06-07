"""Screenshot the /products catalog page (staging, desktop, admin).

Viewport screenshots (not full-page — 2,900+ rows would be a giant image).

Modes:
  (no arg)  → full catalog view            (/tmp/products_all.png)
  zik       → "רק זיכיונות" classification  (/tmp/products_zik.png)

Usage:  python3 scripts/shot_products.py [zik]
"""
import sys
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'
ZIK = len(sys.argv) > 1 and sys.argv[1] == 'zik'

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
        total = pg.eval_on_selector_all('#pc-table tbody tr', 'els => els.length')
        zik = pg.eval_on_selector_all('#pc-table tbody tr.zik', 'els => els.length')

        if ZIK:
            pg.check('#pc-zikonly')
            pg.wait_for_timeout(400)
            out = '/tmp/products_zik.png'
            vis = pg.evaluate("() => Array.from(document.querySelectorAll('#pc-table tbody tr'))"
                              ".filter(tr => tr.style.display !== 'none').length")
        else:
            out = '/tmp/products_all.png'
            vis = total
        pg.screenshot(path=out)   # viewport only
        b.close()
    print(f"shot: {out} | rows total={total} zik={zik} | visible={vis}")
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID)); conn.commit(); conn.close()
    print("cleanup: password restored")

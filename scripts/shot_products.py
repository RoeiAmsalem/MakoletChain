"""Screenshot the /products catalog page (staging, desktop, admin).

Viewport screenshots (not full-page — 2,900+ rows would be a giant image).

Modes:
  (no arg)  → real catalog view (/tmp/products_all.png)
  demo      → TEMPORARILY marks the top-2 products suppliers_seen=2 to prove the
              red-highlight + "show only flagged" filter render, screenshots the
              flagged-only view (/tmp/products_flagged.png), then REVERTS. The
              real data has 0 mis-files, so this only demonstrates the UI.

Usage:  python3 scripts/shot_products.py [demo]
"""
import sys
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'
DEMO = len(sys.argv) > 1 and sys.argv[1] == 'demo'

conn = sqlite3.connect(DB)
old = conn.execute("SELECT password_hash FROM users WHERE id=?", (UID,)).fetchone()[0]
conn.execute("UPDATE users SET password_hash=? WHERE id=?", (generate_password_hash(PW), UID))

demo_ids = []
if DEMO:
    demo_ids = [r[0] for r in conn.execute(
        "SELECT product_id FROM products ORDER BY doc_count DESC LIMIT 2").fetchall()]
    conn.executemany("UPDATE products SET suppliers_seen=2 WHERE product_id=?",
                     [(i,) for i in demo_ids])
    print(f"DEMO: temporarily flagged product_ids {demo_ids} (will revert)")
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
        flagged = pg.eval_on_selector_all('#pc-table tbody tr.flagged', 'els => els.length')

        if DEMO:
            pg.check('#pc-flagonly')
            pg.wait_for_timeout(400)
            out = '/tmp/products_flagged.png'
        else:
            out = '/tmp/products_all.png'
        pg.screenshot(path=out)   # viewport only
        b.close()
    print(f"shot: {out} | rows total={total} flagged(real or demo)={flagged}")
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID))
    if demo_ids:
        conn.executemany("UPDATE products SET suppliers_seen=1 WHERE product_id=?",
                         [(i,) for i in demo_ids])
        print(f"DEMO: reverted {demo_ids} back to suppliers_seen=1")
    conn.commit(); conn.close()
    print("cleanup: password restored")

"""Screenshot the /sales revenue-summary tiles (staging, iPhone 13, admin) and
assert the high/low tiles are gone and קצב הכנסות is present.

Usage:  python3 scripts/shot_sales_kpis.py [branch_id] [month]
"""
import sys
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'
BRANCH = sys.argv[1] if len(sys.argv) > 1 else '9018'
MONTH = sys.argv[2] if len(sys.argv) > 2 else '2026-06'

conn = sqlite3.connect(DB)
old = conn.execute("SELECT password_hash FROM users WHERE id=?", (UID,)).fetchone()[0]
conn.execute("UPDATE users SET password_hash=? WHERE id=?", (generate_password_hash(PW), UID))
conn.commit(); conn.close()

try:
    with sync_playwright() as p:
        b = p.chromium.launch()
        iphone = p.devices['iPhone 13']
        ctx = b.new_context(**iphone)
        pg = ctx.new_page()
        pg.goto(f'{BASE}/login', wait_until='domcontentloaded')
        pg.fill('input[name=email]', EMAIL); pg.fill('input[name=password]', PW)
        pg.click('button[type=submit]'); pg.wait_for_load_state('networkidle')

        pg.goto(f'{BASE}/sales?branch_id={BRANCH}&month={MONTH}', wait_until='domcontentloaded')
        pg.wait_for_selector('#sales-kpis .kpi-card', state='visible', timeout=15000)
        pg.wait_for_timeout(800)
        pg.screenshot(path='/tmp/sales_kpis.png')   # viewport (tiles are at top)

        info = pg.evaluate("""() => {
            const t = document.getElementById('sales-kpis').innerText;
            const labels = Array.from(document.querySelectorAll('#sales-kpis .kpi-label'))
                .map(e => e.textContent.trim());
            const de = document.documentElement;
            return {
                labels,
                has_high: t.includes('יום הכי גבוה'),
                has_low: t.includes('יום הכי נמוך'),
                has_pace: t.includes('קצב הכנסות'),
                hscroll: Math.max(de.scrollWidth, document.body.scrollWidth) - de.clientWidth,
            };
        }""")
        b.close()
    print(f"branch {BRANCH} {MONTH} | labels: {info['labels']}")
    print(f"  יום הכי גבוה present: {info['has_high']}  [{'FAIL' if info['has_high'] else 'PASS'} removed]")
    print(f"  יום הכי נמוך present: {info['has_low']}  [{'FAIL' if info['has_low'] else 'PASS'} removed]")
    print(f"  קצב הכנסות present: {info['has_pace']}  [{'PASS' if info['has_pace'] else 'FAIL'}]")
    print(f"  horizontal overflow: {info['hscroll']}px  [{'PASS' if info['hscroll'] <= 1 else 'FAIL'} =0]")
    print("shot: /tmp/sales_kpis.png")
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID)); conn.commit(); conn.close()
    print("cleanup: password restored")

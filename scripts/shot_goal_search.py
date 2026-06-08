"""Verify + screenshot the budget supplier list: no duplicate suppliers, and the
search box filters by substring. Staging, admin.

Usage:  python3 scripts/shot_goal_search.py <branch_id> <search_term> <label>
"""
import sys
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'
BRANCH = sys.argv[1]
TERM = sys.argv[2]
LABEL = sys.argv[3] if len(sys.argv) > 3 else 'search'

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

        pg.goto(f'{BASE}/goods?branch_id={BRANCH}', wait_until='domcontentloaded')
        pg.click('#goods-view-goals')
        pg.wait_for_selector('#goal-table tbody tr', state='visible', timeout=15000)
        pg.wait_for_timeout(700)

        # duplicate check: any data-supplier value appearing more than once?
        dups = pg.evaluate("""() => {
            const names = Array.from(document.querySelectorAll('#goal-tbody tr'))
                .map(tr => tr.getAttribute('data-supplier'));
            const seen = {}, dup = [];
            names.forEach(n => { seen[n] = (seen[n]||0)+1; });
            Object.keys(seen).forEach(n => { if (seen[n] > 1) dup.push(n + ' x' + seen[n]); });
            return { total: names.length, dups: dup };
        }""")

        pg.fill('#goal-search', TERM)
        pg.wait_for_timeout(400)
        res = pg.evaluate("""() => {
            const vis = Array.from(document.querySelectorAll('#goal-tbody tr'))
                .filter(tr => tr.style.display !== 'none')
                .map(tr => tr.getAttribute('data-supplier'));
            return { visible: vis.length, names: vis.slice(0, 8) };
        }""")
        pg.screenshot(path=f'/tmp/goal_{LABEL}.png')
        b.close()
    print(f"branch {BRANCH} | total rows={dups['total']} | duplicate data-supplier={dups['dups']}")
    print(f"search '{TERM}' → {res['visible']} visible: {res['names']}")
    print(f"shot: /tmp/goal_{LABEL}.png")
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID)); conn.commit(); conn.close()
    print("cleanup: password restored")

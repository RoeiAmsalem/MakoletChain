"""Mobile 390px screenshot of /goal on staging (branch 9015).

Seeds a few realistic budgets so the יתרה column shows red/green, logs in as
admin (temp password, restored after), switches to branch 9015, and shoots the
full page. The seeded budgets are LEFT in place on staging as demo data.
"""
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'
BRANCH = 9015
# Two below-pace budgets (green) + one over-pace (red), one big roster supplier.
SEED = [
    ('החברה המרכזית  - קוקה קולה', 50000),  # projected ~43.7k -> green
    ('אבאל יזמות', 20000),                    # projected ~26.5k -> red
    ('ויליפוד אינטרנשיונל-יבשים/מצונ', 40000),  # projected ~32.3k -> green
]

conn = sqlite3.connect(DB)
for name, amt in SEED:
    conn.execute(
        "INSERT INTO supplier_budgets (branch_id, supplier_name, monthly_budget) "
        "VALUES (?, ?, ?) ON CONFLICT(branch_id, supplier_name) "
        "DO UPDATE SET monthly_budget=excluded.monthly_budget", (BRANCH, name, amt))
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
        pg.goto(f'{BASE}/goal?branch_id={BRANCH}', wait_until='domcontentloaded')
        pg.wait_for_selector('#goal-table', state='visible', timeout=15000)
        pg.wait_for_timeout(1000)
        pg.screenshot(path='/tmp/goal_mobile.png', full_page=True)
        print('shot /tmp/goal_mobile.png')
        b.close()
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID)); conn.commit(); conn.close()
    print('password restored; seeded budgets kept on staging')

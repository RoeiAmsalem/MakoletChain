"""Screenshots + PASS/FAIL for the /goods budget front-end changes (staging).

  A. יעדים view at 390px (branch 9015): ספק | תקציב | יתרה only, no קצב column,
     no clip — /tmp/goods_yaadim_390.png + asserts the קצב <th> is display:none.
  B. מסמכים → לפי ספק at 390px (branch 9015, current month): budgeted suppliers
     show the "תקציב · יתרה" line — /tmp/goods_grouped_390.png + asserts >=1.
  C. GUARD: לפי ספק on a NON-current month must show NO budget line. Branch 126
     (unfloored) at 2026-05 with a seeded budget on a May supplier → expect 0.

Temp admin password + the seeded 126 budget are both cleaned up at the end.
"""
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'
GUARD_BRANCH, GUARD_SUPPLIER, GUARD_MONTH = 126, 'שטראוס מצונן', '2026-05'

conn = sqlite3.connect(DB)
old = conn.execute("SELECT password_hash FROM users WHERE id=?", (UID,)).fetchone()[0]
conn.execute("UPDATE users SET password_hash=? WHERE id=?", (generate_password_hash(PW), UID))
conn.execute("INSERT INTO supplier_budgets (branch_id, supplier_name, monthly_budget) VALUES (?,?,?) "
             "ON CONFLICT(branch_id, supplier_name) DO UPDATE SET monthly_budget=excluded.monthly_budget",
             (GUARD_BRANCH, GUARD_SUPPLIER, 5000))
conn.commit(); conn.close()

results = {}
try:
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(viewport={'width': 390, 'height': 844})
        pg.goto(f'{BASE}/login', wait_until='domcontentloaded')
        pg.fill('input[name=email]', EMAIL); pg.fill('input[name=password]', PW)
        pg.click('button[type=submit]'); pg.wait_for_load_state('networkidle')

        # A — יעדים view
        pg.goto(f'{BASE}/goods?branch_id=9015', wait_until='domcontentloaded')
        pg.click('#goods-view-goals')
        pg.wait_for_selector('#goal-table', state='visible', timeout=15000)
        pg.wait_for_timeout(800)
        pg.screenshot(path='/tmp/goods_yaadim_390.png', full_page=True)
        kotzev_hidden = pg.eval_on_selector(
            '#goal-table thead th:nth-child(3)',
            'el => getComputedStyle(el).display === "none"')
        results['A koTzev column hidden @390'] = kotzev_hidden

        # B — לפי ספק grouped, current month (control: annotations present)
        pg.goto(f'{BASE}/goods?branch_id=9015&view=grouped', wait_until='domcontentloaded')
        pg.wait_for_selector('.supplier-group', state='visible', timeout=15000)
        pg.wait_for_timeout(1500)
        results['B grouped sg-budget count (9015, current)'] = pg.locator('.sg-budget').count()
        pg.screenshot(path='/tmp/goods_grouped_390.png', full_page=True)

        # C — GUARD: grouped on a non-current month must NOT annotate
        pg.goto(f'{BASE}/goods?branch_id={GUARD_BRANCH}&view=grouped&month={GUARD_MONTH}',
                wait_until='domcontentloaded')
        pg.wait_for_selector('.supplier-group', state='visible', timeout=15000)
        pg.wait_for_timeout(1500)
        results['C grouped sg-budget count (126, 2026-05 non-current)'] = pg.locator('.sg-budget').count()
        b.close()
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID))
    conn.execute("DELETE FROM supplier_budgets WHERE branch_id=? AND supplier_name=?",
                 (GUARD_BRANCH, GUARD_SUPPLIER))
    conn.commit(); conn.close()
    print('cleanup: password restored, seeded 126 budget removed')

print('shot /tmp/goods_yaadim_390.png')
print('shot /tmp/goods_grouped_390.png')
for k, v in results.items():
    print(f'  {k}: {v}')
a_pass = results.get('A koTzev column hidden @390') is True
b_pass = results.get('B grouped sg-budget count (9015, current)', 0) >= 1
c_pass = results.get('C grouped sg-budget count (126, 2026-05 non-current)', -1) == 0
print(f"TASK1 (יעדים fits @390, no קצב col): {'PASS' if a_pass else 'FAIL'}")
print(f"TASK2 (לפי ספק annotated, current): {'PASS' if b_pass else 'FAIL'}")
print(f"GUARD (no annotation non-current):  {'PASS' if c_pass else 'FAIL'}")

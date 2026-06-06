"""Screenshots + edit-flow check for the formatted תקציב inputs (staging).

  - יעדים view at desktop (1280px) and mobile (390px), branch 9015: תקציב shows
    ₪ + commas like קצב/יתרה.  -> /tmp/goods_budget_desktop.png, _mobile.png
  - Edit flow on the קוקה קולה row: idle shows "₪ 50,000", focus shows the raw
    "50000", change to 55000 + blur reformats to "₪ 55,000" and persists to the
    DB. Restores 50000 afterwards.
"""
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'
BRANCH, SUP_LIKE = 9015, '%קוקה קולה%'


def db_budget():
    c = sqlite3.connect(DB)
    row = c.execute("SELECT monthly_budget FROM supplier_budgets WHERE branch_id=? AND supplier_name LIKE ?",
                    (BRANCH, SUP_LIKE)).fetchone()
    c.close()
    return row[0] if row else None


def open_goals(pg):
    pg.goto(f'{BASE}/goods?branch_id={BRANCH}', wait_until='domcontentloaded')
    pg.click('#goods-view-goals')
    pg.wait_for_selector('#goal-table', state='visible', timeout=15000)
    pg.wait_for_timeout(700)


conn = sqlite3.connect(DB)
old = conn.execute("SELECT password_hash FROM users WHERE id=?", (UID,)).fetchone()[0]
conn.execute("UPDATE users SET password_hash=? WHERE id=?", (generate_password_hash(PW), UID))
conn.commit(); conn.close()

res = {}
try:
    with sync_playwright() as p:
        b = p.chromium.launch()
        # login (mobile page)
        pg = b.new_page(viewport={'width': 390, 'height': 844})
        pg.goto(f'{BASE}/login', wait_until='domcontentloaded')
        pg.fill('input[name=email]', EMAIL); pg.fill('input[name=password]', PW)
        pg.click('button[type=submit]'); pg.wait_for_load_state('networkidle')

        # mobile screenshot
        open_goals(pg)
        pg.screenshot(path='/tmp/goods_budget_mobile.png', full_page=True)

        # edit flow on the קוקה קולה row (mobile page)
        inp = pg.locator('#goal-tbody tr', has_text='קוקה קולה').locator('.goal-budget-input')
        res['idle'] = inp.input_value()
        inp.click()
        res['focus'] = inp.input_value()
        inp.fill('55000')
        pg.keyboard.press('Tab')
        pg.wait_for_timeout(1500)  # save + re-render
        inp2 = pg.locator('#goal-tbody tr', has_text='קוקה קולה').locator('.goal-budget-input')
        res['after_blur'] = inp2.input_value()
        res['db_after'] = db_budget()

        # desktop screenshot (reuse session via storage_state)
        state = pg.context.storage_state()
        ctx2 = b.new_context(viewport={'width': 1280, 'height': 900}, storage_state=state)
        pg2 = ctx2.new_page()
        open_goals(pg2)
        pg2.screenshot(path='/tmp/goods_budget_desktop.png')
        b.close()
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID))
    conn.execute("UPDATE supplier_budgets SET monthly_budget=50000 WHERE branch_id=? AND supplier_name LIKE ?",
                 (BRANCH, SUP_LIKE))
    conn.commit(); conn.close()
    print('cleanup: password restored, קוקה קולה budget reset to 50000')

print('shot /tmp/goods_budget_desktop.png')
print('shot /tmp/goods_budget_mobile.png')
for k in ('idle', 'focus', 'after_blur', 'db_after'):
    print(f'  {k}: {res.get(k)!r}')
ok = (res.get('idle') == '₪ 50,000' and res.get('focus') == '50000'
      and res.get('after_blur') == '₪ 55,000' and res.get('db_after') == 55000)
print(f"EDIT FLOW (idle fmt → focus raw → blur fmt → persisted): {'PASS' if ok else 'FAIL'}")

"""Verify the budgeted-only summary totals on branch 127 (תיכון) + screenshot.

Prints _goal_data totals (סה"כ תקציב / קצב חזוי / יתרה), the per-supplier rows,
and the /goods reconciliation (Σ per-supplier mtd == /goods pre-VAT MTD total).
Then captures the יעדים view under a real iPhone 13 device profile.

Usage:  python3 scripts/diag_goal_127.py
"""
import sqlite3
import app as app_module
from app import app, _goal_data, _goods_doc_context
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

BRANCH = 127
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'


def conn():
    c = sqlite3.connect(app_module.DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


# ── numbers ──
c = conn()
data = _goal_data(BRANCH, c)
t = data['totals']
month = data['month']
goods_total = _goods_doc_context(BRANCH, month, c)['total_before_vat']
c.close()

print(f"=== branch {BRANCH} — month {month}, day {data['days_elapsed']}/{data['days_in_month']} ===")
print(f"סה\"כ תקציב : {t['budget']:.2f}")
print(f"קצב חזוי   : {t['projected']:.2f}")
print(f"יתרה       : {t['remaining']:.2f}")
budgeted = [s for s in data['suppliers'] if s['budget']]
print(f"\nbudgeted suppliers ({len(budgeted)}):")
for s in budgeted:
    print(f"  {s['supplier_name']} | budget {s['budget']:.0f} | proj {s['projected']:.2f} | rem {s['remaining']:.2f}")
print(f"\nper-supplier rows total: {len(data['suppliers'])} suppliers (ALL listed, unchanged)")
sum_mtd = round(sum(s['mtd_spend'] for s in data['suppliers']), 2)
match = 'MATCH' if abs(sum_mtd - goods_total) < 0.02 else 'MISMATCH'
print(f"reconcile: Σ per-supplier mtd = {sum_mtd:.2f} | /goods MTD = {goods_total:.2f} -> {match}")

# ── screenshot under real iPhone 13 profile ──
c = conn()
old = c.execute("SELECT password_hash FROM users WHERE id=?", (UID,)).fetchone()[0]
c.execute("UPDATE users SET password_hash=? WHERE id=?", (generate_password_hash(PW), UID))
c.commit(); c.close()
try:
    with sync_playwright() as p:
        iphone = p.devices['iPhone 13']
        b = p.chromium.launch()
        ctx = b.new_context(**iphone)
        pg = ctx.new_page()
        pg.goto(f'{BASE}/login', wait_until='domcontentloaded')
        pg.fill('input[name=email]', EMAIL); pg.fill('input[name=password]', PW)
        pg.click('button[type=submit]'); pg.wait_for_load_state('networkidle')
        pg.goto(f'{BASE}/goods?branch_id={BRANCH}', wait_until='domcontentloaded')
        pg.click('#goods-view-goals')
        pg.wait_for_selector('#goal-table', state='visible', timeout=15000)
        pg.wait_for_timeout(900)
        pg.screenshot(path='/tmp/goal_127_totals.png', full_page=True)
        b.close()
finally:
    c = conn()
    c.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID)); c.commit(); c.close()
    print("\ncleanup: password restored")
print("shot /tmp/goal_127_totals.png")

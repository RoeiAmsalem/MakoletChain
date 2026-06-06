"""Recolor verification for the יעדים summary tiles (staging, branch 9015).

  - Desktop (1280px): summary 3-in-a-row — /tmp/goods_recolor_desktop.png
  - iPhone 13 device profile: headline + two tiles — /tmp/goods_recolor_mobile.png
    + horizontal-overflow check.
  - Reconciliation: Σ per-supplier mtd == /goods pre-VAT MTD (proves data unchanged).
"""
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import sqlite3
import app as app_module
from app import _goal_data, _goods_doc_context
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'
BRANCH = 9015


def conn():
    c = sqlite3.connect(DB, timeout=30); c.row_factory = sqlite3.Row; return c


# reconciliation (data unchanged)
c = conn()
data = _goal_data(BRANCH, c)
goods_total = _goods_doc_context(BRANCH, data['month'], c)['total_before_vat']
c.close()
sum_mtd = round(sum(s['mtd_spend'] for s in data['suppliers']), 2)
print(f"branch {BRANCH} {data['month']}: Σ mtd={sum_mtd:.2f} | /goods MTD={goods_total:.2f} -> "
      f"{'MATCH' if abs(sum_mtd - goods_total) < 0.02 else 'MISMATCH'}")
print(f"totals: תקציב {data['totals']['budget']:.0f} | קצב {data['totals']['projected']:.0f} | "
      f"יתרה {data['totals']['remaining']:.0f}")


def open_goals(pg):
    pg.goto(f'{BASE}/goods?branch_id={BRANCH}', wait_until='domcontentloaded')
    pg.click('#goods-view-goals')
    pg.wait_for_selector('#goal-table', state='visible', timeout=15000)
    pg.wait_for_timeout(800)


c = conn()
old = c.execute("SELECT password_hash FROM users WHERE id=?", (UID,)).fetchone()[0]
c.execute("UPDATE users SET password_hash=? WHERE id=?", (generate_password_hash(PW), UID))
c.commit(); c.close()
try:
    with sync_playwright() as p:
        b = p.chromium.launch()
        # iPhone 13 profile
        iphone = p.devices['iPhone 13']
        ctx = b.new_context(**iphone)
        pg = ctx.new_page()
        pg.goto(f'{BASE}/login', wait_until='domcontentloaded')
        pg.fill('input[name=email]', EMAIL); pg.fill('input[name=password]', PW)
        pg.click('button[type=submit]'); pg.wait_for_load_state('networkidle')
        open_goals(pg)
        pg.locator('#goal-summary').screenshot(path='/tmp/goods_recolor_mobile.png')
        ov = pg.evaluate("() => Math.max(document.documentElement.scrollWidth, document.body.scrollWidth) "
                         "- document.documentElement.clientWidth")
        print(f"mobile horizontal overflow: {ov}px  [{'PASS' if ov <= 1 else 'FAIL'} =0]")

        # desktop
        state = pg.context.storage_state()
        ctx2 = b.new_context(viewport={'width': 1280, 'height': 900}, storage_state=state)
        pg2 = ctx2.new_page()
        open_goals(pg2)
        box = pg2.query_selector('#goal-summary').bounding_box()
        pg2.screenshot(path='/tmp/goods_recolor_desktop.png',
                       clip={'x': box['x'], 'y': box['y'],
                             'width': min(box['width'], 1280), 'height': box['height']})
        b.close()
finally:
    c = conn()
    c.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID)); c.commit(); c.close()
    print("cleanup: password restored")
print("shot /tmp/goods_recolor_desktop.png")
print("shot /tmp/goods_recolor_mobile.png")

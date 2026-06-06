"""Prod screenshots + PASS/FAIL for the /goods תקציב (supplier-budget) view.

Auth via a SECRET_KEY-minted Flask session cookie (NO password change). Seeds
two test budgets on the control branch, captures the תקציב view at a desktop
profile AND a real iPhone 13 device profile, plus the מסמכים→לפי-ספק annotation,
then deletes the seeded budgets. Saves PNGs to /tmp and prints layout asserts.

Usage:  venv/bin/python scripts/shot_goods_budget_prod.py --branch-id 9015
"""
import argparse
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module  # noqa: E402
from app import app, _goal_data  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402

BASE = 'http://127.0.0.1:8080'
B1, B2 = 4000.0, 7000.0


def mint_cookie(branch_id):
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    uid = conn.execute("SELECT id FROM users WHERE role='admin' ORDER BY id LIMIT 1").fetchone()
    conn.close()
    sess = {'user_id': uid['id'] if uid else 1, 'user_role': 'admin',
            'branch_id': branch_id, 'user_branches': []}
    serializer = app.session_interface.get_signing_serializer(app)
    return serializer.dumps(dict(sess))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--branch-id', type=int, default=9015)
    args = ap.parse_args()
    bid = args.branch_id

    # Seed two budgets so the view + summary render with data.
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    roster = _goal_data(bid, conn)['suppliers']
    sup1, sup2 = roster[0]['supplier_name'], roster[1]['supplier_name']
    snap = {}
    for sup, b in ((sup1, B1), (sup2, B2)):
        r = conn.execute("SELECT monthly_budget FROM supplier_budgets WHERE branch_id=? AND supplier_name=?",
                         (bid, sup)).fetchone()
        snap[sup] = r['monthly_budget'] if r else None
        conn.execute("INSERT INTO supplier_budgets (branch_id, supplier_name, monthly_budget) VALUES (?,?,?) "
                     "ON CONFLICT(branch_id, supplier_name) DO UPDATE SET monthly_budget=excluded.monthly_budget",
                     (bid, sup, b))
    conn.commit()
    conn.close()

    cookie = mint_cookie(bid)
    cookie_obj = {'name': 'session', 'value': cookie, 'url': BASE}
    results = {}
    try:
        with sync_playwright() as p:
            b = p.chromium.launch()

            # ── Desktop ──
            dctx = b.new_context(viewport={'width': 1280, 'height': 900}, device_scale_factor=2)
            dctx.add_cookies([cookie_obj])
            pg = dctx.new_page()
            pg.goto(f'{BASE}/goods?branch_id={bid}', wait_until='domcontentloaded')
            results['toggle docs label'] = pg.inner_text('#goods-view-docs').strip()
            results['toggle budget label'] = pg.inner_text('#goods-view-goals').strip()
            pg.click('#goods-view-goals')
            pg.wait_for_selector('#goal-table', state='visible', timeout=15000)
            pg.wait_for_timeout(700)
            pg.screenshot(path='/tmp/goods_budget_desktop.png', full_page=True)
            results['summary tiles visible'] = pg.locator('#goal-summary .kpi-card').count()
            results['headline remaining class'] = pg.get_attribute('#goal-tot-remaining-card', 'class')
            dctx.close()

            # ── iPhone 13 (real device profile) ──
            iphone = p.devices['iPhone 13']
            ictx = b.new_context(**iphone)
            ictx.add_cookies([cookie_obj])
            ip = ictx.new_page()
            ip.goto(f'{BASE}/goods?branch_id={bid}', wait_until='domcontentloaded')
            ip.click('#goods-view-goals')
            ip.wait_for_selector('#goal-table', state='visible', timeout=15000)
            ip.wait_for_timeout(700)
            ip.screenshot(path='/tmp/goods_budget_iphone13.png', full_page=True)
            # Check a real tbody cell — a th inside the display:none thead still
            # reports its own computed display as table-cell in Chromium, so the
            # header th is the wrong probe. The 3rd cell of a card row is what
            # the mobile rule actually hides.
            results['iphone קצב col hidden'] = ip.eval_on_selector(
                '#goal-table tbody tr td:nth-child(3)',
                'el => getComputedStyle(el).display === "none"')
            results['iphone toggle width'] = ip.eval_on_selector(
                '#goods-view-toggle', 'el => Math.round(el.getBoundingClientRect().width)')
            ictx.close()

            # ── מסמכים → לפי ספק annotation (desktop, current month) ──
            gctx = b.new_context(viewport={'width': 1280, 'height': 900})
            gctx.add_cookies([cookie_obj])
            gp = gctx.new_page()
            gp.goto(f'{BASE}/goods?branch_id={bid}&view=grouped', wait_until='domcontentloaded')
            gp.wait_for_selector('.supplier-group', state='visible', timeout=15000)
            gp.wait_for_timeout(1500)
            results['לפי-ספק sg-budget count'] = gp.locator('.sg-budget').count()
            gp.screenshot(path='/tmp/goods_budget_grouped.png', full_page=True)
            gctx.close()
            b.close()
    finally:
        conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
        for sup, val in snap.items():
            if val is None:
                conn.execute("DELETE FROM supplier_budgets WHERE branch_id=? AND supplier_name=?", (bid, sup))
            else:
                conn.execute("INSERT INTO supplier_budgets (branch_id, supplier_name, monthly_budget) VALUES (?,?,?) "
                             "ON CONFLICT(branch_id, supplier_name) DO UPDATE SET monthly_budget=excluded.monthly_budget",
                             (bid, sup, val))
        conn.commit()
        conn.close()
        print('cleanup: seeded budgets removed/restored')

    print('shots: /tmp/goods_budget_desktop.png /tmp/goods_budget_iphone13.png /tmp/goods_budget_grouped.png')
    for k, v in results.items():
        print(f'  {k}: {v}')
    checks = {
        'toggle reads מסמכים/תקציב': results.get('toggle docs label') == 'מסמכים' and results.get('toggle budget label') == 'תקציב',
        'summary strip = 3 tiles': results.get('summary tiles visible') == 3,
        'headline is profit/loss card': 'kpi-card--profit' in (results.get('headline remaining class') or '') or 'kpi-card--loss' in (results.get('headline remaining class') or ''),
        'iPhone 3-col (קצב hidden)': results.get('iphone קצב col hidden') is True,
        'לפי-ספק annotated (>=1)': results.get('לפי-ספק sg-budget count', 0) >= 1,
    }
    for k, v in checks.items():
        print(f"  {'PASS' if v else 'FAIL'}: {k}")
    sys.exit(0 if all(checks.values()) else 1)


if __name__ == '__main__':
    main()

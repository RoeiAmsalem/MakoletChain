"""Prod screenshots + PASS/FAIL for the /goods תקציב (supplier-budget) view —
actual-spending model: 4-tile strip (incl. slate קצב הזמנות), 5-col table
(תקציב · הוצאה · קצב · יתרה), green/red/neutral row tints.

Auth via a SECRET_KEY-minted Flask session cookie (NO password change). Seeds
budgets that produce BOTH an under (green) and an over (red) row plus leaves a
roster supplier with 0 הוצאה, captures desktop + a real iPhone 13 device
profile, then restores budgets (net-zero). Saves PNGs to /tmp + prints asserts.

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


def mint_cookie(branch_id):
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    uid = conn.execute("SELECT id FROM users WHERE role='admin' ORDER BY id LIMIT 1").fetchone()
    conn.close()
    sess = {'user_id': uid['id'] if uid else 1, 'user_role': 'admin',
            'branch_id': branch_id, 'user_branches': []}
    return app.session_interface.get_signing_serializer(app).dumps(dict(sess))


PROBE = """() => {
    const rows = [...document.querySelectorAll('#goal-table tbody tr')];
    const de = document.documentElement;
    const txt = id => (document.getElementById(id)||{}).textContent || '';
    return {
        rows: rows.length,
        toggle_budget: (document.getElementById('goods-view-goals')||{}).textContent || '',
        strip_tiles: document.querySelectorAll('#goal-summary .goal-kpis .kpi-card').length,
        has_pace_tile: !!document.querySelector('#goal-summary .goal-kpi-pace'),
        orderpace: txt('goal-tot-orderpace'),
        tot_spent: txt('goal-tot-spent'),
        header_spend: [...document.querySelectorAll('#goal-table thead th')].some(th => th.textContent.trim() === 'הוצאה'),
        header_pace: [...document.querySelectorAll('#goal-table thead th')].some(th => th.textContent.trim() === 'קצב'),
        spend_cells: document.querySelectorAll('#goal-table tbody td.goal-spend').length,
        pace_cells: document.querySelectorAll('#goal-table tbody td.goal-pace').length,
        under: document.querySelectorAll('#goal-table tbody tr.goal-under').length,
        over: document.querySelectorAll('#goal-table tbody tr.goal-over').length,
        zero_spend_rows: rows.filter(r => (r.querySelector('td.goal-spend')||{}).textContent.replace(/[^0-9]/g,'') === '0').length,
        hscroll: Math.max(de.scrollWidth, document.body.scrollWidth) - de.clientWidth,
    };
}"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--branch-id', type=int, default=9015)
    args = ap.parse_args()
    bid = args.branch_id

    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    roster = _goal_data(bid, conn)['suppliers']
    # over (red): top spender, budget < its spend. under (green): a 0-spend
    # roster supplier, budget 5000 (also proves a roster supplier with 0 הוצאה).
    spender = next((s for s in roster if s['mtd_spend'] > 0), roster[0])
    zero = next((s for s in roster if s['mtd_spend'] == 0), roster[-1])
    seeds = {spender['supplier_name']: max(1.0, round(spender['mtd_spend'] / 2)),
             zero['supplier_name']: 5000.0}
    snap = {}
    for sup, val in seeds.items():
        r = conn.execute("SELECT monthly_budget FROM supplier_budgets WHERE branch_id=? AND supplier_name=?",
                         (bid, sup)).fetchone()
        snap[sup] = r['monthly_budget'] if r else None
        conn.execute("INSERT INTO supplier_budgets (branch_id, supplier_name, monthly_budget) VALUES (?,?,?) "
                     "ON CONFLICT(branch_id, supplier_name) DO UPDATE SET monthly_budget=excluded.monthly_budget",
                     (bid, sup, val))
    conn.commit()
    conn.close()

    cookie_obj = {'name': 'session', 'value': mint_cookie(bid), 'url': BASE}
    out = {}
    try:
        with sync_playwright() as p:
            b = p.chromium.launch()

            dctx = b.new_context(viewport={'width': 1366, 'height': 900}, device_scale_factor=2)
            dctx.add_cookies([cookie_obj])
            pg = dctx.new_page()
            pg.goto(f'{BASE}/goods?branch_id={bid}', wait_until='domcontentloaded')
            pg.click('#goods-view-goals')
            pg.wait_for_selector('#goal-table tbody tr', state='visible', timeout=15000)
            pg.wait_for_timeout(700)
            pg.screenshot(path='/tmp/goods_budget_desktop.png', full_page=True)
            out['desktop'] = pg.evaluate(PROBE)
            dctx.close()

            iphone = p.devices['iPhone 13']
            ictx = b.new_context(**iphone)
            ictx.add_cookies([cookie_obj])
            ip = ictx.new_page()
            ip.goto(f'{BASE}/goods?branch_id={bid}', wait_until='domcontentloaded')
            ip.click('#goods-view-goals')
            ip.wait_for_selector('#goal-table tbody tr', state='visible', timeout=15000)
            ip.wait_for_timeout(700)
            ip.screenshot(path='/tmp/goods_budget_iphone13.png', full_page=True)
            m = ip.evaluate(PROBE)
            m['input_h'] = ip.eval_on_selector('.goal-budget-input', 'el => Math.round(el.getBoundingClientRect().height)')
            m['input_font'] = ip.eval_on_selector('.goal-budget-input', 'el => getComputedStyle(el).fontSize')
            out['iphone'] = m
            ictx.close()
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
        print('cleanup: seeded budgets removed/restored (net-zero)')

    d, i = out['desktop'], out['iphone']
    print('shots: /tmp/goods_budget_desktop.png  /tmp/goods_budget_iphone13.png')
    print(f"desktop: {d}")
    print(f"iphone : {i}")
    checks = {
        'toggle reads תקציב': d['toggle_budget'].strip() == 'תקציב',
        'strip = 4 tiles (desktop+mobile)': d['strip_tiles'] == 4 and i['strip_tiles'] == 4,
        'slate קצב הזמנות tile + value': d['has_pace_tile'] and d['orderpace'] not in ('', '—'),
        'הוצאה + קצב columns (desktop)': d['header_spend'] and d['header_pace'],
        'הוצאה/קצב cells populated': d['spend_cells'] == d['rows'] and d['pace_cells'] == d['rows'],
        'row tints: >=1 green + >=1 red': d['under'] >= 1 and d['over'] >= 1,
        'roster supplier with 0 הוצאה shown': d['zero_spend_rows'] >= 1,
        'קצב הזמנות > budgeted הוצאה': _num(d['orderpace']) > _num(d['tot_spent']),
        'mobile 0 horizontal overflow': i['hscroll'] <= 1,
        'mobile input >=44px / >=16px': i['input_h'] >= 44 and float(str(i['input_font']).replace('px', '') or 0) >= 16,
    }
    for k, v in checks.items():
        print(f"  {'PASS' if v else 'FAIL'}: {k}")
    sys.exit(0 if all(checks.values()) else 1)


def _num(s):
    try:
        return float(str(s).replace('₪', '').replace(',', '').strip() or 0)
    except ValueError:
        return 0.0


if __name__ == '__main__':
    main()

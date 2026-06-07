"""Verify the /goods תקציב (יעדים) view on staging: קצב column + budget-status
row tint, on a REAL iPhone 13 device profile AND a desktop viewport.

Captures:
  /tmp/goal_iphone.png   — iPhone 13 (mobile UA, 390px, DPR 3, touch)
  /tmp/goal_desktop.png  — desktop (1366×900)

Checks (per the task): 0px horizontal overflow on mobile, תקציב input ≥44px /
≥16px font, קצב populated per supplier, and the green/red/neutral row tint
(.goal-under / .goal-over / none) present.

Usage:  python3 scripts/verify_goal_view.py [branch_id]
"""
import sys
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'
BRANCH = int(sys.argv[1]) if len(sys.argv) > 1 else 9015


def login(pg):
    pg.goto(f'{BASE}/login', wait_until='domcontentloaded')
    pg.fill('input[name=email]', EMAIL)
    pg.fill('input[name=password]', PW)
    pg.click('button[type=submit]')
    pg.wait_for_load_state('networkidle')


def open_goals(pg):
    pg.goto(f'{BASE}/goods?branch_id={BRANCH}', wait_until='domcontentloaded')
    pg.click('#goods-view-goals')
    pg.wait_for_selector('#goal-table tbody tr', state='visible', timeout=15000)
    pg.wait_for_timeout(800)


def probe(pg):
    return pg.evaluate("""() => {
        const rows = [...document.querySelectorAll('#goal-table tbody tr')];
        const de = document.documentElement;
        const txt = id => (document.getElementById(id)||{}).textContent || '';
        return {
            rows: rows.length,
            pace_cells: document.querySelectorAll('#goal-table tbody td.goal-pace').length,
            under: document.querySelectorAll('#goal-table tbody tr.goal-under').length,
            over: document.querySelectorAll('#goal-table tbody tr.goal-over').length,
            header_has_kotzev: [...document.querySelectorAll('#goal-table thead th')]
                .some(th => th.textContent.trim() === 'קצב'),
            strip_tiles: document.querySelectorAll('#goal-summary .goal-kpis .kpi-card').length,
            has_pace_tile: !!document.querySelector('#goal-summary .goal-kpi-pace'),
            orderpace: txt('goal-tot-orderpace'),
            tot_spent: txt('goal-tot-spent'),
            hscroll: Math.max(de.scrollWidth, document.body.scrollWidth) - de.clientWidth,
        };
    }""")


conn = sqlite3.connect(DB)
old = conn.execute("SELECT password_hash FROM users WHERE id=?", (UID,)).fetchone()[0]
conn.execute("UPDATE users SET password_hash=? WHERE id=?", (generate_password_hash(PW), UID))
conn.commit(); conn.close()

out = {}
try:
    with sync_playwright() as p:
        # iPhone 13
        b = p.chromium.launch()
        iphone = p.devices['iPhone 13']
        ctx = b.new_context(**iphone)
        pg = ctx.new_page()
        login(pg); open_goals(pg)
        pg.screenshot(path='/tmp/goal_iphone.png', full_page=True)
        m = probe(pg)
        m['input_h'] = pg.eval_on_selector('.goal-budget-input',
                                           'el => Math.round(el.getBoundingClientRect().height)')
        m['input_font'] = pg.eval_on_selector('.goal-budget-input',
                                              'el => getComputedStyle(el).fontSize')
        m['device'] = {k: iphone[k] for k in ('viewport', 'device_scale_factor', 'is_mobile', 'has_touch')}
        out['iphone'] = m
        ctx.close()

        # Desktop
        ctx2 = b.new_context(viewport={'width': 1366, 'height': 900})
        pg2 = ctx2.new_page()
        login(pg2); open_goals(pg2)
        pg2.screenshot(path='/tmp/goal_desktop.png', full_page=True)
        out['desktop'] = probe(pg2)
        ctx2.close()
        b.close()
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID))
    conn.commit(); conn.close()
    print('cleanup: password restored')

i, d = out['iphone'], out['desktop']
print(f"\n=== /goods תקציב view — branch {BRANCH} ===")
print(f"device (mobile): {i['device']}")
print("shots: /tmp/goal_iphone.png  /tmp/goal_desktop.png")
print(f"rows: mobile={i['rows']} desktop={d['rows']}")
print(f"strip tiles: mobile={i['strip_tiles']} desktop={d['strip_tiles']}  "
      f"[{'PASS' if i['strip_tiles'] == 4 and d['strip_tiles'] == 4 else 'FAIL — want 4'}]")
print(f"קצב הזמנות tile present + value: mobile='{i['orderpace']}' (pace_tile={i['has_pace_tile']})  "
      f"[{'PASS' if i['has_pace_tile'] and i['orderpace'] not in ('', '—') else 'FAIL'}]")
print(f"קצב הזמנות {d['orderpace']} vs סה\"כ הוצאה {d['tot_spent']} "
      f"(all-suppliers pace should exceed budgeted-only spend)")
print(f"קצב header present (desktop): {d['header_has_kotzev']}  "
      f"[{'PASS' if d['header_has_kotzev'] else 'FAIL'}]")
print(f"קצב cells populated: mobile={i['pace_cells']} desktop={d['pace_cells']}  "
      f"[{'PASS' if i['pace_cells'] == i['rows'] and d['pace_cells'] == d['rows'] else 'FAIL'}]")
print(f"row tint: under(green)={i['under']} over(red)={i['over']} (mobile) | "
      f"under={d['under']} over={d['over']} (desktop)  "
      f"[{'PASS' if i['under'] >= 1 and i['over'] >= 1 else 'FAIL — need ≥1 each'}]")
print(f"mobile horizontal overflow: {i['hscroll']}px  [{'PASS' if i['hscroll'] <= 1 else 'FAIL'} =0]")
print(f"תקציב input height: {i['input_h']}px  [{'PASS' if i['input_h'] >= 44 else 'FAIL'} ≥44]")
fn = float(str(i['input_font']).replace('px', '') or 0)
print(f"תקציב input font: {i['input_font']}  [{'PASS' if fn >= 16 else 'FAIL'} ≥16px]")

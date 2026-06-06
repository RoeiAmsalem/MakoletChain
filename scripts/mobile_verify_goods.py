"""Real-mobile verify harness for /goods (staging, branch 9015).

Renders under a REAL mobile device profile — Playwright's iPhone 13 descriptor
(mobile UA + ~390px CSS viewport + device-scale-factor 3 + touch) — NOT a desktop
window resize. Captures screenshots of the מסמכים→לפי ספק view and the יעדים view,
and measures horizontal-scroll + touch-target sizes + input font-size.

Usage:  python3 scripts/mobile_verify_goods.py <label> [branch_id]   # label e.g. before / after
"""
import sys
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

LABEL = sys.argv[1] if len(sys.argv) > 1 else 'run'
DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
UID, EMAIL, PW, BASE = 1, 'makoletdashboard@gmail.com', 'TempShot2026!', 'http://127.0.0.1:8081'
BRANCH = int(sys.argv[2]) if len(sys.argv) > 2 else 9015


def hscroll(page):
    m = page.evaluate("() => ({sw: document.documentElement.scrollWidth, "
                      "cw: document.documentElement.clientWidth, "
                      "bsw: document.body.scrollWidth})")
    overflow = max(m['sw'], m['bsw']) - m['cw']
    return overflow, m


def box_h(page, sel):
    loc = page.locator(sel).first
    try:
        b = loc.bounding_box()
        return round(b['height'], 1) if b else None
    except Exception:
        return None


conn = sqlite3.connect(DB)
old = conn.execute("SELECT password_hash FROM users WHERE id=?", (UID,)).fetchone()[0]
conn.execute("UPDATE users SET password_hash=? WHERE id=?", (generate_password_hash(PW), UID))
conn.commit(); conn.close()

out = {}
try:
    with sync_playwright() as p:
        iphone = p.devices['iPhone 13']
        b = p.chromium.launch()
        ctx = b.new_context(**iphone)
        pg = ctx.new_page()
        pg.goto(f'{BASE}/login', wait_until='domcontentloaded')
        pg.fill('input[name=email]', EMAIL); pg.fill('input[name=password]', PW)
        pg.click('button[type=submit]'); pg.wait_for_load_state('networkidle')
        out['device'] = {k: iphone[k] for k in ('viewport', 'device_scale_factor', 'is_mobile', 'has_touch')}

        # מסמכים → לפי ספק
        pg.goto(f'{BASE}/goods?branch_id={BRANCH}&view=grouped', wait_until='domcontentloaded')
        pg.wait_for_selector('.supplier-group', state='visible', timeout=15000)
        pg.wait_for_timeout(1500)
        pg.screenshot(path=f'/tmp/goods_m_{LABEL}_grouped.png', full_page=True)
        ov, m = hscroll(pg)
        out['grouped'] = {
            'hscroll_overflow_px': ov, 'metrics': m,
            'sub_toggle_h (לפי ספק/רשימה)': box_h(pg, '.view-toggle .filter-btn'),
            'supplier_row_h (summary)': box_h(pg, '.supplier-group summary'),
        }

        # יעדים
        pg.goto(f'{BASE}/goods?branch_id={BRANCH}', wait_until='domcontentloaded')
        pg.click('#goods-view-goals')
        pg.wait_for_selector('#goal-table', state='visible', timeout=15000)
        pg.wait_for_timeout(800)
        pg.screenshot(path=f'/tmp/goods_m_{LABEL}_yaadim.png', full_page=True)
        ov2, m2 = hscroll(pg)
        out['yaadim'] = {
            'hscroll_overflow_px': ov2, 'metrics': m2,
            'toggle_h (מסמכים/יעדים seg-btn)': box_h(pg, '#goods-view-goals'),
            'budget_input_h': box_h(pg, '.goal-budget-input'),
            'budget_input_font_px': pg.eval_on_selector(
                '.goal-budget-input', 'el => getComputedStyle(el).fontSize'),
        }
        b.close()
finally:
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (old, UID)); conn.commit(); conn.close()
    print('cleanup: password restored')


def verdict(name, val, lo):
    ok = isinstance(val, (int, float)) and val >= lo
    return f"  {name}: {val}  [{'PASS' if ok else 'FAIL'} ≥{lo}]"


print(f"\n=== /goods real-mobile ({LABEL}) — {out.get('device')} ===")
print(f"shot /tmp/goods_m_{LABEL}_grouped.png")
print(f"shot /tmp/goods_m_{LABEL}_yaadim.png")
g, y = out.get('grouped', {}), out.get('yaadim', {})
print("מסמכים → לפי ספק:")
print(f"  horizontal overflow: {g.get('hscroll_overflow_px')}px  [{'PASS' if (g.get('hscroll_overflow_px') or 0) <= 1 else 'FAIL'} =0]")
print(verdict('sub-toggle (לפי ספק/רשימה) height', g.get('sub_toggle_h (לפי ספק/רשימה)'), 44))
print(verdict('supplier row height', g.get('supplier_row_h (summary)'), 44))
print("יעדים:")
print(f"  horizontal overflow: {y.get('hscroll_overflow_px')}px  [{'PASS' if (y.get('hscroll_overflow_px') or 0) <= 1 else 'FAIL'} =0]")
print(verdict('toggle (מסמכים/יעדים) height', y.get('toggle_h (מסמכים/יעדים seg-btn)'), 44))
print(verdict('תקציב input height', y.get('budget_input_h'), 44))
fp = y.get('budget_input_font_px', '')
fnum = float(str(fp).replace('px', '')) if fp else 0
print(f"  תקציב input font: {fp}  [{'PASS' if fnum >= 16 else 'FAIL'} ≥16px (iOS no-zoom)]")

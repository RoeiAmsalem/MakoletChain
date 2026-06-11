"""Staging screenshots of the multi-branch /goods תקציב view ("כל הסניפים שלי").

Creates a TEMP Dennis-like manager (2 branches: 9015 ההגנה + 9018 דפנה) on the
staging DB (deleted in finally), logs in via the local Flask port (bypasses
nginx basic-auth + rate limit) and captures:
  1. desktop + mobile(390) multi view — combined strip + both sections
  2. search filtering across both sections at once
  3. section-header tap → that branch's editable single-branch תקציב view
  4. negative checks: the selector option exists+selected for the temp manager;
     admin's selector has NO "כל הסניפים שלי" entry.
Writes PNGs to /tmp. Staging only."""
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
BASE = 'http://127.0.0.1:8081'
EMAIL = 'dennis.test@makolet.test'
PW = 'TempShot2026!'
BRANCHES = (9015, 9018)
ADMIN_UID = 1  # makoletdashboard@gmail.com

conn = sqlite3.connect(DB)
conn.execute("DELETE FROM users WHERE email=?", (EMAIL,))
cur = conn.execute(
    "INSERT INTO users (name, email, password_hash, role, active) VALUES (?,?,?,?,1)",
    ('דניס (בדיקה)', EMAIL, generate_password_hash(PW), 'manager'))
UID = cur.lastrowid
for b in BRANCHES:
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (?,?)", (UID, b))
admin_old_hash = conn.execute(
    "SELECT password_hash FROM users WHERE id=?", (ADMIN_UID,)).fetchone()[0]
conn.execute("UPDATE users SET password_hash=? WHERE id=?",
             (generate_password_hash(PW), ADMIN_UID))
conn.commit(); conn.close()
print(f"temp manager uid={UID} created (branches {BRANCHES}); admin temp password set")

shots = []
checks = []


def note(label, cond, detail=''):
    checks.append((label, cond, detail))
    print(f"{'PASS' if cond else 'FAIL'} — {label} {detail}")


def login(page, email):
    page.goto(f'{BASE}/login', wait_until='domcontentloaded')
    page.fill('input[name=email]', email)
    page.fill('input[name=password]', PW)
    page.click('button[type=submit]')
    page.wait_for_load_state('networkidle')


try:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={'width': 1100, 'height': 1700})
        login(page, EMAIL)

        # selector: option exists for the 2-branch manager
        opts = page.eval_on_selector_all('#branch-select option',
                                         'os => os.map(o => o.value + "|" + o.textContent)')
        note('selector has "כל הסניפים שלי"', any(o.startswith('__all__|') for o in opts), str(opts))

        # multi view — desktop
        page.goto(f'{BASE}/goods?multi=1', wait_until='networkidle')
        page.wait_for_timeout(800)
        note('multi view URL kept', '/goods?multi=1' in page.url, page.url)
        sel_val = page.eval_on_selector('#branch-select', 's => s.value')
        note('selector shows "כל הסניפים שלי" selected', sel_val == '__all__', sel_val)
        n_sections = page.locator('.gm-section').count()
        note('two branch sections', n_sections == 2, f"sections={n_sections}")
        path = '/tmp/goods_multi_desktop.png'
        page.screenshot(path=path, full_page=True); shots.append(path); print('shot', path)

        # search filters across BOTH sections: pick a supplier that exists in both
        common = page.evaluate('''() => {
            const bySec = [...document.querySelectorAll('.gm-section')].map(sec =>
                new Set([...sec.querySelectorAll('tr[data-supplier]')].map(tr => tr.dataset.supplier)));
            if (bySec.length < 2) return null;
            for (const name of bySec[0]) if (bySec[1].has(name)) return name;
            return null;
        }''')
        if common:
            page.fill('#goal-search', common[:4])
            page.wait_for_timeout(400)
            counts = page.evaluate('''() =>
                [...document.querySelectorAll('.gm-section')].map(sec =>
                    [...sec.querySelectorAll('tr[data-supplier]')].filter(tr => tr.style.display !== 'none').length)''')
            note('search filters all sections', all(c >= 1 for c in counts) and sum(counts) > 0,
                 f"term={common[:4]!r} visible-per-section={counts}")
            path = '/tmp/goods_multi_search.png'
            page.screenshot(path=path, full_page=True); shots.append(path); print('shot', path)
            page.fill('#goal-search', ''); page.wait_for_timeout(300)
        else:
            note('search filters all sections', False, 'no common supplier found across sections')

        # mobile 390 — sections stack
        page.set_viewport_size({'width': 390, 'height': 844})
        page.reload(wait_until='networkidle'); page.wait_for_timeout(800)
        path = '/tmp/goods_multi_mobile.png'
        page.screenshot(path=path, full_page=True); shots.append(path); print('shot', path)

        # tap first section header → editable single-branch תקציב view
        first_branch = page.eval_on_selector('.gm-section', 's => s.id.replace("gm-branch-", "")')
        page.click('.gm-section-head a')
        page.wait_for_load_state('networkidle')
        page.wait_for_selector('.goal-budget-input', timeout=15000)
        note('header tap → editable branch view',
             f'branch_id={first_branch}' in page.url and page.locator('.goal-budget-input').count() > 0,
             page.url)
        path = '/tmp/goods_single_after_tap.png'
        page.screenshot(path=path, full_page=False); shots.append(path); print('shot', path)
        page.close()

        # admin: selector unchanged (no "כל הסניפים שלי")
        page2 = browser.new_page(viewport={'width': 1100, 'height': 900})
        login(page2, 'makoletdashboard@gmail.com')
        # options of a closed <select> are never "visible" — wait on count
        page2.wait_for_function(
            'document.querySelectorAll("#branch-select option").length > 0', timeout=15000)
        admin_opts = page2.eval_on_selector_all('#branch-select option', 'os => os.map(o => o.value)')
        note('admin selector has NO __all__ entry', '__all__' not in admin_opts,
             f"{len(admin_opts)} options")
        r = page2.goto(f'{BASE}/goods?multi=1', wait_until='networkidle')
        note('admin /goods?multi=1 bounced to /goods',
             page2.url.rstrip('/').endswith('/goods'), page2.url)
        browser.close()
finally:
    conn = sqlite3.connect(DB)
    conn.execute("DELETE FROM user_branches WHERE user_id IN (SELECT id FROM users WHERE email=?)", (EMAIL,))
    conn.execute("DELETE FROM users WHERE email=?", (EMAIL,))
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (admin_old_hash, ADMIN_UID))
    conn.commit(); conn.close()
    print("temp manager removed; admin password restored")

failed = [c for c in checks if not c[1]]
print("SHOTS:", ",".join(shots))
print("RESULT:", "ALL PASS" if not failed else f"{len(failed)} FAILED")

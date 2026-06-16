"""Prove the סניף בודד | כל הסניפים scope toggle shows ONLY under תקציב.

Renders a real 2+-branch manager's /goods page (forged session, no password),
writes the full HTML to a temp file, and drives it in headless chromium to read
the COMPUTED display of #goods-budget-scope as the user flips views — the only
faithful way to verify a CSS visibility fix (the `hidden` attribute is always in
the HTML; what matters is whether the browser actually hides it).

Checks (2+-branch manager):
  • initial (מסמכים default)         → toggle display:none  (GONE)
  • click תקציב                      → toggle display:flex   (shown)
  • click מסמכים                     → toggle display:none   (GONE again)
  • back to תקציב after picking      → toggle shown + last scope (כל הסניפים) kept
Plus a string check that admin + single-branch managers never get the element.

Read-only: never writes the DB. Needs Playwright+chromium (present on prod).
Usage: venv/bin/python scripts/verify_scope_toggle_visibility.py
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app import app, _list_visible_branches, get_db  # noqa: E402

ok = True
HTML_PATH = '/tmp/_scope_toggle_check.html'


def check(label, cond, detail=''):
    global ok
    print(f"{'PASS' if cond else 'FAIL'} — {label}{': ' + detail if detail else ''}")
    ok = ok and bool(cond)


def forge(client, uid, name, email, role, branches):
    with client.session_transaction() as s:
        s['user_id'] = uid
        s['user_name'] = name
        s['user_role'] = role
        s['user_email'] = (email or '').strip().lower()
        s['user_branches'] = branches
        if branches:
            s['branch_id'] = branches[0]


# ── discover subjects from the live DB ──
with app.test_request_context():
    db = get_db()
    mgr = db.execute(
        "SELECT u.id, u.name, u.email FROM users u "
        "JOIN user_branches ub ON ub.user_id = u.id "
        "JOIN branches b ON b.id = ub.branch_id "
        "WHERE u.role = 'manager' AND u.active = 1 AND b.active = 1 "
        "  AND b.id NOT IN (9999, 9998) "
        "GROUP BY u.id HAVING COUNT(DISTINCT b.id) >= 2 ORDER BY u.id LIMIT 1"
    ).fetchone()
    if not mgr:
        sys.exit("FAIL — no 2+-branch manager on this DB")
    mgr_branches = [r['branch_id'] for r in db.execute(
        "SELECT branch_id FROM user_branches WHERE user_id = ? ORDER BY branch_id",
        (mgr['id'],)).fetchall()]
    admin = db.execute("SELECT id, name, email FROM users WHERE role='admin' AND active=1 "
                       "ORDER BY id LIMIT 1").fetchone()
    single = db.execute(
        "SELECT u.id, u.name, u.email FROM users u JOIN user_branches ub ON ub.user_id=u.id "
        "WHERE u.role='manager' AND u.active=1 GROUP BY u.id "
        "HAVING COUNT(DISTINCT ub.branch_id)=1 ORDER BY u.id LIMIT 1").fetchone()
    single_branches = [r['branch_id'] for r in db.execute(
        "SELECT branch_id FROM user_branches WHERE user_id=?",
        (single['id'],)).fetchall()] if single else []

print(f"subject: manager {mgr['name']!r} (id={mgr['id']}) branches={mgr_branches}")

app.config['TESTING'] = True
client = app.test_client()

# ── render the manager's /goods to a file for the browser ──
forge(client, mgr['id'], mgr['name'], mgr['email'], 'manager', mgr_branches)
html = client.get('/goods').get_data(as_text=True)
check('manager /goods has the scope-toggle element', 'id="goods-budget-scope"' in html)
with open(HTML_PATH, 'w', encoding='utf-8') as fh:
    fh.write(html)

# ── server-gating string checks (no browser needed) ──
if admin:
    forge(client, admin['id'], admin['name'], admin['email'], 'admin', [])
    ah = client.get('/goods').get_data(as_text=True)
    check('admin: scope-toggle element NOT rendered', 'id="goods-budget-scope"' not in ah)
if single:
    forge(client, single['id'], single['name'], single['email'], 'manager', single_branches)
    sh = client.get('/goods').get_data(as_text=True)
    check('single-branch manager: scope-toggle element NOT rendered',
          'id="goods-budget-scope"' not in sh)
else:
    print("note: no single-branch manager on this DB — string check skipped")

# ── browser: computed display of the toggle across view switches ──
from playwright.sync_api import sync_playwright  # noqa: E402

def disp(page):
    return page.eval_on_selector('#goods-budget-scope', 'el => getComputedStyle(el).display')

def hidden(page, sel):
    return page.eval_on_selector(sel, 'el => el.hidden')

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    page.goto('file://' + HTML_PATH)
    page.wait_for_selector('#goods-view-goals')

    check('init (מסמכים): toggle display:none', disp(page) == 'none', disp(page))

    page.click('#goods-view-goals')          # → תקציב
    page.wait_for_timeout(150)
    check('תקציב: toggle display:flex', disp(page) == 'flex', disp(page))

    page.click('#goods-scope-all')           # pick כל הסניפים
    page.wait_for_timeout(150)
    check('כל הסניפים: multi view shown', hidden(page, '#goods-multi-view') is False)
    check('כל הסניפים: single pane hidden', hidden(page, '#goods-goals-view') is True)

    page.click('#goods-view-docs')           # → מסמכים
    page.wait_for_timeout(150)
    check('back to מסמכים: toggle display:none', disp(page) == 'none', disp(page))

    page.click('#goods-view-goals')          # → תקציב again
    page.wait_for_timeout(150)
    check('return to תקציב: toggle display:flex', disp(page) == 'flex', disp(page))
    check('return to תקציב: last scope (כל הסניפים) kept',
          hidden(page, '#goods-multi-view') is False
          and hidden(page, '#goods-goals-view') is True)

    page.click('#goods-scope-single')        # back to סניף בודד works
    page.wait_for_timeout(150)
    check('סניף בודד: single pane shown', hidden(page, '#goods-goals-view') is False)
    check('סניף בודד: multi view hidden', hidden(page, '#goods-multi-view') is True)

    browser.close()

try:
    os.remove(HTML_PATH)
except OSError:
    pass

print('\n' + ('ALL CHECKS PASSED' if ok else 'CHECKS FAILED'))
sys.exit(0 if ok else 1)

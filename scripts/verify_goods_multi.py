"""Verify the multi-branch /goods תקציב view (editable, managers with 2+ branches).

1. Render check: simulate a 2-branch manager session, GET /goods?multi=1,
   assert 200 + combined strip + one section per branch + inline budget inputs.
2. Reconciliation per section: Σ mtd_spend over that branch's _goal_data
   suppliers == the same branch's /goods incl-VAT MTD total (the
   _goods_doc_context aggregation the budget feature reconciles to) — Δ 0.00.
3. Combined strip == sum of the per-section budgeted-only totals.
4. Gating: admin and a single-branch manager get redirected off ?multi=1.

Usage: python scripts/verify_goods_multi.py [branch_id branch_id ...]
Defaults to 9015 9018 (Dennis: ההגנה + דפנה). Run from the repo root
(local or /opt/makolet-chain-staging).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app, _goal_data, _goods_doc_context, get_db, _now_il  # noqa: E402

BRANCH_IDS = [int(a) for a in sys.argv[1:]] or [9015, 9018]

ok = True


def check(label, cond, detail):
    global ok
    print(f"{'PASS' if cond else 'FAIL'} — {label}: {detail}")
    ok = ok and cond


def session_as(client, role, branches):
    with client.session_transaction() as s:
        s['user_id'] = 999999
        s['user_name'] = 'verify'
        s['user_role'] = role
        s['user_email'] = 'verify@test.local'
        s['user_branches'] = branches
        if branches:
            s['branch_id'] = branches[0]


TEST_UID = 999999

with app.test_request_context():
    db = get_db()
    month = _now_il().strftime('%Y-%m')
    # The multi view lists branches from user_branches (DB), not the session —
    # give the fake verify user real assignment rows (removed at the end).
    db.execute('DELETE FROM user_branches WHERE user_id = ?', (TEST_UID,))
    for b in BRANCH_IDS:
        db.execute('INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)',
                   (TEST_UID, b))
    db.commit()
    names = {b: db.execute('SELECT name FROM branches WHERE id=?', (b,)).fetchone()
             for b in BRANCH_IDS}
    for b, row in names.items():
        if row is None:
            print(f"FAIL — branch {b} not found in DB")
            sys.exit(1)

    total_budget = total_spent = 0.0
    for bid in BRANCH_IDS:
        data = _goal_data(bid, db)
        sum_mtd = round(sum(s['mtd_spend'] for s in data['suppliers']), 2)
        goods_mtd = round(_goods_doc_context(bid, month, db)['total'], 2)
        delta = round(sum_mtd - goods_mtd, 2)
        check(f"branch {bid} ({names[bid]['name']}) reconciliation",
              abs(delta) < 0.01,
              f"Σ mtd_spend={sum_mtd:,.2f} vs /goods incl-VAT MTD={goods_mtd:,.2f} Δ={delta:+.2f}")
        total_budget += data['totals']['budget']
        total_spent += data['totals']['spent']
    total_budget = round(total_budget, 2)
    total_spent = round(total_spent, 2)

app.config['TESTING'] = True
client = app.test_client()

# 2-branch manager → 200 + combined + per-branch sections
session_as(client, 'manager', BRANCH_IDS)
r = client.get('/goods?multi=1')
html = r.get_data(as_text=True)
check('multi view renders for 2-branch manager', r.status_code == 200,
      f"status={r.status_code}")
check('combined strip present', 'תקציב — כל הסניפים שלי' in html, 'header found' if 'תקציב — כל הסניפים שלי' in html else 'header MISSING')
for bid in BRANCH_IDS:
    check(f"section for branch {bid}", f'id="gm-branch-{bid}"' in html,
          'section rendered' if f'id="gm-branch-{bid}"' in html else 'section MISSING')
# Editable: each section carries inline budget inputs (one per supplier row).
n_inputs = html.count('class="goal-budget-input')
check('editable (budget inputs present)', n_inputs > 0,
      f'{n_inputs} inputs')
# Each section rides its own branch_id so edits post to the right store.
for bid in BRANCH_IDS:
    has = f'data-branch-id="{bid}"' in html
    check(f"section {bid} carries data-branch-id", has,
          'present' if has else 'MISSING')

# Combined strip == sum of sections (compare rendered numbers)
fmt = lambda v: '₪ {:,.0f}'.format(v)
check('combined Σ תקציב == sum of sections', fmt(total_budget) in html,
      f"{fmt(total_budget)}")
check('combined Σ הוצאה == sum of sections', fmt(total_spent) in html,
      f"{fmt(total_spent)}")
check('combined יתרה == sum of sections',
      fmt(round(total_budget - total_spent, 2)) in html,
      f"{fmt(round(total_budget - total_spent, 2))}")

# Gating: admin → redirect; single-branch manager → redirect
session_as(client, 'admin', [])
r = client.get('/goods?multi=1')
check('admin bounced off multi mode', r.status_code == 302,
      f"status={r.status_code} → {r.headers.get('Location')}")
session_as(client, 'manager', BRANCH_IDS[:1])
r = client.get('/goods?multi=1')
check('single-branch manager bounced off multi mode', r.status_code == 302,
      f"status={r.status_code} → {r.headers.get('Location')}")
# and their normal /goods has no "כל הסניפים שלי" flag
r = client.get('/goods')
html = r.get_data(as_text=True)
check('single-branch /goods: selector flag off',
      'const SHOW_ALL_MY_BRANCHES = false' in html, 'flag false')
session_as(client, 'admin', [])
r = client.get('/goods')
html = r.get_data(as_text=True)
check('admin /goods: selector flag off',
      'const SHOW_ALL_MY_BRANCHES = false' in html, 'flag false')

# Cleanup the temp assignment rows
with app.test_request_context():
    db = get_db()
    db.execute('DELETE FROM user_branches WHERE user_id = ?', (TEST_UID,))
    db.commit()

print('\n' + ('ALL CHECKS PASSED' if ok else 'CHECKS FAILED'))
sys.exit(0 if ok else 1)

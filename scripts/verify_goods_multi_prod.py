"""Verify the in-page "כל הסניפים" editable multi-branch תקציב toggle on PROD.

Re-port of the dev multi-branch budget view, with the entry point changed to an
in-page toggle on the goods תקציב view (סניף בודד | כל הסניפים) instead of a
branch-selector option. This script proves the whole contract against the real
prod DB + app, WITHOUT a password and WITHOUT touching any real supplier budget:

  • Discovers a real 2+-branch manager from user_branches (no hardcoded user).
  • Forges that user's session via session_transaction (same keys /login sets),
    so the real ownership logic (session['user_branches']) is exercised.
  • All writes go to a SYNTHETIC supplier ('__verify_probe__') under the
    manager's OWN branches; _goal_data includes budgeted suppliers in its roster,
    so the probe shows up in each branch's single-view payload with mtd_spend=0
    (it never perturbs reconciliation). Probe rows are deleted in finally.

Checks:
  1. 2+-manager /goods → the סניף בודד|כל הסניפים toggle renders; the combined
     view has one editable section per branch (data-branch-id + budget inputs).
  2. A probe budget posted with an EXPLICIT branch_id persists to the CORRECT
     branch (DB read + that branch's single-view /api/goal/data), echoes
     branch_id, and does NOT leak to the other branch.
  3. A forged write to a branch the manager does NOT own → 403, no DB write.
  4. Reconciliation per section: Σ mtd_spend == /goods incl-VAT MTD (Δ 0.00).
  5. Combined strip == Σ of the per-section budgeted-only totals.
  6. Admin and single-branch manager → NO toggle, no multi view (unchanged).

Safe to run on prod. Usage: python scripts/verify_goods_multi_prod.py
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app import (  # noqa: E402
    app, _goal_data, _goods_doc_context, _list_visible_branches, get_db, _now_il,
)

PROBE = '__verify_probe__'      # synthetic supplier — never a real one
VA, VB = 111111.0, 222222.0     # distinctive probe budgets per branch
ok = True


def check(label, cond, detail=''):
    global ok
    print(f"{'PASS' if cond else 'FAIL'} — {label}{': ' + detail if detail else ''}")
    ok = ok and bool(cond)


def read_budget(branch_id, supplier):
    with app.test_request_context():
        db = get_db()
        row = db.execute(
            "SELECT monthly_budget FROM supplier_budgets "
            "WHERE branch_id = ? AND supplier_name = ?",
            (branch_id, supplier)).fetchone()
        return row['monthly_budget'] if row else None


def delete_probe():
    with app.test_request_context():
        db = get_db()
        db.execute("DELETE FROM supplier_budgets WHERE supplier_name = ?", (PROBE,))
        db.commit()


def forge(client, user):
    with client.session_transaction() as s:
        s['user_id'] = user['id']
        s['user_name'] = user['name']
        s['user_role'] = user['role']
        s['user_email'] = (user['email'] or '').strip().lower()
        s['user_branches'] = user['branches']
        if user['branches']:
            s['branch_id'] = user['branches'][0]


def view_budget(client, branch_id, supplier):
    js = client.get(f'/api/goal/data?branch_id={branch_id}').get_json() or {}
    for s in js.get('suppliers', []):
        if s['supplier_name'] == supplier:
            return s['budget']
    return 'MISSING'


# ── Discover the test subjects from the real prod DB ──────────────────────────
with app.test_request_context():
    db = get_db()
    month = _now_il().strftime('%Y-%m')

    # A manager with >= 2 ACTIVE, non-demo branches (matches what the route's
    # _list_visible_branches will section). Demo branches (9999/9998) excluded.
    mgr_row = db.execute(
        "SELECT u.id, u.name, u.email FROM users u "
        "JOIN user_branches ub ON ub.user_id = u.id "
        "JOIN branches b ON b.id = ub.branch_id "
        "WHERE u.role = 'manager' AND u.active = 1 AND b.active = 1 "
        "  AND b.id NOT IN (9999, 9998) "
        "GROUP BY u.id HAVING COUNT(DISTINCT b.id) >= 2 "
        "ORDER BY u.id LIMIT 1"
    ).fetchone()
    if not mgr_row:
        sys.exit("FAIL — no manager with 2+ active branches found on this DB")
    mgr_branches = [r['branch_id'] for r in db.execute(
        "SELECT branch_id FROM user_branches WHERE user_id = ? ORDER BY branch_id",
        (mgr_row['id'],)).fetchall()]
    mgr = {'id': mgr_row['id'], 'name': mgr_row['name'], 'email': mgr_row['email'],
           'role': 'manager', 'branches': mgr_branches}

    # The sections the route would render (active, non-demo, the manager's).
    sec_branches = [b['id'] for b in _list_visible_branches(mgr['id'], 'manager')]
    BA, BB = sec_branches[0], sec_branches[1]

    # A branch the manager does NOT own (for the 403 negative).
    not_owned_row = db.execute(
        "SELECT id FROM branches WHERE active = 1 AND id NOT IN (9999, 9998) "
        "  AND id NOT IN (%s) ORDER BY id LIMIT 1"
        % ','.join('?' * len(mgr_branches)),
        mgr_branches).fetchone()
    NOT_OWNED = not_owned_row['id'] if not_owned_row else None

    # An admin and a single-branch manager (for the no-toggle checks).
    admin_row = db.execute(
        "SELECT id, name, email FROM users WHERE role = 'admin' AND active = 1 "
        "ORDER BY id LIMIT 1").fetchone()
    single_row = db.execute(
        "SELECT u.id, u.name, u.email FROM users u "
        "JOIN user_branches ub ON ub.user_id = u.id "
        "WHERE u.role = 'manager' AND u.active = 1 "
        "GROUP BY u.id HAVING COUNT(DISTINCT ub.branch_id) = 1 "
        "ORDER BY u.id LIMIT 1").fetchone()

print(f"subject: manager {mgr['name']!r} (id={mgr['id']}) branches={mgr['branches']} "
      f"→ sections [{BA}, {BB}], not-owned={NOT_OWNED}")

app.config['TESTING'] = True
client = app.test_client()

try:
    # ── login-free: forge the manager's real session ──
    forge(client, mgr)

    # ── 1. toggle renders + editable sections per branch ──
    r = client.get('/goods')
    html = r.get_data(as_text=True)
    check('manager /goods renders', r.status_code == 200, f"status={r.status_code}")
    check('סניף בודד|כל הסניפים toggle present', 'id="goods-budget-scope"' in html
          and 'כל הסניפים' in html and 'סניף בודד' in html)
    check('combined multi view present', 'id="goods-multi-view"' in html)
    for b in (BA, BB):
        check(f"section {b} carries data-branch-id", f'data-branch-id="{b}"' in html)
    n_inputs = html.count('class="goal-budget-input')
    check('inline budget inputs present in multi view', n_inputs >= 2,
          f"{n_inputs} inputs")

    # ── 2. probe write into section A → correct branch, echoes id, no leak ──
    r = client.post('/api/goal/budget',
                    json={'branch_id': BA, 'supplier_name': PROBE, 'monthly_budget': VA})
    j = r.get_json() or {}
    check(f'save to owned branch {BA} ok', r.status_code == 200 and j.get('ok'),
          f"status={r.status_code}")
    check(f'response echoes branch_id={BA}', j.get('branch_id') == BA,
          f"branch_id={j.get('branch_id')}")
    check(f'persisted to ({BA}, probe)', read_budget(BA, PROBE) == VA,
          f"db={read_budget(BA, PROBE)} want={VA}")
    check(f'did NOT leak to ({BB}, probe)', read_budget(BB, PROBE) is None,
          f"db={read_budget(BB, PROBE)}")
    check(f'{BA} single-view shows probe budget', view_budget(client, BA, PROBE) == VA,
          f"view={view_budget(client, BA, PROBE)} want={VA}")

    # ── 3. probe write into section B → correct branch, A untouched ──
    r = client.post('/api/goal/budget',
                    json={'branch_id': BB, 'supplier_name': PROBE, 'monthly_budget': VB})
    j = r.get_json() or {}
    check(f'save to owned branch {BB} ok', r.status_code == 200 and j.get('ok'),
          f"status={r.status_code}")
    check(f'persisted to ({BB}, probe)', read_budget(BB, PROBE) == VB,
          f"db={read_budget(BB, PROBE)} want={VB}")
    check(f'{BA} probe unchanged after {BB} write', read_budget(BA, PROBE) == VA,
          f"db={read_budget(BA, PROBE)} want={VA}")
    check(f'{BB} single-view shows probe budget', view_budget(client, BB, PROBE) == VB,
          f"view={view_budget(client, BB, PROBE)} want={VB}")

    # ── 4. negative: forged write to a non-owned branch → 403, no write ──
    if NOT_OWNED is not None:
        before = read_budget(NOT_OWNED, PROBE)
        r = client.post('/api/goal/budget',
                        json={'branch_id': NOT_OWNED, 'supplier_name': PROBE,
                              'monthly_budget': 999999.0})
        check(f'forged write to non-owned branch {NOT_OWNED} → 403',
              r.status_code == 403, f"status={r.status_code}")
        check(f'forged write left ({NOT_OWNED}, probe) unwritten',
              read_budget(NOT_OWNED, PROBE) == before,
              f"db={read_budget(NOT_OWNED, PROBE)}")
    else:
        check('non-owned branch available for 403 test', False, 'none found')

    # ── 5. reconciliation per section (Δ0) + 6. combined == Σ sections ──
    with app.test_request_context():
        db = get_db()
        tot_budget = tot_spent = 0.0
        for b in (BA, BB):
            data = _goal_data(b, db)
            sum_mtd = round(sum(s['mtd_spend'] for s in data['suppliers']), 2)
            goods_mtd = round(_goods_doc_context(b, month, db)['total'], 2)
            delta = round(sum_mtd - goods_mtd, 2)
            check(f'branch {b} reconciliation Δ0', abs(delta) < 0.01,
                  f"Σ mtd={sum_mtd:,.2f} vs /goods MTD={goods_mtd:,.2f} Δ={delta:+.2f}")
            tot_budget += data['totals']['budget']
            tot_spent += data['totals']['spent']
        tot_budget = round(tot_budget, 2)
        tot_spent = round(tot_spent, 2)

    r = client.get('/goods')
    html = r.get_data(as_text=True)
    fmt = lambda v: '₪ {:,.0f}'.format(v)
    check('combined Σ תקציב == sum of sections', fmt(tot_budget) in html, fmt(tot_budget))
    check('combined Σ הוצאה == sum of sections', fmt(tot_spent) in html, fmt(tot_spent))
    check('combined יתרה == sum of sections',
          fmt(round(tot_budget - tot_spent, 2)) in html,
          fmt(round(tot_budget - tot_spent, 2)))

    # ── 7. admin → no toggle, view unchanged ──
    if admin_row:
        forge(client, {'id': admin_row['id'], 'name': admin_row['name'],
                       'email': admin_row['email'], 'role': 'admin', 'branches': []})
        html = client.get('/goods').get_data(as_text=True)
        check('admin: NO scope toggle', 'id="goods-budget-scope"' not in html)
        check('admin: NO multi view', 'id="goods-multi-view"' not in html)
        check('admin: normal single תקציב view intact', 'id="goods-goals-view"' in html)
    else:
        check('admin user available', False, 'none found')

    # ── 8. single-branch manager → no toggle ──
    if single_row:
        with app.test_request_context():
            db = get_db()
            sb = [r['branch_id'] for r in db.execute(
                "SELECT branch_id FROM user_branches WHERE user_id = ?",
                (single_row['id'],)).fetchall()]
        forge(client, {'id': single_row['id'], 'name': single_row['name'],
                       'email': single_row['email'], 'role': 'manager', 'branches': sb})
        html = client.get('/goods').get_data(as_text=True)
        check('single-branch manager: NO scope toggle', 'id="goods-budget-scope"' not in html)
        check('single-branch manager: NO multi view', 'id="goods-multi-view"' not in html)
    else:
        print("note: no single-branch manager on this DB — skipped (not a failure)")

finally:
    delete_probe()
    leftover = read_budget(BA, PROBE), read_budget(BB, PROBE)
    print(f"cleanup: probe rows after delete = {leftover} (expect (None, None))")

print('\n' + ('ALL CHECKS PASSED' if ok else 'CHECKS FAILED'))
sys.exit(0 if ok else 1)

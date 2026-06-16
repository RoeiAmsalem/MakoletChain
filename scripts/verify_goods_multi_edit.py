"""Prove the EDITABLE multi-branch תקציב view saves each edit to the CORRECT
branch — as the real dennis-test manager (9015 ההגנה + 9018 דפנה).

Logs in through POST /login (real session, real user_branches from the DB),
then drives the same /api/goal/budget endpoint the combined page posts to:

1. Render: GET /goods?multi=1 → editable inputs + a data-branch-id per section.
2. Save a budget for a supplier in the 9018 section → persists to (9018, sup),
   the SAME supplier under 9015 is untouched, response echoes branch_id=9018.
3. Save a budget for a supplier in the 9015 section → persists to (9015, sup),
   9018 untouched.
4. Negative: forge a branch_id dennis does NOT own (126) → 403, no DB write.
5. Reconciliation per section after the edits: Σ mtd_spend == /goods incl-VAT
   MTD (Δ 0.00) — budgets never move spend.
6. Combined strip == sum of the per-section budgeted-only totals.

Every real supplier_budgets value touched is snapshotted up front and RESTORED
at the end, so staging is left exactly as found. STAGING ONLY — refuses to run
from the prod tree. Run from /opt/makolet-chain-staging.

Usage: python scripts/verify_goods_multi_edit.py
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT.startswith('/opt/makolet-chain') and 'staging' not in ROOT:
    sys.exit('REFUSED: this script is staging-only — never run on prod.')
sys.path.insert(0, ROOT)

from app import app, _goal_data, _goods_doc_context, get_db, _now_il  # noqa: E402

EMAIL = 'dennis-test@makoletchain.com'
PASSWORD = 'Dennis2026!'
B18, B15 = 9018, 9015          # dennis owns both
NOT_OWNED = 126                # אינשטיין — dennis does NOT own it
V18, V15 = 131313.0, 242424.0  # distinctive test budgets

ok = True


def check(label, cond, detail=''):
    global ok
    print(f"{'PASS' if cond else 'FAIL'} — {label}{': ' + detail if detail else ''}")
    ok = ok and bool(cond)


def read_budget(branch_id, supplier):
    """Direct DB read of one (branch, supplier) budget, fresh connection."""
    with app.test_request_context():
        db = get_db()
        row = db.execute(
            "SELECT monthly_budget FROM supplier_budgets "
            "WHERE branch_id = ? AND supplier_name = ?",
            (branch_id, supplier)).fetchone()
        return row['monthly_budget'] if row else None


def post_budget(client, branch_id, supplier, amount):
    return client.post('/api/goal/budget', json={
        'branch_id': branch_id, 'supplier_name': supplier, 'monthly_budget': amount})


# Pick a real supplier from each section's roster (guaranteed a row in the DOM).
with app.test_request_context():
    db = get_db()
    month = _now_il().strftime('%Y-%m')
    d18 = _goal_data(B18, db)
    d15 = _goal_data(B15, db)
    if not d18['suppliers'] or not d15['suppliers']:
        sys.exit(f"FAIL — empty roster (9018={len(d18['suppliers'])} "
                 f"9015={len(d15['suppliers'])}); cannot run edit proof")
    sup18 = d18['suppliers'][0]['supplier_name']
    sup15 = d15['suppliers'][0]['supplier_name']

# Snapshot every key we will touch, so we can restore staging exactly.
snap = {
    (B18, sup18): read_budget(B18, sup18),
    (B15, sup18): read_budget(B15, sup18),
    (B15, sup15): read_budget(B15, sup15),
    (B18, sup15): read_budget(B18, sup15),
    (NOT_OWNED, sup18): read_budget(NOT_OWNED, sup18),
}

app.config['TESTING'] = True
client = app.test_client()

# ── login as the real dennis-test user ──
r = client.post('/login', data={'email': EMAIL, 'password': PASSWORD})
logged_in = r.status_code == 302
check('login as dennis-test', logged_in, f"status={r.status_code}")
if not logged_in:
    print('\nCHECKS FAILED (cannot proceed without login)')
    sys.exit(1)

# ── 1. render: editable inputs + per-section branch_id ──
r = client.get('/goods?multi=1')
html = r.get_data(as_text=True)
check('multi view renders', r.status_code == 200, f"status={r.status_code}")
input_needle = 'class="goal-budget-input'
check('inline budget inputs present', input_needle in html,
      f"{html.count(input_needle)} inputs")
for b in (B18, B15):
    check(f"section {b} carries data-branch-id", f'data-branch-id="{b}"' in html)

# ── 2. save into the 9018 section ──
r = post_budget(client, B18, sup18, V18)
j = r.get_json() or {}
check(f"9018 save ok ({sup18!r})", r.status_code == 200 and j.get('ok'), f"status={r.status_code}")
check('9018 response echoes branch_id=9018', j.get('branch_id') == B18, f"branch_id={j.get('branch_id')}")
check('9018 persisted to (9018, sup)', read_budget(B18, sup18) == V18,
      f"db={read_budget(B18, sup18)} want={V18}")
check('9018 save did NOT leak to (9015, sup)', read_budget(B15, sup18) == snap[(B15, sup18)],
      f"db={read_budget(B15, sup18)} want={snap[(B15, sup18)]}")

# ── 3. save into the 9015 section ──
base_9018_sup15 = read_budget(B18, sup15)   # value before the 9015 write
r = post_budget(client, B15, sup15, V15)
j = r.get_json() or {}
check(f"9015 save ok ({sup15!r})", r.status_code == 200 and j.get('ok'), f"status={r.status_code}")
check('9015 response echoes branch_id=9015', j.get('branch_id') == B15, f"branch_id={j.get('branch_id')}")
check('9015 persisted to (9015, sup)', read_budget(B15, sup15) == V15,
      f"db={read_budget(B15, sup15)} want={V15}")
check('9015 save did NOT leak to (9018, sup)', read_budget(B18, sup15) == base_9018_sup15,
      f"db={read_budget(B18, sup15)} want={base_9018_sup15}")

# ── 4. negative: forge a non-owned branch_id → rejected, no write ──
r = post_budget(client, NOT_OWNED, sup18, 999999.0)
check('forged write to non-owned branch 126 → 403', r.status_code == 403, f"status={r.status_code}")
check('forged write left (126, sup) unchanged', read_budget(NOT_OWNED, sup18) == snap[(NOT_OWNED, sup18)],
      f"db={read_budget(NOT_OWNED, sup18)} want={snap[(NOT_OWNED, sup18)]}")

# ── 5. reconciliation per section after the edits (Δ0) ──
with app.test_request_context():
    db = get_db()
    tot_budget = tot_spent = 0.0
    for b, name in ((B18, 'דפנה'), (B15, 'ההגנה')):
        data = _goal_data(b, db)
        sum_mtd = round(sum(s['mtd_spend'] for s in data['suppliers']), 2)
        goods_mtd = round(_goods_doc_context(b, month, db)['total'], 2)
        delta = round(sum_mtd - goods_mtd, 2)
        check(f"branch {b} ({name}) reconciliation Δ0", abs(delta) < 0.01,
              f"Σ mtd={sum_mtd:,.2f} vs /goods MTD={goods_mtd:,.2f} Δ={delta:+.2f}")
        tot_budget += data['totals']['budget']
        tot_spent += data['totals']['spent']
    tot_budget = round(tot_budget, 2)
    tot_spent = round(tot_spent, 2)

# ── 6. combined strip == sum of sections (the route's own construction) ──
r = client.get('/goods?multi=1')
html = r.get_data(as_text=True)
fmt = lambda v: '₪ {:,.0f}'.format(v)
check('combined Σ תקציב == sum of sections', fmt(tot_budget) in html, fmt(tot_budget))
check('combined Σ הוצאה == sum of sections', fmt(tot_spent) in html, fmt(tot_spent))
check('combined יתרה == sum of sections', fmt(round(tot_budget - tot_spent, 2)) in html,
      fmt(round(tot_budget - tot_spent, 2)))

# ── restore staging to exactly the snapshot ──
for (b, sup), prior in snap.items():
    if b == NOT_OWNED:
        continue   # never wrote here
    # 0 clears the row; a prior value re-asserts it (same endpoint, dennis owns b).
    post_budget(client, b, sup, prior if prior is not None else 0)
restored = all(read_budget(b, sup) == prior for (b, sup), prior in snap.items())
check('staging restored to snapshot', restored)

print('\n' + ('ALL CHECKS PASSED' if ok else 'CHECKS FAILED'))
sys.exit(0 if ok else 1)

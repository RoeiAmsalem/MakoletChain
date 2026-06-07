"""Root-cause diag: do floored stores drop supplier_roster names from the
תקציב page list? For each branch print roster / current-month / budgeted counts,
the EXPECTED union size, the ACTUAL _goal_data supplier count, and any dropped
roster names.

Usage:  python3 scripts/diag_goal_floor.py
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module
from app import _goal_data, _goods_doc_context, _now_il, _branch_visible_from
from agents.supplier_roster import prior_two_month_suppliers

BRANCHES = [9018, 9016, 9019, 9015]


def _conn():
    c = sqlite3.connect(app_module.DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


month = _now_il().strftime('%Y-%m')
print(f"current month = {month}\n")

for bid in BRANCHES:
    c = _conn()
    vf = _branch_visible_from(bid, c)
    stored = {r['supplier_name'] for r in c.execute(
        "SELECT supplier_name FROM supplier_roster WHERE branch_id=?", (bid,)).fetchall()}
    budgets = {r['supplier_name'] for r in c.execute(
        "SELECT supplier_name FROM supplier_budgets WHERE branch_id=?", (bid,)).fetchall()}
    cur = {g['supplier'] for g in _goods_doc_context(bid, month, c)['groups']}
    # The supplier list SHOULD include every budgetable supplier = the true
    # prior-2-month roster ∪ current ∪ budgeted, whether or not the monthly
    # build has populated the supplier_roster table yet.
    true_roster = set(prior_two_month_suppliers(c, bid))
    should = (true_roster | cur | budgets) - {'—', None}

    data = _goal_data(bid, c)
    listed = {s['supplier_name'] for s in data['suppliers']}
    missing = should - listed
    c.close()

    print(f"branch {bid} | visible_from={vf}")
    print(f"  stored_roster_table={len(stored)}  true_prior2mo={len(true_roster)}  "
          f"current={len(cur)}  budgeted={len(budgets)}")
    print(f"  SHOULD list={len(should)} | _goal_data listed={len(listed)}")
    flag = 'OK' if not missing else f'BUG — {len(missing)} budgetable suppliers MISSING'
    print(f"  -> {flag}")
    if missing:
        print(f"     missing sample: {list(missing)[:6]}")
    print()

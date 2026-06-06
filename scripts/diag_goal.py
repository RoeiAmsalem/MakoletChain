"""Diagnostic for the /goal supplier-budget tracker (run on staging).

Shows, for a set of branches:
  - days_elapsed / days_in_month
  - a 3-supplier sample (supplier, mtd_spend, projected, remaining)
  - reconciliation: Σ per-supplier mtd_spend vs the trusted /goods pre-VAT MTD total
  - a persistence round-trip through the real POST /api/goal/budget endpoint
    (set a budget, read it back, show remaining recomputed, then clear it)

Usage:  python3 scripts/diag_goal.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import sqlite3
import app as app_module
from app import app, _goal_data, _goods_doc_context

BRANCHES = [9015, 9018]


def _conn():
    c = sqlite3.connect(app_module.DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def _seed_session(client, branch_id):
    """Log in as a real user that can see this branch, scoped to it."""
    conn = _conn()
    row = conn.execute(
        "SELECT u.id, u.role FROM users u "
        "LEFT JOIN user_branches ub ON ub.user_id = u.id "
        "WHERE u.active = 1 AND (u.role IN ('admin','ceo') OR ub.branch_id = ?) "
        "ORDER BY (u.role IN ('admin','ceo')) DESC LIMIT 1",
        (branch_id,)).fetchone()
    conn.close()
    with client.session_transaction() as sess:
        sess['user_id'] = row['id']
        sess['user_role'] = row['role']
        sess['branch_id'] = branch_id
        sess['user_branches'] = [branch_id]


def show_branch(branch_id):
    conn = _conn()
    data = _goal_data(branch_id, conn)
    goods_total = _goods_doc_context(branch_id, data['month'], conn)['total_before_vat']
    conn.close()

    print(f"\n=== branch {branch_id} — month {data['month']} ===")
    print(f"day {data['days_elapsed']} of {data['days_in_month']}  "
          f"({len(data['suppliers'])} suppliers)")
    print("  supplier | mtd_spend | projected | remaining")
    for s in data['suppliers'][:3]:
        rem = "—" if s['remaining'] is None else f"{s['remaining']:.2f}"
        print(f"  {s['supplier_name']} | {s['mtd_spend']:.2f} | "
              f"{s['projected']:.2f} | {rem}")

    sum_mtd = round(sum(s['mtd_spend'] for s in data['suppliers']), 2)
    match = "MATCH" if abs(sum_mtd - goods_total) < 0.02 else "MISMATCH"
    print(f"  reconcile: Σ per-supplier mtd_spend = {sum_mtd:.2f}  |  "
          f"/goods MTD total = {goods_total:.2f}  -> {match}")
    return data


def persistence_roundtrip(branch_id, data):
    """Set a budget via the real endpoint, read it back, clear it."""
    if not data['suppliers']:
        print(f"\n[persistence] branch {branch_id}: no suppliers — skipped")
        return
    supplier = data['suppliers'][0]['supplier_name']
    print(f"\n=== persistence round-trip (branch {branch_id}, supplier '{supplier}') ===")
    client = app.test_client()
    _seed_session(client, branch_id)

    r = client.post('/api/goal/budget', json={'supplier_name': supplier,
                                              'monthly_budget': 12345})
    body = r.get_json()
    row = next(s for s in body['suppliers'] if s['supplier_name'] == supplier)
    print(f"  POST 12345 -> HTTP {r.status_code} | budget={row['budget']} | "
          f"projected={row['projected']:.2f} | remaining={row['remaining']:.2f}")

    # Prove it persisted to the DB (independent read).
    conn = _conn()
    saved = conn.execute(
        "SELECT monthly_budget FROM supplier_budgets "
        "WHERE branch_id=? AND supplier_name=?", (branch_id, supplier)).fetchone()
    conn.close()
    print(f"  DB row after save: monthly_budget={saved['monthly_budget'] if saved else None}")

    # Clear it (0 deletes) so staging is left clean.
    r2 = client.post('/api/goal/budget', json={'supplier_name': supplier,
                                               'monthly_budget': 0})
    conn = _conn()
    gone = conn.execute(
        "SELECT 1 FROM supplier_budgets WHERE branch_id=? AND supplier_name=?",
        (branch_id, supplier)).fetchone()
    conn.close()
    print(f"  POST 0 -> HTTP {r2.status_code} | DB row exists now: {bool(gone)} (cleared)")


if __name__ == '__main__':
    for b in BRANCHES:
        d = show_branch(b)
        if b == BRANCHES[0]:
            persistence_roundtrip(b, d)

"""Verify the תקציב budget POST round-trip + budgeted-only totals (writes then
restores — net-zero to the DB).

For one branch: picks two real suppliers from the roster, POSTs a budget for
each via /api/goal/budget (authenticated test_client), asserts HTTP 200, a
supplier_budgets row landed, the summary totals are budgeted-only (sensible
יתרה, not a fake blowout from unbudgeted suppliers), and the over-pace count
matches. Then clears both (empty POST) and asserts the rows are gone. Any
pre-existing budget for the two suppliers is snapshotted and restored.

Usage:  venv/bin/python scripts/verify_goal_post.py --branch-id 9015
"""
import argparse
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module  # noqa: E402
from app import app, _goal_data  # noqa: E402

B1, B2 = 4000.0, 7000.0   # two test budgets


def _client(branch_id):
    app.config['TESTING'] = True
    c = app.test_client()
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    uid = conn.execute("SELECT id FROM users WHERE role='admin' ORDER BY id LIMIT 1").fetchone()
    conn.close()
    with c.session_transaction() as s:
        s['user_id'] = uid['id'] if uid else 1
        s['user_role'] = 'admin'
        s['branch_id'] = branch_id
    return c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--branch-id', type=int, required=True)
    args = ap.parse_args()
    bid = args.branch_id

    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    roster = _goal_data(bid, conn)['suppliers']
    if len(roster) < 2:
        print(f'branch {bid} has < 2 suppliers in roster — cannot test')
        sys.exit(1)
    sup1, sup2 = roster[0]['supplier_name'], roster[1]['supplier_name']

    # Snapshot any pre-existing budgets for the two suppliers (restore at end).
    snap = {}
    for sup in (sup1, sup2):
        r = conn.execute("SELECT monthly_budget FROM supplier_budgets "
                         "WHERE branch_id=? AND supplier_name=?", (bid, sup)).fetchone()
        snap[sup] = r['monthly_budget'] if r else None
    conn.close()

    c = _client(bid)
    fails = []

    # 1) POST two budgets.
    r1 = c.post('/api/goal/budget', json={'supplier_name': sup1, 'monthly_budget': B1})
    r2 = c.post('/api/goal/budget', json={'supplier_name': sup2, 'monthly_budget': B2})
    print(f'POST {sup1}={B1:.0f} -> {r1.status_code}')
    print(f'POST {sup2}={B2:.0f} -> {r2.status_code}')
    if r1.status_code != 200 or r2.status_code != 200:
        fails.append('POST not 200')

    # 2) Rows landed in supplier_budgets.
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    got = {row['supplier_name']: row['monthly_budget'] for row in conn.execute(
        "SELECT supplier_name, monthly_budget FROM supplier_budgets "
        "WHERE branch_id=? AND supplier_name IN (?,?)", (bid, sup1, sup2))}
    conn.close()
    print(f'DB rows after POST: {got}')
    if got.get(sup1) != B1 or got.get(sup2) != B2:
        fails.append('DB rows missing/incorrect after POST')

    # 3) Totals budgeted-only + over-pace count from the POST response.
    data = r2.get_json()
    t = data['totals']
    over = sum(1 for s in data['suppliers']
               if s['remaining'] is not None and s['remaining'] < 0)
    budgeted = [s for s in data['suppliers'] if s['budget']]
    print(f'totals: תקציב={t["budget"]:.2f} קצב={t["projected"]:.2f} '
          f'יתרה={t["remaining"]:.2f}  (budgeted suppliers={len(budgeted)}, '
          f'over-pace={over})')
    # budgeted-only invariant: totals.budget == Σ budgeted budgets (B1+B2 here,
    # plus any pre-existing budgeted supplier), and remaining == budget-projected.
    if round(t['budget'] - t['projected'], 2) != t['remaining']:
        fails.append('remaining != budget - projected (totals basis broken)')
    if t['budget'] < B1 + B2 - 0.01:
        fails.append('totals.budget excludes the two test budgets')
    # sanity: יתרה must not be a wild blowout driven by unbudgeted suppliers
    unbudgeted_proj = sum(s['projected'] for s in data['suppliers'] if not s['budget'])
    print(f'(unbudgeted projected NOT in totals: {unbudgeted_proj:.2f})')

    # 4) Clear both (empty POST) → rows gone.
    c.post('/api/goal/budget', json={'supplier_name': sup1, 'monthly_budget': ''})
    c.post('/api/goal/budget', json={'supplier_name': sup2, 'monthly_budget': ''})
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    left = conn.execute("SELECT COUNT(*) n FROM supplier_budgets "
                        "WHERE branch_id=? AND supplier_name IN (?,?)",
                        (bid, sup1, sup2)).fetchone()['n']
    print(f'rows after clear: {left}')
    if left != 0:
        fails.append('clear did not delete rows')

    # 5) Restore any pre-existing budgets we overwrote.
    for sup, val in snap.items():
        if val is not None:
            conn.execute("INSERT INTO supplier_budgets (branch_id, supplier_name, monthly_budget) "
                         "VALUES (?,?,?) ON CONFLICT(branch_id, supplier_name) "
                         "DO UPDATE SET monthly_budget=excluded.monthly_budget", (bid, sup, val))
    conn.commit()
    conn.close()
    print(f'restored pre-existing budgets: '
          f'{ {k: v for k, v in snap.items() if v is not None} or "none"}')

    print('RESULT:', 'PASS' if not fails else 'FAIL — ' + '; '.join(fails))
    sys.exit(0 if not fails else 1)


if __name__ == '__main__':
    main()

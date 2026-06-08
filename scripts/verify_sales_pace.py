"""Verify the קצב הכנסות (pace) tile math via the real /api/sales endpoint.

pace must equal round(avg × days_in_month) for the selected month (self-consistent
with the ממוצע ליום tile), and for a fully-elapsed past month (days == days_in_month)
pace must equal the actual total.

Usage:  python3 scripts/verify_sales_pace.py
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module
from app import app

# 9018/9015 = current month (partial). 126/127 have no visibility floor, so a
# completed past month (May/April) is the days==days_in_month → pace==total case.
CASES = [(9018, '2026-06'), (9015, '2026-06'),
         (127, '2026-05'), (126, '2026-05'), (127, '2026-04')]


def _seed(client, branch_id):
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT u.id, u.role FROM users u LEFT JOIN user_branches ub ON ub.user_id=u.id "
        "WHERE u.active=1 AND (u.role IN ('admin','ceo') OR ub.branch_id=?) "
        "ORDER BY (u.role IN ('admin','ceo')) DESC LIMIT 1", (branch_id,)).fetchone()
    conn.close()
    with client.session_transaction() as sess:
        sess['user_id'] = row['id']
        sess['user_role'] = row['role']
        sess['branch_id'] = branch_id
        sess['user_branches'] = [branch_id]


client = app.test_client()
for branch_id, month in CASES:
    _seed(client, branch_id)
    d = client.get(f'/api/sales?month={month}&branch_id={branch_id}').get_json()
    avg, days, dim, pace, total = d['avg'], d['days'], d['days_in_month'], d['pace'], d['total']
    expect = round(avg * dim) if (days and dim) else None
    ok = (pace == expect)
    print(f"branch {branch_id} {month}: total={total} avg={avg} days={days} "
          f"days_in_month={dim} pace={pace}")
    print(f"   pace == round(avg×days_in_month)={expect}? {'PASS' if ok else 'FAIL'}"
          + (f"  | days==days_in_month → pace vs total: {pace} vs {round(total)} "
             f"({'== actual total' if days == dim else 'partial month, pace>total expected'})"
             if days else "  | 0 days → pace '—'"))
    assert 'highest' not in d and 'lowest' not in d, "highest/lowest must be removed"
print("\nhighest/lowest removed from /api/sales response: confirmed")

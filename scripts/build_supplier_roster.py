"""One-time / manual build of supplier_roster for the current month.

Populates the per-branch full supplier roster NOW (the scheduler otherwise
rebuilds it monthly on the 1st). Prints per-branch supplier counts + the
prior-2-month window used.

Usage:
  python3 scripts/build_supplier_roster.py            # all active branches
  python3 scripts/build_supplier_roster.py 9018       # one branch
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from agents.supplier_roster import build_all, build_for_branch, prior_two_months, DB_PATH

older, newer = prior_two_months()
print(f"supplier_roster build — prior 2 months: {older} + {newer}")

if len(sys.argv) > 1:
    bid = int(sys.argv[1])
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        n = build_for_branch(conn, bid)
    finally:
        conn.close()
    print(f"branch {bid}: {n} suppliers")
else:
    res = build_all()
    total = sum(v for v in res.values() if v >= 0)
    for bid, n in res.items():
        print(f"branch {bid}: {n} suppliers")
    print(f"=== {len(res)} branches, {total} roster rows total ===")

"""READ-ONLY: print the first N תקציב supplier names in _goal_data order +
reconciliation Δ for a branch. Confirms the alphabetical (א→ת) ordering and
that totals/reconciliation are unchanged. No writes.

Usage:  venv/bin/python scripts/show_goal_order.py --branch-id 9015
"""
import argparse
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module  # noqa: E402
from app import _goal_data, _goods_doc_context  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--branch-id', type=int, required=True)
    ap.add_argument('-n', type=int, default=10)
    args = ap.parse_args()

    c = sqlite3.connect('file:' + os.path.abspath(app_module.DB_PATH) + '?mode=ro',
                        uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    data = _goal_data(args.branch_id, c)
    month = data['month']
    goods_total = _goods_doc_context(args.branch_id, month, c)['total_before_vat']
    c.close()

    sup = data['suppliers']
    print(f"branch {args.branch_id}  month {month}  suppliers={len(sup)}")
    print(f"first {args.n} in list order:")
    for i, s in enumerate(sup[:args.n], 1):
        print(f"  {i:>2}. {s['supplier_name']}")

    sum_mtd = round(sum(s['mtd_spend'] for s in sup), 2)
    delta = round(sum_mtd - goods_total, 2)
    print(f"reconciliation: Σmtd {sum_mtd:.2f} vs /goods {goods_total:.2f}  "
          f"Δ {delta:.2f}  {'OK (Δ0)' if delta == 0 else '*** MISMATCH ***'}")


if __name__ == '__main__':
    main()

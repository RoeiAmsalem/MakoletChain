"""Read-only reconciliation for the תקציב (supplier-budget) tracker.

For one branch + current Israel month, prints _goal_data's per-supplier MTD
spend and asserts Σ(per-supplier mtd_spend) == the trusted /goods pre-VAT MTD
total (_goods_doc_context total_before_vat) to the cent — the same invariant
the budget view relies on. Writes NOTHING.

Usage:  venv/bin/python scripts/recon_goal.py --branch-id 9015
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
    args = ap.parse_args()

    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    row = conn.execute('SELECT name FROM branches WHERE id=?',
                       (args.branch_id,)).fetchone()
    name = row['name'] if row else '?'

    data = _goal_data(args.branch_id, conn)
    month = data['month']
    goods_total = _goods_doc_context(args.branch_id, month, conn)['total_before_vat']
    conn.close()

    sum_mtd = round(sum(s['mtd_spend'] for s in data['suppliers']), 2)
    delta = round(sum_mtd - goods_total, 2)

    print(f'branch={args.branch_id} {name}  month={month}  '
          f'day {data["days_elapsed"]}/{data["days_in_month"]}')
    print(f'suppliers in roster      : {len(data["suppliers"])}')
    print(f'Σ per-supplier mtd_spend : {sum_mtd:.2f}')
    print(f'/goods pre-VAT MTD total : {goods_total:.2f}')
    print(f'Δ                        : {delta:.2f}  '
          f'{"OK (Δ0)" if delta == 0 else "*** MISMATCH ***"}')
    print(f'totals (budgeted-only)   : תקציב={data["totals"]["budget"]:.2f}  '
          f'קצב={data["totals"]["projected"]:.2f}  '
          f'יתרה={data["totals"]["remaining"]:.2f}')
    sys.exit(0 if delta == 0 else 1)


if __name__ == '__main__':
    main()

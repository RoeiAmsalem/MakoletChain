#!/usr/bin/env python3
"""Compare BI 902 totals (staging z_report_902) against prod Gmail-Z values.

Usage:
  scripts/compare_z_sources.py --date 2026-05-20
  scripts/compare_z_sources.py --date 2026-05-20 --gmail 126=13721.98 --gmail 127=9182.31

Prints one row per branch — branch_id | bi_902 | gmail | diff | match (Y/N
within ₪1). Read-only.
"""
import argparse
import os
import sqlite3
import sys
from datetime import date

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'db', 'makolet_chain.db')

MATCH_TOLERANCE = 1.0  # ₪


def _parse_gmail_args(items: list[str]) -> dict[int, float]:
    out: dict[int, float] = {}
    for item in items or []:
        if '=' not in item:
            raise SystemExit(f'--gmail must be branch_id=amount, got {item!r}')
        bid, amt = item.split('=', 1)
        out[int(bid)] = float(amt)
    return out


def main():
    ap = argparse.ArgumentParser(description='Compare BI 902 vs Gmail-Z totals')
    ap.add_argument('--date', default=(date.today().isoformat()),
                    help='YYYY-MM-DD (default: today)')
    ap.add_argument('--gmail', action='append', default=[],
                    help='branch_id=amount (repeatable) — prod Gmail-Z totals')
    args = ap.parse_args()

    gmail = _parse_gmail_args(args.gmail)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        'SELECT z.branch_id, b.name AS branch_name, z.amount, z.z_number '
        'FROM z_report_902 z '
        'LEFT JOIN branches b ON b.id = z.branch_id '
        'WHERE z.date = ? '
        'ORDER BY z.branch_id',
        (args.date,)
    ).fetchall()

    if not rows:
        print(f'no z_report_902 rows for {args.date}')
        return 0

    header = f'{"branch":<8}{"name":<24}{"bi_902":>12}{"gmail":>12}{"diff":>10}  match'
    print(header)
    print('-' * len(header))

    any_mismatch = False
    for r in rows:
        bid = r['branch_id']
        name = (r['branch_name'] or '')[:22]
        bi = r['amount']
        g = gmail.get(bid)
        if g is None:
            diff_s = '--'
            match_s = '?'
        else:
            diff = round(bi - g, 2)
            diff_s = f'{diff:+.2f}'
            ok = abs(diff) <= MATCH_TOLERANCE
            match_s = 'Y' if ok else 'N'
            if not ok:
                any_mismatch = True
        bi_s = f'{bi:.2f}' if bi is not None else '--'
        g_s = f'{g:.2f}' if g is not None else '--'
        print(f'{bid:<8}{name:<24}{bi_s:>12}{g_s:>12}{diff_s:>10}  {match_s}')

    return 1 if any_mismatch else 0


if __name__ == '__main__':
    sys.exit(main())

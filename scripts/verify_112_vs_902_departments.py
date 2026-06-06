"""Verify report 112 dept amounts match the 902-stored numbers to the cent.

Reads the EXISTING z_department_sales rows for a (branch, date) — these were
written by the old 902-XLS path — then pulls report 112 fresh for the same day
and compares per department: 902 amount vs 112 sale_incl_vat. Prints the delta
and the new cost/profit/margin columns 112 adds (which 902 never had).

Read-only — never writes to the DB.

Usage (staging):
  venv/bin/python scripts/verify_112_vs_902_departments.py --branch-id 126
  venv/bin/python scripts/verify_112_vs_902_departments.py --branch-id 126 --date 2026-05-27
"""
import argparse
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import agents.aviv_z_report as zr  # noqa: E402

DB_PATH = zr.DB_PATH


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--branch-id', type=int, required=True)
    ap.add_argument('--date', help='YYYY-MM-DD (default: latest date with '
                                    'existing dept rows for the branch)')
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute('SELECT aviv_branch_id, name FROM branches WHERE id=?',
                       (args.branch_id,)).fetchone()
    if not row or row['aviv_branch_id'] is None:
        print(f'branch {args.branch_id} has no aviv_branch_id')
        sys.exit(1)
    aviv_id = row['aviv_branch_id']

    date = args.date
    if not date:
        d = conn.execute(
            'SELECT MAX(date) AS d FROM z_department_sales WHERE branch_id=?',
            (args.branch_id,)).fetchone()
        date = d['d'] if d else None
    if not date:
        print(f'branch {args.branch_id} has no existing dept rows to compare')
        sys.exit(1)

    print(f'branch={args.branch_id} {row["name"]} aviv={aviv_id} date={date}')

    stored = {r['dept_code']: r for r in conn.execute(
        'SELECT dept_code, dept_name, amount FROM z_department_sales '
        'WHERE branch_id=? AND date=?', (args.branch_id, date)).fetchall()}
    print(f'existing (902-sourced) dept rows: {len(stored)}')

    tok = zr._refresh(zr._login_chain_account())
    depts = zr.fetch_112_departments(aviv_id, date, tok)
    pulled = {d['dept_code']: d for d in depts}
    print(f'report-112 dept rows pulled: {len(pulled)}')

    codes = sorted(set(stored) | set(pulled))
    max_delta = 0.0
    mismatches = 0
    only_112 = []
    only_902 = []
    print(f'\n{"code":>5} {"902 amount":>12} {"112 sale":>12} {"Δ":>9}  name')
    for c in codes:
        s = stored.get(c)
        p = pulled.get(c)
        if s is None:
            only_112.append(c)
            continue
        if p is None:
            only_902.append(c)
            print(f'{c:>5} {s["amount"]:>12.2f} {"—":>12} {"(no 112)":>9}  {s["dept_name"]}')
            continue
        delta = round(p['sale_incl_vat'] - s['amount'], 2)
        max_delta = max(max_delta, abs(delta))
        if abs(delta) >= 0.01:
            mismatches += 1
        flag = '' if abs(delta) < 0.01 else '  <-- Δ'
        print(f'{c:>5} {s["amount"]:>12.2f} {p["sale_incl_vat"]:>12.2f} '
              f'{delta:>9.2f}  {p["dept_name"]}{flag}')

    print(f'\nmatched depts compared: {len(codes) - len(only_112) - len(only_902)}')
    print(f'max |Δ|: {max_delta:.2f}   mismatches (|Δ|>=0.01): {mismatches}')
    if only_112:
        print(f'depts only in 112 (902 missed): {only_112}')
    if only_902:
        print(f'depts only in 902 (not in 112): {only_902}')

    # Show the new columns 112 adds, for the home-tile depts.
    print('\nnew 112 columns (home tiles 5/83/2):')
    for c in (5, 83, 2):
        d = pulled.get(c)
        if d:
            print(f'  dept {c:>3} {d["dept_name"]}: sale_incl_vat={d["sale_incl_vat"]} '
                  f'cost_ex_vat={d["cost_ex_vat"]} profit={d["profit"]} '
                  f'profit_pct={d["profit_pct"]} contrib_pct={d["contrib_pct"]}')


if __name__ == '__main__':
    main()

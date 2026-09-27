"""Staging proof for the BilBoy re-read window: run the REAL nightly Wolt path
(agents.bilboy.sync_wolt_fees_window) for every active branch with an
injected Israel "today", then print the resulting עמלות Wolt rows.

    python scripts/sim_wolt_window.py --today 2026-09-03
    python scripts/sim_wolt_window.py --today 2026-09-10 --branches 9020,9017

Reads BilBoy (GET only) and writes source='bilboy_wolt' fixed_expenses rows
exactly as the nightly would. REFUSES to run from the prod tree.
"""
import argparse
import os
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

if os.path.realpath(ROOT) == '/opt/makolet-chain':
    print('REFUSING: this is the prod tree. Staging only.')
    sys.exit(2)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT, '.env'))

import logging
import requests

from agents.bilboy import (
    CHAIN_TOKEN_ENV, WOLT_EXPENSE_NAME, _api_get, _get_db, sync_wolt_fees_window,
)
from utils.sync_window import months_to_sync

logging.basicConfig(level=logging.WARNING, format='%(levelname)s %(message)s')
log = logging.getLogger('sim_wolt')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--today', required=True, help='Israel date, YYYY-MM-DD')
    ap.add_argument('--branches', default='')
    args = ap.parse_args()
    today = date.fromisoformat(args.today)

    token = os.environ.get(CHAIN_TOKEN_ENV) or ''
    if not token:
        print('NO BILBOY_CHAIN_TOKEN'); sys.exit(2)
    session = requests.Session()
    session.headers.update({'Authorization': f'Bearer {token}'})

    conn = _get_db()
    branches = [dict(r) for r in conn.execute(
        "SELECT id, name, bilboy_branch_id, franchise_supplier FROM branches "
        "WHERE active=1 AND bilboy_branch_id IS NOT NULL ORDER BY id")]
    conn.close()
    if args.branches:
        want = {int(b) for b in args.branches.split(',')}
        branches = [b for b in branches if b['id'] in want]

    print(f'today(IL)={today} months_to_sync={months_to_sync(today)} branches={len(branches)}')
    calls = 0
    for b in branches:
        franchise = b['franchise_supplier'] or 'זיכיונות המכולת בע"מ'
        raw = _api_get(session, '/customer/suppliers',
                       params={'customerBranchId': str(b['bilboy_branch_id']), 'all': 'true'})
        calls += 1
        sups = raw.get('suppliers') if isinstance(raw, dict) else raw
        fr_ids = [str(s.get('id') or s.get('supplierId'))
                  for s in (sups or [])
                  if franchise in (s.get('title') or s.get('name') or s.get('supplierName') or '')
                  and (s.get('id') or s.get('supplierId'))]
        res = sync_wolt_fees_window(session, str(b['bilboy_branch_id']), b['id'],
                                    fr_ids, log, today_il=today, branch_name=b['name'])
        calls += sum(r.get('api_calls', 0) for r in res)
        summary = ' | '.join(
            f"{r['month']} {r['action']} ₪{r['amount']:,.2f}" if 'action' in r else
            (f"{r['month']} late-check ₪{r.get('amount', 0):,.2f}" if r.get('checked')
             else f"{r['month']} late-check skipped (row present)")
            for r in res)
        print(f"{b['id']:>5} {b['name']:<24} {summary}")
    print(f'BilBoy calls: {calls}')

    conn = _get_db()
    rows = conn.execute(
        "SELECT id, branch_id, month, amount FROM fixed_expenses WHERE name=? "
        "AND source='bilboy_wolt' AND month >= '2026-08' ORDER BY month, branch_id",
        (WOLT_EXPENSE_NAME,)).fetchall()
    conn.close()
    print(f'\nעמלות Wolt rows (month >= 2026-08): {len(rows)}')
    for r in rows:
        print(f"  id={r['id']} {r['branch_id']} {r['month']} ₪{r['amount']:,.2f}")


if __name__ == '__main__':
    main()

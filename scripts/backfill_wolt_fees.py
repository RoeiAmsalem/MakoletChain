"""Backfill the system 'עמלות Wolt' fixed-expense rows from זיכיונות BilBoy docs.

Usage (on the server, venv python):
    python scripts/backfill_wolt_fees.py                # dry-run, all branches, Jun+Jul+Aug 2026
    python scripts/backfill_wolt_fees.py --apply        # write the rows
    python scripts/backfill_wolt_fees.py --months 2026-07 --branches 9020

ANCHOR STOP-RULE: before applying, the audited July anchors must land to the
shekel — 9020 2026-07 = ₪19,236 (±1) and 9020 2026-06 = ₪1,961 (±1), per the
read-only itemization of 2026-08-06. If either misses, the script REFUSES to
apply and exits non-zero.

Counts every BilBoy call. Read-only against BilBoy; writes only fixed_expenses
rows (source='bilboy_wolt') and only with --apply.
"""
import argparse
import calendar
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

import requests

from agents.bilboy import (
    _api_get, _get_db, extract_wolt_fee_total, fetch_wolt_fee_docs,
    upsert_wolt_fee_expense, CHAIN_TOKEN_ENV,
)

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
log = logging.getLogger('wolt_backfill')

# Audited anchors (incl VAT): branch -> {month: amount}
ANCHORS = {
    9020: {'2026-07': 19236.0, '2026-06': 1961.0},
}
ANCHOR_TOLERANCE = 1.0


def month_range(month: str):
    y, m = map(int, month.split('-'))
    last = calendar.monthrange(y, m)[1]
    return f'{month}-01', f'{month}-{last:02d}'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true', help='Write rows (default: dry-run)')
    ap.add_argument('--months', default='2026-06,2026-07,2026-08')
    ap.add_argument('--branches', default='', help='Comma-separated local ids (default: all active with bilboy_branch_id)')
    args = ap.parse_args()

    token = os.environ.get(CHAIN_TOKEN_ENV) or ''
    if not token:
        print('NO BILBOY_CHAIN_TOKEN in env'); sys.exit(2)
    session = requests.Session()
    session.headers.update({'Authorization': f'Bearer {token}'})

    conn = _get_db()
    q = ("SELECT id, name, bilboy_branch_id, franchise_supplier FROM branches "
         "WHERE active=1 AND bilboy_branch_id IS NOT NULL ORDER BY id")
    branches = [dict(r) for r in conn.execute(q).fetchall()]
    if args.branches:
        want = {int(b) for b in args.branches.split(',')}
        branches = [b for b in branches if b['id'] in want]

    months = [m.strip() for m in args.months.split(',') if m.strip()]
    total_calls = 0
    results = []          # (branch_id, name, month, docs, amount)

    for b in branches:
        bb_id = str(b['bilboy_branch_id'])
        franchise = b['franchise_supplier'] or 'זיכיונות המכולת בע"מ'
        # 1 suppliers call per branch to resolve the franchise supplier id(s)
        raw = _api_get(session, '/customer/suppliers',
                       params={'customerBranchId': bb_id, 'all': 'true'})
        total_calls += 1
        sup_list = raw.get('suppliers') if isinstance(raw, dict) else raw
        fr_ids = []
        for s in (sup_list or []):
            nm = s.get('title') or s.get('name') or s.get('supplierName') or ''
            sid = str(s.get('id') or s.get('supplierId') or '')
            if franchise and franchise in nm and sid:
                fr_ids.append(sid)
        if not fr_ids:
            log.info('branch %d (%s): no franchise supplier on BilBoy — skip', b['id'], b['name'])
            continue
        for month in months:
            frm, to = month_range(month)
            docs, calls = fetch_wolt_fee_docs(session, bb_id, fr_ids, frm, to, log)
            total_calls += calls
            amount = extract_wolt_fee_total(docs)
            results.append((b['id'], b['name'], month, len(docs), amount))

    # ── Anchor gate ──
    anchor_fail = False
    for bid, anchors in ANCHORS.items():
        for month, want in anchors.items():
            got = next((r[4] for r in results if r[0] == bid and r[2] == month), None)
            if got is None:
                continue  # branch/month not in this run's scope
            ok = abs(got - want) <= ANCHOR_TOLERANCE
            print(f'ANCHOR {bid} {month}: got ₪{got:,.2f} want ₪{want:,.2f} '
                  f'{"OK" if ok else "FAIL"}')
            if not ok:
                anchor_fail = True

    print(f'\n{"branch":>6} {"name":<22} {"month":<8} {"docs":>4} {"wolt ₪":>12}')
    for bid, name, month, ndocs, amount in results:
        print(f'{bid:>6} {name:<22} {month:<8} {ndocs:>4} {amount:>12,.2f}')
    print(f'\nBilBoy calls: {total_calls}')

    if anchor_fail:
        print('\nANCHOR MISMATCH — refusing to apply. Nothing written.')
        sys.exit(1)

    if not args.apply:
        print('\nDRY-RUN — no rows written. Re-run with --apply.')
        return

    written = 0
    for bid, name, month, ndocs, amount in results:
        action = upsert_wolt_fee_expense(conn, bid, month, amount)
        if action in ('upserted',):
            written += 1
        print(f'  {bid} {month}: {action} ₪{amount:,.2f}')
    conn.close()
    print(f'\nAPPLIED — {written} rows upserted.')


if __name__ == '__main__':
    main()

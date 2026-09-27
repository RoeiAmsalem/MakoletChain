"""Backfill the system 'עמלות Wolt' fixed-expense rows from זיכיונות BilBoy docs.

Usage (on the server, venv python):
    python scripts/backfill_wolt_fees.py                # dry-run, all branches, Jun+Jul+Aug 2026
    python scripts/backfill_wolt_fees.py --apply        # write the rows
    python scripts/backfill_wolt_fees.py --months 2026-07 --branches 9020

ANCHOR STOP-RULE: every audited anchor is ALWAYS fetched and checked — even
when --months/--branches exclude it — and a missing/unreadable anchor counts
as a FAIL (never silently skipped). Anchors: 9020 2026-06 = ₪1,961 (±1) and
2026-07 = ₪19,236 (±1) per the 2026-08-06 itemization; 9020 2026-08 =
₪20,207.00 and 9017 2026-08 = ₪5,728.00 to the agora per the 2026-09-27
zik audit. If any misses, the script REFUSES to apply and exits non-zero.

Counts every BilBoy call. Read-only against BilBoy; writes only fixed_expenses
rows (source='bilboy_wolt') and only with --apply.
"""
import argparse
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
from utils.sync_window import month_bounds

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
log = logging.getLogger('wolt_backfill')

# Audited anchors (incl VAT): branch -> {month: (amount, tolerance)}
ANCHORS = {
    9020: {'2026-06': (1961.0, 1.0), '2026-07': (19236.0, 1.0),
           '2026-08': (20207.00, 0.005)},
    9017: {'2026-08': (5728.00, 0.005)},
}


def fetch_targets(branch_ids, months):
    """(branch_id, month) pairs to fetch: the requested scope PLUS every
    anchor, so the anchor gate can never be scoped away."""
    targets = {(b, m) for b in branch_ids for m in months}
    targets |= {(b, m) for b, ms in ANCHORS.items() for m in ms}
    return sorted(targets)


def check_anchors(results):
    """results: {(branch_id, month): amount or None (unreadable)}.
    Returns (lines, failed). A missing or unreadable anchor is a FAIL."""
    lines, failed = [], False
    for bid, ms in ANCHORS.items():
        for month, (want, tol) in ms.items():
            got = results.get((bid, month))
            if got is None:
                lines.append(f'ANCHOR {bid} {month}: MISSING (not fetched / '
                             f'unreadable) want ₪{want:,.2f} FAIL')
                failed = True
                continue
            ok = abs(got - want) <= tol
            lines.append(f'ANCHOR {bid} {month}: got ₪{got:,.2f} want '
                         f'₪{want:,.2f} {"OK" if ok else "FAIL"}')
            failed = failed or not ok
    return lines, failed


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
    all_branches = {r['id']: dict(r) for r in conn.execute(q).fetchall()}
    scope = sorted(all_branches)
    if args.branches:
        scope = sorted({int(b) for b in args.branches.split(',')})
    months = [m.strip() for m in args.months.split(',') if m.strip()]
    targets = fetch_targets(scope, months)
    requested = {(b, m) for b in scope for m in months}

    total_calls = 0
    amounts = {}          # (branch_id, month) -> amount | None (unreadable)
    info = {}             # (branch_id, month) -> (name, docs)
    fr_cache = {}
    for bid, month in targets:
        b = all_branches.get(bid)
        if b is None:
            log.warning('branch %d: not active / no bilboy_branch_id — cannot read', bid)
            amounts[(bid, month)] = None
            continue
        bb_id = str(b['bilboy_branch_id'])
        if bid not in fr_cache:
            franchise = b['franchise_supplier'] or 'זיכיונות המכולת בע"מ'
            # 1 suppliers call per branch to resolve the franchise supplier id(s)
            raw = _api_get(session, '/customer/suppliers',
                           params={'customerBranchId': bb_id, 'all': 'true'})
            total_calls += 1
            sup_list = raw.get('suppliers') if isinstance(raw, dict) else raw
            fr_cache[bid] = [
                str(s.get('id') or s.get('supplierId') or '')
                for s in (sup_list or [])
                if franchise and franchise in (s.get('title') or s.get('name')
                                               or s.get('supplierName') or '')
                and (s.get('id') or s.get('supplierId'))]
        fr_ids = fr_cache[bid]
        if not fr_ids:
            log.info('branch %d (%s): no franchise supplier on BilBoy — skip', bid, b['name'])
            amounts[(bid, month)] = None
            continue
        frm, to = month_bounds(month)
        docs, calls, failures = fetch_wolt_fee_docs(session, bb_id, fr_ids, frm, to, log)
        total_calls += calls
        # A partial read is unreadable — never written, and fails an anchor.
        amounts[(bid, month)] = None if failures else extract_wolt_fee_total(docs)
        info[(bid, month)] = (b['name'], len(docs))

    # ── Anchor gate (always runs) ──
    lines, anchor_fail = check_anchors(amounts)
    for ln in lines:
        print(ln)

    print(f'\n{"branch":>6} {"name":<22} {"month":<8} {"docs":>4} {"wolt ₪":>12}')
    for (bid, month) in targets:
        name, ndocs = info.get((bid, month), ('?', 0))
        amt = amounts.get((bid, month))
        shown = 'UNREADABLE' if amt is None else f'{amt:,.2f}'
        tag = '' if (bid, month) in requested else '  (anchor only)'
        print(f'{bid:>6} {name:<22} {month:<8} {ndocs:>4} {shown:>12}{tag}')
    print(f'\nBilBoy calls: {total_calls}')

    if anchor_fail:
        print('\nANCHOR MISMATCH — refusing to apply. Nothing written.')
        sys.exit(1)

    if not args.apply:
        print('\nDRY-RUN — no rows written. Re-run with --apply.')
        return

    written = 0
    for (bid, month) in sorted(requested):
        amt = amounts.get((bid, month))
        if amt is None:
            print(f'  {bid} {month}: skipped (unreadable)')
            continue
        action = upsert_wolt_fee_expense(conn, bid, month, amt)
        if action == 'upserted':
            written += 1
        print(f'  {bid} {month}: {action} ₪{amt:,.2f}')
    conn.close()
    print(f'\nAPPLIED — {written} rows upserted.')


if __name__ == '__main__':
    main()

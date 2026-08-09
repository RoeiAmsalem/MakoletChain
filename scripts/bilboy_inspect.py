#!/usr/bin/env python3
"""READ-ONLY BilBoy inspector — what the sync sees before it filters.

Two questions this answers, without writing a single row:
  1. What are the unknown-status docs the sync silently drops? (branch 9020
     June 2026 dropped 72 docs / ₪218,201.72 on status 11, which is not in
     agents.bilboy.KNOWN_STATUSES = {3, 5, 7, 9}.)
  2. What is actually invoiced under branches.franchise_supplier
     (זיכיונות המכולת בע"מ) — the supplier the sync must NEVER import into
     goods_documents — including line items, so Wolt fees and anything else
     hiding in there is visible.

It reuses agents.bilboy's own session/auth/paging helpers so it sees exactly
what the sync sees. It calls ONLY GET endpoints and never touches the DB.

Usage: python3 scripts/bilboy_inspect.py <branch_id> <YYYY-MM> [--items]
       --items also dumps the line items of each franchise doc.
"""
import os
import sys
from calendar import monthrange
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.bilboy import (  # noqa: E402
    _api_get, _branch_session, _get_branch_config, _setup_logger,
    KNOWN_STATUSES, EXCLUDED_STATUSES, is_wolt_fee_line,
)


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    branch_id = int(sys.argv[1])
    year, month = (int(x) for x in sys.argv[2].split('-'))
    want_items = '--items' in sys.argv

    from_date = f'{year}-{month:02d}-01'
    to_date = f'{year}-{month:02d}-{monthrange(year, month)[1]:02d}'

    branch = _get_branch_config(branch_id)
    log = _setup_logger(branch_id)
    session = _branch_session(branch, branch_id)
    bb = str(branch['bilboy_branch_id'])
    franchise = (branch.get('franchise_supplier') or '').strip()
    print(f"branch={branch_id} {branch['name']}  bilboy_branch_id={bb}")
    print(f"window {from_date} .. {to_date}   franchise_supplier={franchise!r}")
    print(f"KNOWN_STATUSES={sorted(KNOWN_STATUSES)}  EXCLUDED={sorted(EXCLUDED_STATUSES)}\n")

    sup = _api_get(session, '/customer/suppliers',
                   params={'customerBranchId': bb, 'all': 'true'})
    slist = sup.get('suppliers') if isinstance(sup, dict) else sup
    all_ids, franchise_ids = [], []
    name_by_id = {}
    for s in (slist or []):
        sid = str(s.get('id') or s.get('supplierId') or '')
        nm = (s.get('title') or s.get('name') or s.get('supplierName') or '').strip()
        if not sid:
            continue
        all_ids.append(sid)
        name_by_id[sid] = nm
        if franchise and franchise in nm:
            franchise_ids.append(sid)
    print(f"suppliers: {len(all_ids)} total, franchise-matching: "
          f"{[(i, name_by_id[i]) for i in franchise_ids]}\n")

    # ── every doc header in the window, batched by 30 like the sync ──
    hlist = []
    for i in range(0, max(len(all_ids), 1), 30):
        batch = all_ids[i:i + 30]
        h = _api_get(session, '/customer/docs/headers', params={
            'suppliers': ','.join(batch), 'branches': bb,
            'from': f'{from_date}T00:00:00', 'to': f'{to_date}T00:00:00'})
        hlist += h if isinstance(h, list) else (
            h.get('data') or h.get('docs') or h.get('headers') or [])

    by_status = defaultdict(list)
    for h in hlist:
        by_status[h.get('status')].append(h)

    print('=' * 72)
    print('ALL DOC HEADERS BY STATUS')
    print(f"{'status':>7} {'docs':>5} {'amount':>14}  verdict")
    for st in sorted(by_status, key=lambda x: (x is None, x)):
        rows = by_status[st]
        amt = sum(float(r.get('totalWithVat') or 0) for r in rows)
        if st in EXCLUDED_STATUSES:
            v = 'EXCLUDED (superseded)'
        elif st in KNOWN_STATUSES:
            v = 'kept'
        else:
            v = '>>> UNKNOWN — DROPPED <<<'
        print(f"{str(st):>7} {len(rows):>5} {amt:>14,.2f}  {v}")

    for st, rows in sorted(by_status.items(), key=lambda kv: (kv[0] is None, kv[0])):
        if st in KNOWN_STATUSES or st in EXCLUDED_STATUSES:
            continue
        print(f"\n--- UNKNOWN STATUS {st}: {len(rows)} docs ---")
        for r in sorted(rows, key=lambda x: str(x.get('date')))[:15]:
            print(f"  ref={r.get('refNumber') or r.get('number')} "
                  f"date={str(r.get('date'))[:10]} type={r.get('type')} "
                  f"total={float(r.get('totalWithVat') or 0):>11,.2f} "
                  f"supplier={(r.get('supplierName') or '').strip()[:34]}")
        if len(rows) > 15:
            print(f"  ... {len(rows) - 15} more")

    # ── doc TYPE breakdown (ALLOWED_DOC_TYPES = {2,3,4,5}; others dropped) ──
    from agents.bilboy import ALLOWED_DOC_TYPES
    print('\n' + '=' * 72)
    print('DOC HEADERS BY TYPE (kept statuses only, franchise excluded)')
    keptrows = [h for h in hlist
                if h.get('status') not in EXCLUDED_STATUSES
                and not (franchise and franchise in (h.get('supplierName') or ''))]
    bytype2 = defaultdict(lambda: [0, 0.0])
    for h in keptrows:
        b = bytype2[h.get('type')]
        b[0] += 1
        b[1] += float(h.get('totalWithVat') or 0)
    print(f"{'type':>5} {'docs':>5} {'amount':>14}  verdict")
    for t in sorted(bytype2, key=lambda x: (x is None, x)):
        n, amt = bytype2[t]
        v = 'kept' if t in ALLOWED_DOC_TYPES else '>>> DROPPED (wrong type) <<<'
        print(f"{str(t):>5} {n:>5} {amt:>14,.2f}  {v}")
    for t in sorted(bytype2, key=lambda x: (x is None, x)):
        if t in ALLOWED_DOC_TYPES:
            continue
        print(f"\n--- DROPPED TYPE {t} ---")
        for r in sorted([h for h in keptrows if h.get('type') == t],
                        key=lambda x: -abs(float(x.get('totalWithVat') or 0)))[:20]:
            print(f"  ref={r.get('refNumber') or r.get('number')} "
                  f"date={str(r.get('date'))[:10]} status={r.get('status')} "
                  f"total={float(r.get('totalWithVat') or 0):>12,.2f} "
                  f"supplier={(r.get('supplierName') or '').strip()[:34]}")

    # ── franchise supplier detail ──
    print('\n' + '=' * 72)
    print(f'FRANCHISE SUPPLIER DOCS ({franchise}) — NEVER imported to goods')
    fdocs = [h for h in hlist
             if franchise and franchise in (h.get('supplierName') or '')]
    if not fdocs:
        print('  none in this window')
    ftot = sum(float(d.get('totalWithVat') or 0) for d in fdocs)
    print(f"  {len(fdocs)} docs, total ₪{ftot:,.2f}")
    bytype = defaultdict(lambda: [0, 0.0])
    for d in fdocs:
        b = bytype[(d.get('type'), d.get('status'))]
        b[0] += 1
        b[1] += float(d.get('totalWithVat') or 0)
    for (t, st), (n, amt) in sorted(bytype.items(), key=lambda kv: str(kv[0])):
        print(f"    type={t} status={st}: {n} docs  ₪{amt:,.2f}")

    for d in sorted(fdocs, key=lambda x: str(x.get('date'))):
        print(f"\n  ref={d.get('refNumber') or d.get('ref')} "
              f"date={str(d.get('date'))[:10]} type={d.get('type')} "
              f"status={d.get('status')} total=₪{float(d.get('totalWithVat') or 0):,.2f}")
        if not want_items or d.get('type') not in (2, 3):
            continue
        try:
            raw = _api_get(session, '/customer/doc',
                           params={'docId': d.get('id')}, timeout=15)
        except Exception as e:
            print(f'      (detail failed: {e})')
            continue
        items = ((raw or {}).get('body') or {}).get('items') or []
        for it in items:
            mark = 'WOLT>' if is_wolt_fee_line(it) else '     '
            print(f"      {mark} {(it.get('name') or '')[:52]:<52} "
                  f"₪{float(it.get('total') or 0):>11,.2f}")


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""READ-ONLY זיכיונות line audit — what a line-router WOULD do for one month.

Fetches every franchise (זיכיונות המכולת) doc + line items for every active
branch (excl. demo 9998/9999), caches the raw JSON, then classifies each line
and compares against what the dashboard already counts. Never writes the DB
(opened mode=ro); only GET calls to BilBoy, hard-capped.

Usage:
  python3 scripts/zik_audit.py 2026-08 fetch      # BilBoy calls → /tmp cache
  python3 scripts/zik_audit.py 2026-08 report     # classify from cache, 0 calls
Options: --cap N (default 450), --branches 9015,9018
"""
import json
import os
import re
import sqlite3
import sys
import time
from calendar import monthrange
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

DB = os.path.join(ROOT, 'db', 'makolet_chain.db')
STAGING_DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
ZIK = 'זיכיונות המכולת'
EXCLUDED_BRANCHES = (9998, 9999)
THROTTLE = 0.35


def ro(path):
    conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def cache_path(month):
    return f'/tmp/zik_audit_{month}.json'


# ───────────────────────────── fetch ─────────────────────────────

class Budget:
    def __init__(self, cap):
        self.cap, self.used = cap, 0

    def get(self, api_get, session, path, **kw):
        if self.used >= self.cap:
            raise RuntimeError(f'BilBoy call cap {self.cap} reached')
        self.used += 1
        time.sleep(THROTTLE)
        return api_get(session, path, **kw)


def _hlist(h):
    return h if isinstance(h, list) else (
        h.get('data') or h.get('docs') or h.get('headers') or [])


def fetch(month, cap, only):
    from agents.bilboy import _api_get, _branch_session, EXCLUDED_STATUSES
    y, m = (int(x) for x in month.split('-'))
    frm = f'{month}-01T00:00:00'
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    to = f'{ny}-{nm:02d}-01T00:00:00'   # exclusive-ish; filter by date prefix below

    conn = ro(DB)
    branches = [dict(r) for r in conn.execute(
        'SELECT * FROM branches WHERE active=1 AND id NOT IN (?,?) ORDER BY id',
        EXCLUDED_BRANCHES)]
    conn.close()
    if only:
        branches = [b for b in branches if b['id'] in only]

    budget = Budget(cap)
    out = {'month': month, 'branches': {}}
    # PHASE A — headers only
    for b in branches:
        bid, bb = b['id'], str(b.get('bilboy_branch_id') or '')
        rec = {'name': b['name'], 'bb': bb, 'franchise_ids': [], 'headers': [],
               'error': None}
        out['branches'][str(bid)] = rec
        if not bb:
            rec['error'] = 'no bilboy_branch_id'
            continue
        try:
            s = _branch_session(b, bid)
            sup = budget.get(_api_get, s, '/customer/suppliers',
                             params={'customerBranchId': bb, 'all': 'true'})
            slist = sup.get('suppliers') if isinstance(sup, dict) else sup
            fids = [str(x.get('id') or x.get('supplierId'))
                    for x in (slist or [])
                    if ZIK in (x.get('title') or x.get('name') or x.get('supplierName') or '')]
            rec['franchise_ids'] = fids
            if not fids:
                continue
            h = budget.get(_api_get, s, '/customer/docs/headers', params={
                'suppliers': ','.join(fids), 'branches': bb, 'from': frm, 'to': to})
            rec['headers'] = [x for x in _hlist(h)
                              if str(x.get('date') or '')[:7] == month]
        except Exception as e:
            rec['error'] = f'{type(e).__name__}: {str(e)[:160]}'
        n = len(rec['headers'])
        print(f"A {bid:>5} {b['name']:<22} fids={rec['franchise_ids']} docs={n} "
              f"err={rec['error']}", flush=True)

    todo = [(bid, h) for bid, r in out['branches'].items() for h in r['headers']
            if h.get('status') not in EXCLUDED_STATUSES]
    projected = budget.used + len(todo)
    print(f'PHASE A done: calls={budget.used}  detail docs={len(todo)}  '
          f'projected={projected}/{cap}', flush=True)
    if projected > cap:
        json.dump(out, open(cache_path(month), 'w'), ensure_ascii=False)
        sys.exit(f'STOP: projected {projected} > cap {cap} — ask before continuing')

    # PHASE B — details
    sessions = {}
    for bid, h in todo:
        b = next(x for x in branches if str(x['id']) == bid)
        s = sessions.get(bid) or _branch_session(b, int(bid))
        sessions[bid] = s
        try:
            raw = budget.get(_api_get, s, '/customer/doc',
                             params={'docId': h.get('id')}, timeout=20)
            h['_detail'] = raw
        except Exception as e:
            h['_detail_error'] = f'{type(e).__name__}: {str(e)[:160]}'
    out['calls'] = budget.used
    json.dump(out, open(cache_path(month), 'w'), ensure_ascii=False)
    print(f'PHASE B done: BilBoy calls used {budget.used}/{cap} → {cache_path(month)}')


if __name__ == '__main__':
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    if len(args) < 2 or not re.fullmatch(r'\d{4}-\d{2}', args[0]):
        sys.exit(__doc__)
    month, mode = args[0], args[1]
    cap = 450
    only = None
    for a in sys.argv[1:]:
        if a.startswith('--cap='):
            cap = int(a.split('=', 1)[1])
        if a.startswith('--branches='):
            only = {int(x) for x in a.split('=', 1)[1].split(',')}
    if mode == 'fetch':
        fetch(month, cap, only)
    elif mode == 'report':
        from zik_audit_report import report  # noqa  (added below)
        report(month)
    else:
        sys.exit(__doc__)

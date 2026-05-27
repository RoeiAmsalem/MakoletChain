"""Read-only probe: BilBoy CHAIN token (userId=136, יניב בן אלי).

Goal: confirm the chain token works against BilBoy's REST API and figure out
what bilboy.py needs to change to support a one-token-many-branches setup
(mirrors the Aviv chain migration).

Usage:
  BILBOY_CHAIN_TOKEN='<jwt>' python3 scripts/probe_bilboy_chain.py

NEVER logs the token. NEVER writes to DB. Probe-only.

Steps:
  1. GET /api/user/branches with Bearer <token>
     → list every chain branch (BilBoy customerBranchId + name)
     → flag 126 / 127 if present
  2. For the first matching branch (prefer 126), do a tiny read-only doc pull:
     /api/customer/suppliers (filter franchise) + /api/customer/docs/headers
     for a 14-day window. Print response shape + a few records.
  3. Compare against the local DB's goods_documents (if any) for the same branch.
"""

import os
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

import requests


API_BASE = 'https://app.billboy.co.il:5050/api'
DB_PATH = Path(__file__).resolve().parent.parent / 'db' / 'makolet_chain.db'


def fail(msg: str, code: int = 2) -> int:
    print(f'ERROR: {msg}', file=sys.stderr)
    return code


def short(v, n: int = 120) -> str:
    s = str(v)
    return s if len(s) <= n else s[:n] + '...'


def main() -> int:
    token = os.environ.get('BILBOY_CHAIN_TOKEN')
    if not token:
        return fail('BILBOY_CHAIN_TOKEN not set in env')

    s = requests.Session()
    s.headers.update({'Authorization': f'Bearer {token}'})

    # ── TASK 1: /user/branches ──────────────────────────────────────
    print('━━━ TASK 1: GET /api/user/branches ━━━')
    r = s.get(f'{API_BASE}/user/branches', timeout=30)
    print(f'[branches] HTTP {r.status_code}')
    if r.status_code != 200:
        print(f'[branches] body={short(r.text, 300)!r}')
        return fail('chain token rejected')

    try:
        body = r.json()
    except Exception as e:
        return fail(f'non-JSON: {e}')

    branches = body if isinstance(body, list) else (
        body.get('branches') or body.get('data') or []
    )
    print(f'[branches] count={len(branches)}')
    if branches:
        sample = branches[0]
        print(f'[branches] first-record keys={list(sample.keys())}')

    # print every branch (id + name) — limit to relevant id-ish fields
    bb_ids = []
    for b in branches:
        bid = b.get('branchId') or b.get('id') or b.get('customerBranchId') or b.get('branch_id')
        name = (b.get('branchName') or b.get('name') or b.get('title') or '').strip()
        print(f'  bb_id={bid!r:>6}  name={name!r}')
        try:
            bb_ids.append(int(bid))
        except (TypeError, ValueError):
            pass

    has_126 = 126 in bb_ids
    has_127 = 127 in bb_ids
    print(f'\n[branches] contains 126 (Shimon)? {has_126}')
    print(f'[branches] contains 127 (Tichon)? {has_127}')

    # ── TASK 2: docs probe for one branch ──────────────────────────
    target = 126 if has_126 else (bb_ids[0] if bb_ids else None)
    if target is None:
        return fail('no usable branch id in chain', 0)

    print(f'\n━━━ TASK 2: docs probe with customerBranchId={target} ━━━')

    # suppliers
    rs = s.get(f'{API_BASE}/customer/suppliers',
               params={'customerBranchId': target, 'all': 'true'},
               timeout=30)
    print(f'[suppliers] HTTP {rs.status_code}')
    if rs.status_code != 200:
        print(f'[suppliers] body={short(rs.text, 300)!r}')
        return fail('suppliers endpoint failed', 0)

    sbody = rs.json()
    suppliers = sbody.get('suppliers') if isinstance(sbody, dict) else sbody
    print(f'[suppliers] count={len(suppliers or [])}')
    if suppliers:
        first = suppliers[0]
        print(f'[suppliers] first-record keys={list(first.keys())}')
        print(f'[suppliers] sample={short(first, 200)}')

    # narrow to first 30 supplier ids (URL limit per CLAUDE.md)
    keep = []
    for sup in (suppliers or []):
        sid = sup.get('id') or sup.get('supplierId')
        if sid:
            keep.append(str(sid))
        if len(keep) >= 30:
            break
    if not keep:
        return fail('no supplier ids', 0)

    today = date.today()
    frm = (today - timedelta(days=14)).isoformat()
    to = today.isoformat()

    # Try with customerBranchId first (chain shape we expect), then fall back
    # to legacy ?branches=N if the chain endpoint demands the new param name.
    attempts = [
        ('customerBranchId', {'suppliers': ','.join(keep), 'customerBranchId': target,
                              'from': f'{frm}T00:00:00', 'to': f'{to}T00:00:00'}),
        ('branches (legacy)', {'suppliers': ','.join(keep), 'branches': target,
                               'from': f'{frm}T00:00:00', 'to': f'{to}T00:00:00'}),
    ]

    docs = None
    used_shape = None
    for label, params in attempts:
        rd = s.get(f'{API_BASE}/customer/docs/headers', params=params, timeout=30)
        print(f'[docs/{label}] HTTP {rd.status_code}')
        if rd.status_code == 200:
            try:
                jb = rd.json()
            except Exception as e:
                print(f'[docs/{label}] non-JSON: {e}')
                continue
            lst = jb if isinstance(jb, list) else (
                jb.get('data') or jb.get('docs') or jb.get('headers') or []
            )
            print(f'[docs/{label}] count={len(lst)} top_shape={type(jb).__name__}')
            if lst:
                print(f'[docs/{label}] first-record keys={list(lst[0].keys())}')
                # sample 3 records — id-ish fields only, no PII dumps
                for d in lst[:3]:
                    print(
                        '  doc: '
                        f'type={d.get("type")} '
                        f'status={d.get("status")} '
                        f'ref={(d.get("refNumber") or d.get("number"))!r} '
                        f'supplier={short(d.get("supplierName"), 30)!r} '
                        f'date={(d.get("date") or d.get("documentDate"))!r} '
                        f'amount={d.get("totalWithVat") or d.get("totalAmount") or d.get("amount")}'
                    )
            docs = lst
            used_shape = label
            break
        else:
            print(f'[docs/{label}] body={short(rd.text, 200)!r}')

    if docs is None:
        return fail('both docs param shapes failed', 0)

    # ── TASK 3: compare against local DB ───────────────────────────
    print(f'\n━━━ TASK 3: compare to local goods_documents (branch_id=126) ━━━')
    if not DB_PATH.exists():
        print(f'[db] {DB_PATH} not present — skipping local comparison')
    else:
        conn = sqlite3.connect(f'file:{DB_PATH}?mode=ro', uri=True)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT COUNT(*) as n, COALESCE(SUM(amount),0) as total "
            "FROM goods_documents WHERE branch_id=126 AND doc_date >= ?",
            (frm,)
        ).fetchone()
        conn.close()
        print(f'[db] local docs for 126 since {frm}: n={rows["n"]} total=₪{rows["total"]:.2f}')
        chain_total = sum(
            float(d.get('totalWithVat') or d.get('totalAmount') or d.get('amount') or 0)
            for d in docs
        )
        print(f'[chain] same window via chain token: n={len(docs)} total=₪{chain_total:.2f}')
        print(f'[shape] used param={used_shape!r}')

    print('\nDONE — token never logged.')
    return 0


if __name__ == '__main__':
    sys.exit(main())

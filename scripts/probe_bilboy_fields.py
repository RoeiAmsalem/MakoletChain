"""READ-ONLY: what does BilBoy expose per document that we don't store?

Writes NOTHING. Outbound calls only to BilBoy (chain token), same endpoints the
nightly agent uses, plus probes for a line-item/detail endpoint. Dumps the full
header JSON shape and tests whether line detail is nested (free) or a separate
per-doc call.

Usage: python scripts/probe_bilboy_fields.py [local_branch_id] [YYYY-MM]
"""
import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

load_dotenv()

API_BASE = "https://app.billboy.co.il:5050/api"
DB = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')

local_id = int(sys.argv[1]) if len(sys.argv) > 1 else 126
month = sys.argv[2] if len(sys.argv) > 2 else '2026-05'
y, m = month.split('-')
from_date = f'{y}-{m}-01T00:00:00'
to_date = f'{y}-{m}-28T00:00:00'

con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row
br = con.execute("SELECT bilboy_branch_id, franchise_supplier FROM branches WHERE id=?",
                 (local_id,)).fetchone()
bb_id = str(br['bilboy_branch_id'])
franchise = br['franchise_supplier'] or ''
print(f"branch {local_id} -> bilboy_branch_id={bb_id}  month={month}  franchise={franchise!r}\n")

token = os.environ.get('BILBOY_CHAIN_TOKEN') or ''
assert token, "BILBOY_CHAIN_TOKEN not set"
S = requests.Session()
S.headers.update({'Authorization': f'Bearer {token}'})


def get(path, **params):
    r = S.get(f"{API_BASE}{path}", params=params, timeout=30)
    return r.status_code, r


# 1) suppliers
sc, r = get('/customer/suppliers', customerBranchId=bb_id, all='true')
raw = r.json()
suppliers = raw.get('suppliers') if isinstance(raw, dict) else raw
print(f"=== /customer/suppliers -> HTTP {sc}, {len(suppliers)} suppliers")
if suppliers:
    print("    full keys of one supplier:")
    print("    " + json.dumps(suppliers[0], ensure_ascii=False, indent=2)[:800])
ids = [str(s.get('id') or s.get('supplierId') or '') for s in suppliers
       if franchise not in (s.get('title') or s.get('name') or '')]
ids = [i for i in ids if i][:30]

# 2) docs/headers
sc, r = get('/customer/docs/headers', suppliers=','.join(ids), branches=bb_id,
            **{'from': from_date, 'to': to_date})
body = r.json()
docs = body if isinstance(body, list) else (body.get('data') or body.get('docs')
                                            or body.get('headers') or [])
print(f"\n=== /customer/docs/headers -> HTTP {sc}, {len(docs)} docs")
if not docs:
    print("    no docs this month — try another month arg")
    sys.exit(0)

# union of all keys across docs + a sample value for each
allkeys = {}
for d in docs:
    for k, v in d.items():
        if k not in allkeys:
            allkeys[k] = v
print(f"\n=== EVERY field present in a doc header ({len(allkeys)} keys) ===")
for k in sorted(allkeys):
    v = allkeys[k]
    vs = json.dumps(v, ensure_ascii=False)
    if isinstance(v, (list, dict)):
        vs = f"<{type(v).__name__} len={len(v)}>  " + vs[:300]
    print(f"  {k:24} = {vs[:160]}")

# full dump of ONE doc
print("\n=== FULL JSON of first doc header ===")
print(json.dumps(docs[0], ensure_ascii=False, indent=2)[:2500])

# detect nested line items in the header (FREE if present)
nested = [k for k, v in docs[0].items() if isinstance(v, list) and v]
print(f"\n=== nested arrays in header (FREE line-items if any): {nested or 'NONE'}")

# 3) hunt for a per-document detail/line endpoint
doc_id = (docs[0].get('id') or docs[0].get('docId') or docs[0].get('documentId')
          or docs[0].get('refNumber') or docs[0].get('number'))
print(f"\n=== probing detail endpoints for doc id={doc_id} (extra call test) ===")
candidates = [
    ('/customer/docs/detail', {'id': doc_id, 'branches': bb_id}),
    ('/customer/docs/lines', {'id': doc_id, 'branches': bb_id}),
    ('/customer/docs/rows', {'id': doc_id, 'branches': bb_id}),
    ('/customer/docs/items', {'id': doc_id, 'branches': bb_id}),
    (f'/customer/docs/{doc_id}', {'branches': bb_id}),
    ('/customer/doc', {'id': doc_id, 'branches': bb_id}),
    ('/customer/docs/headers', {'suppliers': ','.join(ids), 'branches': bb_id,
                                'from': from_date, 'to': to_date, 'withLines': 'true'}),
]
for path, params in candidates:
    try:
        r = S.get(f"{API_BASE}{path}", params=params, timeout=20)
        snip = r.text[:200].replace('\n', ' ')
        print(f"  {path:30} {json.dumps(params, ensure_ascii=False)[:60]:62} -> HTTP {r.status_code}  {snip}")
    except Exception as e:
        print(f"  {path:30} -> EXC {e!r}")

"""READ-ONLY follow-up: nail the BilBoy per-document line-item endpoint.

/customer/doc 400'd asking for 'docId'; /customer/docs/items 400'd asking for
'suppliers'. Probe both with the right params and dump full JSON (esp. nested
line arrays). Writes NOTHING.

Usage: python scripts/probe_bilboy_doc_detail.py [local_branch_id] [YYYY-MM]
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
frm, to = f'{y}-{m}-01T00:00:00', f'{y}-{m}-28T00:00:00'

con = sqlite3.connect(DB); con.row_factory = sqlite3.Row
bb_id = str(con.execute("SELECT bilboy_branch_id FROM branches WHERE id=?",
                        (local_id,)).fetchone()[0])
S = requests.Session()
S.headers.update({'Authorization': f"Bearer {os.environ['BILBOY_CHAIN_TOKEN']}"})


def get(path, **p):
    r = S.get(f"{API_BASE}{path}", params=p, timeout=30)
    return r


# grab one doc + its supplier id from headers
sup = get('/customer/suppliers', customerBranchId=bb_id, all='true').json()
sup = sup.get('suppliers') if isinstance(sup, dict) else sup
sids = [str(s['id']) for s in sup][:30]
docs = get('/customer/docs/headers', suppliers=','.join(sids), branches=bb_id,
           **{'from': frm, 'to': to}).json()
doc = docs[0]
doc_id, num = doc['id'], doc['number']
sup_id = next((str(s['id']) for s in sup if s.get('title') == doc['supplierName']), sids[0])
print(f"probe doc id={doc_id} number={num} supplier={doc['supplierName']!r} sup_id={sup_id}\n")


def dump(label, r):
    print(f"--- {label}: HTTP {r.status_code}")
    if r.status_code != 200:
        print("    " + r.text[:220].replace('\n', ' ')); return
    try:
        j = r.json()
    except Exception:
        print("    (non-JSON) " + r.text[:200]); return
    s = json.dumps(j, ensure_ascii=False, indent=2)
    print("    " + s[:1800].replace('\n', '\n    '))
    # surface nested line arrays
    node = j[0] if isinstance(j, list) and j else j
    if isinstance(node, dict):
        for k, v in node.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                print(f"\n    >>> LINE ARRAY '{k}' ({len(v)} rows); first row keys: {list(v[0].keys())}")
                print("    first row: " + json.dumps(v[0], ensure_ascii=False)[:400])


dump("/customer/doc?docId&branches", get('/customer/doc', docId=doc_id, branches=bb_id))
dump("/customer/doc?docId&customerBranchId", get('/customer/doc', docId=doc_id, customerBranchId=bb_id))
dump("/customer/doc?docId only", get('/customer/doc', docId=doc_id))
dump("/customer/docs/items?suppliers&branches&from&to",
     get('/customer/docs/items', suppliers=sup_id, branches=bb_id, **{'from': frm, 'to': to}))
dump("/customer/doc?docId&number&branches", get('/customer/doc', docId=doc_id, number=num, branches=bb_id))

"""READ-ONLY probe after the live ₪1 payment: why is /billing/payments/list
empty, and why does the CRM entity read return None fields when the document's
embedded customer clearly carries ExternalIdentifier='26'?

Reads only allowlisted endpoints via utils.sumit. No writes, no patching."""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT.startswith('/opt/makolet-chain') and 'staging' not in ROOT:
    sys.exit('REFUSED: staging-only.')
sys.path.insert(0, ROOT)

from dotenv import load_dotenv  # noqa: E402
load_dotenv(os.path.join(ROOT, '.env'))

from utils import sumit  # noqa: E402

print('── A. payments, wider window + raw envelope ──')
for params in ({'Date_From': '2026-06-01', 'Date_To': '2026-07-03'},
               {'DateFrom': '2026-06-01', 'DateTo': '2026-07-03'}):
    data = sumit._post('/billing/payments/list/', StartIndex=0, **params)
    payload = data.get('Data') or {}
    pays = payload.get('Payments') or []
    print(f"  params={list(params.keys())} Status={data.get('Status')} "
          f"payments={len(pays)} data_keys={sorted(payload.keys())}")
    for p in pays[:5]:
        print('   ', {k: p.get(k) for k in ('ID', 'CustomerID', 'Amount', 'Date',
                                            'ValidPayment', 'Status')})

print('\n── B. both ₪1 documents in detail ──')
for doc_id in (2087415637, 2087418238):
    doc = sumit.get_document(doc_id)
    cust = doc.get('Customer') if isinstance(doc.get('Customer'), dict) else {}
    print(f"  doc {doc_id}: Type={doc.get('Type')} Value={doc.get('DocumentValue')} "
          f"Desc={doc.get('Description')!r}")
    print(f"    customer: ID={cust.get('ID')} Name={cust.get('Name')!r} "
          f"Email={cust.get('EmailAddress')!r} ExternalIdentifier={cust.get('ExternalIdentifier')!r}")

print('\n── C. raw CRM entity reads (why None?) ──')
folder = sumit._customers_folder_id()
print(f"  customers folder id: {folder}")
listing = sumit._post('/crm/data/listentities/', Folder=folder)
rows = next((v for v in (listing.get('Data') or {}).values()
             if isinstance(v, list)), [])
print(f"  entities listed: {len(rows)}; first row raw: "
      f"{json.dumps(rows[0], ensure_ascii=False)[:400] if rows else None}")
for r in rows[:4]:
    cid = r.get('ID') if isinstance(r, dict) else None
    if cid is None:
        continue
    det = sumit._post('/crm/data/getentity/', Folder=folder, ID=cid)
    print(f"  getentity {cid}: raw Data = "
          f"{json.dumps(det.get('Data'), ensure_ascii=False)[:600]}")

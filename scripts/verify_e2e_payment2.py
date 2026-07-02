"""Second live e2e verification (july-test uid=29, real July clock, STAGING).

Runs רענן, then proves the join picked the RIGHT manager while TWO tagged
customers exist in SUMIT: new payment → receipt doc → ExternalIdentifier=='29',
uid=29 flips to paid, uid=26 (dennis) untouched. READ-ONLY vs SUMIT."""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT.startswith('/opt/makolet-chain') and 'staging' not in ROOT:
    sys.exit('REFUSED: staging-only.')
sys.path.insert(0, ROOT)

from app import app, get_db, _run_billing_sync, _now_il  # noqa: E402
from utils import sumit  # noqa: E402

month_start = _now_il().strftime('%Y-%m') + '-01'

print('── 0. sync (רענן) ──')
with app.test_request_context():
    print(' ', _run_billing_sync(get_db()))

print('\n── 1. payments this month ──')
payments = sumit.list_payments(month_start)
for p in sorted(payments, key=lambda x: str(x.get('Date'))):
    print(f"  ID={p.get('ID')} CustomerID={p.get('CustomerID')} "
          f"Amount={p.get('Amount')} Date={p.get('Date')} Valid={p.get('ValidPayment')}")

print('\n── 2. ALL documents → embedded customer tag (right-manager check) ──')
docs = sumit.list_documents(month_start)
for d in sorted(docs, key=lambda x: x.get('DocumentNumber') or 0):
    detail = sumit.get_document(d.get('DocumentID'))
    cust = detail.get('Customer') if isinstance(detail.get('Customer'), dict) else {}
    print(f"  doc №{d.get('DocumentNumber')} (₪{d.get('DocumentValue')}, "
          f"CustomerID={d.get('CustomerID')}) → Customer.ID={cust.get('ID')} "
          f"Name={cust.get('Name')!r} Email={cust.get('EmailAddress')!r} "
          f"ExternalIdentifier={cust.get('ExternalIdentifier')!r}")

print('\n── 3+4. manager_billing uid 29 (new) and 26 (must be untouched) ──')
with app.test_request_context():
    for uid in (29, 26):
        row = get_db().execute(
            'SELECT user_id, active, last_paid_date, last_status, updated_at '
            'FROM manager_billing WHERE user_id=?', (uid,)).fetchone()
        print(' ', dict(row) if row else None)

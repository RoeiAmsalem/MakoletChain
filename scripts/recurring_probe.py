"""READ-ONLY probe: recurring charge (הוראת קבע) objects for our test customers.

Phase-1 research for the בטל-מנוי button. Calls ONLY list/get endpoints:
  /billing/recurring/listforcustomer/  (IncludeInactive=true)
  /billing/paymentmethods/getforcustomer/
Direct requests (not utils/sumit — its allowlist doesn't carry these yet; the
probe enforces its own read-only set). NOTHING here writes to SUMIT.
"""
import json
import os
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

_ALLOWED = {'/billing/recurring/listforcustomer/',
            '/billing/paymentmethods/getforcustomer/'}


def post(endpoint, **body):
    assert endpoint in _ALLOWED, f'probe refuses non-read endpoint {endpoint}'
    creds = {'CompanyID': int(os.environ['SUMIT_ORG_ID']),
             'APIKey': os.environ['SUMIT_API_KEY']}
    r = requests.post('https://api.sumit.co.il' + endpoint,
                      json={'Credentials': creds, **body}, timeout=30)
    r.raise_for_status()
    return r.json()


# known SUMIT customer IDs from the e2e receipts (tag → customer)
CUSTOMERS = {
    '26': 2087415602,
    '29': 2087478523,
    '30': 2095375206,
    '31': 2095377663,
}

# find uid=33's customer via the allowlisted read client (receipt №40006)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import sumit  # noqa: E402
for d in sumit.list_documents('2026-07-01'):
    if d.get('DocumentNumber') == 40006:
        det = sumit.get_document(d['DocumentID'])
        cust = det.get('Customer') or {}
        print(f"doc 40006 → customer ID={cust.get('ID')} "
              f"tag={cust.get('ExternalIdentifier')!r}")
        CUSTOMERS['33'] = cust.get('ID')

for tag, cid in CUSTOMERS.items():
    print(f'\n── tag {tag} (customer {cid}) ──')
    res = post('/billing/recurring/listforcustomer/',
               Customer={'ID': cid}, IncludeInactive=True)
    if res.get('Status') != 0:
        print('  recurring list error:', res.get('UserErrorMessage'))
        continue
    items = (res.get('Data') or {}).get('RecurringItems') or []
    print(f'  recurring items: {len(items)}')
    for it in items:
        print('  ', json.dumps(it, ensure_ascii=False)[:400])
    pm = post('/billing/paymentmethods/getforcustomer/', Customer={'ID': cid})
    data = pm.get('Data') or {}
    pm_obj = data.get('PaymentMethod') if isinstance(data, dict) else data
    if isinstance(pm_obj, dict):
        masked = {k: v for k, v in pm_obj.items()
                  if k in ('ID', 'CreditCard_LastDigits', 'CreditCard_ExpirationMonth',
                           'CreditCard_ExpirationYear', 'Type')}
        print(f'  saved payment method: {masked}')
    else:
        print(f'  saved payment method: {pm_obj!r} '
              f'(status={pm.get("Status")}, err={pm.get("UserErrorMessage")!r})')

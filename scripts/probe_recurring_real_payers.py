"""READ-ONLY probe: recurring (הוראת קבע) items for REAL paying managers.

Phase-2 cancel re-verify (docs/billing_cancel_spec.md). Resolves each active
payer's SUMIT customer id from billing_payment_resolutions (zero API calls),
then lists their recurring items with IncludeInactive=true — proving we can
target the exact RecurringCustomerItemID the cancel endpoint needs.

ONLY calls /billing/recurring/listforcustomer/ (own single-endpoint allowlist).
NOTHING here writes to SUMIT.
"""
import json
import os
import sqlite3
import sys

import requests
from dotenv import load_dotenv

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(APP_DIR, '.env'))

_ALLOWED = {'/billing/recurring/listforcustomer/'}
calls = 0


def post(endpoint, **body):
    global calls
    assert endpoint in _ALLOWED, f'probe refuses non-read endpoint {endpoint}'
    creds = {'CompanyID': int(os.environ['SUMIT_ORG_ID']),
             'APIKey': os.environ['SUMIT_API_KEY']}
    calls += 1
    r = requests.post('https://api.sumit.co.il' + endpoint,
                      json={'Credentials': creds, **body}, timeout=30)
    r.raise_for_status()
    return r.json()


# Optional argv[1] = alternate DB path (e.g. prod's copy) — SELECT-only either way.
db_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(APP_DIR, 'db', 'makolet_chain.db')
db = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
db.row_factory = sqlite3.Row
rows = db.execute("""
    SELECT mb.user_id, u.name, mb.sumit_tag, mb.last_paid_date,
           (SELECT r.customer_id FROM billing_payment_resolutions r
             WHERE r.tag = mb.sumit_tag AND r.resolution = 'matched'
             ORDER BY r.payment_id DESC LIMIT 1) AS customer_id
    FROM manager_billing mb JOIN users u ON u.id = mb.user_id
    WHERE mb.active = 1 AND mb.last_paid_date IS NOT NULL
    ORDER BY mb.user_id
""").fetchall()

print(f'{len(rows)} active payers with a last_paid_date')
for r in rows:
    print(f"\n── uid {r['user_id']} {r['name']} tag={r['sumit_tag']!r} "
          f"last_paid={r['last_paid_date']} → customer {r['customer_id']}")
    if not r['customer_id']:
        print('   no matched resolution row — cannot resolve customer without a doc fetch; skipping')
        continue
    res = post('/billing/recurring/listforcustomer/',
               Customer={'ID': r['customer_id']}, IncludeInactive=True)
    if res.get('Status') != 0:
        print('   list error:', res.get('UserErrorMessage'))
        continue
    items = (res.get('Data') or {}).get('RecurringItems') or []
    print(f'   recurring items: {len(items)}')
    for it in items:
        keep = {k: it.get(k) for k in ('ID', 'Status', 'UnitPrice', 'Quantity',
                                       'Date_Start', 'Date_NextBilling', 'Description')}
        item = it.get('Item') or {}
        keep['Item'] = {'ID': item.get('ID'), 'Name': item.get('Name')}
        print('   ', json.dumps(keep, ensure_ascii=False))

print(f'\ntotal SUMIT calls: {calls}')
sys.exit(0)

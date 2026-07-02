"""E2E billing verification after Roei's live ₪1 test payment (STAGING).

Runs the standard read-only SUMIT sync, then reports every link of the chain:
payment → customer(ExternalIdentifier) → manager_billing(uid) → paywall state
→ issued receipt document. READ-ONLY vs SUMIT throughout; the only DB writes
are the sync's own last_paid_date/last_status columns. No card data anywhere.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT.startswith('/opt/makolet-chain') and 'staging' not in ROOT:
    sys.exit('REFUSED: this script is staging-only — never run on prod.')
sys.path.insert(0, ROOT)

from app import app, get_db, _run_billing_sync, _billing_state, _now_il  # noqa: E402
from utils import sumit  # noqa: E402

UID = 26
EMAIL = 'dennis-test@makoletchain.com'
PASSWORD = 'Dennis2026!'
month_start = _now_il().strftime('%Y-%m') + '-01'

print('── 0. sync (רענן) ──')
with app.test_request_context():
    print(_run_billing_sync(get_db()))

print('\n── 1. payments this month ──')
payments = sumit.list_payments(month_start)
for p in payments:
    print(f"  ID={p.get('ID')} CustomerID={p.get('CustomerID')} "
          f"Amount={p.get('Amount')} Date={p.get('Date')} "
          f"Valid={p.get('ValidPayment')} Status={p.get('StatusDescription') or p.get('Status')}")

print('\n── 2. customer records (THE tag check) ──')
customers = sumit.list_customers()
for c in customers:
    marker = '  ← tag matches uid 26 ✓' if str(c.get('external_identifier')) == str(UID) else ''
    print(f"  id={c['id']} name={c['name']!r} email={c['email']!r} "
          f"ExternalIdentifier={c['external_identifier']!r}{marker}")

print('\n── 3. manager_billing uid=26 ──')
with app.test_request_context():
    row = get_db().execute(
        'SELECT user_id, active, fee, last_paid_date, last_status, updated_at '
        'FROM manager_billing WHERE user_id=?', (UID,)).fetchone()
    print(' ', dict(row) if row else None)

    print('\n── 4. paywall state for uid=26 (app env, incl. BILLING_FAKE_TODAY) ──')
    print(' ', _billing_state(UID, 'manager', EMAIL))

print('\n── 4b. dennis through the real routes ──')
app.config['TESTING'] = True
client = app.test_client()
client.post('/login', data={'email': EMAIL, 'password': PASSWORD})
html = client.get('/').get_data(as_text=True)
print(f"  home banner gone: {'billing-warning-banner' not in html}")
acct = client.get('/account').get_data(as_text=True)
print(f"  /account shows 'המנוי פעיל ✓': {'המנוי פעיל' in acct}")

print('\n── 5. documents (receipts) this month ──')
docs = sumit.list_documents(month_start)
for d in docs:
    print(f"  DocumentID={d.get('DocumentID')} No={d.get('DocumentNumber')} "
          f"Type={d.get('Type') or d.get('DocumentType')!r} Date={d.get('Date')} "
          f"Value={d.get('DocumentValue')} Customer={d.get('CustomerName')!r} "
          f"ExtRef={d.get('ExternalReference')!r}")
if docs:
    newest = max(docs, key=lambda d: str(d.get('Date') or ''))
    doc = sumit.get_document(newest.get('DocumentID'))
    cust = doc.get('Customer') if isinstance(doc.get('Customer'), dict) else {}
    print('  newest doc detail:')
    print(f"    keys: {sorted(doc.keys())}")
    print(f"    embedded customer: Name={cust.get('Name')!r} "
          f"Email={cust.get('EmailAddress')!r} "
          f"ExternalIdentifier={cust.get('ExternalIdentifier')!r}")
else:
    print('  (no documents returned)')

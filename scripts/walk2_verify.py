"""Walk #2 post-payment verification (walk-test uid=31, STAGING, fake clock).

Runs רענן, then proves the join picked the RIGHT manager while FOUR tagged
customers exist in SUMIT: the new ₪1 payment → receipt doc →
ExternalIdentifier=='31', uid=31 flips to paid TODAY, uids 26/29/30 untouched,
and walk-test's rendered pages flip to the green paid state (banner gone,
'המנוי פעיל ✓' hero). READ-ONLY vs SUMIT."""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT.startswith('/opt/makolet-chain') and 'staging' not in ROOT:
    sys.exit('REFUSED: staging-only.')
sys.path.insert(0, ROOT)

from app import app, get_db, _run_billing_sync, _billing_state, _now_il  # noqa: E402
from utils import sumit  # noqa: E402

EMAIL, PASSWORD, UID = 'walk-test@makoletchain.com', 'Walk2026!', 31
TODAY = _now_il().strftime('%Y-%m-%d')
month_start = TODAY[:7] + '-01'
failures = []


def check(ok, label, extra=''):
    print(f"{'PASS' if ok else 'FAIL'} — {label}{(' — ' + extra) if extra else ''}")
    if not ok:
        failures.append(label)


print('── 0. sync (רענן) ──')
with app.test_request_context():
    print(' ', _run_billing_sync(get_db()))

print('\n── 1. payments this month ──')
for p in sorted(sumit.list_payments(month_start), key=lambda x: str(x.get('Date'))):
    print(f"  ID={p.get('ID')} CustomerID={p.get('CustomerID')} "
          f"Amount={p.get('Amount')} Date={p.get('Date')} Valid={p.get('ValidPayment')}")

print('\n── 2. ALL receipts → embedded customer tag (right-manager check) ──')
tag31 = []
for d in sorted(sumit.list_documents(month_start),
                key=lambda x: x.get('DocumentNumber') or 0):
    detail = sumit.get_document(d.get('DocumentID'))
    cust = detail.get('Customer') if isinstance(detail.get('Customer'), dict) else {}
    ext = cust.get('ExternalIdentifier')
    print(f"  doc №{d.get('DocumentNumber')} (₪{d.get('DocumentValue')}, "
          f"CustomerID={d.get('CustomerID')}) → Customer.ID={cust.get('ID')} "
          f"Name={cust.get('Name')!r} Email={cust.get('EmailAddress')!r} "
          f"ExternalIdentifier={ext!r}")
    if str(ext) == str(UID):
        tag31.append((d.get('DocumentNumber'), cust.get('Name'),
                      cust.get('EmailAddress'), cust.get('ID')))
check(len(tag31) == 1, f'exactly ONE receipt carries ExternalIdentifier={UID}',
      f'found={len(tag31)}')
if tag31:
    doc_no, name, email, cid = tag31[0]
    print(f"  → walk #2 receipt: doc №{doc_no}, payer {name!r}, email {email!r}, "
          f"SUMIT customer {cid}")

print('\n── 3. manager_billing rows ──')
with app.test_request_context():
    db = get_db()
    rows = {r['user_id']: dict(r) for r in db.execute(
        'SELECT user_id, active, last_paid_date, last_status FROM manager_billing '
        'WHERE user_id IN (26,29,30,31)').fetchall()}
    for uid in (26, 29, 30, 31):
        print(' ', rows.get(uid))
    check(rows[UID]['last_status'] == 'paid'
          and rows[UID]['last_paid_date'] == TODAY,
          f'uid=31 flipped to paid, last_paid_date={TODAY}')
    check(rows[26]['last_paid_date'] == '2026-07-02'
          and rows[29]['last_paid_date'] == '2026-07-02'
          and rows[30]['last_paid_date'] == '2026-07-03'
          and all(rows[u]['last_status'] == 'paid' for u in (26, 29, 30)),
          'uids 26/29/30 untouched (paid 07-02, 07-02, 07-03)')
    st = _billing_state(UID, 'manager', EMAIL, db)
    check(st.get('state') == 'ok', 'uid=31 paywall state == ok', str(st))

print('\n── 4. rendered pages (walk-test session) ──')
app.config['TESTING'] = True
client = app.test_client()
r = client.post('/login', data={'email': EMAIL, 'password': PASSWORD})
check(r.status_code == 302, 'walk-test login')
home = client.get('/').get_data(as_text=True)
check('billing-warning-banner' not in home, 'home banner GONE')
acct = client.get('/account').get_data(as_text=True)
check('המנוי פעיל' in acct and 'kpi-card--profit' in acct,
      '/account shows green "המנוי פעיל ✓" hero')
check(re.search(r'class="pay-btn"', acct) is None
      or 'customerexternalidentifier=31' in acct,
      '/account pay link (if shown) still tagged 31')

sys.exit(1 if failures else 0)

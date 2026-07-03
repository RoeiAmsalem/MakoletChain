"""Verify the FULL manager-experience walk (uid=33) — data side, NO sync.

The flip already happened during Roei's walk (layer A). This only READS:
SUMIT receipts (find the one tagged '33'), manager_billing (uid=33 paid, all
others untouched), billing_sync_runs (which source flipped him — expect
'payment'), and _billing_state(33)=='ok'. READ-ONLY vs SUMIT and vs our rows.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT.startswith('/opt/makolet-chain') and 'staging' not in ROOT:
    sys.exit('REFUSED: staging-only.')
sys.path.insert(0, ROOT)

from app import app, get_db, _billing_state, _now_il  # noqa: E402
from utils import sumit  # noqa: E402

UID, EMAIL = 33, 'fulltest@makoletchain.com'
TODAY = _now_il().strftime('%Y-%m-%d')
failures = []


def check(ok, label, extra=''):
    print(f"{'PASS' if ok else 'FAIL'} — {label}{(' — ' + extra) if extra else ''}")
    if not ok:
        failures.append(label)


print('── receipts this month → embedded tag ──')
tag_hits = []
for d in sorted(sumit.list_documents(TODAY[:7] + '-01'),
                key=lambda x: x.get('DocumentNumber') or 0):
    detail = sumit.get_document(d.get('DocumentID'))
    cust = detail.get('Customer') if isinstance(detail.get('Customer'), dict) else {}
    ext = cust.get('ExternalIdentifier')
    print(f"  doc №{d.get('DocumentNumber')} (₪{d.get('DocumentValue')}) → "
          f"ExternalIdentifier={ext!r} Email={cust.get('EmailAddress')!r}")
    if str(ext) == str(UID):
        tag_hits.append((d.get('DocumentNumber'), cust.get('Name'),
                         cust.get('EmailAddress')))
check(len(tag_hits) == 1, f'exactly ONE receipt tagged {UID}', str(tag_hits))
if tag_hits:
    print(f'  → receipt doc №{tag_hits[0][0]}, payer {tag_hits[0][1]!r}, '
          f'email {tag_hits[0][2]!r}')

print('\n── manager_billing ──')
with app.test_request_context():
    db = get_db()
    rows = {r['user_id']: dict(r) for r in db.execute(
        "SELECT user_id, active, last_status, last_paid_date FROM manager_billing "
        "WHERE user_id IN (26,29,30,31,32,33)").fetchall()}
    for u in sorted(rows):
        print(' ', rows[u])
    check(rows[UID]['last_status'] == 'paid'
          and rows[UID]['last_paid_date'] == TODAY,
          f'uid=33 paid, last_paid_date={TODAY}')
    check(rows[26]['last_paid_date'] == '2026-07-02'
          and rows[29]['last_paid_date'] == '2026-07-02'
          and rows[30]['last_paid_date'] == '2026-07-03'
          and rows[31]['last_paid_date'] == '2026-07-03'
          and rows[32]['last_status'] == 'unpaid',
          'all other rows untouched (26/29 → 07-02, 30/31 → 07-03, 32 unpaid)')

    print('\n── billing_sync_runs (today) ──')
    runs = db.execute(
        "SELECT id, started_at, source, ok, payments_seen, paid_managers "
        "FROM billing_sync_runs WHERE started_at LIKE ? ORDER BY id",
        (TODAY + '%',)).fetchall()
    for r in runs:
        print(f"  #{r['id']} {r['started_at']} source={r['source']} ok={r['ok']} "
              f"payments={r['payments_seen']} paid={r['paid_managers']}")
    # the flip run: first run today where paid_managers reached 5 (uid=33 joined)
    flips = [r for r in runs if (r['paid_managers'] or 0) >= 5]
    check(bool(flips) and flips[0]['source'] == 'payment',
          "flip came from source='payment' (instant layer)",
          f"first 5-paid run: #{flips[0]['id']} {flips[0]['started_at']} "
          f"source={flips[0]['source']}" if flips else 'none reached 5')

    st = _billing_state(UID, 'manager', EMAIL, db)
    check(st.get('state') == 'ok', '_billing_state(33) == ok', str(st))

sys.exit(1 if failures else 0)

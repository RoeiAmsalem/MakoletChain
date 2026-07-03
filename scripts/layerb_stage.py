"""Stage the Layer-B (cron-alone) final test on STAGING.

Subject is uid=32 (alert-demo) — NOT uid=31: walk-test's real July payments
live in SUMIT, so any sync instantly re-marks him paid and 'warning' is
unreachable for him. alert-demo has no SUMIT payment, an active billing row,
and alert_state='warning' — exactly a real manager mid-grace whose card SUMIT
is about to charge.

Does: re-activate uid=32 (unpaid, alert_state='warning', activated 07-03),
run ONE stamping sync (staleness guard), verify state=warning, print the
tagged ₪1 link. The flip itself must come from the CRON sweep only.

STAGING ONLY. Read-only vs SUMIT.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT.startswith('/opt/makolet-chain') and 'staging' not in ROOT:
    sys.exit('REFUSED: this script is staging-only — never run on prod.')
sys.path.insert(0, ROOT)

from app import app, get_db, _run_billing_sync_logged, _billing_state, \
    _billing_today, _manager_payment_link, _now_il  # noqa: E402

EMAIL, UID = 'alert-demo@makoletchain.com', 32
failures = []


def check(ok, label, extra=''):
    print(f"{'PASS' if ok else 'FAIL'} — {label}{(' — ' + extra) if extra else ''}")
    if not ok:
        failures.append(label)


check(_billing_today().isoformat() == '2026-07-06',
      'fake clock live (2026-07-06)', f'today={_billing_today()}')

with app.test_request_context():
    db = get_db()
    db.execute(
        "UPDATE manager_billing SET active=1, last_paid_date=NULL, "
        "last_status='unpaid', alert_state='warning', alert_date='2026-07-06', "
        "activated_at='2026-07-03', updated_at=? WHERE user_id=?",
        (_now_il().strftime('%Y-%m-%d %H:%M'), UID))
    db.commit()

    res = _run_billing_sync_logged(db, 'manual')   # stamping sync (Task 1 only)
    check(res.get('connected') and not res.get('error'), 'stamping sync ran',
          str(res))

    row = db.execute("SELECT * FROM manager_billing WHERE user_id=?",
                     (UID,)).fetchone()
    check(row['active'] == 1 and row['last_status'] == 'unpaid'
          and not row['last_paid_date'] and row['alert_state'] == 'warning',
          'uid=32: active, unpaid, alert_state=warning (paid-✓ alert armed)',
          f"status={row['last_status']!r} paid={row['last_paid_date']!r} "
          f"alert={row['alert_state']!r}")

    st = _billing_state(UID, 'manager', EMAIL, db)
    check(st.get('state') == 'warning', 'state == warning', str(st))

print(f"TAGGED LINK: {_manager_payment_link(UID)}")
sys.exit(1 if failures else 0)

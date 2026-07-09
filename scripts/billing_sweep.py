"""Billing motor layer B: the ONCE-DAILY sweep (cron-driven, like the Z agent).

Runs the READ-ONLY SUMIT sync + the layer-C transition alerts + the manager
lock-notification email. Since the SUMIT webhook (layer A½) became the primary
event-driven sync, this is a daily SAFETY NET only — SUMIT meters API calls,
so the sweep's jobs are: catch missed webhooks, pick up SUMIT's automatic
monthly recurring charges, run the warning/lock/paid transition alerts once a
day, and email each newly-LOCKED manager once (run_lock_pass in
billing_reminder.py — same Gmail sender and dry-run convention as the 08:30
reminder, zero extra SUMIT calls).

Gates, in order:
  - BILLING_SYNC_ENABLED (own flag, default TRUE; deliberately NOT gated by
    ENABLE_AGENTS — billing must keep syncing even where agents are off, e.g.
    staging).
  - IL hour == BILLING_SWEEP_HOUR (default 09). Cron on the UTC box fires at
    both 06:10 and 07:10 UTC; this gate lets exactly one run through at
    09:10 IL year-round across DST shifts.

On SUMIT failure: retry once after RETRY_DELAY_SECONDS, then one 🟠 brrr.
Fail-open is sacred: a failed sweep changes nothing — _billing_state's
staleness guard already exempts anyone whose row wasn't synced this month.
"""
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
# sibling scripts/ imports (billing_reminder) regardless of how we were invoked
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

RETRY_DELAY_SECONDS = 300
# IL hour the daily sweep runs at. Overridable for on-demand staging runs
# (BILLING_SWEEP_HOUR=<current IL hour> to force a run outside 09:xx).
SWEEP_HOUR = 9


def _sync_ok(res):
    return bool(res.get('connected') and not res.get('error'))


def run_sweep(retry_delay=RETRY_DELAY_SECONDS):
    """Returns 'disabled' | 'outside-window' | 'ok' | 'failed'."""
    if os.environ.get('BILLING_SYNC_ENABLED', 'true').strip().lower() in (
            'false', '0', 'no'):
        print('[billing-sweep] BILLING_SYNC_ENABLED=false — skipped')
        return 'disabled'

    from app import app, get_db, _run_billing_sync_logged, _billing_alert_pass, \
        _now_il
    from utils.notify import notify

    sweep_hour = int(os.environ.get('BILLING_SWEEP_HOUR', str(SWEEP_HOUR))
                     or SWEEP_HOUR)
    hour = _now_il().hour
    if hour != sweep_hour:
        print(f'[billing-sweep] not the daily {sweep_hour:02d}:xx IL slot '
              f'(hour={hour}) — skipped')
        return 'outside-window'

    with app.app_context():
        db = get_db()
        res = _run_billing_sync_logged(db, 'auto')
        if not _sync_ok(res):
            print(f'[billing-sweep] sync failed ({res}) — retrying in '
                  f'{retry_delay}s')
            time.sleep(retry_delay)
            res = _run_billing_sync_logged(db, 'auto')
        if not _sync_ok(res):
            err = res.get('error') or res.get('message') or 'unknown'
            notify('Billing sweep failed',
                   f'SUMIT sync failed twice (retry {retry_delay}s apart): '
                   f'{str(err)[:280]}', medium=True)
            print(f'[billing-sweep] FAILED after retry: {res}')
            return 'failed'
        # Alerts only run on FRESH data — a failed sync must never produce a
        # warning/lock alert off stale rows.
        sent = _billing_alert_pass(db)
        # Lock-notification emails ride the same fresh-state run. run_lock_pass
        # handles SMTP failures itself (flag unset + one 🟠); this catch is
        # only for a pass-level crash, which must never take down the sweep.
        try:
            import billing_reminder
            lres = billing_reminder.run_lock_pass(db)
            lock_note = (f"lock_emails={len(lres['sent'])} "
                         f"dry={len(lres['would_send'])} "
                         f"failed={len(lres['failed'])}")
        except Exception as e:
            print(f'[billing-sweep] lock-email pass crashed '
                  f'(sweep unaffected): {e}')
            notify('Billing lock-email pass crashed', str(e)[:280],
                   medium=True)
            lock_note = 'lock_emails=crashed'
        print(f"[billing-sweep] ok — payments_seen={res.get('payments_seen')} "
              f"paid_managers={res.get('paid_managers')} "
              f"alerts={len(sent)} {sent} {lock_note}")
        return 'ok'


if __name__ == '__main__':
    rc = run_sweep()
    sys.exit(0 if rc in ('ok', 'disabled', 'outside-window') else 1)

"""Billing motor layer B: the scheduled sweep (cron-driven, like the Z agent).

Runs the READ-ONLY SUMIT sync + the layer-C transition alerts. This is the
layer that catches payments nobody clicks through — SUMIT's automatic monthly
recharges, pay-and-close-tab — and it feeds the warning/lock/paid alerts.

Gates, in order:
  - BILLING_SYNC_ENABLED (own flag, default TRUE; deliberately NOT gated by
    ENABLE_AGENTS — billing must keep syncing even where agents are off, e.g.
    staging).
  - 07:00–23:00 Israel window (cron on the UTC box fires every 2h; this gate
    keeps the IL window exact across DST shifts).

On SUMIT failure: retry once after RETRY_DELAY_SECONDS, then one 🟠 brrr.
Fail-open is sacred: a failed sweep changes nothing — _billing_state's
staleness guard already exempts anyone whose row wasn't synced this month.
"""
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

RETRY_DELAY_SECONDS = 300


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

    hour = _now_il().hour
    if not 7 <= hour <= 23:
        print(f'[billing-sweep] outside 07-23 IL window (hour={hour}) — skipped')
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
        print(f"[billing-sweep] ok — payments_seen={res.get('payments_seen')} "
              f"paid_managers={res.get('paid_managers')} "
              f"alerts={len(sent)} {sent}")
        return 'ok'


if __name__ == '__main__':
    rc = run_sweep()
    sys.exit(0 if rc in ('ok', 'disabled', 'outside-window') else 1)

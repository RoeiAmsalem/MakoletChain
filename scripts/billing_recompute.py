"""One-shot repair + audit for the 2026-08-01 billing rollover bug.

The old state machine keyed everything on the CALENDAR MONTH, so the Aug-1
rollover (a) flipped paid-in-July managers to warning and (b) reset the
days_unpaid anchor, unlocking never-paid managers who were locked in July.
The fix rebuilds the machine on paid_until (= last_paid_date + 1 month).

This script recomputes every active-billed manager under BOTH machines and
prints the full table: last_paid_date → paid_until → old state vs new state.
Dry-run by default. With --apply it also repairs the bug's side effects:
  - alert_state := the corrected alert state, set SILENTLY (this morning's
    bug-induced ok→warning / locked→warning transitions must not fire
    another round of brrr alerts tomorrow).
  - reminder_sent_month / locked_email_sent_month: rows still holding the
    old 'YYYY-MM' month key while the manager is warning/locked get the new
    cycle key (cycle_start) — they were already emailed THIS cycle, and a
    format mismatch alone must not trigger a duplicate email.
Zero SUMIT calls; reads/writes the local DB only.

Usage: venv/bin/python scripts/billing_recompute.py [--apply]
       (BILLING_FAKE_TODAY=YYYY-MM-DD for staging simulations)
"""
import os
import re
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

MONTH_KEY_RE = re.compile(r'^\d{4}-\d{2}$')     # old-format dedup flag


def _old_state(mb, today, start, grace):
    """The PRE-FIX month-keyed machine, frozen here for the diff column."""
    if not mb['active']:
        return 'exempt'
    month = today.strftime('%Y-%m')
    if (mb['last_paid_date'] or '')[:7] == month:
        return 'ok'
    if (mb['updated_at'] or '')[:7] != month:
        return 'exempt'
    anchor = max(start, today.replace(day=1))
    if mb['activated_at']:
        anchor = max(anchor, date.fromisoformat(mb['activated_at'][:10]))
    if today < anchor:
        return 'exempt'
    days = (today - anchor).days + 1
    return 'warning' if days <= grace else 'locked'


def run(apply=False):
    from app import (app, get_db, _billing_state, _billing_today,
                     _billing_alert_state_of, _paid_until,
                     BILLING_GRACE_DAYS, BILLING_START_DATE)

    today = _billing_today()
    start = date.fromisoformat(BILLING_START_DATE)
    with app.app_context():
        db = get_db()
        rows = db.execute(
            "SELECT mb.*, u.name, u.email, u.role FROM manager_billing mb "
            "JOIN users u ON u.id = mb.user_id "
            "WHERE mb.active = 1 AND u.active = 1 ORDER BY mb.user_id"
        ).fetchall()

        print(f"[recompute] today={today}  start={BILLING_START_DATE}  "
              f"grace={BILLING_GRACE_DAYS}  mode={'APPLY' if apply else 'dry-run'}")
        hdr = (f"{'uid':>4} {'name':<22} {'last_paid':<11} {'paid_until':<11} "
               f"{'old':<8} {'new':<8} {'days':>4} {'cycle_start':<11} chg")
        print(hdr)
        print('-' * len(hdr))

        changed = []
        for mb in rows:
            st = _billing_state(mb['user_id'], mb['role'], mb['email'], db)
            old = _old_state(mb, today, start, BILLING_GRACE_DAYS)
            new = st['state']
            pu = _paid_until(mb['last_paid_date'])
            chg = '  *' if old != new else ''
            if old != new:
                changed.append((mb['user_id'], mb['name'], old, new))
            print(f"{mb['user_id']:>4} {(mb['name'] or mb['email'])[:22]:<22} "
                  f"{(mb['last_paid_date'] or '—'):<11} "
                  f"{(pu.isoformat() if pu else '—'):<11} "
                  f"{old:<8} {new:<8} "
                  f"{st.get('days_unpaid', ''):>4} "
                  f"{st.get('cycle_start', '—'):<11}{chg}")

            if not apply:
                continue
            # 1. alert_state → corrected state, silently (no brrr)
            alert = _billing_alert_state_of(st)
            if alert is not None and alert != mb['alert_state']:
                db.execute(
                    "UPDATE manager_billing SET alert_state=?, alert_date=? "
                    "WHERE user_id=?",
                    (alert, today.isoformat(), mb['user_id']))
                print(f"     └ alert_state: {mb['alert_state']!r} → {alert!r}")
            # 2. old month-format email flags → this cycle's key (already
            #    emailed this cycle; don't let the format change re-send)
            cycle = st.get('cycle_start')
            if cycle and new in ('warning', 'locked'):
                for col in ('reminder_sent_month', 'locked_email_sent_month'):
                    val = mb[col]
                    if val and MONTH_KEY_RE.match(val):
                        db.execute(
                            f"UPDATE manager_billing SET {col}=? WHERE user_id=?",
                            (cycle, mb['user_id']))
                        print(f"     └ {col}: {val!r} → {cycle!r}")
        if apply:
            db.commit()

        print('-' * len(hdr))
        if changed:
            print(f"[recompute] {len(changed)} state change(s) vs the old machine:")
            for uid, name, old, new in changed:
                print(f"  uid {uid} {name}: {old} → {new}")
        else:
            print('[recompute] no state changes vs the old machine')
        return changed


if __name__ == '__main__':
    run(apply='--apply' in sys.argv)

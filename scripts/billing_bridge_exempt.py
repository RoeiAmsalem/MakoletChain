#!/usr/bin/env python3
"""One-off BRIDGE unlock for uid 21/22/4 (2026-08-09 lockout incident).

WHY: uid 21, 22 and 4 computed to 'locked' (days_unpaid=9 > BILLING_GRACE_DAYS=5)
and have been shut out of a paid product since 2026-08-06. There is NO expiring
grace/override column in manager_billing, and the only manual control that exists
(/api/admin/billing/assign-payment) works by writing last_paid_date — i.e. by
asserting money arrived. For uid 21 and 4 no August money arrived, so using it
would fabricate payment truth.

WHAT: flips manager_billing.active 1 -> 0 for the three uids. _billing_state
returns {'state': 'exempt'} on `not mb['active']` (app.py:5227), so the paywall
stops blocking them immediately. last_paid_date / last_status are NOT touched:
billing truth stays exactly as the sweep left it.

THIS IS A BRIDGE, NOT THE FIX. It does not expire. Migration 045 should add a
real grace_until column; once it ships these rows go back to active=1, and uid 22
gets a truthful paid date once his dropped August payment is identified.

Read-modify-write is limited to (active, updated_at) on three user_ids.
Usage:  python3 scripts/billing_bridge_exempt.py [--apply]
Without --apply it is a dry run: prints BEFORE + gate verdict and changes nothing.
"""
import argparse
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TARGETS = (21, 22, 4)
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       'db', 'makolet_chain.db')


def _row(db, uid):
    return db.execute(
        "SELECT mb.user_id, u.name, u.email, u.role, mb.active, "
        "mb.last_paid_date, mb.last_status, mb.updated_at, mb.alert_state, "
        "mb.alert_date FROM manager_billing mb JOIN users u ON u.id = mb.user_id "
        "WHERE mb.user_id=?", (uid,)).fetchone()


def _show(label, r, gate):
    print(f"  {label:<7} active={r['active']}  last_paid_date={r['last_paid_date']!r}  "
          f"last_status={r['last_status']!r}  updated_at={r['updated_at']!r}")
    print(f"  {'':<7} paid_until=<COLUMN DOES NOT EXIST>  "
          f"override_col=<COLUMN DOES NOT EXIST>")
    print(f"  {'':<7} GATE _billing_state() -> {gate}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true',
                    help='actually write; omit for dry run')
    args = ap.parse_args()

    # Import the REAL gate function the app uses — never re-implement it.
    from app import _billing_state, BILLING_START_DATE, BILLING_GRACE_DAYS, _billing_today

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    print(f"today={_billing_today().isoformat()}  "
          f"BILLING_START_DATE={BILLING_START_DATE}  "
          f"BILLING_GRACE_DAYS={BILLING_GRACE_DAYS}")
    print(f"mode={'APPLY' if args.apply else 'DRY-RUN'}\n")

    verdicts = {}
    for uid in TARGETS:
        r = _row(db, uid)
        if r is None:
            print(f"uid {uid}: NO manager_billing ROW — skipped\n")
            continue
        before_gate = _billing_state(r['user_id'], r['role'], r['email'], db)
        print(f"uid {uid} — {r['name']} <{r['email']}> role={r['role']}")
        _show('BEFORE', r, before_gate)

        if args.apply:
            now_iso = __import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M')
            sql = ("UPDATE manager_billing SET active=0, updated_at=? "
                   "WHERE user_id=?")
            print(f"  EXEC    {sql}  -- params=({now_iso!r}, {uid})")
            db.execute(sql, (now_iso, uid))
            db.commit()
            r2 = _row(db, uid)
            after_gate = _billing_state(r2['user_id'], r2['role'], r2['email'], db)
            _show('AFTER', r2, after_gate)
            verdicts[uid] = after_gate.get('state')
        else:
            verdicts[uid] = before_gate.get('state')
        print()

    unlocked = [u for u, s in verdicts.items() if s == 'exempt']
    print('=' * 60)
    print('gate verdict per uid: ' +
          '  '.join(f'{u}={s}' for u, s in verdicts.items()))
    ok = all(verdicts.get(u) == 'exempt' for u in TARGETS)
    print(f"21/22/4 can now load the dashboard: {'YES' if ok else 'NO'}")
    print(f"(exempt = paywall lets the request through; {len(unlocked)}/3 exempt)")


if __name__ == '__main__':
    main()

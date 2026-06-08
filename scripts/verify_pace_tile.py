# -*- coding: utf-8 -*-
"""READ-ONLY verification of the קצב הכנסות (pace) tile — replicates /api/sales's
exact math (total/days/avg/days_in_month/pace) against the prod DB (mode=ro).

Checks: 9018 June pace, 9015 pace == avg×30, a completed past month → pace ≈ total,
and a below-floor month → pace None. No writes. Imports app._month_below_floor /
_now_il only (both read-only, take an explicit connection where needed).
"""
import calendar
import os
import sqlite3
import sys

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import app as app_module                       # noqa: E402
from app import _month_below_floor             # noqa: E402

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')


def conn_ro():
    c = sqlite3.connect('file:' + os.path.abspath(DB_PATH) + '?mode=ro', uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def compute(c, branch_id, month):
    """Exact replica of api_sales: returns (total, days, avg, days_in_month, pace)."""
    if _month_below_floor(branch_id, month, c):
        return (0, 0, 0, None, None)
    rows = c.execute(
        "SELECT amount FROM daily_sales WHERE branch_id=? AND strftime('%Y-%m', date)=?",
        (branch_id, month)).fetchall()
    total = sum(r['amount'] for r in rows)
    days = len(rows)
    avg = round(total / days, 2) if days else 0
    try:
        y, mo = month.split('-')
        days_in_month = calendar.monthrange(int(y), int(mo))[1]
    except (ValueError, AttributeError):
        days_in_month = None
    pace = round(avg * days_in_month) if (days and days_in_month) else None
    return (total, days, avg, days_in_month, pace)


def show(c, branch_id, month, note=''):
    total, days, avg, dim, pace = compute(c, branch_id, month)
    paces = '—' if pace is None else f'₪{pace:,.0f}'
    print(f"  {branch_id} {month}: days={days} avg=₪{avg:,.2f} dim={dim} "
          f"pace={paces}  {note}")
    return (total, days, avg, dim, pace)


def main():
    c = conn_ro()
    print("verify קצב הכנסות (pace) — replica of /api/sales\n")

    print("1. PROD 9018 June (expect avg ₪10,127.60 × 30 ≈ ₪303,800, subtitle 7 days):")
    t, d, a, dim, p = show(c, 9018, '2026-06')
    print(f"     → tile value ₪{p:,.0f}, subtitle 'לפי ממוצע {d} ימים'  "
          f"{'✓' if dim == 30 and p == round(a*30) else '✗'}")

    print("\n2. Sanity 9015 June — pace == ממוצע ליום × 30:")
    t, d, a, dim, p = show(c, 9015, '2026-06')
    print(f"     → pace {p} == round(avg×30) {round(a*30)}  {'✓' if p == round(a*30) else '✗'}")

    print("\n3. Completed past month (days == days_in_month) → pace ≈ actual total:")
    # auto-pick: a non-floored branch+month whose day-count fills the calendar month.
    picked = None
    for bid in (126, 127, 9018, 9015, 9001):
        for m in ('2026-05', '2026-04', '2026-03'):
            t, d, a, dim, p = compute(c, bid, m)
            if d and dim and d == dim:
                picked = (bid, m, t, d, a, dim, p)
                break
        if picked:
            break
    if picked:
        bid, m, t, d, a, dim, p = picked
        show(c, bid, m, '(full month)')
        print(f"     → pace ₪{p:,.0f} ≈ total ₪{t:,.0f}  "
              f"Δ=₪{abs(p-t):,.2f}  {'✓' if abs(p - t) < d else '✗'}")
    else:
        print("     (no fully-populated past month found in sampled branches)")

    print("\n4. Below-floor month → pace None → '—':")
    bid, m = 9018, '2026-05'                 # chain store, visible_from 2026-06-01
    floored = _month_below_floor(bid, m, c)
    t, d, a, dim, p = compute(c, bid, m)
    print(f"  {bid} {m}: below_floor={floored} pace={p}  "
          f"{'✓ —' if floored and p is None else '✗'}")
    c.close()


if __name__ == '__main__':
    main()

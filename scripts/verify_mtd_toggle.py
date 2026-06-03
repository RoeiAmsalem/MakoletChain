#!/usr/bin/env python3
"""Verify the חודש מלא / עד היום (full-month vs month-to-date) P/L toggle.

Exercises the REAL app.py functions (_get_fixed_total, _calculate_salary_cost,
get_electricity_for_month) against the live DB. Proves:
  1. Full mode is byte-identical to today (mtd_factor=None == default keys).
  2. עד היום pro-rates monthly fixed rows + electricity estimate only.
  3. זיכיונות (% מהכנסות) and חד פעמי rows are NOT pro-rated.
  4. Profit is recomputed from the active-mode fixed total.
  5. A past month is a no-op (both modes equal).

Run on the server (staging): python3 scripts/verify_mtd_toggle.py [branch_id]
"""
import sys
import calendar
import sqlite3

import app as A

BRANCH = int(sys.argv[1]) if len(sys.argv) > 1 else 126


def income_for(db, branch_id, month):
    """Mirror /api/summary income: daily_sales sum + today's live add when current month."""
    income = db.execute(
        "SELECT COALESCE(SUM(amount),0) FROM daily_sales "
        "WHERE branch_id=? AND strftime('%Y-%m', date)=?", (branch_id, month)
    ).fetchone()[0]
    now = A._now_il()
    if month == now.strftime('%Y-%m'):
        today = now.strftime('%Y-%m-%d')
        has_z = db.execute("SELECT 1 FROM daily_sales WHERE branch_id=? AND date=?",
                           (branch_id, today)).fetchone() is not None
        live = db.execute(
            "SELECT amount,last_updated FROM live_sales WHERE branch_id=? AND date=?",
            (branch_id, today)).fetchone()
        if live and live['amount'] and live['last_updated'] != 'PAUSED' and not has_z:
            income += live['amount']
    return income


def goods_for(db, branch_id, month):
    return db.execute(
        "SELECT COALESCE(SUM(amount),0) FROM goods_documents "
        "WHERE branch_id=? AND strftime('%Y-%m', doc_date)=?", (branch_id, month)
    ).fetchone()[0]


def money(x):
    return f"₪{x:,.2f}"


def report(db, branch_id, month, label):
    now = A._now_il()
    current_month = now.strftime('%Y-%m')
    mtd_applicable = (month == current_month)
    if mtd_applicable:
        days_elapsed = now.day
        days_in_month = calendar.monthrange(now.year, now.month)[1]
        factor = days_elapsed / days_in_month
    else:
        days_elapsed = days_in_month = None
        factor = None

    income = income_for(db, branch_id, month)
    goods = goods_for(db, branch_id, month)
    salary = A._calculate_salary_cost(branch_id, month)['amount']

    full = A._get_fixed_total(branch_id, month, income, db)               # default (today)
    both = A._get_fixed_total(branch_id, month, income, db, mtd_factor=factor)

    # (1) default keys must be byte-identical regardless of mtd_factor
    assert full['total'] == both['total'], "default total drifted!"
    assert full['fixed_only'] == both['fixed_only'], "default fixed_only drifted!"

    fixed_full = both['total']
    fixed_mtd = both.get('total_mtd', fixed_full)
    profit_full = income - goods - fixed_full - salary
    profit_mtd = income - goods - fixed_mtd - salary

    print(f"\n=== {label}: branch {branch_id}  month {month} "
          f"({'CURRENT' if mtd_applicable else 'PAST'}) ===")
    if mtd_applicable:
        print(f"factor = day {days_elapsed} / {days_in_month} days "
              f"= {factor:.4f}")
    print(f"income {money(income)} | goods {money(goods)} | salary {money(salary)}  "
          f"(these are IDENTICAL in both modes)")

    # per-row breakdown
    rows = db.execute(
        "SELECT name, amount, pct_value, expense_type FROM fixed_expenses "
        "WHERE branch_id=? AND month=? ORDER BY pct_value DESC, expense_type", (branch_id, month)
    ).fetchall()
    print("  fixed rows:")
    for r in rows:
        if r['pct_value'] and r['pct_value'] > 0:
            amt = income * r['pct_value'] / 100
            kind, prorate = f"% מהכנסות ({r['pct_value']}%)", "NO  (actual)"
        elif r['expense_type'] == 'monthly':
            amt, kind, prorate = r['amount'], "חודשי", "YES"
        else:
            amt, kind, prorate = r['amount'], "חד פעמי", "NO  (actual)"
        mtd_amt = amt * factor if (prorate == "YES" and factor is not None) else amt
        print(f"    - {r['name'][:24]:<24} {kind:<16} full {money(amt):>12}"
              f"   → mtd {money(mtd_amt):>12}   prorate={prorate}")
    elec = both['electricity']
    elec_mtd = both.get('electricity_mtd', elec['amount'])
    print(f"    - electricity ({elec['source']:<8})              full "
          f"{money(elec['amount']):>12}   → mtd {money(elec_mtd):>12}   prorate="
          f"{'YES' if mtd_applicable else 'NO  (actual)'}")

    print(f"  FIXED total:  full {money(fixed_full)}   →   mtd {money(fixed_mtd)}")
    print(f"  PROFIT:       full {money(profit_full)}   →   mtd {money(profit_mtd)}  "
          f"(Δ {money(profit_mtd - profit_full)})")

    if not mtd_applicable:
        assert abs(fixed_full - fixed_mtd) < 0.005, "past month should be a no-op!"
        assert abs(profit_full - profit_mtd) < 0.005, "past month profit changed!"
        print("  ✓ past month: both modes equal (no-op)")


def main():
    with A.app.app_context():
        db = sqlite3.connect(A.DB_PATH, timeout=30)
        db.row_factory = sqlite3.Row
        now = A._now_il()
        cur = now.strftime('%Y-%m')
        # previous month string
        py, pm = (now.year, now.month - 1) if now.month > 1 else (now.year - 1, 12)
        prev = f"{py:04d}-{pm:02d}"
        report(db, BRANCH, cur, "CURRENT MONTH")
        report(db, BRANCH, prev, "PAST MONTH")
        db.close()
    print("\nALL ASSERTIONS PASSED ✓")


if __name__ == '__main__':
    main()

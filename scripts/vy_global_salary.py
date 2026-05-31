"""Staging VY: global (flat monthly) salary type — _calculate_salary_cost.

Runs inside an app context against the staging DB. Inserts a sandbox branch +
employees, exercises _calculate_salary_cost, then ROLLS BACK so nothing
persists. Verifies:
  - global employee adds exactly its flat amount (no proration)
  - hourly + global sum correctly
  - Aviv hours for a global employee do NOT change cost (no double-count)
  - editing the amount updates cost; switching hourly->global behaves
  - existing hourly-only branches unaffected
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, get_db, _calculate_salary_cost

BID = 999990  # sandbox branch id, well outside the real range
MONTH = '2026-05'

results = []


def check(label, got, want):
    ok = abs(got - want) < 0.001
    results.append((ok, label, got, want))
    print(f"{'PASS' if ok else 'FAIL'} — {label}: got {got}, want {want}")


with app.app_context():
    db = get_db()
    try:
        db.execute("INSERT INTO branches (id, name, city, active) VALUES (?, 'VY-SANDBOX', 'x', 1)", (BID,))

        # 1) Pure global employee -> flat amount.
        db.execute("INSERT INTO employees (branch_id, name, role, hourly_rate, salary_type, global_salary, active) "
                   "VALUES (?, 'גלובלי משה', 'מנהל', 0, 'global', 10000, 1)", (BID,))
        check('global-only = flat 10000', _calculate_salary_cost(BID, MONTH)['amount'], 10000)

        # 2) Add hourly employee with hours -> sum.
        db.execute("INSERT INTO employees (branch_id, name, role, hourly_rate, salary_type, active) "
                   "VALUES (?, 'שעתי דנה', 'ערב', 50, 'hourly', 1)", (BID,))
        db.execute("INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, total_salary, source) "
                   "VALUES (?, ?, 'שעתי דנה', 100, 5000, 'aviv_report')", (BID, MONTH))
        check('global 10000 + hourly 100x50 = 15000', _calculate_salary_cost(BID, MONTH)['amount'], 15000)

        # 3) Aviv hours for the GLOBAL employee must not change cost.
        db.execute("INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, total_salary, source) "
                   "VALUES (?, ?, 'גלובלי משה', 200, 12000, 'aviv_report')", (BID, MONTH))
        check('global hours ignored (still 15000)', _calculate_salary_cost(BID, MONTH)['amount'], 15000)

        # 4) Edit the global amount.
        db.execute("UPDATE employees SET global_salary=12500 WHERE branch_id=? AND name='גלובלי משה'", (BID,))
        check('edit global 12500 -> 17500', _calculate_salary_cost(BID, MONTH)['amount'], 17500)

        # 5) Switch the hourly employee to global.
        db.execute("UPDATE employees SET salary_type='global', global_salary=8000, hourly_rate=0 "
                   "WHERE branch_id=? AND name='שעתי דנה'", (BID,))
        check('switch hourly->global: 12500+8000 = 20500', _calculate_salary_cost(BID, MONTH)['amount'], 20500)

    finally:
        db.rollback()  # discard everything — staging DB untouched

# Hourly-only regression: pick a real branch with hours and confirm it is a
# plain hours x rate number (no global rows -> source != 'global').
with app.app_context():
    db = get_db()
    row = db.execute(
        "SELECT branch_id, month FROM employee_hours "
        "WHERE source IN ('aviv_api','aviv_report') GROUP BY branch_id, month "
        "HAVING SUM(total_hours) > 0 LIMIT 1").fetchone()
    if row:
        s = _calculate_salary_cost(row['branch_id'], row['month'])
        ok = s['source'] != 'global' and s['amount'] >= 0
        results.append((ok, f"real hourly branch {row['branch_id']} {row['month']} unaffected", s['amount'], s['amount']))
        print(f"{'PASS' if ok else 'FAIL'} — real hourly branch {row['branch_id']} {row['month']}: "
              f"amount={s['amount']} source={s['source']}")

failed = [r for r in results if not r[0]]
print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
sys.exit(1 if failed else 0)

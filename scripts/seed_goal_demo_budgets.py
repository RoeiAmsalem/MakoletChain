"""Seed demo supplier budgets on a staging branch so the תקציב view shows all
three יתרה color states for the verification screenshot:

  • under budget  → יתרה > 0  → GREEN
  • over  budget  → יתרה < 0  → RED
  • exactly on budget → יתרה == 0 → NEUTRAL (no color)

It reads the branch's actual current-month spend (mtd_spend, the הוצאה value)
and sets each demo supplier's budget RELATIVE to its real spend so the three
states are guaranteed regardless of the live numbers:
  supplier[0].budget = spend + 300   (under  → +300 green)
  supplier[1].budget = spend          (exact  → 0 neutral)
  supplier[2].budget = spend − 200    (over   → −200 red)

Clears the branch's other budgets first so the screenshot is clean. STAGING
data only — supplier_budgets is the same table the UI's ₪ input writes.

Usage:  python3 scripts/seed_goal_demo_budgets.py [branch_id]
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module
from app import _goal_data

BRANCH = int(sys.argv[1]) if len(sys.argv) > 1 else 9015


def main():
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    data = _goal_data(BRANCH, conn)
    spenders = sorted((s for s in data['suppliers'] if s['mtd_spend'] >= 250),
                      key=lambda s: -s['mtd_spend'])
    if len(spenders) < 3:
        print(f"branch {BRANCH}: only {len(spenders)} suppliers with spend ≥ 250 "
              f"this month — need 3 to demo all states. Pick another branch.")
        return

    under, exact, over = spenders[0], spenders[1], spenders[2]
    plan = [
        (under['supplier_name'], round(under['mtd_spend'] + 300, 2), 'under → +300 GREEN'),
        (exact['supplier_name'], round(exact['mtd_spend'], 2),       'exact → 0 NEUTRAL'),
        (over['supplier_name'],  round(over['mtd_spend'] - 200, 2),  'over → −200 RED'),
    ]

    # Clean slate for a clear screenshot.
    conn.execute("DELETE FROM supplier_budgets WHERE branch_id = ?", (BRANCH,))
    for name, budget, _ in plan:
        conn.execute(
            "INSERT INTO supplier_budgets (branch_id, supplier_name, monthly_budget, updated_at) "
            "VALUES (?, ?, ?, datetime('now'))", (BRANCH, name, budget))
    conn.commit()

    # Read back through _goal_data to show exactly what the cards will render.
    data = _goal_data(BRANCH, conn)
    by_name = {s['supplier_name']: s for s in data['suppliers']}
    print(f"=== seeded demo budgets on branch {BRANCH} (month {data['month']}) ===")
    print(f"{'supplier':<22} {'תקציב':>10} {'הוצאה':>10} {'יתרה':>10}  expect")
    for name, budget, label in plan:
        s = by_name[name]
        print(f"{name:<22} {s['budget']:>10.2f} {s['mtd_spend']:>10.2f} "
              f"{s['remaining']:>10.2f}  {label}")
    t = data['totals']
    print(f"\ntotals (budgeted-only): תקציב={t['budget']:.2f}  הוצאה={t['spent']:.2f}  "
          f"יתרה={t['remaining']:.2f}")
    conn.close()


if __name__ == '__main__':
    main()

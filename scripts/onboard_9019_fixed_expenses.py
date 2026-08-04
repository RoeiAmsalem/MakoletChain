"""One-shot onboarding insert: branch 9019 (כפר סירקין) fixed expenses.

Source: onboarding/sirkin_expenses.pdf (מאזן בוחן Jan–Jul 2026), averages over
complete months Jan–Jun, VAT applied per line. Approved by Roei 2026-08-04.
Excluded by decision: שכר דירה (unclear — 2 months only), הוצאות רכב (personal).

Idempotent: INSERT OR IGNORE against UNIQUE(branch_id, name, month, expense_type).
Default is dry-run; pass --apply to write. After apply, prints the stored rows
and a P&L-style render (income + fixed incl. live 5% זיכיונות) per month.

Usage:
  python scripts/onboard_9019_fixed_expenses.py --db /opt/makolet-chain/db/makolet_chain.db [--apply]
"""

import argparse
import sqlite3

BRANCH_ID = 9019
MONTHS = ['2026-05', '2026-06', '2026-07', '2026-08']

# name -> stored amount (VAT-inclusive where applicable)
ROWS = [
    ('הנהלת חשבונות', 2360.0),        # 2,000 ex-VAT, identical Feb–Jul
    ('פרסום ושיווק', 4882.0),          # avg 4,137.01 Jan–Jun × 1.18
    ('אחזקה', 1870.0),                 # avg 1,584.86 Jan–Jun × 1.18
    ('טלפון בזק', 167.0),              # avg 141.72 Jan–Jun × 1.18
    ('ארנונה ומים', 2922.0),           # 17,533.70 / 6 — municipal, no VAT
    ('עמלות כרטיסי אשראי', 2908.0),    # avg 2,907.56 Feb–Jun — clearing, no VAT
]


def render_month(conn, month):
    income = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM daily_sales "
        "WHERE branch_id=? AND substr(date,1,7)=?",
        (BRANCH_ID, month)).fetchone()[0]
    rows = conn.execute(
        "SELECT name, amount, pct_value FROM fixed_expenses "
        "WHERE branch_id=? AND month=? ORDER BY name",
        (BRANCH_ID, month)).fetchall()
    fixed_total = 0.0
    print(f"\n  {month}  (income ₪{income:,.0f})")
    for name, amount, pct in rows:
        if pct:
            live = income * pct / 100.0
            fixed_total += live
            print(f"    {name:<28} {pct}% של הכנסות = ₪{live:,.0f}")
        else:
            fixed_total += amount
            print(f"    {name:<28} ₪{amount:,.0f}")
    print(f"    {'TOTAL fixed':<28} ₪{fixed_total:,.0f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', required=True)
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    inserted = skipped = 0
    for month in MONTHS:
        for name, amount in ROWS:
            if args.apply:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO fixed_expenses "
                    "(branch_id, month, name, amount, expense_type, pct_value) "
                    "VALUES (?, ?, ?, ?, 'monthly', NULL)",
                    (BRANCH_ID, month, name, amount))
                if cur.rowcount:
                    inserted += 1
                else:
                    skipped += 1
            else:
                exists = conn.execute(
                    "SELECT 1 FROM fixed_expenses WHERE branch_id=? AND month=? "
                    "AND name=? AND expense_type='monthly'",
                    (BRANCH_ID, month, name)).fetchone()
                print(f"  [dry-run] {month} {name} ₪{amount:,.0f}"
                      f"{'  (EXISTS — would skip)' if exists else ''}")

    if args.apply:
        conn.commit()
        print(f"inserted={inserted} skipped_existing={skipped}")
        for month in MONTHS:
            render_month(conn, month)
    conn.close()


if __name__ == '__main__':
    main()

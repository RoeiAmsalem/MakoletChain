"""ONE-OFF data correction (option A): branch 9007 closed two Z's on
2026-06-01 (Z1752 3603.21 + Z1751 4289.48). daily_sales held only Z1752 due to
UNIQUE(branch_id,date). Set the row to the combined day total so the dashboard
revenue is correct. Schema fix (multi-Z/day) is deferred to option B.

Touches EXACTLY ONE row (branch 9007, date 2026-06-01). Guards:
 - aborts unless exactly one matching row exists
 - aborts unless current amount is the known 3603.21 (idempotent / safe re-run)
 - aborts unless UPDATE affects exactly 1 row
 - whole-table checksum before/after proves no other row changed
Run: python scripts/fix_9007_20260601_daily_sales.py        (dry-run, shows plan)
     python scripts/fix_9007_20260601_daily_sales.py --apply (commits)
"""
import os
import sqlite3
import sys

DB = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
BRANCH, DATE = 9007, '2026-06-01'
NEW_AMOUNT, NEW_TXNS = 7892.69, 195
EXPECT_OLD_AMOUNT = 3603.21
APPLY = '--apply' in sys.argv


def checksum(con):
    r = con.execute("SELECT COUNT(*) c, ROUND(COALESCE(SUM(amount),0),2) s, "
                    "COALESCE(SUM(transactions),0) t FROM daily_sales").fetchone()
    return r['c'], r['s'], r['t']


con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row

rows = con.execute("SELECT id, amount, transactions, source FROM daily_sales "
                   "WHERE branch_id=? AND date=?", (BRANCH, DATE)).fetchall()
if len(rows) != 1:
    sys.exit(f"ABORT: expected exactly 1 row, found {len(rows)}")
row = rows[0]
print(f"BEFORE: id={row['id']} amount={row['amount']} txns={row['transactions']} source={row['source']}")

if round(row['amount'], 2) == NEW_AMOUNT:
    sys.exit("NO-OP: row already at the corrected total — nothing to do.")
if round(row['amount'], 2) != EXPECT_OLD_AMOUNT:
    sys.exit(f"ABORT: current amount {row['amount']} != expected {EXPECT_OLD_AMOUNT} "
             "— state differs from diagnosis, refusing to overwrite.")

cnt0, sum0, txn0 = checksum(con)
print(f"plan: amount {row['amount']} -> {NEW_AMOUNT}, txns {row['transactions']} -> {NEW_TXNS}")
print(f"table checksum BEFORE: rows={cnt0} sum_amount={sum0} sum_txns={txn0}")

if not APPLY:
    print("\nDRY-RUN — re-run with --apply to commit. Nothing written.")
    sys.exit(0)

cur = con.execute("UPDATE daily_sales SET amount=?, transactions=? "
                  "WHERE branch_id=? AND date=?", (NEW_AMOUNT, NEW_TXNS, BRANCH, DATE))
if cur.rowcount != 1:
    con.rollback()
    sys.exit(f"ABORT+ROLLBACK: UPDATE affected {cur.rowcount} rows (expected 1)")

cnt1, sum1, txn1 = checksum(con)
# every other row must be untouched: row-count same, totals move by exactly our delta
if cnt1 != cnt0 or round(sum1 - sum0, 2) != round(NEW_AMOUNT - EXPECT_OLD_AMOUNT, 2) \
        or (txn1 - txn0) != (NEW_TXNS - row['transactions']):
    con.rollback()
    sys.exit(f"ABORT+ROLLBACK: checksum delta wrong "
             f"(rows {cnt0}->{cnt1}, sum {sum0}->{sum1}, txns {txn0}->{txn1})")

con.commit()
after = con.execute("SELECT id, amount, transactions, source FROM daily_sales "
                    "WHERE branch_id=? AND date=?", (BRANCH, DATE)).fetchone()
print(f"AFTER:  id={after['id']} amount={after['amount']} txns={after['transactions']} source={after['source']}")
print(f"table checksum AFTER:  rows={cnt1} sum_amount={sum1} sum_txns={txn1}")
print(f"delta: sum_amount +{round(sum1 - sum0, 2)} (expected +{round(NEW_AMOUNT - EXPECT_OLD_AMOUNT, 2)}), "
      f"rows unchanged={cnt0 == cnt1}")
print("COMMITTED — exactly 1 row changed.")

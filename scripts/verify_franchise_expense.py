"""Verify the זיכיונות default-expense feature (migration 017 + autoseed hook).

Run against a THROWAWAY copy of a real DB — it mutates branches/fixed_expenses.
  python3 scripts/verify_franchise_expense.py /tmp/verify_copy.db

Checks:
  A. New-branch autoseed hook seeds a זיכיונות 5% row for the current month.
  B. Idempotent — re-running autoseed creates no duplicate.
  C. id<>9999 guard skips the demo branch.
  D. Existing roster: every active branch (id<>9999) has exactly one זיכיונות
     row this month, none missing, none duplicated.
"""
import sqlite3
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, '.')
from agents.aviv_z_report import (
    autoseed_chain_branches,
    _seed_default_franchise_expense,
    CHAIN_AUTOSEED_LOCAL_ID_OFFSET,
)

MONTH = datetime.now(ZoneInfo('Asia/Jerusalem')).strftime('%Y-%m')
db = sys.argv[1]
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row
fails = []


def rows_for(bid):
    return conn.execute(
        "SELECT pct_value, expense_type, amount FROM fixed_expenses "
        "WHERE branch_id=? AND month=? AND name='זיכיונות'", (bid, MONTH)
    ).fetchall()


# A + B: autoseed a brand-new fake chain branch (aviv_id 7777 -> local 16777)
fake_aviv = 7777
local_id = CHAIN_AUTOSEED_LOCAL_ID_OFFSET + fake_aviv
conn.execute("DELETE FROM fixed_expenses WHERE branch_id=?", (local_id,))
conn.execute("DELETE FROM branches WHERE id=?", (local_id,))
seeded = autoseed_chain_branches(conn, [{'id': fake_aviv, 'name': 'בדיקה זמני'}])
r = rows_for(local_id)
if local_id in seeded and len(r) == 1 and r[0]['pct_value'] == 5.0 and r[0]['expense_type'] == 'monthly' and r[0]['amount'] == 0:
    print(f"A new-branch hook: PASS — branch {local_id} got זיכיונות 5%/monthly/amount=0")
else:
    fails.append(f"A new-branch hook: FAIL — seeded={seeded} rows={[dict(x) for x in r]}")

# B: re-run autoseed; branch already exists, must not duplicate the expense row
autoseed_chain_branches(conn, [{'id': fake_aviv, 'name': 'בדיקה זמני'}])
_seed_default_franchise_expense(conn, local_id)  # direct re-call too
r = rows_for(local_id)
if len(r) == 1:
    print("B idempotent: PASS — re-run produced no duplicate")
else:
    fails.append(f"B idempotent: FAIL — {len(r)} זיכיונות rows after re-run")

# C: 9999 guard
conn.execute("DELETE FROM fixed_expenses WHERE branch_id=9999 AND name='זיכיונות' AND month=?", (MONTH,))
_seed_default_franchise_expense(conn, 9999)
if len(rows_for(9999)) == 0:
    print("C 9999 guard: PASS — no row seeded for demo branch")
else:
    fails.append("C 9999 guard: FAIL — a row was seeded for 9999")

# D: existing roster coverage (real branches, exclude our fake test branch)
missing = conn.execute(
    "SELECT id FROM branches b WHERE active=1 AND id<>9999 AND id<>? "
    "AND NOT EXISTS (SELECT 1 FROM fixed_expenses f WHERE f.branch_id=b.id "
    "AND f.month=? AND f.name='זיכיונות')", (local_id, MONTH)
).fetchall()
dupes = conn.execute(
    "SELECT branch_id FROM fixed_expenses WHERE name='זיכיונות' AND month=? "
    "GROUP BY branch_id HAVING COUNT(*)>1", (MONTH,)
).fetchall()
if not missing and not dupes:
    print("D roster coverage: PASS — every active branch has exactly one זיכיונות row this month")
else:
    fails.append(f"D roster coverage: FAIL — missing={[x['id'] for x in missing]} dupes={[x['branch_id'] for x in dupes]}")

conn.rollback()  # throwaway DB, but don't persist anyway
conn.close()
if fails:
    print("\n".join(fails))
    sys.exit(1)
print(f"\nALL PASS — month={MONTH}")

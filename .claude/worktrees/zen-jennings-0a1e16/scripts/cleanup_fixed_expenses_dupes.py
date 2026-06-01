"""One-time idempotent cleanup: delete duplicate fixed_expenses rows,
keeping the lowest ID per (branch_id, month, name) group."""

import sqlite3
import sys

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else '/opt/makolet-chain-staging/db/makolet_chain.db'

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# Show duplicates before deleting
dupes = conn.execute('''
  SELECT branch_id, month, name, COUNT(*) as cnt, GROUP_CONCAT(id) as ids
  FROM fixed_expenses
  GROUP BY branch_id, month, name
  HAVING COUNT(*) > 1
  ORDER BY branch_id, month, name
''').fetchall()

print(f'Found {len(dupes)} duplicate groups')
for d in dupes:
    print(f"  branch={d['branch_id']} month={d['month']} name={d['name']} count={d['cnt']} ids={d['ids']}")

# Delete duplicates, keeping the row with the lowest id per group
result = conn.execute('''
  DELETE FROM fixed_expenses
  WHERE id NOT IN (
    SELECT MIN(id) FROM fixed_expenses GROUP BY branch_id, month, name
  )
''')
conn.commit()
print(f'Deleted {result.rowcount} duplicate rows')
conn.close()

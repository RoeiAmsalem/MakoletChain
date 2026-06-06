"""READ-ONLY audit: per-department (מכירות לפי מחלקה) sales coverage, all 18 branches.

Writes NOTHING. Pure SELECTs against db/makolet_chain.db. Answers, per branch,
for the current month:
  - does z_department_sales have rows? (dept-sales feature backing table)
  - row count + distinct days + latest date
  - does z_report_902 exist for the month? (proves the 902 Z pull itself works)
  - does daily_sales exist for the month? (proves revenue is flowing at all)

The 3-way split (902 Z yes/no  vs  dept rows yes/no  vs  daily_sales) separates
"upstream Aviv gap" from "we pull the Z but not its dept block".

Usage: python scripts/audit_dept_sales.py [YYYY-MM]
"""
import os
import sqlite3
import sys

DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                  'db', 'makolet_chain.db')

month = sys.argv[1] if len(sys.argv) > 1 else None
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
c = conn.cursor()

if month is None:
    # Anchor on the newest daily_sales date in the DB (server clock is UTC; we
    # want the month the data actually lives in).
    row = c.execute("SELECT MAX(date) d FROM daily_sales").fetchone()
    month = (row['d'] or '2026-06')[:7]

print(f"=== Department-sales audit · month={month} ===")
print(f"DB={DB}\n")

branches = c.execute(
    "SELECT id, name, active, agents_enabled, aviv_branch_id, visible_from "
    "FROM branches ORDER BY id").fetchall()

# Global latest dept date (any branch) for context.
g = c.execute("SELECT MAX(date) d FROM z_department_sales").fetchone()
print(f"global latest z_department_sales date = {g['d']}")
g2 = c.execute("SELECT MAX(date) d FROM z_report_902").fetchone()
print(f"global latest z_report_902 date       = {g2['d']}")
g3 = c.execute("SELECT MAX(date) d FROM daily_sales").fetchone()
print(f"global latest daily_sales date        = {g3['d']}\n")

hdr = (f"{'branch':6} {'aviv':4} {'act':3} {'agE':3} "
       f"{'dept?':5} {'dRows':5} {'dDays':5} {'dLatest':10} "
       f"{'902?':4} {'902Rows':7} {'902Latest':10} "
       f"{'dsLatest':10} {'visible_from':12}  name")
print(hdr)
print("-" * len(hdr))

missing = []
for b in branches:
    bid = b['id']
    av = b['aviv_branch_id']
    # dept rows this month
    d = c.execute(
        "SELECT COUNT(*) n, COUNT(DISTINCT date) dd, MAX(date) ld "
        "FROM z_department_sales WHERE branch_id=? AND substr(date,1,7)=?",
        (bid, month)).fetchone()
    # 902 Z rows this month
    z = c.execute(
        "SELECT COUNT(*) n, MAX(date) ld "
        "FROM z_report_902 WHERE branch_id=? AND substr(date,1,7)=?",
        (bid, month)).fetchone()
    # daily_sales this month
    ds = c.execute(
        "SELECT MAX(date) ld FROM daily_sales WHERE branch_id=? "
        "AND substr(date,1,7)=?", (bid, month)).fetchone()

    has_dept = 'Y' if d['n'] > 0 else 'N'
    if d['n'] == 0:
        missing.append((bid, b['name'], av, z['n'], z['ld'], ds['ld'],
                        b['active'], b['agents_enabled'], b['visible_from']))

    print(f"{bid:6} {str(av):4} {b['active']:3} {b['agents_enabled']:3} "
          f"{has_dept:5} {d['n']:5} {d['dd']:5} {str(d['ld'] or '-'):10} "
          f"{('Y' if z['n']>0 else 'N'):4} {z['n']:7} {str(z['ld'] or '-'):10} "
          f"{str(ds['ld'] or '-'):10} {str(b['visible_from'] or '-'):12}  {b['name']}")

print("\n=== MISSING dept-sales this month (root-cause inputs) ===")
if not missing:
    print("none — every branch has dept rows this month.")
for (bid, name, av, z_n, z_ld, ds_ld, act, agE, vf) in missing:
    # Classify
    if act != 1 or agE != 1:
        cause = "INACTIVE/agents-disabled (excluded from scheduler)"
    elif av is None:
        cause = "NO aviv_branch_id mapping (agent can't pull 902 at all)"
    elif ds_ld is None:
        cause = "NO daily_sales this month either (store closed / no trading / pre-visible_from)"
    elif z_n == 0:
        cause = "902 Z NOT pulled this month (upstream 902 report gap) but daily_sales exists"
    else:
        cause = "902 Z EXISTS but dept block empty/unparsed (our-side XLS parse OR Aviv XLS has no dept section)"
    print(f"  {bid} {name} | aviv={av} act={act} agE={agE} vf={vf} "
          f"| 902rows={z_n} 902latest={z_ld} dsLatest={ds_ld}\n      → {cause}")

# Also: any branch where 902 Z exists but ZERO dept rows EVER (all-time) — the
# sharpest "we have the Z but never its departments" signal.
print("\n=== All-time: 902 Z present but dept rows ZERO (ever) ===")
rows = c.execute(
    "SELECT b.id, b.name, "
    "(SELECT COUNT(*) FROM z_report_902 z WHERE z.branch_id=b.id) z902, "
    "(SELECT COUNT(*) FROM z_department_sales d WHERE d.branch_id=b.id) dept "
    "FROM branches b ORDER BY b.id").fetchall()
for r in rows:
    if r['z902'] > 0 and r['dept'] == 0:
        print(f"  {r['id']} {r['name']} | z902_all={r['z902']} dept_all=0  ← has Z, never any dept")
print("\n(full all-time z902/dept counts per branch:)")
for r in rows:
    print(f"  {r['id']:6} z902_all={r['z902']:5} dept_all={r['dept']:6}  {r['name']}")

conn.close()

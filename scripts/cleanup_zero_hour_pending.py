"""One-time cleanup: drop zero-hour employee_match_pending rows.

Before commit 0b43ba8 the Aviv employer-report agent wrote unmatched names with
0 hours into employee_match_pending on every sync, so cleared zero-hour names
(e.g. רוי ברק on 9020) regenerated at 16:00 + 23:30. 0b43ba8 stops the WRITE;
this removes the stale rows already sitting in the panel — they never self-heal
(full-month delete+reinsert only touches employee_hours, not pending).

Targets ONLY noise rows:  resolved=0 AND source='aviv_report' AND hours<=0/NULL.
Real unmatched names WITH hours (new stores' roster-pending names) are left
untouched. Idempotent: re-running finds nothing once clean.

Read-only unless --apply is passed.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, get_db

APPLY = '--apply' in sys.argv

SELECT = """
    SELECT id, branch_id, month, csv_name, hours, source
    FROM employee_match_pending
    WHERE resolved = 0 AND source = 'aviv_report' AND (hours IS NULL OR hours <= 0)
    ORDER BY branch_id, id
"""

with app.app_context():
    db = get_db()
    rows = db.execute(SELECT).fetchall()

    if not rows:
        print("Nothing to clean — 0 zero-hour aviv_report pending rows. (idempotent no-op)")
        sys.exit(0)

    print(f"{len(rows)} zero-hour pending row(s) targeted:")
    for r in rows:
        print(f"  id={r['id']:<4} branch={r['branch_id']:<5} {r['month']}  "
              f"hours={r['hours']}  {r['csv_name']}")

    if not APPLY:
        print("\nDRY-RUN — no rows deleted. Re-run with --apply to delete.")
        sys.exit(0)

    ids = [r['id'] for r in rows]
    db.execute(
        f"DELETE FROM employee_match_pending WHERE id IN ({','.join('?' * len(ids))})",
        ids)
    db.commit()
    print(f"\nDeleted {len(ids)} row(s).")

"""One-shot migration: strip "{id} " prefix from polluted csv_name values
written by the aviv_employees_report agent before the parser fix landed.

For each unresolved employee_match_pending row where source='aviv_report' and
csv_name starts with digits + whitespace:
  - Extract the leading digits → aviv_employee_id (only if currently NULL)
  - Strip leading digits + whitespace from csv_name

Idempotent: rows that no longer match the regex are left alone, so running
twice is a no-op on the second pass.

Usage:
  python scripts/cleanup_aviv_report_pending_names.py [--db PATH] [--dry-run]
"""

import argparse
import os
import re
import sqlite3
import sys

DEFAULT_DB = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')

PREFIX_RE = re.compile(r'^(\d+)\s+(.*)$')


def cleanup(db_path: str, dry_run: bool = False) -> dict:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT id, csv_name, aviv_employee_id FROM employee_match_pending "
            "WHERE source='aviv_report' AND resolved=0"
        ).fetchall()

        changed = 0
        skipped = 0
        for row in rows:
            m = PREFIX_RE.match((row['csv_name'] or '').strip())
            if not m:
                skipped += 1
                continue
            new_aviv_id = int(m.group(1))
            new_name = m.group(2).strip()
            keep_aviv_id = row['aviv_employee_id'] or new_aviv_id
            if not dry_run:
                conn.execute(
                    "UPDATE employee_match_pending "
                    "SET csv_name=?, aviv_employee_id=? WHERE id=?",
                    (new_name, keep_aviv_id, row['id']))
            changed += 1

        if not dry_run:
            conn.commit()
        return {'changed': changed, 'skipped': skipped, 'total': len(rows)}
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--db', default=DEFAULT_DB)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    res = cleanup(args.db, dry_run=args.dry_run)
    label = 'DRY RUN — would update' if args.dry_run else 'updated'
    print(f"{label} {res['changed']} of {res['total']} aviv_report pending rows "
          f"({res['skipped']} already clean)")
    return 0


if __name__ == '__main__':
    sys.exit(main())

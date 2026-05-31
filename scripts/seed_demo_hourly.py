#!/usr/bin/env python3
"""Additively place the demo sales-by-hour data into hourly_sales for BOTH demo
branches (9999 + 9998), so the /api/sales-by-hour chart renders on the home page.

This is the surgical, run-on-prod applier: it writes ONLY hourly_sales rows for
the two demo branches and touches nothing else — no daily_sales/goods/employees,
no real branch, no agent config. (The full seed_demo_branch{,_2}.py scripts also
place this data on a rebuild; this script lets you add it to a live demo DB
without re-running the full seed.)

The hourly shape is imported from seed_demo_branch.DEMO_HOURLY (single source of
truth). Identical data for 9999 and 9998 (demo stores are intentionally identical).
Placed on TODAY so it falls in the current month the chart aggregates.

Idempotent: DELETE each demo branch's hourly_sales rows, then re-insert
(PK branch_id,date,hour). Re-running never duplicates.

Usage:
    python scripts/seed_demo_hourly.py [path/to/makolet_chain.db]
Defaults to <repo>/db/makolet_chain.db.
"""
import os
import sys
import sqlite3
from datetime import date

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from scripts.seed_demo_branch import DEMO_HOURLY, cols, insert_dict  # noqa: E402

DEFAULT_DB = os.path.join(REPO_ROOT, 'db', 'makolet_chain.db')
DEMO_BRANCH_IDS = (9999, 9998)   # the ONLY branches this script may write


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB
    if not os.path.exists(db_path):
        sys.exit(f"[demo-hourly] ERROR: database not found at {db_path}")
    today = date.today().isoformat()
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('BEGIN')
        hcols = cols(conn, 'hourly_sales')
        for bid in DEMO_BRANCH_IDS:
            conn.execute('DELETE FROM hourly_sales WHERE branch_id = ?', (bid,))
            total = 0
            for hour, amount, txns in DEMO_HOURLY:
                insert_dict(conn, 'hourly_sales', {
                    'branch_id': bid, 'date': today, 'hour': hour,
                    'amount': amount, 'transactions': txns,
                }, hcols, or_ignore=True)
                total += amount
            print(f"[demo-hourly] branch {bid}: {len(DEMO_HOURLY)} rows for {today} "
                  f"(₪{total:,.0f} total, peak 19:00)")
        conn.commit()
        print("[demo-hourly] DONE — committed.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()

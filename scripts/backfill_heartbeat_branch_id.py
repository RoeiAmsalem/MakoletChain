#!/usr/bin/env python3
"""One-shot: stamp branch_id on legacy heartbeat rows that landed before the
server-side fallback. For each heartbeat with NULL/empty branch_id, copy the
branch_id of the nearest PRECEDING page_view from the same user.

Default = dry run. Pass --apply to actually write.

Usage:
    python scripts/backfill_heartbeat_branch_id.py            # dry run
    python scripts/backfill_heartbeat_branch_id.py --apply    # write
"""
import argparse
import os
import sqlite3
import sys

DEFAULT_DB = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')


def backfill(db_path, apply=False):
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT id, user_id, created_at FROM user_events "
        "WHERE event_type='heartbeat' "
        "AND (branch_id IS NULL OR branch_id='') "
        "ORDER BY id"
    ).fetchall()

    backfilled = 0
    skipped = 0
    for r in rows:
        match = conn.execute(
            "SELECT branch_id FROM user_events "
            "WHERE user_id=? AND event_type='page_view' "
            "AND branch_id IS NOT NULL AND branch_id != '' "
            "AND created_at <= ? "
            "ORDER BY created_at DESC LIMIT 1",
            (r['user_id'], r['created_at'])
        ).fetchone()
        if not match:
            skipped += 1
            print(f"[skip] heartbeat id={r['id']} user_id={r['user_id']} "
                  f"created_at={r['created_at']} — no preceding page_view")
            continue
        if apply:
            conn.execute("UPDATE user_events SET branch_id=? WHERE id=?",
                         (match['branch_id'], r['id']))
        backfilled += 1

    if apply:
        conn.commit()

    total_with_bid = conn.execute(
        "SELECT COUNT(*) FROM user_events "
        "WHERE event_type='heartbeat' AND branch_id IS NOT NULL AND branch_id != ''"
    ).fetchone()[0]

    conn.close()

    mode = 'APPLY' if apply else 'DRY-RUN'
    print(f"[{mode}] Backfilled {backfilled} rows, skipped {skipped} "
          f"(no preceding page_view), total heartbeats now with branch_id: "
          f"{total_with_bid}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--apply', action='store_true',
                   help='Actually write changes (default: dry run)')
    p.add_argument('--db', default=DEFAULT_DB, help='Path to SQLite DB')
    args = p.parse_args()

    if not os.path.exists(args.db):
        print(f"DB not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    backfill(args.db, apply=args.apply)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Seed the 18 chain branches into the branches table (idempotent).

Sourced from staging's verified branches map: each row carries the local
branch id, the Aviv BI branch id (aviv_branch_id), the BilBoy chain store id
(bilboy_branch_id), the Hebrew store name, and active=1. Everything else is
left NULL so per-branch onboarding (Aviv creds, gmail_label, franchise_supplier,
IEC, etc.) can fill them in later.

Idempotent: uses INSERT OR IGNORE keyed on the primary key `id`, so re-running
never overwrites an existing branch. In particular branches 126 (אינשטיין) and
127 (התיכון) already exist on prod and are left completely untouched.

Usage:
    python scripts/seed_chain_branches.py [path/to/makolet_chain.db]

Defaults to <repo>/db/makolet_chain.db (the prod/staging convention).
"""
import os
import sqlite3
import sys

DEFAULT_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'db', 'makolet_chain.db'
)

# (id, aviv_branch_id, bilboy_branch_id, name) — verified against staging's
# branches table (18 rows, both ids set). active=1, all other columns NULL.
CHAIN_BRANCHES = [
    (126,  3,  126,  'מכולת אינשטיין'),
    (127,  8,  170,  'המכולת תיכון'),
    (9001, 1,  99,   'קדיש לוז'),
    (9002, 2,  2653, 'קק"ל'),
    (9006, 6,  106,  'נווה זיו'),
    (9007, 7,  125,  "ז'בוטינסקי"),
    (9009, 9,  3606, 'שבטי ישראל'),
    (9010, 10, 124,  'שומרת'),
    (9011, 11, 107,  'ויצמן'),
    (9012, 12, 123,  'בצת'),
    (9013, 13, 122,  'לימן'),
    (9014, 14, 3327, 'קרן היסוד'),
    (9015, 15, 483,  'הגנה'),
    (9016, 16, 2267, 'המכולת קריית טבעון'),
    (9017, 17, 2337, 'המכולת רמת השרון'),
    (9018, 18, 3684, 'המכולת דפנה'),
    (9019, 19, 4724, 'כפר סירקין'),
    (9020, 20, 4901, 'רמת גן'),
]


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB
    if not os.path.exists(db_path):
        print(f"[seed] ERROR: database not found at {db_path}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cur = conn.cursor()
        inserted, skipped = [], []
        for bid, aviv, bilboy, name in CHAIN_BRANCHES:
            cur.execute(
                "INSERT OR IGNORE INTO branches "
                "(id, name, active, aviv_branch_id, bilboy_branch_id) "
                "VALUES (?, ?, 1, ?, ?)",
                (bid, name, aviv, bilboy),
            )
            (inserted if cur.rowcount == 1 else skipped).append((bid, name))
        conn.commit()
    finally:
        conn.close()

    print(f"[seed] inserted {len(inserted)} | skipped {len(skipped)} "
          f"(already present) | total {len(CHAIN_BRANCHES)}")
    for bid, name in inserted:
        print(f"  + {bid}\t{name}")
    for bid, name in skipped:
        print(f"  = {bid}\t{name} (unchanged)")
    return 0


if __name__ == '__main__':
    sys.exit(main())

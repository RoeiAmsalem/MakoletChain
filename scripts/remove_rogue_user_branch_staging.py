"""One-off: remove rogue user_branches row on STAGING.

Investigation context: staging DB had Shimon (shimonmakolet@gmail.com, user_id
historically=2) with rows for BOTH branch 126 AND 127 in user_branches.
Production has him on 126 only — staging diverged at some point (likely via
/admin/users testing). Server `get_branch_id()` uses user_branches as the
allow-list, so the extra row gave Shimon real read access to branch 127's data
on staging.

This script removes that single row, with safety checks:
  - Refuses to run if the user_id row doesn't match email shimonmakolet@gmail.com
  - Prints the before/after rows for the user
  - Supports --dry-run

Usage on staging:
    python3 scripts/remove_rogue_user_branch_staging.py
    python3 scripts/remove_rogue_user_branch_staging.py --dry-run

NEVER run on prod — prod already has the correct single-branch row. The script
will refuse if the rogue row is already absent (idempotent no-op).
"""
import argparse
import os
import sqlite3
import sys

DEFAULT_DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
TARGET_EMAIL = 'shimonmakolet@gmail.com'
ROGUE_BRANCH_ID = 127
KEEP_BRANCH_ID = 126


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default=DEFAULT_DB)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f'ERROR: db not found: {args.db}', file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(args.db, timeout=30)
    conn.row_factory = sqlite3.Row

    user = conn.execute(
        'SELECT id, email FROM users WHERE email = ?', (TARGET_EMAIL,)
    ).fetchone()
    if not user:
        print(f'ERROR: no user with email {TARGET_EMAIL}', file=sys.stderr)
        sys.exit(1)
    user_id = user['id']

    before = conn.execute(
        'SELECT branch_id FROM user_branches WHERE user_id = ? ORDER BY branch_id',
        (user_id,)
    ).fetchall()
    before_ids = [r['branch_id'] for r in before]
    print(f'user_id={user_id} ({TARGET_EMAIL})')
    print(f'before: user_branches = {before_ids}')

    if ROGUE_BRANCH_ID not in before_ids:
        print(f'[skip] rogue row branch_id={ROGUE_BRANCH_ID} already absent — nothing to do')
        return 0
    if KEEP_BRANCH_ID not in before_ids:
        print(f'ERROR: refusing to delete — user does not own branch {KEEP_BRANCH_ID}; '
              f'something is off, investigate manually', file=sys.stderr)
        sys.exit(2)

    if args.dry_run:
        print(f'[dry-run] would DELETE user_branches WHERE user_id={user_id} '
              f'AND branch_id={ROGUE_BRANCH_ID}')
        return 0

    conn.execute(
        'DELETE FROM user_branches WHERE user_id = ? AND branch_id = ?',
        (user_id, ROGUE_BRANCH_ID)
    )
    conn.commit()

    after = conn.execute(
        'SELECT branch_id FROM user_branches WHERE user_id = ? ORDER BY branch_id',
        (user_id,)
    ).fetchall()
    after_ids = [r['branch_id'] for r in after]
    print(f'after:  user_branches = {after_ids}')

    if after_ids == [KEEP_BRANCH_ID]:
        print('OK: rogue row removed. Shimon must log out and back in to refresh session.')
        return 0
    print(f'WARNING: unexpected post-state {after_ids}', file=sys.stderr)
    return 3


if __name__ == '__main__':
    sys.exit(main())

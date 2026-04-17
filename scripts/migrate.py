#!/usr/bin/env python3
"""
Database migration runner for MakoletChain.
Applies numbered SQL migrations from migrations/ folder.

Usage:
    python3 scripts/migrate.py            # apply all pending migrations
    python3 scripts/migrate.py --status   # show applied + pending migrations
    python3 scripts/migrate.py --dry-run  # list what would be applied
"""

import argparse
import glob
import os
import sqlite3
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'db', 'makolet_chain.db')
MIGRATIONS_DIR = os.path.join(BASE_DIR, 'migrations')


def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def ensure_migrations_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _migrations (
            filename TEXT PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()


def get_applied(conn):
    rows = conn.execute("SELECT filename, applied_at FROM _migrations ORDER BY filename").fetchall()
    return {row[0]: row[1] for row in rows}


def get_migration_files():
    pattern = os.path.join(MIGRATIONS_DIR, '*.sql')
    files = sorted(os.path.basename(f) for f in glob.glob(pattern))
    return files


def cmd_status(conn):
    applied = get_applied(conn)
    files = get_migration_files()
    all_names = sorted(set(list(applied.keys()) + files))

    if not all_names:
        print("[migrate] No migrations found.")
        return

    print(f"{'Migration':<45} {'Status':<10} {'Applied at'}")
    print("-" * 80)
    for name in all_names:
        if name in applied:
            print(f"{name:<45} {'applied':<10} {applied[name]}")
        else:
            print(f"{name:<45} {'pending':<10} -")

    applied_count = sum(1 for n in all_names if n in applied)
    pending_count = len(all_names) - applied_count
    print(f"\n{applied_count} applied, {pending_count} pending.")


def cmd_apply(conn, dry_run=False):
    applied = get_applied(conn)
    files = get_migration_files()
    pending = [f for f in files if f not in applied]

    if not pending:
        print(f"[migrate] All migrations up to date ({len(applied)} applied).")
        return 0

    if dry_run:
        print(f"[migrate] Dry run — {len(pending)} migration(s) would be applied:")
        for f in pending:
            print(f"  - {f}")
        return 0

    for filename in pending:
        filepath = os.path.join(MIGRATIONS_DIR, filename)
        print(f"[migrate] Applying {filename}...", end=" ", flush=True)
        try:
            with open(filepath, 'r') as fh:
                sql = fh.read()
            conn.execute("BEGIN")
            conn.executescript(sql)
            conn.execute("INSERT INTO _migrations (filename) VALUES (?)", (filename,))
            conn.commit()
            print("OK")
        except Exception as e:
            conn.rollback()
            print("FAILED")
            print(f"[migrate] Error applying {filename}: {e}", file=sys.stderr)
            return 1

    total = len(applied) + len(pending)
    print(f"[migrate] All migrations up to date ({total} applied).")
    return 0


def main():
    parser = argparse.ArgumentParser(description="MakoletChain database migration runner")
    parser.add_argument("--status", action="store_true", help="Show migration status")
    parser.add_argument("--dry-run", action="store_true", help="List pending migrations without applying")
    args = parser.parse_args()

    if not os.path.exists(DB_PATH):
        print(f"[migrate] ERROR: Database not found at {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = get_connection()
    ensure_migrations_table(conn)

    if args.status:
        cmd_status(conn)
        conn.close()
        sys.exit(0)

    exit_code = cmd_apply(conn, dry_run=args.dry_run)
    conn.close()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

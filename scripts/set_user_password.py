#!/usr/bin/env python3
"""Set (or reset) a user's password by email — no password ever in argv/code/git.

Reads the new password from the DEMO_PASSWORD env var if set, otherwise prompts
interactively (hidden input). Hashes with werkzeug (same scheme as the app's
check_password_hash) and updates the user row. Email match is case-insensitive,
matching the app's login normalization.

Usage:
    # interactive (recommended — nothing hits shell history):
    python scripts/set_user_password.py demo@makoletchain.com
    # non-interactive:
    DEMO_PASSWORD='...' python scripts/set_user_password.py demo@makoletchain.com [db_path]
"""
import os
import sys
import getpass
import sqlite3

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from werkzeug.security import generate_password_hash  # noqa: E402

DEFAULT_DB = os.path.join(REPO_ROOT, 'db', 'makolet_chain.db')


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: set_user_password.py <email> [db_path]")
    email = sys.argv[1]
    db_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DB
    if not os.path.exists(db_path):
        sys.exit(f"ERROR: database not found at {db_path}")

    pw = os.environ.get('DEMO_PASSWORD')
    if not pw:
        pw = getpass.getpass(f"New password for {email}: ")
        if pw != getpass.getpass("Confirm: "):
            sys.exit("ERROR: passwords do not match")
    if len(pw) < 6:
        sys.exit("ERROR: password must be at least 6 characters")

    conn = sqlite3.connect(db_path, timeout=30)
    try:
        row = conn.execute('SELECT id, name FROM users WHERE LOWER(email)=LOWER(?)',
                           (email,)).fetchone()
        if not row:
            sys.exit(f"ERROR: no user with email {email}")
        conn.execute('UPDATE users SET password_hash=? WHERE id=?',
                     (generate_password_hash(pw), row[0]))
        conn.commit()
        print(f"OK — password set for {row[1]} <{email}> (id={row[0]})")
    finally:
        conn.close()


if __name__ == '__main__':
    main()

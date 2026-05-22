#!/usr/bin/env python3
"""Throwaway: dump the filters/902 response shape for one branch.

Used once to confirm the Z-list field naming/wrapping. Safe to delete
after agent verification.
"""
import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import agents.aviv_z_report as zr  # noqa: E402

BRANCH_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 126

conn = sqlite3.connect(zr.DB_PATH)
conn.row_factory = sqlite3.Row
row = conn.execute('SELECT * FROM branches WHERE id=?', (BRANCH_ID,)).fetchone()
token, aviv_branch_id = zr._login(row['aviv_user_id'], row['aviv_password'] or row['aviv_user_id'])
token = zr._refresh(token)
filters = zr.fetch_902_filters(aviv_branch_id, token)
print('aviv_branch_id:', aviv_branch_id)
print(json.dumps(filters, ensure_ascii=False, indent=2)[:5000])

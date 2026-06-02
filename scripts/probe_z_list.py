"""READ-ONLY: dump Aviv report-902 Z-list for a branch, highlight a date.

Writes NOTHING. Shows every Z↔date pair Aviv exposes so we can see when a store
closed 2+ Z's on one day. Usage: python scripts/probe_z_list.py LOCAL_ID DATE
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlite3  # noqa: E402
import urllib3  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

load_dotenv()
urllib3.disable_warnings()
from agents.aviv_z_report import (  # noqa: E402
    _login_chain_account, fetch_902_z_list, _iter_z_entries,
)

local_id = int(sys.argv[1])
target = sys.argv[2]
DB = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
abid = sqlite3.connect(DB).execute(
    "SELECT aviv_branch_id FROM branches WHERE id=?", (local_id,)).fetchone()[0]

token = _login_chain_account()
filters = fetch_902_z_list(abid, token)
entries = list(_iter_z_entries(filters))
print(f"branch {local_id} (aviv_branch_id={abid}): {len(entries)} Z entries\n")

same_day = [e for e in entries if e['date'] == target]
print(f"=== Z entries on {target}: {len(same_day)} ===")
for e in sorted(same_day, key=lambda x: x['z_number']):
    print(f"    Z={e['z_number']}  date={e['date']}")

print(f"\n=== context (Z entries around {target}) ===")
ctx = sorted([e for e in entries if abs((__import__('datetime').date.fromisoformat(e['date'])
              - __import__('datetime').date.fromisoformat(target)).days) <= 2],
             key=lambda x: (x['date'], x['z_number']))
for e in ctx:
    print(f"    Z={e['z_number']}  date={e['date']}")

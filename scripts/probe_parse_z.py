"""READ-ONLY: fetch + parse ONE specific Z's 902 PDF and print the parsed
total/txns. Writes NOTHING (no DB, no saved PDF).

Usage: python scripts/probe_parse_z.py LOCAL_ID Z_NUMBER
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
    _login_chain_account, submit_902, download_pdf, parse_902_pdf,
)

local_id = int(sys.argv[1])
z = int(sys.argv[2])
DB = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
abid = sqlite3.connect(DB).execute(
    "SELECT aviv_branch_id FROM branches WHERE id=?", (local_id,)).fetchone()[0]

token = _login_chain_account()
url = submit_902(abid, z, token)
pdf = download_pdf(url, token)
parsed = parse_902_pdf(pdf)
print(f"branch {local_id} (aviv={abid}) Z={z}: total={parsed.get('total')} "
      f"txns={parsed.get('transactions')} avg={parsed.get('avg_per_txn')}")
print(f"payment_breakdown={parsed.get('payment_breakdown')}")

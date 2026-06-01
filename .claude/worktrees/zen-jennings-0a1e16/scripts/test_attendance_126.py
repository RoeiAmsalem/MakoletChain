"""One-time test: process attendance CSV from Shimon's forwarded email for branch 126."""
import os, sys, logging
sys.path.insert(0, '/opt/makolet-chain')
os.chdir('/opt/makolet-chain')
from dotenv import load_dotenv
load_dotenv('/opt/makolet-chain/.env')

import imaplib
from agents.gmail_agent import _sync_attendance_csv, _get_branch_config, DB_PATH

GMAIL_ADDRESS = os.environ.get('GMAIL_ADDRESS', '')
GMAIL_APP_PASSWORD = os.environ.get('GMAIL_APP_PASSWORD', '')

# Setup logging to stdout
log = logging.getLogger('test_attendance')
log.setLevel(logging.DEBUG)
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
log.addHandler(sh)

branch_id = 126
branch = _get_branch_config(branch_id)
print(f"Branch: {branch['name']}, gmail_label: {branch.get('gmail_label')}")

# Check existing employee_hours for context
import sqlite3
conn = sqlite3.connect(DB_PATH, timeout=30)
conn.row_factory = sqlite3.Row
rows = conn.execute(
    "SELECT month, COUNT(*) as cnt FROM employee_hours WHERE branch_id=? GROUP BY month ORDER BY month DESC LIMIT 5",
    (branch_id,)
).fetchall()
print(f"\nExisting employee_hours for branch {branch_id}:")
for r in rows:
    print(f"  {r['month']}: {r['cnt']} employees")
conn.close()

# Connect to Gmail and run attendance sync
print(f"\nConnecting to Gmail as {GMAIL_ADDRESS}...")
mail = imaplib.IMAP4_SSL('imap.gmail.com', 993)
mail.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
mail.select('inbox')

print("Running _sync_attendance_csv...")
result = _sync_attendance_csv(mail, branch, branch_id, log)
print(f"\nResult: {result}")

mail.logout()

# Show what's in employee_hours now
conn = sqlite3.connect(DB_PATH, timeout=30)
conn.row_factory = sqlite3.Row
rows = conn.execute(
    "SELECT month, employee_name, total_hours, total_salary, source "
    "FROM employee_hours WHERE branch_id=? ORDER BY month DESC, employee_name LIMIT 30",
    (branch_id,)
).fetchall()
print(f"\nEmployee hours after sync:")
for r in rows:
    print(f"  {r['month']} | {r['employee_name']:20s} | {r['total_hours']:6.1f}h | ₪{r['total_salary']:,.0f} | {r['source']}")
conn.close()

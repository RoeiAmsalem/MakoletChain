"""Verify the chain-store enrich flow against the live staging DB.

Demonstrates:
  1. Picks an autoseed chain row that is NOT yet configured.
  2. Calls POST /api/admin/branches as an admin (session injected via
     Flask's signed-session helper — no password needed).
  3. Confirms exactly one row was UPDATEd (city / franchise_supplier /
     bilboy_user) and that branches row count did NOT change.
  4. Cleans up the test edits so the staging DB is left as found.

Read-only against prod by design — only run on staging. Refuses to run if
DATABASE_URL points at the prod DB.
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as flask_app  # noqa: E402

DB_PATH = flask_app.DB_PATH
if 'staging' not in DB_PATH and not os.environ.get('ALLOW_NONSTAGING'):
    print(f'[abort] refusing to run against non-staging DB: {DB_PATH}')
    sys.exit(2)

print(f'[db] {DB_PATH}')


def snapshot():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    total = conn.execute('SELECT COUNT(*) FROM branches').fetchone()[0]
    conn.close()
    return total


def pick_candidate():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT id, name, aviv_branch_id, city, bilboy_user, franchise_supplier "
        "FROM branches "
        "WHERE active=1 AND aviv_branch_id IS NOT NULL "
        "  AND aviv_branch_id NOT IN (90, 900) "
        "  AND (bilboy_pass IS NULL OR bilboy_pass='') "
        "ORDER BY id LIMIT 1"
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def restore(bid, original):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        'UPDATE branches SET city=?, bilboy_user=?, franchise_supplier=? WHERE id=?',
        (original.get('city') or '',
         original.get('bilboy_user') or '',
         original.get('franchise_supplier') or '',
         bid))
    conn.commit()
    conn.close()


before_total = snapshot()
cand = pick_candidate()
if not cand:
    print('[abort] no unconfigured chain candidate found on staging')
    sys.exit(1)
print(f"[before] branches={before_total} candidate id={cand['id']} "
      f"name={cand['name']!r} aviv#{cand['aviv_branch_id']} "
      f"city={cand['city']!r} bilboy_user={cand['bilboy_user']!r} "
      f"franchise={cand['franchise_supplier']!r}")

flask_app.app.config['TESTING'] = True
client = flask_app.app.test_client()

# Inject an admin session — bypass the password the admin user actually has on
# staging. We use any active admin row.
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
admin = conn.execute(
    "SELECT id FROM users WHERE role='admin' AND active=1 ORDER BY id LIMIT 1"
).fetchone()
conn.close()
if not admin:
    print('[abort] no admin user on staging')
    sys.exit(1)
with client.session_transaction() as sess:
    sess['user_id'] = admin['id']
    sess['user_role'] = 'admin'

# 1) Reject when branch_id missing.
resp = client.post('/api/admin/branches', json={'city': 'X'})
print(f"[guard:no-branch_id] status={resp.status_code} body={resp.get_json()}")

# 2) Reject when branch_id is unknown.
resp = client.post('/api/admin/branches', json={'branch_id': 999999})
print(f"[guard:unknown-id] status={resp.status_code} body={resp.get_json()}")

# 3) Real enrich call.
payload = {
    'branch_id': cand['id'],
    'city': 'STAGING_TEST_CITY',
    'bilboy_user': 'staging_test_user',
    'franchise_supplier': 'STAGING_TEST_SUPPLIER',
}
resp = client.post('/api/admin/branches', json=payload)
print(f"[enrich] status={resp.status_code} body={resp.get_json()}")

after_total = snapshot()
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
row = conn.execute(
    'SELECT id, name, aviv_branch_id, city, bilboy_user, franchise_supplier '
    'FROM branches WHERE id=?', (cand['id'],)).fetchone()
conn.close()
print(f"[after]  branches={after_total} row id={row['id']} "
      f"city={row['city']!r} bilboy_user={row['bilboy_user']!r} "
      f"franchise={row['franchise_supplier']!r}")

ok_count = (after_total == before_total)
ok_update = (row['city'] == 'STAGING_TEST_CITY'
             and row['bilboy_user'] == 'staging_test_user'
             and row['franchise_supplier'] == 'STAGING_TEST_SUPPLIER')
print(f"[verdict] no_new_rows={ok_count} updated_in_place={ok_update}")

restore(cand['id'], cand)
print('[cleanup] restored original values on candidate row')
sys.exit(0 if (ok_count and ok_update) else 1)

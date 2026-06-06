"""Diagnostic for the /goods יעדים relocation (run on staging).

Verifies, via the Flask test client against the staging DB:
  - GET /goal now 404 (standalone page retired)
  - GET /api/goal/data still 200
  - POST /api/goal/budget still 200, persists a DB row, and 0 clears it
  - branch 9015's stored city (proves the header '— null' is a cityless branch)

Usage:  python3 scripts/diag_goods_goals.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import sqlite3
import app as app_module
from app import app

BRANCH = 9015


def _conn():
    c = sqlite3.connect(app_module.DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def _seed_session(client):
    conn = _conn()
    row = conn.execute("SELECT id, role FROM users WHERE active=1 AND role='admin' LIMIT 1").fetchone()
    conn.close()
    with client.session_transaction() as sess:
        sess['user_id'] = row['id']
        sess['user_role'] = row['role']
        sess['branch_id'] = BRANCH
        sess['user_branches'] = [BRANCH]


client = app.test_client()
_seed_session(client)

r_goal = client.get('/goal')
print(f"GET /goal            -> HTTP {r_goal.status_code} (expect 404)")

r_data = client.get(f'/api/goal/data?branch_id={BRANCH}')
print(f"GET /api/goal/data   -> HTTP {r_data.status_code} (expect 200)")

# persistence round-trip
supplier = '__diag supplier__'
r_set = client.post('/api/goal/budget', json={'supplier_name': supplier, 'monthly_budget': 7777})
conn = _conn()
saved = conn.execute("SELECT monthly_budget FROM supplier_budgets WHERE branch_id=? AND supplier_name=?",
                     (BRANCH, supplier)).fetchone()
conn.close()
print(f"POST budget 7777     -> HTTP {r_set.status_code} | DB row monthly_budget={saved['monthly_budget'] if saved else None}")

r_clr = client.post('/api/goal/budget', json={'supplier_name': supplier, 'monthly_budget': 0})
conn = _conn()
gone = conn.execute("SELECT 1 FROM supplier_budgets WHERE branch_id=? AND supplier_name=?",
                    (BRANCH, supplier)).fetchone()
city = conn.execute("SELECT name, city FROM branches WHERE id=?", (BRANCH,)).fetchone()
conn.close()
print(f"POST budget 0        -> HTTP {r_clr.status_code} | DB row exists now: {bool(gone)} (cleared)")
print(f"branch {BRANCH}: name={city['name']!r} city={city['city']!r}  -> header shows just the name")

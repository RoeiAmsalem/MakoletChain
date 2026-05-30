#!/usr/bin/env python3
"""Verify the demo branch (9999) renders for the scoped account and that no
agent touched it. Read-only — makes no writes. Run on prod after seeding.

    cd /opt/makolet-chain && venv/bin/python scripts/verify_demo_branch.py
"""
import os
import sys
import sqlite3

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

DB = os.path.join(REPO_ROOT, 'db', 'makolet_chain.db')
DEMO_ID = 9999
DEMO_EMAIL = 'demo-store@makoletchain.com'

import app as A  # noqa: E402
A.app.config['TESTING'] = True

results = []
def check(label, ok, detail=''):
    results.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}: {label}" + (f" — {detail}" if detail else ''))

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
uid = conn.execute("SELECT id FROM users WHERE email=?", (DEMO_EMAIL,)).fetchone()['id']

# ── No-agent guarantee ──────────────────────────────────────────────────────
nruns = conn.execute("SELECT COUNT(*) c FROM agent_runs WHERE branch_id=?", (DEMO_ID,)).fetchone()['c']
check("ZERO agent_runs for demo branch", nruns == 0, f"found {nruns}")
ae = conn.execute("SELECT agents_enabled FROM branches WHERE id=?", (DEMO_ID,)).fetchone()['agents_enabled']
check("demo branch agents_enabled=0", ae == 0, f"agents_enabled={ae}")
nbad = conn.execute("SELECT COUNT(*) c FROM branches WHERE agents_enabled IS NOT 1 AND id!=?", (DEMO_ID,)).fetchone()['c']
check("all REAL branches still agents_enabled=1", nbad == 0, f"{nbad} real branches not enabled")
for col in ('aviv_user_id', 'aviv_branch_id', 'bilboy_branch_id', 'bilboy_pass', 'gmail_label', 'iec_token'):
    v = conn.execute(f"SELECT {col} FROM branches WHERE id=?", (DEMO_ID,)).fetchone()[0]
    check(f"agent-config {col} IS NULL", v is None, f"got {v!r}")

# ── Data present ────────────────────────────────────────────────────────────
for tbl, want in [('daily_sales', 59), ('goods_documents', 24),
                  ('employee_hours', 10), ('employee_match_pending', 5),
                  ('fixed_expenses', 11)]:
    c = conn.execute(f"SELECT COUNT(*) c FROM {tbl} WHERE branch_id=?", (DEMO_ID,)).fetchone()['c']
    check(f"{tbl} rows for 9999", c == want, f"got {c}, want {want}")

# ── PDF wired ───────────────────────────────────────────────────────────────
latest = conn.execute("SELECT MAX(date) d FROM daily_sales WHERE branch_id=?", (DEMO_ID,)).fetchone()['d']
pdf = os.path.join(REPO_ROOT, 'data', 'pdfs', str(DEMO_ID), f'z_{latest}.pdf')
check("Z-PDF exists for most-recent demo date", os.path.exists(pdf), pdf)

# ── Pages render + scoping (logged in as the scoped manager) ────────────────
c = A.app.test_client()
with c.session_transaction() as s:
    s['user_id'] = uid; s['user_name'] = 'demo'; s['user_role'] = 'manager'
    s['user_branches'] = [DEMO_ID]; s['branch_id'] = DEMO_ID
for path in ['/', '/sales', '/goods', '/employees', '/fixed-expenses']:
    r = c.get(path)
    check(f"GET {path} renders", r.status_code == 200, f"status={r.status_code}")
vis = [b['id'] for b in (c.get('/api/branches').get_json() or [])]
check("scoped account sees ONLY [9999]", vis == [DEMO_ID], f"got {vis}")
for path in ['/ops', '/admin/users', '/admin/branches']:
    check(f"blocked from {path}", c.get(path).status_code in (302, 403))

# Demo headline numbers (real prod schema -> salary query runs)
summ = c.get(f'/api/summary?month=2026-05&branch_id={DEMO_ID}').get_json() or {}
print(f"\n[demo numbers] /api/summary 2026-05: revenue={summ.get('revenue')} "
      f"expenses={summ.get('expenses')} salary={summ.get('salary')}")
pend = c.get(f'/api/employee-match-pending?month=2026-05&branch_id={DEMO_ID}').get_json() or {}
print(f"[demo numbers] pending matches: {len(pend.get('pending', []))} "
      f"(employees defined: {len(pend.get('employees', []))})")

conn.close()
print(f"\n=== {sum(results)}/{len(results)} checks passed ===")
sys.exit(0 if all(results) else 1)

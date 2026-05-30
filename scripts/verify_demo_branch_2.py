#!/usr/bin/env python3
"""Read-only verification for the SECOND demo branch (id 9998) + multi-store demo.

Asserts the guardrails from the build spec:
  * 9998 exists, active, agents_enabled=0, NULL credentials, ZERO agent_runs.
  * 9998 data is IDENTICAL to 9999 (per-table row counts + key totals match).
  * ₪4,000 live tile present for BOTH 9998 and 9999 (today).
  * demo-store@ scoped to EXACTLY {9998, 9999}; no real branch reachable; no
    other user holds 9998.
  * data/pdfs/9998/ mirrors data/pdfs/9999/ (Z-PDF previewer).

Exits non-zero if any check FAILs. Touches nothing (SELECT-only + os.listdir).

Usage: python scripts/verify_demo_branch_2.py [path/to/makolet_chain.db]
"""
import os
import sys
import sqlite3
from datetime import date

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB = os.path.join(REPO_ROOT, 'db', 'makolet_chain.db')
PDF_BASE = os.path.join(REPO_ROOT, 'data', 'pdfs')

SRC_ID = 9999
ID = 9998
EMAIL = 'demo-store@makoletchain.com'
EXPECT_SCOPE = [9998, 9999]
CREDENTIAL_COLS = [
    'aviv_user_id', 'aviv_password', 'bilboy_user', 'bilboy_pass', 'gmail_label',
    'franchise_supplier', 'aviv_branch_id', 'bilboy_branch_id', 'iec_token',
]
COPY_TABLES = ['daily_sales', 'goods_documents', 'fixed_expenses',
               'employees', 'employee_hours', 'employee_match_pending']

fails = 0


def check(label, ok, detail=''):
    global fails
    print(f"{'PASS' if ok else 'FAIL'} — {label}" + (f" :: {detail}" if detail else ''))
    if not ok:
        fails += 1


def main():
    db = sqlite3.connect(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB)
    db.row_factory = sqlite3.Row
    q = db.execute

    # 1. branch exists + flags
    b = q('SELECT * FROM branches WHERE id = ?', (ID,)).fetchone()
    check('9998 exists', b is not None)
    if b:
        check('9998 active=1', b['active'] == 1, f"active={b['active']}")
        check('9998 agents_enabled=0', b['agents_enabled'] == 0,
              f"agents_enabled={b['agents_enabled']}")
        bk = b.keys()
        nulls = [c for c in CREDENTIAL_COLS if c in bk and b[c] is not None]
        check('9998 credentials all NULL', not nulls, f"non-null: {nulls}")

    # 2. zero agent_runs (same proof as 9999)
    ar = q('SELECT COUNT(*) FROM agent_runs WHERE branch_id = ?', (ID,)).fetchone()[0]
    check('9998 zero agent_runs', ar == 0, f"count={ar}")

    # 3. data identical to 9999 (row counts per table)
    for t in COPY_TABLES:
        a = q(f'SELECT COUNT(*) FROM {t} WHERE branch_id = ?', (ID,)).fetchone()[0]
        c = q(f'SELECT COUNT(*) FROM {t} WHERE branch_id = ?', (SRC_ID,)).fetchone()[0]
        check(f'{t} row count 9998==9999', a == c, f"9998={a} 9999={c}")

    # 4. key totals identical
    for t, col, datecol in [('daily_sales', 'amount', None),
                            ('goods_documents', 'amount', None)]:
        a = q(f'SELECT ROUND(COALESCE(SUM({col}),0),2) FROM {t} WHERE branch_id=?', (ID,)).fetchone()[0]
        c = q(f'SELECT ROUND(COALESCE(SUM({col}),0),2) FROM {t} WHERE branch_id=?', (SRC_ID,)).fetchone()[0]
        check(f'{t} SUM({col}) 9998==9999', a == c, f"9998={a} 9999={c}")

    # 5. live ₪4,000 tile today for BOTH demo branches
    today = date.today().isoformat()
    for bid in (ID, SRC_ID):
        r = q('SELECT amount FROM live_sales WHERE branch_id=? AND date=?',
              (bid, today)).fetchone()
        check(f'live tile today for {bid} = 4000', r is not None and r['amount'] == 4000.0,
              f"row={dict(r) if r else None}")

    # 6. demo user scope EXACTLY {9998, 9999}
    u = q('SELECT id, role FROM users WHERE LOWER(email)=LOWER(?)', (EMAIL,)).fetchone()
    check('demo user exists', u is not None)
    if u:
        check('demo user role=manager', u['role'] == 'manager', f"role={u['role']}")
        scope = [r[0] for r in q(
            'SELECT branch_id FROM user_branches WHERE user_id=? ORDER BY branch_id',
            (u['id'],)).fetchall()]
        check('demo user scope == {9998,9999} (no real branch)', scope == EXPECT_SCOPE,
              f"scope={scope}")

    # 7. no OTHER user holds 9998
    others = q('SELECT COUNT(*) FROM user_branches WHERE branch_id=? AND user_id!=?',
               (ID, u['id'] if u else -1)).fetchone()[0]
    check('no other user linked to 9998', others == 0, f"others={others}")

    # 8. PDF mirror
    src_pdfs = set(f for f in os.listdir(os.path.join(PDF_BASE, str(SRC_ID)))
                   if f.lower().endswith('.pdf')) if os.path.isdir(os.path.join(PDF_BASE, str(SRC_ID))) else set()
    dst_dir = os.path.join(PDF_BASE, str(ID))
    dst_pdfs = set(f for f in os.listdir(dst_dir) if f.lower().endswith('.pdf')) if os.path.isdir(dst_dir) else set()
    check('9998 Z-PDFs mirror 9999', src_pdfs and src_pdfs <= dst_pdfs,
          f"9999={sorted(src_pdfs)} 9998={sorted(dst_pdfs)}")

    db.close()
    print(f"\n{'ALL CHECKS PASSED' if fails == 0 else f'{fails} CHECK(S) FAILED'}")
    sys.exit(1 if fails else 0)


if __name__ == '__main__':
    main()

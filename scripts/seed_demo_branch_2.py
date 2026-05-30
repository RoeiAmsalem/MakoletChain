#!/usr/bin/env python3
"""Seed the SECOND demo branch "מכולת הדגמה 2" (id 9998) for the multi-store demo.

Purpose: give the scoped demo account TWO stores so the sales demo can show the
header store-switcher and the "more than one branch" experience — all on fake
data, with NO real branch ever visible.

9998 is an EXACT copy of branch 9999's hand-placed data (Roei confirmed identical
numbers are fine), created with NULL agent-config + agents_enabled=0 so — exactly
like 9999 — no scheduled agent ever touches it (zero agent_runs, no alerts).

What it does (idempotent, additive-only; every write is scoped to branch 9998,
branch 9999's live tile, or the demo user — a real branch's rows are never
updated or deleted):

  * branches            — UPSERT id 9998, active=1, agents_enabled=0, ALL
                          integration credentials forced NULL (copied from 9999,
                          which already has them NULL). name = "מכולת הדגמה 2".
  * data copy 9999→9998 — daily_sales, goods_documents, fixed_expenses,
                          employees, employee_hours, employee_match_pending.
                          DELETE 9998's rows then INSERT…SELECT from 9999 with
                          branch_id rewritten (schema-introspecting: all columns
                          except the autoincrement id are copied verbatim).
  * live_sales          — a static ₪4,000 "live now" tile row for BOTH 9998 and
                          9999, for a forward window of dates (so the live tile
                          renders on demo day). The live tile reads live_sales,
                          not daily_sales, so this never affects revenue totals.
  * data/pdfs/9998/     — clone of every data/pdfs/9999/*.pdf so the /sales Z-PDF
                          previewer works (the copied daily_sales dates match).
  * user_branches       — demo-store@makoletchain.com scoped to EXACTLY
                          {9998, 9999}: any non-demo link is stripped, both demo
                          links added (INSERT OR IGNORE). With 2 branches the
                          manager gets the header switcher; access control
                          (login derives the allowed set from user_branches;
                          _get_branch_id validates membership) makes reaching any
                          real branch structurally impossible.

Re-runnable: re-running re-copies 9998's data and re-asserts the scope; it never
touches real branches (126/127/9xxx) or other users' rows.

Usage:
    python scripts/seed_demo_branch_2.py [path/to/makolet_chain.db]
Defaults to <repo>/db/makolet_chain.db (the prod/staging convention).
Prereq: scripts/seed_demo_branch.py must have been run first (creates branch
9999's data + the demo-store@ user). This script copies from 9999 and will exit
with a clear error if either is missing.
"""
import os
import sys
import shutil
import sqlite3
from datetime import date, timedelta

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB = os.path.join(REPO_ROOT, 'db', 'makolet_chain.db')
PDF_BASE = os.path.join(REPO_ROOT, 'data', 'pdfs')

DEMO_SRC_ID = 9999                       # copy data FROM here
DEMO2_ID = 9998                          # …TO here
DEMO2_NAME = 'מכולת הדגמה 2'
DEMO_EMAIL = 'demo-store@makoletchain.com'
DEMO_BRANCH_IDS = (9998, 9999)           # the ONLY branches the demo user may see

LIVE_AMOUNT = 4000.0                      # static fake "live now" revenue
LIVE_TXNS = 89                           # → believable basket ≈ ₪44.9
LIVE_WINDOW_DAYS = 14                    # today .. +13, so the tile shows on demo day

# Integration/credential columns force-NULLed so no agent can ever reach 9998.
# (9999 already has these NULL; we null defensively in case one was ever set.)
CREDENTIAL_COLS = [
    'aviv_user_id', 'aviv_password', 'bilboy_user', 'bilboy_pass', 'gmail_label',
    'franchise_supplier', 'aviv_branch_id', 'bilboy_branch_id',
    'iec_contract', 'iec_token', 'iec_user_id', 'iec_bp_number',
    'iec_contract_id', 'iec_last_sync_at',
]

# branch_id-keyed data tables copied 9999 → 9998 (order: employees before its
# pending matches, though 9999's pending rows have suggested_employee_id=NULL).
COPY_TABLES = [
    'daily_sales', 'goods_documents', 'fixed_expenses',
    'employees', 'employee_hours', 'employee_match_pending',
]


def cols(conn, table):
    return [r[1] for r in conn.execute(f'PRAGMA table_info({table})').fetchall()]


def seed_branch(conn):
    bcols = cols(conn, 'branches')
    if 'agents_enabled' not in bcols:
        sys.exit("[demo2] ERROR: branches.agents_enabled missing — run migrate.py "
                 "(migration 017) before seeding.")
    src = conn.execute('SELECT * FROM branches WHERE id = ?', (DEMO_SRC_ID,)).fetchone()
    if src is None:
        sys.exit(f"[demo2] ERROR: source branch {DEMO_SRC_ID} not found — run "
                 "scripts/seed_demo_branch.py first.")
    row = dict(src)
    row['id'] = DEMO2_ID
    row['name'] = DEMO2_NAME
    row['active'] = 1
    row['agents_enabled'] = 0            # kill-switch: every agent selector skips it
    for c in CREDENTIAL_COLS:
        if c in row:
            row[c] = None
    conn.execute('DELETE FROM branches WHERE id = ?', (DEMO2_ID,))
    keys = [k for k in row.keys() if k in bcols]
    conn.execute(
        f"INSERT INTO branches ({','.join(keys)}) VALUES ({','.join('?' for _ in keys)})",
        [row[k] for k in keys])
    print(f"[demo2] branch {DEMO2_ID} '{DEMO2_NAME}' (active=1, agents_enabled=0, "
          f"city='{row.get('city')}', NULL credentials)")


def copy_table(conn, table):
    """Copy all of branch 9999's rows in `table` to 9998 (id column dropped,
    branch_id rewritten). Schema-introspecting so prod schema drift is fine."""
    tcols = cols(conn, table)
    if 'branch_id' not in tcols:
        sys.exit(f"[demo2] ERROR: {table} has no branch_id column.")
    copy_cols = [c for c in tcols if c != 'id']
    collist = ','.join(copy_cols)
    bi = copy_cols.index('branch_id')
    conn.execute(f'DELETE FROM {table} WHERE branch_id = ?', (DEMO2_ID,))
    rows = conn.execute(
        f'SELECT {collist} FROM {table} WHERE branch_id = ?', (DEMO_SRC_ID,)).fetchall()
    ph = ','.join('?' for _ in copy_cols)
    for r in rows:
        vals = list(r)
        vals[bi] = DEMO2_ID
        conn.execute(f'INSERT INTO {table} ({collist}) VALUES ({ph})', vals)
    print(f"[demo2] {table}: copied {len(rows)} rows from {DEMO_SRC_ID}")
    return len(rows)


def seed_live(conn, today):
    """Static ₪4,000 'live now' tile for BOTH demo branches, across a forward
    date window (the tile reads live_sales WHERE date = today). UNIQUE(branch_id,
    date) → upsert is idempotent. No real branch writes here (9999/9998 only)."""
    lcols = cols(conn, 'live_sales')
    base = {'amount': LIVE_AMOUNT, 'transactions': LIVE_TXNS}
    inserted = 0
    for bid in DEMO_BRANCH_IDS:
        for i in range(LIVE_WINDOW_DAYS):
            d = (today + timedelta(days=i)).isoformat()
            row = {'branch_id': bid, 'date': d,
                   'amount': base['amount'], 'transactions': base['transactions'],
                   'last_updated': f'{d} 12:00:00'}
            keys = [k for k in row if k in lcols]
            updates = ','.join(f'{k}=excluded.{k}' for k in keys
                               if k not in ('branch_id', 'date'))
            conn.execute(
                f"INSERT INTO live_sales ({','.join(keys)}) "
                f"VALUES ({','.join('?' for _ in keys)}) "
                f"ON CONFLICT(branch_id, date) DO UPDATE SET {updates}",
                [row[k] for k in keys])
            inserted += 1
    print(f"[demo2] live_sales: ₪{LIVE_AMOUNT:,.0f} tile for branches "
          f"{DEMO_BRANCH_IDS} × {LIVE_WINDOW_DAYS} days "
          f"(from {today.isoformat()})")
    return inserted


def clone_pdfs():
    """Clone data/pdfs/9999/*.pdf → data/pdfs/9998/ so the Z-PDF previewer works
    (copied daily_sales dates match the filenames). Server-local file copy only —
    no scp/rsync, no deploy."""
    src_dir = os.path.join(PDF_BASE, str(DEMO_SRC_ID))
    dst_dir = os.path.join(PDF_BASE, str(DEMO2_ID))
    if not os.path.isdir(src_dir):
        print(f"[demo2] Z-PDF: SKIPPED — no source dir {src_dir}")
        return 0
    os.makedirs(dst_dir, exist_ok=True)
    n = 0
    for f in sorted(os.listdir(src_dir)):
        if f.lower().endswith('.pdf'):
            dst = os.path.join(dst_dir, f)
            if not os.path.exists(dst):
                shutil.copy2(os.path.join(src_dir, f), dst)
            n += 1
    print(f"[demo2] Z-PDF: {n} file(s) present in data/pdfs/{DEMO2_ID}/")
    return n


def scope_account(conn):
    """Scope demo-store@ to EXACTLY {9998, 9999}, and ensure no other user holds
    9998. Never touches a real branch link or another user's real branches."""
    urow = conn.execute('SELECT id FROM users WHERE LOWER(email) = LOWER(?)',
                        (DEMO_EMAIL,)).fetchone()
    if urow is None:
        sys.exit(f"[demo2] ERROR: demo user {DEMO_EMAIL} not found — run "
                 "scripts/seed_demo_branch.py first.")
    uid = urow[0]
    # Strip any NON-demo branch from the demo user (defensive: guarantees the
    # switcher can ONLY ever list the two demo stores).
    conn.execute(
        'DELETE FROM user_branches WHERE user_id = ? AND branch_id NOT IN (?, ?)',
        (uid, *DEMO_BRANCH_IDS))
    for bid in DEMO_BRANCH_IDS:
        conn.execute('INSERT OR IGNORE INTO user_branches (user_id, branch_id) VALUES (?, ?)',
                     (uid, bid))
    # 9998 must belong to NO ONE but the demo manager (mirror the 9999 safety).
    conn.execute('DELETE FROM user_branches WHERE branch_id = ? AND user_id != ?',
                 (DEMO2_ID, uid))
    scope = [r[0] for r in conn.execute(
        'SELECT branch_id FROM user_branches WHERE user_id = ? ORDER BY branch_id', (uid,))]
    print(f"[demo2] account: {DEMO_EMAIL} (id={uid}) -> branches {scope}")
    return uid, scope


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB
    if not os.path.exists(db_path):
        sys.exit(f"[demo2] ERROR: database not found at {db_path}")
    today = date.today()
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('BEGIN')
        seed_branch(conn)
        for t in COPY_TABLES:
            copy_table(conn, t)
        seed_live(conn, today)
        uid, scope = scope_account(conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    # File clone is outside the DB transaction (filesystem, not DB state).
    clone_pdfs()
    if scope != sorted(DEMO_BRANCH_IDS):
        sys.exit(f"[demo2] POST-CHECK FAILED: demo user scope is {scope}, "
                 f"expected {sorted(DEMO_BRANCH_IDS)}")
    print("[demo2] DONE — committed.")


if __name__ == '__main__':
    main()

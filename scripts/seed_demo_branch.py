#!/usr/bin/env python3
"""Seed the self-contained DEMO branch "מכולת הדגמה" (id 9999) for the sales demo.

A polished, fictional store with hand-placed data across all five pages, shown
via a manager-role account scoped to ONLY this branch. NO agent ever touches it:
the branch is created with NULL agent-config AND agents_enabled=0 (migration 017),
so every scheduled agent skips it structurally.

What it creates (all scoped to branch 9999 / the demo user — never touches a real
branch's rows):
  * branches            — id 9999, active=1, NULL aviv/bilboy/gmail config, agents_enabled=0
  * daily_sales         — Apr 1 – May 29 2026, ~₪15k/day with realistic weekday/
                          Friday/Shabbat variation, source='demo'
  * goods_documents     — ~2 dozen believable supplier invoices/delivery notes/credits
  * employee_hours      — copied from branch 126 (Apr+May) so /employees has real hours
  * employee_match_pending — one unresolved pending row per employee (the
                          "nothing matches yet" review state — no employees rows exist
                          for 9999, so they all show as new-employee matches to approve)
  * fixed_expenses      — rent / municipal tax / electricity / insurance (חודשי),
                          a fridge repair (חד פעמי), credit-card fees (% מהכנסות)
  * users + user_branches — manager 'משתמש הדגמה' (demo@makoletchain.com) -> 9999 only.
                          Password is a deliberately UNUSABLE placeholder; set it with
                          scripts/set_user_password.py (never hardcoded here).
  * data/pdfs/9999/     — one real branch-126 Z-report PDF copied in and wired to the
                          most recent demo daily_sales date so the /sales previewer works.

Idempotent + additive-only: every write is scoped to branch_id=9999 or the demo user.
Re-running DELETEs only 9999's data rows and reinserts them; the demo user is created
with INSERT OR IGNORE so a password you've already set is preserved. Real branches
(126/127/9xxx) are never read-for-delete, updated, or touched.

Schema-introspecting: prod's schema has drifted ahead of the repo baseline
(employee_hours uses total_hours/total_salary/source, etc.), so column lists are read
live via PRAGMA table_info rather than assumed.

Usage:
    python scripts/seed_demo_branch.py [path/to/makolet_chain.db]
Defaults to <repo>/db/makolet_chain.db (the prod/staging convention).
"""
import os
import sys
import random
import shutil
from datetime import date, timedelta

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

import sqlite3  # noqa: E402

DEFAULT_DB = os.path.join(REPO_ROOT, 'db', 'makolet_chain.db')

DEMO_BRANCH_ID = 9999
DEMO_NAME = 'מכולת הדגמה'
DEMO_CITY = 'הדגמה'
DEMO_EMAIL = 'demo@makoletchain.com'
DEMO_USER_NAME = 'משתמש הדגמה'
# Deliberately unusable hash: werkzeug.check_password_hash returns False for a
# malformed hash (no exception), so login is impossible until set_user_password.py
# is run. Keeps any real password out of code/git.
UNUSABLE_HASH = 'set-me-with-set_user_password.py'

SRC_EMPLOYEE_BRANCH = 126            # copy employee_hours from here
DEMO_MONTHS = ('2026-04', '2026-05')  # months we populate
PENDING_MONTH = '2026-05'            # month the /employees pending UI defaults to
RANGE_START = date(2026, 4, 1)
RANGE_END = date(2026, 5, 29)        # day before "today" (2026-05-30) so it looks live

RNG = random.Random(DEMO_BRANCH_ID)   # fixed seed -> reproducible re-runs


def cols(conn, table):
    return [r[1] for r in conn.execute(f'PRAGMA table_info({table})').fetchall()]


def insert_dict(conn, table, row, table_cols, or_ignore=False):
    """Insert dict `row`, keeping only keys that are real columns on `table`."""
    keys = [k for k in row if k in table_cols]
    verb = 'INSERT OR IGNORE INTO' if or_ignore else 'INSERT INTO'
    sql = f"{verb} {table} ({','.join(keys)}) VALUES ({','.join('?' for _ in keys)})"
    conn.execute(sql, [row[k] for k in keys])


# ─────────────────────────────────────────────────────────────────────────────
def seed_branch(conn):
    bcols = cols(conn, 'branches')
    if 'agents_enabled' not in bcols:
        sys.exit("[demo] ERROR: branches.agents_enabled missing — run migrate.py "
                 "(migration 017) before seeding.")
    # Wipe + recreate the branch row (scoped to 9999 only). All agent-config
    # columns are intentionally omitted -> they stay NULL.
    conn.execute('DELETE FROM branches WHERE id = ?', (DEMO_BRANCH_ID,))
    insert_dict(conn, 'branches', {
        'id': DEMO_BRANCH_ID, 'name': DEMO_NAME, 'city': DEMO_CITY,
        'active': 1, 'agents_enabled': 0,
    }, bcols)
    print(f"[demo] branch {DEMO_BRANCH_ID} '{DEMO_NAME}' (active=1, agents_enabled=0, NULL config)")


def seed_daily_sales(conn):
    dcols = cols(conn, 'daily_sales')
    conn.execute('DELETE FROM daily_sales WHERE branch_id = ?', (DEMO_BRANCH_ID,))
    n = 0
    d = RANGE_START
    while d <= RANGE_END:
        wd = d.weekday()  # Mon=0 .. Sun=6 ; Israel: Fri=4, Sat=5
        if wd == 4:        # Friday — short day
            amount = RNG.randrange(9000, 11500, 50)
        elif wd == 5:      # Saturday/Shabbat — evening only
            amount = RNG.randrange(4500, 7000, 50)
        else:              # Sun–Thu — full day around ₪15k
            amount = RNG.randrange(12500, 17500, 50)
        basket = RNG.uniform(46, 54)
        txns = int(round(amount / basket))
        insert_dict(conn, 'daily_sales', {
            'branch_id': DEMO_BRANCH_ID, 'date': d.isoformat(),
            'amount': amount, 'transactions': txns, 'source': 'demo',
        }, dcols)
        n += 1
        d += timedelta(days=1)
    print(f"[demo] daily_sales: {n} days ({RANGE_START} → {RANGE_END}), source='demo'")
    return n


SUPPLIERS = [
    'תנובה', 'שטראוס', 'אסם', 'קוקה קולה', 'יטבתה', 'עלית', 'תלמה',
    'מאפיית אנג׳ל', 'סנפרוסט', 'דיפלומט', 'פריניר', 'גד מוצרי חלב',
    'טמפו משקאות', 'נטו מ.ע.', 'סוגת', 'יוניליוור', 'מטרנה', 'וויסוצקי',
]


def seed_goods(conn):
    gcols = cols(conn, 'goods_documents')
    conn.execute('DELETE FROM goods_documents WHERE branch_id = ?', (DEMO_BRANCH_ID,))
    # ~2 dozen docs spread across Apr+May. doc_type: 3=invoice, 2=delivery, 4=credit.
    n = 0
    ref = 1001
    for i, supplier in enumerate(SUPPLIERS):
        # 1–2 docs per supplier
        for _ in range(RNG.choice([1, 1, 2])):
            month = RNG.choice([4, 5])
            day = RNG.randint(1, 28)
            doc_date = date(2026, month, day).isoformat()
            roll = RNG.random()
            doc_type = 3 if roll < 0.7 else (2 if roll < 0.92 else 4)
            amount = RNG.randrange(800, 9000, 10)
            if doc_type == 4:            # credit note — negative
                amount = -RNG.randrange(150, 1200, 10)
            insert_dict(conn, 'goods_documents', {
                'branch_id': DEMO_BRANCH_ID, 'doc_date': doc_date,
                'supplier': supplier, 'ref_number': f'DEMO-{ref}',
                'amount': amount, 'doc_type': doc_type,
            }, gcols)
            ref += 1
            n += 1
    print(f"[demo] goods_documents: {n} docs across Apr+May (types 3/2/4)")
    return n


def seed_employee_hours(conn):
    """Copy branch-126 employee_hours (Apr+May) into 9999, schema-agnostic."""
    ecols = cols(conn, 'employee_hours')
    copy_cols = [c for c in ecols if c != 'id']
    conn.execute('DELETE FROM employee_hours WHERE branch_id = ?', (DEMO_BRANCH_ID,))
    placeholders = ','.join('?' for _ in DEMO_MONTHS)
    src_rows = conn.execute(
        f"SELECT {','.join(copy_cols)} FROM employee_hours "
        f"WHERE branch_id = ? AND month IN ({placeholders})",
        (SRC_EMPLOYEE_BRANCH, *DEMO_MONTHS)
    ).fetchall()

    if src_rows:
        bi = copy_cols.index('branch_id')
        for r in src_rows:
            vals = list(r)
            vals[bi] = DEMO_BRANCH_ID
            conn.execute(
                f"INSERT INTO employee_hours ({','.join(copy_cols)}) "
                f"VALUES ({','.join('?' for _ in copy_cols)})", vals)
        print(f"[demo] employee_hours: copied {len(src_rows)} rows from branch "
              f"{SRC_EMPLOYEE_BRANCH} ({'+'.join(DEMO_MONTHS)})")
        return len(src_rows)

    # Fallback: branch 126 had no rows for those months — synthesize a few.
    hours_col = 'total_hours' if 'total_hours' in ecols else 'hours'
    rate_col = 'rate' if 'rate' in ecols else None
    fallback = [('דנה לוי', 168, 42), ('יוסי כהן', 152, 45),
                ('מאיה בר', 140, 40), ('אבי מזרחי', 96, 48)]
    n = 0
    for month in DEMO_MONTHS:
        for name, hrs, rate in fallback:
            row = {'branch_id': DEMO_BRANCH_ID, 'month': month,
                   'employee_name': name, hours_col: hrs, 'source': 'aviv_report'}
            if 'total_salary' in ecols:
                row['total_salary'] = hrs * rate
            if rate_col:
                row[rate_col] = rate
            insert_dict(conn, 'employee_hours', row, ecols)
            n += 1
    print(f"[demo] employee_hours: branch {SRC_EMPLOYEE_BRANCH} empty for "
          f"{DEMO_MONTHS} — synthesized {n} fallback rows")
    return n


def seed_pending(conn):
    """One unresolved pending match per employee for PENDING_MONTH — the
    'nothing matches yet' review state (no employees rows exist for 9999)."""
    ecols = cols(conn, 'employee_hours')
    pcols = cols(conn, 'employee_match_pending')
    hours_col = 'total_hours' if 'total_hours' in ecols else 'hours'
    sal_col = 'total_salary' if 'total_salary' in ecols else None
    rate_col = 'rate' if 'rate' in ecols else None

    conn.execute('DELETE FROM employee_match_pending WHERE branch_id = ?', (DEMO_BRANCH_ID,))
    rows = conn.execute(
        f"SELECT employee_name, {hours_col} AS hrs"
        + (f", {sal_col} AS sal" if sal_col else "")
        + (f", {rate_col} AS rate" if rate_col else "")
        + " FROM employee_hours WHERE branch_id = ? AND month = ?",
        (DEMO_BRANCH_ID, PENDING_MONTH)
    ).fetchall()

    n = 0
    for r in rows:
        name = r['employee_name']
        hrs = r['hrs'] or 0
        if sal_col and r['sal']:
            salary = r['sal']
        elif rate_col and r['rate']:
            salary = hrs * r['rate']
        else:
            salary = hrs * 45
        insert_dict(conn, 'employee_match_pending', {
            'branch_id': DEMO_BRANCH_ID, 'month': PENDING_MONTH, 'csv_name': name,
            'suggested_employee_id': None, 'confidence': 'low',
            'hours': hrs, 'salary': round(salary, 2),
            'source': 'aviv_report', 'is_new_employee': 1, 'resolved': 0,
        }, pcols)
        n += 1
    print(f"[demo] employee_match_pending: {n} unresolved rows for {PENDING_MONTH} "
          f"(unmatched -> review UI)")
    return n


def seed_fixed_expenses(conn):
    fcols = cols(conn, 'fixed_expenses')
    conn.execute('DELETE FROM fixed_expenses WHERE branch_id = ?', (DEMO_BRANCH_ID,))
    # expense_type: 'monthly' | 'onetime'. percent encoded via pct_value>0 + amount=0.
    monthly = [('שכר דירה', 12000), ('ארנונה', 2800), ('חשמל', 3400), ('ביטוח עסק', 950)]
    n = 0
    for month in DEMO_MONTHS:
        for name, amt in monthly:
            insert_dict(conn, 'fixed_expenses', {
                'branch_id': DEMO_BRANCH_ID, 'month': month, 'name': name,
                'amount': amt, 'expense_type': 'monthly', 'pct_value': None,
            }, fcols)
            n += 1
        # % מהכנסות — credit-card clearing fees
        insert_dict(conn, 'fixed_expenses', {
            'branch_id': DEMO_BRANCH_ID, 'month': month, 'name': 'עמלות סליקת אשראי',
            'amount': 0, 'expense_type': 'monthly', 'pct_value': 1.5,
        }, fcols)
        n += 1
    # חד פעמי — one-time, April only
    insert_dict(conn, 'fixed_expenses', {
        'branch_id': DEMO_BRANCH_ID, 'month': '2026-04', 'name': 'שיפוץ מקרר תעשייתי',
        'amount': 4500, 'expense_type': 'onetime', 'pct_value': None,
    }, fcols)
    n += 1
    print(f"[demo] fixed_expenses: {n} rows (חודשי + % מהכנסות + חד פעמי)")
    return n


def seed_account(conn):
    ucols = cols(conn, 'users')
    # Create the demo manager iff absent (preserve any password already set).
    insert_dict(conn, 'users', {
        'name': DEMO_USER_NAME, 'email': DEMO_EMAIL,
        'password_hash': UNUSABLE_HASH, 'role': 'manager', 'active': 1,
    }, ucols, or_ignore=True)
    uid = conn.execute('SELECT id FROM users WHERE LOWER(email)=LOWER(?)',
                       (DEMO_EMAIL,)).fetchone()[0]
    # Scope strictly to the demo branch: remove any prior links for THIS user
    # only, then link 9999. (Never touches other users' user_branches.)
    conn.execute('DELETE FROM user_branches WHERE user_id = ?', (uid,))
    conn.execute('INSERT OR IGNORE INTO user_branches (user_id, branch_id) VALUES (?, ?)',
                 (uid, DEMO_BRANCH_ID))
    has_pw = conn.execute('SELECT password_hash FROM users WHERE id=?', (uid,)).fetchone()[0]
    state = 'password NOT set (placeholder)' if has_pw == UNUSABLE_HASH else 'password already set'
    print(f"[demo] account: manager '{DEMO_USER_NAME}' <{DEMO_EMAIL}> id={uid} "
          f"-> branch {DEMO_BRANCH_ID} only | {state}")
    return uid


def copy_zpdf(conn):
    """Copy one real branch-126 Z PDF into the demo branch and wire it to the
    most recent demo daily_sales date so the /sales previewer works."""
    src_dir = os.path.join(REPO_ROOT, 'data', 'pdfs', str(SRC_EMPLOYEE_BRANCH))
    dst_dir = os.path.join(REPO_ROOT, 'data', 'pdfs', str(DEMO_BRANCH_ID))
    if not os.path.isdir(src_dir):
        print(f"[demo] Z-PDF: SKIPPED — no source dir {src_dir} (run on prod where 126 PDFs live)")
        return False
    pdfs = sorted(f for f in os.listdir(src_dir) if f.startswith('z_') and f.endswith('.pdf'))
    if not pdfs:
        print(f"[demo] Z-PDF: SKIPPED — no z_*.pdf in {src_dir}")
        return False
    latest_date = conn.execute(
        'SELECT MAX(date) FROM daily_sales WHERE branch_id=?', (DEMO_BRANCH_ID,)).fetchone()[0]
    os.makedirs(dst_dir, exist_ok=True)
    src = os.path.join(src_dir, pdfs[-1])
    dst = os.path.join(dst_dir, f'z_{latest_date}.pdf')
    shutil.copy2(src, dst)
    print(f"[demo] Z-PDF: copied {pdfs[-1]} -> data/pdfs/{DEMO_BRANCH_ID}/z_{latest_date}.pdf")
    return True


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB
    if not os.path.exists(db_path):
        sys.exit(f"[demo] ERROR: database not found at {db_path}")
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('BEGIN')
        seed_branch(conn)
        seed_daily_sales(conn)
        seed_goods(conn)
        seed_employee_hours(conn)
        seed_pending(conn)
        seed_fixed_expenses(conn)
        seed_account(conn)
        copy_zpdf(conn)
        conn.commit()
        print("[demo] DONE — committed.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()

"""Builds the per-branch supplier_roster (migration 029).

The /goods תקציב page lists a branch's FULL supplier roster so a manager can
budget any supplier before ordering this month. The roster = distinct BilBoy
goods supplier names over the PRIOR 2 calendar months (run in June → April +
May), with two deliberate rules:

  • IGNORE the visible_from display floor. New chain stores have a forward floor
    (display-only), but their BilBoy goods exist pre-floor — so their roster must
    still include those prior-2-month suppliers. We query goods_documents
    directly, never through _month_below_floor.
  • EXCLUDE the branch's franchise supplier (branches.franchise_supplier, the
    "זיכיונות" supplier) — it is locked and never budgeted.

Replace-on-refresh: each run deletes the branch's roster rows and reinserts.
Scheduled monthly on the 1st (IL) via scheduler.py + a one-time build script
(scripts/build_supplier_roster.py).
"""
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from utils.text import clean_supplier_name

log = logging.getLogger(__name__)

IL_TZ = ZoneInfo('Asia/Jerusalem')
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')


def prior_two_months(now: datetime | None = None) -> tuple[str, str]:
    """Return the (older, newer) prior-2-calendar-month labels as 'YYYY-MM'
    relative to `now` (Israel time). E.g. now in June → ('2026-04', '2026-05')."""
    now = now or datetime.now(IL_TZ)
    first = now.replace(day=1)
    newer_last = first - timedelta(days=1)                 # last day of prev month
    older_last = newer_last.replace(day=1) - timedelta(days=1)  # last day 2 months ago
    return older_last.strftime('%Y-%m'), newer_last.strftime('%Y-%m')


def build_for_branch(conn, branch_id: int, now: datetime | None = None) -> int:
    """Replace branch_id's roster with distinct goods suppliers from the prior 2
    calendar months. NO visible_from floor; franchise supplier + blank/— names
    excluded. Returns the number of roster rows written."""
    older, newer = prior_two_months(now)

    frow = conn.execute(
        'SELECT franchise_supplier FROM branches WHERE id = ?', (branch_id,)
    ).fetchone()
    franchise = ''
    if frow is not None:
        franchise = (frow[0] if not hasattr(frow, 'keys') else frow['franchise_supplier']) or ''
    franchise = clean_supplier_name(franchise)

    # Direct goods query — deliberately NOT floor-guarded (see module docstring).
    rows = conn.execute(
        "SELECT DISTINCT supplier FROM goods_documents "
        "WHERE branch_id = ? AND strftime('%Y-%m', doc_date) IN (?, ?) "
        "AND supplier IS NOT NULL AND TRIM(supplier) NOT IN ('', '—')",
        (branch_id, older, newer)
    ).fetchall()

    names = []
    seen = set()
    for r in rows:
        s = clean_supplier_name(r[0] if not hasattr(r, 'keys') else r['supplier'])
        if not s or s == '—':
            continue
        if franchise and s == franchise:        # locked franchise supplier
            continue
        if s in seen:
            continue
        seen.add(s)
        names.append(s)

    conn.execute('DELETE FROM supplier_roster WHERE branch_id = ?', (branch_id,))
    conn.executemany(
        "INSERT OR IGNORE INTO supplier_roster (branch_id, supplier_name, updated_at) "
        "VALUES (?, ?, datetime('now'))",
        [(branch_id, n) for n in names])
    conn.commit()
    return len(names)


def build_all(db_path: str | None = None, now: datetime | None = None) -> dict:
    """Rebuild the roster for every active branch. Returns {branch_id: count}
    (count -1 on a per-branch failure — one branch never aborts the loop)."""
    conn = sqlite3.connect(db_path or DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        bids = [r['id'] for r in conn.execute(
            'SELECT id FROM branches WHERE active = 1 ORDER BY id').fetchall()]
        out = {}
        for bid in bids:
            try:
                out[bid] = build_for_branch(conn, bid, now=now)
            except Exception as e:
                log.error('supplier roster build failed for branch %d: %s', bid, e)
                out[bid] = -1
        return out
    finally:
        conn.close()

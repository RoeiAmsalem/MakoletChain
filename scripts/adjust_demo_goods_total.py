#!/usr/bin/env python3
"""Scale the demo branch (9999) goods_documents amounts in place to an exact total.

Updates the EXISTING rows only (no inserts/deletes) — preserves ids, ref numbers,
supplier names, dates, and invoice/delivery/credit badges. Amounts are scaled
proportionally to sum to exactly GOODS_TARGET_TOTAL (₪250,000), with the rounding
remainder absorbed into the largest invoice. Uses the same scale_amounts_to_total
helper as the seed, so re-running the seed reproduces the same total.

Idempotent: re-running on a sum that's already at target scales by factor 1.0.
Scoped strictly to branch 9999 — never touches a real branch.

    cd /opt/makolet-chain && venv/bin/python scripts/adjust_demo_goods_total.py
"""
import os
import sys
import sqlite3

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, 'scripts'))

from seed_demo_branch import (  # noqa: E402
    DEFAULT_DB, DEMO_BRANCH_ID, GOODS_TARGET_TOTAL, scale_amounts_to_total,
)


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB
    if not os.path.exists(db_path):
        sys.exit(f"[adjust] ERROR: database not found at {db_path}")
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT id, amount, doc_type FROM goods_documents "
            "WHERE branch_id = ? ORDER BY id", (DEMO_BRANCH_ID,)
        ).fetchall()
        if not rows:
            sys.exit(f"[adjust] ERROR: no goods_documents for branch {DEMO_BRANCH_ID}")
        before = sum(r['amount'] for r in rows)
        new_amts = scale_amounts_to_total(
            [r['amount'] for r in rows], [r['doc_type'] for r in rows],
            GOODS_TARGET_TOTAL)
        conn.execute('BEGIN')
        for r, a in zip(rows, new_amts):
            conn.execute(
                "UPDATE goods_documents SET amount = ? WHERE id = ? AND branch_id = ?",
                (a, r['id'], DEMO_BRANCH_ID))
        conn.commit()
        after = conn.execute(
            "SELECT COUNT(*) n, SUM(amount) s FROM goods_documents WHERE branch_id = ?",
            (DEMO_BRANCH_ID,)).fetchone()
        print(f"[adjust] branch {DEMO_BRANCH_ID}: {after['n']} docs, "
              f"₪{before:,.0f} -> ₪{after['s']:,.0f} (target ₪{GOODS_TARGET_TOTAL:,})")
        if after['s'] != GOODS_TARGET_TOTAL:
            sys.exit(f"[adjust] ERROR: total {after['s']} != target {GOODS_TARGET_TOTAL}")
        print("[adjust] DONE — exact.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()

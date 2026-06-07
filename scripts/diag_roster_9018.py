"""READ-ONLY diagnostic: is 9018's budget-roster genuinely complete or undercounting?

Compares 9018 (דפנה) vs 126 (אינשטיין):
  - per-month distinct goods suppliers (last 4 months), franchise (זיכיונות) excluded
  - earliest/latest goods date + per-month doc-count coverage
  - the roster BUILD-window query result vs the live supplier_roster table count
  - the live-derive query is byte-identical to the build query, so we run that one
    SELECT and label it as representing BOTH paths

Opens the DB read-only (mode=ro). No writes, ever.

Usage:  venv/bin/python scripts/diag_roster_9018.py
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from agents.supplier_roster import prior_two_months  # read-only helper

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
BRANCHES = [9018, 126]
MONTHS = ['2026-03', '2026-04', '2026-05', '2026-06']

# Exact SELECT from build_for_branch / prior_two_month_suppliers (read-only;
# the DELETE+INSERT of the builder is deliberately NOT run here).
BUILD_SELECT = (
    "SELECT DISTINCT supplier FROM goods_documents "
    "WHERE branch_id = ? AND strftime('%Y-%m', doc_date) IN (?, ?) "
    "AND supplier IS NOT NULL AND TRIM(supplier) NOT IN ('', '—')"
)


def conn_ro():
    uri = 'file:' + os.path.abspath(DB_PATH) + '?mode=ro'
    c = sqlite3.connect(uri, uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def main():
    older, newer = prior_two_months()
    c = conn_ro()
    print(f"prior_two_months() window (June run) = {older} + {newer}")
    print("=" * 64)

    for bid in BRANCHES:
        b = c.execute("SELECT name, franchise_supplier, visible_from FROM branches WHERE id=?",
                      (bid,)).fetchone()
        franchise = (b['franchise_supplier'] or '').strip() if b else ''
        print(f"\n### branch {bid} — {b['name'] if b else '?'}")
        print(f"    franchise_supplier={franchise!r}  visible_from={b['visible_from'] if b else None!r}")

        cov = c.execute(
            "SELECT MIN(doc_date) mn, MAX(doc_date) mx, COUNT(*) n FROM goods_documents "
            "WHERE branch_id=?", (bid,)).fetchone()
        print(f"    goods_documents: {cov['n']} docs, earliest={cov['mn']} latest={cov['mx']}")

        # per-month distinct suppliers (franchise excluded) + doc count
        print(f"    {'month':<9} {'distinct_suppliers':>18} {'docs':>7}")
        for m in MONTHS:
            params = [bid, m]
            fclause = ''
            if franchise:
                fclause = ' AND TRIM(supplier) <> ?'
                params.append(franchise)
            sup = c.execute(
                "SELECT COUNT(DISTINCT supplier) k FROM goods_documents "
                "WHERE branch_id=? AND strftime('%Y-%m', doc_date)=? "
                "AND supplier IS NOT NULL AND TRIM(supplier) NOT IN ('', '—')" + fclause,
                params).fetchone()['k']
            docs = c.execute(
                "SELECT COUNT(*) k FROM goods_documents "
                "WHERE branch_id=? AND strftime('%Y-%m', doc_date)=?",
                (bid, m)).fetchone()['k']
            print(f"    {m:<9} {sup:>18} {docs:>7}")

        # build-window query (== live-derive query), franchise excluded in py
        rows = c.execute(BUILD_SELECT, (bid, older, newer)).fetchall()
        names = set()
        for r in rows:
            s = (r['supplier'] or '').strip()
            if s and s != '—' and not (franchise and s == franchise):
                names.add(s)
        table_n = c.execute("SELECT COUNT(*) k FROM supplier_roster WHERE branch_id=?",
                            (bid,)).fetchone()['k']
        print(f"    build/live-derive SELECT (Apr+May, franchise-excluded) = {len(names)}")
        print(f"    supplier_roster TABLE rows (built on prod)             = {table_n}")
        print(f"    match: {'YES' if len(names) == table_n else 'NO — differ'}")

    c.close()


if __name__ == '__main__':
    main()

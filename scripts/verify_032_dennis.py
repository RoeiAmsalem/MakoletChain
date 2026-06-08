# -*- coding: utf-8 -*-
"""READ-ONLY post-deploy verification for migration 032 (supplier-name cleanup).

Proves on PROD:
  1. goods_documents row count + distinct ref_number UNCHANGED by 032 (pass the
     before-values as args to assert count-in == count-out).
  2. dirty supplier rows == 0 across goods_documents / supplier_budgets /
     supplier_roster.
  3. 9018 מרינה shows ONCE in the budget list with the correct merged spend.
  4. RECONCILIATION 9018 + 9015: Σ per-supplier mtd_spend == /goods pre-VAT MTD (Δ0).
  5. 127 budget list contains 'תה ויסוצקי (ישראל) בע"מ' (so typing "ויסוצקי" filters to it).

Opens the prod DB mode=ro. No writes. Imports app._goal_data/_goods_doc_context
(both take an explicit db connection — the ro one here).

Usage:  venv/bin/python scripts/verify_032_dennis.py [--rows-before N --ref-before N]
"""
import argparse
import os
import sqlite3
import sys

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import app as app_module                                   # noqa: E402
from app import _goal_data, _goods_doc_context, _now_il    # noqa: E402

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
CLEAN = ("TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE({c},CHAR(13),' '),"
         "CHAR(10),' '),CHAR(9),' '),'  ',' '),'  ',' '))")


def conn_ro():
    c = sqlite3.connect('file:' + os.path.abspath(DB_PATH) + '?mode=ro', uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def dirty_count(c, table, col):
    q = (f"SELECT COUNT(*) k FROM {table} WHERE {col} IS NOT NULL "
         f"AND {col} <> " + CLEAN.format(c=col))
    return c.execute(q).fetchone()['k']


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--rows-before', type=int, default=None)
    ap.add_argument('--ref-before', type=int, default=None)
    a = ap.parse_args()

    c = conn_ro()
    month = _now_il().strftime('%Y-%m')
    print(f"verify 032 — month={month}")

    # 1. counts
    rows = c.execute("SELECT COUNT(*) k FROM goods_documents").fetchone()['k']
    refs = c.execute("SELECT COUNT(DISTINCT ref_number) k FROM goods_documents").fetchone()['k']
    rmsg = '' if a.rows_before is None else (
        f"  (before {a.rows_before} → {'UNCHANGED ✓' if a.rows_before == rows else 'CHANGED ✗'})")
    fmsg = '' if a.ref_before is None else (
        f"  (before {a.ref_before} → {'UNCHANGED ✓' if a.ref_before == refs else 'CHANGED ✗'})")
    print(f"1. goods_documents rows = {rows}{rmsg}")
    print(f"   distinct ref_number  = {refs}{fmsg}")

    # 2. dirty == 0
    dg = dirty_count(c, 'goods_documents', 'supplier')
    db_ = dirty_count(c, 'supplier_budgets', 'supplier_name')
    dr = dirty_count(c, 'supplier_roster', 'supplier_name')
    ok2 = (dg == db_ == dr == 0)
    print(f"2. dirty rows — goods={dg} budgets={db_} roster={dr}  "
          f"{'✓ all 0' if ok2 else '✗ NONZERO'}")

    # 3. 9018 מרינה once + merged spend
    g = _goal_data(9018, c)
    marina = [s for s in g['suppliers'] if 'מרינה' in s['supplier_name']]
    print(f"3. 9018 מרינה rows in budget list = {len(marina)} "
          f"{'✓ once' if len(marina) == 1 else '✗'}")
    for s in marina:
        print(f"   {s['supplier_name']!r}  mtd_spend=₪{s['mtd_spend']:,.2f}  "
              f"budget={s['budget']}")

    # 4. reconciliation 9018 + 9015
    print("4. reconciliation (Σ per-supplier mtd_spend vs /goods pre-VAT MTD):")
    for bid in (9018, 9015):
        gd = _goal_data(bid, c)
        sum_mtd = round(sum(s['mtd_spend'] for s in gd['suppliers']), 2)
        goods_total = round(_goods_doc_context(bid, month, c)['total_before_vat'], 2)
        delta = round(sum_mtd - goods_total, 2)
        print(f"   {bid}: Σmtd=₪{sum_mtd:,.2f}  /goods=₪{goods_total:,.2f}  "
              f"Δ={delta}  {'✓' if delta == 0 else '✗'}")

    # 5. 127 ויסוצקי findable
    g127 = _goal_data(127, c)
    vis = [s['supplier_name'] for s in g127['suppliers'] if 'ויסוצקי' in s['supplier_name']]
    print(f"5. 127 budget list — names containing 'ויסוצקי': {vis}  "
          f"{'✓ search hits' if vis else '✗ none'}")
    c.close()


if __name__ == '__main__':
    main()

"""Prove the budget feature is incl-VAT and the two views agree.

תקציב view  = _goal_data: הוצאה = mtd_spend, יתרה = remaining.
"לפי ספק"    = the doc group headline (incl-VAT SUM(amount)) + an annotation that
              reuses _goal_data.remaining. So per supplier:
    "לפי ספק" headline spend  == _goal_data mtd_spend   (both incl-VAT amount)
    "לפי ספק" יתרה            == _goal_data remaining    (same source)
We also reconcile Σ incl-VAT spend == Σ incl-VAT goods (SUM(amount)), and show
/goods's pre-VAT total is a DIFFERENT (untouched) number.

Usage:  python3 scripts/verify_budget_vat.py
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module
from app import _goal_data, _goods_doc_context
from utils.text import clean_supplier_name

MONTH = '2026-06'   # _goal_data uses the current IL month (June on staging)


def _conn():
    c = sqlite3.connect(app_module.DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


for branch in (9018, 9015):
    c = _conn()
    goal = _goal_data(branch, c)
    goods = _goods_doc_context(branch, MONTH, c)
    c.close()

    # "לפי ספק" headline per supplier = incl-VAT group total (SUM amount).
    incl = {clean_supplier_name(g['supplier']): round(g['total'], 2) for g in goods['groups']}
    pre = {clean_supplier_name(g['supplier']): round(g['total_before_vat'], 2) for g in goods['groups']}
    gm = {s['supplier_name']: s for s in goal['suppliers']}

    sum_mtd = round(sum(s['mtd_spend'] for s in goal['suppliers']), 2)
    goods_incl = round(goods['total'], 2)
    goods_pre = round(goods['total_before_vat'], 2)
    print(f"\n=== branch {branch} {MONTH} ===")
    print(f"RECONCILE incl-VAT: Σ per-supplier mtd_spend={sum_mtd} | "
          f"Σ goods SUM(amount)={goods_incl} | Δ={round(sum_mtd-goods_incl,2)}")
    print(f"/goods pre-VAT total (UNTOUCHED, different base) = {goods_pre}")

    # per-supplier side-by-side: budgeted first, then highlight נסטלה.
    rows = sorted(goal['suppliers'],
                  key=lambda s: (s['budget'] is None, -s['mtd_spend']))
    picks = [s for s in rows if s['budget'] is not None][:5]
    nestle = [s for s in goal['suppliers'] if 'נסטלה' in s['supplier_name']]
    for n in nestle:
        if n not in picks:
            picks.append(n)
    print("  supplier | לפי-ספק spend (incl) | תקציב הוצאה (mtd) | match | "
          "budget | יתרה | budget−incl | match")
    for s in picks:
        nm = s['supplier_name']
        lpc = incl.get(nm)
        m1 = '✓' if lpc == s['mtd_spend'] else '✗'
        bm = (round(s['budget'] - (lpc if lpc is not None else 0), 2)
              if s['budget'] is not None else None)
        m2 = '✓' if (s['remaining'] is not None and bm == s['remaining']) else '✗'
        print(f"   {nm} | {lpc} | {s['mtd_spend']} | {m1} | "
              f"{s['budget']} | {s['remaining']} | {bm} | {m2}")

    # produce/exempt example: the supplier with incl-VAT closest to pre-VAT
    # (ratio ~1.0 = VAT-exempt, spend did NOT jump ~18%).
    ratios = sorted(((round(incl[nm] / pre[nm], 4), nm) for nm in incl if pre.get(nm)),
                    key=lambda x: x[0])
    if ratios:
        r, nm = ratios[0]
        kind = 'exempt — no VAT jump' if r < 1.02 else f'carries VAT (~{(r-1)*100:.0f}%)'
        print(f"  lowest VAT-ratio supplier: {nm} incl={incl[nm]} pre={pre[nm]} "
              f"ratio={r} ({kind})")

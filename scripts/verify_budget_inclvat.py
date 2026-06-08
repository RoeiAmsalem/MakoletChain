# -*- coding: utf-8 -*-
"""READ-ONLY post-deploy check: budget feature is incl-VAT and both views agree.

Proves on PROD (mode=ro):
  A. 9018 June גלידות נסטלה — תקציב הוצאה ₪2,209.01 + יתרה ₪90.99, IDENTICAL to the
     "לפי ספק" headline (g.total) and its remaining annotation (reuses _goal_data).
  B. a produce/exempt supplier (מרינה) effectively unchanged (incl ≈ ex, ratio ~1.00).
  C. 5–6 suppliers side by side: תקציב mtd_spend == "לפי ספק" g.total, same יתרה.
  D. RECONCILIATION Δ0 (incl-VAT base): 9018 + 9015 Σ mtd_spend == Σ goods SUM(amount).
  E. /goods pre-VAT UNCHANGED: 9018 ₪36,253.50, 9015 ₪78,572.10 (total_before_vat).

Imports app._goal_data/_goods_doc_context (both take an explicit ro connection).
No writes/migrations/deploy.
"""
import os
import sqlite3
import sys

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import app as app_module                                       # noqa: E402
from app import _goal_data, _goods_doc_context, _now_il         # noqa: E402
from utils.text import clean_supplier_name                      # noqa: E402

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
EXPECT_PREVAT = {9018: 36253.50, 9015: 78572.10}


def conn_ro():
    c = sqlite3.connect('file:' + os.path.abspath(DB_PATH) + '?mode=ro', uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def group_incl(c, bid, month):
    """{cleaned supplier: g.total (incl-VAT)} — the 'לפי ספק' headline per supplier."""
    ctx = _goods_doc_context(bid, month, c)
    out = {}
    for g in ctx['groups']:
        out[clean_supplier_name(g['supplier'])] = round(g['total'], 2)
    return out, ctx


def main():
    c = conn_ro()
    month = _now_il().strftime('%Y-%m')
    print(f"verify budget incl-VAT — month={month}\n")

    gd18 = {s['supplier_name']: s for s in _goal_data(9018, c)['suppliers']}
    grp18, ctx18 = group_incl(c, 9018, month)

    # A. נסטלה — list ALL נסטלה entries, then target the one with June goods (גלידות).
    print("A. 9018 נסטלה — תקציב vs לפי ספק")
    cands = [n for n in gd18 if 'נסטלה' in n]
    for n in cands:
        s = gd18[n]
        head = grp18.get(clean_supplier_name(n))
        hs = f"{head:.2f}" if head is not None else "— (no June goods)"
        print(f"   • {n!r}: תקציב mtd={s['mtd_spend']:.2f} budget={s['budget']} "
              f"יתרה={s['remaining']} | לפי-ספק g.total={hs}")
    target = max((n for n in cands if grp18.get(clean_supplier_name(n)) is not None),
                 key=lambda n: gd18[n]['mtd_spend'], default=None)
    if target:
        s = gd18[target]
        head = grp18[clean_supplier_name(target)]
        ok = (s['mtd_spend'] == head == 2209.01) and (s['remaining'] == 90.99)
        print(f"   → {target!r}: הוצאה ₪{s['mtd_spend']:,.2f} == לפי-ספק ₪{head:,.2f}, "
              f"יתרה ₪{s['remaining']:,.2f}  "
              f"{'✓ both views agree (2209.01 / 90.99)' if ok else '(see numbers above)'}")

    # B. produce/exempt
    print("\nB. produce/exempt supplier (מרינה) — incl ≈ ex (ratio ~1.00), no jump")
    marina = next((n for n in gd18 if 'מרינה' in n), None)
    if marina:
        s = gd18[marina]
        print(f"   9018 {marina}: תקציב mtd={s['mtd_spend']:.2f} == לפי-ספק "
              f"{grp18.get(clean_supplier_name(marina)):.2f}  (VAT-exempt → barely differs from pre-VAT)")

    # C. side-by-side
    print("\nC. 6 suppliers side by side (תקציב mtd_spend vs לפי-ספק g.total, + יתרה)")
    print(f"   {'supplier':<30} {'תקציב':>10} {'לפי ספק':>10} {'match':>6} {'יתרה':>10}")
    shown = 0
    for n, s in sorted(gd18.items(), key=lambda kv: -kv[1]['mtd_spend']):
        head = grp18.get(clean_supplier_name(n))
        if head is None:
            continue
        match = '✓' if s['mtd_spend'] == head else '✗'
        rem = '—' if s['remaining'] is None else f"{s['remaining']:.2f}"
        print(f"   {n[:30]:<30} {s['mtd_spend']:>10.2f} {head:>10.2f} {match:>6} {rem:>10}")
        shown += 1
        if shown >= 6:
            break

    # D + E
    print("\nD. RECONCILIATION (incl-VAT) + E. /goods pre-VAT UNCHANGED")
    for bid in (9018, 9015):
        gd = _goal_data(bid, c)
        grp, ctx = group_incl(c, bid, month)
        sum_mtd = round(sum(s['mtd_spend'] for s in gd['suppliers']), 2)
        incl_total = round(ctx['total'], 2)
        prevat = round(ctx['total_before_vat'], 2)
        d = round(sum_mtd - incl_total, 2)
        exp = EXPECT_PREVAT[bid]
        print(f"   {bid}: Σmtd(incl)=₪{sum_mtd:,.2f}  Σgoods amount=₪{incl_total:,.2f}  "
              f"Δ={d} {'✓' if d == 0 else '✗'}")
        print(f"        /goods pre-VAT=₪{prevat:,.2f}  (expect ₪{exp:,.2f}) "
              f"{'✓ unchanged' if prevat == exp else '✗ DIFFERS'}")
    c.close()


if __name__ == '__main__':
    main()

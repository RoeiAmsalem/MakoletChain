# -*- coding: utf-8 -*-
"""READ-ONLY: confirm the תקציב-vs-"לפי ספק" per-supplier spend mismatch is a
VAT-base difference (ex-VAT vs incl-VAT), and measure its spread chain-wide.

Two /goods views sum DIFFERENT columns of the SAME goods_documents rows:

  • "לפי ספק" (grouped) headline per supplier  → g.total  = Σ amount (INCL-VAT)
      app.py goods() :921   g['total'] += d['amount']
      templates/goods.html:365   <span class="sg-total">₪ {{ g.total }}</span>

  • תקציב (budget) הוצאה per supplier          → cur_spend = Σ amount_before_vat (EX-VAT)
      app.py _goods_doc_context :1066  g['total_before_vat'] += d['amount_before_vat']
      app.py _goal_data :1132          cur_spend[k] += g['total_before_vat']
      where amount_before_vat (app.py :1047-1049 / :902-903):
          round(total_without_vat,2) if total_without_vat else round(amount/1.17,2)

  amount             = BilBoy totalWithVat   → VAT-INCLUSIVE   (bilboy.py write)
  total_without_vat  = BilBoy authoritative pre-VAT (migration 024), else amount/1.17

So for a taxable supplier incl/ex ≈ 1.18; for a VAT-exempt one (fresh produce) ≈ 1.00.
The תקציב יתרה = budget − EX-VAT spend, so it disagrees with the incl-VAT headline a
manager sees in "לפי ספק".

100% READ-ONLY (SELECT only, mode=ro). No writes/migrations/deploy. Does not change
any view or sync logic.
"""
import argparse
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

HERE = os.path.dirname(__file__)
DEFAULT_DB = os.path.join(HERE, '..', 'db', 'makolet_chain.db')
IL = ZoneInfo('Asia/Jerusalem')
FOCUS = [9018, 9015, 126]
NESTLE_LIKE = 'נסטלה'


def conn_ro(path):
    c = sqlite3.connect('file:' + os.path.abspath(path) + '?mode=ro', uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def amount_before_vat(row):
    """Exact replica of app.py's per-row pre-VAT derivation."""
    twv = row['total_without_vat']
    amt = row['amount'] or 0.0
    return round(twv, 2) if twv else round(amt / 1.17, 2)


def per_supplier(c, branch_id, month):
    """Return {supplier: (incl_vat_total, ex_vat_total, docs)} replicating BOTH
    aggregations over the SAME rows the two views read (all goods_documents for
    branch+month — status/type already filtered at sync time)."""
    rows = c.execute(
        "SELECT supplier, amount, total_without_vat, doc_type "
        "FROM goods_documents WHERE branch_id=? AND strftime('%Y-%m', doc_date)=?",
        (branch_id, month)).fetchall()
    agg = defaultdict(lambda: [0.0, 0.0, 0])     # incl, ex, docs
    for r in rows:
        s = r['supplier'] or '—'
        agg[s][0] += (r['amount'] or 0.0)        # g.total  (incl-VAT)
        agg[s][1] += amount_before_vat(r)        # total_before_vat (ex-VAT)
        agg[s][2] += 1
    return {s: (round(v[0], 2), round(v[1], 2), v[2]) for s, v in agg.items()}


def classify(ratio):
    if ratio is None:
        return '—'
    if 1.16 <= ratio <= 1.205:
        return 'VAT (taxable ~18%)'
    if 0.995 <= ratio <= 1.005:
        return 'exempt (~0%)'
    if 1.16 <= ratio < 1.175:
        return 'VAT (/1.17 fallback)'
    return '⚠ OTHER'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default=os.environ.get('PROD_DB', DEFAULT_DB))
    ap.add_argument('--month', default=datetime.now(IL).strftime('%Y-%m'))
    a = ap.parse_args()
    c = conn_ro(a.db)
    month = a.month
    print(f"DB={os.path.abspath(a.db)} (mode=ro)  month={month}\n")

    # ── 1. CODE PATHS (see module docstring; echoed for the report) ──
    print("=" * 80)
    print("1. CODE PATHS — exact column each view sums")
    print("=" * 80)
    print("  לפי ספק (grouped) headline : g.total = Σ amount  (INCL-VAT)")
    print("     app.py:921  g['total'] += d['amount']  · goods.html:365 sg-total {{ g.total }}")
    print("  תקציב הוצאה (budget)        : cur_spend = Σ amount_before_vat  (EX-VAT)")
    print("     app.py:1066 g['total_before_vat'] += d['amount_before_vat'] · _goal_data:1132")
    print("     amount_before_vat = round(total_without_vat,2) if set else round(amount/1.17,2)")

    # ── 2. PER-SUPPLIER COMPARISON (focus branches) ──
    print("\n" + "=" * 80)
    print(f"2. PER-SUPPLIER COMPARISON — {month}  (incl-VAT 'לפי ספק' vs ex-VAT 'תקציב')")
    print("=" * 80)
    bname = {r['id']: r['name'] for r in c.execute("SELECT id,name FROM branches")}
    for bid in FOCUS:
        sup = per_supplier(c, bid, month)
        print(f"\n■ branch {bid} {bname.get(bid,'?')}  ({len(sup)} suppliers)")
        print(f"   {'supplier':<30} {'לפי ספק(incl)':>13} {'תקציב(ex)':>11} {'ratio':>6}  class")
        for s, (incl, ex, n) in sorted(sup.items(), key=lambda kv: -kv[1][0]):
            ratio = round(incl / ex, 4) if ex else None
            mark = '  ← נסטלה' if NESTLE_LIKE in s else ''
            rstr = f"{ratio:.3f}" if ratio else '—'
            print(f"   {s[:30]:<30} {incl:>13,.2f} {ex:>11,.2f} {rstr:>6}  "
                  f"{classify(ratio)}{mark}")
        odd = [(s, round(incl/ex, 4)) for s, (incl, ex, n) in sup.items()
               if ex and classify(round(incl/ex, 4)) == '⚠ OTHER']
        if odd:
            print(f"   ⚠ ratio NOT explained by VAT (neither ~1.18 nor ~1.00): "
                  f"{[(s[:20], r) for s, r in odd]}")

    # ── 3. INTERNAL INCONSISTENCY — נסטלה 9018 יתרה uses EX-VAT ──
    print("\n" + "=" * 80)
    print("3. INTERNAL INCONSISTENCY — תקציב יתרה uses EX-VAT, 'לפי ספק' shows INCL-VAT")
    print("=" * 80)
    sup18 = per_supplier(c, 9018, month)
    nestle = [(s, v) for s, v in sup18.items() if NESTLE_LIKE in s]
    for s, (incl, ex, n) in nestle:
        brow = c.execute("SELECT monthly_budget FROM supplier_budgets "
                         "WHERE branch_id=9018 AND supplier_name=?", (s,)).fetchone()
        budget = brow['monthly_budget'] if brow else None
        print(f"  9018 {s!r}")
        print(f"     לפי ספק headline (incl-VAT) = ₪{incl:,.2f}")
        print(f"     תקציב הוצאה      (ex-VAT)   = ₪{ex:,.2f}")
        if budget is not None:
            print(f"     budget = ₪{budget:,.2f}")
            print(f"     יתרה shown   = budget − EX-VAT  = ₪{round(budget-ex,2):,.2f}  ✓ (ex-VAT)")
            print(f"     יתרה if incl = budget − INCL-VAT = ₪{round(budget-incl,2):,.2f}  "
                  f"(NOT what's shown)")
        else:
            print("     (no saved budget row for נסטלה on 9018 — יתרה would be '—')")

    # ── 4. DATA — both columns present? which each view uses ──
    print("\n" + "=" * 80)
    print("4. DATA — goods_documents stores BOTH ex-VAT and incl-VAT")
    print("=" * 80)
    cols = [r[1] for r in c.execute("PRAGMA table_info(goods_documents)").fetchall()]
    print(f"  columns: amount={'amount' in cols} (incl-VAT)  "
          f"total_without_vat={'total_without_vat' in cols} (ex-VAT)")
    rows = c.execute(
        "SELECT doc_date, ref_number, amount, total_without_vat FROM goods_documents "
        "WHERE branch_id=9018 AND supplier LIKE ? AND strftime('%Y-%m',doc_date)=? "
        "ORDER BY doc_date", ('%' + NESTLE_LIKE + '%', month)).fetchall()
    print(f"  sample נסטלה 9018 {month} rows (amount=incl, total_without_vat=ex):")
    for r in rows[:6]:
        ex = r['total_without_vat']
        ratio = round(r['amount'] / ex, 4) if ex else None
        src = 'authoritative' if ex else 'NULL → amount/1.17 fallback'
        print(f"     {r['doc_date']} ref {r['ref_number']}: incl ₪{r['amount']:,.2f} "
              f"ex ₪{(ex or 0):,.2f} ratio {ratio}  [{src}]")
    nulls = c.execute(
        "SELECT COUNT(*) k FROM goods_documents WHERE strftime('%Y-%m',doc_date)=? "
        "AND (total_without_vat IS NULL OR total_without_vat=0)", (month,)).fetchone()['k']
    tot = c.execute("SELECT COUNT(*) k FROM goods_documents WHERE strftime('%Y-%m',doc_date)=?",
                    (month,)).fetchone()['k']
    print(f"  {month} chain rows with NULL/0 total_without_vat (→ /1.17 fallback): "
          f"{nulls}/{tot}")

    # ── 5. SPREAD — chain-wide mismatch count ──
    print("\n" + "=" * 80)
    print(f"5. SPREAD — chain-wide (branch,supplier) groups, {month}")
    print("=" * 80)
    branches = [r['id'] for r in c.execute(
        "SELECT id FROM branches WHERE active=1 AND id NOT IN (9998,9999) ORDER BY id")]
    mismatch = exempt = other = 0
    total_groups = 0
    incl_sum = ex_sum = 0.0
    for bid in branches:
        for s, (incl, ex, n) in per_supplier(c, bid, month).items():
            total_groups += 1
            incl_sum += incl
            ex_sum += ex
            ratio = round(incl / ex, 4) if ex else None
            cl = classify(ratio)
            if cl.startswith('VAT'):
                if incl - ex > 1.0:
                    mismatch += 1
            elif cl.startswith('exempt'):
                exempt += 1
            else:
                other += 1
    print(f"  total (branch,supplier) groups : {total_groups}")
    print(f"  mismatch (taxable, incl−ex > ₪1): {mismatch}")
    print(f"  ~no gap (VAT-exempt ~1.00)      : {exempt}")
    print(f"  ⚠ other (unexplained by VAT)    : {other}")
    print(f"  chain Σ incl-VAT ₪{incl_sum:,.0f}  vs  Σ ex-VAT ₪{ex_sum:,.0f}  "
          f"(overall ratio {round(incl_sum/ex_sum,4) if ex_sum else '—'})")

    c.close()
    print("\nCONCLUSION: see banner in the report.")


if __name__ == '__main__':
    main()

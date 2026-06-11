"""Sanity-check gross-profit math for 9018 (דפנה) and 9015 (הגנה).

NEW (incl-VAT, consistent with רווח תפעולי and the revenue tile):
  gross = daily_sales.amount − goods_documents.amount   (both as-is, incl-VAT)
OLD (ex-VAT, replaced) shown side-by-side for comparison:
  gross = income/1.17 − SUM(COALESCE(total_without_vat, amount/1.17))
"""
import sqlite3
from datetime import date, timedelta

VAT = 1.17
DB = "db/makolet_chain.db"
# current month + previous month (previous = last full month, for timing sanity)
_t = date.today()
_prev = _t.replace(day=1) - timedelta(days=1)
MONTHS = [_t.strftime("%Y-%m"), _prev.strftime("%Y-%m")]

con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row

cols = [r[1] for r in con.execute("PRAGMA table_info(goods_documents)")]
has_twv = "total_without_vat" in cols
goods_exvat_expr = (
    "COALESCE(total_without_vat, amount/1.17)" if has_twv else "amount/1.17"
)

for bid in (9018, 9015):
    name = con.execute("SELECT name FROM branches WHERE id=?", (bid,)).fetchone()
    name = name["name"] if name else "?"
    print(f"\n[{bid}] {name}")
    for MONTH in MONTHS:
        income = con.execute(
            "SELECT COALESCE(SUM(amount),0) FROM daily_sales "
            "WHERE branch_id=? AND strftime('%Y-%m', date)=?", (bid, MONTH)
        ).fetchone()[0]
        goods = con.execute(
            "SELECT COALESCE(SUM(amount),0) FROM goods_documents "
            "WHERE branch_id=? AND strftime('%Y-%m', doc_date)=?", (bid, MONTH)
        ).fetchone()[0]
        goods_exvat = con.execute(
            f"SELECT COALESCE(SUM({goods_exvat_expr}),0) "
            "FROM goods_documents "
            "WHERE branch_id=? AND strftime('%Y-%m', doc_date)=?", (bid, MONTH)
        ).fetchone()[0]

        if income > 0 and goods > 0:
            gross = income - goods
            pct = gross / income * 100
            old_gross = income / VAT - goods_exvat
            old_pct = old_gross / (income / VAT) * 100
            print(f"  {MONTH}: rev {income:>10,.0f} | goods {goods:>10,.0f} | "
                  f"NEW GROSS {gross:>9,.0f} ({pct:.1f}%) | "
                  f"old ex-VAT {old_gross:>9,.0f} ({old_pct:.1f}%)")
        else:
            print(f"  {MONTH}: —  (missing revenue or goods)")

con.close()

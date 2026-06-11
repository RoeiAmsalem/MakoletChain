"""Sanity-check gross-profit math for 9018 (דפנה) and 9015 (הגנה), current month.

Gross = revenue ex-VAT − goods ex-VAT, all on a consistent ex-VAT basis.
  revenue ex-VAT = daily_sales.amount / 1.17   (daily_sales is incl-VAT)
  goods   ex-VAT = SUM(COALESCE(total_without_vat, amount/1.17))
"""
import sqlite3
from datetime import date

VAT = 1.17
DB = "db/makolet_chain.db"
MONTH = date.today().strftime("%Y-%m")

con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row

cols = [r[1] for r in con.execute("PRAGMA table_info(goods_documents)")]
has_twv = "total_without_vat" in cols
goods_exvat_expr = (
    "COALESCE(total_without_vat, amount/1.17)" if has_twv else "amount/1.17"
)
print(f"goods ex-VAT basis: {goods_exvat_expr}  (total_without_vat present: {has_twv})")

for bid in (9018, 9015):
    name = con.execute("SELECT name FROM branches WHERE id=?", (bid,)).fetchone()
    name = name["name"] if name else "?"

    income_incl = con.execute(
        "SELECT COALESCE(SUM(amount),0) FROM daily_sales "
        "WHERE branch_id=? AND strftime('%Y-%m', date)=?", (bid, MONTH)
    ).fetchone()[0]

    goods_incl = con.execute(
        "SELECT COALESCE(SUM(amount),0) FROM goods_documents "
        "WHERE branch_id=? AND strftime('%Y-%m', doc_date)=?", (bid, MONTH)
    ).fetchone()[0]

    goods_exvat = con.execute(
        f"SELECT COALESCE(SUM({goods_exvat_expr}),0) "
        "FROM goods_documents "
        "WHERE branch_id=? AND strftime('%Y-%m', doc_date)=?", (bid, MONTH)
    ).fetchone()[0]

    income_exvat = income_incl / VAT
    gross = income_exvat - goods_exvat
    pct = (gross / income_exvat * 100) if income_exvat > 0 else None

    print(f"\n[{bid}] {name}  ({MONTH})")
    print(f"  income incl-VAT : {income_incl:,.0f}")
    print(f"  income ex-VAT   : {income_exvat:,.0f}")
    print(f"  goods  incl-VAT : {goods_incl:,.0f}")
    print(f"  goods  ex-VAT   : {goods_exvat:,.0f}")
    if pct is None:
        print("  gross           : —  (no revenue)")
    else:
        print(f"  GROSS ₪         : {gross:,.0f}")
        print(f"  GROSS %         : {pct:.1f}%")

con.close()

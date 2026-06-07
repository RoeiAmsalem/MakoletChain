"""READ-ONLY pass 2: catch REAL repeated-product price history + cross-supplier
collisions by sampling the most-recent invoices of ONE dense branch WITHOUT the
per-supplier dedup (so the same product re-ordered shows up across docs).

Junk/placeholder lines (פיקדון deposit, פריט חסום blocked-item, and the catch-all
code 32323232) are excluded — they legitimately recur and aren't catalog products.

Bounded (≤ CAP fetches). DB mode=ro. No writes.
"""
import os
import sys
import time
import sqlite3
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module                       # noqa: E402
from agents.bilboy import fetch_doc_detail      # noqa: E402

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
BRANCH = 9015
CAP = 35
JUNK_CODES = {'32323232', '', '0'}
JUNK_NAMES = {'פיקדון', 'פריט חסום'}


def conn_ro():
    c = sqlite3.connect('file:' + os.path.abspath(DB_PATH) + '?mode=ro', uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def main():
    c = conn_ro()
    rows = c.execute(
        "SELECT bilboy_doc_id, supplier, doc_date FROM goods_documents "
        "WHERE branch_id=? AND doc_type=3 AND bilboy_doc_id IS NOT NULL "
        "AND TRIM(bilboy_doc_id)<>'' AND doc_date >= date('now','-2 months') "
        "ORDER BY doc_date DESC LIMIT ?", (BRANCH, CAP)).fetchall()
    c.close()

    code_sup = defaultdict(set)                 # code -> {supplier}
    code_hist = defaultdict(list)              # code -> [(date, supplier, name, price)]
    pid_name = defaultdict(set)                # productId -> {name}
    items = fetched = 0

    for r in rows:
        try:
            raw = fetch_doc_detail(BRANCH, r['bilboy_doc_id'])
        except Exception:
            continue
        its = ((raw or {}).get('body') or {}).get('items') or []
        if its:
            fetched += 1
        for it in its:
            nm = (it.get('name') or '').strip()
            code = str(it.get('barcode') or it.get('catalogNumber') or '').strip()
            if code in JUNK_CODES or nm in JUNK_NAMES:
                continue
            items += 1
            code_sup[code].add(r['supplier'])
            code_hist[code].append((r['doc_date'], r['supplier'], nm, it.get('priceWithoutVat')))
            pid = it.get('productId') or it.get('id')
            if pid is not None:
                pid_name[str(pid)].add(nm)
        time.sleep(0.2)

    print(f"branch {BRANCH}: fetched {fetched}/{len(rows)} invoices, {items} real line items")
    print(f"distinct product codes: {len(code_sup)}")

    repeats = {k: v for k, v in code_hist.items() if len(v) >= 2}
    print(f"\ncodes appearing in >=2 sampled invoices (repeat products): {len(repeats)}")
    price_varies = {k: v for k, v in repeats.items()
                    if len({p[3] for p in v if p[3] is not None}) >= 2}
    print(f"  of those, unit cost (priceWithoutVat) VARIES across invoices: {len(price_varies)}")
    for k, v in list(price_varies.items())[:3]:
        print(f"  [code {k}] '{v[0][2][:34]}':")
        for d, sup, nm, pr in sorted(v)[:5]:
            print(f"     {d}  {sup[:24]:<24} {pr}")

    multi = {k: v for k, v in code_sup.items() if len(v) > 1}
    print(f"\ncodes under >1 supplier (re-classify candidates): {len(multi)}")
    for k, sups in list(multi.items())[:3]:
        print(f"  [code {k}] '{code_hist[k][0][2][:34]}' under: {sorted(sups)}")

    incon = {k: v for k, v in pid_name.items() if len(v) > 1}
    print(f"\nproductId mapping to >1 distinct name (key stability check): {len(incon)}")
    for k, names in list(incon.items())[:3]:
        print(f"  [productId {k}] names={sorted(n[:30] for n in names)}")


if __name__ == '__main__':
    main()

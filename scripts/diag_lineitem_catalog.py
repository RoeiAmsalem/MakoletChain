"""READ-ONLY: characterize BilBoy invoice line-item data for a product catalog.

Samples a bounded number of invoices per branch (one most-recent per supplier
for breadth, capped), live-fetches each doc's line items via the EXISTING
agents.bilboy.fetch_doc_detail (the same /customer/doc call the /goods modal
uses), and reports:

  Q1 fields   — every RAW key seen per line item + a few full sample items;
                whether a product CODE/barcode exists vs only a free-text name.
  Q2 scale    — docs in the last 2 months/branch (DB) + distinct barcodes /
                distinct normalized names IN THE SAMPLE (order-of-magnitude).
  Q3 the mess — a product (barcode, else normalized name) appearing under 2+
                suppliers across docs — the re-classify candidates.
  Q4 price    — a barcode whose unit cost (priceWithoutVat) varies across docs.

Bounded sample (≤ CAP fetches/branch) — NOT a full re-pull. DB opened mode=ro;
no writes anywhere.
"""
import os
import sys
import time
import sqlite3
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module           # noqa: E402  (for DB_PATH)
from agents.bilboy import fetch_doc_detail  # noqa: E402

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
BRANCHES = [126, 9015]   # אינשטיין + הגנה
CAP = 28                 # max live doc fetches per branch (sampling, not re-pull)


def conn_ro():
    c = sqlite3.connect('file:' + os.path.abspath(DB_PATH) + '?mode=ro', uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def norm(name):
    return ' '.join((name or '').split()).strip().lower()


def main():
    c = conn_ro()
    all_keys = set()
    sample_raw_items = []         # a few full raw items (across suppliers/branches)
    grand_distinct_barcode = set()
    grand_distinct_name = set()

    for bid in BRANCHES:
        bname = c.execute("SELECT name FROM branches WHERE id=?", (bid,)).fetchone()['name']
        docs2mo = c.execute(
            "SELECT COUNT(*) k FROM goods_documents WHERE branch_id=? AND doc_type=3 "
            "AND doc_date >= date('now','-2 months')", (bid,)).fetchone()['k']

        # One most-recent invoice per supplier (breadth → cross-supplier collisions).
        rows = c.execute(
            "SELECT bilboy_doc_id, supplier, doc_date FROM goods_documents "
            "WHERE branch_id=? AND doc_type=3 AND bilboy_doc_id IS NOT NULL "
            "AND TRIM(bilboy_doc_id)<>'' AND doc_date >= date('now','-2 months') "
            "ORDER BY doc_date DESC", (bid,)).fetchall()
        picked, seen_sup = [], set()
        for r in rows:                       # first pass: one per supplier
            if r['supplier'] in seen_sup:
                continue
            seen_sup.add(r['supplier'])
            picked.append(r)
            if len(picked) >= CAP:
                break
        for r in rows:                       # backfill with extra docs if room
            if len(picked) >= CAP:
                break
            if r not in picked:
                picked.append(r)

        barcode_sup = defaultdict(set)       # barcode -> {supplier}
        name_sup = defaultdict(set)          # norm name -> {supplier}
        barcode_prices = defaultdict(list)   # barcode -> [(date, supplier, name, price)]
        items_total = items_with_code = 0
        fetched = errs = 0

        for r in picked:
            try:
                raw = fetch_doc_detail(bid, r['bilboy_doc_id'])
            except Exception:
                errs += 1
                continue
            body = (raw or {}).get('body') or {}
            its = body.get('items') or (raw or {}).get('items') or []
            if not its:
                continue
            fetched += 1
            for it in its:
                all_keys.update(it.keys())
                items_total += 1
                code = (str(it.get('barcode') or it.get('catalogNumber') or '')).strip()
                nm = it.get('name') or ''
                price = it.get('priceWithoutVat')
                if code:
                    items_with_code += 1
                    barcode_sup[code].add(r['supplier'])
                    grand_distinct_barcode.add(code)
                    barcode_prices[code].append((r['doc_date'], r['supplier'], nm, price))
                if nm:
                    name_sup[norm(nm)].add(r['supplier'])
                    grand_distinct_name.add(norm(nm))
            if len(sample_raw_items) < 4 and its:
                sample_raw_items.append((bname, r['supplier'], its[0]))
            time.sleep(0.2)                  # gentle — don't add load

        print(f"\n### branch {bid} — {bname}")
        print(f"  invoices (doc_type=3) last 2 months in DB: {docs2mo}")
        print(f"  sampled docs fetched ok: {fetched}/{len(picked)} (errors {errs})")
        print(f"  line items in sample: {items_total}  (with code/barcode: {items_with_code} "
              f"= {round(100*items_with_code/max(items_total,1))}%)")
        print(f"  distinct barcodes in sample: {len(barcode_sup)} | "
              f"distinct normalized names: {len(name_sup)}")
        if items_total:
            print(f"  avg items/invoice (sample): {round(items_total/max(fetched,1),1)}")

        # Q3 — product under multiple suppliers
        multi_code = {k: v for k, v in barcode_sup.items() if len(v) > 1}
        multi_name = {k: v for k, v in name_sup.items() if len(v) > 1}
        print(f"  barcodes under >1 supplier: {len(multi_code)} | "
              f"names under >1 supplier: {len(multi_name)}")
        for k, sups in list(multi_code.items())[:3]:
            ex = barcode_prices[k][0]
            print(f"    [code {k}] '{ex[2][:34]}' under: {sorted(sups)}")
        if not multi_code:
            for k, sups in list(multi_name.items())[:3]:
                print(f"    [name] '{k[:40]}' under: {sorted(sups)}")

        # Q4 — price variation for a repeated barcode
        for k, plist in barcode_prices.items():
            prices = {p[3] for p in plist if p[3] is not None}
            if len(plist) >= 2 and len(prices) >= 2:
                print(f"  price varies — code {k} '{plist[0][2][:30]}':")
                for d, sup, nm, pr in plist[:4]:
                    print(f"    {d}  {sup[:22]:<22} priceWithoutVat={pr}")
                break

    print("\n" + "=" * 64)
    print("Q1 — ALL raw line-item keys seen across the sample:")
    print("  " + ", ".join(sorted(all_keys)))
    print("\nFew full RAW sample line items (branch | supplier | item):")
    for bname, sup, it in sample_raw_items:
        compact = {k: it.get(k) for k in sorted(it.keys())}
        print(f"  [{bname} | {sup[:20]}] {compact}")
    print(f"\nChain-ish (2 sampled branches) distinct barcodes={len(grand_distinct_barcode)} "
          f"distinct names={len(grand_distinct_name)}")
    c.close()


if __name__ == '__main__':
    main()

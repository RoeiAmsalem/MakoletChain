"""Probe: dump BilBoy line-item field names from a few real docs (staging).

Confirms the product-code field (productId vs id), barcode/price/name keys, and
shows whether junk codes (32323232 / פיקדון / פריט חסום) appear — before the
catalog build commits to those field names.

Usage:  python3 scripts/probe_bilboy_lineitem.py [n_docs_per_branch]
"""
import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module
from agents.bilboy import fetch_doc_detail

N = int(sys.argv[1]) if len(sys.argv) > 1 else 2

conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
conn.row_factory = sqlite3.Row
branches = [r['id'] for r in conn.execute(
    "SELECT id FROM branches WHERE active=1 ORDER BY id").fetchall()]

shown = 0
for bid in branches:
    docs = conn.execute(
        "SELECT bilboy_doc_id, supplier, doc_date FROM goods_documents "
        "WHERE branch_id=? AND bilboy_doc_id IS NOT NULL "
        "AND strftime('%Y-%m', doc_date) >= '2026-05' ORDER BY doc_date DESC LIMIT ?",
        (bid, N)).fetchall()
    for d in docs:
        try:
            raw = fetch_doc_detail(bid, d['bilboy_doc_id'])
        except Exception as e:
            print(f"branch {bid} doc {d['bilboy_doc_id']}: ERROR {e}")
            continue
        items = ((raw or {}).get('body') or {}).get('items') or []
        print(f"\n=== branch {bid} | supplier={d['supplier']!r} | doc={d['bilboy_doc_id']} "
              f"| {len(items)} items ===")
        if items:
            print("item[0] keys:", sorted(items[0].keys()))
            for it in items[:3]:
                print("  ", json.dumps({k: it.get(k) for k in
                      ('productId', 'id', 'barcode', 'catalogNumber', 'name',
                       'priceWithoutVat', 'total', 'qty')}, ensure_ascii=False))
            shown += 1
        if shown >= 4:
            break
    if shown >= 4:
        break

conn.close()

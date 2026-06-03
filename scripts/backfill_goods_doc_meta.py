#!/usr/bin/env python3
"""
Backfill the migration-024 enrichment columns on EXISTING goods_documents rows.

Re-fetches BilBoy /customer/docs/headers (the same call the nightly agent makes,
no extra per-doc calls) for a date window and UPDATEs total_without_vat, paid,
bilboy_status and bilboy_doc_id on rows that already exist, matched by
(branch_id, ref_number). This makes the /goods click-into-invoice detail work
for historical invoices that were synced before column 024 existed.

It NEVER INSERTs, DELETEs, or re-runs dedup — existing rows, totals and the
5-layer dedup are untouched. Rows whose ref_number isn't found are skipped.

Usage:
    python3 scripts/backfill_goods_doc_meta.py [branch_id] [--months N]

    branch_id  optional — limit to one branch (default: all active chain branches)
    --months   how many months back to enrich, including current (default 3)
"""

import argparse
import sqlite3
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agents.bilboy import (
    _api_get, _branch_session, _get_branch_config, _get_db,
)


def _supplier_ids(session, bb_branch_id, franchise_supplier):
    raw = _api_get(session, '/customer/suppliers',
                   params={'customerBranchId': bb_branch_id, 'all': 'true'})
    suppliers = raw.get('suppliers') if isinstance(raw, dict) else raw
    ids = []
    for s in (suppliers or []):
        name = s.get('title') or s.get('name') or s.get('supplierName') or ''
        sid = str(s.get('id') or s.get('supplierId') or '')
        if franchise_supplier and franchise_supplier in name:
            continue
        if sid:
            ids.append(sid)
    return ids


def _fetch_headers(session, ids, bb_branch_id, frm, to):
    docs = []
    BATCH = 30
    for i in range(0, max(len(ids), 1), BATCH):
        batch = ids[i:i + BATCH]
        if not batch:
            break
        r = _api_get(session, '/customer/docs/headers', params={
            'suppliers': ','.join(batch),
            'branches': bb_branch_id,
            'from': f'{frm}T00:00:00',
            'to': f'{to}T00:00:00',
        })
        lst = r if isinstance(r, list) else (
            r.get('data') or r.get('docs') or r.get('headers') or [])
        docs.extend(lst)
    return docs


def backfill_branch(branch_id, months):
    branch = _get_branch_config(branch_id)
    bb_branch_id = branch.get('bilboy_branch_id')
    if not bb_branch_id:
        print(f"[branch {branch_id}] no bilboy_branch_id — skip")
        return 0
    franchise_supplier = branch.get('franchise_supplier') or 'זיכיונות המכולת בע"מ'
    session = _branch_session(branch, branch_id)

    today = date.today()
    y, m = today.year, today.month
    start_m = m - (months - 1)
    start_y = y
    while start_m <= 0:
        start_m += 12
        start_y -= 1
    frm = date(start_y, start_m, 1).isoformat()
    to = today.isoformat()

    ids = _supplier_ids(session, str(bb_branch_id), franchise_supplier)
    if not ids:
        print(f"[branch {branch_id}] no suppliers — skip")
        return 0
    headers = _fetch_headers(session, ids, str(bb_branch_id), frm, to)

    conn = _get_db()
    updated = 0
    for doc in headers:
        ref = str(doc.get('refNumber') or doc.get('number') or '').lstrip('0') or '0'
        twv = float(doc.get('totalWithoutVat') or 0) or None
        paid = 1 if doc.get('paid') else 0
        status = doc.get('status')
        uuid = doc.get('id')
        if not uuid:
            continue
        cur = conn.execute(
            "UPDATE goods_documents SET total_without_vat=?, paid=?, "
            "bilboy_status=?, bilboy_doc_id=? WHERE branch_id=? AND ref_number=?",
            (twv, paid, status, uuid, branch_id, ref)
        )
        updated += cur.rowcount
    conn.commit()
    conn.close()
    print(f"[branch {branch_id}] headers={len(headers)} rows_updated={updated} "
          f"window={frm}..{to}")
    return updated


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('branch_id', nargs='?', type=int)
    ap.add_argument('--months', type=int, default=3)
    args = ap.parse_args()

    if args.branch_id:
        branch_ids = [args.branch_id]
    else:
        conn = _get_db()
        rows = conn.execute(
            "SELECT id FROM branches WHERE active=1 AND bilboy_branch_id IS NOT NULL "
            "ORDER BY id").fetchall()
        conn.close()
        branch_ids = [r['id'] for r in rows]

    total = 0
    for bid in branch_ids:
        try:
            total += backfill_branch(bid, args.months)
        except Exception as e:
            print(f"[branch {bid}] ERROR: {e}")
    print(f"DONE — {total} rows enriched across {len(branch_ids)} branch(es).")


if __name__ == '__main__':
    main()

"""POC: build a chain-wide product catalog from BilBoy invoice line-items, and
classify products that BilBoy files under the franchise supplier (זיכיונות) but
are actually real goods.

Sources of line-items (last 2 calendar months, all stores):
  1. goods_documents — the normal synced docs (real suppliers).
  2. The זיכיונות franchise docs that the goods sync EXCLUDES — fetched here
     DIRECTLY from BilBoy (/customer/suppliers → franchise id → /customer/docs/
     headers → /customer/doc). CATALOG ONLY — bilboy.py's goods sync, /goods and
     budget still never count זיכיונות.

Each line → a product_observation (supplier = the document's supplier, so
franchise docs are tagged with the זיכיונות name). Then per product_id we derive
the most-common supplier, the most-common REAL (non-franchise) supplier as the
suggested_supplier, and a classification_status:
  • auto         — seen under זיכיונות AND under a real supplier → auto-map.
  • needs-review — seen ONLY under זיכיונות → manual review.

The live line-item fetch misses in bulk, so empty/failed docs are retried over
several passes (gentle throttle). Staging only.

Usage:  python3 scripts/build_product_catalog.py
"""
import os
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module
from agents.bilboy import (_get_branch_config, _branch_session, _api_get,
                           ALLOWED_DOC_TYPES, EXCLUDED_STATUSES)

IL_TZ = ZoneInfo('Asia/Jerusalem')
DB = app_module.DB_PATH

JUNK_CODES = {'32323232'}
JUNK_NAMES = {'פיקדון', 'פריט חסום'}
FRANCHISE_MATCH = 'זיכיונות'          # substring identifying the franchise supplier
DEFAULT_FRANCHISE = 'זיכיונות המכולת בע"מ'

SLEEP = 0.12          # gentle throttle between BilBoy calls
MAX_PASSES = 3        # retry empty/failed docs over this many passes
CALL_CAP = 14000      # hard safety cap on total calls


def _fetch(session, doc_id):
    return _api_get(session, '/customer/doc', params={'docId': doc_id}, timeout=15)


def _fetch_franchise_docs(session, bb_branch_id, franchise_name, from_date, to_date):
    """Headers for the זיכיונות franchise supplier ONLY — the docs the goods sync
    drops. Returns [{bilboy_doc_id, supplier, doc_date}]. Best-effort: any API
    hiccup returns []."""
    if not bb_branch_id or not franchise_name:
        return []
    try:
        raw = _api_get(session, '/customer/suppliers',
                       params={'customerBranchId': bb_branch_id, 'all': 'true'},
                       timeout=20)
    except Exception:
        return []
    suppliers = raw.get('suppliers') if isinstance(raw, dict) else raw
    fids = []
    for s in (suppliers or []):
        name = s.get('title') or s.get('name') or s.get('supplierName') or ''
        if FRANCHISE_MATCH in name:
            sid = str(s.get('id') or s.get('supplierId') or '')
            if sid:
                fids.append(sid)
    if not fids:
        return []
    try:
        hdr = _api_get(session, '/customer/docs/headers',
                       params={'suppliers': ','.join(fids), 'branches': bb_branch_id,
                               'from': f'{from_date}T00:00:00', 'to': f'{to_date}T00:00:00'},
                       timeout=30)
    except Exception:
        return []
    lst = hdr if isinstance(hdr, list) else (
        hdr.get('data') or hdr.get('docs') or hdr.get('headers') or [])
    out = []
    for d in lst:
        if d.get('status') in EXCLUDED_STATUSES:
            continue
        if d.get('type') not in ALLOWED_DOC_TYPES:
            continue
        sup = d.get('supplierName') or ''
        if FRANCHISE_MATCH not in sup:
            continue
        did = d.get('id')
        if not did:
            continue
        out.append({'bilboy_doc_id': did, 'supplier': sup.strip(),
                    'doc_date': str(d.get('date') or d.get('documentDate') or '')[:10]})
    return out


def main():
    conn = sqlite3.connect(DB, timeout=30)
    conn.row_factory = sqlite3.Row

    now = datetime.now(IL_TZ)
    cur_m = now.strftime('%Y-%m')
    first_cur = now.replace(day=1)
    prev_m = (first_cur - timedelta(days=1)).strftime('%Y-%m')
    from_date = (first_cur - timedelta(days=1)).replace(day=1).date().isoformat()
    to_date = now.date().isoformat()
    print(f"=== product catalog build — window {prev_m} + {cur_m} ===")

    # ── STEP 0: is זיכיונות already in our data? (expected: NO) ──
    gz = conn.execute("SELECT COUNT(*) c FROM goods_documents "
                      "WHERE supplier LIKE '%' || ? || '%'", (FRANCHISE_MATCH,)).fetchone()['c']
    oz = conn.execute("SELECT COUNT(*) c FROM product_observations "
                      "WHERE supplier LIKE '%' || ? || '%'", (FRANCHISE_MATCH,)).fetchone()['c']
    print(f"\n--- pre-check: זיכיונות in data today ---")
    print(f"  goods_documents rows under זיכיונות: {gz} (expected 0 — sync excludes them)")
    print(f"  product_observations under זיכיונות: {oz}")

    branches = [r['id'] for r in conn.execute(
        "SELECT id FROM branches WHERE active=1 ORDER BY id").fetchall()]

    docs_by_branch = {}
    for bid in branches:
        rows = conn.execute(
            "SELECT bilboy_doc_id, supplier, doc_date FROM goods_documents "
            "WHERE branch_id=? AND bilboy_doc_id IS NOT NULL "
            "AND strftime('%Y-%m', doc_date) IN (?, ?)",
            (bid, prev_m, cur_m)).fetchall()
        if rows:
            docs_by_branch[bid] = [dict(r) for r in rows]

    # ── STEP 1: token check ──
    print("\n--- token check ---")
    sessions, confs, ok_branches = {}, {}, []
    for bid in list(docs_by_branch):
        try:
            branch = _get_branch_config(bid)
            sess = _branch_session(branch, bid)
        except Exception as e:
            print(f"  branch {bid}: NO TOKEN ({str(e)[:60]})")
            continue
        probed = False
        for d in docs_by_branch[bid][:3]:
            try:
                _fetch(sess, d['bilboy_doc_id'])
                probed = True
                break
            except PermissionError:
                break
            except Exception:
                continue
            finally:
                time.sleep(SLEEP)
        if probed:
            sessions[bid] = sess
            confs[bid] = branch
            ok_branches.append(bid)
            print(f"  branch {bid}: OK ({len(docs_by_branch[bid])} goods docs)")
        else:
            print(f"  branch {bid}: token/fetch FAILED — skipped")

    if not ok_branches:
        print("\nSTOP: no store can fetch BilBoy line-items (tokens missing/expired).")
        conn.close()
        sys.exit(1)

    # ── STEP 1b: fetch the EXCLUDED זיכיונות doc headers per store ──
    print("\n--- fetching זיכיונות doc headers (the sync-excluded docs) ---")
    zik_docs_by_branch = {}
    zik_total = 0
    for bid in ok_branches:
        conf = confs[bid]
        franchise = (conf.get('franchise_supplier') or DEFAULT_FRANCHISE).strip()
        bb_id = conf.get('bilboy_branch_id')
        zdocs = _fetch_franchise_docs(sessions[bid], bb_id, franchise, from_date, to_date)
        time.sleep(SLEEP)
        if zdocs:
            zik_docs_by_branch[bid] = zdocs
            zik_total += len(zdocs)
        print(f"  branch {bid}: {len(zdocs)} זיכיונות docs")
    print(f"  → {zik_total} זיכיונות docs across the chain")

    # ── STEP 2: fetch line-items (goods + זיכיונות), retry over passes ──
    pending = [(bid, d) for bid in ok_branches for d in docs_by_branch[bid]]
    pending += [(bid, d) for bid in zik_docs_by_branch for d in zik_docs_by_branch[bid]]
    total_docs = len(pending)
    name_map = {}
    calls = 0
    print(f"\n--- fetching line-items for {total_docs} docs "
          f"({zik_total} of them זיכיונות; up to {MAX_PASSES} passes) ---")

    for p in range(1, MAX_PASSES + 1):
        still, got = [], 0
        for bid, d in pending:
            if calls >= CALL_CAP:
                still.append((bid, d))
                continue
            calls += 1
            try:
                raw = _fetch(sessions[bid], d['bilboy_doc_id'])
            except Exception:
                still.append((bid, d))
                time.sleep(SLEEP)
                continue
            items = ((raw or {}).get('body') or {}).get('items') or []
            if not items:
                still.append((bid, d))
                time.sleep(SLEEP)
                continue
            supplier = (d['supplier'] or '').strip()
            ddate = d['doc_date']
            for it in items:
                pid = str(it.get('productId') or it.get('id') or '').strip()
                name = (it.get('name') or '').strip()
                if not pid or pid in JUNK_CODES or name in JUNK_NAMES:
                    continue
                barcode = (it.get('barcode') or it.get('catalogNumber') or '').strip()
                conn.execute(
                    "INSERT OR IGNORE INTO product_observations "
                    "(product_id, branch_id, doc_id, doc_date, supplier, price, qty) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (pid, bid, d['bilboy_doc_id'], ddate, supplier,
                     it.get('priceWithoutVat'), it.get('qty')))
                prev = name_map.get(pid)
                if name and (prev is None or (ddate or '') >= (prev[2] or '')):
                    name_map[pid] = (name, barcode, ddate)
            got += 1
            time.sleep(SLEEP)
        conn.commit()
        print(f"  pass {p}: {got} docs yielded items, {len(still)} still empty/failed "
              f"(calls so far {calls})")
        pending = still
        if not pending:
            break

    fetched_ok = total_docs - len(pending)
    miss_rate = (len(pending) / total_docs * 100) if total_docs else 0
    print(f"\nfetched {fetched_ok}/{total_docs} docs with items | "
          f"final miss/empty {len(pending)} ({miss_rate:.1f}%) | {calls} calls")

    # ── STEP 3: derive products + classification ──
    print("\n--- deriving products + classification ---")
    agg = {r['product_id']: r for r in conn.execute(
        "SELECT product_id, COUNT(DISTINCT supplier) AS ss, "
        "       COUNT(DISTINCT doc_id) AS dc, MAX(doc_date) AS ls "
        "FROM product_observations GROUP BY product_id").fetchall()}

    sup_counts = defaultdict(Counter)
    for r in conn.execute(
            "SELECT product_id, supplier, COUNT(*) AS c "
            "FROM product_observations GROUP BY product_id, supplier").fetchall():
        sup_counts[r['product_id']][r['supplier'] or ''] += r['c']

    latest_price = {}
    for r in conn.execute(
            "SELECT o.product_id, o.price, o.doc_date FROM product_observations o "
            "JOIN (SELECT product_id, MAX(doc_date) md FROM product_observations "
            "      GROUP BY product_id) m "
            "  ON m.product_id=o.product_id AND m.md=o.doc_date").fetchall():
        latest_price[r['product_id']] = (r['price'], r['doc_date'])

    conn.execute("DELETE FROM products")
    for pid, a in agg.items():
        counts = sup_counts[pid]
        supplier = counts.most_common(1)[0][0] if counts else None
        # franchise vs real split
        franchise_names = [s for s in counts if FRANCHISE_MATCH in s]
        real_counts = Counter({s: c for s, c in counts.items()
                               if s and FRANCHISE_MATCH not in s})
        raw_supplier = franchise_names[0] if franchise_names else None
        suggested = real_counts.most_common(1)[0][0] if real_counts else None
        if raw_supplier:
            status = 'auto' if suggested else 'needs-review'
        else:
            status = None
        name, barcode, _ = name_map.get(pid, ('', '', None))
        price, price_date = latest_price.get(pid, (None, None))
        conn.execute(
            "INSERT INTO products (product_id, barcode, name, supplier, suppliers_seen, "
            "latest_price, latest_price_date, last_seen, doc_count, raw_supplier, "
            "suggested_supplier, classification_status, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            (pid, barcode, name, supplier, a['ss'], price, price_date, a['ls'],
             a['dc'], raw_supplier, suggested, status))
    conn.commit()

    # ── stats ──
    total = conn.execute("SELECT COUNT(*) c FROM products").fetchone()['c']
    obs = conn.execute("SELECT COUNT(*) c FROM product_observations").fetchone()['c']
    zik = conn.execute("SELECT COUNT(*) c FROM products WHERE raw_supplier IS NOT NULL").fetchone()['c']
    auto = conn.execute("SELECT COUNT(*) c FROM products WHERE classification_status='auto'").fetchone()['c']
    review = conn.execute("SELECT COUNT(*) c FROM products WHERE classification_status='needs-review'").fetchone()['c']
    print(f"\n=== CATALOG: {total} products | {obs} observations | "
          f"{zik} זיכיונות-filed | {auto} auto-mapped | {review} need review ===")
    print("\nsample AUTO-mapped (זיכיונות → suggested real supplier):")
    for r in conn.execute(
            "SELECT name, raw_supplier, suggested_supplier, latest_price, barcode "
            "FROM products WHERE classification_status='auto' "
            "ORDER BY doc_count DESC LIMIT 6").fetchall():
        print(f"  {r['name']!r} | {r['raw_supplier']} → {r['suggested_supplier']!r} "
              f"| ₪{r['latest_price']} | {r['barcode']}")
    print("\nsample NEEDS-REVIEW (זיכיונות-only):")
    for r in conn.execute(
            "SELECT name, latest_price, barcode FROM products "
            "WHERE classification_status='needs-review' ORDER BY doc_count DESC LIMIT 6").fetchall():
        print(f"  {r['name']!r} | ₪{r['latest_price']} | {r['barcode']}")

    conn.close()


if __name__ == '__main__':
    main()

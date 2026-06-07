"""POC: build a chain-wide product catalog from BilBoy invoice line-items.

Walks goods_documents across ALL stores for the last 2 calendar months, fetches
each doc's line-items (agents.bilboy → GET /customer/doc), stores them as
product_observations, then derives one products row per product_id (most-common
supplier across the chain, suppliers_seen, latest price, last seen, doc count).

The live line-item fetch misses ~50% in bulk, so docs that come back empty/failed
are retried over several passes (gentle throttle between calls). Staging only.

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
from agents.bilboy import _get_branch_config, _branch_session, _api_get

IL_TZ = ZoneInfo('Asia/Jerusalem')
DB = app_module.DB_PATH

JUNK_CODES = {'32323232'}
JUNK_NAMES = {'פיקדון', 'פריט חסום'}

SLEEP = 0.12          # gentle throttle between BilBoy calls
MAX_PASSES = 3        # retry empty/failed docs over this many passes
CALL_CAP = 12000      # hard safety cap on total calls


def _fetch(session, doc_id):
    return _api_get(session, '/customer/doc', params={'docId': doc_id}, timeout=15)


def main():
    conn = sqlite3.connect(DB, timeout=30)
    conn.row_factory = sqlite3.Row

    now = datetime.now(IL_TZ)
    cur_m = now.strftime('%Y-%m')
    prev_m = (now.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    print(f"=== product catalog build — window {prev_m} + {cur_m} ===")

    branches = [r['id'] for r in conn.execute(
        "SELECT id FROM branches WHERE active=1 ORDER BY id").fetchall()]

    # Docs with a BilBoy id in the window, per branch.
    docs_by_branch = {}
    for bid in branches:
        rows = conn.execute(
            "SELECT bilboy_doc_id, supplier, doc_date FROM goods_documents "
            "WHERE branch_id=? AND bilboy_doc_id IS NOT NULL "
            "AND strftime('%Y-%m', doc_date) IN (?, ?)",
            (bid, prev_m, cur_m)).fetchall()
        if rows:
            docs_by_branch[bid] = [dict(r) for r in rows]

    # ── STEP 1: token check — must be able to fetch line-items per store ──
    print("\n--- token check ---")
    sessions = {}
    ok_branches = []
    for bid in list(docs_by_branch):
        try:
            branch = _get_branch_config(bid)
            sess = _branch_session(branch, bid)
        except Exception as e:
            print(f"  branch {bid}: NO TOKEN ({str(e)[:60]})")
            continue
        # Live probe: fetch one doc. Success (even 0 items) = token valid; a
        # 401 raises PermissionError.
        probed = False
        for d in docs_by_branch[bid][:3]:
            try:
                _fetch(sess, d['bilboy_doc_id'])
                probed = True
                break
            except PermissionError:
                break
            except Exception:
                continue  # transient — try next sample doc
            finally:
                time.sleep(SLEEP)
        if probed:
            sessions[bid] = sess
            ok_branches.append(bid)
            print(f"  branch {bid}: OK ({len(docs_by_branch[bid])} docs)")
        else:
            print(f"  branch {bid}: token/fetch FAILED — skipped")

    if not ok_branches:
        print("\nSTOP: no store can fetch BilBoy line-items (tokens missing/expired).")
        conn.close()
        sys.exit(1)

    # ── STEP 2: fetch line-items, retrying empty/failed docs over passes ──
    pending = [(bid, d) for bid in ok_branches for d in docs_by_branch[bid]]
    total_docs = len(pending)
    name_map = {}          # product_id -> (name, barcode, doc_date) newest
    calls = 0
    print(f"\n--- fetching line-items for {total_docs} docs "
          f"(up to {MAX_PASSES} passes) ---")

    for p in range(1, MAX_PASSES + 1):
        still = []
        got = 0
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
                still.append((bid, d))       # empty → retry next pass
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
                price = it.get('priceWithoutVat')
                qty = it.get('qty')
                conn.execute(
                    "INSERT OR IGNORE INTO product_observations "
                    "(product_id, branch_id, doc_id, doc_date, supplier, price, qty) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (pid, bid, d['bilboy_doc_id'], ddate, supplier, price, qty))
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

    # ── STEP 3: derive one products row per product_id ──
    print("\n--- deriving products ---")
    agg = {r['product_id']: r for r in conn.execute(
        "SELECT product_id, COUNT(DISTINCT supplier) AS ss, "
        "       COUNT(DISTINCT doc_id) AS dc, MAX(doc_date) AS ls "
        "FROM product_observations GROUP BY product_id").fetchall()}

    # most-common supplier per product
    sup_counts = defaultdict(Counter)
    for r in conn.execute(
            "SELECT product_id, supplier, COUNT(*) AS c "
            "FROM product_observations GROUP BY product_id, supplier").fetchall():
        sup_counts[r['product_id']][r['supplier']] += r['c']

    # latest price = price at the newest doc_date for that product
    latest_price = {}
    for r in conn.execute(
            "SELECT o.product_id, o.price, o.doc_date FROM product_observations o "
            "JOIN (SELECT product_id, MAX(doc_date) md FROM product_observations "
            "      GROUP BY product_id) m "
            "  ON m.product_id=o.product_id AND m.md=o.doc_date").fetchall():
        latest_price[r['product_id']] = (r['price'], r['doc_date'])

    conn.execute("DELETE FROM products")
    for pid, a in agg.items():
        supplier = sup_counts[pid].most_common(1)[0][0] if sup_counts[pid] else None
        name, barcode, _ = name_map.get(pid, ('', '', None))
        price, price_date = latest_price.get(pid, (None, None))
        conn.execute(
            "INSERT INTO products (product_id, barcode, name, supplier, "
            "suppliers_seen, latest_price, latest_price_date, last_seen, doc_count, "
            "updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            (pid, barcode, name, supplier, a['ss'], price, price_date,
             a['ls'], a['dc']))
    conn.commit()

    # ── stats ──
    total = conn.execute("SELECT COUNT(*) c FROM products").fetchone()['c']
    flagged = conn.execute(
        "SELECT COUNT(*) c FROM products WHERE suppliers_seen > 1").fetchone()['c']
    obs = conn.execute("SELECT COUNT(*) c FROM product_observations").fetchone()['c']
    print(f"\n=== CATALOG: {total} distinct products | {obs} observations | "
          f"{flagged} flagged (suppliers_seen>1) ===")
    print("\nsample products:")
    for r in conn.execute(
            "SELECT name, supplier, latest_price, barcode, last_seen, doc_count "
            "FROM products ORDER BY doc_count DESC LIMIT 5").fetchall():
        print(f"  {r['name']!r} | {r['supplier']!r} | ₪{r['latest_price']} | "
              f"{r['barcode']} | {r['last_seen']} | {r['doc_count']} docs")
    print("\nsample FLAGGED (suppliers_seen>1):")
    for r in conn.execute(
            "SELECT name, supplier, suppliers_seen, latest_price, barcode "
            "FROM products WHERE suppliers_seen > 1 ORDER BY suppliers_seen DESC "
            "LIMIT 5").fetchall():
        print(f"  {r['name']!r} | top={r['supplier']!r} | "
              f"{r['suppliers_seen']} suppliers | ₪{r['latest_price']} | {r['barcode']}")

    conn.close()


if __name__ == '__main__':
    main()

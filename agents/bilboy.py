"""
BilBoy agent (branch-aware) — fetches goods documents from BilBoy API.

Doc types: 2 (delivery note), 3 (invoice), 4 (credit invoice), 5 (return note)
Full month delete + reinsert (clean sync).
NEVER include docs where supplier matches branch.franchise_supplier.
"""

import logging
import os
import sqlite3
import time
from datetime import date, datetime
from pathlib import Path

import requests

from utils.notify import notify


def _friendly_bilboy_error(e: Exception) -> str:
    msg = str(e)
    if '400' in msg:
        return "BilBoy rejected the request (400) — likely too many suppliers in one call."
    if '401' in msg or 'token' in msg.lower() or 'expired' in msg.lower():
        return "BilBoy token expired — needs to be refreshed from the browser."
    if '500' in msg:
        return "BilBoy server error — their server is having issues."
    if 'Connection' in msg or 'timeout' in msg.lower():
        return "Could not connect to BilBoy — network or server issue."
    if 'JSONDecodeError' in msg or 'json' in msg.lower():
        return "BilBoy returned an unexpected response — possible API change."
    return msg[:120]


API_BASE = "https://app.billboy.co.il:5050/api"
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
ALLOWED_DOC_TYPES = {2, 3, 4, 5}

# BilBoy document lifecycle statuses.
# 3=normal, 9=finalized. 7=superseded (replaced by newer doc, hidden in BilBoy UI).
# Unknown statuses are dropped and alerted via brrr so we notice new lifecycle states.
ACCEPTED_STATUSES = {3, 9}


def _get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _get_branch_config(branch_id: int) -> dict:
    conn = _get_db()
    row = conn.execute('SELECT * FROM branches WHERE id = ?', (branch_id,)).fetchone()
    conn.close()
    if not row:
        raise ValueError(f"Branch {branch_id} not found")
    return dict(row)


def _setup_logger(branch_id: int) -> logging.Logger:
    logger = logging.getLogger(f'bilboy_{branch_id}')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        log_dir = Path(__file__).parent.parent / 'logs'
        log_dir.mkdir(exist_ok=True)
        fh = logging.FileHandler(log_dir / f'bilboy_{branch_id}.log', encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(sh)
    return logger


def _api_get(session: requests.Session, path: str, params=None):
    url = f"{API_BASE}{path}"
    resp = session.get(url, params=params, timeout=30)
    if resp.status_code == 401:
        raise PermissionError("BilBoy token expired")
    resp.raise_for_status()
    return resp.json()


def run_bilboy(branch_id: int) -> dict:
    """
    Fetch goods documents from BilBoy for a branch.
    Full month delete + reinsert strategy.
    Returns {success, docs_count, total_amount}.
    """
    log = _setup_logger(branch_id)
    log.info("Starting BilBoy sync for branch %d", branch_id)
    t0 = time.time()

    # Insert agent_runs start (guard against duplicate within 60s)
    conn_run = _get_db()
    recent = conn_run.execute(
        "SELECT id FROM agent_runs WHERE branch_id=? AND agent='bilboy' AND status='running' "
        "AND started_at >= datetime('now', '-60 seconds')",
        (branch_id,)
    ).fetchone()
    if recent:
        run_id = recent['id']
    else:
        cur = conn_run.execute(
            "INSERT INTO agent_runs (branch_id, agent, started_at, status) VALUES (?, 'bilboy', datetime('now'), 'running')",
            (branch_id,)
        )
        run_id = cur.lastrowid
        conn_run.commit()
    conn_run.close()

    try:
        branch = _get_branch_config(branch_id)
        token = branch.get('bilboy_pass') or ''
        franchise_supplier = branch.get('franchise_supplier') or 'זיכיונות המכולת בע"מ'

        if not token:
            log.warning("No BilBoy token for branch %d", branch_id)
            return {'success': False, 'docs_count': 0, 'total_amount': 0, 'error': 'no token'}

        session = requests.Session()
        session.headers.update({'Authorization': f'Bearer {token}'})

        # Get BilBoy branch
        branches_data = _api_get(session, '/user/branches')
        if not branches_data:
            raise ValueError("No branches from BilBoy API")
        first = branches_data[0] if isinstance(branches_data, list) else branches_data
        bb_branch_id = str(first.get('branchId') or first.get('id') or first.get('branch_id', ''))

        # Get suppliers, filter out franchise
        raw = _api_get(session, '/customer/suppliers', params={
            'customerBranchId': bb_branch_id, 'all': 'true'
        })
        suppliers = raw.get('suppliers') if isinstance(raw, dict) else raw
        keep_ids = []
        if suppliers:
            for s in suppliers:
                name = s.get('title') or s.get('name') or s.get('supplierName') or ''
                sid = str(s.get('id') or s.get('supplierId') or '')
                if franchise_supplier and franchise_supplier in name:
                    log.info("Filtered out franchise supplier: %s", name)
                    continue
                if sid:
                    keep_ids.append(sid)

        if not keep_ids:
            log.warning("No supplier IDs found")
            return {'success': True, 'docs_count': 0, 'total_amount': 0}

        # Full month date range
        today = date.today()
        from_date = date(today.year, today.month, 1).isoformat()
        to_date = today.isoformat()

        # Fetch docs in batches of 30 suppliers to avoid 400 from URL length limit
        BATCH_SIZE = 30
        all_raw_docs = []
        for i in range(0, max(len(keep_ids), 1), BATCH_SIZE):
            batch = keep_ids[i:i+BATCH_SIZE]
            if not batch:
                break
            batch_docs = _api_get(session, '/customer/docs/headers', params={
                'suppliers': ','.join(batch),
                'branches': bb_branch_id,
                'from': f'{from_date}T00:00:00',
                'to': f'{to_date}T00:00:00',
            })
            batch_list = batch_docs if isinstance(batch_docs, list) else (
                batch_docs.get('data') or batch_docs.get('docs') or
                batch_docs.get('headers') or []
            )
            all_raw_docs.extend(batch_list)
            log.info("Batch %d-%d: %d docs", i, i+len(batch), len(batch_list))

        docs = all_raw_docs
        log.info("API returned %d raw documents total (%d batches)",
                 len(docs), (len(keep_ids) + BATCH_SIZE - 1) // max(BATCH_SIZE, 1))

        # ── Status filter: drop superseded (status=7) and unknown statuses ──
        raw_sum = 0
        status_7_sum = 0
        unknown_sum = 0
        skip_status = 0
        skip_superseded = 0
        status_filtered = []
        for doc in docs:
            amount = float(doc.get('totalWithVat') or doc.get('totalAmount') or doc.get('amount') or 0)
            raw_sum += amount
            status = doc.get('status')
            if status == 7:
                skip_superseded += 1
                status_7_sum += amount
                continue
            if status is not None and status not in ACCEPTED_STATUSES:
                skip_status += 1
                unknown_sum += amount
                ref = doc.get('refNumber') or doc.get('number') or '?'
                bk_id = doc.get('bookKeepingId') or '?'
                log.warning("Unknown BilBoy status %s for doc ref=%s bookKeepingId=%s", status, ref, bk_id)
                notify(f"⚠️ BilBoy — {branch.get('name', f'Branch {branch_id}')}",
                       f"Unknown BilBoy status {status} for doc ref={ref}/bookKeepingId={bk_id} — investigate.")
                continue
            status_filtered.append(doc)

        if skip_superseded > 0:
            log.info("Dropped %d superseded (status=7) docs totaling ₪%.2f", skip_superseded, status_7_sum)
        if skip_status > 0:
            log.warning("Dropped %d unknown-status docs totaling ₪%.2f", skip_status, unknown_sum)

        # ── BookKeepingId dedup: if two accepted docs share same bookKeepingId,
        #    keep the one with higher status (9 beats 3) ──
        bk_map = {}
        for doc in status_filtered:
            bk_id = doc.get('bookKeepingId')
            if bk_id is not None:
                existing = bk_map.get(bk_id)
                if existing is not None:
                    old_status = existing.get('status') or 0
                    new_status = doc.get('status') or 0
                    if new_status >= old_status:
                        old_ref = existing.get('refNumber') or existing.get('number') or '?'
                        new_ref = doc.get('refNumber') or doc.get('number') or '?'
                        log.info("Deduped bookKeepingId=%s: kept status=%s ref=%s, dropped status=%s ref=%s",
                                 bk_id, new_status, new_ref, old_status, old_ref)
                        bk_map[bk_id] = doc
                    else:
                        new_ref = doc.get('refNumber') or doc.get('number') or '?'
                        kept_ref = existing.get('refNumber') or existing.get('number') or '?'
                        log.info("Deduped bookKeepingId=%s: kept status=%s ref=%s, dropped status=%s ref=%s",
                                 bk_id, existing.get('status'), kept_ref, doc.get('status'), new_ref)
                else:
                    bk_map[bk_id] = doc
            else:
                # No bookKeepingId — pass through (will be caught by ref_number dedup)
                bk_map[id(doc)] = doc  # unique key
        docs = list(bk_map.values())

        # Process documents
        records = []
        skip_franchise = 0
        skip_zeros = 0
        skip_type = 0
        for doc in docs:
            doc_type = doc.get('type')
            if doc_type not in ALLOWED_DOC_TYPES:
                skip_type += 1
                continue

            supplier = doc.get('supplierName') or ''
            if franchise_supplier and franchise_supplier in supplier:
                skip_franchise += 1
                continue

            amount = float(doc.get('totalWithVat') or doc.get('totalAmount') or doc.get('amount') or 0)
            if amount == 0:
                skip_zeros += 1
                continue

            raw_date = doc.get('date') or doc.get('documentDate') or today.isoformat()
            ref_number = str(doc.get('refNumber') or doc.get('number') or '').lstrip('0') or '0'

            records.append({
                'doc_date': str(raw_date)[:10],
                'supplier': supplier,
                'ref_number': ref_number,
                'amount': amount,
                'doc_type': doc_type,
            })

        # Dedup by ref_number (Layer 1 — catches leading-zero variants)
        seen = set()
        deduped = []
        for r in records:
            key = r['ref_number']
            if key in seen:
                continue
            seen.add(key)
            deduped.append(r)
        records = deduped

        log.info("After filtering: %d records (skipped: %d franchise, %d zero, %d wrong type, %d superseded, %d unknown status)",
                 len(records), skip_franchise, skip_zeros, skip_type, skip_superseded, skip_status)

        # Full month delete + reinsert
        conn = _get_db()
        month_pattern = today.strftime('%Y-%m') + '%'
        conn.execute(
            "DELETE FROM goods_documents WHERE branch_id = ? AND doc_date LIKE ?",
            (branch_id, month_pattern)
        )

        for r in records:
            conn.execute(
                "INSERT OR REPLACE INTO goods_documents (branch_id, doc_date, supplier, ref_number, amount, doc_type) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (branch_id, r['doc_date'], r['supplier'], r['ref_number'], r['amount'], r['doc_type'])
            )
        conn.commit()

        total_amount = sum(r['amount'] for r in records)
        accepted_sum = total_amount  # what we intended to insert

        # ── Post-sync reconciliation ──────────────────────────────
        # Part A: DB vs accepted (catches insert bugs)
        db_total_row = conn.execute(
            "SELECT COALESCE(SUM(amount), 0) as total FROM goods_documents "
            "WHERE branch_id = ? AND doc_date LIKE ?",
            (branch_id, month_pattern)
        ).fetchone()
        db_total = db_total_row['total']
        conn.close()

        diff = abs(db_total - accepted_sum)
        status = 'success'
        message = f"{len(records)} docs, ₪{total_amount:,.0f}"

        # Log reconciliation breakdown to bilboy log
        log.info("Reconciliation breakdown: raw=₪%.2f accepted=₪%.2f (kept) status7=₪%.2f (dropped, lifecycle) unknown=₪%.2f (dropped, alerted)",
                 raw_sum, accepted_sum, status_7_sum, unknown_sum)

        if diff > 10:
            status = 'warning'
            message = f"{len(records)} docs, ₪{total_amount:,.0f} — insert gap ₪{diff:,.0f}"
            log.warning("RECONCILIATION FAIL: DB=%.2f vs accepted=%.2f diff=%.2f — data lost during insert",
                        db_total, accepted_sum, diff)
            notify(f"❌ BilBoy — {branch.get('name', f'Branch {branch_id}')}",
                   f"Reconciliation failed: DB ₪{db_total:,.0f} vs accepted ₪{accepted_sum:,.0f} — gap ₪{diff:,.0f}.")
        else:
            log.info("Reconciliation OK: DB=%.2f accepted=%.2f ✅", db_total, accepted_sum)

        # Part B: Health check — flag high superseded ratio
        if raw_sum > 0 and status_7_sum > raw_sum * 0.3:
            log.warning("High superseded ratio: %.0f%% of raw total is status=7 (₪%.2f / ₪%.2f)",
                        (status_7_sum / raw_sum) * 100, status_7_sum, raw_sum)
            notify(f"⚠️ BilBoy — {branch.get('name', f'Branch {branch_id}')}",
                   f"High superseded ratio: {status_7_sum / raw_sum * 100:.0f}% of docs are status=7 — lots of invoice edits?")

        duration = time.time() - t0
        conn_fin = _get_db()
        conn_fin.execute(
            "UPDATE agent_runs SET finished_at=datetime('now'), status=?, docs_count=?, amount=?, message=?, duration_seconds=? WHERE id=?",
            (status, len(records), total_amount, message, round(duration, 1), run_id)
        )
        conn_fin.commit()
        conn_fin.close()

        log.info("BilBoy sync complete: %d docs, total=%.2f", len(records), total_amount)
        return {'success': True, 'docs_count': len(records), 'total_amount': total_amount}

    except PermissionError:
        log.error("BilBoy token expired for branch %d", branch_id)
        duration = time.time() - t0
        try:
            conn_err = _get_db()
            conn_err.execute(
                "UPDATE agent_runs SET finished_at=datetime('now'), status='error', message='token_expired', duration_seconds=? WHERE id=?",
                (round(duration, 1), run_id)
            )
            conn_err.commit()
            conn_err.close()
        except Exception:
            pass
        notify(
            f"🔑 BilBoy — {branch.get('name', f'Branch {branch_id}')}",
            "BilBoy token expired — needs to be refreshed from the browser."
        )
        return {'success': False, 'docs_count': 0, 'total_amount': 0, 'error': 'token_expired'}

    except Exception as e:
        log.error("BilBoy sync failed: %s", e, exc_info=True)
        duration = time.time() - t0
        try:
            conn_err = _get_db()
            conn_err.execute(
                "UPDATE agent_runs SET finished_at=datetime('now'), status='error', message=?, duration_seconds=? WHERE id=?",
                (str(e)[:500], round(duration, 1), run_id)
            )
            conn_err.commit()
            conn_err.close()
        except Exception:
            pass
        notify(f"❌ BilBoy — {branch.get('name', f'Branch {branch_id}')}", _friendly_bilboy_error(e))
        return {'success': False, 'docs_count': 0, 'total_amount': 0, 'error': str(e)}


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='BilBoy goods sync')
    parser.add_argument('branch_id', nargs='?', type=int, default=126, help='Branch ID')
    parser.add_argument('--branch-id', type=int, dest='branch_id_flag', help='Branch ID (flag form)')
    parser.add_argument('--year', type=int, help='Override year')
    parser.add_argument('--month', type=int, help='Override month')
    args = parser.parse_args()
    bid = args.branch_id_flag or args.branch_id
    # Year/month override: monkey-patch date.today() for the run
    if args.year and args.month:
        _orig_today = date.today
        class _DateOverride(date):
            @classmethod
            def today(cls):
                from calendar import monthrange
                day = min(_orig_today().day, monthrange(args.year, args.month)[1])
                return date(args.year, args.month, day)
        import builtins
        # Patch date.today used in this module
        globals()['date'] = _DateOverride
    print(run_bilboy(bid))

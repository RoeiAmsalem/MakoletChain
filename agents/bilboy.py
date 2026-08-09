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

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests

from utils.notify import notify
from utils.text import clean_supplier_name


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

# Chain-account auth: when BILBOY_USE_CHAIN=1 in env and the branch has
# bilboy_branch_id set, use the single chain JWT (BILBOY_CHAIN_TOKEN) and
# query BilBoy with ?branches=<bilboy_branch_id>. Otherwise fall back to the
# per-store JWT in branches.bilboy_pass + /user/branches[0] discovery.
USE_CHAIN_AUTH = os.environ.get('BILBOY_USE_CHAIN', '').strip().lower() in (
    '1', 'true', 'yes', 'on')
CHAIN_TOKEN_ENV = 'BILBOY_CHAIN_TOKEN'

# BilBoy document lifecycle statuses:
#   3 = active invoice
#   5 = accepted/processed
#   7 = replacement invoice (visible in BilBoy UI — keep)
#   9 = superseded original (hidden in BilBoy UI — drop)
# Unknown statuses are dropped and alerted via brrr so we notice new lifecycle states.
KNOWN_STATUSES = {3, 5, 7, 9}
EXCLUDED_STATUSES = {9}

# ── Wolt fee extraction (זיכיונות המכולת type-3 invoices) ────────────────────
# Wolt commissions are billed to each store via the franchise supplier, whose
# docs are otherwise excluded from goods entirely. The fee lines carry BilBoy
# CATEGORY codes in catalogNumber (61/62/63/67/69 — captured live from the
# 9020/9018/9015 itemization), with names like:
#   עמלת וולט 17% + מע"מ · וואלט + עמלה 17%+ מעמ · דמי שירות קבועים וולט + 3.8
#   למשלוח · קמפיין וולט · דמי שירות עמלה 10%
# A line is a Wolt fee if its category code matches OR its name mentions
# וולט/וואלט (belt and braces — the 10% service-fee line has no וולט in the
# name but always sits on a pure-Wolt doc and carries a Wolt category code).
# These lines are bucket-B operating fees: NEVER goods, NEVER the 5% royalty.
WOLT_LINE_CATS = {'61', '62', '63', '67', '69'}
WOLT_EXPENSE_NAME = 'עמלות Wolt'
WOLT_EXPENSE_SOURCE = 'bilboy_wolt'


def is_wolt_fee_line(item: dict) -> bool:
    cat = str(item.get('catalogNumber') or item.get('barcode') or '').strip()
    name = str(item.get('name') or '')
    return cat in WOLT_LINE_CATS or 'וולט' in name or 'וואלט' in name


def extract_wolt_fee_total(docs: list) -> float:
    """Incl-VAT Wolt fee total across franchise type-3 docs with line items.

    Each doc: {'totalWithVat', 'totalWithoutVat', 'items': [{name,
    catalogNumber, total}]} — line totals are ex-VAT. A doc whose Wolt lines
    sum to its own totalWithoutVat (±₪1) is a pure-Wolt invoice → use its
    totalWithVat verbatim (this is what makes the audited anchors land to the
    shekel). A mixed doc contributes its Wolt lines scaled by the doc's own
    VAT ratio.
    """
    total = 0.0
    for d in docs:
        items = d.get('items') or []
        wolt_ex = sum(float(it.get('total') or 0)
                      for it in items if is_wolt_fee_line(it))
        if not wolt_ex:
            continue
        twv = float(d.get('totalWithVat') or 0)
        two = float(d.get('totalWithoutVat') or 0)
        if two and abs(wolt_ex - two) <= 1.0:
            total += twv
        elif two:
            total += wolt_ex * (twv / two)
        else:
            total += wolt_ex
    return round(total, 2)


def upsert_wolt_fee_expense(conn, branch_id: int, month: str, amount: float) -> str:
    """Write the system-managed 'עמלות Wolt' fixed-expense row for branch+month.

    amount > 0  → insert-or-overwrite OUR row (source='bilboy_wolt'). The ON
                  CONFLICT WHERE guard means a manual row that happens to share
                  the name is NEVER hijacked — we detect and report 'blocked'.
    amount == 0 → delete our row if present (a no-Wolt branch shows nothing).
    Idempotent by construction; the nightly recompute makes the row grow MTD.
    """
    amount = round(float(amount or 0), 2)
    if amount > 0.005:
        conn.execute(
            "INSERT INTO fixed_expenses "
            "(branch_id, month, name, amount, expense_type, pct_value, locked, source) "
            "VALUES (?, ?, ?, ?, 'monthly', NULL, 1, ?) "
            "ON CONFLICT(branch_id, month, name) DO UPDATE SET "
            "  amount=excluded.amount, locked=1, source=excluded.source "
            "  WHERE fixed_expenses.source=?",
            (branch_id, month, WOLT_EXPENSE_NAME, amount,
             WOLT_EXPENSE_SOURCE, WOLT_EXPENSE_SOURCE))
        conn.commit()
        row = conn.execute(
            "SELECT source FROM fixed_expenses WHERE branch_id=? AND month=? AND name=?",
            (branch_id, month, WOLT_EXPENSE_NAME)).fetchone()
        src = row['source'] if row else None
        return 'upserted' if src == WOLT_EXPENSE_SOURCE else 'blocked_by_manual_row'
    cur = conn.execute(
        "DELETE FROM fixed_expenses WHERE branch_id=? AND month=? AND name=? AND source=?",
        (branch_id, month, WOLT_EXPENSE_NAME, WOLT_EXPENSE_SOURCE))
    conn.commit()
    return 'deleted' if cur.rowcount else 'none'


def fetch_wolt_fee_docs(session, bb_branch_id: str, franchise_ids: list,
                        from_date: str, to_date: str, log) -> tuple:
    """Fetch the franchise supplier's type-3 invoices with line items.

    Returns (docs, api_calls). Only type-3 invoices are fetched in detail —
    Wolt fees are always billed on type-3 (delivery notes/credits never carry
    them), which keeps the added BilBoy cost to 1 headers call + one detail
    call per franchise invoice (~5-10/branch/month).
    """
    if not franchise_ids:
        return [], 0
    calls = 0
    headers = _api_get(session, '/customer/docs/headers', params={
        'suppliers': ','.join(franchise_ids),
        'branches': bb_branch_id,
        'from': f'{from_date}T00:00:00',
        'to': f'{to_date}T00:00:00',
    })
    calls += 1
    hlist = headers if isinstance(headers, list) else (
        headers.get('data') or headers.get('docs') or headers.get('headers') or [])
    docs = []
    for h in hlist:
        if h.get('type') != 3 or h.get('status') in EXCLUDED_STATUSES:
            continue
        doc_id = h.get('id')
        if not doc_id:
            continue
        try:
            raw = _api_get(session, '/customer/doc',
                           params={'docId': doc_id}, timeout=15)
            calls += 1
        except Exception as e:
            log.warning("wolt-fees: doc detail failed for %s: %s",
                        doc_id, str(e)[:120])
            continue
        body = (raw or {}).get('body') or {}
        docs.append({
            'totalWithVat': h.get('totalWithVat'),
            'totalWithoutVat': h.get('totalWithoutVat'),
            'items': [{'name': it.get('name'),
                       'catalogNumber': it.get('catalogNumber') or it.get('barcode'),
                       'total': it.get('total')}
                      for it in (body.get('items') or [])],
        })
    return docs, calls


def sync_wolt_fees(session, bb_branch_id: str, branch_id: int,
                   franchise_ids: list, from_date: str, to_date: str,
                   month: str, log) -> dict:
    """Recompute + upsert the month's Wolt fee row from the franchise docs."""
    docs, calls = fetch_wolt_fee_docs(session, bb_branch_id, franchise_ids,
                                      from_date, to_date, log)
    amount = extract_wolt_fee_total(docs)
    conn = _get_db()
    try:
        action = upsert_wolt_fee_expense(conn, branch_id, month, amount)
    finally:
        conn.close()
    log.info("wolt-fees: branch=%d month=%s docs=%d amount=₪%.2f action=%s (+%d BilBoy calls)",
             branch_id, month, len(docs), amount, action, calls)
    if action == 'blocked_by_manual_row':
        log.warning("wolt-fees: manual row named %r exists for branch=%d month=%s — system row NOT written",
                    WOLT_EXPENSE_NAME, branch_id, month)
    return {'amount': amount, 'action': action, 'api_calls': calls,
            'docs': len(docs)}


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


def _api_get(session: requests.Session, path: str, params=None, timeout=30):
    url = f"{API_BASE}{path}"
    resp = session.get(url, params=params, timeout=timeout)
    if resp.status_code == 401:
        raise PermissionError("BilBoy token expired")
    resp.raise_for_status()
    return resp.json()


def _branch_session(branch: dict, branch_id: int) -> requests.Session:
    """Build an authenticated BilBoy session for a branch using the same
    chain-token / per-store-token resolution as run_bilboy."""
    chain_token = os.environ.get(CHAIN_TOKEN_ENV) or ''
    mapped_bb_id = branch.get('bilboy_branch_id')
    if USE_CHAIN_AUTH and chain_token and mapped_bb_id:
        token = chain_token
    else:
        token = branch.get('bilboy_pass') or ''
    if not token:
        raise ValueError(f"No BilBoy token for branch {branch_id}")
    session = requests.Session()
    session.headers.update({'Authorization': f'Bearer {token}'})
    return session


def fetch_doc_detail(branch_id: int, bilboy_doc_id: str) -> dict:
    """ON-DEMAND fetch of a single document's line items from BilBoy.

    Calls GET /customer/doc?docId=<uuid>. Result is NOT stored — this exists so
    the /goods click-into-invoice view can show what was bought without the
    ~2,560 calls/month a nightly pre-fetch would cost. Returns the raw
    {header, body} dict from BilBoy; the caller normalizes it for the UI.
    """
    branch = _get_branch_config(branch_id)
    session = _branch_session(branch, branch_id)
    # Shorter timeout than the nightly sync — this serves a live user click, so
    # a hung BilBoy connection must surface as an error fast, not hold the
    # request open. The frontend's abort timeout sits just above this (12s).
    return _api_get(session, '/customer/doc', params={'docId': bilboy_doc_id},
                    timeout=10)


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
        franchise_supplier = branch.get('franchise_supplier') or 'זיכיונות המכולת בע"מ'

        # ── Auth path: chain (env token + bilboy_branch_id) vs per-store ──
        chain_token = os.environ.get(CHAIN_TOKEN_ENV) or ''
        mapped_bb_id = branch.get('bilboy_branch_id')
        if USE_CHAIN_AUTH and chain_token and mapped_bb_id:
            auth_source = 'chain'
            token = chain_token
            bb_branch_id = str(mapped_bb_id)
            log.info("Auth path: chain (BILBOY_USE_CHAIN=1, bilboy_branch_id=%s)",
                     bb_branch_id)
        else:
            auth_source = 'per_store'
            token = branch.get('bilboy_pass') or ''
            if not token:
                log.warning("No BilBoy token for branch %d (per-store fallback)", branch_id)
                return {'success': False, 'docs_count': 0, 'total_amount': 0, 'error': 'no token'}
            log.info("Auth path: per_store (no chain token/flag/mapping for branch %d)",
                     branch_id)

        session = requests.Session()
        session.headers.update({'Authorization': f'Bearer {token}'})

        # Resolve BilBoy branch id. In chain mode we already know it from
        # branches.bilboy_branch_id and skip the per-store discovery call.
        if auth_source != 'chain':
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
        franchise_ids = []          # kept OUT of goods; used for Wolt fee extraction
        if suppliers:
            for s in suppliers:
                name = s.get('title') or s.get('name') or s.get('supplierName') or ''
                sid = str(s.get('id') or s.get('supplierId') or '')
                if franchise_supplier and franchise_supplier in name:
                    log.info("Filtered out franchise supplier: %s", name)
                    if sid:
                        franchise_ids.append(sid)
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

        # ── Status filter: drop superseded (status=9) and unknown statuses ──
        raw_sum = sum(float(d.get('totalWithVat') or 0) for d in docs)
        excluded_sum = 0
        unknown_sum = 0
        skip_superseded = 0
        skip_unknown = 0
        status_filtered = []
        for doc in docs:
            status = doc.get('status')
            if status in EXCLUDED_STATUSES:
                skip_superseded += 1
                excluded_sum += float(doc.get('totalWithVat') or 0)
                continue
            if status is not None and status not in KNOWN_STATUSES:
                skip_unknown += 1
                unknown_sum += float(doc.get('totalWithVat') or 0)
                ref = doc.get('refNumber') or doc.get('number') or '?'
                log.warning("Unknown BilBoy status %s for doc ref=%s", status, ref)
                notify(f"⚠️ BilBoy — {branch.get('name', f'Branch {branch_id}')}",
                       f"Unknown BilBoy status {status} for doc {ref} on branch {branch_id} — please review.")
                continue
            status_filtered.append(doc)

        if skip_superseded > 0:
            log.info("Dropped %d superseded (status=9) docs totaling ₪%.2f", skip_superseded, excluded_sum)
        if skip_unknown > 0:
            log.warning("Dropped %d unknown-status docs totaling ₪%.2f", skip_unknown, unknown_sum)

        docs = status_filtered

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

            # Normalize at write time so goods_documents never stores the
            # trailing-\n / stray-whitespace variants BilBoy sometimes returns
            # (the root cause of the budget duplicate-supplier bug). Only the
            # stored string is cleaned — dedup/franchise/status logic unchanged.
            supplier = clean_supplier_name(doc.get('supplierName'))
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
                # FREE enrichment fields (already in this response) — display only,
                # not part of dedup. See migration 024.
                'total_without_vat': float(doc.get('totalWithoutVat') or 0) or None,
                'paid': 1 if doc.get('paid') else 0,
                'bilboy_status': doc.get('status'),
                'bilboy_doc_id': doc.get('id'),
            })

        # Dedup by ref_number
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
                 len(records), skip_franchise, skip_zeros, skip_type, skip_superseded, skip_unknown)

        # Full month delete + reinsert
        conn = _get_db()
        month_pattern = today.strftime('%Y-%m') + '%'
        conn.execute(
            "DELETE FROM goods_documents WHERE branch_id = ? AND doc_date LIKE ?",
            (branch_id, month_pattern)
        )

        for r in records:
            conn.execute(
                "INSERT OR REPLACE INTO goods_documents "
                "(branch_id, doc_date, supplier, ref_number, amount, doc_type, "
                " total_without_vat, paid, bilboy_status, bilboy_doc_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (branch_id, r['doc_date'], r['supplier'], r['ref_number'], r['amount'], r['doc_type'],
                 r['total_without_vat'], r['paid'], r['bilboy_status'], r['bilboy_doc_id'])
            )
        conn.commit()

        total_amount = sum(r['amount'] for r in records)
        accepted_sum = total_amount

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

        # Reconciliation breakdown log
        month_str = today.strftime('%Y-%m')
        recon_ok = '✅' if diff <= 10 else '❌'
        log.info("Reconciliation: branch=%d month=%s raw=₪%.2f accepted=₪%.2f excluded_status9=₪%.2f unknown=₪%.2f %s",
                 branch_id, month_str, raw_sum, accepted_sum, excluded_sum, unknown_sum, recon_ok)

        # ── Wolt fee row (system fixed-expense) — never fails the sync ──
        # Recomputes the current month's עמלות Wolt from the franchise docs
        # and upserts the source='bilboy_wolt' row. MTD semantics: the row
        # grows as new franchise invoices land during the month.
        try:
            sync_wolt_fees(session, bb_branch_id, branch_id, franchise_ids,
                           from_date, to_date, month_str, log)
        except Exception as e:
            log.warning("wolt-fees sync failed (goods sync unaffected): %s",
                        str(e)[:200])

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
        if raw_sum > 0 and excluded_sum > raw_sum * 0.2:
            log.warning("High superseded ratio: %.0f%% of raw total is status=9 (₪%.2f / ₪%.2f)",
                        (excluded_sum / raw_sum) * 100, excluded_sum, raw_sum)

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
        # Critical + chain-wide: one token serves all branches, so a 401 storms
        # every branch in the run. Fixed dedup_key collapses it to ONE page.
        notify(
            f"🔑 BilBoy — {branch.get('name', f'Branch {branch_id}')}",
            "BilBoy token expired — needs to be refreshed from the browser.",
            critical=True, dedup_key="bilboy_token_expired"
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
    parser.add_argument('--all-active', action='store_true',
                        help='Iterate every active branch with per-branch try/except '
                             '(mirrors scheduler.nightly_sync). Used by staging cron.')
    args = parser.parse_args()
    if args.year and args.month:
        _orig_today = date.today
        class _DateOverride(date):
            @classmethod
            def today(cls):
                # run_bilboy uses today() as the window END (from = the 1st).
                # For a PAST month that end must be the month's last day —
                # clamping to the real today's day-of-month truncated the
                # window (e.g. re-pulling July on Aug 9 fetched Jul 1-9) and,
                # because the sync is full-month delete + reinsert, silently
                # DESTROYED the rest of the month. Only the current month is
                # clamped, where a future end date is meaningless anyway.
                from calendar import monthrange
                last = monthrange(args.year, args.month)[1]
                now = _orig_today()
                if (args.year, args.month) < (now.year, now.month):
                    day = last
                else:
                    day = min(now.day, last)
                return date(args.year, args.month, day)
        globals()['date'] = _DateOverride

    if args.all_active:
        conn = _get_db()
        ids = [r['id'] for r in conn.execute(
            'SELECT id FROM branches WHERE active=1 AND agents_enabled=1 ORDER BY id').fetchall()]
        conn.close()
        print(f"[bilboy_all] running for {len(ids)} active branches: {ids}")
        ok = fail = 0
        for bid in ids:
            try:
                r = run_bilboy(bid)
                ok += 1 if r.get('success') else 0
                fail += 0 if r.get('success') else 1
                print(f"[bilboy_all] branch={bid} {r}")
            except Exception as e:
                fail += 1
                print(f"[bilboy_all] branch={bid} EXC {e!r}")
        print(f"[bilboy_all] done: ok={ok} fail={fail}")
    else:
        bid = args.branch_id_flag or args.branch_id
        print(run_bilboy(bid))

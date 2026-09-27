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
from utils.sync_window import il_today, month_bounds, months_to_sync, prev_month
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
# Category codes that NEVER count even if the name matched: 65 is the iPad
# hardware line that rides on the same franchise invoices (seen Aug 2026).
WOLT_EXCLUDED_CATS = {'65'}
WOLT_EXPENSE_NAME = 'עמלות Wolt'
WOLT_EXPENSE_SOURCE = 'bilboy_wolt'
# Wolt fees arrive on invoices (3) and are reversed on credit notes (4).
WOLT_DOC_TYPES = {3, 4}
# 3 open · 5 accepted · 7 replacement · 11 settled/archived. 9 (superseded)
# never counts; any other status is skipped and logged.
WOLT_COUNTED_STATUSES = {3, 5, 7, 11}
# A manager-entered row whose name contains any of these is (probably) the
# same money: the system row is NOT written and brrr is alerted instead.
WOLT_MANUAL_NAME_HINTS = ('וולט', 'וואלט', 'wolt', 'עמלות מכירה', 'משלוחים')
# Manager rows that match a hint above but were REVIEWED and confirmed NOT to
# be Wolt fees. Exact (branch_id, name) pairs only — the same name at another
# branch, or any other name at this branch, still blocks + alerts.
WOLT_GUARD_CONFIRMED_NOT_WOLT = {
    # reviewed 2026-09-27 by Roei: 9016 קריית טבעון, ₪158/mo, not Wolt
    (9016, 'עמלות מכירה'),
}


def is_wolt_fee_line(item: dict) -> bool:
    cat = str(item.get('catalogNumber') or item.get('barcode') or '').strip()
    if cat in WOLT_EXCLUDED_CATS:
        return False
    name = str(item.get('name') or '')
    return cat in WOLT_LINE_CATS or 'וולט' in name or 'וואלט' in name


def extract_wolt_fee_total(docs: list) -> float:
    """Incl-VAT Wolt fee total across franchise invoice/credit docs.

    Each doc: {'type', 'totalWithVat', 'totalWithoutVat', 'items': [{name,
    catalogNumber, total}]} — line totals are ex-VAT. Only the Wolt lines
    count, scaled by the doc's own VAT ratio (totalWithVat/totalWithoutVat),
    so a pure-Wolt invoice lands exactly on its incl-VAT total and a mixed
    doc contributes only its Wolt share. A type-4 credit note always REDUCES
    the total (BilBoy sends it negative; the sign is forced defensively).
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
        amt = wolt_ex * (twv / two) if two else wolt_ex
        if d.get('type') == 4:
            amt = -abs(amt)
        total += amt
    return round(total, 2)


def _manual_wolt_like_rows(conn, branch_id: int, month: str) -> list:
    """Names of non-system fixed_expenses rows for branch+month that look like
    Wolt fees (see WOLT_MANUAL_NAME_HINTS), minus the reviewed exact
    (branch_id, name) pairs in WOLT_GUARD_CONFIRMED_NOT_WOLT."""
    rows = conn.execute(
        "SELECT name FROM fixed_expenses WHERE branch_id=? AND month=? "
        "AND COALESCE(source, 'manual') != ?",
        (branch_id, month, WOLT_EXPENSE_SOURCE)).fetchall()
    return [r[0] for r in rows
            if any(h in (r[0] or '').lower() for h in WOLT_MANUAL_NAME_HINTS)
            and (branch_id, r[0]) not in WOLT_GUARD_CONFIRMED_NOT_WOLT]


def upsert_wolt_fee_expense(conn, branch_id: int, month: str, amount: float) -> str:
    """Write the system-managed 'עמלות Wolt' fixed-expense row for branch+month.

    amount > 0  → insert-or-overwrite OUR row (source='bilboy_wolt'), UNLESS a
                  manager row looks like the same money (fuzzy name, see
                  WOLT_MANUAL_NAME_HINTS) → nothing written, 'blocked_by_manual_row'.
                  The ON CONFLICT WHERE guard is the second lock: a manual row
                  sharing the exact name is never hijacked.
    amount <= 0 → delete our row if present (a no-Wolt branch shows nothing).
    Only ever writes/deletes source='bilboy_wolt' rows. Idempotent.
    """
    amount = round(float(amount or 0), 2)
    if amount > 0.005:
        if _manual_wolt_like_rows(conn, branch_id, month):
            return 'blocked_by_manual_row'
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
        src = row[0] if row else None
        return 'upserted' if src == WOLT_EXPENSE_SOURCE else 'blocked_by_manual_row'
    cur = conn.execute(
        "DELETE FROM fixed_expenses WHERE branch_id=? AND month=? AND name=? AND source=?",
        (branch_id, month, WOLT_EXPENSE_NAME, WOLT_EXPENSE_SOURCE))
    conn.commit()
    return 'deleted' if cur.rowcount else 'none'


def fetch_wolt_fee_docs(session, bb_branch_id: str, franchise_ids: list,
                        from_date: str, to_date: str, log) -> tuple:
    """Fetch the franchise supplier's invoices + credit notes with line items.

    Returns (docs, api_calls, detail_failures). Cost: 1 headers call + one
    detail call per counted type-3/4 franchise doc (~5-15/branch/month).
    Docs are windowed by their DATE, so an invoice dated the 31st but issued
    days later still lands in its date's month.
    """
    if not franchise_ids:
        return [], 0, 0
    calls = 0
    failures = 0
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
        if h.get('type') not in WOLT_DOC_TYPES:
            continue
        status = h.get('status')
        if status not in WOLT_COUNTED_STATUSES:
            if status not in EXCLUDED_STATUSES:
                log.warning("wolt-fees: skipping doc ref=%s with unknown status %s",
                            h.get('refNumber'), status)
            continue
        doc_id = h.get('id')
        if not doc_id:
            continue
        try:
            raw = _api_get(session, '/customer/doc',
                           params={'docId': doc_id}, timeout=15)
            calls += 1
        except Exception as e:
            failures += 1
            log.warning("wolt-fees: doc detail failed for %s: %s",
                        doc_id, str(e)[:120])
            continue
        body = (raw or {}).get('body') or {}
        docs.append({
            'type': h.get('type'),
            'totalWithVat': h.get('totalWithVat'),
            'totalWithoutVat': h.get('totalWithoutVat'),
            'items': [{'name': it.get('name'),
                       'catalogNumber': it.get('catalogNumber') or it.get('barcode'),
                       'total': it.get('total')}
                      for it in (body.get('items') or [])],
        })
    return docs, calls, failures


def sync_wolt_fees(session, bb_branch_id: str, branch_id: int,
                   franchise_ids: list, month: str, log,
                   branch_name: str = None) -> dict:
    """Recompute + upsert one month's Wolt fee row from the franchise docs
    (full calendar month). A partial read (any doc-detail failure) writes
    nothing — a transient glitch must never shrink or delete a good row."""
    from_date, to_date = month_bounds(month)
    docs, calls, failures = fetch_wolt_fee_docs(
        session, bb_branch_id, franchise_ids, from_date, to_date, log)
    amount = extract_wolt_fee_total(docs)
    bname = branch_name or f'Branch {branch_id}'
    if failures:
        action = 'skipped_partial_read'
    else:
        conn = _get_db()
        try:
            action = upsert_wolt_fee_expense(conn, branch_id, month, amount)
            blockers = (_manual_wolt_like_rows(conn, branch_id, month)
                        if action == 'blocked_by_manual_row' else [])
        finally:
            conn.close()
        if action == 'blocked_by_manual_row':
            log.warning("wolt-fees: manager row(s) %s look like Wolt fees for "
                        "branch=%d month=%s — system row NOT written",
                        blockers, branch_id, month)
            notify(f"⚠️ Wolt fees — {bname}",
                   f"{month}: BilBoy Wolt fees ₪{amount:,.2f} NOT written — a "
                   f"manager expense row looks like the same money "
                   f"({', '.join(blockers) or WOLT_EXPENSE_NAME}). Review "
                   f"fixed expenses to avoid double counting.")
    log.info("wolt-fees: branch=%d month=%s docs=%d amount=₪%.2f action=%s (+%d BilBoy calls)",
             branch_id, month, len(docs), amount, action, calls)
    return {'month': month, 'amount': amount, 'action': action,
            'api_calls': calls, 'docs': len(docs), 'detail_failures': failures}


def check_prev_month_wolt(session, bb_branch_id: str, branch_id: int,
                          franchise_ids: list, month: str, log,
                          branch_name: str = None) -> dict:
    """Outside the re-read window: if the previous month still has NO Wolt
    row but BilBoy now holds a Wolt invoice for it, alert — never write
    (a month outside the window is written only by the backfill script)."""
    conn = _get_db()
    try:
        row = conn.execute(
            "SELECT amount FROM fixed_expenses WHERE branch_id=? AND month=? "
            "AND name=? AND source=?",
            (branch_id, month, WOLT_EXPENSE_NAME, WOLT_EXPENSE_SOURCE)).fetchone()
        manual = _manual_wolt_like_rows(conn, branch_id, month)
    finally:
        conn.close()
    if (row and float(row[0] or 0) > 0) or manual:
        return {'month': month, 'checked': False}
    from_date, to_date = month_bounds(month)
    docs, calls, _ = fetch_wolt_fee_docs(
        session, bb_branch_id, franchise_ids, from_date, to_date, log)
    amount = extract_wolt_fee_total(docs)
    if amount > 0.005:
        bname = branch_name or f'Branch {branch_id}'
        log.warning("wolt-fees: %s still ₪0 but BilBoy has ₪%.2f Wolt fees for branch=%d",
                    month, amount, branch_id)
        notify(f"⚠️ Wolt fees missing — {bname}",
               f"{month} Wolt fees still ₪0 on the dashboard but BilBoy has "
               f"₪{amount:,.2f} — arrived after the re-read window. Run "
               f"scripts/backfill_wolt_fees.py --months {month} "
               f"--branches {branch_id} --apply.")
    return {'month': month, 'checked': True, 'amount': amount, 'api_calls': calls}


def sync_wolt_fees_window(session, bb_branch_id: str, branch_id: int,
                          franchise_ids: list, log, today_il: date = None,
                          branch_name: str = None) -> list:
    """Nightly Wolt entry point: sync every month in months_to_sync(today IL)
    — [prev, current] on days 1..BILBOY_PREV_MONTH_DAYS, else [current] plus
    a read-only late-invoice check on the previous month."""
    today_il = today_il or il_today()
    months = months_to_sync(today_il)
    results = [sync_wolt_fees(session, bb_branch_id, branch_id, franchise_ids,
                              m, log, branch_name) for m in months]
    if len(months) == 1:
        results.append(check_prev_month_wolt(
            session, bb_branch_id, branch_id, franchise_ids,
            prev_month(months[0]), log, branch_name))
    return results


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
        # TODO(goods window): goods still reads only the current month (and by
        # the server's UTC date), so status-7 reissues and late invoices that
        # BilBoy adds for last month after the 1st are never picked up. Once
        # migration 046 (UNIQUE branch+ref+supplier+date) is on prod, move goods
        # onto utils.sync_window.months_to_sync — the same mechanism the Wolt
        # rows use below — re-pulling each month in the list.
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

        # Dedup on exactly the key the table is UNIQUE on (migration 046):
        # ref_number + supplier + doc_date (branch_id is fixed for this run).
        # Keying on ref_number alone was the in-run twin of the eviction bug —
        # it silently dropped a second supplier's document that happened to
        # reuse the same number, before the DB ever saw it.
        seen = set()
        deduped = []
        for r in records:
            key = (r['ref_number'], r['supplier'], r['doc_date'])
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

        # ── Wolt fee rows (system fixed-expense) — never fails the sync ──
        # Own window, independent of the goods window above: months_to_sync
        # (Israel date) re-reads the PREVIOUS month on days 1-7, because Wolt
        # invoices are dated the 31st but issued in the next month's first
        # days. MTD semantics within a month: the row grows as invoices land.
        try:
            sync_wolt_fees_window(session, bb_branch_id, branch_id,
                                  franchise_ids, log,
                                  branch_name=branch.get('name'))
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
                from calendar import monthrange
                day = min(_orig_today().day, monthrange(args.year, args.month)[1])
                return date(args.year, args.month, day)
        globals()['date'] = _DateOverride

    if args.all_active:
        conn = _get_db()
        ids = [r['id'] for r in conn.execute(
            'SELECT id FROM branches WHERE active=1 ORDER BY id').fetchall()]
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

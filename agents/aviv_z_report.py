"""Aviv BI report 902 (העתק Z) parallel validation agent — STAGING ONLY.

Mirrors what the prod Gmail-Z agent extracts (total + transactions) per branch
per day, sourced from the BI 902 PDF instead of email. Writes to z_report_902
(separate table) so the Gmail-Z → daily_sales path is untouched.

Pipeline per branch:
  1. POST /avivbi/v2/account/login                      → token + aviv_branch_id
  2. GET  /avivbi/v2/reports/filters/902?branch=X       → Z-list (Z↔date pairs)
  3. POST /avivbi/v2/reports/result/?branch=X           → JSON {url}
       body: id=902, outputType=PDF, filters=[ID_Z, TO_Z] (both = resolved Z)
  4. GET  the returned url                              → PDF bytes
  5. parse PDF → total / transactions / avg / payment breakdown
  6. UPSERT into z_report_902 by (branch_id, date)

Each branch is independent — one branch failing must not stop the loop.
401 on the submit triggers exactly one re-login + retry.
"""

import io
import json
import logging
import os
import re
import sqlite3
import time
from datetime import date, datetime, timedelta

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)

BASE = 'https://bi1.aviv-pos.co.il:8443/avivbi/v2'
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
Z_REPORT_ID = 902

# Where /sales reads PDF previews from (mirror of app.py's PDF_BASE). Saving 902
# PDFs here makes the "צפה" preview work on 902 rows identically to Gmail-Z rows.
PDF_BASE = os.path.join(os.path.dirname(__file__), '..', 'data', 'pdfs')

# Retry policy for transient Aviv failures on filters/902 (404/5xx/network).
# Closed-day "no Z for date" is a 200 response and does NOT consume retries.
FILTERS_MAX_ATTEMPTS = 3
FILTERS_RETRY_BACKOFF_SEC = 2

# Chain-account auth: when AVIV_Z_USE_CHAIN=1 in env, run_all_branches logs in
# ONCE with chain creds (AVIV_CHAIN_USER / AVIV_CHAIN_PASS) and reuses the
# token for every branch. The URL branch param comes from branches.aviv_branch_id.
# Default OFF so existing per-branch behavior is preserved unless explicitly flipped.
USE_CHAIN_AUTH = os.environ.get('AVIV_Z_USE_CHAIN', '').strip().lower() in (
    '1', 'true', 'yes', 'on')
CHAIN_USER_ENV = 'AVIV_CHAIN_USER'
CHAIN_PASS_ENV = 'AVIV_CHAIN_PASS'

# Mirror successful 902 pulls into daily_sales so the dashboard's existing
# Z source picks them up. INSERT OR IGNORE — never overwrites a Gmail-Z row
# or a previously-mirrored row. Closed-day sentinels do NOT mirror.
MIRROR_TO_DAILY_SALES = os.environ.get('AVIV_Z_TO_DAILY_SALES', '').strip().lower() in (
    '1', 'true', 'yes', 'on')


# ---- PDF parsing ----------------------------------------------------------

# Aviv PDF text comes out RTL-reversed (visual order), so we match the
# reversed Hebrew strings. The gmail-Z parser uses the same trick — these are
# extended to cover the payment breakdown lines that appear in 902.
TOTAL_RTL = re.compile(r'([\d,]+\.?\d*)\s*₪\s*:כ"הס')
TXNS_RTL = re.compile(r'(\d+)\s*תואקסע\s*תומכ')
AVG_RTL = re.compile(r'([\d,]+\.?\d*)\s*₪\s*הקסעל\s*עצוממ')

# Sale-by-payment lines — value first, then "₪ ... הריכמ" (RTL of "מכירה ...").
CASH_RTL = re.compile(r'([\d,]+\.?\d*)\s*₪\s*ןמוזמב\s*הריכמ')
CHECK_RTL = re.compile(r'([\d,]+\.?\d*)\s*₪\s*קיצב\s*הריכמ')
CREDIT_RTL = re.compile(r'([\d,]+\.?\d*)\s*₪\s*יארשא\s*סיטרכב\s*הריכמ')
HAKAFA_RTL = re.compile(r'([\d,]+\.?\d*)\s*₪\s*הפקהב\s*הריכמ')
TRANSFER_RTL = re.compile(r'([\d,]+\.?\d*)\s*₪\s*תיאקנב\s*הרבעהב\s*הריכמ')
# סועד is reported as a debt payment line, not a sale. Number may abut ₪.
SOED_RTL = re.compile(r'([\d,]+\.?\d*)\s*₪?\s*דעוס\s*יסיטרכב\s*בוח\s*םולשת')


def _to_float(s: str | None) -> float | None:
    if s is None:
        return None
    try:
        return float(s.replace(',', ''))
    except (ValueError, AttributeError):
        return None


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Try pdfplumber first, fall back to PyMuPDF — same pattern as Gmail-Z."""
    text = ''
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text += (page.extract_text() or '') + '\n'
    except Exception:
        text = ''
    if not text.strip():
        try:
            import fitz
            doc = fitz.open(stream=pdf_bytes, filetype='pdf')
            for page in doc:
                text += page.get_text() + '\n'
            doc.close()
        except Exception:
            pass
    return text


def parse_902_pdf(pdf_bytes: bytes) -> dict:
    """Parse a Z-902 PDF → {total, transactions, avg_per_txn, payment_breakdown}.

    payment_breakdown contains the six payment-method amounts seen on the Z:
    cash / credit / hakafa / soed / check / transfer. Missing methods stay None.
    """
    text = _extract_pdf_text(pdf_bytes)

    total = _to_float(TOTAL_RTL.search(text).group(1)) if TOTAL_RTL.search(text) else None
    txns_m = TXNS_RTL.search(text)
    txns = int(txns_m.group(1)) if txns_m else None
    avg = _to_float(AVG_RTL.search(text).group(1)) if AVG_RTL.search(text) else None

    def _g(pat):
        m = pat.search(text)
        return _to_float(m.group(1)) if m else None

    payment_breakdown = {
        'cash': _g(CASH_RTL),
        'credit': _g(CREDIT_RTL),
        'hakafa': _g(HAKAFA_RTL),
        'soed': _g(SOED_RTL),
        'check': _g(CHECK_RTL),
        'transfer': _g(TRANSFER_RTL),
    }

    return {
        'total': total,
        'transactions': txns,
        'avg_per_txn': avg,
        'payment_breakdown': payment_breakdown,
    }


# ---- Aviv API -------------------------------------------------------------

class AuthExpired(Exception):
    """Raised on 401 — caller re-logs in and retries once."""


def _login(username: str, password: str) -> tuple[str, int | None]:
    """POST /account/login → (token, first aviv_branch_id)."""
    r = requests.post(f'{BASE}/account/login',
                      json={'user': username, 'password': password},
                      timeout=15, verify=False)
    r.raise_for_status()
    data = r.json()
    token = data.get('token') or data.get('value')
    branches = data.get('branches', [])
    aviv_branch_id = branches[0]['id'] if branches else None
    return token, aviv_branch_id


def _login_chain_account() -> str:
    """Login with chain-owner creds from env. Returns token. Never logs the password."""
    user = os.environ.get(CHAIN_USER_ENV)
    pw = os.environ.get(CHAIN_PASS_ENV)
    if not user or not pw:
        raise RuntimeError(
            f'{CHAIN_USER_ENV} / {CHAIN_PASS_ENV} not set in env')
    r = requests.post(f'{BASE}/account/login',
                      json={'user': user, 'password': pw},
                      timeout=15, verify=False)
    r.raise_for_status()
    data = r.json() or {}
    token = data.get('token') or data.get('value')
    if not token:
        raise Exception('chain login response missing token')
    return token


def _refresh(token: str) -> str:
    time.sleep(0.3)
    r = requests.post(f'{BASE}/account/refresh',
                      headers={'Authtoken': token, 'Content-Type': 'application/json'},
                      json={}, timeout=10, verify=False)
    j = r.json()
    return j.get('token') or j.get('value') or token


def fetch_902_filters(aviv_branch_id: int, token: str) -> dict:
    """GET /reports/filters/902?branch=X → raw JSON (contains Z list)."""
    url = f'{BASE}/reports/filters/{Z_REPORT_ID}?branch={aviv_branch_id}'
    r = requests.get(url, headers={'Authtoken': token}, timeout=30, verify=False)
    if r.status_code == 401:
        raise AuthExpired('filters/902 401')
    r.raise_for_status()
    return r.json()


_Z_LABEL_RE = re.compile(r'Z:\s*(\d+)\s*\|\s*(\d{1,2}/\d{1,2}/\d{2,4})')


def _iter_z_entries(filters_json) -> list[dict]:
    """Walk the filters JSON and return a flat list of {z_number, date} entries.

    Aviv's actual shape (captured live):
      [{"id":1,"name":"ID_Z","filterType":"INTEGER","possibleValues":[
          {"2525":"Z: 2525|20/05/2026"}, ...]}, ...]

    Each Z option is a single-key dict whose key is the Z number (as a string)
    and whose value is a "Z: <num>|DD/MM/YYYY" label.

    The walk also accepts a couple of likely alternate shapes ({key,value}
    pairs, ISO date strings) so future schema variants don't silently break
    the agent. The label parser is what unblocks the captured shape.
    """
    entries: list[dict] = []
    seen: set[int] = set()

    def _try_parse_date(s: str) -> str | None:
        s = str(s or '').strip()
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S',
                    '%d/%m/%Y', '%d/%m/%y'):
            try:
                d = datetime.strptime(s.split(' (')[0], fmt).date()
                return d.isoformat()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(s[:19]).date().isoformat()
        except (ValueError, TypeError):
            return None

    def _add(z: int | None, d: str | None):
        if not z or not d or z in seen:
            return
        seen.add(z)
        entries.append({'z_number': z, 'date': d})

    def _visit(node):
        if isinstance(node, dict):
            # Captured shape: single-key dict {z_str: "Z: <z>|DD/MM/YYYY"}.
            if len(node) == 1:
                (k, v), = node.items()
                if isinstance(v, str):
                    m = _Z_LABEL_RE.search(v)
                    if m:
                        try:
                            _add(int(m.group(1)), _try_parse_date(m.group(2)))
                        except ValueError:
                            pass
            # Alternate shape: explicit {"key": <z>, "value": <date>}.
            k_val = node.get('key')
            v_val = node.get('value')
            if k_val is not None and isinstance(v_val, (str, int, float)):
                try:
                    z = int(k_val)
                except (TypeError, ValueError):
                    z = None
                # The value might itself be a Z-label or a bare date.
                v_str = str(v_val)
                m = _Z_LABEL_RE.search(v_str)
                if m:
                    _add(int(m.group(1)), _try_parse_date(m.group(2)))
                else:
                    _add(z, _try_parse_date(v_str))
            for child in node.values():
                _visit(child)
        elif isinstance(node, list):
            for item in node:
                _visit(item)

    _visit(filters_json)
    return entries


def resolve_z_for_date(filters_json, target_date: str) -> int | None:
    """Pick the Z number matching target_date ('YYYY-MM-DD'). None if not found."""
    for e in _iter_z_entries(filters_json):
        if e['date'] == target_date:
            return e['z_number']
    return None


def build_submit_body(from_z: int, to_z: int) -> dict:
    """Exact body shape captured from BI DevTools."""
    return {
        'id': Z_REPORT_ID,
        'outputType': 'PDF',
        'filters': [
            {'id': 1, 'name': 'ID_Z', 'filterType': 'INTEGER', 'value': from_z},
            {'id': 2, 'name': 'TO_Z', 'filterType': 'INTEGER', 'value': to_z},
        ],
    }


def submit_902(aviv_branch_id: int, z_number: int, token: str) -> str:
    """POST /reports/result/?branch=X → file url. Raises AuthExpired on 401."""
    body = build_submit_body(z_number, z_number)
    url = f'{BASE}/reports/result/?branch={aviv_branch_id}'
    r = requests.post(url, json=body,
                      headers={'Authtoken': token, 'Content-Type': 'application/json'},
                      timeout=60, verify=False)
    if r.status_code == 401:
        raise AuthExpired('reports/result 401')
    r.raise_for_status()
    j = r.json()
    file_url = j.get('url')
    if not file_url:
        raise RuntimeError(f'reports/result missing url: {j}')
    return file_url


def download_pdf(file_url: str, token: str) -> bytes:
    """GET the report URL with Authtoken → PDF bytes."""
    r = requests.get(file_url, headers={'Authtoken': token},
                     timeout=60, verify=False)
    if r.status_code == 401:
        raise AuthExpired('pdf download 401')
    r.raise_for_status()
    return r.content


# ---- DB upsert ------------------------------------------------------------

def record_closed_day(conn, branch_id: int, target_date: str) -> None:
    """Insert a sentinel row (z_number=NULL, amount=NULL) so backfill passes
    later in the night recognize this (branch, date) as resolved-no-data and
    don't re-probe Aviv. INSERT OR IGNORE: never overwrite a real row.
    """
    conn.execute('''
        INSERT OR IGNORE INTO z_report_902
          (branch_id, date, z_number, amount, transactions, avg_per_txn,
           payment_breakdown, fetched_at)
        VALUES (?, ?, NULL, NULL, NULL, NULL, NULL, datetime('now'))
    ''', (branch_id, target_date))
    conn.commit()


def _save_z_pdf(branch_id: int, target_date: str, pdf_bytes: bytes) -> str:
    """Save the Z PDF where /sales reads previews from.

    Path: <PDF_BASE>/<branch_id>/z_<date>.pdf — identical to the Gmail-Z layout,
    so /api/sales/pdf/* serves 902 and Gmail rows from the same files.
    """
    pdf_dir = os.path.join(PDF_BASE, str(branch_id))
    os.makedirs(pdf_dir, exist_ok=True)
    pdf_path = os.path.join(pdf_dir, f'z_{target_date}.pdf')
    with open(pdf_path, 'wb') as f:
        f.write(pdf_bytes)
    return pdf_path


def mirror_to_daily_sales(conn, branch_id: int, target_date: str,
                          amount: float, transactions: int | None) -> bool:
    """Bridge: surface a successful 902 pull on the dashboard via daily_sales.

    Uses INSERT OR IGNORE so Gmail-Z (or any earlier writer) is never
    overwritten. Returns True if a row was actually inserted.
    """
    cur = conn.execute(
        "INSERT OR IGNORE INTO daily_sales "
        "(branch_id, date, amount, transactions, source, fetched_at) "
        "VALUES (?, ?, ?, ?, 'z_report', datetime('now'))",
        (branch_id, target_date, amount, transactions or 0),
    )
    conn.commit()
    return cur.rowcount > 0


def upsert_z_report(conn, branch_id: int, target_date: str, z_number: int,
                    parsed: dict) -> None:
    """Write to z_report_902 ONLY. Never daily_sales."""
    pb = parsed.get('payment_breakdown')
    pb_json = json.dumps(pb, ensure_ascii=False) if pb else None
    conn.execute('''
        INSERT INTO z_report_902
          (branch_id, date, z_number, amount, transactions, avg_per_txn,
           payment_breakdown, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(branch_id, date) DO UPDATE SET
          z_number=excluded.z_number,
          amount=excluded.amount,
          transactions=excluded.transactions,
          avg_per_txn=excluded.avg_per_txn,
          payment_breakdown=excluded.payment_breakdown,
          fetched_at=excluded.fetched_at
    ''', (branch_id, target_date, z_number,
          parsed.get('total'), parsed.get('transactions'),
          parsed.get('avg_per_txn'), pb_json))
    conn.commit()


# ---- Per-branch runner ----------------------------------------------------

def run_for_branch(branch_id: int, target_date: str | None = None,
                   conn: sqlite3.Connection | None = None,
                   chain_token: str | None = None) -> dict:
    """Fetch + parse + upsert one branch's Z for target_date (default yesterday).

    If chain_token is provided, skip per-branch login and read aviv_branch_id
    from the branches table (chain-account mode).
    """
    target_date = target_date or (date.today() - timedelta(days=1)).isoformat()

    owns_conn = conn is None
    if owns_conn:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row

    try:
        branch = conn.execute('SELECT * FROM branches WHERE id=?',
                              (branch_id,)).fetchone()
        if not branch:
            return {'ok': False, 'branch_id': branch_id, 'error': 'branch not found'}

        username = branch['aviv_user_id']
        password = (branch['aviv_password'] or username) if username else None

        if chain_token is not None:
            # Chain-account mode: token already issued; URL branch param from DB.
            aviv_branch_id = branch['aviv_branch_id']
            if aviv_branch_id is None:
                return {'ok': False, 'branch_id': branch_id,
                        'error': 'chain mode but branches.aviv_branch_id is NULL'}
            token = chain_token

            def _reauth():
                return _login_chain_account(), aviv_branch_id
        else:
            # Per-branch mode (legacy / fallback).
            if not username:
                return {'ok': False, 'branch_id': branch_id, 'error': 'no aviv creds'}
            token, aviv_branch_id = _login(username, password)
            token = _refresh(token)

            def _reauth():
                tok, _ = _login(username, password)
                return _refresh(tok), aviv_branch_id

        # Retry filters/902 on transient Aviv failures (404/5xx/network/timeout).
        # A 200 response is treated as authoritative — closed-day "no Z for date"
        # is the legitimate skip path and must NOT retry.
        filters = None
        last_err: Exception | None = None
        for attempt in range(1, FILTERS_MAX_ATTEMPTS + 1):
            try:
                filters = fetch_902_filters(aviv_branch_id, token)
                break
            except Exception as e:
                last_err = e
                log.warning(
                    'branch=%d filters/902 attempt %d/%d failed: %s',
                    branch_id, attempt, FILTERS_MAX_ATTEMPTS, e)
                if attempt == FILTERS_MAX_ATTEMPTS:
                    break
                time.sleep(FILTERS_RETRY_BACKOFF_SEC)
                token, aviv_branch_id = _reauth()
        if filters is None:
            return {'ok': False, 'branch_id': branch_id, 'date': target_date,
                    'error': f'filters/902 failed after {FILTERS_MAX_ATTEMPTS} '
                             f'attempts: {str(last_err)[:160]}'}

        z_number = resolve_z_for_date(filters, target_date)
        if not z_number:
            # Filters call succeeded but no Z for this date → store was closed.
            # Mark resolved so 03/04/05 backfill passes skip this branch.
            record_closed_day(conn, branch_id, target_date)
            return {'ok': False, 'branch_id': branch_id,
                    'date': target_date, 'error': 'no Z for date'}

        try:
            file_url = submit_902(aviv_branch_id, z_number, token)
            pdf_bytes = download_pdf(file_url, token)
        except AuthExpired:
            log.info('branch=%d 401 — re-login + retry', branch_id)
            token, aviv_branch_id = _reauth()
            file_url = submit_902(aviv_branch_id, z_number, token)
            pdf_bytes = download_pdf(file_url, token)

        parsed = parse_902_pdf(pdf_bytes)
        if parsed.get('total') is None:
            return {'ok': False, 'branch_id': branch_id, 'date': target_date,
                    'z_number': z_number, 'error': 'parse failed (no total)'}

        _save_z_pdf(branch_id, target_date, pdf_bytes)
        upsert_z_report(conn, branch_id, target_date, z_number, parsed)
        log.info('branch=%d date=%s z=%d total=%.2f txns=%s',
                 branch_id, target_date, z_number,
                 parsed['total'], parsed['transactions'])

        if MIRROR_TO_DAILY_SALES:
            inserted = mirror_to_daily_sales(
                conn, branch_id, target_date,
                parsed['total'], parsed.get('transactions'))
            log.info('daily_sales mirror branch=%d date=%s inserted=%s',
                     branch_id, target_date, inserted)

        return {'ok': True, 'branch_id': branch_id, 'date': target_date,
                'z_number': z_number, **parsed}
    finally:
        if owns_conn:
            conn.close()


def _branch_ids_for_date(conn, target_date: str, missing_only: bool,
                         chain_mode: bool = False) -> list[int]:
    """All active branches, or only those missing a z_report_902 row for target_date.

    A "row" is anything — a real Z OR a closed-day sentinel (z_number IS NULL).
    Both count as resolved; backfill must not re-probe either.

    chain_mode=True requires branches.aviv_branch_id NOT NULL; per-branch mode
    requires aviv_user_id NOT NULL.
    """
    where = ('aviv_branch_id IS NOT NULL' if chain_mode
             else 'aviv_user_id IS NOT NULL')
    all_branches = [r['id'] for r in conn.execute(
        f'SELECT id FROM branches WHERE active=1 AND {where} ORDER BY id'
    ).fetchall()]
    if not missing_only:
        return all_branches
    done = {r['branch_id'] for r in conn.execute(
        'SELECT branch_id FROM z_report_902 WHERE date=?',
        (target_date,)
    ).fetchall()}
    return [bid for bid in all_branches if bid not in done]


def run_all_branches(target_date: str | None = None,
                     missing_only: bool = False,
                     conn: sqlite3.Connection | None = None) -> list[dict]:
    """Run every active branch sequentially. One branch's failure never aborts the loop.

    With missing_only=True, branches that already have a z_report_902 row for
    target_date (real Z or closed-day sentinel) are skipped — used by the
    30-minute interval backfill ticks (~05:00–12:00 IL) so each branch is
    retried until it lands and resolved branches stop being re-probed.
    """
    target_date = target_date or (date.today() - timedelta(days=1)).isoformat()
    results: list[dict] = []
    owns_conn = conn is None
    if owns_conn:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
    try:
        bids = _branch_ids_for_date(conn, target_date, missing_only,
                                    chain_mode=USE_CHAIN_AUTH)
        if missing_only:
            log.info('backfill pass: %d branch(es) missing for %s: %s',
                     len(bids), target_date, bids)

        # In chain mode: one login + one refresh, reuse the token for all branches.
        chain_token: str | None = None
        if USE_CHAIN_AUTH:
            if not bids:
                log.info('chain mode: no branches with aviv_branch_id set')
            else:
                try:
                    chain_token = _login_chain_account()
                    chain_token = _refresh(chain_token)
                    log.info('chain auth: 1 login for %d branch(es): %s',
                             len(bids), bids)
                except Exception as e:
                    log.error('chain login failed; aborting run: %s', e)
                    return [{'ok': False, 'branch_id': bid,
                             'date': target_date,
                             'error': f'chain login failed: {str(e)[:160]}'}
                            for bid in bids]

        for bid in bids:
            try:
                results.append(run_for_branch(bid, target_date, conn=conn,
                                              chain_token=chain_token))
            except Exception as e:
                log.exception('aviv_z_report failed for branch %d', bid)
                results.append({'ok': False, 'branch_id': bid,
                                'date': target_date, 'error': str(e)[:200]})
    finally:
        if owns_conn:
            conn.close()
    return results


if __name__ == '__main__':
    import argparse
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    ap = argparse.ArgumentParser(description='Aviv BI report 902 Z-validation agent')
    ap.add_argument('--branch-id', type=int,
                    help='Single branch id; omit to run all active branches')
    ap.add_argument('--date', help='YYYY-MM-DD (default: yesterday)')
    ap.add_argument('--missing-only', action='store_true',
                    help='Only attempt branches missing a row for target_date '
                         '(for hourly backfill passes after the primary 02:00 IL run)')
    args = ap.parse_args()

    if args.branch_id:
        out = run_for_branch(args.branch_id, args.date)
        print(out)
        sys.exit(0 if out.get('ok') else 1)
    else:
        out = run_all_branches(args.date, missing_only=args.missing_only)
        for r in out:
            print(r)
        sys.exit(0 if all(r.get('ok') for r in out) else 1)

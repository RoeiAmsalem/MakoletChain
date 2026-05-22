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


def _iter_z_entries(filters_json) -> list[dict]:
    """Walk the filters JSON and return a flat list of {z_number, date} entries.

    The exact wrapping shape varies (Aviv sometimes returns {data: [...]},
    sometimes a bare list of filter objects, each with a "value" list of
    options). Each option looks like {"key": <Z>, "value": <date_str>} or
    similar. We scan defensively for any dict that has a numeric key + a
    parseable date in its value, since the field names sometimes vary.
    """
    entries: list[dict] = []

    def _try_parse_date(s: str) -> str | None:
        s = str(s or '').strip()
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S',
                    '%d/%m/%Y', '%d/%m/%y'):
            try:
                d = datetime.strptime(s.split(' (')[0], fmt).date()
                return d.isoformat()
            except ValueError:
                continue
        # Try ISO leading prefix
        try:
            return datetime.fromisoformat(s[:19]).date().isoformat()
        except (ValueError, TypeError):
            return None

    def _visit(node):
        if isinstance(node, dict):
            # Candidate Z option: numeric "key" + stringy "value" w/ a date.
            k = node.get('key')
            v = node.get('value')
            if k is not None and isinstance(v, (str, int, float)):
                try:
                    z = int(k)
                except (TypeError, ValueError):
                    z = None
                d = _try_parse_date(str(v))
                if z and d:
                    entries.append({'z_number': z, 'date': d})
            for child in node.values():
                _visit(child)
        elif isinstance(node, list):
            for item in node:
                _visit(item)

    _visit(filters_json)
    # Dedup by z_number (keep first seen).
    seen: set[int] = set()
    out: list[dict] = []
    for e in entries:
        if e['z_number'] in seen:
            continue
        seen.add(e['z_number'])
        out.append(e)
    return out


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
                   conn: sqlite3.Connection | None = None) -> dict:
    """Fetch + parse + upsert one branch's Z for target_date (default yesterday)."""
    target_date = target_date or (date.today() - timedelta(days=1)).isoformat()

    owns_conn = conn is None
    if owns_conn:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row

    try:
        branch = conn.execute('SELECT * FROM branches WHERE id=?',
                              (branch_id,)).fetchone()
        if not branch or not branch['aviv_user_id']:
            return {'ok': False, 'branch_id': branch_id, 'error': 'no aviv creds'}

        username = branch['aviv_user_id']
        password = branch['aviv_password'] or username

        token, aviv_branch_id = _login(username, password)
        token = _refresh(token)

        filters = fetch_902_filters(aviv_branch_id, token)
        z_number = resolve_z_for_date(filters, target_date)
        if not z_number:
            return {'ok': False, 'branch_id': branch_id,
                    'date': target_date, 'error': 'no Z for date'}

        try:
            file_url = submit_902(aviv_branch_id, z_number, token)
            pdf_bytes = download_pdf(file_url, token)
        except AuthExpired:
            log.info('branch=%d 401 — re-login + retry', branch_id)
            token, _ = _login(username, password)
            token = _refresh(token)
            file_url = submit_902(aviv_branch_id, z_number, token)
            pdf_bytes = download_pdf(file_url, token)

        parsed = parse_902_pdf(pdf_bytes)
        if parsed.get('total') is None:
            return {'ok': False, 'branch_id': branch_id, 'date': target_date,
                    'z_number': z_number, 'error': 'parse failed (no total)'}

        upsert_z_report(conn, branch_id, target_date, z_number, parsed)
        log.info('branch=%d date=%s z=%d total=%.2f txns=%s',
                 branch_id, target_date, z_number,
                 parsed['total'], parsed['transactions'])
        return {'ok': True, 'branch_id': branch_id, 'date': target_date,
                'z_number': z_number, **parsed}
    finally:
        if owns_conn:
            conn.close()


def run_all_branches(target_date: str | None = None) -> list[dict]:
    """Run every active branch sequentially. One branch's failure never aborts the loop."""
    target_date = target_date or (date.today() - timedelta(days=1)).isoformat()
    results: list[dict] = []
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            'SELECT id FROM branches WHERE active=1 AND aviv_user_id IS NOT NULL'
        ).fetchall()
        for row in rows:
            bid = row['id']
            try:
                results.append(run_for_branch(bid, target_date, conn=conn))
            except Exception as e:
                log.exception('aviv_z_report failed for branch %d', bid)
                results.append({'ok': False, 'branch_id': bid,
                                'date': target_date, 'error': str(e)[:200]})
    finally:
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
    args = ap.parse_args()

    if args.branch_id:
        out = run_for_branch(args.branch_id, args.date)
        print(out)
        sys.exit(0 if out.get('ok') else 1)
    else:
        out = run_all_branches(args.date)
        for r in out:
            print(r)
        sys.exit(0 if all(r.get('ok') for r in out) else 1)

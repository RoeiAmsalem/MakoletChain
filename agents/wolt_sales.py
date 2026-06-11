"""Monthly Wolt revenue per branch — Aviv BI report 203, Wolt tender only.

Report 203 ("מכירות בחתך כרטיסי סועד") breaks sales down by soed-card
company (GOODI, Wolt, ...). Filtered to inDcType=20 it returns Wolt rows
only. The amounts are incl-VAT — verified against the 902 Z payment lines
(₪-for-₪ match), i.e. the SAME basis as daily_sales.amount. Wolt is a
payment tender INSIDE total revenue: a slice, never an addition.

Pipeline per branch per month (chain token, branches.aviv_branch_id):
  1. POST /avivbi/v2/reports/result/?branch=X
       {id:203, outputType:'XLS', filters:[fromDate;toDate=<month window>,
        inDcType=[20], showPie, orderBy]}                    → JSON {url}
  2. GET that url → XLS
  3. Parse: prefer the סה"כ terminator row (it already sums any
     company-name splits, e.g. 'Wolt' + 'Wolt IL', deduplicated); fall back
     to summing the data rows if the terminator is missing.
  4. Full-month overwrite into wolt_sales (branch_id, year_month):
     amount > 0 → upsert; amount == 0 / no rows → DELETE (no row = no Wolt,
     and the /sales tile stays hidden).

Schedule: piggybacks the employer-report cadence (scheduler.run_aviv_report_all
calls run_all_branches() after the hours pull) — every run refreshes the
CURRENT month and re-fetches the PREVIOUS month.
"""

import argparse
import calendar
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)

BASE = 'https://bi1.aviv-pos.co.il:8443/avivbi/v2'
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
IL_TZ = ZoneInfo('Asia/Jerusalem')

REPORT_ID = 203
WOLT_DC_TYPE = 20          # inDcType code for the Wolt tender (NOT משלוחים)
TOTAL_LABELS = ('סה"כ', "סה''כ", 'סה”כ', 'סהכ')
# Seconds between Aviv calls — same gentle-throttle idea as the other agents.
BETWEEN_BRANCH_SLEEP = 1.5


class AuthExpired(Exception):
    """Raised on 401 — caller re-logs in and retries once."""


def _login_chain_account() -> str:
    """Chain-owner login from env (AVIV_CHAIN_USER/PASS). Same creds the
    902/112 agent uses. Never logs the password."""
    user = os.environ.get('AVIV_CHAIN_USER')
    pw = os.environ.get('AVIV_CHAIN_PASS')
    if not user or not pw:
        raise RuntimeError('AVIV_CHAIN_USER / AVIV_CHAIN_PASS not set in env')
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
                      headers={'Authtoken': token,
                               'Content-Type': 'application/json'},
                      json={}, timeout=10, verify=False)
    j = r.json()
    return j.get('token') or j.get('value') or token


def _month_window(month: str) -> tuple[str, str]:
    """'YYYY-MM' → (from, to) datetime strings covering the whole month."""
    y, m = map(int, month.split('-'))
    last = calendar.monthrange(y, m)[1]
    return f'{month}-01 00:00:00', f'{month}-{last:02d} 23:59:59'


def submit_203_wolt(aviv_branch_id: int, month: str, token: str) -> str:
    """POST /reports/result/ for report 203, Wolt tender, one month → file url.

    Filter shape captured live from /reports/filters/203 (probe_203_wolt.py):
    fromDate;toDate DATETIMERANGE + inDcType MULTICHOICE + the two required
    defaults (showPie, orderBy). Raises AuthExpired on 401.
    """
    from_dt, to_dt = _month_window(month)
    body = {
        'id': REPORT_ID,
        'outputType': 'XLS',
        'filters': [
            {'id': 1, 'name': 'fromDate;toDate', 'filterType': 'DATETIMERANGE',
             'value': [from_dt, to_dt]},
            {'id': 2, 'name': 'inDcType', 'filterType': 'MULTICHOICE',
             'value': [WOLT_DC_TYPE]},
            {'id': 3, 'name': 'showPie', 'filterType': 'BOOLEAN',
             'value': True},
            {'id': None, 'name': 'orderBy', 'filterType': 'SORTBY',
             'value': ['sum(dc.sum) DESC']},
        ],
    }
    url = f'{BASE}/reports/result/?branch={aviv_branch_id}'
    r = requests.post(url, json=body,
                      headers={'Authtoken': token,
                               'Content-Type': 'application/json'},
                      timeout=60, verify=False)
    if r.status_code == 401:
        raise AuthExpired('203 reports/result 401')
    r.raise_for_status()
    j = r.json()
    file_url = j.get('url')
    if not file_url:
        raise RuntimeError(f'203 reports/result missing url: {j}')
    return file_url


def download_xls(file_url: str, token: str) -> bytes:
    r = requests.get(file_url, headers={'Authtoken': token},
                     timeout=60, verify=False)
    if r.status_code == 401:
        raise AuthExpired('203 xls download 401')
    r.raise_for_status()
    return r.content


def parse_203_wolt_total(xls_bytes: bytes) -> float:
    """Parse a Wolt-filtered 203 XLS → total ₪ (0.0 when no Wolt rows).

    Layout captured live (probe_203_wolt.py, branch 9001 2026-05):
      row 0: ['% תרומה', 'סה"כ', 'כמות', 'שם חברה']     ← header
      rows : ['100.00 %', '1761.27', '25', 'Wolt']      ← company rows
      last : ['100.00 %', '1761.27', '', 'סה"כ']        ← terminator

    Column positions are discovered from the header (not hardcoded). The
    terminator row is preferred — Aviv already sums company-name splits
    there (dedup for free); summing the data rows is the fallback.
    """
    import xlrd

    wb = xlrd.open_workbook(file_contents=xls_bytes, formatting_info=False)
    sh = wb.sheet_by_index(0)

    amount_col = name_col = header_row = None
    for i in range(sh.nrows):
        vals = {c: str(sh.cell_value(i, c)).strip() for c in range(sh.ncols)}
        if 'שם חברה' in vals.values():
            for c, v in vals.items():
                if v == 'שם חברה':
                    name_col = c
                elif v in TOTAL_LABELS:
                    amount_col = c
            header_row = i
            break
    if header_row is None or amount_col is None or name_col is None:
        # No header at all → Aviv rendered an empty report (no Wolt rows).
        return 0.0

    def _amt(i):
        try:
            return float(str(sh.cell_value(i, amount_col)).replace(',', ''))
        except (ValueError, TypeError):
            return None

    row_sum = 0.0
    for i in range(header_row + 1, sh.nrows):
        name = str(sh.cell_value(i, name_col)).strip()
        if not name:
            continue
        if name in TOTAL_LABELS or name.startswith('סה'):
            total = _amt(i)
            if total is not None:
                return round(total, 2)
            break
        a = _amt(i)
        if a is not None:
            row_sum += a
    return round(row_sum, 2)


def upsert_wolt_month(conn, branch_id: int, month: str, amount: float) -> str:
    """Full-month overwrite. amount > 0 → upsert; else delete (no row = no
    Wolt that month, tile hidden). Returns 'upsert' / 'delete' / 'noop'."""
    if amount > 0:
        conn.execute(
            "INSERT INTO wolt_sales (branch_id, year_month, amount, updated_at) "
            "VALUES (?, ?, ?, datetime('now')) "
            "ON CONFLICT(branch_id, year_month) DO UPDATE SET "
            "amount=excluded.amount, updated_at=excluded.updated_at",
            (branch_id, month, amount))
        conn.commit()
        return 'upsert'
    cur = conn.execute(
        "DELETE FROM wolt_sales WHERE branch_id=? AND year_month=?",
        (branch_id, month))
    conn.commit()
    return 'delete' if cur.rowcount else 'noop'


def run_for_branch(branch_id: int, months: list[str], token: str,
                   conn: sqlite3.Connection) -> dict:
    """Fetch + store Wolt totals for one branch over the given months."""
    branch = conn.execute(
        'SELECT id, name, aviv_branch_id FROM branches WHERE id=?',
        (branch_id,)).fetchone()
    if not branch or branch['aviv_branch_id'] is None:
        return {'ok': False, 'branch_id': branch_id,
                'error': 'no aviv_branch_id'}
    out = {}
    for month in months:
        try:
            file_url = submit_203_wolt(branch['aviv_branch_id'], month, token)
            amount = parse_203_wolt_total(download_xls(file_url, token))
        except AuthExpired:
            raise  # caller re-logs in and retries the branch once
        action = upsert_wolt_month(conn, branch_id, month, amount)
        out[month] = amount
        log.info('wolt branch=%d month=%s amount=%.2f (%s)',
                 branch_id, month, amount, action)
        time.sleep(0.5)
    return {'ok': True, 'branch_id': branch_id, 'months': out}


def _current_and_previous_month() -> list[str]:
    now = datetime.now(IL_TZ)
    cur = now.strftime('%Y-%m')
    prev_y, prev_m = (now.year, now.month - 1) if now.month > 1 \
        else (now.year - 1, 12)
    return [cur, f'{prev_y:04d}-{prev_m:02d}']


def run_all_branches(months: list[str] | None = None) -> list[dict]:
    """All active branches with an aviv_branch_id, one chain login. Default
    months = current + previous (full overwrite both). One branch failing
    never aborts the loop."""
    months = months or _current_and_previous_month()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    results: list[dict] = []
    try:
        bids = [r['id'] for r in conn.execute(
            'SELECT id FROM branches WHERE active=1 '
            'AND aviv_branch_id IS NOT NULL ORDER BY id').fetchall()]
        token = _refresh(_login_chain_account())
        log.info('wolt 203 pull: %d branch(es), months=%s', len(bids), months)
        for bid in bids:
            try:
                try:
                    results.append(run_for_branch(bid, months, token, conn))
                except AuthExpired:
                    token = _refresh(_login_chain_account())
                    results.append(run_for_branch(bid, months, token, conn))
            except Exception as e:
                log.exception('wolt 203 failed for branch %d', bid)
                results.append({'ok': False, 'branch_id': bid,
                                'error': str(e)[:200]})
            time.sleep(BETWEEN_BRANCH_SLEEP)
    finally:
        conn.close()
    ok = sum(1 for r in results if r.get('ok'))
    log.info('wolt 203 pull complete: %d/%d ok', ok, len(results))
    return results


if __name__ == '__main__':
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    ap = argparse.ArgumentParser(description='Wolt monthly revenue (Aviv 203)')
    ap.add_argument('--months', nargs='*',
                    help="Months 'YYYY-MM' (default: current + previous)")
    ap.add_argument('--branch-id', type=int,
                    help='Single branch id; omit for all active branches')
    args = ap.parse_args()

    if args.branch_id:
        c = sqlite3.connect(DB_PATH, timeout=30)
        c.row_factory = sqlite3.Row
        try:
            tok = _refresh(_login_chain_account())
            res = run_for_branch(args.branch_id,
                                 args.months or _current_and_previous_month(),
                                 tok, c)
        finally:
            c.close()
        print(res)
        sys.exit(0 if res.get('ok') else 1)
    res = run_all_branches(args.months)
    for r in res:
        print(r)
    sys.exit(0 if all(r.get('ok') for r in res) else 1)

"""Fetches employee hours from Aviv BI's employer's report (report id 301,
"דוח נוכחות כללי-A4"). Runs in parallel with aviv_employees.py until cutover.

Pipeline (3 HTTP calls on bi1.aviv-pos.co.il:8443):
  1. POST /avivbi/v2/account/login        → auth token + aviv branch id
  2. POST /avivbi/v2/reports/result/?branch=X with filters → JSON {url}
  3. GET that url                         → legacy .xls (CFB) bytes

Schedule (wired in scheduler.py):
  - Sun-Thu 16:00 IL — current month only
  - Sun-Thu 23:30 IL — current + previous month
  - Fri 20:00 IL     — current month only
  - Sat 23:30 IL     — current + previous month

Each scheduled run does a full overwrite for source='aviv_report' rows.
"""

import logging
import os
import re
import sqlite3
import time
from datetime import date, datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from agents._employee_matching import match_employee_name, strip_store_suffix

log = logging.getLogger(__name__)

BASE = 'https://bi1.aviv-pos.co.il:8443/avivbi/v2'
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
REPORTS_BASE = f'{BASE}/reports'
EMPLOYER_REPORT_ID = 301

# Chain-account auth: AVIV_EMP_USE_CHAIN=1 → run_all_branches does ONE chain
# login (AVIV_CHAIN_USER / AVIV_CHAIN_PASS) and reuses the token for every
# branch. The per-branch URL param comes from branches.aviv_branch_id. Default
# off so the legacy per-branch path is preserved unless explicitly flipped.
USE_CHAIN_AUTH = os.environ.get('AVIV_EMP_USE_CHAIN', '').strip().lower() in (
    '1', 'true', 'yes', 'on')
CHAIN_USER_ENV = 'AVIV_CHAIN_USER'
CHAIN_PASS_ENV = 'AVIV_CHAIN_PASS'

SUBTOTAL_PREFIX = "סה''כ שורות"
NO_CLOCKOUT = 'אין יציאה'


class AuthExpired(Exception):
    """Raised when Aviv returns 401 — caller should re-login and retry."""


RETRY_BACKOFF_SECONDS = 30
RETRY_MAX_ATTEMPTS = 3

# Between-branch jitter in run_all_branches so report-301 pulls don't hammer
# Aviv back-to-back. Lives in the agent (not the scheduler) so both scheduled
# and CLI runs get it. First branch runs immediately; each subsequent branch
# waits this long.
JITTER_SECONDS = 30


def _http_with_retry(method, url, **kwargs):
    """Retry on 4xx (except 401) / 5xx. 30s backoff. Max 3 attempts.

    401 returns immediately (caller handles re-auth). Connection errors retry
    too. Final attempt returns the last response or re-raises the last
    connection error.
    """
    last_response = None
    for attempt in range(RETRY_MAX_ATTEMPTS):
        try:
            r = requests.request(method, url, **kwargs)
            last_response = r
            if r.status_code == 401:
                return r
            if 400 <= r.status_code < 600:
                if attempt < RETRY_MAX_ATTEMPTS - 1:
                    log.warning(
                        "aviv_report attempt %d failed (HTTP %d), retrying in %ds",
                        attempt + 1, r.status_code, RETRY_BACKOFF_SECONDS)
                    time.sleep(RETRY_BACKOFF_SECONDS)
                    continue
            return r
        except requests.RequestException as e:
            if attempt < RETRY_MAX_ATTEMPTS - 1:
                log.warning(
                    "aviv_report attempt %d connection error (%s), retrying in %ds",
                    attempt + 1, e, RETRY_BACKOFF_SECONDS)
                time.sleep(RETRY_BACKOFF_SECONDS)
                continue
            raise
    return last_response


def _login(username, password):
    """Reuse same login flow as aviv_employees.py."""
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
        raise RuntimeError(f'{CHAIN_USER_ENV} / {CHAIN_PASS_ENV} not set in env')
    r = requests.post(f'{BASE}/account/login',
                      json={'user': user, 'password': pw},
                      timeout=15, verify=False)
    r.raise_for_status()
    data = r.json() or {}
    token = data.get('token') or data.get('value')
    if not token:
        raise Exception('chain login response missing token')
    return token


def _refresh(token):
    time.sleep(0.3)
    r = requests.post(f'{BASE}/account/refresh',
                      headers={'Authtoken': token, 'Content-Type': 'application/json'},
                      json={}, timeout=10, verify=False)
    return r.json().get('token') or r.json().get('value') or token


def fetch_report_list(aviv_branch_id: int, auth_token: str) -> list:
    """Fetch list of available reports for a branch.

    Returns list of category dicts (each has .reports). Empty list if POS is offline (404).
    Raises AuthExpired on 401 so caller can re-login.
    """
    url = f'{REPORTS_BASE}?branch={aviv_branch_id}'
    headers = {'Authtoken': auth_token}
    r = _http_with_retry('GET', url, headers=headers, timeout=30, verify=False)
    if r.status_code == 404:
        log.info("Branch %s POS offline — no reports available", aviv_branch_id)
        return []
    if r.status_code == 401:
        raise AuthExpired('reports list 401')
    r.raise_for_status()
    j = r.json()
    # Aviv wraps the list in {"data": [...]} — unwrap to a list of categories.
    if isinstance(j, dict):
        return j.get('data', [])
    return j


def find_employer_report_id(reports: list) -> int:
    """Verify report 301 exists in the report list; return 301 if found."""
    for category in reports:
        for r in category.get('reports', []):
            if r.get('id') == EMPLOYER_REPORT_ID:
                return EMPLOYER_REPORT_ID
    raise ValueError(f"Report {EMPLOYER_REPORT_ID} not in available report list")


def fetch_employer_report(aviv_branch_id: int, from_date: str, to_date: str,
                          auth_token: str) -> bytes:
    """POST to /reports/result, follow returned URL, return XLS bytes.

    from_date / to_date: 'YYYY-MM-DD'. Server appends 00:00:00 / 23:59:59.
    """
    filters = [
        {"id": 1, "name": "fromDate;toDate", "filterType": "DATETIMERANGE",
         "value": [f"{from_date} 00:00:00", f"{to_date} 23:59:59"]},
        {"id": 1, "name": "DayOfWeek", "filterType": "MULTICHOICE", "value": []},
        {"id": 2, "name": "inEmployeeId", "filterType": "MULTICHOICE", "value": []},
        {"id": 3, "name": "showNumericHours", "filterType": "BOOLEAN", "value": False},
        {"id": 4, "name": "ShowBreaks", "filterType": "BOOLEAN", "value": False},
        {"id": 5, "name": "showChartBar", "filterType": "BOOLEAN", "value": True},
        {"id": 6, "name": "showPerPage", "filterType": "BOOLEAN", "value": False},
        {"id": 7, "name": "CmbFieldAgg", "filterType": "COMBO",
         "value": {"key": 0, "value": "", "defaultValue": False}},
        {"id": None, "name": "orderBy", "filterType": "SORTBY",
         "value": {"defaultValue": True, "key": "w.user_id", "value": "מספר עובד"}},
    ]
    body = {"id": EMPLOYER_REPORT_ID, "outputType": "XLS", "filters": filters}
    headers = {'Authtoken': auth_token, 'Content-Type': 'application/json'}

    url = f'{BASE}/reports/result/?branch={aviv_branch_id}'
    r = _http_with_retry('POST', url, json=body, headers=headers, timeout=60, verify=False)
    if r.status_code == 401:
        raise AuthExpired('reports/result 401')
    r.raise_for_status()
    j = r.json()
    file_url = j.get('url')
    if not file_url:
        raise RuntimeError(f'reports/result missing url: {j}')

    g = requests.get(file_url, headers=headers, timeout=30, verify=False)
    g.raise_for_status()
    return g.content


def parse_hh_mm(s) -> float:
    """'108:34' → 108.5667, '49:26' → 49.4333. Returns 0.0 on empty/invalid."""
    if not s:
        return 0.0
    s = str(s).strip()
    if ':' not in s:
        return 0.0
    parts = s.split(':')
    try:
        h = int(parts[0])
        m = int(parts[1])
        return h + m / 60.0
    except (ValueError, IndexError):
        return 0.0


def _split_id_prefix(raw: str) -> tuple[int | None, str]:
    """'551 אגם צאצאן תיכון' -> (551, 'אגם צאצאן תיכון').

    If the first whitespace-separated token is purely digits, treat it as the
    Aviv employee_id and return the rest of the string. Otherwise return
    (None, raw_unchanged). Store suffix (e.g. 'תיכון') is left intact for the
    matcher to handle.
    """
    raw = (raw or '').strip()
    if not raw:
        return None, raw
    parts = raw.split(None, 1)
    if len(parts) == 2 and parts[0].isdigit():
        return int(parts[0]), parts[1].strip()
    return None, raw


def parse_employer_report(xls_bytes: bytes) -> list[dict]:
    """Parse legacy .xls → list of {raw_name, aviv_employee_id, total_hours, shift_count, open_shift_count}.

    Sheet 0 has 9 columns; row 0 is header, then groups of rows per employee
    ending with a "סה''כ שורות N" subtotal row. Final row of file is a
    grand-total row which we skip.

    col 8 = "{id} {name} {store_suffix}" on first row of each group; blank on
            continuation rows; "סה''כ שורות N" on subtotal rows.
    col 3 = "אין יציאה" on shifts with no clock-out.
    col 2 = "HH:MM" hours (can exceed 24h).

    The numeric id prefix is split off into aviv_employee_id; raw_name keeps
    the (possibly suffixed) name for downstream matching.
    """
    import xlrd
    wb = xlrd.open_workbook(file_contents=xls_bytes)
    sh = wb.sheet_by_index(0)

    results: list[dict] = []
    current_name = None
    current_aviv_id = None
    current_open = 0

    for i in range(1, sh.nrows):
        col2 = str(sh.cell(i, 2).value).strip()
        col3 = str(sh.cell(i, 3).value).strip()
        col8 = str(sh.cell(i, 8).value).strip()

        if col8.startswith(SUBTOTAL_PREFIX):
            if current_name is not None:
                m = re.search(r'(\d+)', col8)
                shift_count = int(m.group(1)) if m else 0
                results.append({
                    'raw_name': current_name,
                    'aviv_employee_id': current_aviv_id,
                    'total_hours': round(parse_hh_mm(col2), 4),
                    'shift_count': shift_count,
                    'open_shift_count': current_open,
                })
                current_name = None
                current_aviv_id = None
                current_open = 0
            continue

        if col8:
            current_aviv_id, current_name = _split_id_prefix(col8)
            current_open = 1 if col3 == NO_CLOCKOUT else 0
        else:
            if col3 == NO_CLOCKOUT:
                current_open += 1

    return results


def update_employee_hours(branch_id: int, month: str, parsed: list[dict], conn) -> dict:
    """Apply parsed report to employee_hours table for (branch_id, month).

    Strategy: DELETE existing rows where source='aviv_report', then upsert each
    parsed employee. Rows from source='aviv_api' are not deleted, but may be
    overwritten via ON CONFLICT (UNIQUE is on branch_id+month+employee_name).

    Unmatched names go into employee_match_pending for manager review (mirrors
    aviv_employees.py behaviour) and are NOT inserted into employee_hours.

    Returns: {matched, unmatched, open_shifts_total, total_hours, written_names}
    """
    conn.execute('''CREATE TABLE IF NOT EXISTS employee_aliases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL,
        alias_name TEXT NOT NULL,
        branch_id INTEGER NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(branch_id, alias_name)
    )''')

    conn.execute(
        "DELETE FROM employee_hours WHERE branch_id=? AND month=? AND source='aviv_report'",
        (branch_id, month))

    branch_row = conn.execute('SELECT name FROM branches WHERE id=?', (branch_id,)).fetchone()
    branch_name = branch_row[0] if branch_row else ''

    db_employees_rows = conn.execute(
        'SELECT id, name, hourly_rate FROM employees WHERE branch_id=? AND active=1',
        (branch_id,)).fetchall()
    db_employees = [{'id': r[0], 'name': r[1], 'hourly_rate': r[2]} for r in db_employees_rows]

    matched = 0
    unmatched = 0
    open_shifts_total = 0
    total_hours = 0.0
    written_names: list[str] = []

    for row in parsed:
        raw_name = row['raw_name']
        aviv_emp_id = row.get('aviv_employee_id')
        hours = float(row['total_hours'])
        # Zero-hour names are noise: skip entirely. They get no employee_hours
        # row and no employee_match_pending entry — they never enter the
        # pipeline. parse_hh_mm() collapses empty/"0"/"0:00"/invalid all to 0.0.
        if hours <= 0:
            continue
        open_shifts_total += int(row.get('open_shift_count', 0))
        total_hours += hours

        emp_id, confidence, db_name, rate = match_employee_name(
            raw_name, db_employees, branch_name, branch_id)

        if confidence in ('exact', 'high') and emp_id and db_name:
            salary = round(hours * (rate or 0), 2)
            conn.execute('''
                INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, total_salary, source)
                VALUES (?, ?, ?, ?, ?, 'aviv_report')
                ON CONFLICT(branch_id, month, employee_name) DO UPDATE SET
                    total_hours=excluded.total_hours,
                    total_salary=excluded.total_salary,
                    source='aviv_report'
            ''', (branch_id, month, db_name, round(hours, 2), salary))
            matched += 1
            written_names.append(db_name)
        else:
            unmatched += 1
            # Store the suffix-stripped name so pending rows match what the
            # matcher (and manager UI) actually compares against. Matched-path
            # already does this implicitly via match_employee_name → _clean_name.
            stored_name = strip_store_suffix(raw_name, branch_name) or raw_name
            existing = conn.execute(
                '''SELECT id FROM employee_match_pending
                   WHERE branch_id=? AND month=? AND csv_name=? AND resolved=0''',
                (branch_id, month, stored_name)).fetchone()
            if not existing:
                is_new = 1 if emp_id is None else 0
                try:
                    conn.execute('''
                        INSERT INTO employee_match_pending
                        (branch_id, month, csv_name, aviv_employee_id, suggested_employee_id,
                         confidence, hours, salary, source, is_new_employee)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'aviv_report', ?)
                    ''', (branch_id, month, stored_name, aviv_emp_id, emp_id, confidence,
                          round(hours, 2), is_new))
                except sqlite3.OperationalError:
                    # Schema variant — fall back to minimal insert
                    conn.execute('''
                        INSERT INTO employee_match_pending
                        (branch_id, month, csv_name, suggested_employee_id,
                         confidence, hours, salary)
                        VALUES (?, ?, ?, ?, ?, ?, 0)
                    ''', (branch_id, month, stored_name, emp_id, confidence,
                          round(hours, 2)))
            else:
                conn.execute(
                    'UPDATE employee_match_pending SET hours=?, aviv_employee_id=COALESCE(?, aviv_employee_id) WHERE id=?',
                    (round(hours, 2), aviv_emp_id, existing[0]))

    conn.commit()

    return {
        'matched': matched,
        'unmatched': unmatched,
        'open_shifts_total': open_shifts_total,
        'total_hours': round(total_hours, 2),
        'written_names': written_names,
    }


def _month_window(today: date, *, current: bool):
    """Return (month_str, from_date, to_date) for current or previous month."""
    if current:
        first = today.replace(day=1)
        return (today.strftime('%Y-%m'),
                first.strftime('%Y-%m-%d'),
                today.strftime('%Y-%m-%d'))
    # previous month: full month
    first_this = today.replace(day=1)
    last_prev = first_this.replace(day=1)
    # subtract one day to get last day of prev month
    from datetime import timedelta
    last_prev = first_this - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    return (last_prev.strftime('%Y-%m'),
            first_prev.strftime('%Y-%m-%d'),
            last_prev.strftime('%Y-%m-%d'))


def run_for_branch(branch_id: int, include_previous_month: bool = False,
                   today: date | None = None,
                   chain_token: str | None = None) -> dict:
    """Main entry point per branch per scheduled run.

    Always pulls current-month-to-date. When include_previous_month=True,
    additionally re-pulls the entire previous month (used for 23:30 + Sat runs
    so late corrections to clock-outs are captured).

    If chain_token is provided, skip per-branch login and read aviv_branch_id
    from the branches table (chain-account mode).
    """
    today = today or date.today()
    auth_path = 'chain' if chain_token is not None else 'per_store'
    t0 = time.time()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    branch = None
    run_id = None

    try:
        cur = conn.execute(
            "INSERT INTO agent_runs (branch_id, agent, status, started_at) "
            "VALUES (?, 'aviv_report', 'running', datetime('now'))",
            (branch_id,))
        run_id = cur.lastrowid
        conn.commit()

        branch = conn.execute('SELECT * FROM branches WHERE id=?', (branch_id,)).fetchone()
        if not branch:
            msg = 'branch not found'
            conn.execute(
                "UPDATE agent_runs SET status='error', message=?, "
                "finished_at=datetime('now'), duration_seconds=? WHERE id=?",
                (msg, round(time.time() - t0, 2), run_id))
            conn.commit()
            return {'ok': False, 'error': msg}

        if chain_token is not None:
            aviv_branch_id = branch['aviv_branch_id']
            if aviv_branch_id is None:
                msg = 'chain mode but branches.aviv_branch_id is NULL'
                conn.execute(
                    "UPDATE agent_runs SET status='error', message=?, "
                    "finished_at=datetime('now'), duration_seconds=? WHERE id=?",
                    (msg, round(time.time() - t0, 2), run_id))
                conn.commit()
                return {'ok': False, 'error': msg}
            token = chain_token
            password = None  # not used in chain mode
        else:
            if not branch['aviv_user_id']:
                msg = 'No Aviv credentials'
                conn.execute(
                    "UPDATE agent_runs SET status='error', message=?, "
                    "finished_at=datetime('now'), duration_seconds=? WHERE id=?",
                    (msg, round(time.time() - t0, 2), run_id))
                conn.commit()
                return {'ok': False, 'error': msg}
            password = branch['aviv_password'] or branch['aviv_user_id']
            token, aviv_branch_id = _login(branch['aviv_user_id'], password)
            token = _refresh(token)

        reports = fetch_report_list(aviv_branch_id, token)
        if not reports:
            msg = 'POS offline, skipping'
            conn.execute(
                "UPDATE agent_runs SET status='success', message=?, "
                "finished_at=datetime('now'), duration_seconds=? WHERE id=?",
                (msg, round(time.time() - t0, 2), run_id))
            conn.commit()
            log.info("branch=%d %s", branch_id, msg)
            return {'ok': True, 'skipped': True, 'reason': 'pos_offline',
                    'branch_id': branch_id, 'auth_path': auth_path}

        find_employer_report_id(reports)  # raises if missing

        current_window = _month_window(today, current=True)
        current_month_str = current_window[0]
        windows = [current_window]
        if include_previous_month:
            windows.append(_month_window(today, current=False))

        agg = {'matched': 0, 'unmatched': 0, 'open_shifts_total': 0,
               'total_hours': 0.0, 'months': []}

        for month_str, from_d, to_d in windows:
            try:
                xls_bytes = fetch_employer_report(aviv_branch_id, from_d, to_d, token)
            except AuthExpired:
                if chain_token is not None:
                    token = _login_chain_account()
                else:
                    token, _ = _login(branch['aviv_user_id'], password)
                    token = _refresh(token)
                xls_bytes = fetch_employer_report(aviv_branch_id, from_d, to_d, token)

            parsed = parse_employer_report(xls_bytes)
            res = update_employee_hours(branch_id, month_str, parsed, conn)
            log.info("branch=%d month=%s matched=%d unmatched=%d open_shifts=%d total_hours=%.2f",
                     branch_id, month_str, res['matched'], res['unmatched'],
                     res['open_shifts_total'], res['total_hours'])
            agg['matched'] += res['matched']
            agg['unmatched'] += res['unmatched']
            agg['open_shifts_total'] += res['open_shifts_total']
            agg['total_hours'] += res['total_hours']
            agg['months'].append({'month': month_str, **res})

            # Mirror the value /ops shows as "שעות החודש". Gated to the
            # current-month window so the optional previous-month re-pull at
            # 23:30 / Sat does NOT clobber it with a backdated total. Same
            # column aviv_live.scrape_hours_end_of_day writes — single field,
            # two writers, last-write-wins (we run after the live scrape).
            if month_str == current_month_str:
                conn.execute(
                    "UPDATE branches SET hours_this_month=?, "
                    "hours_updated_at=datetime('now') WHERE id=?",
                    (res['total_hours'], branch_id))
                conn.commit()

        msg = (f"matched={agg['matched']} unmatched={agg['unmatched']} "
               f"open_shifts={agg['open_shifts_total']} hours={agg['total_hours']:.1f}")
        conn.execute(
            "UPDATE agent_runs SET status='success', docs_count=?, amount=?, "
            "message=?, finished_at=datetime('now'), duration_seconds=? WHERE id=?",
            (agg['matched'], agg['total_hours'], msg,
             round(time.time() - t0, 2), run_id))
        conn.commit()

        if agg['unmatched'] > 0 or agg['open_shifts_total'] >= 3:
            try:
                from utils.notify import notify
                bname = branch['name'] if branch else f'Branch {branch_id}'
                notify(
                    f'Aviv employer report — {bname}',
                    f"{agg['unmatched']} unmatched, {agg['open_shifts_total']} open shifts."
                )
            except Exception:
                pass

        return {'ok': True, 'branch_id': branch_id, 'auth_path': auth_path, **agg}

    except Exception as e:
        log.exception('aviv_report failed for branch %d', branch_id)
        msg = str(e)[:200]
        try:
            if run_id:
                conn.execute(
                    "UPDATE agent_runs SET status='error', message=?, "
                    "finished_at=datetime('now'), duration_seconds=? WHERE id=?",
                    (msg, round(time.time() - t0, 2), run_id))
                conn.commit()
        except Exception:
            pass
        return {'ok': False, 'error': msg}
    finally:
        conn.close()


def run_all_branches(include_previous_month: bool = False) -> list[dict]:
    """Run report 301 for every active branch.

    When USE_CHAIN_AUTH is on, one chain login + reuse the token; only branches
    with aviv_branch_id NOT NULL are included. Per-branch failures (Exception)
    are caught so the loop never aborts mid-run.
    """
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        if USE_CHAIN_AUTH:
            rows = conn.execute(
                'SELECT id FROM branches '
                'WHERE active=1 AND aviv_branch_id IS NOT NULL ORDER BY id'
            ).fetchall()
        else:
            rows = conn.execute(
                'SELECT id FROM branches '
                'WHERE active=1 AND aviv_user_id IS NOT NULL ORDER BY id'
            ).fetchall()
        bids = [r['id'] for r in rows]
    finally:
        conn.close()

    chain_token: str | None = None
    if USE_CHAIN_AUTH:
        if not bids:
            log.info('chain mode: no branches with aviv_branch_id set')
            return []
        try:
            chain_token = _login_chain_account()
            chain_token = _refresh(chain_token)
            log.info('chain auth: 1 login for %d branch(es): %s', len(bids), bids)
        except Exception as e:
            log.error('chain login failed; aborting employer-report run: %s', e)
            # Critical + systemic: chain login down aborts the whole run.
            from utils.notify import notify
            notify('❌ Employer report (chain)',
                   f'Chain login failed; run aborted. {str(e)[:120]}',
                   critical=True, dedup_key="aviv_chain_auth")
            return [{'ok': False, 'branch_id': bid,
                     'error': f'chain login failed: {str(e)[:160]}'}
                    for bid in bids]

    from utils.notify import batch_start, batch_flush
    results: list[dict] = []
    failed = set()
    batch_start("Employer report", total=len(bids))
    for idx, bid in enumerate(bids):
        if idx > 0:
            time.sleep(JITTER_SECONDS)  # anti-thundering jitter between branches
        try:
            r = run_for_branch(
                bid, include_previous_month=include_previous_month,
                chain_token=chain_token)
            results.append(r)
            if isinstance(r, dict) and r.get('ok') is False:
                failed.add(bid)
        except Exception as e:
            log.exception('aviv_report failed for branch %d', bid)
            results.append({'ok': False, 'branch_id': bid, 'error': str(e)[:200]})
            failed.add(bid)
    batch_flush(failed=len(failed))
    return results


if __name__ == '__main__':
    import argparse
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    ap = argparse.ArgumentParser(description='Aviv employer report agent')
    ap.add_argument('--branch-id', type=int,
                    help='Single branch id; omit to run all active branches')
    ap.add_argument('--include-previous', action='store_true',
                    help='Also re-fetch previous full month')
    args = ap.parse_args()

    if args.branch_id:
        result = run_for_branch(args.branch_id,
                                include_previous_month=args.include_previous)
        print(result)
        sys.exit(0 if result.get('ok') else 1)
    else:
        out = run_all_branches(include_previous_month=args.include_previous)
        for r in out:
            print(r)
        sys.exit(0 if all(r.get('ok') for r in out) else 1)

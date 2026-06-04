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

import json
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
    """'108:34' → 108.5667, '49:26' → 49.4333. Returns 0.0 on empty/invalid.

    Per-shift cells carry seconds ('02:59:01'); subtotal cells are HH:MM. Both
    are accepted — a third ':' part is read as seconds when present.
    """
    if not s:
        return 0.0
    s = str(s).strip()
    if ':' not in s:
        return 0.0
    parts = s.split(':')
    try:
        h = int(parts[0])
        m = int(parts[1])
        sec = int(parts[2]) if len(parts) >= 3 and parts[2] != '' else 0
        return h + m / 60.0 + sec / 3600.0
    except (ValueError, IndexError):
        return 0.0


def _parse_aviv_dt(s) -> datetime | None:
    """'13/04/2026 19:01:34' → datetime, '13/04/2026 19:01' → datetime.

    Returns None on empty/unparseable. Aviv emits day-first dd/mm/yyyy.
    """
    s = (s or '').strip()
    if not s:
        return None
    for fmt in ('%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M', '%d/%m/%Y'):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


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
    """Parse legacy .xls → list of per-employee dicts.

    Each dict: {raw_name, aviv_employee_id, total_hours, shift_count,
    open_shift_count, shifts}. `shifts` is a list of per-shift dicts:
    {shift_date, start_ts, end_ts, hours, day_of_week, is_open} where:
      - shift_date / start_ts / end_ts are 'YYYY-MM-DD[ HH:MM:SS]' or None
      - hours is a float (0.0 for open shifts — no clock-out, no hours)
      - is_open is True for the "אין יציאה" (no clock-out) case

    Sheet 0 has 9 columns; row 0 is header, then groups of rows per employee
    ending with a "סה''כ שורות N" subtotal row. Final row of file is a
    grand-total row which we skip.

    col 2 = "HH:MM:SS" shift hours / "HH:MM" subtotal hours (can exceed 24h).
    col 3 = "אין יציאה" on shifts with no clock-out.
    col 4 = exit timestamp "dd/mm/yyyy HH:MM:SS".
    col 5 = entry timestamp "dd/mm/yyyy HH:MM:SS".
    col 6 = day-of-week ("יום ב").
    col 8 = "{id} {name} {store_suffix}" on first row of each group; blank on
            continuation rows; "סה''כ שורות N" on subtotal rows.

    total_hours comes from the subtotal row (authoritative — NEVER the sum of
    shift rows). The numeric id prefix is split off into aviv_employee_id;
    raw_name keeps the (possibly suffixed) name for downstream matching.
    """
    import xlrd
    wb = xlrd.open_workbook(file_contents=xls_bytes)
    sh = wb.sheet_by_index(0)

    results: list[dict] = []
    current_name = None
    current_aviv_id = None
    current_open = 0
    current_shifts: list[dict] = []

    for i in range(1, sh.nrows):
        col2 = str(sh.cell(i, 2).value).strip()
        col3 = str(sh.cell(i, 3).value).strip()
        col4 = str(sh.cell(i, 4).value).strip()
        col5 = str(sh.cell(i, 5).value).strip()
        col6 = str(sh.cell(i, 6).value).strip()
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
                    'shifts': current_shifts,
                })
                current_name = None
                current_aviv_id = None
                current_open = 0
                current_shifts = []
            continue

        if col8:
            current_aviv_id, current_name = _split_id_prefix(col8)
            current_open = 0
            current_shifts = []

        # Append a shift row for any line that carries shift data. The first
        # row of a group (with col8 set) is itself a shift; continuation rows
        # have a blank col8. Subtotal rows are handled above and never reach here.
        is_open = (col3 == NO_CLOCKOUT)
        start_dt = _parse_aviv_dt(col5)
        end_dt = _parse_aviv_dt(col4)
        if is_open:
            current_open += 1
        if start_dt or end_dt or is_open:
            ref_dt = start_dt or end_dt
            current_shifts.append({
                'shift_date': ref_dt.strftime('%Y-%m-%d') if ref_dt else None,
                'start_ts': start_dt.strftime('%Y-%m-%d %H:%M:%S') if start_dt else None,
                'end_ts': end_dt.strftime('%Y-%m-%d %H:%M:%S') if end_dt else None,
                'hours': round(parse_hh_mm(col2), 4),
                'day_of_week': col6 or None,
                'is_open': is_open,
            })

    return results


def write_employee_shifts(conn, branch_id: int, month: str, employee_name: str,
                          shifts: list, *, classify: bool = True,
                          is_global: bool = False, shabbat_windows=None,
                          has_buckets=None, source: str = 'aviv_report') -> int:
    """Insert per-shift drill-down rows for ONE employee under `employee_name`.

    Shared by the report agent (matched path) and api_pending_add_new (instant
    write on add). Does NOT delete first — the caller controls overwrite scope.
    Classifies in place (regular/overtime/Shabbat) when the buckets columns exist
    and classify=True. `source` MUST stay 'aviv_report' so the nightly
    full-overwrite (DELETE per branch+month+source) reconciles to identical rows.
    Returns the number of shift rows written.
    """
    if not shifts:
        return 0
    if has_buckets is None:
        has_buckets = _table_has_column(conn, 'employee_shifts', 'regular_hours')
    if classify and has_buckets:
        from agents.shift_classify import classify_shifts
        classify_shifts(shifts, shabbat_windows or [], is_global=is_global)
    n = 0
    for sh in shifts:
        if has_buckets:
            conn.execute('''
                INSERT INTO employee_shifts
                (branch_id, month, employee_name, shift_date, start_ts, end_ts,
                 hours, day_of_week, is_open, source,
                 regular_hours, overtime_hours, shabbat_hours)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (branch_id, month, employee_name, sh.get('shift_date'),
                  sh.get('start_ts'), sh.get('end_ts'),
                  round(float(sh.get('hours') or 0), 4), sh.get('day_of_week'),
                  1 if sh.get('is_open') else 0, source,
                  sh.get('regular_hours'), sh.get('overtime_hours'),
                  sh.get('shabbat_hours')))
        else:
            conn.execute('''
                INSERT INTO employee_shifts
                (branch_id, month, employee_name, shift_date, start_ts, end_ts,
                 hours, day_of_week, is_open, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (branch_id, month, employee_name, sh.get('shift_date'),
                  sh.get('start_ts'), sh.get('end_ts'),
                  round(float(sh.get('hours') or 0), 4), sh.get('day_of_week'),
                  1 if sh.get('is_open') else 0, source))
        n += 1
    return n


def _table_has_column(conn, table: str, column: str) -> bool:
    """True if `table` has `column` (used to gate migration-023 bucket writes)."""
    try:
        cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.OperationalError:
        return False
    return any((c[1] == column) for c in cols)


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

    # Per-shift drill-down rows (migration 022). Full-overwrite alongside the
    # monthly total so a re-sync replaces cleanly with no duplicate shifts.
    # Display-only — never summed for the salary total. Guarded so the agent
    # keeps working on a DB that has not yet applied migration 022.
    shifts_table_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='employee_shifts'"
    ).fetchone() is not None
    if shifts_table_exists:
        conn.execute(
            "DELETE FROM employee_shifts WHERE branch_id=? AND month=? AND source='aviv_report'",
            (branch_id, month))

    # Shift classification (migration 023) — regular/overtime/Shabbat buckets,
    # DISPLAY ONLY. Computed at sync time and stored on employee_shifts. Guarded
    # so the agent keeps working on a DB that hasn't applied migration 023 yet.
    shifts_have_buckets = shifts_table_exists and _table_has_column(
        conn, 'employee_shifts', 'regular_hours')
    shabbat_windows = []
    if shifts_have_buckets:
        from agents.shift_classify import load_shabbat_windows
        shabbat_windows = load_shabbat_windows(conn)

    branch_row = conn.execute('SELECT name FROM branches WHERE id=?', (branch_id,)).fetchone()
    branch_name = branch_row[0] if branch_row else ''

    db_employees_rows = conn.execute(
        "SELECT id, name, hourly_rate, COALESCE(salary_type, 'hourly') AS salary_type "
        "FROM employees WHERE branch_id=? AND active=1",
        (branch_id,)).fetchall()
    db_employees = [{'id': r[0], 'name': r[1], 'hourly_rate': r[2]} for r in db_employees_rows]
    # emp_id → is_global, for skipping classification on flat-pay employees.
    global_by_id = {r[0]: (r[3] == 'global') for r in db_employees_rows}

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

            # Write this employee's shift rows under the same canonical db_name
            # so /api/employee-shifts can join on employee_hours.employee_name.
            # Shared helper — same code path the instant add-from-pending write uses.
            if shifts_table_exists:
                write_employee_shifts(
                    conn, branch_id, month, db_name, row.get('shifts', []),
                    classify=True, is_global=global_by_id.get(emp_id, False),
                    shabbat_windows=shabbat_windows, has_buckets=shifts_have_buckets,
                    source='aviv_report')
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
            # RAW shift list (pre-classification) carried on the pending row so the
            # add-from-pending path can write employee_shifts INSTANTLY (migration 026).
            shifts_payload = json.dumps(row.get('shifts') or [], ensure_ascii=False)
            if not existing:
                is_new = 1 if emp_id is None else 0
                try:
                    conn.execute('''
                        INSERT INTO employee_match_pending
                        (branch_id, month, csv_name, aviv_employee_id, suggested_employee_id,
                         confidence, hours, salary, source, is_new_employee, shifts_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'aviv_report', ?, ?)
                    ''', (branch_id, month, stored_name, aviv_emp_id, emp_id, confidence,
                          round(hours, 2), is_new, shifts_payload))
                except sqlite3.OperationalError:
                    # Schema variant (pre-026) — fall back to minimal insert
                    conn.execute('''
                        INSERT INTO employee_match_pending
                        (branch_id, month, csv_name, suggested_employee_id,
                         confidence, hours, salary)
                        VALUES (?, ?, ?, ?, ?, ?, 0)
                    ''', (branch_id, month, stored_name, emp_id, confidence,
                          round(hours, 2)))
            else:
                try:
                    conn.execute(
                        'UPDATE employee_match_pending SET hours=?, '
                        'aviv_employee_id=COALESCE(?, aviv_employee_id), shifts_json=? WHERE id=?',
                        (round(hours, 2), aviv_emp_id, shifts_payload, existing[0]))
                except sqlite3.OperationalError:
                    conn.execute(
                        'UPDATE employee_match_pending SET hours=?, '
                        'aviv_employee_id=COALESCE(?, aviv_employee_id) WHERE id=?',
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
                   chain_token: str | None = None,
                   include_current_month: bool = True,
                   notify_anomalies: bool = True) -> dict:
    """Main entry point per branch per scheduled run.

    By default pulls current-month-to-date. When include_previous_month=True,
    additionally re-pulls the entire previous month (used for 23:30 + Sat runs
    so late corrections to clock-outs are captured).

    Set include_current_month=False (with include_previous_month=True) for a
    previous-month-ONLY pull — used by the monthly reconciliation on the 10th so
    it doesn't drag the current month along (no extra Aviv load).

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
        windows = []
        if include_current_month:
            windows.append(current_window)
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

        msg = (f"matched={agg['matched']} unmatched={agg['unmatched']} "
               f"open_shifts={agg['open_shifts_total']} hours={agg['total_hours']:.1f}")
        conn.execute(
            "UPDATE agent_runs SET status='success', docs_count=?, amount=?, "
            "message=?, finished_at=datetime('now'), duration_seconds=? WHERE id=?",
            (agg['matched'], agg['total_hours'], msg,
             round(time.time() - t0, 2), run_id))
        conn.commit()

        if notify_anomalies and (agg['unmatched'] > 0 or agg['open_shifts_total'] >= 3):
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


# ── Monthly reconciliation (BilBoy-style) ─────────────────────────────────
# Current-month data already self-heals on every run (windows start at the 1st).
# This is the ONE-SHOT end-of-month confirmation: on the 10th, re-pull the
# PREVIOUS month and check the stored totals didn't move after Roei may have
# considered them final. Modeled on bilboy.py's post-sync reconciliation —
# silent full-overwrite (late corrections fix themselves), then ✅ when the DB
# matches / ❌ + brrr when it changed beyond tolerance.

# Tolerances ignore rounding noise (hours/salary stored to 2dp). Mirrors
# BilBoy's ~₪10 idea; a move in EITHER dimension beyond its tolerance flags it.
RECON_HOURS_TOLERANCE = 0.5    # hours — well under one shift's worth
RECON_SALARY_TOLERANCE = 10.0  # ₪ — same order as BilBoy's reconciliation gap


def _recon_logger():
    """Dedicated reconciliation log file (mirrors bilboy_reconciliation.log)."""
    rlog = logging.getLogger('aviv_hours_reconciliation')
    if not rlog.handlers:
        log_dir = Path(__file__).resolve().parent.parent / 'logs'
        log_dir.mkdir(exist_ok=True)
        fh = logging.FileHandler(log_dir / 'hours_reconciliation.log', encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
        rlog.addHandler(fh)
        rlog.addHandler(logging.StreamHandler())
        rlog.setLevel(logging.INFO)
        rlog.propagate = False
    return rlog


def _prev_month_totals(conn, branch_id: int, month: str) -> tuple[float, float]:
    """(total_hours, total_salary) summed over source='aviv_report' rows."""
    row = conn.execute(
        "SELECT COALESCE(SUM(total_hours), 0), COALESCE(SUM(total_salary), 0) "
        "FROM employee_hours WHERE branch_id=? AND month=? AND source='aviv_report'",
        (branch_id, month)).fetchone()
    return float(row[0] or 0), float(row[1] or 0)


def reconcile_previous_month(today: date | None = None, force: bool = False) -> dict:
    """10th-of-month final re-check of the PREVIOUS month for every branch.

    Date-gated to the 10th (force=True runs any day, for tests/manual). Per
    branch: snapshot the stored previous-month totals, re-pull ONLY the previous
    month (silent full-overwrite), then compare. Match within tolerance → ✅ log;
    beyond tolerance → ❌ log + a brrr digest line so Roei learns last month's
    numbers moved. The re-pull runs with notify_anomalies=False so it does NOT
    re-fire unmatched/open-shift notices (those alerted during the month) —
    the digest carries only "changed" branches.
    """
    today = today or date.today()
    rlog = _recon_logger()

    if today.day != 10 and not force:
        log.info("monthly hours reconciliation: today=%s is not the 10th — skipping",
                 today.isoformat())
        return {'ran': False, 'reason': 'not_10th', 'day': today.day}

    prev_month = _month_window(today, current=False)[0]

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        if USE_CHAIN_AUTH:
            rows = conn.execute(
                'SELECT id, name FROM branches '
                'WHERE active=1 AND aviv_branch_id IS NOT NULL ORDER BY id').fetchall()
        else:
            rows = conn.execute(
                'SELECT id, name FROM branches '
                'WHERE active=1 AND aviv_user_id IS NOT NULL ORDER BY id').fetchall()
        branches = [(r['id'], r['name']) for r in rows]
    finally:
        conn.close()

    rlog.info("=== Monthly hours reconciliation: prev_month=%s branches=%d "
              "(tolerance %.1fh / ₪%.0f) ===",
              prev_month, len(branches), RECON_HOURS_TOLERANCE, RECON_SALARY_TOLERANCE)

    from utils.notify import notify, batch_start, batch_flush

    chain_token = None
    if USE_CHAIN_AUTH:
        if not branches:
            rlog.info("chain mode: no branches with aviv_branch_id — nothing to reconcile")
            return {'ran': True, 'month': prev_month, 'checked': 0, 'changed': 0}
        try:
            chain_token = _login_chain_account()
            chain_token = _refresh(chain_token)
        except Exception as e:
            rlog.error("chain login failed; reconciliation aborted: %s", e)
            notify('❌ Hours reconciliation (chain)',
                   f'Chain login failed; {prev_month} reconciliation aborted. {str(e)[:120]}',
                   critical=True, dedup_key='aviv_recon_chain_auth')
            return {'ran': False, 'reason': 'chain_login_failed', 'month': prev_month}

    # verb="flagged" → INFO-tier digest ("N branches flagged"); this is
    # informational ("numbers moved"), never a hard failure.
    batch_start("Monthly hours reconciliation", total=len(branches), verb="flagged")
    checked = 0
    changed = 0
    for idx, (bid, bname) in enumerate(branches):
        if idx > 0:
            time.sleep(JITTER_SECONDS)  # anti-thundering jitter between branches

        snap = sqlite3.connect(DB_PATH, timeout=30)
        try:
            before_h, before_s = _prev_month_totals(snap, bid, prev_month)
        finally:
            snap.close()

        try:
            res = run_for_branch(bid, include_previous_month=True,
                                 include_current_month=False, today=today,
                                 chain_token=chain_token, notify_anomalies=False)
        except Exception as e:
            rlog.error("❌ branch=%d %s re-pull FAILED: %s", bid, bname, str(e)[:160])
            notify(f'Hours reconciliation — {bname}',
                   f'{prev_month}: re-pull failed ({str(e)[:100]}).')
            continue
        if isinstance(res, dict) and res.get('ok') is False:
            rlog.error("❌ branch=%d %s re-pull error: %s", bid, bname, res.get('error'))
            notify(f'Hours reconciliation — {bname}',
                   f"{prev_month}: re-pull error ({str(res.get('error'))[:100]}).")
            continue

        snap = sqlite3.connect(DB_PATH, timeout=30)
        try:
            after_h, after_s = _prev_month_totals(snap, bid, prev_month)
        finally:
            snap.close()

        checked += 1
        dh = abs(after_h - before_h)
        ds = abs(after_s - before_s)
        if dh > RECON_HOURS_TOLERANCE or ds > RECON_SALARY_TOLERANCE:
            changed += 1
            rlog.info("❌ branch=%d %s month=%s CHANGED hours %.2f→%.2f (Δ%.2f) "
                      "salary ₪%.2f→₪%.2f (Δ₪%.2f)",
                      bid, bname, prev_month, before_h, after_h, dh,
                      before_s, after_s, ds)
            notify(f'Hours changed — {bname}',
                   f'{prev_month} moved after month-end: '
                   f'hours {before_h:.1f}→{after_h:.1f}, '
                   f'salary ₪{before_s:,.0f}→₪{after_s:,.0f}.')
        else:
            rlog.info("✅ branch=%d %s month=%s OK hours=%.2f salary=₪%.2f",
                      bid, bname, prev_month, after_h, after_s)

    batch_flush()
    rlog.info("=== Reconciliation complete: %d checked, %d changed ===", checked, changed)
    return {'ran': True, 'month': prev_month, 'checked': checked, 'changed': changed}


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
    ap.add_argument('--reconcile-prev', action='store_true',
                    help='Run the monthly previous-month reconciliation (10th-of-month gated)')
    ap.add_argument('--force', action='store_true',
                    help='With --reconcile-prev: ignore the 10th-of-month date gate')
    args = ap.parse_args()

    if args.reconcile_prev:
        result = reconcile_previous_month(force=args.force)
        print(result)
        sys.exit(0)
    elif args.branch_id:
        result = run_for_branch(args.branch_id,
                                include_previous_month=args.include_previous)
        print(result)
        sys.exit(0 if result.get('ok') else 1)
    else:
        out = run_all_branches(include_previous_month=args.include_previous)
        for r in out:
            print(r)
        sys.exit(0 if all(r.get('ok') for r in out) else 1)

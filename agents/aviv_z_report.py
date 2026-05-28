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
from zoneinfo import ZoneInfo

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)

BASE = 'https://bi1.aviv-pos.co.il:8443/avivbi/v2'
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
Z_REPORT_ID = 902

# The 02:00 IL primary run lives in IL time but the server clock is UTC.
# date.today() at 02:00 IL == 23:00 UTC = still "yesterday-UTC", so it would
# resolve "yesterday" to two-days-ago-IL. Always anchor on Israel time.
IL_TZ = ZoneInfo('Asia/Jerusalem')


def _yesterday_il() -> str:
    """Return yesterday in Israel time as YYYY-MM-DD."""
    return (datetime.now(IL_TZ).date() - timedelta(days=1)).isoformat()

# Where /sales reads PDF previews from (mirror of app.py's PDF_BASE). Saving 902
# PDFs here makes the "צפה" preview work on 902 rows identically to Gmail-Z rows.
PDF_BASE = os.path.join(os.path.dirname(__file__), '..', 'data', 'pdfs')

# Retry-through-transient policy for the Z-list fetch (possible-values +
# filters/902 fallback). Closed-day "no Z for date" is a 200 response and
# does NOT consume retries.
#
# Background: Aviv's filter-cache warms up unpredictably in the morning. On
# 2026-05-28 branch 126 flipped from 404 → 200 with Z 2530 present sometime
# between 03:00:11 UTC and 03:11:57 UTC (~11 min). Our previous 3-attempt
# × 2s loop (~6s) gave up far inside the warm-up window and waited 30 min
# until the next cron tick. The website rides through the transient errors;
# this loop does too — by wall-clock budget, not by attempt count.
#
# Retry on any non-200, non-AuthExpired outcome (404, 5xx, network/timeout).
# Don't classify transient vs permanent by status code — let the OUTCOME
# (budget elapsed without a 200) make the call. AuthExpired bubbles up via
# its existing one-shot re-login between attempts.
FILTERS_RETRY_TOTAL_SECONDS = 240
# Capped-linear backoff between attempts (1st sleep, 2nd, ... then cap).
_FILTERS_BACKOFF_SCHEDULE = (5, 10, 15, 20, 30)

# Chain-account auth: when AVIV_Z_USE_CHAIN=1 in env, run_all_branches logs in
# ONCE with chain creds (AVIV_CHAIN_USER / AVIV_CHAIN_PASS) and reuses the
# token for every branch. The URL branch param comes from branches.aviv_branch_id.
# Default OFF so existing per-branch behavior is preserved unless explicitly flipped.
USE_CHAIN_AUTH = os.environ.get('AVIV_Z_USE_CHAIN', '').strip().lower() in (
    '1', 'true', 'yes', 'on')
CHAIN_USER_ENV = 'AVIV_CHAIN_USER'
CHAIN_PASS_ENV = 'AVIV_CHAIN_PASS'

# When AVIV_Z_CHAIN_AUTOSEED=1, the chain-mode primary run first calls
# /account/branches and INSERT OR IGNOREs a minimal branches row for every
# aviv branch we haven't seen yet (synthetic local id = 9000 + aviv_branch_id,
# name from the API). This is how the diagnostic widens from 126/127 to the
# whole chain without a migration. STAGING-only by convention (the flag is
# only set in /opt/makolet-chain-staging/.env).
AUTOSEED_CHAIN = os.environ.get('AVIV_Z_CHAIN_AUTOSEED', '').strip().lower() in (
    '1', 'true', 'yes', 'on')

# Offset for synthetic local branch ids when seeding chain branches we don't
# already have a row for. Franchise numbers are <200 in practice; 9000+ keeps
# the synthetic rows well clear of real ones.
CHAIN_AUTOSEED_LOCAL_ID_OFFSET = 9000

# Chain branches returned by /account/branches that are NOT operating stores
# and must never be seeded, iterated, or shown on /z-status. Source of truth
# lives here so a future autoseed run cannot silently re-add them.
#   90  → 'בשכונה HO'         (chain headquarters)
#   900 → 'שבטי ישראל - ישן' (legacy/decommissioned store)
EXCLUDED_CHAIN_AVIV_IDS: set[int] = {90, 900}

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


def fetch_chain_branches(token: str) -> list[dict]:
    """POST /account/branches → [{id, name}, ...] for the logged-in chain account.

    Returns [] on any non-200 response or unexpected shape — the caller falls
    back to whatever's already in the local branches table. Names may be in
    Hebrew.
    """
    r = requests.post(f'{BASE}/account/branches', json={},
                      headers={'Authtoken': token, 'Content-Type': 'application/json'},
                      timeout=15, verify=False)
    if r.status_code != 200:
        log.warning('chain /account/branches non-200: %s body=%r',
                    r.status_code, r.text[:200])
        return []
    try:
        body = r.json()
    except Exception as e:
        log.warning('chain /account/branches non-JSON: %s', e)
        return []
    if not isinstance(body, list):
        log.warning('chain /account/branches unexpected shape: %s',
                    type(body).__name__)
        return []
    out: list[dict] = []
    for b in body:
        if not isinstance(b, dict):
            continue
        bid = b.get('id')
        name = b.get('name')
        if bid is None:
            continue
        try:
            out.append({'id': int(bid), 'name': str(name) if name else f'Aviv #{bid}'})
        except (TypeError, ValueError):
            continue
    return out


def autoseed_chain_branches(conn, chain_branches: list[dict]) -> list[int]:
    """For each chain branch with no existing row in `branches` that has the
    same aviv_branch_id, INSERT OR IGNORE a synthetic row. Returns the list of
    local branch ids that were newly seeded.

    Synthetic local id = CHAIN_AUTOSEED_LOCAL_ID_OFFSET + aviv_branch_id.
    Existing rows (e.g. 126→aviv 3, 127→aviv 8) are detected via aviv_branch_id
    and skipped — never reassigned.
    """
    if not chain_branches:
        return []
    existing = {row['aviv_branch_id'] for row in conn.execute(
        "SELECT aviv_branch_id FROM branches "
        "WHERE aviv_branch_id IS NOT NULL").fetchall()}
    seeded: list[int] = []
    for b in chain_branches:
        aviv_id = b['id']
        if aviv_id in EXCLUDED_CHAIN_AVIV_IDS:
            # HQ / legacy / non-store chain entries — never seed.
            continue
        if aviv_id in existing:
            continue
        local_id = CHAIN_AUTOSEED_LOCAL_ID_OFFSET + aviv_id
        cur = conn.execute(
            "INSERT OR IGNORE INTO branches (id, name, active, aviv_branch_id) "
            "VALUES (?, ?, 1, ?)",
            (local_id, b['name'], aviv_id))
        if cur.rowcount > 0:
            seeded.append(local_id)
    if seeded:
        conn.commit()
    return seeded


def _refresh(token: str) -> str:
    time.sleep(0.3)
    r = requests.post(f'{BASE}/account/refresh',
                      headers={'Authtoken': token, 'Content-Type': 'application/json'},
                      json={}, timeout=10, verify=False)
    j = r.json()
    return j.get('token') or j.get('value') or token


def fetch_902_filters(aviv_branch_id: int, token: str) -> dict:
    """GET /reports/filters/902?branch=X → raw JSON.

    For eager branches (e.g. 8, 127) this body contains the full Z list under
    ID_Z.possibleValues. For lazy branches (e.g. 1) possibleValues is null and
    we have to go to the possible-values endpoint instead. Kept as the
    fallback path; the primary read is now fetch_902_z_list().
    """
    url = f'{BASE}/reports/filters/{Z_REPORT_ID}?branch={aviv_branch_id}'
    r = requests.get(url, headers={'Authtoken': token}, timeout=30, verify=False)
    if r.status_code == 401:
        raise AuthExpired('filters/902 401')
    r.raise_for_status()
    return r.json()


def fetch_902_id_z_possible_values(aviv_branch_id: int, token: str) -> list:
    """GET /reports/filters/902/possible-values?filter=ID_Z&branch=X → Z list.

    This is the endpoint the BI web UI uses to populate the Z dropdown. It
    works under chain auth for EVERY branch — including branches like 1
    whose main /reports/filters/902 returns ID_Z.possibleValues=null. Body
    shape is a flat list of single-key dicts: [{"<z>": "Z: <z>|DD/MM/YYYY"}, ...]
    which _iter_z_entries already handles.
    """
    url = (f'{BASE}/reports/filters/{Z_REPORT_ID}/possible-values'
           f'?filter=ID_Z&branch={aviv_branch_id}')
    r = requests.get(url, headers={'Authtoken': token}, timeout=30, verify=False)
    if r.status_code == 401:
        raise AuthExpired('filters/902/possible-values 401')
    r.raise_for_status()
    return r.json()


def fetch_902_z_list(aviv_branch_id: int, token: str):
    """Return a JSON body resolve_z_for_date can iterate.

    Tries the possible-values endpoint first (works for every chain branch).
    If it 200s with at least one Z entry, that body is returned. If it fails
    (non-200, transport error, empty list, parse error), falls back to the
    legacy /reports/filters/902 read so eager branches keep working even if
    the new endpoint regresses.

    AuthExpired is allowed to propagate so run_for_branch's re-auth retry
    still works against the new endpoint.
    """
    try:
        body = fetch_902_id_z_possible_values(aviv_branch_id, token)
        if _iter_z_entries(body):
            log.info('branch_aviv=%d Z-list via possible-values',
                     aviv_branch_id)
            return body
        log.info('branch_aviv=%d possible-values returned no Z entries, '
                 'falling back to filters/902', aviv_branch_id)
    except AuthExpired:
        raise
    except Exception as e:
        log.warning('branch_aviv=%d possible-values failed (%s) — '
                    'falling back to filters/902',
                    aviv_branch_id, str(e)[:160])
    return fetch_902_filters(aviv_branch_id, token)


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


def build_submit_body(from_z: int, to_z: int, output_type: str = 'PDF') -> dict:
    """Exact body shape captured from BI DevTools.

    output_type: 'PDF' (default; what /sales preview reads) or 'XLS' (structured
    Excel — the only other outputType the server accepts for 902; XLSX/JSON/
    CSV/HTML all 400). XLS is parsed by the dept agent to extract the
    per-department breakdown that the PDF only surfaces as RTL-reversed text.
    """
    return {
        'id': Z_REPORT_ID,
        'outputType': output_type,
        'filters': [
            {'id': 1, 'name': 'ID_Z', 'filterType': 'INTEGER', 'value': from_z},
            {'id': 2, 'name': 'TO_Z', 'filterType': 'INTEGER', 'value': to_z},
        ],
    }


def submit_902(aviv_branch_id: int, z_number: int, token: str,
               output_type: str = 'PDF') -> str:
    """POST /reports/result/?branch=X → file url. Raises AuthExpired on 401."""
    body = build_submit_body(z_number, z_number, output_type=output_type)
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


def download_xls(file_url: str, token: str) -> bytes:
    """GET the report URL with Authtoken → XLS bytes (legacy BIFF .xls)."""
    r = requests.get(file_url, headers={'Authtoken': token},
                     timeout=60, verify=False)
    if r.status_code == 401:
        raise AuthExpired('xls download 401')
    r.raise_for_status()
    return r.content


# ---- XLS dept parser ------------------------------------------------------
#
# Aviv's 902 XLS has the per-department breakdown as a contiguous block on the
# single sheet ('העתק Z'). Shape captured live from branch 127 Z 1324:
#
#   row N   : ['מכירות בחתך מחלקה', '', '', ...]               ← section title
#   row N+1 : ["סה''כ", 'כמות', 'מחלקה']                        ← column header
#   row N+2 : ['<amount>', '<qty>', '<code> <name>']           ← dept rows
#   ...
#   row M   : ['<total_amount>', '<total_qty>', "סה''כ"]        ← terminator
#
# Hebrew is NOT RTL-reversed here (unlike the PDF). Column C contains the dept
# code as a leading integer followed by a space and the Hebrew name. Some rows
# are blank (Aviv pads with empty rows between groups in some renders) — those
# get skipped silently. The terminator is a row whose column-C value equals
# the Hebrew "סה''כ" exactly.
#
# Locating the section: scan rows for any cell containing 'מחלקה' AND look
# ahead for the "סה''כ / כמות / מחלקה" header. Don't hardcode a row index.

_DEPT_SECTION_TITLE = 'מכירות בחתך מחלקה'
_DEPT_TOTAL_LABEL = "סה''כ"
_DEPT_HEADER_QTY = 'כמות'
_DEPT_HEADER_NAME = 'מחלקה'
_DEPT_CODE_NAME_RE = re.compile(r'^\s*(\d+)\s+(.+?)\s*$')


def _xls_cell_str(v) -> str:
    """xlrd returns floats for numeric cells; normalize everything to str."""
    if v is None:
        return ''
    if isinstance(v, float):
        # Codes/qty come through as floats from xlrd. Keep precision for qty;
        # the caller handles int parsing for the dept code.
        if v.is_integer():
            return str(int(v))
        return str(v)
    return str(v).strip()


def _xls_cell_float(v) -> float | None:
    """Parse a cell value as float. Strings may have commas (rare here)."""
    if v is None or v == '':
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(',', '').strip())
    except (ValueError, AttributeError):
        return None


def parse_902_xls_departments(xls_bytes: bytes) -> list[dict]:
    """Extract dept rows from a 902 XLS.

    Aviv's XLS lays the dept section out as 3 columns that are NOT adjacent —
    captured live, the cells live in columns 0 / 19 / 30 (amount / qty /
    "<code> <name>") with everything between them empty. Future renders may
    shift these positions, so we discover them from the header row itself
    rather than hardcoding indices: the header row has the literal strings
    "סה''כ" / "כמות" / "מחלקה" in three columns, and the data rows below it
    use the same three column indices.

    Returns a list of {'dept_code', 'dept_name', 'amount', 'qty'} dicts (one
    per dept). Empty list if the section can't be located — never raises on
    parse anomalies; callers treat dept data as supplementary.
    """
    try:
        import xlrd
    except ImportError:
        log.warning('xlrd not installed; cannot parse 902 XLS departments')
        return []

    try:
        wb = xlrd.open_workbook(file_contents=xls_bytes, formatting_info=False)
    except Exception as e:
        log.warning('open 902 XLS failed: %s', str(e)[:160])
        return []

    sh = wb.sheet_by_index(0)

    # 1) Find the header row + the columns it places amount / qty / name in.
    amount_col = qty_col = name_col = None
    header_row = None
    for i in range(sh.nrows):
        cells = {c: _xls_cell_str(sh.cell_value(i, c)) for c in range(sh.ncols)
                 if sh.cell_value(i, c) != ''}
        if not cells:
            continue
        has_total = any(v == _DEPT_TOTAL_LABEL for v in cells.values())
        has_qty = any(v == _DEPT_HEADER_QTY for v in cells.values())
        has_name = any(v == _DEPT_HEADER_NAME for v in cells.values())
        if has_total and has_qty and has_name:
            for c, v in cells.items():
                if v == _DEPT_TOTAL_LABEL:
                    amount_col = c
                elif v == _DEPT_HEADER_QTY:
                    qty_col = c
                elif v == _DEPT_HEADER_NAME:
                    name_col = c
            header_row = i
            break

    if header_row is None:
        log.warning('dept section header not found in 902 XLS')
        return []
    if amount_col is None or qty_col is None or name_col is None:
        log.warning('dept header row %d missing one of the three columns '
                    '(amount=%s qty=%s name=%s)',
                    header_row, amount_col, qty_col, name_col)
        return []

    # 2) Iterate rows after the header until the terminator (name col == "סה''כ").
    departments: list[dict] = []
    seen_codes: set[int] = set()
    for i in range(header_row + 1, sh.nrows):
        a_raw = sh.cell_value(i, amount_col)
        b_raw = sh.cell_value(i, qty_col)
        c_str = _xls_cell_str(sh.cell_value(i, name_col))

        if not c_str and (a_raw == '' or a_raw is None) and (
                b_raw == '' or b_raw is None):
            # Blank padding row — Aviv splits the table mid-stream sometimes.
            continue

        if c_str.strip() == _DEPT_TOTAL_LABEL:
            # Terminator row (grand total). Do not store as a dept.
            break

        m = _DEPT_CODE_NAME_RE.match(c_str)
        if not m:
            # Unexpected shape (e.g. a sub-header) — log and skip rather than
            # poison the table.
            log.info('skipping unparseable dept row %d: name_col=%r', i, c_str)
            continue

        try:
            dept_code = int(m.group(1))
        except ValueError:
            log.info('skipping dept row %d with non-int code: %r', i, m.group(1))
            continue
        dept_name = m.group(2).strip()
        amount = _xls_cell_float(a_raw)
        qty = _xls_cell_float(b_raw)
        if amount is None:
            # No amount = nothing useful to store.
            continue
        if dept_code in seen_codes:
            # Defensive: re-runs of a corrupted Z occasionally repeat a row.
            # Keep the first occurrence; log the second.
            log.info('duplicate dept_code=%d at row %d — keeping first',
                     dept_code, i)
            continue
        seen_codes.add(dept_code)
        departments.append({
            'dept_code': dept_code,
            'dept_name': dept_name,
            'amount': amount,
            'qty': qty,
        })

    return departments


# ---- DB upsert ------------------------------------------------------------

def record_closed_day(conn, branch_id: int, target_date: str,
                      trigger_type: str = 'auto',
                      auth_source: str | None = None) -> None:
    """Insert a sentinel row (z_number=NULL, amount=NULL) so backfill passes
    later in the night recognize this (branch, date) as resolved-no-data and
    don't re-probe Aviv. INSERT OR IGNORE: never overwrite a real row.

    Metadata is recorded for the sentinel too — /z-status surfaces it the
    same way it does for real rows.
    """
    if trigger_type not in TRIGGER_TYPES:
        trigger_type = 'auto'
    if auth_source is not None and auth_source not in AUTH_SOURCES:
        auth_source = None
    conn.execute('''
        INSERT OR IGNORE INTO z_report_902
          (branch_id, date, z_number, amount, transactions, avg_per_txn,
           payment_breakdown, fetched_at, trigger_type, auth_source)
        VALUES (?, ?, NULL, NULL, NULL, NULL, NULL,
                datetime('now'), ?, ?)
    ''', (branch_id, target_date, trigger_type, auth_source))
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


# Allowed enum values for trigger_type / auth_source. Anything else gets
# rejected at write time so /z-status never has to handle bogus strings.
TRIGGER_TYPES = ('auto', 'manual')
AUTH_SOURCES = ('chain', 'per_store')


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


def upsert_department_sales(conn, branch_id: int, target_date: str,
                            departments: list[dict]) -> int:
    """INSERT OR REPLACE every dept row for (branch_id, date).

    Returns the number of rows written. The transaction first clears any
    prior rows for the (branch, date) so a re-pull with FEWER departments
    (Aviv occasionally drops a dept from the report) doesn't leave stale
    rows behind. Single commit at the end.
    """
    if not departments:
        return 0
    conn.execute(
        'DELETE FROM z_department_sales WHERE branch_id=? AND date=?',
        (branch_id, target_date))
    conn.executemany(
        'INSERT OR REPLACE INTO z_department_sales '
        '(branch_id, date, dept_code, dept_name, amount, qty, fetched_at) '
        "VALUES (?, ?, ?, ?, ?, ?, datetime('now'))",
        [(branch_id, target_date, d['dept_code'], d['dept_name'],
          d['amount'], d.get('qty')) for d in departments])
    conn.commit()
    return len(departments)


def upsert_z_report(conn, branch_id: int, target_date: str, z_number: int,
                    parsed: dict, trigger_type: str = 'auto',
                    auth_source: str | None = None) -> None:
    """Write to z_report_902 ONLY. Never daily_sales.

    trigger_type ('auto'/'manual') and auth_source ('chain'/'per_store') are
    recorded on every write so /z-status can surface provenance accurately.
    Unknown values are coerced (trigger_type → 'auto', auth_source → NULL)
    rather than silently propagating bad data into the table.
    """
    if trigger_type not in TRIGGER_TYPES:
        trigger_type = 'auto'
    if auth_source is not None and auth_source not in AUTH_SOURCES:
        auth_source = None
    pb = parsed.get('payment_breakdown')
    pb_json = json.dumps(pb, ensure_ascii=False) if pb else None
    conn.execute('''
        INSERT INTO z_report_902
          (branch_id, date, z_number, amount, transactions, avg_per_txn,
           payment_breakdown, fetched_at, trigger_type, auth_source)
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?)
        ON CONFLICT(branch_id, date) DO UPDATE SET
          z_number=excluded.z_number,
          amount=excluded.amount,
          transactions=excluded.transactions,
          avg_per_txn=excluded.avg_per_txn,
          payment_breakdown=excluded.payment_breakdown,
          fetched_at=excluded.fetched_at,
          trigger_type=excluded.trigger_type,
          auth_source=excluded.auth_source
    ''', (branch_id, target_date, z_number,
          parsed.get('total'), parsed.get('transactions'),
          parsed.get('avg_per_txn'), pb_json,
          trigger_type, auth_source))
    conn.commit()


# ---- Per-branch runner ----------------------------------------------------

def run_for_branch(branch_id: int, target_date: str | None = None,
                   conn: sqlite3.Connection | None = None,
                   chain_token: str | None = None,
                   trigger_type: str = 'auto') -> dict:
    """Fetch + parse + upsert one branch's Z for target_date (default yesterday).

    If chain_token is provided, skip per-branch login and read aviv_branch_id
    from the branches table (chain-account mode). auth_source is derived
    here: 'chain' if chain_token was passed in, 'per_store' otherwise.
    trigger_type is recorded verbatim on the row.
    """
    target_date = target_date or _yesterday_il()
    auth_source = 'chain' if chain_token is not None else 'per_store'

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

        # Retry Z-list fetch on transient Aviv failures (404/5xx/network/timeout)
        # against a wall-clock budget — see FILTERS_RETRY_TOTAL_SECONDS comment.
        # A 200 response is treated as authoritative — closed-day "no Z for date"
        # is the legitimate skip path and must NOT retry.
        # fetch_902_z_list prefers the possible-values endpoint and falls back
        # to filters/902 internally — see its docstring.
        filters = None
        last_err: Exception | None = None
        fetch_t0 = time.time()
        deadline = fetch_t0 + FILTERS_RETRY_TOTAL_SECONDS
        attempt = 0
        while True:
            attempt += 1
            try:
                filters = fetch_902_z_list(aviv_branch_id, token)
                elapsed = time.time() - fetch_t0
                if attempt > 1:
                    log.info(
                        'branch=%d Z-list succeeded on attempt %d after %.1fs '
                        '(retry-through)', branch_id, attempt, elapsed)
                break
            except Exception as e:
                last_err = e
                elapsed = time.time() - fetch_t0
                log.warning(
                    'branch=%d Z-list attempt %d failed at +%.1fs: %s',
                    branch_id, attempt, elapsed, str(e)[:160])
                if time.time() >= deadline:
                    break
                # Capped-linear backoff: 5, 10, 15, 20, then 30 repeating.
                bidx = min(attempt - 1, len(_FILTERS_BACKOFF_SCHEDULE) - 1)
                sleep_secs = _FILTERS_BACKOFF_SCHEDULE[bidx]
                sleep_secs = min(sleep_secs, max(0.0, deadline - time.time()))
                if sleep_secs <= 0:
                    break
                time.sleep(sleep_secs)
                # Refresh token between attempts in case it expired. If re-auth
                # itself blows up, the cause isn't transient — bail.
                try:
                    token, aviv_branch_id = _reauth()
                except Exception as re_err:
                    log.error(
                        'branch=%d re-auth failed mid-retry: %s — aborting '
                        'retry loop', branch_id, re_err)
                    last_err = re_err
                    break
        if filters is None:
            elapsed = time.time() - fetch_t0
            return {'ok': False, 'branch_id': branch_id, 'date': target_date,
                    'error': f'Z-list fetch transient-give-up after {attempt} '
                             f'attempts over {elapsed:.0f}s: '
                             f'{str(last_err)[:160]}'}

        z_number = resolve_z_for_date(filters, target_date)
        if not z_number:
            # Filters call succeeded but no Z for this date → store was closed.
            # Mark resolved so 03/04/05 backfill passes skip this branch.
            record_closed_day(conn, branch_id, target_date,
                              trigger_type=trigger_type,
                              auth_source=auth_source)
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
        upsert_z_report(conn, branch_id, target_date, z_number, parsed,
                        trigger_type=trigger_type,
                        auth_source=auth_source)
        log.info('branch=%d date=%s z=%d total=%.2f txns=%s',
                 branch_id, target_date, z_number,
                 parsed['total'], parsed['transactions'])

        # Department breakdown — supplementary; never fails the Z pull.
        # Rides on the same Z that landed: same auth, same z_number, same
        # branch URL. Wrapped in a broad try so any XLS-side failure
        # (network, parse, server flake) is logged and swallowed.
        try:
            xls_url = submit_902(aviv_branch_id, z_number, token,
                                 output_type='XLS')
            xls_bytes = download_xls(xls_url, token)
            departments = parse_902_xls_departments(xls_bytes)
            if departments:
                n = upsert_department_sales(conn, branch_id, target_date,
                                            departments)
                log.info('branch=%d date=%s dept_rows=%d', branch_id,
                         target_date, n)
            else:
                log.warning('branch=%d date=%s dept parse returned 0 rows',
                            branch_id, target_date)
        except AuthExpired:
            # 401 mid-XLS — re-auth and retry once, mirroring the PDF path's
            # one-shot retry. Still wrapped so a second failure doesn't
            # bubble out and break the Z pull's return value.
            try:
                token, aviv_branch_id = _reauth()
                xls_url = submit_902(aviv_branch_id, z_number, token,
                                     output_type='XLS')
                xls_bytes = download_xls(xls_url, token)
                departments = parse_902_xls_departments(xls_bytes)
                if departments:
                    n = upsert_department_sales(conn, branch_id, target_date,
                                                departments)
                    log.info('branch=%d date=%s dept_rows=%d (post-reauth)',
                             branch_id, target_date, n)
            except Exception as e:
                log.warning('branch=%d date=%s dept pull failed after '
                            're-auth: %s', branch_id, target_date,
                            str(e)[:160])
        except Exception as e:
            log.warning('branch=%d date=%s dept pull failed: %s',
                        branch_id, target_date, str(e)[:160])

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
    # Belt-and-suspenders: even if migration 014 was skipped or someone
    # re-seeded HQ/legacy by hand, filter them out at iteration time too.
    # Only applicable in chain_mode — per-store dbs may not have the column.
    if chain_mode and EXCLUDED_CHAIN_AVIV_IDS:
        exclude_csv = ','.join(str(x) for x in sorted(EXCLUDED_CHAIN_AVIV_IDS))
        where += f' AND aviv_branch_id NOT IN ({exclude_csv})'
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
                     conn: sqlite3.Connection | None = None,
                     trigger_type: str = 'auto') -> list[dict]:
    """Run every active branch sequentially. One branch's failure never aborts the loop.

    With missing_only=True, branches that already have a z_report_902 row for
    target_date (real Z or closed-day sentinel) are skipped — used by the
    03/04/05 IL backfill passes.
    """
    target_date = target_date or _yesterday_il()
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
            # Autoseed must run BEFORE _branch_ids_for_date so newly seeded
            # branches are included in the iteration list. We need the token
            # first to call /account/branches.
            if AUTOSEED_CHAIN:
                try:
                    chain_token = _login_chain_account()
                    chain_token = _refresh(chain_token)
                    chain_list = fetch_chain_branches(chain_token)
                    seeded = autoseed_chain_branches(conn, chain_list)
                    if seeded:
                        log.info('autoseed: %d new chain branch row(s): %s',
                                 len(seeded), seeded)
                    else:
                        log.info('autoseed: 0 new rows (got %d chain branches '
                                 'from /account/branches)', len(chain_list))
                    # Recompute the branch list now that autoseed may have
                    # added rows.
                    bids = _branch_ids_for_date(conn, target_date, missing_only,
                                                chain_mode=True)
                except Exception as e:
                    log.error('autoseed failed; falling back to existing '
                              'branches table: %s', e)
                    chain_token = None

            if not bids:
                log.info('chain mode: no branches with aviv_branch_id set')
            else:
                try:
                    if chain_token is None:
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
                                              chain_token=chain_token,
                                              trigger_type=trigger_type))
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
    ap.add_argument('--manual', action='store_true',
                    help="Mark this run as trigger_type='manual' in z_report_902. "
                         "Default is 'auto' (cron/scheduler invocation).")
    args = ap.parse_args()

    trigger = 'manual' if args.manual else 'auto'

    if args.branch_id:
        # Single-branch CLI: in chain mode, issue a chain token here so
        # autoseeded rows (which have no per-store creds) can still be pulled
        # one at a time. Without this the agent would error 'no aviv creds'
        # for any branch that came from /account/branches autoseed.
        chain_token = None
        if USE_CHAIN_AUTH:
            try:
                chain_token = _refresh(_login_chain_account())
            except Exception as e:
                log.error('chain login failed for single-branch CLI: %s', e)
                sys.exit(2)
        out = run_for_branch(args.branch_id, args.date,
                             chain_token=chain_token, trigger_type=trigger)
        print(out)
        sys.exit(0 if out.get('ok') else 1)
    else:
        out = run_all_branches(args.date, missing_only=args.missing_only,
                               trigger_type=trigger)
        for r in out:
            print(r)
        sys.exit(0 if all(r.get('ok') for r in out) else 1)

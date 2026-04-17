"""
Gmail Z-report agent (branch-aware) — connects to Gmail via IMAP,
searches for Z-report emails matching branch.gmail_label in subject,
parses PDF attachments with PyMuPDF, saves to daily_sales.
"""

import email
import email.header
import email.utils
import imaplib
import logging
import os
import re
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from utils.notify import notify


def _friendly_gmail_error(e: Exception) -> str:
    msg = str(e)
    if 'AUTHENTICATIONFAILED' in msg:
        return "Gmail authentication failed — app password may have changed."
    if 'IMAP' in msg or 'imap' in msg.lower():
        return "Gmail IMAP connection failed — could not reach inbox."
    if 'timeout' in msg.lower():
        return "Gmail connection timed out."
    if 'SSL' in msg:
        return "Gmail SSL connection error."
    return msg[:120]


DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993

# Gmail credentials — must be set in .env
GMAIL_ADDRESS = os.environ.get('GMAIL_ADDRESS', '')
GMAIL_APP_PASSWORD = os.environ.get('GMAIL_APP_PASSWORD', '')
AVIV_SENDER_EMAIL = os.environ.get('AVIV_SENDER_EMAIL', 'avivpost@avivpos.co.il')

# RTL PDF: "20295.85 ₪ :כ"הס"
TOTAL_PATTERN_RTL = re.compile(r'([\d,]+\.?\d*)\s*₪\s*:כ"הס')
# LTR: סה"כ: ₪ 12377.92
TOTAL_PATTERN_LTR = re.compile(r'סה["\u05f4]כ[:\s]+₪?\s*([\d,]+\.?\d*)')
# Transaction count: "200 תואקסע תומכ" (RTL for "כמות עסקאות 200")
TRANSACTIONS_PATTERN = re.compile(r'(\d+)\s*תואקסע\s*תומכ')


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
    logger = logging.getLogger(f'gmail_{branch_id}')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        log_dir = Path(__file__).parent.parent / 'logs'
        log_dir.mkdir(exist_ok=True)
        fh = logging.FileHandler(log_dir / f'gmail_{branch_id}.log', encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(sh)
    return logger


def _decode_filename(raw: str) -> str:
    if not raw:
        return ""
    parts = email.header.decode_header(raw)
    result = ""
    for part, enc in parts:
        if isinstance(part, bytes):
            result += part.decode(enc or "utf-8", errors="replace")
        else:
            result += part
    return result.strip()


def _extract_z_pdf(msg) -> bytes | None:
    for part in msg.walk():
        ct = part.get_content_type()
        if ct not in ("application/pdf", "application/octet-stream"):
            continue
        raw_fn = part.get_filename() or ""
        filename = _decode_filename(raw_fn)
        if filename.lower().startswith("z_") and filename.lower().endswith(".pdf"):
            return part.get_payload(decode=True)
    return None


def _extract_total_from_pdf(pdf_bytes: bytes) -> tuple[float | None, int]:
    """Extract total amount and transaction count from Z-report PDF.
    Returns (total, transactions)."""
    import io
    total = None
    transactions = 0
    full_text = ""

    # Try pdfplumber first (matches MakoletDashboard's working parser)
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                full_text += text + "\n"
                if total is None:
                    match = TOTAL_PATTERN_RTL.search(text)
                    if match:
                        total = float(match.group(1).replace(",", ""))
                    else:
                        match = TOTAL_PATTERN_LTR.search(text)
                        if match:
                            total = float(match.group(1).replace(",", ""))
    except ImportError:
        pass

    # Fallback to PyMuPDF if pdfplumber didn't find total
    if total is None:
        try:
            import fitz
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            for page in doc:
                text = page.get_text()
                full_text += text + "\n"
                if total is None:
                    match = TOTAL_PATTERN_RTL.search(text)
                    if match:
                        total = float(match.group(1).replace(",", ""))
                    else:
                        match = TOTAL_PATTERN_LTR.search(text)
                        if match:
                            total = float(match.group(1).replace(",", ""))
            doc.close()
        except ImportError:
            pass

    # Extract transaction count from full text
    tx_match = TRANSACTIONS_PATTERN.search(full_text)
    if tx_match:
        transactions = int(tx_match.group(1))

    return total, transactions


def _parse_attendance_csv(csv_text: str) -> list[dict]:
    """Parse Aviv attendance CSV into employee records.

    CSV columns: עובד, יום בשבוע, תאריך כניסה, תאריך יציאה, הערות, כמות שעות
    Employee rows start with 'ID NAME', continuation rows have empty first col.
    Summary rows: סה''כ שורות N  with total hours as HH:MM in last column.

    NOTE: The same employee may appear with different name variants mid-CSV
    (e.g. "441 עידן בקון" then "441 עידן בקון איינשטיין"). We track by
    employee ID so the summary line captures the correct total.
    """
    employees = []
    current_name = None
    current_emp_id = None
    current_hours = 0.0

    for line in csv_text.strip().splitlines():
        cols = line.split(',')
        if len(cols) < 6:
            continue

        first_col = cols[0].strip()
        hours_col = cols[-1].strip()

        # Summary row for current employee
        if first_col.startswith("סה''כ שורות") or first_col.startswith('סה"כ שורות'):
            if current_name and hours_col:
                # Parse HH:MM format
                try:
                    parts = hours_col.split(':')
                    h = int(parts[0])
                    m = int(parts[1]) if len(parts) > 1 else 0
                    current_hours = h + m / 60.0
                except (ValueError, IndexError):
                    pass
                employees.append({
                    'name': current_name,
                    'csv_emp_id': current_emp_id,
                    'total_hours': round(current_hours, 2),
                })
            current_name = None
            current_emp_id = None
            current_hours = 0.0
            continue

        # New employee row (starts with digit = employee ID)
        if first_col and first_col[0].isdigit():
            # Extract ID and name: "382 רועי אמסלם" -> id=382, name="רועי אמסלם"
            parts = first_col.split(None, 1)
            if len(parts) >= 2:
                new_id = parts[0]
                new_name = parts[1].strip()
                # Same employee ID with different name variant — keep longest name
                if current_emp_id == new_id:
                    if len(new_name) > len(current_name or ''):
                        current_name = new_name
                else:
                    current_emp_id = new_id
                    current_name = new_name
            continue

    return employees


def _check_alias(csv_name: str, branch_id: int, db_employees: list):
    """Check employee_aliases table for a match. Returns (emp_id, confidence, name, rate) or None."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        alias = conn.execute(
            '''SELECT ea.employee_id FROM employee_aliases ea
               JOIN employees e ON e.id = ea.employee_id
               WHERE ea.branch_id=? AND ea.alias_name=? AND e.active=1''',
            (branch_id, csv_name.strip())
        ).fetchone()
        conn.close()
        if alias:
            emp_id = alias['employee_id']
            for emp in db_employees:
                if emp['id'] == emp_id:
                    return (emp_id, 'exact', emp['name'], emp['hourly_rate'])
    except Exception:
        pass
    return None


def _match_employee_name(csv_name: str, db_employees: list, branch_name: str = '', branch_id: int = 0) -> tuple:
    """Match CSV employee name to DB employee.

    Returns (employee_id, confidence, matched_db_name, hourly_rate)
    confidence: 'exact', 'high', 'low', 'none'
    """
    # Check aliases first
    if branch_id:
        alias_match = _check_alias(csv_name, branch_id, db_employees)
        if alias_match:
            return alias_match

    # Clean the CSV name: strip branch suffixes
    cleaned = _clean_name(csv_name, branch_name)

    best_match = None
    best_score = 0.0

    for emp in db_employees:
        db_name = emp['name'].strip()
        db_clean = _clean_name(db_name, branch_name)

        # Exact match after cleaning
        if cleaned == db_clean:
            return (emp['id'], 'exact', db_name, emp['hourly_rate'])

        # One contains the other (handles "עידן" matching "עידן בקון")
        if cleaned.startswith(db_clean) or db_clean.startswith(cleaned):
            return (emp['id'], 'exact', db_name, emp['hourly_rate'])

        csv_words = cleaned.split()
        db_words = db_clean.split()
        if not csv_words or not db_words:
            continue

        # First name matches
        if csv_words[0] == db_words[0]:
            overlap = len(set(csv_words) & set(db_words))
            score = overlap / max(len(csv_words), len(db_words))
            if score > best_score:
                best_score = score
                best_match = emp

        # First + last name match (ignore middle names)
        if len(db_words) >= 2:
            first, last = db_words[0], db_words[-1]
            if first in csv_words and last in csv_words:
                score = 0.8
                if score > best_score:
                    best_score = score
                    best_match = emp

        if len(csv_words) >= 2:
            first, last = csv_words[0], csv_words[-1]
            if first in db_words and last in db_words:
                score = 0.8
                if score > best_score:
                    best_score = score
                    best_match = emp

    if best_match:
        if best_score >= 0.5:
            return (best_match['id'], 'high', best_match['name'], best_match['hourly_rate'])
        else:
            return (best_match['id'], 'low', best_match['name'], best_match['hourly_rate'])

    return (None, 'none', None, 0)


def _ensure_pending_table(conn):
    """Create employee_match_pending table if it doesn't exist."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS employee_match_pending (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER,
            month TEXT,
            csv_name TEXT,
            suggested_employee_id INTEGER,
            confidence TEXT,
            hours REAL,
            salary REAL,
            created_at TEXT DEFAULT (datetime('now')),
            resolved INTEGER DEFAULT 0
        )
    ''')
    # Add is_new_employee column if missing
    try:
        conn.execute('ALTER TABLE employee_match_pending ADD COLUMN is_new_employee INTEGER DEFAULT 0')
        conn.commit()
    except sqlite3.OperationalError:
        pass
    # Ensure employee_aliases table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS employee_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            alias_name TEXT NOT NULL,
            branch_id INTEGER NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(branch_id, alias_name)
        )
    ''')


def _sync_attendance_csv(mail, branch: dict, branch_id: int, log) -> str | None:
    """Search for attendance CSV emails, parse and save employee hours."""
    gmail_label = branch.get('gmail_label') or ''
    if not gmail_label:
        return None

    # Determine current month
    now_il = datetime.now(ZoneInfo('Asia/Jerusalem'))
    current_month = now_il.strftime('%Y-%m')

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    _ensure_pending_table(conn)

    # Search for recent emails (can't use Hebrew in IMAP SUBJECT search)
    since_str = (date.today() - timedelta(days=35)).strftime("%d-%b-%Y")
    criteria = f'(SINCE "{since_str}")'
    status, data = mail.search(None, criteria)

    if status != "OK" or not data or not data[0]:
        log.info("No recent emails found for attendance CSV")
        conn.close()
        return None

    msg_ids = data[0].split()
    log.info("Scanning %d recent emails for attendance CSV", len(msg_ids))

    csv_content = None
    report_month = current_month  # default, overridden by email date
    for msg_id in reversed(msg_ids):  # Most recent first
        status, msg_data = mail.fetch(msg_id, "(RFC822)")
        if status != "OK":
            continue

        msg = email.message_from_bytes(msg_data[0][1])
        subject = str(email.header.make_header(email.header.decode_header(msg.get("Subject", ""))))

        # Must match branch label and contain attendance keyword
        if gmail_label not in subject:
            continue
        if 'נוכחות' not in subject:
            continue

        # Detect report month from email date
        # CSV sent on 1st-5th of month belongs to PREVIOUS month
        msg_date_str = msg.get("Date", "")
        try:
            msg_date = email.utils.parsedate_to_datetime(msg_date_str)
            if msg_date.day <= 5:
                prev = msg_date.replace(day=1) - timedelta(days=1)
                report_month = prev.strftime('%Y-%m')
            else:
                report_month = msg_date.strftime('%Y-%m')
            log.info("Email date: %s → report month: %s", msg_date_str, report_month)
        except Exception:
            log.warning("Could not parse email date: %s, using current month", msg_date_str)

        # Look for CSV attachment
        for part in msg.walk():
            fn_raw = part.get_filename()
            if not fn_raw:
                continue
            decoded_parts = email.header.decode_header(fn_raw)
            filename = ""
            for p, enc in decoded_parts:
                if isinstance(p, bytes):
                    filename += p.decode(enc or "utf-8", errors="replace")
                else:
                    filename += p

            if '.csv' not in filename.lower():
                continue

            payload = part.get_payload(decode=True)
            if not payload:
                continue

            # Try decoding
            for enc in ['utf-8', 'windows-1255', 'iso-8859-8']:
                try:
                    csv_content = payload.decode(enc)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue

            if csv_content:
                log.info("Found attendance CSV: %s (%d bytes)", filename, len(payload))
                break

        if csv_content:
            break

    if not csv_content:
        log.info("No attendance CSV attachment found")
        conn.close()
        return None

    # Check if CSV already verified for this month
    csv_verified = conn.execute(
        "SELECT COUNT(*) as cnt FROM employee_hours_discrepancies WHERE branch_id=? AND month=?",
        (branch_id, report_month)
    ).fetchone()['cnt']
    if csv_verified > 0:
        log.info("CSV already verified for %s — skipping", report_month)
        conn.close()
        return "already verified"

    # Parse CSV
    employees = _parse_attendance_csv(csv_content)
    if not employees:
        log.warning("Could not parse any employees from attendance CSV")
        conn.close()
        return None

    # Ensure discrepancies table exists
    conn.execute('''CREATE TABLE IF NOT EXISTS employee_hours_discrepancies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        branch_id INTEGER NOT NULL,
        month TEXT NOT NULL,
        employee_id INTEGER,
        employee_name TEXT NOT NULL,
        api_hours REAL,
        csv_hours REAL,
        difference REAL,
        created_at TEXT DEFAULT (datetime('now')),
        resolved INTEGER DEFAULT 0,
        resolution TEXT
    )''')

    # Load DB employees for matching
    db_employees = [dict(r) for r in conn.execute(
        "SELECT id, name, hourly_rate FROM employees WHERE branch_id = ? AND active = 1",
        (branch_id,)
    ).fetchall()]

    # Get branch name for fuzzy matching
    branch_row = conn.execute("SELECT name FROM branches WHERE id = ?", (branch_id,)).fetchone()
    branch_name = branch_row['name'] if branch_row else ''

    # Check if API data exists for this month
    api_rows = conn.execute(
        "SELECT employee_name, total_hours, source FROM employee_hours "
        "WHERE branch_id=? AND month=? AND source='aviv_api'",
        (branch_id, report_month)
    ).fetchall()
    has_api_data = len(api_rows) > 0
    api_hours_map = {r['employee_name']: r['total_hours'] for r in api_rows}

    # Ensure is_csv_only column exists on employee_match_pending
    try:
        conn.execute("ALTER TABLE employee_match_pending ADD COLUMN is_csv_only INTEGER DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        pass

    # Ensure verified_by_csv column exists on employee_hours
    try:
        conn.execute("ALTER TABLE employee_hours ADD COLUMN verified_by_csv INTEGER DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        pass

    total_hours_all = sum(e['total_hours'] for e in employees)
    matched_count = 0
    discrepancy_count = 0
    csv_only_count = 0
    saved_count = 0
    pending_count = 0
    csv_matched_api_names = set()  # track which API employees were found in CSV

    for emp in employees:
        emp_id, confidence, matched_name, rate = _match_employee_name(
            emp['name'], db_employees, branch_name, branch_id
        )
        salary = round(emp['total_hours'] * rate, 2) if rate > 0 else 0

        if confidence in ('exact', 'high') and matched_name:
            if has_api_data:
                # CSV is VERIFICATION — compare with existing API hours
                api_hours = api_hours_map.get(matched_name, None)
                if api_hours is not None:
                    csv_matched_api_names.add(matched_name)
                    diff = abs(api_hours - emp['total_hours'])
                    if diff > 0.5:
                        # Case 1b: Discrepancy
                        conn.execute(
                            "INSERT INTO employee_hours_discrepancies "
                            "(branch_id, month, employee_id, employee_name, api_hours, csv_hours, difference) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (branch_id, report_month, emp_id, matched_name,
                             api_hours, emp['total_hours'], round(diff, 2))
                        )
                        discrepancy_count += 1
                        log.warning("  DISCREPANCY %s: API=%.1fh CSV=%.1fh diff=%.1fh",
                                    matched_name, api_hours, emp['total_hours'], diff)
                    else:
                        # Case 1a: Match — mark verified
                        conn.execute(
                            "UPDATE employee_hours SET verified_by_csv = 1 "
                            "WHERE branch_id = ? AND month = ? AND employee_name = ?",
                            (branch_id, report_month, matched_name)
                        )
                        matched_count += 1
                        log.info("  MATCH %s: API=%.1fh CSV=%.1fh (within tolerance)",
                                 matched_name, api_hours, emp['total_hours'])
                else:
                    # Case 2: CSV employee NOT in API — flag as csv_only pending
                    conn.execute(
                        "INSERT INTO employee_match_pending "
                        "(branch_id, month, csv_name, suggested_employee_id, confidence, "
                        "hours, salary, is_new_employee, is_csv_only, source) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, 'csv')",
                        (branch_id, report_month, matched_name, emp_id, confidence,
                         emp['total_hours'], salary)
                    )
                    csv_only_count += 1
                    log.warning("  CSV-ONLY %s: %.1fh (not in API, pending manager approval)",
                                matched_name, emp['total_hours'])
            else:
                # No API data at all — save CSV as source of truth (old behavior)
                conn.execute(
                    "INSERT OR REPLACE INTO employee_hours "
                    "(branch_id, month, employee_name, total_hours, total_salary, source) "
                    "VALUES (?, ?, ?, ?, ?, 'csv')",
                    (branch_id, report_month, matched_name, emp['total_hours'], salary)
                )
                saved_count += 1
                log.info("  %s → %s (csv, %.1fh, ₪%.0f)",
                         emp['name'], matched_name, emp['total_hours'], salary)
        else:
            # Low confidence — save to pending
            is_new = 1 if confidence == 'none' and emp_id is None else 0
            conn.execute(
                "INSERT INTO employee_match_pending "
                "(branch_id, month, csv_name, suggested_employee_id, confidence, hours, salary, is_new_employee) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (branch_id, report_month, emp['name'], emp_id, confidence,
                 emp['total_hours'], salary, is_new)
            )
            pending_count += 1
            log.warning("  %s → PENDING (%s, suggested: %s)", emp['name'], confidence, matched_name)

    # Case 3: API employees NOT in CSV
    api_only_names = []
    if has_api_data:
        for api_name in api_hours_map:
            if api_name not in csv_matched_api_names:
                api_only_names.append(api_name)
                log.warning("  API-ONLY %s: %.1fh in API but not in CSV",
                            api_name, api_hours_map[api_name])

    conn.commit()
    conn.close()

    # Notification
    branch_label = branch.get('name', f'Branch {branch_id}')
    if has_api_data:
        parts = [f"{matched_count} matched"]
        if discrepancy_count:
            parts.append(f"{discrepancy_count} discrepancies")
        if csv_only_count:
            parts.append(f"{csv_only_count} CSV-only (pending)")
        if api_only_names:
            parts.append(f"{len(api_only_names)} API-only")
        detail = ', '.join(parts)
        result_msg = f"End-of-month verification {report_month}: {detail}"

        if discrepancy_count > 0 or csv_only_count > 0 or api_only_names:
            msg_parts = []
            if discrepancy_count:
                msg_parts.append(f"{discrepancy_count} hour discrepancies")
            if csv_only_count:
                msg_parts.append(f"{csv_only_count} employees in CSV but not in API")
            if api_only_names:
                names_str = ', '.join(api_only_names[:5])
                msg_parts.append(f"API-only employees not in CSV: {names_str}")
            notify(f"CSV Verification — {branch_label}",
                   f"{report_month}: {'. '.join(msg_parts)}. Review on the employees page.")
        else:
            notify(f"CSV Verification — {branch_label}",
                   f"All {matched_count} employees matched API data for {report_month}.")
    else:
        result_msg = (f"Attendance CSV {report_month}: {saved_count} employees saved"
                      f", {total_hours_all:.1f} hours")
        if pending_count > 0:
            notify(f"Attendance — {branch_label}",
                   f"{pending_count} employees from CSV could not be matched — manual review needed.")

    log.info(result_msg)
    return result_msg


def _clean_name(name: str, branch_name: str = '') -> str:
    """Strip branch/store name suffixes from employee name."""
    store_words = ['איינשטיין', 'אינשטיין', 'einstein']
    if branch_name:
        store_words.append(branch_name.strip())
        store_words.extend(branch_name.strip().split())

    words = name.strip().split()
    while words and any(w.lower() == words[-1].lower() for w in store_words):
        words.pop()
    return ' '.join(words).strip()


def _name_tokens(name: str) -> list:
    """Split name into tokens, lowercased."""
    return [w.strip() for w in name.split() if w.strip()]


def _match_employee_rate(csv_name: str, emp_rates: dict, branch_name: str = '') -> float:
    """Smart fuzzy matching between CSV employee name and DB employee names.

    Handles:
    - Exact match
    - Store name suffix in CSV (strip it)
    - Middle names inserted between first and last name
    - First + last name match regardless of middle names
    - Any 2+ consecutive DB name tokens appear in CSV name

    Returns hourly rate or 0.0 if no match found.
    """
    csv_clean = _clean_name(csv_name, branch_name)
    csv_tokens = _name_tokens(csv_clean)

    best_match_rate = 0.0
    best_score = 0

    for db_name, rate in emp_rates.items():
        db_clean = _clean_name(db_name, branch_name)
        db_tokens = _name_tokens(db_clean)

        # 1. Exact match (after cleaning)
        if csv_clean == db_clean:
            return rate

        # 2. One contains the other (prefix/suffix)
        if csv_clean.startswith(db_clean) or db_clean.startswith(csv_clean):
            return rate

        # 3. First + last name match (ignore middle names)
        if len(db_tokens) >= 2:
            first = db_tokens[0]
            last = db_tokens[-1]
            if first in csv_tokens and last in csv_tokens:
                score = 3
                if score > best_score:
                    best_score = score
                    best_match_rate = rate

        # 4. CSV first + last match DB (reversed — DB has middle name)
        if len(csv_tokens) >= 2:
            first = csv_tokens[0]
            last = csv_tokens[-1]
            if first in db_tokens and last in db_tokens:
                score = 3
                if score > best_score:
                    best_score = score
                    best_match_rate = rate

        # 5. Token overlap score — count matching tokens
        common = set(csv_tokens) & set(db_tokens)
        if len(common) >= 2:
            score = len(common)
            if score > best_score:
                best_score = score
                best_match_rate = rate

    return best_match_rate


def run_gmail_sync(branch_id: int) -> dict:
    """
    Search Gmail for Z-report emails matching branch.gmail_label,
    parse PDFs, save to daily_sales.
    Returns {success, new_reports, skipped}.
    """
    log = _setup_logger(branch_id)
    log.info("Starting Gmail sync for branch %d", branch_id)
    t0 = time.time()

    # Insert agent_runs start
    conn_run = _get_db()
    cur = conn_run.execute(
        "INSERT INTO agent_runs (branch_id, agent, started_at, status) VALUES (?, 'gmail', datetime('now'), 'running')",
        (branch_id,)
    )
    run_id = cur.lastrowid
    conn_run.commit()
    conn_run.close()

    try:
        if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
            log.error("GMAIL_ADDRESS or GMAIL_APP_PASSWORD not set in .env")
            return {'success': False, 'new_reports': 0, 'skipped': 0, 'error': 'missing gmail credentials in .env'}

        branch = _get_branch_config(branch_id)
        gmail_label = branch.get('gmail_label') or ''
        if not gmail_label:
            log.warning("No gmail_label for branch %d", branch_id)
            return {'success': False, 'new_reports': 0, 'skipped': 0, 'error': 'no gmail_label'}

        # Create PDF storage dir
        pdf_dir = Path(__file__).parent.parent / 'data' / 'pdfs' / str(branch_id)
        pdf_dir.mkdir(parents=True, exist_ok=True)

        # Connect to Gmail
        mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
        mail.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        mail.select("inbox")

        # Search last 7 days for emails from Aviv sender
        since_str = (date.today() - timedelta(days=7)).strftime("%d-%b-%Y")
        criteria = f'(FROM "{AVIV_SENDER_EMAIL}" SINCE "{since_str}")'
        status, data = mail.search(None, criteria)

        if status != "OK" or not data or not data[0]:
            log.info("No emails found")
            mail.logout()
            return {'success': True, 'new_reports': 0, 'skipped': 0}

        msg_ids = data[0].split()
        log.info("Found %d emails to scan", len(msg_ids))

        # Get existing dates in DB for this branch
        conn = _get_db()
        cutoff = (date.today() - timedelta(days=7)).isoformat()
        existing = set(
            r['date'] for r in conn.execute(
                "SELECT date FROM daily_sales WHERE branch_id = ? AND date >= ?",
                (branch_id, cutoff)
            ).fetchall()
        )

        new_reports = 0
        skipped = 0

        for msg_id in msg_ids:
            status, msg_data = mail.fetch(msg_id, "(RFC822)")
            if status != "OK":
                continue

            msg = email.message_from_bytes(msg_data[0][1])

            # Check subject contains gmail_label
            subject = msg.get("Subject", "")
            decoded_subject = str(email.header.make_header(email.header.decode_header(subject)))
            if gmail_label not in decoded_subject:
                continue

            # Parse email date
            date_str_raw = msg.get("Date")
            if not date_str_raw:
                continue
            try:
                dt = email.utils.parsedate_to_datetime(date_str_raw)
                email_date = dt.astimezone(ZoneInfo("Asia/Jerusalem")).date()
            except Exception:
                continue

            date_str = email_date.isoformat()

            # Skip if already in DB (but allow overwriting provisionals)
            if date_str in existing:
                prov_row = conn.execute(
                    "SELECT source FROM daily_sales WHERE branch_id=? AND date=?",
                    (branch_id, date_str)
                ).fetchone()
                if prov_row and prov_row['source'] != 'live_provisional':
                    skipped += 1
                    continue
                # Provisional exists — continue to overwrite with real Z-report

            # Extract PDF
            pdf_bytes = _extract_z_pdf(msg)
            if pdf_bytes is None:
                log.warning("No Z PDF found for %s", date_str)
                continue

            # Parse total and transactions from PDF
            total, transactions = _extract_total_from_pdf(pdf_bytes)
            if total is None:
                log.warning("Could not parse total from PDF for %s", date_str)
                continue

            # Save PDF to disk
            pdf_filename = f"z_{date_str}.pdf"
            pdf_path = pdf_dir / pdf_filename
            with open(pdf_path, 'wb') as f:
                f.write(pdf_bytes)

            # Check if a provisional existed for this date
            provisional = conn.execute(
                "SELECT amount FROM daily_sales "
                "WHERE branch_id=? AND date=? AND source='live_provisional'",
                (branch_id, date_str)
            ).fetchone()

            if provisional:
                diff = abs(total - provisional['amount'])
                pct = (diff / total * 100) if total else 0
                log.info(
                    "Branch %d date %s: Z=₪%.2f, Provisional=₪%.2f, diff=₪%.2f (%.1f%%)",
                    branch_id, date_str, total, provisional['amount'], diff, pct
                )
                if diff > 500:
                    log.warning(
                        "LARGE DIFF branch %d %s: Z vs provisional diff ₪%.2f",
                        branch_id, date_str, diff
                    )
                # Delete provisional before inserting real Z-report
                conn.execute(
                    "DELETE FROM daily_sales WHERE branch_id=? AND date=? AND source='live_provisional'",
                    (branch_id, date_str)
                )

            # Insert real Z-report
            conn.execute(
                "INSERT OR IGNORE INTO daily_sales (branch_id, date, amount, transactions, source) "
                "VALUES (?, ?, ?, ?, 'z_report')",
                (branch_id, date_str, total, transactions)
            )
            conn.commit()
            existing.add(date_str)
            new_reports += 1
            log.info("Saved Z-report for %s: %.2f (%d transactions)", date_str, total, transactions)

        conn.close()

        # ── Attendance CSV parsing ──────────────────────────────
        attendance_msg = None
        try:
            attendance_msg = _sync_attendance_csv(mail, branch, branch_id, log)
        except Exception as e:
            log.error("Attendance CSV sync failed: %s", e, exc_info=True)

        mail.logout()

        duration = time.time() - t0
        status = 'success'
        if new_reports > 0:
            message = f"{new_reports} דוחות חדשים, {skipped} דילוגים"
        elif skipped > 0:
            message = f"הכל מעודכן ({skipped} קיימים)"
        else:
            status = 'warning'
            message = "אין Z-report"
            notify(f"⚠️ Gmail — {branch.get('name', f'Branch {branch_id}')}",
                   "No Z-reports found in the last 7 days.")

        conn_fin = _get_db()
        conn_fin.execute(
            "UPDATE agent_runs SET finished_at=datetime('now'), status=?, docs_count=?, message=?, duration_seconds=? WHERE id=?",
            (status, new_reports, message, round(duration, 1), run_id)
        )
        conn_fin.commit()
        conn_fin.close()

        log.info("Gmail sync complete: %d new, %d skipped", new_reports, skipped)
        return {'success': True, 'new_reports': new_reports, 'skipped': skipped}

    except Exception as e:
        log.error("Gmail sync failed: %s", e, exc_info=True)
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
        notify(f"❌ Gmail — {branch.get('name', f'Branch {branch_id}')}", _friendly_gmail_error(e))
        return {'success': False, 'new_reports': 0, 'skipped': 0, 'error': str(e)}


if __name__ == '__main__':
    import sys
    bid = int(sys.argv[1]) if len(sys.argv) > 1 else 126
    print(run_gmail_sync(bid))

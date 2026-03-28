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
    conn = sqlite3.connect(DB_PATH)
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
    """
    employees = []
    current_name = None
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
                    'total_hours': round(current_hours, 2),
                })
            current_name = None
            current_hours = 0.0
            continue

        # New employee row (starts with digit = employee ID)
        if first_col and first_col[0].isdigit():
            # Extract name: "382 רועי אמסלם" -> "רועי אמסלם"
            parts = first_col.split(None, 1)
            if len(parts) >= 2:
                current_name = parts[1].strip()
            continue

    return employees


def _sync_attendance_csv(mail, branch: dict, branch_id: int, log) -> str | None:
    """Search for attendance CSV emails, parse and save employee hours."""
    gmail_label = branch.get('gmail_label') or ''
    if not gmail_label:
        return None

    # Determine current month
    now_il = datetime.now(ZoneInfo('Asia/Jerusalem'))
    current_month = now_il.strftime('%Y-%m')

    # Check if already processed this month
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    count = conn.execute(
        "SELECT COUNT(*) as cnt FROM employee_hours WHERE branch_id=? AND month=?",
        (branch_id, current_month)
    ).fetchone()['cnt']
    if count > 0:
        log.info("Attendance CSV already processed for %s (%d employees)", current_month, count)
        conn.close()
        return "already processed"

    # Search for attendance emails in last 35 days
    since_str = (date.today() - timedelta(days=35)).strftime("%d-%b-%Y")
    criteria = f'(SUBJECT "נוכחות באקסל" SINCE "{since_str}")'
    status, data = mail.search(None, criteria)

    if status != "OK" or not data or not data[0]:
        # Try broader search
        criteria = f'(SINCE "{since_str}")'
        status, data = mail.search(None, criteria)
        if status != "OK" or not data or not data[0]:
            log.info("No attendance CSV emails found")
            conn.close()
            return None

    msg_ids = data[0].split()
    log.info("Scanning %d emails for attendance CSV", len(msg_ids))

    csv_content = None
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

    # Parse CSV
    employees = _parse_attendance_csv(csv_content)
    if not employees:
        log.warning("Could not parse any employees from attendance CSV")
        conn.close()
        return None

    # Calculate totals
    total_hours_all = sum(e['total_hours'] for e in employees)

    # We don't have salary in the CSV, so store hours only (salary=0)
    # avg_rate will be calculated when salary data is available
    for emp in employees:
        conn.execute(
            "INSERT OR REPLACE INTO employee_hours "
            "(branch_id, month, employee_name, total_hours, total_salary, source) "
            "VALUES (?, ?, ?, ?, 0, 'csv')",
            (branch_id, current_month, emp['name'], emp['total_hours'])
        )

    # Update branch hours
    conn.execute(
        "UPDATE branches SET hours_this_month=?, hours_updated_at=? WHERE id=?",
        (total_hours_all, now_il.isoformat(), branch_id)
    )
    conn.commit()
    conn.close()

    msg = f"📊 נוכחות: {len(employees)} עובדים, {total_hours_all:.1f} שעות"
    log.info(msg)
    return msg


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
            notify("⚠️ אין Z-report", f"סניף {branch_id} — לא נמצאו דוחות ב-7 ימים אחרונים")

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
        notify("❌ Gmail נכשל", f"סניף {branch_id} — {e}")
        return {'success': False, 'new_reports': 0, 'skipped': 0, 'error': str(e)}


if __name__ == '__main__':
    import sys
    bid = int(sys.argv[1]) if len(sys.argv) > 1 else 126
    print(run_gmail_sync(bid))

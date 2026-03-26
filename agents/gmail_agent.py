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
from datetime import date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993

# Gmail credentials (same as MakoletDashboard)
GMAIL_ADDRESS = os.environ.get('GMAIL_ADDRESS', 'makoletdeshboard@gmail.com')
GMAIL_APP_PASSWORD = os.environ.get('GMAIL_APP_PASSWORD', 'tulgwyjhilhjxfwi')
AVIV_SENDER_EMAIL = os.environ.get('AVIV_SENDER_EMAIL', 'avivpost@avivpos.co.il')

# RTL PDF: "20295.85 ₪ :כ"הס"
TOTAL_PATTERN_RTL = re.compile(r'([\d,]+\.?\d*)\s*₪\s*:כ"הס')
# LTR: סה"כ: ₪ 12377.92
TOTAL_PATTERN_LTR = re.compile(r'סה["\u05f4]כ[:\s]+₪?\s*([\d,]+\.?\d*)')


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


def _extract_total_from_pdf(pdf_bytes: bytes) -> float | None:
    import io
    # Try pdfplumber first (matches MakoletDashboard's working parser)
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                match = TOTAL_PATTERN_RTL.search(text)
                if match:
                    return float(match.group(1).replace(",", ""))
                match = TOTAL_PATTERN_LTR.search(text)
                if match:
                    return float(match.group(1).replace(",", ""))
    except ImportError:
        pass
    # Fallback to PyMuPDF
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        for page in doc:
            text = page.get_text()
            match = TOTAL_PATTERN_RTL.search(text)
            if match:
                doc.close()
                return float(match.group(1).replace(",", ""))
            match = TOTAL_PATTERN_LTR.search(text)
            if match:
                doc.close()
                return float(match.group(1).replace(",", ""))
        doc.close()
    except ImportError:
        pass
    return None


def run_gmail_sync(branch_id: int) -> dict:
    """
    Search Gmail for Z-report emails matching branch.gmail_label,
    parse PDFs, save to daily_sales.
    Returns {success, new_reports, skipped}.
    """
    log = _setup_logger(branch_id)
    log.info("Starting Gmail sync for branch %d", branch_id)

    try:
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

            # Skip if already in DB
            if date_str in existing:
                skipped += 1
                continue

            # Extract PDF
            pdf_bytes = _extract_z_pdf(msg)
            if pdf_bytes is None:
                log.warning("No Z PDF found for %s", date_str)
                continue

            # Parse total from PDF
            total = _extract_total_from_pdf(pdf_bytes)
            if total is None:
                log.warning("Could not parse total from PDF for %s", date_str)
                continue

            # Save PDF to disk
            pdf_filename = f"z_{date_str}.pdf"
            pdf_path = pdf_dir / pdf_filename
            with open(pdf_path, 'wb') as f:
                f.write(pdf_bytes)

            # Insert into daily_sales
            conn.execute(
                "INSERT OR IGNORE INTO daily_sales (branch_id, date, amount, transactions, source) "
                "VALUES (?, ?, ?, 0, 'z_report')",
                (branch_id, date_str, total)
            )
            conn.commit()
            existing.add(date_str)
            new_reports += 1
            log.info("Saved Z-report for %s: %.2f", date_str, total)

        conn.close()
        mail.logout()

        log.info("Gmail sync complete: %d new, %d skipped", new_reports, skipped)
        return {'success': True, 'new_reports': new_reports, 'skipped': skipped}

    except Exception as e:
        log.error("Gmail sync failed: %s", e, exc_info=True)
        return {'success': False, 'new_reports': 0, 'skipped': 0, 'error': str(e)}


if __name__ == '__main__':
    import sys
    bid = int(sys.argv[1]) if len(sys.argv) > 1 else 126
    print(run_gmail_sync(bid))

"""
Aviv POS live scraper (branch-aware) — uses Playwright to scrape bi-aviv.web.app/status.

Reads credentials from branches table. Saves to live_sales with branch_id.
Store hours: 06:30–23:05 Israel time (zoneinfo, NOT pytz).
"""

import logging
import os
import re
import sqlite3
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
STATUS_URL = "https://bi-aviv.web.app/status"
IL_TZ = ZoneInfo('Asia/Jerusalem')


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
    logger = logging.getLogger(f'aviv_live_{branch_id}')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        log_dir = Path(__file__).parent.parent / 'logs'
        log_dir.mkdir(exist_ok=True)
        fh = logging.FileHandler(log_dir / f'aviv_live_{branch_id}.log', encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(sh)
    return logger


def _is_store_hours() -> bool:
    now = datetime.now(IL_TZ)
    start = now.replace(hour=6, minute=30, second=0, microsecond=0)
    end = now.replace(hour=23, minute=5, second=0, microsecond=0)
    return start <= now <= end


def _scrape(branch: dict, log: logging.Logger) -> dict:
    from playwright.sync_api import sync_playwright

    user_id = branch.get('aviv_user_id') or ''
    password = branch.get('aviv_password') or user_id  # default: same as user_id

    log.info("Starting Playwright scrape for user %s", user_id)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context().new_page()

        page.goto(STATUS_URL, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(2000)

        # Login if redirected to sign-in
        if "sign-in" in page.url:
            log.info("On login page — filling credentials for %s", user_id)
            inputs = page.locator("input")
            if inputs.count() >= 2:
                inputs.nth(0).fill(user_id)
                inputs.nth(1).fill(password)
            checkbox = page.locator("input[type='checkbox']")
            if checkbox.count() > 0:
                checkbox.first.check()
            login_btn = page.locator("button", has_text="התחברות")
            if login_btn.count() == 0:
                login_btn = page.locator("button[type='submit']")
            login_btn.first.click()
            log.info("Clicked login, waiting...")
            page.wait_for_timeout(5000)

        # Navigate to /status
        if "status" not in page.url:
            status_link = page.locator("a[href*='status']")
            if status_link.count() == 0:
                status_link = page.locator("text=Online")
            if status_link.count() > 0:
                status_link.first.click()
                page.wait_for_timeout(3000)
            else:
                page.goto(STATUS_URL, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(3000)

        # Wait for content
        try:
            page.wait_for_selector("text=תאריך עדכון אחרון", timeout=15000)
        except Exception:
            page.wait_for_selector("text=₪", timeout=10000)
        page.wait_for_timeout(2000)

        raw_text = page.inner_text("body")
        log.info("Page text (%d chars): %s", len(raw_text), raw_text[:400])

        amount = 0.0
        transactions = 0
        last_updated = ""

        if "תאריך עדכון אחרון" in raw_text:
            ts_match = re.search(r"תאריך עדכון אחרון\s*\n\s*(\d{1,2}:\d{2}\s+\d{2}/\d{2}/\d{2})", raw_text)
            if ts_match:
                last_updated = ts_match.group(1).strip()
            amt_match = re.search(r"תאריך עדכון אחרון.*?₪\s?([\d,]+(?:\.\d+)?)", raw_text, re.DOTALL)
            if amt_match:
                amount = float(amt_match.group(1).replace(",", ""))
            tx_match = re.search(r"תאריך עדכון אחרון.*?₪[\d,]+(?:\.\d+)?\s*\n\s*\((\d+)\)", raw_text, re.DOTALL)
            if tx_match:
                transactions = int(tx_match.group(1))
        else:
            amt_match = re.search(r"מכירות\s*\n.*?\n\s*₪\s?([\d,]+(?:\.\d+)?)", raw_text, re.DOTALL)
            if amt_match:
                amount = float(amt_match.group(1).replace(",", ""))
            tx_match = re.search(r"(\d+)\s*\n\s*\d+\s*\n\s*₪[\d,]+\s*\n\s*עסקאות", raw_text)
            if tx_match:
                transactions = int(tx_match.group(1))
            last_updated = datetime.now(IL_TZ).strftime("%H:%M %d/%m/%y")

        log.info("Scraped: amount=₪%.2f, tx=%d, last_updated=%s", amount, transactions, last_updated)
        browser.close()

    return {
        'date': date.today().isoformat(),
        'amount': amount,
        'transactions': transactions,
        'last_updated': last_updated,
        'fetched_at': datetime.now(IL_TZ).isoformat(),
    }


def run_aviv_live(branch_id: int) -> dict:
    """
    Scrape Aviv POS live sales for a branch.
    Returns {success, amount, transactions}.
    """
    log = _setup_logger(branch_id)

    if not _is_store_hours():
        log.info("Outside store hours, skipping")
        return {'success': True, 'amount': 0, 'transactions': 0}

    try:
        branch = _get_branch_config(branch_id)
        if not branch.get('aviv_user_id'):
            log.warning("No aviv_user_id for branch %d", branch_id)
            return {'success': False, 'amount': 0, 'transactions': 0, 'error': 'no credentials'}

        data = _scrape(branch, log)

        # Save to DB
        conn = _get_db()
        conn.execute(
            "INSERT OR REPLACE INTO live_sales (branch_id, date, amount, transactions, last_updated, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (branch_id, data['date'], data['amount'], data['transactions'],
             data['last_updated'], data['fetched_at'])
        )
        conn.commit()
        conn.close()

        log.info("Saved: ₪%.2f (%d tx)", data['amount'], data['transactions'])
        return {'success': True, 'amount': data['amount'], 'transactions': data['transactions']}

    except Exception as e:
        log.error("Aviv live scrape failed: %s", e, exc_info=True)
        return {'success': False, 'amount': 0, 'transactions': 0, 'error': str(e)}


if __name__ == '__main__':
    import sys
    bid = int(sys.argv[1]) if len(sys.argv) > 1 else 126
    print(run_aviv_live(bid))

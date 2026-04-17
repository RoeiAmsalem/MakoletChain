"""
Aviv POS live scraper (branch-aware).

Primary: REST API on bi1.aviv-pos.co.il (fast, no browser).
Fallback: Playwright scrape of bi-aviv.web.app/status if REST fails.

Reads credentials from branches table. Saves to live_sales with branch_id.
Day-aware store hours (zoneinfo, NOT pytz):
  Sun–Thu 06:30–23:30, Fri 06:30–19:00, Sat 16:30–23:30
"""

import logging
import os
import re
import sqlite3
import time
import urllib3
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from utils.notify import notify

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def _friendly_error(e: Exception) -> str:
    msg = str(e)
    if 'Quota' in msg or 'quota' in msg or 'Bandwidth' in msg:
        return "Aviv BI is down — bandwidth quota exceeded on their end."
    if 'wait_for_selector' in msg and 'Timeout' in msg:
        return "Aviv BI page loaded but data took too long to appear."
    if 'goto' in msg and 'Timeout' in msg:
        return "Aviv BI website failed to load — could be down or unreachable."
    if 'net::ERR' in msg:
        code = msg.split('net::ERR_')[1].split()[0] if 'net::ERR_' in msg else 'unknown'
        return f"Network error connecting to Aviv BI ({code})."
    if '401' in msg:
        return "Aviv BI login failed — credentials may have changed."
    if '500' in msg:
        return "Aviv BI server error — their server is having issues."
    if 'sign-in' in msg.lower():
        return "Aviv BI login page issue — could not authenticate."
    # Strip any Playwright call log noise
    clean = msg.split('Call log:')[0].strip()
    clean = clean.replace('playwright._impl._errors.TimeoutError: ', '')
    clean = clean.replace('Page.goto: ', '')
    clean = clean.replace('Page.wait_for_selector: ', '')
    return clean[:120] if clean else "Unknown Aviv BI error."


DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
STATUS_URL = "https://bi-aviv.web.app/status"
API_BASE = "https://bi1.aviv-pos.co.il:8443/avivbi/v2"
API_PLAIN = "https://bi1.aviv-pos.co.il:65010"
API_TIMEOUT = 15
IL_TZ = ZoneInfo('Asia/Jerusalem')


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


STORE_SCHEDULE = {
    0: (6, 30, 23, 30),   # Monday
    1: (6, 30, 23, 30),   # Tuesday
    2: (6, 30, 23, 30),   # Wednesday
    3: (6, 30, 23, 30),   # Thursday
    4: (6, 30, 19, 0),    # Friday — closes early
    5: (16, 30, 23, 30),  # Saturday — opens late
    6: (6, 30, 23, 30),   # Sunday
}


def _is_store_hours() -> bool:
    now = datetime.now(IL_TZ)
    sh, sm, eh, em = STORE_SCHEDULE[now.weekday()]
    start = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end = now.replace(hour=eh, minute=em, second=0, microsecond=0)
    return start <= now <= end


def get_next_opening() -> str:
    """Return next store opening time as HH:MM string."""
    now = datetime.now(IL_TZ)
    sh, sm, eh, em = STORE_SCHEDULE[now.weekday()]
    open_today = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
    # If before today's opening, return today's opening
    if now < open_today:
        return f"{sh:02d}:{sm:02d}"
    # Otherwise return tomorrow's opening
    tomorrow_wd = (now.weekday() + 1) % 7
    tsh, tsm, _, _ = STORE_SCHEDULE[tomorrow_wd]
    return f"{tsh:02d}:{tsm:02d}"


# Total hours this month (authoritative — used at 23:30)
_monthly_hours_pattern = re.compile(
    r'([\d,]+\.?\d*)\s*שעות עובדים מתחילת החודש'
    r'|שעות עובדים מתחילת החודש[^\d]*([\d,]+\.?\d*)'
)

# Current shift hours (used at 16:00)
_shift_hours_pattern = re.compile(
    r'([\d,]+\.?\d*)\s*שעות עובדים במשמרת'
    r'|שעות עובדים במשמרת[^\d]*([\d,]+\.?\d*)'
)


def _parse_hours(pattern, text):
    m = pattern.search(text)
    if not m:
        return 0.0
    val = m.group(1) or m.group(2) or '0'
    return float(val.replace(',', ''))


def _fmt_last_updated(tm: str) -> str:
    """Convert API 'YYYY-MM-DD HH:MM:SS' to 'HH:MM dd/mm/yy'."""
    if not tm:
        return datetime.now(IL_TZ).strftime("%H:%M %d/%m/%y")
    try:
        dt = datetime.strptime(tm, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%H:%M %d/%m/%y")
    except Exception:
        return tm


def _scrape_api(branch: dict, log: logging.Logger) -> dict:
    """REST API path: login → status. No browser."""
    user_id = branch.get('aviv_user_id') or ''
    password = branch.get('aviv_password') or user_id

    log.info("REST API scrape for user %s", user_id)

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'MakoletChain/1.0',
    }

    # 1) Login
    r = requests.post(
        f"{API_BASE}/account/login",
        json={'user': user_id, 'password': password},
        headers=headers,
        timeout=API_TIMEOUT,
        verify=False,
    )
    if r.status_code == 401:
        raise Exception("401 Aviv BI login failed — credentials may have changed")
    r.raise_for_status()
    login_data = r.json()

    token = login_data.get('value')
    branches_list = login_data.get('branches') or []
    if not token or not branches_list:
        raise Exception(f"Login response missing token or branches: {str(login_data)[:200]}")

    api_branch_id = branches_list[0]['id']

    # 2) Status
    r2 = requests.post(
        f"{API_PLAIN}/raw/status/plain",
        json={'branches': [api_branch_id]},
        headers={**headers, 'Authtoken': token},
        timeout=API_TIMEOUT,
        verify=False,
    )
    r2.raise_for_status()
    rows = r2.json()
    if not rows:
        raise Exception("Status response empty")
    row = rows[0]

    amount = float(row.get('dealTotal') or 0)
    transactions = int(row.get('dealCount') or 0)
    last_updated = _fmt_last_updated(row.get('tmUpdate') or '')
    monthly_hours = float(row.get('totalEmployeeHours') or 0)
    shift_hours = float(row.get('currentEmployeeHours') or 0)
    cancellation_total = float(row.get('cancellationTotal') or 0)
    discount_total = float(row.get('discountTotal') or 0)
    running_total = float(row.get('runningDealTotal') or 0)
    running_count = int(row.get('runningDealCount') or 0)

    log.info(
        "REST: amount=₪%.2f, tx=%d, monthly_hours=%.2f, shift_hours=%.2f, last_updated=%s",
        amount, transactions, monthly_hours, shift_hours, last_updated,
    )

    return {
        'date': date.today().isoformat(),
        'amount': amount,
        'transactions': transactions,
        'last_updated': last_updated,
        'fetched_at': datetime.now(IL_TZ).isoformat(),
        'monthly_hours': monthly_hours,
        'shift_hours': shift_hours,
        'cancellation_total': cancellation_total,
        'discount_total': discount_total,
        'running_total': running_total,
        'running_count': running_count,
    }


def _scrape_playwright(branch: dict, log: logging.Logger) -> dict:
    from playwright.sync_api import sync_playwright

    user_id = branch.get('aviv_user_id') or ''
    password = branch.get('aviv_password') or user_id  # default: same as user_id

    log.info("Starting Playwright scrape for user %s", user_id)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context().new_page()

        page.goto(STATUS_URL, wait_until="domcontentloaded", timeout=30000)
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
                page.goto(STATUS_URL, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(3000)

        # Wait for content
        try:
            page.wait_for_selector("text=תאריך עדכון אחרון", timeout=20000)
        except Exception:
            page.wait_for_selector("text=₪", timeout=20000)
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

        # Scrape employee hours — two distinct fields
        monthly_hours = _parse_hours(_monthly_hours_pattern, raw_text)
        shift_hours = _parse_hours(_shift_hours_pattern, raw_text)
        log.info("Scraped: amount=₪%.2f, tx=%d, monthly_hours=%.2f, shift_hours=%.2f, last_updated=%s",
                 amount, transactions, monthly_hours, shift_hours, last_updated)
        browser.close()

    return {
        'date': date.today().isoformat(),
        'amount': amount,
        'transactions': transactions,
        'last_updated': last_updated,
        'fetched_at': datetime.now(IL_TZ).isoformat(),
        'monthly_hours': monthly_hours,
        'shift_hours': shift_hours,
    }


def _scrape(branch: dict, log: logging.Logger) -> dict:
    """Try REST API first; fall back to Playwright if it fails."""
    try:
        return _scrape_api(branch, log)
    except Exception as e:
        log.warning("REST API failed (%s), falling back to Playwright", e)
        return _scrape_playwright(branch, log)


def _save_hourly_snapshot(conn, branch_id: int, data: dict, log: logging.Logger):
    """Save revenue to hourly_sales by computing delta from last snapshot.

    Each scrape gives cumulative dealTotal for the day. We find the previous
    snapshot's cumulative total, compute the delta, and add it to the current hour.
    """
    try:
        today = data['date']
        now = datetime.now(IL_TZ)
        current_hour = now.hour
        amount = data['amount']
        transactions = data['transactions']

        # Get previous cumulative total from live_sales (before we overwrote it)
        prev = conn.execute(
            'SELECT amount, transactions FROM live_sales WHERE branch_id=? AND date=?',
            (branch_id, today)
        ).fetchone()

        if prev and prev['amount']:
            prev_amount = float(prev['amount'])
            prev_tx = int(prev['transactions'] or 0)
        else:
            prev_amount = 0
            prev_tx = 0

        delta_amount = amount - prev_amount
        delta_tx = transactions - prev_tx

        if delta_amount > 0:
            conn.execute(
                '''INSERT INTO hourly_sales (branch_id, date, hour, amount, transactions)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(branch_id, date, hour)
                   DO UPDATE SET amount = amount + ?, transactions = transactions + ?''',
                (branch_id, today, current_hour, delta_amount, delta_tx,
                 delta_amount, delta_tx)
            )
            log.info("Hourly snapshot: hour=%d, delta=₪%.2f (%d tx)", current_hour, delta_amount, delta_tx)
    except Exception as e:
        log.warning("Failed to save hourly snapshot: %s", e)


def handle_zero_detection(branch_id: int, conn, logger: logging.Logger):
    """
    Called when Aviv BI returns amount=0.
    If previous fetch was > 0 → store closed, save provisional Z-report.
    If before 23:30 → pause scraper for 2 hours (early closure).
    """
    now = datetime.now(IL_TZ)
    today = now.date().isoformat()

    # Get last non-zero amount for today
    row = conn.execute(
        'SELECT amount, fetched_at FROM live_sales '
        'WHERE branch_id=? AND date=? AND amount > 0 '
        'ORDER BY fetched_at DESC LIMIT 1',
        (branch_id, today)
    ).fetchone()

    if not row:
        return  # Never had data today, nothing to save

    last_amount = row['amount']

    # Save provisional Z-report to daily_sales (only if no real Z-report exists)
    existing = conn.execute(
        'SELECT source FROM daily_sales WHERE branch_id=? AND date=?',
        (branch_id, today)
    ).fetchone()

    if not existing:
        conn.execute(
            'INSERT OR REPLACE INTO daily_sales '
            '(branch_id, date, amount, transactions, source) '
            'VALUES (?, ?, ?, 0, ?)',
            (branch_id, today, last_amount, 'live_provisional')
        )
        conn.commit()
        logger.info(
            "Branch %d: Zero detected, saved provisional ₪%.2f for %s",
            branch_id, last_amount, today
        )
    elif existing['source'] == 'live_provisional':
        # Update provisional with latest best amount
        conn.execute(
            'UPDATE daily_sales SET amount=? WHERE branch_id=? AND date=?',
            (last_amount, branch_id, today)
        )
        conn.commit()

    # Early closure detection: if before 23:30 → pause 2 hours
    end_normal = now.replace(hour=23, minute=30, second=0, microsecond=0)
    if now < end_normal:
        conn.execute(
            'INSERT OR REPLACE INTO live_sales '
            '(branch_id, date, amount, transactions, last_updated, fetched_at) '
            'VALUES (?, ?, 0, 0, ?, ?)',
            (branch_id, today, 'PAUSED', datetime.now(IL_TZ).isoformat())
        )
        conn.commit()
        logger.info(
            "Branch %d: Early closure detected at %s, pausing 2 hours",
            branch_id, now.strftime('%H:%M')
        )


def run_aviv_live(branch_id: int) -> dict:
    """
    Scrape Aviv POS live sales for a branch.
    Returns {success, amount, transactions}.
    """
    log = _setup_logger(branch_id)
    t0 = time.time()

    if not _is_store_hours():
        log.info("Outside store hours, skipping")
        return {'success': True, 'amount': 0, 'transactions': 0, 'skipped': 'outside_hours'}

    # Check credentials BEFORE creating agent_runs record
    branch = _get_branch_config(branch_id)
    if not branch.get('aviv_user_id'):
        log.info("No aviv_user_id for branch %d, skipping", branch_id)
        return {'success': True, 'skipped': 'no_credentials'}

    # Insert agent_runs start
    conn_run = _get_db()
    cur = conn_run.execute(
        "INSERT INTO agent_runs (branch_id, agent, started_at, status) VALUES (?, 'aviv_live', datetime('now'), 'running')",
        (branch_id,)
    )
    run_id = cur.lastrowid
    conn_run.commit()
    conn_run.close()

    try:

        # Check if paused due to early closure
        conn = _get_db()
        today = date.today().isoformat()
        pause_row = conn.execute(
            'SELECT last_updated, fetched_at FROM live_sales '
            'WHERE branch_id=? AND date=? ORDER BY fetched_at DESC LIMIT 1',
            (branch_id, today)
        ).fetchone()

        if pause_row and pause_row['last_updated'] == 'PAUSED':
            paused_at = datetime.fromisoformat(pause_row['fetched_at'])
            if datetime.now(IL_TZ) - paused_at < timedelta(hours=2):
                log.info("Branch %d: Scraper paused (early closure), skipping", branch_id)
                conn.close()
                return {'success': True, 'amount': 0, 'transactions': 0, 'skipped': 'paused'}
            # 2 hours passed → resume normally

        data = _scrape(branch, log)

        # Zero detection: if amount is 0 and we previously had data
        if data['amount'] == 0:
            handle_zero_detection(branch_id, conn, log)
            conn.close()
            return {'success': True, 'amount': 0, 'transactions': 0, 'zero_detected': True}

        # Save hourly snapshot BEFORE overwriting live_sales (needs previous cumulative total)
        _save_hourly_snapshot(conn, branch_id, data, log)

        # Save to DB
        conn.execute(
            "INSERT OR REPLACE INTO live_sales "
            "(branch_id, date, amount, transactions, last_updated, fetched_at, "
            "cancellation_total, discount_total, running_total, running_count) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (branch_id, data['date'], data['amount'], data['transactions'],
             data['last_updated'], data['fetched_at'],
             data.get('cancellation_total', 0), data.get('discount_total', 0),
             data.get('running_total', 0), data.get('running_count', 0))
        )

        # Save employee hours to branches table
        if data.get('monthly_hours', 0) > 0:
            conn.execute(
                '''UPDATE branches SET
                    hours_this_month = ?,
                    hours_updated_at = ?
                    WHERE id = ?''',
                (data['monthly_hours'], datetime.now(IL_TZ).isoformat(), branch_id)
            )

        conn.commit()
        conn.close()

        duration = time.time() - t0
        status = 'success'
        message = f"₪{data['amount']:,.0f} ({data['transactions']} tx)"
        if data['amount'] == 0 and _is_store_hours():
            status = 'warning'
            message = "סכום 0 בשעות פעילות"
            notify(f"⚠️ Aviv Live — {branch.get('name', f'Branch {branch_id}')}", "Revenue is showing ₪0 during store hours.")

        conn_fin = _get_db()
        conn_fin.execute(
            "UPDATE agent_runs SET finished_at=datetime('now'), status=?, amount=?, message=?, duration_seconds=? WHERE id=?",
            (status, data['amount'], message, round(duration, 1), run_id)
        )
        conn_fin.commit()
        conn_fin.close()

        log.info("Saved: ₪%.2f (%d tx)", data['amount'], data['transactions'])
        return {'success': True, 'amount': data['amount'], 'transactions': data['transactions']}

    except Exception as e:
        log.error("Aviv live scrape failed: %s", e, exc_info=True)
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
        notify(f"❌ Aviv Live — {branch.get('name', f'Branch {branch_id}')}", _friendly_error(e))
        return {'success': False, 'amount': 0, 'transactions': 0, 'error': str(e)}


def scrape_hours_end_of_day(branch_id: int) -> dict:
    """23:30 job — scrape authoritative monthly total.
    Replaces hours_this_month AND updates hours_baseline for tomorrow.
    Uses: שעות עובדים מתחילת החודש"""
    log = _setup_logger(branch_id)
    branch = _get_branch_config(branch_id)

    if not branch.get('aviv_user_id'):
        return {'success': False, 'error': 'no credentials'}

    try:
        result = _scrape(branch, log)
        monthly_hours = result.get('monthly_hours', 0)

        conn = _get_db()
        conn.execute('''UPDATE branches SET
            hours_this_month = ?,
            hours_baseline = ?,
            hours_updated_at = ?
            WHERE id = ?''',
            (monthly_hours, monthly_hours,
             datetime.now(IL_TZ).isoformat(), branch_id))
        conn.commit()
        conn.close()

        log.info("End-of-day hours: %.1f (baseline set)", monthly_hours)
        return {'success': True, 'hours': monthly_hours, 'type': 'end_of_day'}
    except Exception as e:
        log.error("End-of-day hours scrape failed: %s", e)
        return {'success': False, 'error': str(e)}


def scrape_hours_midday(branch_id: int) -> dict:
    """16:00 job — add today's current shift to last night's baseline.
    Uses: hours_baseline + שעות עובדים במשמרת
    This gives a live estimate mid-day without waiting for 23:30."""
    log = _setup_logger(branch_id)
    branch = _get_branch_config(branch_id)

    if not branch.get('aviv_user_id'):
        return {'success': False, 'error': 'no credentials'}

    try:
        result = _scrape(branch, log)
        shift_hours = result.get('shift_hours', 0)

        conn = _get_db()
        row = conn.execute(
            'SELECT hours_baseline FROM branches WHERE id=?', (branch_id,)
        ).fetchone()
        baseline = row['hours_baseline'] if row and row['hours_baseline'] else 0

        estimated_total = round(baseline + shift_hours, 2)

        conn.execute('''UPDATE branches SET
            hours_this_month = ?,
            hours_updated_at = ?
            WHERE id = ?''',
            (estimated_total, datetime.now(IL_TZ).isoformat(), branch_id))
        conn.commit()
        conn.close()

        log.info("Midday hours: baseline=%.1f + shift=%.1f = %.2f", baseline, shift_hours, estimated_total)
        return {
            'success': True,
            'baseline': baseline,
            'shift_hours': shift_hours,
            'total': estimated_total,
            'type': 'midday'
        }
    except Exception as e:
        log.error("Midday hours scrape failed: %s", e)
        return {'success': False, 'error': str(e)}


if __name__ == '__main__':
    import sys
    bid = int(sys.argv[1]) if len(sys.argv) > 1 else 126
    print(run_aviv_live(bid))

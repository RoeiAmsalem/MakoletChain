"""Backfill March 2026 daily sales for branch 127 from Aviv BI dashboard.

Uses the rdr date picker: set date input values via JS, click Save (לשמור),
then extract sales amount and transactions from the dashboard.
"""
from playwright.sync_api import sync_playwright
import time, re, sqlite3

DB_PATH = '/opt/makolet-chain/db/makolet_chain.db'
BRANCH_ID = 127
USER = 'Tichon123'
PASSWORD = 'Tichon123'


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def login(page):
    page.goto('https://bi-aviv.web.app/sign-in', wait_until='networkidle', timeout=30000)
    page.wait_for_timeout(2000)
    page.locator('input').nth(0).fill(USER)
    page.locator('input').nth(1).fill(PASSWORD)
    checkbox = page.locator("input[type='checkbox']")
    if checkbox.count() > 0:
        checkbox.first.check()
    page.locator('button', has_text='\u05d4\u05ea\u05d7\u05d1\u05e8\u05d5\u05ea').first.click()
    page.wait_for_timeout(5000)


def get_current_date(page):
    body = page.inner_text('body')
    m = re.search(r'(\d{4}-\d{2}-\d{2})', body)
    return m.group(1) if m else None


def open_picker(page):
    """Open the date picker by clicking the date <P> element."""
    pos = page.evaluate('''() => {
        const ps = document.querySelectorAll('p');
        for (const p of ps) {
            if (/^\\d{4}-\\d{2}-\\d{2}$/.test(p.textContent.trim())) {
                const rect = p.getBoundingClientRect();
                if (rect.width > 0)
                    return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2 };
            }
        }
        return null;
    }''')
    if not pos:
        return False
    page.mouse.click(pos['x'], pos['y'])
    page.wait_for_timeout(2000)
    count = page.evaluate('() => document.querySelectorAll(".rdrDay").length')
    return count > 0


def set_date_and_save(page, date_str):
    """Set date in rdr picker inputs and click Save.
    date_str format: DD/MM/YYYY
    Returns True if date changed successfully."""
    # Set both start and end date inputs to the same value
    page.evaluate('''(dateStr) => {
        const inputs = document.querySelectorAll('.rdrDateInput input');
        const nativeSetter = Object.getOwnPropertyDescriptor(
            HTMLInputElement.prototype, 'value').set;
        // inputs[0] and [1] are start/end of the main date range
        nativeSetter.call(inputs[0], dateStr);
        inputs[0].dispatchEvent(new Event('input', { bubbles: true }));
        inputs[0].dispatchEvent(new Event('change', { bubbles: true }));
        inputs[0].dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));

        nativeSetter.call(inputs[1], dateStr);
        inputs[1].dispatchEvent(new Event('input', { bubbles: true }));
        inputs[1].dispatchEvent(new Event('change', { bubbles: true }));
        inputs[1].dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
    }''', date_str)
    page.wait_for_timeout(500)

    # Click Save
    page.locator('button', has_text='\u05dc\u05e9\u05de\u05d5\u05e8').click()
    page.wait_for_timeout(3000)


def extract_sales(body):
    """Extract sales amount and transactions from dashboard body text."""
    amt_match = re.search(
        r'\u05de\u05db\u05d9\u05e8\u05d5\u05ea\s*\n[^\n]*\n\u20aa([\d,]+)', body)
    if not amt_match:
        amt_match = re.search(r'\u20aa([\d,]+)', body)
    amount = float(amt_match.group(1).replace(',', '')) if amt_match else 0.0

    tx_match = re.search(r'(\d+)\s+\u05e2\u05e1\u05e7\u05d0\u05d5\u05ea', body)
    transactions = int(tx_match.group(1)) if tx_match else 0

    return amount, transactions


# ── Main ──
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(viewport={'width': 1920, 'height': 1080})
    page = context.new_page()
    login(page)

    # Clean up previous backfill
    conn = get_db()
    conn.execute(
        "DELETE FROM daily_sales WHERE branch_id=? AND source='aviv_backfill'",
        (BRANCH_ID,))
    conn.commit()
    conn.close()
    print('Cleaned up previous backfill')

    today_date = get_current_date(page)
    print(f'Dashboard date: {today_date}')

    results = {}

    # Scrape days 1-28 (skip 29 = today, still in progress)
    for day in range(1, 29):
        date_str_picker = f'{day:02d}/03/2026'
        expected_iso = f'2026-03-{day:02d}'

        # Open picker
        if not open_picker(page):
            print(f'Day {day}: could not open picker, retrying...')
            page.wait_for_timeout(2000)
            if not open_picker(page):
                print(f'Day {day}: SKIPPED (picker failed)')
                continue

        # Set date and save
        set_date_and_save(page, date_str_picker)

        # Verify
        shown = get_current_date(page)
        if shown != expected_iso:
            print(f'Day {day}: SKIP (shown={shown}, expected={expected_iso})')
            continue

        body = page.inner_text('body')
        amount, tx = extract_sales(body)
        results[day] = (amount, tx)
        print(f'{expected_iso}: {amount:>10,.0f}  tx={tx}')

    browser.close()

    # Insert into DB
    print(f'\n=== INSERTING {len(results)} days ===')
    conn = get_db()
    inserted = 0
    for day_num in sorted(results.keys()):
        amount, tx = results[day_num]
        day_str = f'2026-03-{day_num:02d}'
        existing = conn.execute(
            'SELECT source FROM daily_sales WHERE branch_id=? AND date=?',
            (BRANCH_ID, day_str)).fetchone()
        if existing and existing['source'] == 'z_report':
            print(f'  {day_str}: SKIP (z_report)')
            continue
        conn.execute(
            'INSERT OR REPLACE INTO daily_sales '
            '(branch_id, date, amount, transactions, source) '
            'VALUES (?, ?, ?, ?, ?)',
            (BRANCH_ID, day_str, amount, tx, 'aviv_backfill'))
        inserted += 1
    conn.commit()
    conn.close()
    print(f'Inserted: {inserted}')

    # Verify
    conn = get_db()
    rows = conn.execute(
        'SELECT date, amount, transactions, source '
        'FROM daily_sales WHERE branch_id=? AND date LIKE ? ORDER BY date',
        (BRANCH_ID, '2026-03%')).fetchall()
    print(f'\n=== daily_sales branch {BRANCH_ID}, March 2026 ===')
    total = 0
    for r in rows:
        print(f'  {r["date"]}: {r["amount"]:>10,.0f}  tx={r["transactions"]:>4}  src={r["source"]}')
        total += r['amount']
    print(f'TOTAL: {len(rows)} days, {total:,.0f}')

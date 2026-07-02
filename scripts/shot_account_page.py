"""Screenshot /account on STAGING for the two billing states + print the
rendered SUMIT pay link for the active manager. Read-only against the site.
Run ON the staging server (targets the local Flask port, behind the basic
auth): venv/bin/python scripts/shot_account_page.py"""
import sys

from playwright.sync_api import sync_playwright

BASE = 'http://127.0.0.1:8081'
CASES = [
    ('dennis-test@makoletchain.com', 'Dennis2026!', '/tmp/account_active.png'),
    ('inactive-test@makoletchain.com', 'Inactive2026!', '/tmp/account_inactive.png'),
]

with sync_playwright() as p:
    browser = p.chromium.launch()
    for email, password, out in CASES:
        ctx = browser.new_context(viewport={'width': 390, 'height': 844},
                                  locale='he-IL')
        page = ctx.new_page()
        page.goto(f'{BASE}/login', wait_until='domcontentloaded')
        page.fill('input[name="email"]', email)
        page.fill('input[name="password"]', password)
        page.click('button[type="submit"]')
        page.wait_for_load_state('domcontentloaded')
        page.goto(f'{BASE}/account', wait_until='domcontentloaded')
        page.wait_for_timeout(1200)
        page.screenshot(path=out, full_page=True)
        pay = page.locator('a.pay-btn')
        link = pay.get_attribute('href') if pay.count() else None
        print(f'{email} → {out}  pay_link={link}')
        ctx.close()
    browser.close()
sys.exit(0)

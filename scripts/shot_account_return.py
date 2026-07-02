"""Screenshot /account with the SUMIT post-payment OG- return params (STAGING,
run on the server). Simulates the redirect landing — display-only params."""
from playwright.sync_api import sync_playwright

BASE = 'http://127.0.0.1:8081'

with sync_playwright() as p:
    browser = p.chromium.launch()
    ctx = browser.new_context(viewport={'width': 390, 'height': 844}, locale='he-IL')
    page = ctx.new_page()
    page.goto(f'{BASE}/login', wait_until='domcontentloaded')
    page.fill('input[name="email"]', 'july-test@makoletchain.com')
    page.fill('input[name="password"]', 'July2026!')
    page.click('button[type="submit"]')
    page.wait_for_load_state('domcontentloaded')
    page.goto(f'{BASE}/account?OG-PaymentID=test&OG-PaymentType=CreditCard'
              f'&OG-DocumentNumber=40002', wait_until='domcontentloaded')
    page.wait_for_timeout(1200)
    page.screenshot(path='/tmp/account_return.png', full_page=True)
    print('→ /tmp/account_return.png')
    browser.close()

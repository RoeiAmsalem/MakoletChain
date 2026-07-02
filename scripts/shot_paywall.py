"""Screenshot the paywall states on STAGING. Run ON the staging server with
BILLING_FAKE_TODAY set in .env (service restarted) to the date under test:

  venv/bin/python scripts/shot_paywall.py warning   # fake today 2026-07-06
  venv/bin/python scripts/shot_paywall.py locked    # fake today 2026-07-12

warning → dennis home page with the countdown banner.
locked  → dennis /sales (must redirect to /account lock card) + the exempt
          inactive-test manager's untouched home page.
Read-only against the site."""
import sys

from playwright.sync_api import sync_playwright

BASE = 'http://127.0.0.1:8081'
DENNIS = ('dennis-test@makoletchain.com', 'Dennis2026!')
EXEMPT = ('inactive-test@makoletchain.com', 'Inactive2026!')
MODE = sys.argv[1] if len(sys.argv) > 1 else 'warning'


def shoot(p, creds, path, out, expect_url=None):
    browser = p.chromium.launch()
    ctx = browser.new_context(viewport={'width': 390, 'height': 844},
                              locale='he-IL')
    page = ctx.new_page()
    page.goto(f'{BASE}/login', wait_until='domcontentloaded')
    page.fill('input[name="email"]', creds[0])
    page.fill('input[name="password"]', creds[1])
    page.click('button[type="submit"]')
    page.wait_for_load_state('domcontentloaded')
    page.goto(f'{BASE}{path}', wait_until='domcontentloaded')
    page.wait_for_timeout(1500)
    final = page.url
    page.screenshot(path=out, full_page=False)
    print(f'{creds[0]} {path} → landed {final} → {out}')
    if expect_url and expect_url not in final:
        print(f'FAIL — expected to land on {expect_url}')
    browser.close()


with sync_playwright() as p:
    if MODE == 'warning':
        shoot(p, DENNIS, '/', '/tmp/paywall_warning.png')
    else:
        shoot(p, DENNIS, '/sales', '/tmp/paywall_locked.png',
              expect_url='/account')
        shoot(p, EXEMPT, '/', '/tmp/paywall_exempt.png')

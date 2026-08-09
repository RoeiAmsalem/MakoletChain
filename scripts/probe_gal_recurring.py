"""READ-ONLY go/no-go probe: is a REAL recurring subscription visible for גל?

Phase-2 cancel gate (docs/billing_cancel_spec.md). Targets ONE known-real
customer — 2126101197 (uid 3, גל; August payment 2230217101) — and dumps the
raw response verbatim. Hard-capped at MAX_CALLS read-only calls.

Allowlist is a single READ endpoint per invocation (argv-selected). There is no
code path here that can cancel, create or update anything in SUMIT.

usage:  python3 scripts/probe_gal_recurring.py recurring
        python3 scripts/probe_gal_recurring.py payment
"""
import json
import os
import sys

import requests
from dotenv import load_dotenv

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(APP_DIR, '.env'))

CUSTOMER_ID = 2126101197          # גל — SUMIT customer card
PAYMENT_ID = 2230217101           # גל — August 2026 payment
MAX_CALLS = 3

_ALLOWED = {
    '/billing/recurring/listforcustomer/',   # READ: standing orders for a customer
    '/billing/payments/list/',               # READ: payments in a date window
}
calls = 0


def post(endpoint, **body):
    """Read-only guarded POST. Prints the exact request, HTTP status and the
    raw response body — no paraphrase, no truncation."""
    global calls
    assert endpoint in _ALLOWED, f'probe refuses non-read endpoint {endpoint}'
    assert calls < MAX_CALLS, f'probe hit the {MAX_CALLS}-call cap'
    creds = {'CompanyID': int(os.environ['SUMIT_ORG_ID']),
             'APIKey': os.environ['SUMIT_API_KEY']}
    calls += 1
    shown = {'Credentials': {'CompanyID': creds['CompanyID'], 'APIKey': '<redacted>'}, **body}
    print(f'\n=== CALL {calls}: POST https://api.sumit.co.il{endpoint}')
    print('request body:', json.dumps(shown, ensure_ascii=False))
    r = requests.post('https://api.sumit.co.il' + endpoint,
                      json={'Credentials': creds, **body}, timeout=30)
    print('HTTP status:', r.status_code)
    print('raw response body:')
    print(r.text)
    return r


mode = sys.argv[1] if len(sys.argv) > 1 else 'recurring'

if mode == 'recurring':
    post('/billing/recurring/listforcustomer/',
         Customer={'ID': CUSTOMER_ID}, IncludeInactive=True)
elif mode == 'payment':
    # Narrow window around the August payment so the body stays readable.
    post('/billing/payments/list/', Date_From='2026-08-01', Date_To='2026-08-10',
         StartIndex=0)
else:
    print('unknown mode', mode)
    sys.exit(2)

print(f'\ntotal SUMIT calls this invocation: {calls}')

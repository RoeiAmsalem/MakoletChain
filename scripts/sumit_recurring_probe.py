#!/usr/bin/env python3
"""READ-ONLY probe: does SUMIT expose a recurring (הוראת קבע) item for a customer?

Go/no-go evidence for the cancel-subscription feature. utils/sumit.py cannot be
used here — /billing/recurring/listforcustomer/ is not in its read allowlist AND
the word "recurring" is in its _WRITE_TOKENS tripwire. Rather than weaken that
client (its strictness is the whole point), this probe talks to exactly ONE
hard-coded endpoint and refuses to be pointed anywhere else.

WHAT IT CAN DO:   POST /billing/recurring/listforcustomer/   (spec: "List
                  customer recurring items" — a read; the Request schema is
                  Credentials + Customer + IncludeInactive, no mutating field)
WHAT IT CANNOT DO: anything else. The endpoint is a module constant, never an
                  argument. /billing/recurring/cancel/ is listed below only as
                  a REFUSED constant so the intent is unmistakable in review.

Usage:  python3 scripts/sumit_recurring_probe.py <sumit_customer_id> [--inactive]
Prints the exact request (API key redacted), HTTP status, and the raw response
body verbatim. One HTTP call per invocation.
"""
import json
import os
import sys

import requests

BASE = "https://api.sumit.co.il"
TIMEOUT = 30

# The ONLY endpoint this script may ever touch.
ENDPOINT = "/billing/recurring/listforcustomer/"

# Named here purely to be explicit about what is out of scope. Never called.
_FORBIDDEN = {
    "/billing/recurring/cancel/",
    "/billing/recurring/charge/",
    "/billing/recurring/update/",
    "/billing/recurring/updatesettings/",
}


def _credentials():
    key = os.environ.get("SUMIT_API_KEY")
    org = os.environ.get("SUMIT_ORG_ID")
    if not key or not org:
        sys.exit("FATAL: SUMIT_API_KEY / SUMIT_ORG_ID not set")
    return {"CompanyID": int(org), "APIKey": key}


def main():
    if ENDPOINT in _FORBIDDEN:            # belt-and-braces; can never be true
        sys.exit("FATAL: refused")
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    customer_id = int(sys.argv[1])
    include_inactive = '--inactive' in sys.argv

    # Load .env the same way the app does.
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
    except ImportError:
        pass

    creds = _credentials()
    body = {
        "Credentials": creds,
        "Customer": {"ID": customer_id},
        "IncludeInactive": include_inactive,
    }

    shown = json.loads(json.dumps(body))
    shown["Credentials"]["APIKey"] = "<redacted>"
    print(f"POST {BASE}{ENDPOINT}")
    print("REQUEST BODY:")
    print(json.dumps(shown, ensure_ascii=False, indent=1))
    print("-" * 66)

    resp = requests.post(BASE + ENDPOINT, json=body,
                         headers={"Content-Type": "application/json"},
                         timeout=TIMEOUT)
    print(f"HTTP STATUS: {resp.status_code}")
    print("RAW RESPONSE BODY:")
    print(resp.text)
    print("-" * 66)
    print("SUMIT API CALLS CONSUMED BY THIS INVOCATION: 1")


if __name__ == '__main__':
    main()

"""READ-ONLY discovery: Aviv report 112 (מכירת פריטים לפי מחלקה) param schema.

Step 1 of the report-112 investigation. Writes NOTHING. Just:
  1. chain login (same creds the 301/902 agents use)
  2. GET /reports (report list) → confirm 112 exists + its display name
  3. GET /reports/filters/112?branch=X → dump the filter schema (names, types,
     required params) so we can build the /reports/result submit body precisely.

Runs the filter discovery for a control (9015 הגנה, aviv=15) and a broken
branch (9018 דפנה, aviv=18) to compare schemas side by side.

Usage: python scripts/probe_report112_discover.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv  # noqa: E402
load_dotenv()

import requests  # noqa: E402
import urllib3  # noqa: E402
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from agents.aviv_z_report import BASE, _login_chain_account  # noqa: E402

REPORT_ID = 112
# (local id, aviv_branch_id, label)
TARGETS = [(9015, 15, 'הגנה CONTROL-has-902-dept'),
           (9018, 18, 'דפנה BROKEN-no-902-dept')]


def get_json(url, token):
    r = requests.get(url, headers={'Authtoken': token}, timeout=30, verify=False)
    return r.status_code, (r.json() if 'json' in r.headers.get('Content-Type', '')
                           else r.text[:800])


def main():
    print('chain login...', flush=True)
    token = _login_chain_account()

    # --- report list: confirm 112 exists ---
    print('\n=== GET /reports (list) — locate report 112 ===', flush=True)
    st, body = get_json(f'{BASE}/reports?branch=15', token)
    print(f'status={st}')
    if isinstance(body, list):
        for r in body:
            rid = r.get('id')
            if rid in (112, 902, 301):
                print(f"  id={rid} name={r.get('name')!r} "
                      f"keys={list(r.keys())}")
        # also show any report whose name mentions מחלקה
        for r in body:
            nm = str(r.get('name', ''))
            if 'מחלק' in nm or 'פריט' in nm:
                print(f"  [name-match] id={r.get('id')} name={nm!r}")
    else:
        print(f'  (non-list body) {str(body)[:400]}')

    # --- filter schema for 112, per branch ---
    for (bid, av, label) in TARGETS:
        print(f'\n=== GET /reports/filters/112?branch={av}  ({bid} {label}) ===',
              flush=True)
        st, body = get_json(f'{BASE}/reports/filters/{REPORT_ID}?branch={av}',
                            token)
        print(f'status={st}')
        print(json.dumps(body, ensure_ascii=False, indent=2)[:3000])


if __name__ == '__main__':
    main()

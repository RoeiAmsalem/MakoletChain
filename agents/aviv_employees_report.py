"""Fetches employee hours from Aviv BI's employer's report.
Replaces aviv_employees.py once parser is implemented.

Endpoint: GET https://bi1.aviv-pos.co.il:8443/avivbi/v2/reports?branch={aviv_branch_id}
Auth: same as aviv_employees.py (login + refresh flow)

Schedule (future — NOT wired yet):
  - Sunday-Thursday: 16:00 + 23:30
  - Friday: 20:00
  - Saturday: 23:30

Status: SKELETON — all functions raise NotImplementedError until we capture
the live API response during store hours and implement the parser.
"""

import logging
import os

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from agents._employee_matching import match_employee_name

log = logging.getLogger(__name__)

BASE = 'https://bi1.aviv-pos.co.il:8443/avivbi/v2'
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
REPORTS_BASE = f'{BASE}/reports'


def _login(username, password):
    """Reuse same login flow as aviv_employees.py."""
    import time
    r = requests.post(f'{BASE}/account/login',
                      json={'user': username, 'password': password},
                      timeout=15, verify=False)
    r.raise_for_status()
    data = r.json()
    token = data.get('token') or data.get('value')
    branches = data.get('branches', [])
    aviv_branch_id = branches[0]['id'] if branches else None
    return token, aviv_branch_id


def _refresh(token):
    """Refresh auth token (single-use tokens)."""
    import time
    time.sleep(0.5)
    r = requests.post(f'{BASE}/account/refresh',
                      headers={'Authtoken': token, 'Content-Type': 'application/json'},
                      json={}, timeout=10, verify=False)
    return r.json().get('token') or r.json().get('value') or token


def fetch_report_list(aviv_branch_id: int, auth_token: str) -> list:
    """Fetch list of available reports for a branch.

    Returns list of report dicts. Empty list if POS is offline (404).
    """
    url = f'{REPORTS_BASE}?branch={aviv_branch_id}'
    headers = {'Authtoken': auth_token}
    r = requests.get(url, headers=headers, timeout=30, verify=False)
    if r.status_code == 404:
        log.info("Branch %s POS offline — no reports available", aviv_branch_id)
        return []
    r.raise_for_status()
    return r.json()


def find_employer_report_id(reports: list) -> int | None:
    """Identify the employer's report from the report list.

    TODO: Implement once we capture a real /reports response during store hours.
    Likely matches by report name in Hebrew (e.g., "דוח מעסיק" or similar).
    """
    raise NotImplementedError("Cannot identify employer report until we see real response shape")


def fetch_employer_report(aviv_branch_id: int, report_id: int, auth_token: str) -> bytes:
    """Download the employer report file (likely Excel).

    TODO: Implement once we know the fetch URL pattern.
    """
    raise NotImplementedError("Cannot fetch report until we see real URL pattern")


def parse_employer_report(report_bytes: bytes) -> list[dict]:
    """Parse report into list of {employee_name, hours, ...} dicts.

    TODO: Implement once we have a sample file.
    Use openpyxl (already in requirements.txt).
    """
    raise NotImplementedError("Cannot parse report until we see structure")


def update_employee_hours(branch_id: int, parsed_rows: list[dict], conn) -> dict:
    """Apply parsed report data to the DB.

    Reuses match_employee_name() from _employee_matching for each row.
    Returns: {matched: int, unmatched: int, errors: int}
    """
    raise NotImplementedError("Implement after parser is done")


def run_for_branch(branch_id: int) -> dict:
    """Main entry point. Called by scheduler per branch."""
    raise NotImplementedError("Wire after all components done")


if __name__ == '__main__':
    import argparse
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    parser = argparse.ArgumentParser(description='Aviv employer report agent (skeleton)')
    parser.add_argument('--branch-id', type=int, required=True)
    args = parser.parse_args()
    result = run_for_branch(args.branch_id)
    print(result)

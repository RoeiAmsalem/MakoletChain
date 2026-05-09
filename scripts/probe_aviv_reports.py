"""Probe Aviv BI for an employee/clock-in report endpoint that returns ALL
employees (including those who didn't ring sales).

Usage:
    python scripts/probe_aviv_reports.py [branch_id]

Default branch_id is 126 (Shimon — has the most employees).

For each candidate endpoint, prints HTTP status + first ~600 chars of the
response body. Also runs the current /employees/sales?type=all to give a
baseline employee count + total hours to compare against.

The "winner" is whichever endpoint returns:
  (a) MORE employees than /employees/sales?type=all, OR
  (b) the same employees but matches what you see in the Aviv BI UI.
"""

import json
import os
import sqlite3
import sys
import time
from datetime import date

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = 'https://bi1.aviv-pos.co.il:8443/avivbi/v2'
RAW_BASE = 'https://bi1.aviv-pos.co.il:65010'
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')


def login(username, password):
    r = requests.post(f'{BASE}/account/login',
                      json={'user': username, 'password': password},
                      timeout=15, verify=False)
    r.raise_for_status()
    data = r.json()
    token = data.get('token') or data.get('value')
    branches = data.get('branches', [])
    aviv_branch_id = branches[0]['id'] if branches else None
    return token, aviv_branch_id


def refresh(token):
    time.sleep(0.3)
    r = requests.post(f'{BASE}/account/refresh',
                      headers={'Authtoken': token, 'Content-Type': 'application/json'},
                      json={}, timeout=10, verify=False)
    return r.json().get('token') or r.json().get('value') or token


def parse_hhmm(s):
    if not s or ':' not in str(s):
        return 0.0
    parts = str(s).split(':')
    try:
        return int(parts[0]) + int(parts[1]) / 60
    except (ValueError, IndexError):
        return 0.0


def show(label, method, url, status, body, note=''):
    print(f'\n{"=" * 78}')
    print(f'  [{label}] {method} {url}')
    print(f'  status={status}  {note}')
    print(f'{"-" * 78}')
    snippet = body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)
    print(snippet[:600])
    if len(snippet) > 600:
        print(f'... [truncated, total {len(snippet)} chars]')


def try_request(label, method, url, **kwargs):
    try:
        r = requests.request(method, url, timeout=20, verify=False, **kwargs)
        try:
            body = r.json()
        except Exception:
            body = r.text
        show(label, method, url, r.status_code, body)
        return r.status_code, body
    except Exception as e:
        show(label, method, url, 'ERR', str(e))
        return None, None


def baseline_count(token, aviv_branch_id, from_date, to_date):
    """Run the current endpoint to establish a baseline."""
    r = requests.post(
        f'{BASE}/employees/sales?type=all',
        headers={'Authtoken': token, 'Content-Type': 'application/json'},
        json={'branches': [aviv_branch_id], 'from': from_date, 'to': to_date, 'employees': []},
        timeout=30, verify=False)
    r.raise_for_status()
    data = r.json()
    emps = []
    for group in data:
        if group.get('title') in ('---', 'Unknown user'):
            continue
        for e in group.get('employees', []):
            n = (e.get('name') or '').strip()
            h = parse_hhmm(e.get('workingHours', '0:00'))
            if n and h > 0:
                emps.append((n, e.get('id'), h))
    print(f'\n{"=" * 78}')
    print(f'  BASELINE: /employees/sales?type=all')
    print(f'  → {len(emps)} employees, total {sum(h for _, _, h in emps):.2f} hrs')
    for n, eid, h in emps:
        print(f'    - {n} (id={eid}) — {h:.2f}h')
    return emps


def main():
    branch_id = int(sys.argv[1]) if len(sys.argv) > 1 else 126

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    branch = conn.execute('SELECT * FROM branches WHERE id=?', (branch_id,)).fetchone()
    conn.close()
    if not branch or not branch['aviv_user_id']:
        print(f'No Aviv credentials for branch {branch_id}')
        return

    user = branch['aviv_user_id']
    pw = branch['aviv_password'] or user
    print(f'Branch {branch_id} ({branch["name"]}) — logging in as {user}...')
    token, aviv_branch_id = login(user, pw)
    token = refresh(token)
    print(f'Logged in. aviv_branch_id={aviv_branch_id}')

    today = date.today()
    from_date = today.replace(day=1).strftime('%Y%m%d') + '000000'
    to_date = today.strftime('%Y%m%d') + '235959'
    print(f'Date window: {from_date} → {to_date}')

    # Baseline first so we have something to compare against
    try:
        baseline_count(token, aviv_branch_id, from_date, to_date)
    except Exception as e:
        print(f'Baseline failed: {e}')

    headers = {'Authtoken': token, 'Content-Type': 'application/json'}

    # 1. GET /reports?branch=X (the "102 reports" hint from CLAUDE.md)
    try_request('1a', 'GET', f'{BASE}/reports?branch={aviv_branch_id}', headers=headers)
    try_request('1b', 'GET', f'{BASE}/reports', headers=headers,
                params={'branch': aviv_branch_id, 'from': from_date, 'to': to_date})

    # 2. POST /raw/employees/list (mirror of /raw/deals/list)
    try_request('2a', 'POST', f'{BASE}/raw/employees/list', headers=headers,
                json={'branches': [aviv_branch_id], 'from': from_date, 'to': to_date})
    try_request('2b', 'POST', f'{RAW_BASE}/raw/employees/list', headers=headers,
                json={'branches': [aviv_branch_id], 'from': from_date, 'to': to_date})

    # 3. POST /dashboard/query on the employees table
    for fields in (
        ['id', 'name', 'position'],
        ['id', 'name', 'position', 'hours'],
        ['id', 'name', 'position', 'workingHours'],
        ['id', 'name', 'position', 'totalHours', 'totalShifts'],
    ):
        try_request(f'3-{",".join(fields)}', 'POST', f'{BASE}/dashboard/query', headers=headers,
                    json={'table': 'employees', 'branches': [aviv_branch_id],
                          'from': from_date, 'to': to_date, 'fields': fields})

    # 4. /employees/sales with alternative type flags
    for t in ('hours', 'attendance', 'clock', 'shifts', 'all_with_clock', 'employees'):
        try_request(f'4-type={t}', 'POST', f'{BASE}/employees/sales?type={t}', headers=headers,
                    json={'branches': [aviv_branch_id], 'from': from_date, 'to': to_date,
                          'employees': []})

    # 5. Common REST shapes
    try_request('5a', 'GET', f'{BASE}/employees', headers=headers,
                params={'branch': aviv_branch_id, 'from': from_date, 'to': to_date})
    try_request('5b', 'GET', f'{BASE}/employees/list', headers=headers,
                params={'branch': aviv_branch_id})
    try_request('5c', 'POST', f'{BASE}/employees/list', headers=headers,
                json={'branches': [aviv_branch_id], 'from': from_date, 'to': to_date})
    try_request('5d', 'POST', f'{BASE}/employees/hours', headers=headers,
                json={'branches': [aviv_branch_id], 'from': from_date, 'to': to_date})
    try_request('5e', 'POST', f'{BASE}/employees/attendance', headers=headers,
                json={'branches': [aviv_branch_id], 'from': from_date, 'to': to_date})
    try_request('5f', 'POST', f'{BASE}/employees/shifts', headers=headers,
                json={'branches': [aviv_branch_id], 'from': from_date, 'to': to_date})

    # 6. Aviv "employer report" — Hebrew name guesses
    try_request('6a', 'POST', f'{BASE}/reports/employees', headers=headers,
                json={'branches': [aviv_branch_id], 'from': from_date, 'to': to_date})
    try_request('6b', 'POST', f'{BASE}/reports/attendance', headers=headers,
                json={'branches': [aviv_branch_id], 'from': from_date, 'to': to_date})
    try_request('6c', 'POST', f'{BASE}/reports/employer', headers=headers,
                json={'branches': [aviv_branch_id], 'from': from_date, 'to': to_date})

    # 7. POST /employees — GET returned 405 "method not supported", path exists
    for body in (
        {},
        {'branches': [aviv_branch_id]},
        {'branches': [aviv_branch_id], 'from': from_date, 'to': to_date},
        {'branches': [aviv_branch_id], 'from': from_date, 'to': to_date, 'employees': []},
        {'branches': [aviv_branch_id], 'from': from_date, 'to': to_date, 'type': 'all'},
        {'branches': [aviv_branch_id], 'from': from_date, 'to': to_date, 'type': 'attendance'},
    ):
        body_label = 'empty' if not body else '+'.join(sorted(body.keys()))
        try_request(f'7-POST/{body_label}', 'POST', f'{BASE}/employees', headers=headers, json=body)

    # 8. /dashboard/query — endpoint exists (400 BadRequest), body shape wrong.
    # Try a few common SQL-like shapes.
    for label, body in (
        ('select+from+where', {
            'select': ['name', 'id', 'workingHours'],
            'from': 'employees',
            'where': {'branch': aviv_branch_id, 'from': from_date, 'to': to_date},
        }),
        ('table+columns', {
            'table': 'employees',
            'columns': ['id', 'name', 'workingHours'],
            'branches': [aviv_branch_id],
            'from': from_date, 'to': to_date,
        }),
        ('queries-list', {
            'queries': [{'table': 'employees', 'branch': aviv_branch_id,
                         'from': from_date, 'to': to_date}],
        }),
        ('aggregate+groupBy', {
            'table': 'employees', 'branches': [aviv_branch_id],
            'from': from_date, 'to': to_date,
            'aggregate': ['workingHours'], 'groupBy': ['id', 'name'],
        }),
        ('deals-by-employee', {
            'table': 'deals', 'branches': [aviv_branch_id],
            'from': from_date, 'to': to_date,
            'aggregate': ['sum'], 'groupBy': ['employee'],
        }),
    ):
        try_request(f'8-{label}', 'POST', f'{BASE}/dashboard/query', headers=headers, json=body)

    # 9. /dashboard/query/envelope (bulk queries — mentioned in CLAUDE.md)
    try_request('9a', 'POST', f'{BASE}/dashboard/query/envelope', headers=headers,
                json={'queries': [{'table': 'employees', 'branches': [aviv_branch_id],
                                   'from': from_date, 'to': to_date}]})

    # 10. /employees/sales — try without ?type= and with empty body to see required shape
    try_request('10a', 'POST', f'{BASE}/employees/sales', headers=headers,
                json={'branches': [aviv_branch_id], 'from': from_date, 'to': to_date,
                      'employees': []})
    try_request('10b', 'POST', f'{BASE}/employees/sales', headers=headers, json={})

    # 12. FULL dump of /employees/sales?type=all — show every group (including
    # '---' and 'Unknown user' which the agent currently skips).
    r = requests.post(
        f'{BASE}/employees/sales?type=all',
        headers={'Authtoken': token, 'Content-Type': 'application/json'},
        json={'branches': [aviv_branch_id], 'from': from_date, 'to': to_date,
              'employees': []},
        timeout=30, verify=False)
    print(f'\n{"=" * 78}')
    print(f'  [12] FULL /employees/sales?type=all dump (every group, every employee)')
    print(f'{"-" * 78}')
    try:
        groups = r.json()
        for g in groups:
            print(f"\nGROUP title='{g.get('title')}':")
            for e in g.get('employees', []):
                print(f"  - {e.get('name')!r} (id={e.get('id')}) "
                      f"workingHours={e.get('workingHours')!r}  "
                      f"raw={ {k: v for k, v in e.items() if k not in ('name','id','workingHours')} }")
    except Exception as e:
        print(f'parse error: {e}')
        print(r.text[:1000])

    # 11. /raw/status/plain — confirm totalEmployeeHours for branch 126.
    # If this number is much bigger than the BASELINE total (92.17h above),
    # it's proof that more employees clocked in than /employees/sales shows.
    try_request('11', 'POST', f'{RAW_BASE}/raw/status/plain', headers=headers,
                json={'branches': [aviv_branch_id]})

    print(f'\n{"=" * 78}')
    print('Done. Look for:')
    print('  - 200 responses from probes 7-10 with employee data')
    print('  - In probe 11: totalEmployeeHours vs the BASELINE total (92.17h).')
    print('    A big gap means more employees clocked in than /employees/sales shows.')


if __name__ == '__main__':
    main()

"""Probe Aviv report 203 (payment tenders) — shape + Wolt rows + VAT basis.

1. GET /reports/filters/203?branch=X        → print the filter spec
2. POST /reports/result/ (XLS, month range) → once Wolt-only (inDcType=[20]),
   once ALL tenders
3. Dump every XLS row; compare the ALL-tenders month total vs
   SUM(daily_sales.amount) for the same month → VAT basis check.

Usage: probe_203_wolt.py [--branch-id 9001] [--month 2026-05]
"""
import argparse
import calendar
import json
import os
import sqlite3
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import requests

from agents.aviv_z_report import (BASE, DB_PATH, _login_chain_account,
                                  _refresh, download_xls)

ap = argparse.ArgumentParser()
ap.add_argument('--branch-id', type=int, default=9001)
ap.add_argument('--month', default='2026-05')
args = ap.parse_args()

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
br = conn.execute('SELECT id, name, aviv_branch_id FROM branches WHERE id=?',
                  (args.branch_id,)).fetchone()
print(f"branch {br['id']} {br['name']} aviv={br['aviv_branch_id']} month={args.month}")
aviv_id = br['aviv_branch_id']

y, m = map(int, args.month.split('-'))
last = calendar.monthrange(y, m)[1]
from_date = f'{args.month}-01 00:00:00'
to_date = f'{args.month}-{last:02d} 23:59:59'

token = _refresh(_login_chain_account())

# --- 1. filter spec -----------------------------------------------------
r = requests.get(f'{BASE}/reports/filters/203?branch={aviv_id}',
                 headers={'Authtoken': token}, timeout=30, verify=False)
print(f'\n=== filters/203 status={r.status_code} ===')
spec = r.json()
print(json.dumps(spec, ensure_ascii=False, indent=1)[:4000])


def submit_203(filters):
    body = {'id': 203, 'outputType': 'XLS', 'filters': filters}
    rr = requests.post(f'{BASE}/reports/result/?branch={aviv_id}', json=body,
                       headers={'Authtoken': token,
                                'Content-Type': 'application/json'},
                       timeout=60, verify=False)
    print(f'submit status={rr.status_code} body={rr.text[:300]}')
    rr.raise_for_status()
    return rr.json()['url']


def dump_xls(xls_bytes, tag):
    import xlrd
    wb = xlrd.open_workbook(file_contents=xls_bytes)
    for si in range(wb.nsheets):
        sh = wb.sheet_by_index(si)
        print(f'--- {tag} sheet[{si}] {sh.name!r} {sh.nrows}x{sh.ncols} ---')
        for i in range(sh.nrows):
            cells = [(c, sh.cell_value(i, c)) for c in range(sh.ncols)
                     if sh.cell_value(i, c) not in ('', None)]
            if cells:
                print(i, cells)


def build_filters(in_dc_type):
    """fromDate;toDate + every other filter from the spec at its default;
    inDcType overridden when requested."""
    filters = []
    for f in (spec if isinstance(spec, list) else []):
        name = f.get('name')
        if name == 'fromDate;toDate':
            filters.append({'id': f.get('id', 1), 'name': name,
                            'filterType': f.get('filterType', 'DATETIMERANGE'),
                            'value': [from_date, to_date]})
        elif name == 'inDcType':
            filters.append({'id': f.get('id', 1), 'name': name,
                            'filterType': f.get('filterType', 'MULTICHOICE'),
                            'value': in_dc_type})
        else:
            filters.append({'id': f.get('id', 1), 'name': name,
                            'filterType': f.get('filterType'),
                            'value': f.get('defaultValue',
                                           f.get('value', []))})
    return filters


# --- 2a. Wolt-only ------------------------------------------------------
print('\n=== Wolt-only (inDcType=[20]) ===')
url = submit_203(build_filters([20]))
dump_xls(download_xls(url, token), 'wolt')

time.sleep(2)

# --- 2b. ALL tenders ----------------------------------------------------
print('\n=== ALL tenders (inDcType=[]) ===')
url = submit_203(build_filters([]))
xls = download_xls(url, token)
dump_xls(xls, 'all')

# --- 3. VAT basis: daily_sales month total ------------------------------
ds = conn.execute(
    "SELECT COALESCE(SUM(amount),0), COUNT(*) FROM daily_sales "
    "WHERE branch_id=? AND strftime('%Y-%m', date)=?",
    (args.branch_id, args.month)).fetchone()
print(f'\ndaily_sales {args.month}: total={ds[0]:,.2f} over {ds[1]} days '
      f'(compare vs the ALL-tenders סה"כ above)')
conn.close()

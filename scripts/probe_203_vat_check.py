"""VAT-basis check for report 203: single-day 203 totals vs the 902 Z.

The 902 Z PDF's payment lines and its grand total are incl-VAT (they tie to
daily_sales.amount, which is incl-VAT). If 203's day total for a soed card
company matches the Z's soed payment line ₪-for-₪, 203's סה"כ is incl-VAT.

Usage: probe_203_vat_check.py [--branch-id 126] [--days 5]
"""
import argparse
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
ap.add_argument('--branch-id', type=int, default=126)
ap.add_argument('--days', type=int, default=5)
args = ap.parse_args()

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
br = conn.execute('SELECT id, name, aviv_branch_id FROM branches WHERE id=?',
                  (args.branch_id,)).fetchone()
aviv_id = br['aviv_branch_id']
print(f"branch {br['id']} {br['name']} aviv={aviv_id}")

rows = conn.execute(
    "SELECT date, amount, payment_breakdown FROM z_report_902 "
    "WHERE branch_id=? AND payment_breakdown IS NOT NULL "
    "ORDER BY date DESC LIMIT ?", (args.branch_id, args.days)).fetchall()

token = _refresh(_login_chain_account())


def fetch_203_day(day):
    body = {'id': 203, 'outputType': 'XLS', 'filters': [
        {'id': 1, 'name': 'fromDate;toDate', 'filterType': 'DATETIMERANGE',
         'value': [f'{day} 00:00:00', f'{day} 23:59:59']},
        {'id': 2, 'name': 'inDcType', 'filterType': 'MULTICHOICE', 'value': []},
        {'id': 3, 'name': 'showPie', 'filterType': 'BOOLEAN', 'value': True},
        {'id': None, 'name': 'orderBy', 'filterType': 'SORTBY',
         'value': ['sum(dc.sum) DESC']},
    ]}
    r = requests.post(f'{BASE}/reports/result/?branch={aviv_id}', json=body,
                      headers={'Authtoken': token,
                               'Content-Type': 'application/json'},
                      timeout=60, verify=False)
    r.raise_for_status()
    import xlrd
    wb = xlrd.open_workbook(file_contents=download_xls(r.json()['url'], token))
    sh = wb.sheet_by_index(0)
    out = {}
    for i in range(1, sh.nrows):
        name = str(sh.cell_value(i, 3)).strip()
        try:
            val = float(str(sh.cell_value(i, 1)).replace(',', ''))
        except ValueError:
            continue
        if name:
            out[name] = val
    return out


for r in rows:
    pb = json.loads(r['payment_breakdown'])
    t203 = fetch_203_day(r['date'])
    total203 = t203.pop('סה"כ', None)
    print(f"\n{r['date']}: Z total={r['amount']:,.2f} "
          f"Z soed={pb.get('soed')} Z credit={pb.get('credit')}")
    print(f"  203 rows={t203} | 203 total={total203}")
    time.sleep(2)

conn.close()

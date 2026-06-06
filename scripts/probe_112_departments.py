"""Read-only probe: report 112 (מכירות לפי מחלקות) for one branch + one day.

Goals:
  1. Dump the raw /reports/filters/112 JSON so we can confirm the exact filter
     names/ids the server expects (fromDate;toDate DATETIMERANGE + inDprtId).
  2. Pull the 112 XLS for the target calendar day, dump every row so we can
     see the fixed-position column layout.
  3. Emit base64 of the XLS bytes so a fixture can be created locally without
     scp (git-only workflow).

Usage:
  python scripts/probe_112_departments.py --branch-id 127 --date 2026-05-27 [--b64]
"""
import argparse
import base64
import json
import os
import sqlite3
import sys

import requests
import urllib3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from agents.aviv_z_report import (  # noqa: E402
    _login_chain_account, _refresh, BASE,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

REPORT_ID = 112
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--branch-id', type=int, default=127)
    ap.add_argument('--date', default='2026-05-27')
    ap.add_argument('--b64', action='store_true',
                    help='Also print base64 of the XLS for fixture creation')
    args = ap.parse_args()

    tok = _refresh(_login_chain_account())

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute('SELECT aviv_branch_id, name FROM branches WHERE id=?',
                       (args.branch_id,)).fetchone()
    aviv_id = row['aviv_branch_id']
    print(f'local_branch={args.branch_id} name={row["name"]!r} aviv_id={aviv_id} '
          f'date={args.date}')

    # 1) Raw filters/112 — confirm filter names/ids.
    fr = requests.get(f'{BASE}/reports/filters/{REPORT_ID}?branch={aviv_id}',
                      headers={'Authtoken': tok}, timeout=30, verify=False)
    print(f'\n=== filters/{REPORT_ID} status={fr.status_code} ===')
    if fr.status_code == 200:
        try:
            fj = fr.json()
            # Print filter skeleton: name / filterType / id (truncate possibleValues)
            def _skel(node):
                if isinstance(node, list):
                    return [_skel(x) for x in node]
                if isinstance(node, dict):
                    out = {}
                    for k, v in node.items():
                        if k == 'possibleValues' and isinstance(v, list):
                            out[k] = f'<{len(v)} values>'
                        else:
                            out[k] = _skel(v)
                    return out
                return node
            print(json.dumps(_skel(fj), ensure_ascii=False, indent=1)[:4000])
        except Exception as e:
            print('filters parse err:', e, fr.text[:300])
    else:
        print('body:', fr.text[:300])

    # 2) Pull the 112 XLS for the target calendar day.
    body = {
        'id': REPORT_ID,
        'outputType': 'XLS',
        'filters': [
            {'id': 1, 'name': 'fromDate;toDate', 'filterType': 'DATETIMERANGE',
             'value': [f'{args.date} 00:00:00', f'{args.date} 23:59:59']},
            {'id': 2, 'name': 'inDprtId', 'filterType': 'MULTICHOICE', 'value': []},
        ],
    }
    headers = {'Authtoken': tok, 'Content-Type': 'application/json'}
    r = requests.post(f'{BASE}/reports/result/?branch={aviv_id}',
                      json=body, headers=headers, timeout=60, verify=False)
    print(f'\n=== reports/result status={r.status_code} ===')
    if r.status_code != 200:
        print('body:', r.text[:500])
        return
    file_url = r.json().get('url')
    print('url:', file_url)
    g = requests.get(file_url, headers={'Authtoken': tok}, timeout=60, verify=False)
    xls = g.content
    print(f'xls bytes={len(xls)}')

    # 3) Dump rows.
    try:
        import xlrd
        wb = xlrd.open_workbook(file_contents=xls, formatting_info=False)
        print(f'sheets: {wb.sheet_names()}')
        for sname in wb.sheet_names():
            sh = wb.sheet_by_name(sname)
            print(f'\n--- sheet={sname!r} rows={sh.nrows} cols={sh.ncols} ---')
            for i in range(sh.nrows):
                vals = [sh.cell_value(i, c) for c in range(sh.ncols)]
                # show as (col_index, value) for non-empty cells so positions are clear
                nonempty = [(c, v) for c, v in enumerate(vals) if v != '']
                if nonempty:
                    print(f'  row {i}: {nonempty}')
    except ImportError:
        print('xlrd not installed')

    if args.b64:
        print('\n=== XLS_BASE64_BEGIN ===')
        print(base64.b64encode(xls).decode())
        print('=== XLS_BASE64_END ===')


if __name__ == '__main__':
    main()

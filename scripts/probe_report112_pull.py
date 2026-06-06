"""READ-ONLY: pull Aviv report 112 (מכירת פריטים לפי מחלקה) for the target branches.

Writes NOTHING. For each branch: discover the 112 filter schema, build the
/reports/result submit body dynamically from it (date range = the probe day,
inDprtId = [] = all depts, booleans/combos = their defaultValue), download the
XLS, and dump the raw sheet so we can see the per-department rows + the rich
fields (cost, profit, margin, contribution) that the 902 dept section lacks.

Targets: 9018/9019 (no 902 dept), 9016/9011 (no/partial 902), 9015/9017 (controls).

Usage: python scripts/probe_report112_pull.py [YYYY-MM-DD]
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
DAY = sys.argv[1] if len(sys.argv) > 1 else '2026-06-05'
# (local id, aviv_branch_id, label)
TARGETS = [
    (9015, 15, 'הגנה CONTROL'),
    (9017, 17, 'רמת השרון CONTROL'),
    (9018, 18, 'דפנה NO-902-dept'),
    (9019, 19, 'כפר סירקין NO-902-dept'),
    (9016, 16, 'קריית טבעון NO-902'),
    (9011, 11, 'ויצמן 902-404'),
]


def get_filters(av, token):
    r = requests.get(f'{BASE}/reports/filters/{REPORT_ID}?branch={av}',
                     headers={'Authtoken': token}, timeout=30, verify=False)
    r.raise_for_status()
    return r.json()


def build_body(filters):
    """Build a submit body from the discovered filter schema."""
    out = []
    for f in filters:
        name = f.get('name')
        ftype = f.get('filterType')
        if ftype == 'DATETIMERANGE':
            val = [f"{DAY} 00:00:00", f"{DAY} 23:59:59"]
        elif ftype == 'MULTICHOICE':
            val = []  # empty = all
        elif ftype == 'BOOLEAN':
            dv = f.get('defaultValue')
            # default chart off to keep the sheet clean
            val = False if name == 'showChart' else (dv if dv is not None else False)
        else:
            val = f.get('defaultValue')
        out.append({'id': f.get('id'), 'name': name,
                    'filterType': ftype, 'value': val})
    return {'id': REPORT_ID, 'outputType': 'XLS', 'filters': out}


def pull(av, token, body):
    r = requests.post(f'{BASE}/reports/result/?branch={av}', json=body,
                      headers={'Authtoken': token,
                               'Content-Type': 'application/json'},
                      timeout=90, verify=False)
    if r.status_code != 200:
        return None, f'result HTTP {r.status_code}: {r.text[:200]}'
    url = (r.json() or {}).get('url')
    if not url:
        return None, f'no url in result: {r.json()}'
    g = requests.get(url, headers={'Authtoken': token}, timeout=60, verify=False)
    if g.status_code != 200:
        return None, f'download HTTP {g.status_code}'
    return g.content, None


def dump_xls(xls_bytes, max_rows=70):
    try:
        import xlrd
    except ImportError:
        return '  xlrd missing'
    try:
        wb = xlrd.open_workbook(file_contents=xls_bytes, formatting_info=False)
    except Exception as e:
        return f'  open failed: {str(e)[:120]}'
    sh = wb.sheet_by_index(0)
    lines = [f'  sheet dims={sh.nrows}x{sh.ncols}']
    for i in range(min(sh.nrows, max_rows)):
        cells = []
        for c in range(sh.ncols):
            v = sh.cell_value(i, c)
            if v == '' or v is None:
                continue
            cells.append(f'{c}:{v}')
        if cells:
            lines.append(f'  [{i:3}] ' + ' | '.join(cells))
    if sh.nrows > max_rows:
        lines.append(f'  ... ({sh.nrows - max_rows} more rows)')
    return '\n'.join(lines)


def main():
    print(f'chain login... (report 112, day={DAY})', flush=True)
    token = _login_chain_account()
    for (bid, av, label) in TARGETS:
        print(f'\n===== {bid} {label} (aviv={av}) =====', flush=True)
        try:
            filters = get_filters(av, token)
        except Exception as e:
            print(f'  filters FAILED: {type(e).__name__}: {str(e)[:160]}')
            continue
        body = build_body(filters)
        print('  submit filters: ' +
              json.dumps([{f["name"]: f["value"]} for f in body['filters']],
                         ensure_ascii=False)[:200])
        xls, err = pull(av, token, body)
        if err:
            print(f'  PULL FAILED → {err}')
            continue
        print(f'  XLS bytes={len(xls)}')
        print(dump_xls(xls))


if __name__ == '__main__':
    main()

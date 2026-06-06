"""READ-ONLY: compare report 112 departments vs the stored 902 dept section.

For control 9015 הגנה on a given day: read z_department_sales (902-sourced) from
the DB and re-pull report 112, then join by dept_code and show amount deltas.
Tells us whether 112's "sale incl-VAT" matches 902's stored amount (so 112 can
swap in cleanly) and what 112 adds (cost/profit/margin/contribution).

Writes NOTHING. Usage: python scripts/probe_report112_vs_902.py [9015] [2026-06-05]
"""
import os
import re
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv  # noqa: E402
load_dotenv()

import requests  # noqa: E402
import urllib3  # noqa: E402
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from agents.aviv_z_report import BASE, _login_chain_account  # noqa: E402

DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                  'db', 'makolet_chain.db')
LOCAL_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 9015
DAY = sys.argv[2] if len(sys.argv) > 2 else '2026-06-05'
REPORT_ID = 112
_CODE_RE = re.compile(r'^\s*(\d+)\s*-\s*(.+?)\s*$')


def _f(v):
    try:
        return float(str(v).replace('%', '').replace(',', '').strip())
    except (ValueError, AttributeError):
        return 0.0


def aviv_id_for(local_id):
    conn = sqlite3.connect(DB)
    r = conn.execute('SELECT aviv_branch_id FROM branches WHERE id=?',
                     (local_id,)).fetchone()
    conn.close()
    return r[0] if r else None


def read_902(local_id, day):
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        'SELECT dept_code, dept_name, amount, qty FROM z_department_sales '
        'WHERE branch_id=? AND date=? ORDER BY dept_code', (local_id, day)
    ).fetchall()
    conn.close()
    return {r['dept_code']: dict(r) for r in rows}


def pull_112(av, token, day):
    filt = requests.get(f'{BASE}/reports/filters/{REPORT_ID}?branch={av}',
                        headers={'Authtoken': token}, timeout=30,
                        verify=False).json()
    body = []
    for f in filt:
        t = f['filterType']
        if t == 'DATETIMERANGE':
            v = [f'{day} 00:00:00', f'{day} 23:59:59']
        elif t == 'MULTICHOICE':
            v = []
        elif t == 'BOOLEAN':
            v = False if f['name'] == 'showChart' else f.get('defaultValue')
        else:
            v = f.get('defaultValue')
        body.append({'id': f['id'], 'name': f['name'], 'filterType': t, 'value': v})
    res = requests.post(f'{BASE}/reports/result/?branch={av}',
                        json={'id': REPORT_ID, 'outputType': 'XLS', 'filters': body},
                        headers={'Authtoken': token, 'Content-Type': 'application/json'},
                        timeout=90, verify=False).json()
    xls = requests.get(res['url'], headers={'Authtoken': token},
                       timeout=60, verify=False).content
    import xlrd
    sh = xlrd.open_workbook(file_contents=xls).sheet_by_index(0)
    # cols: 0 %contrib 1 %profit 2 profit 3 sale_incl_vat 4 cost_ex_vat 5 qty 6 "code - name"
    out = {}
    for i in range(1, sh.nrows):
        name_cell = str(sh.cell_value(i, 6))
        if 'סה' in name_cell:  # grand total row
            continue
        m = _CODE_RE.match(name_cell)
        if not m:
            continue
        code = int(m.group(1))
        out[code] = {
            'name': m.group(2), 'contrib': _f(sh.cell_value(i, 0)),
            'profit_pct': _f(sh.cell_value(i, 1)), 'profit': _f(sh.cell_value(i, 2)),
            'sale_incl_vat': _f(sh.cell_value(i, 3)), 'cost_ex_vat': _f(sh.cell_value(i, 4)),
            'qty': _f(sh.cell_value(i, 5)),
        }
    return out


def main():
    av = aviv_id_for(LOCAL_ID)
    print(f'branch {LOCAL_ID} (aviv={av}) day={DAY}\n')
    token = _login_chain_account()
    d902 = read_902(LOCAL_ID, DAY)
    d112 = pull_112(av, token, DAY)
    print(f'902 z_department_sales rows={len(d902)} | report-112 rows={len(d112)}')
    sum902 = sum(v['amount'] for v in d902.values())
    sum112 = sum(v['sale_incl_vat'] for v in d112.values())
    print(f'902 amount sum  = {sum902:.2f}')
    print(f'112 sale(incl)  = {sum112:.2f}\n')

    codes = sorted(set(d902) | set(d112))
    print(f"{'code':>4} {'902.amt':>10} {'112.sale':>10} {'delta':>8} "
          f"{'902.qty':>8} {'112.qty':>8} {'112.prof':>10} {'mrg%':>6}  name")
    for c in codes:
        a = d902.get(c)
        b = d112.get(c)
        amt902 = a['amount'] if a else None
        sale = b['sale_incl_vat'] if b else None
        delta = (sale - amt902) if (a and b) else None
        name = (b or a or {}).get('name', '?')
        amt_s = f'{amt902:.2f}' if amt902 is not None else '-'
        sale_s = f'{sale:.2f}' if sale is not None else '-'
        delta_s = f'{delta:+.2f}' if delta is not None else '-'
        q902_s = f"{a['qty']:.1f}" if a else '-'
        q112_s = f"{b['qty']:.1f}" if b else '-'
        prof_s = f"{b['profit']:.2f}" if b else '-'
        mrg_s = f"{b['profit_pct']:.1f}" if b else '-'
        print(f"{c:>4} {amt_s:>10} {sale_s:>10} {delta_s:>8} "
              f"{q902_s:>8} {q112_s:>8} {prof_s:>10} {mrg_s:>6}  {name}")


if __name__ == '__main__':
    main()

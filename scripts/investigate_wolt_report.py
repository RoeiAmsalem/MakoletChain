# -*- coding: utf-8 -*-
"""READ-ONLY discovery: find the Aviv "דוח כרטיסי סועד מקוצר" (short diner-cards
report) and determine what Wolt (delivery) revenue we can surface as a tile.

Steps:
  A. chain login → GET /reports?branch=<aviv> → list reports, locate the one whose
     name contains "כרטיסי סועד" (+ "מקוצר") → its report id.
  B. GET /reports/filters/<id>?branch=<aviv> → dump the filter schema.
  C. POST /reports/result (3-call pattern) for ONE branch, May 2026 → dump the full
     XLS structure (sections/rows) so we can see Wolt + other channels + totals.
  D. Loop branches → which have a Wolt row, and the May Wolt ₪.
  E. Z-inclusion: compare the diner-report total / Wolt line vs the 902 Z total
     revenue + payment_breakdown (prod z_report_902) → is Wolt INSIDE the Z total
     (a payment-method breakdown) or a SEPARATE figure?

READ-ONLY: Aviv GETs + SELECT only. No writes/deploy. Imports the existing
aviv_z_report read helpers; changes no agent code. Throttled.
"""
import json
import os
import sqlite3
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import requests                                    # noqa: E402
import urllib3                                     # noqa: E402
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from agents.aviv_z_report import (                 # noqa: E402
    BASE, _login_chain_account, _refresh, download_xls, AuthExpired, DB_PATH,
)

IL = ZoneInfo('Asia/Jerusalem')
FROM_D, TO_D = '2026-05-01', '2026-05-31'
THROTTLE = 0.5
WOLT_KEYS = ('וולט', 'wolt')
CHANNEL_HINTS = ('סועד', 'כרטיס', 'וולט', 'wolt', 'סיבוס', 'cibus', 'תן ביס',
                 'תנביס', '10bis', 'משלוח', 'מקאפ', 'גודי', 'goodi', 'דליברי',
                 'delivery', 'pay', 'ביט', 'bit')


def get_json(url, token):
    r = requests.get(url, headers={'Authtoken': token}, timeout=30, verify=False)
    ct = r.headers.get('Content-Type', '')
    return r.status_code, (r.json() if 'json' in ct else r.text[:1000])


def submit_report(aviv_id, report_id, filters, token, output='XLS'):
    body = {'id': report_id, 'outputType': output, 'filters': filters}
    r = requests.post(f'{BASE}/reports/result/?branch={aviv_id}', json=body,
                      headers={'Authtoken': token, 'Content-Type': 'application/json'},
                      timeout=60, verify=False)
    if r.status_code == 401:
        raise AuthExpired('reports/result 401')
    r.raise_for_status()
    return r.json().get('url')


def build_filters(schema, frm, to):
    """Build a submit body from the discovered schema (date range + empty
    multichoice). Falls back to a 112-style date range if no schema."""
    flist = schema.get('filters') if isinstance(schema, dict) else schema
    out = []
    for f in (flist or []):
        if not isinstance(f, dict):
            continue
        ft, nm, fid = f.get('filterType'), f.get('name'), f.get('id', 1)
        if ft == 'DATETIMERANGE':
            out.append({'id': fid, 'name': nm, 'filterType': ft,
                        'value': [f'{frm} 00:00:00', f'{to} 23:59:59']})
        elif ft == 'MULTICHOICE':
            out.append({'id': fid, 'name': nm, 'filterType': ft, 'value': []})
    if not out:
        out = [{'id': 1, 'name': 'fromDate;toDate', 'filterType': 'DATETIMERANGE',
                'value': [f'{frm} 00:00:00', f'{to} 23:59:59']}]
    return out


def xls_rows(b):
    """Yield (sheet_name, [(row_idx, [cells])]) for non-empty rows."""
    import xlrd
    wb = xlrd.open_workbook(file_contents=b, formatting_info=False)
    for sh in wb.sheets():
        rows = []
        for i in range(sh.nrows):
            cells = [sh.cell_value(i, c) for c in range(sh.ncols)]
            if any(str(x).strip() for x in cells):
                rows.append((i, cells))
        yield sh.name, rows


def cell_floats(cells):
    out = []
    for x in cells:
        if isinstance(x, (int, float)) and x:
            out.append(float(x))
        else:
            s = str(x).replace(',', '').replace('₪', '').strip()
            try:
                if s and s.replace('.', '').replace('-', '').isdigit():
                    out.append(float(s))
            except ValueError:
                pass
    return out


def find_wolt(b):
    """Return (wolt_amount or None, [channel label rows]) from the XLS bytes."""
    wolt = None
    channels = []
    for _name, rows in xls_rows(b):
        for _i, cells in rows:
            line = ' '.join(str(c) for c in cells if str(c).strip())
            low = line.lower()
            if any(h in line or h in low for h in CHANNEL_HINTS):
                nums = cell_floats(cells)
                channels.append((line[:60], max(nums) if nums else None))
            if any(k in low for k in WOLT_KEYS):
                nums = cell_floats(cells)
                if nums:
                    wolt = max(nums)
    return wolt, channels


def conn_ro():
    c = sqlite3.connect('file:' + os.path.abspath(DB_PATH) + '?mode=ro', uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def main():
    c = conn_ro()
    branches = c.execute(
        "SELECT id, name, aviv_branch_id FROM branches "
        "WHERE active=1 AND id NOT IN (9998,9999) AND aviv_branch_id IS NOT NULL "
        "ORDER BY id").fetchall()
    bmap = [(b['id'], b['aviv_branch_id'], b['name']) for b in branches]
    disc_aviv = next((a for _i, a, _n in bmap if a == 15), bmap[0][1])

    token = _refresh(_login_chain_account())

    # ── A. list reports, find diner-cards ──
    print("=" * 80)
    print(f"A. GET /reports?branch={disc_aviv} — locate 'דוח כרטיסי סועד מקוצר'")
    print("=" * 80)
    st, body = get_json(f'{BASE}/reports?branch={disc_aviv}', token)
    reps = body if isinstance(body, list) else (
        body.get('reports') or body.get('data') or [] if isinstance(body, dict) else [])
    report_id = None
    for r in reps:
        rid = r.get('id') or r.get('reportId')
        nm = r.get('name') or r.get('reportName') or r.get('title') or ''
        mark = ''
        if 'כרטיסי סועד' in nm or ('סועד' in nm and 'כרטיס' in nm):
            mark = '   <<< DINER-CARDS'
            if report_id is None or 'מקוצר' in nm:
                report_id = rid
        print(f"   id={rid!s:<6} {nm}{mark}")
    if report_id is None:
        print("\n  !! no report named ~'כרטיסי סועד' found — dumping all names above.")
        c.close()
        return
    print(f"\n  → diner-cards report id = {report_id}")

    # ── B. filter schema ──
    print("\n" + "=" * 80)
    print(f"B. GET /reports/filters/{report_id}?branch={disc_aviv} — filter schema")
    print("=" * 80)
    st, schema = get_json(f'{BASE}/reports/filters/{report_id}?branch={disc_aviv}', token)
    print(json.dumps(schema, ensure_ascii=False)[:1200])
    filters = build_filters(schema, FROM_D, TO_D)
    print(f"\n  submit filters built: {json.dumps(filters, ensure_ascii=False)}")

    # ── C. full dump for one branch ──
    print("\n" + "=" * 80)
    print(f"C. full XLS dump — branch 9015 (aviv {disc_aviv}), {FROM_D}..{TO_D}")
    print("=" * 80)
    try:
        token = _refresh(token)
        url = submit_report(disc_aviv, report_id, filters, token)
        time.sleep(THROTTLE)
        xls = download_xls(url, token)
        for name, rows in xls_rows(xls):
            print(f"  -- sheet {name!r}: {len(rows)} non-empty rows --")
            for i, cells in rows[:120]:
                vals = [str(x) for x in cells if str(x).strip()]
                print(f"   r{i:>3}: {vals}")
    except Exception as e:
        print(f"  dump failed: {type(e).__name__}: {str(e)[:120]}")

    # ── D. Wolt presence per branch ──
    print("\n" + "=" * 80)
    print(f"D. Wolt row per branch ({FROM_D}..{TO_D})")
    print("=" * 80)
    wolt_by_branch = {}
    for bid, aviv, name in bmap:
        try:
            token = _refresh(token)
            url = submit_report(aviv, report_id, filters, token)
            time.sleep(THROTTLE)
            xls = download_xls(url, token)
            wolt, channels = find_wolt(xls)
            wolt_by_branch[bid] = wolt
            chlabels = sorted({ch[0].split()[0] for ch in channels if ch[0]})[:6]
            print(f"   {bid} {name[:16]:<16} aviv={aviv:<4} "
                  f"Wolt={'₪%.0f' % wolt if wolt else '—'}   channels~{chlabels}")
        except AuthExpired:
            token = _refresh(_login_chain_account())
            print(f"   {bid} {name[:16]:<16} aviv={aviv:<4} (re-auth)")
        except Exception as e:
            print(f"   {bid} {name[:16]:<16} aviv={aviv:<4} ERR {type(e).__name__}")
        time.sleep(THROTTLE)

    # ── E. Z-inclusion check ──
    print("\n" + "=" * 80)
    print("E. Is Wolt INSIDE the Z (902) total? — diner total / payment_breakdown")
    print("=" * 80)
    for bid, aviv, name in bmap:
        if not wolt_by_branch.get(bid):
            continue
        z = c.execute(
            "SELECT SUM(amount) tot, COUNT(*) days FROM z_report_902 "
            "WHERE branch_id=? AND strftime('%Y-%m', date)=?", (bid, '2026-05')).fetchone()
        pbs = c.execute(
            "SELECT payment_breakdown FROM z_report_902 "
            "WHERE branch_id=? AND strftime('%Y-%m', date)=? AND payment_breakdown IS NOT NULL "
            "LIMIT 5", (bid, '2026-05')).fetchall()
        pm_keys = set()
        for r in pbs:
            try:
                pb = json.loads(r['payment_breakdown'])
                if isinstance(pb, dict):
                    pm_keys |= set(pb.keys())
                elif isinstance(pb, list):
                    for it in pb:
                        if isinstance(it, dict):
                            pm_keys |= {str(it.get('name') or it.get('type') or '')}
            except Exception:
                pass
        ztot = z['tot'] if z and z['tot'] else 0
        diner_pm = [k for k in pm_keys if any(h in str(k) for h in
                    ('סועד', 'וולט', 'wolt', 'כרטיס', 'סיבוס', 'תן'))]
        print(f"   {bid} {name[:16]:<16} Wolt₪{wolt_by_branch[bid]:,.0f}  "
              f"ZmayTotal₪{ztot:,.0f}  ({z['days'] if z else 0} Z-days)  "
              f"payment_methods_with_diner/wolt={diner_pm or '∅'}")
    c.close()


if __name__ == '__main__':
    main()

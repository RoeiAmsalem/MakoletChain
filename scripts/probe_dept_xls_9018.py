"""READ-ONLY: why do 9018/9019 have 902 Z but ZERO department rows?

Pulls the SAME 902 XLS the nightly agent pulls (chain login → Z-list → submit
XLS → download), for broken branches (9018 דפנה, 9019 כפר סירקין) vs a working
control (9017 רמת השרון). Writes NOTHING to DB/disk.

For each: runs parse_902_xls_departments() AND independently scans the raw sheet
for the dept-section markers ("מכירות בחתך מחלקה" / "מחלקה" header). That tells
us whether the XLS genuinely lacks a dept section (Aviv-side gap) or contains
one our parser can't locate (our-side gap).

Usage: python scripts/probe_dept_xls_9018.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv  # noqa: E402
load_dotenv()

from agents.aviv_z_report import (  # noqa: E402
    _login_chain_account, fetch_902_z_list, resolve_z_for_date,
    submit_902, download_xls, parse_902_xls_departments, _xls_cell_str,
    _iter_z_entries, _DEPT_SECTION_TITLE, _DEPT_TOTAL_LABEL,
    _DEPT_HEADER_QTY, _DEPT_HEADER_NAME,
)

# (local id, aviv_branch_id, label)
TARGETS = [(9018, 18, 'דפנה BROKEN'), (9019, 19, 'כפר סירקין BROKEN'),
           (9017, 17, 'רמת השרון CONTROL')]


def scan_raw(xls_bytes):
    """Independent of the agent parser: locate dept markers in the raw sheet."""
    try:
        import xlrd
    except ImportError:
        return "xlrd missing"
    try:
        wb = xlrd.open_workbook(file_contents=xls_bytes, formatting_info=False)
    except Exception as e:
        return f"open failed: {str(e)[:120]}"
    sh = wb.sheet_by_index(0)
    title_hits, header_hits, name_hits = [], [], []
    for i in range(sh.nrows):
        for cc in range(sh.ncols):
            v = _xls_cell_str(sh.cell_value(i, cc))
            if not v:
                continue
            if _DEPT_SECTION_TITLE in v:
                title_hits.append((i, cc, v[:30]))
            if v == _DEPT_HEADER_NAME:
                name_hits.append((i, cc))
            if v == _DEPT_TOTAL_LABEL or v == _DEPT_HEADER_QTY:
                header_hits.append((i, cc, v))
    # does a single row carry all 3 header labels (what the parser needs)?
    full_header_rows = []
    for i in range(sh.nrows):
        vals = [_xls_cell_str(sh.cell_value(i, cc)) for cc in range(sh.ncols)]
        if (_DEPT_TOTAL_LABEL in vals and _DEPT_HEADER_QTY in vals
                and _DEPT_HEADER_NAME in vals):
            full_header_rows.append(i)
    return (f"dims={sh.nrows}x{sh.ncols} | "
            f"section_title_hits={title_hits[:3]} | "
            f"name('מחלקה')_hits={name_hits[:5]} | "
            f"parser_full_header_rows={full_header_rows}")


def main():
    print("chain login...", flush=True)
    token = _login_chain_account()
    # pick a recent date that has a Z for each branch
    dates = ['2026-06-04', '2026-06-03', '2026-06-02', '2026-06-01']

    for (bid, av, label) in TARGETS:
        print(f"\n===== {bid} {label} (aviv={av}) =====", flush=True)
        try:
            zlist = fetch_902_z_list(av, token)
        except Exception as e:
            print(f"  z-list FAILED: {str(e)[:160]}")
            continue
        entries = _iter_z_entries(zlist)
        print(f"  z-list entries={len(entries)} sample={[ (e['date'], e['z_number']) for e in entries[:4] ]}")
        z = date_used = None
        for dt in dates:
            z = resolve_z_for_date(zlist, dt)
            if z:
                date_used = dt
                break
        if not z:
            print("  no Z resolved for any probe date — skipping")
            continue
        print(f"  using date={date_used} z={z}")
        try:
            xls_url = submit_902(av, z, token, output_type='XLS')
            xls_bytes = download_xls(xls_url, token)
        except Exception as e:
            print(f"  XLS submit/download FAILED: {type(e).__name__}: {str(e)[:160]}")
            continue
        print(f"  XLS bytes={len(xls_bytes)}")
        depts = parse_902_xls_departments(xls_bytes)
        print(f"  parse_902_xls_departments → {len(depts)} rows "
              f"sample={[ (d['dept_code'], d['dept_name'][:10]) for d in depts[:3] ]}")
        print(f"  RAW SCAN: {scan_raw(xls_bytes)}")


if __name__ == '__main__':
    main()

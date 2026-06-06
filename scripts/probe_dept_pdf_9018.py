"""READ-ONLY: does the 902 Z *PDF* carry a department (מחלקה) breakdown?

Prior probe checked the XLS only. This pulls the SAME 902 PDF the nightly agent
pulls (chain login → Z-list → submit PDF → download — identical to the daily_sales
path), for 9018 דפנה / 9019 כפר סירקין (no dept rows) vs 9017 control (has dept).
Writes NOTHING. Re-pulls read-only.

For each branch:
  - extract PDF text (same _extract_pdf_text the agent uses)
  - report whether 'מחלקה' / section title appears, and print the lines around it
  - ALSO pull the XLS and report its dept-section presence, for side-by-side

Usage: python scripts/probe_dept_pdf_9018.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv  # noqa: E402
load_dotenv()

from agents.aviv_z_report import (  # noqa: E402
    _login_chain_account, fetch_902_z_list, resolve_z_for_date,
    submit_902, download_pdf, download_xls, _extract_pdf_text,
    parse_902_xls_departments, _iter_z_entries,
    _DEPT_SECTION_TITLE, _DEPT_HEADER_NAME,
)

TARGETS = [(9018, 18, 'דפנה BROKEN'), (9019, 19, 'כפר סירקין BROKEN'),
           (9017, 17, 'רמת השרון CONTROL')]
DATES = ['2026-06-04', '2026-06-03', '2026-06-02', '2026-06-01']

# Hebrew tokens that would signal a department section in the PDF text.
DEPT_TOKENS = ['מחלקה', _DEPT_SECTION_TITLE, 'מחלקות', 'בחתך']


def dump_pdf_dept(text):
    lines = text.splitlines()
    hit_idx = [i for i, ln in enumerate(lines)
               if any(tok in ln for tok in DEPT_TOKENS)]
    print(f"  PDF text: {len(text)} chars, {len(lines)} lines, "
          f"dept-token line hits={len(hit_idx)}")
    # token frequency
    for tok in DEPT_TOKENS:
        cnt = text.count(tok)
        if cnt:
            print(f"    token {tok!r} appears {cnt}x")
    if not hit_idx:
        print("    → NO dept tokens anywhere in PDF text")
        return
    shown = set()
    for hi in hit_idx[:6]:
        lo, hp = max(0, hi - 1), min(len(lines), hi + 8)
        for j in range(lo, hp):
            if j in shown:
                continue
            shown.add(j)
            print(f"    [{j:4}] {lines[j]}")
        print("    ----")


def main():
    print("chain login...", flush=True)
    token = _login_chain_account()
    for (bid, av, label) in TARGETS:
        print(f"\n===== {bid} {label} (aviv={av}) =====", flush=True)
        try:
            zlist = fetch_902_z_list(av, token)
        except Exception as e:
            print(f"  z-list FAILED: {str(e)[:160]}")
            continue
        z = used = None
        for dt in DATES:
            z = resolve_z_for_date(zlist, dt)
            if z:
                used = dt
                break
        if not z:
            print("  no Z resolved for any probe date — skipping")
            continue
        print(f"  date={used} z={z}")
        # ---- PDF ----
        try:
            pdf_url = submit_902(av, z, token, output_type='PDF')
            pdf_bytes = download_pdf(pdf_url, token)
            print(f"  PDF bytes={len(pdf_bytes)}")
            text = _extract_pdf_text(pdf_bytes)
            dump_pdf_dept(text)
        except Exception as e:
            print(f"  PDF FAILED: {type(e).__name__}: {str(e)[:160]}")
        # ---- XLS (side-by-side) ----
        try:
            xls_url = submit_902(av, z, token, output_type='XLS')
            xls_bytes = download_xls(xls_url, token)
            depts = parse_902_xls_departments(xls_bytes)
            print(f"  XLS bytes={len(xls_bytes)} → parse_xls_departments={len(depts)} rows")
        except Exception as e:
            print(f"  XLS FAILED: {type(e).__name__}: {str(e)[:160]}")


if __name__ == '__main__':
    main()

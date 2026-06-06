"""READ-ONLY: does the 902 Z *PDF* carry a department (מחלקה) breakdown?

Prior probe checked the XLS only. This pulls the SAME 902 PDF the nightly agent
pulls (chain login → Z-list → submit PDF → download — identical to the daily_sales
path), for 9018 דפנה / 9019 כפר סירקין (no dept rows) vs 9017 control (has dept).
Writes NOTHING. Re-pulls read-only.

Aviv's 902 PDF text is RTL-REVERSED (per the agent's own regexes — TOTAL_RTL etc).
So 'מחלקה' would appear as 'הקלחמ'. We search BOTH orientations and dump full text.

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
    parse_902_xls_departments, resolve_z_for_date,
    _DEPT_SECTION_TITLE, _DEPT_HEADER_NAME,
)

TARGETS = [(9018, 18, 'דפנה BROKEN'), (9019, 19, 'כפר סירקין BROKEN'),
           (9017, 17, 'רמת השרון CONTROL')]
DATES = ['2026-06-04', '2026-06-03', '2026-06-02', '2026-06-01']

# Forward + reversed Hebrew dept markers.
_FWD = ['מחלקה', _DEPT_SECTION_TITLE, 'בחתך', 'מחלקות']
TOKENS = []
for t in _FWD:
    TOKENS.append(('fwd', t))
    TOKENS.append(('rev', t[::-1]))


def report(text, dump=False):
    print(f"  PDF text: {len(text)} chars, {len(text.splitlines())} lines")
    any_hit = False
    for kind, tok in TOKENS:
        cnt = text.count(tok)
        if cnt:
            any_hit = True
            print(f"    [{kind}] token {tok!r} appears {cnt}x")
    if not any_hit:
        print("    → NO dept markers (forward or reversed) in PDF text")
    if dump:
        print("    ----- FULL PDF TEXT DUMP -----")
        for i, ln in enumerate(text.splitlines()):
            print(f"    [{i:3}] {ln}")
        print("    ----- END DUMP -----")
    return any_hit


def main():
    print("chain login...", flush=True)
    token = _login_chain_account()
    for (bid, av, label) in TARGETS:
        print(f"\n===== {bid} {label} (aviv={av}) =====", flush=True)
        zlist = fetch_902_z_list(av, token)
        z = used = None
        for dt in DATES:
            z = resolve_z_for_date(zlist, dt)
            if z:
                used = dt
                break
        if not z:
            print("  no Z resolved — skipping")
            continue
        print(f"  date={used} z={z}")
        pdf_url = submit_902(av, z, token, output_type='PDF')
        pdf_bytes = download_pdf(pdf_url, token)
        print(f"  PDF bytes={len(pdf_bytes)}")
        text = _extract_pdf_text(pdf_bytes)
        # Dump full text for the control so we can SEE where depts live in a
        # known-good PDF; broken ones only dumped if they show any marker.
        report(text, dump=True)  # dump all three in full for the record
        # XLS side-by-side
        xls_url = submit_902(av, z, token, output_type='XLS')
        xls_bytes = download_xls(xls_url, token)
        depts = parse_902_xls_departments(xls_bytes)
        print(f"  XLS bytes={len(xls_bytes)} → parse_xls_departments={len(depts)} rows")


if __name__ == '__main__':
    main()

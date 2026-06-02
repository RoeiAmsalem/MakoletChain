"""READ-ONLY diagnostic: why does report 902 /result 500 only for 9016?

Reuses agents.aviv_z_report functions. Writes NOTHING to our DB/filesystem.
The only outbound calls are to Aviv BI (chain login + 902 Z-list + 902 result
generation) — the same calls the nightly agent already makes. Captures the
/reports/result/ response body that the agent normally discards via
raise_for_status(), and runs the IDENTICAL request shape against a working
branch (9017) for side-by-side comparison.

Usage: python scripts/probe_9016_z902.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from agents.aviv_z_report import (  # noqa: E402
    BASE, Z_REPORT_ID, _login_chain_account, fetch_902_z_list,
    resolve_z_for_date, build_submit_body, _iter_z_entries,
)

# (local id, aviv_branch_id)
TARGETS = [(9016, 16), (9017, 17)]      # 9016 = broken, 9017 = working control
DATES = ['2026-06-01', '2026-05-31']


def post_result(aviv_branch_id, z_number, token, output_type='PDF'):
    """POST /reports/result/ but CAPTURE everything — no raise_for_status."""
    body = build_submit_body(z_number, z_number, output_type=output_type)
    url = f'{BASE}/reports/result/?branch={aviv_branch_id}'
    r = requests.post(url, json=body,
                      headers={'Authtoken': token, 'Content-Type': 'application/json'},
                      timeout=60, verify=False)
    ctype = r.headers.get('Content-Type', '?')
    return r.status_code, ctype, r.text[:1500], body


def main():
    token = _login_chain_account()
    print(f"chain login ok (token len={len(token)})\n")

    for local_id, abid in TARGETS:
        print(f"================ branch {local_id} (aviv_branch_id={abid}) ================")
        try:
            filters = fetch_902_z_list(abid, token)
        except Exception as e:
            print(f"  Z-list FETCH FAILED: {e!r}\n")
            continue
        entries = list(_iter_z_entries(filters))
        print(f"  Z-list entries: {len(entries)}")
        if entries:
            sample = sorted(entries, key=lambda e: e['date'])
            print(f"    earliest: z={sample[0]['z_number']} date={sample[0]['date']}")
            print(f"    latest:   z={sample[-1]['z_number']} date={sample[-1]['date']}")

        for d in DATES:
            z = resolve_z_for_date(filters, d)
            print(f"  -- date {d}: resolved Z = {z}")
            if not z:
                print("     (no Z for date — closed-day skip path, not the 500)")
                continue
            for ot in ('PDF', 'XLS'):
                try:
                    code, ctype, body, sent = post_result(abid, z, token, ot)
                    verdict = 'OK' if code == 200 else f'*** {code} ***'
                    print(f"     /result Z={z} outputType={ot}: HTTP {code} {verdict}  ct={ctype}")
                    if code != 200:
                        print(f"        sent body : {sent}")
                        print(f"        resp body : {body!r}")
                except Exception as e:
                    print(f"     /result Z={z} outputType={ot}: EXC {e!r}")

        # branch 16 only: also probe the EARLIEST Z to see if 500 is Z-specific
        if local_id == 9016 and entries:
            ez = sorted(entries, key=lambda e: e['date'])[0]['z_number']
            code, ctype, body, _ = post_result(abid, ez, token, 'PDF')
            print(f"  -- earliest-Z probe Z={ez} PDF: HTTP {code}"
                  + ("" if code == 200 else f"  resp={body!r}"))
        print()


if __name__ == '__main__':
    import urllib3
    urllib3.disable_warnings()
    main()

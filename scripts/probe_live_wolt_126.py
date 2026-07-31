"""Read-only probe — branch 126 Wolt-vs-live diagnosis (2026-07-31).

1. One fresh /raw/status/plain tick for aviv_branch_id=3 (126 אינשטיין) —
   dumps EVERY field in the raw row, especially payments[] (tender split).
2. Report 203 (Wolt, inDcType=20) for TODAY only and for the full month —
   is today's ₪119 visible on the Wolt side?

No DB writes. Run on prod:
  /opt/makolet-chain/venv/bin/python scripts/probe_live_wolt_126.py
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agents.aviv_live import _login_chain_account, API_PLAIN  # noqa: E402
from agents.wolt_sales import (  # noqa: E402
    BASE, REPORT_ID, WOLT_DC_TYPE, _refresh, download_xls,
    parse_203_wolt_total,
)

import requests  # noqa: E402

AVIV_BRANCH_ID = 3   # branches.aviv_branch_id for local 126


def submit_203(token, from_dt, to_dt):
    body = {
        'id': REPORT_ID,
        'outputType': 'XLS',
        'filters': [
            {'id': 1, 'name': 'fromDate;toDate', 'filterType': 'DATETIMERANGE',
             'value': [from_dt, to_dt]},
            {'id': 2, 'name': 'inDcType', 'filterType': 'MULTICHOICE',
             'value': [WOLT_DC_TYPE]},
            {'id': 3, 'name': 'showPie', 'filterType': 'BOOLEAN', 'value': True},
            {'id': None, 'name': 'orderBy', 'filterType': 'SORTBY',
             'value': ['sum(dc.sum) DESC']},
        ],
    }
    r = requests.post(f'{BASE}/reports/result/?branch={AVIV_BRANCH_ID}',
                      json=body,
                      headers={'Authtoken': token,
                               'Content-Type': 'application/json'},
                      timeout=60, verify=False)
    r.raise_for_status()
    return r.json()['url']


def main():
    token = _login_chain_account()

    # --- 1) fresh live status tick, full raw row ---
    r = requests.post(f'{API_PLAIN}/raw/status/plain',
                      json={'branches': [AVIV_BRANCH_ID]},
                      headers={'Content-Type': 'application/json',
                               'Authtoken': token},
                      timeout=30, verify=False)
    r.raise_for_status()
    rows = r.json()
    print('=== RAW /raw/status/plain row (branch 126 / aviv 3) ===')
    print(json.dumps(rows[0] if rows else rows, indent=2, ensure_ascii=False))

    # --- 2) report 203 Wolt: today only, then full July ---
    for label, frm, to in (
        ('TODAY 2026-07-31', '2026-07-31 00:00:00', '2026-07-31 23:59:59'),
        ('FULL 2026-07', '2026-07-01 00:00:00', '2026-07-31 23:59:59'),
    ):
        token = _refresh(token)
        try:
            url = submit_203(token, frm, to)
            total = parse_203_wolt_total(download_xls(url, token))
            print(f'=== 203 Wolt {label}: {total}')
        except Exception as e:
            print(f'=== 203 Wolt {label}: ERROR {e}')


if __name__ == '__main__':
    main()

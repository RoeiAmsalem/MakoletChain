"""Read-only diagnostic: compare aviv branches 1 (no Z?), 16 (HTTP 500?) vs 8 (working).

Goals
  1. Branch 1 (קדיש לוז): does filters/902 return ANY Z entries at all? Is it an
     idle branch that never produces Z, or just closed yesterday?
  2. Branch 16 (קריית טבעון): the report submission returned HTTP 500 during the
     manual pull. Probe filters/902 AND reports/result/ now to see whether the
     500 is persistent or transient, and which endpoint it actually happens on.
  3. Branch 8 (תיכון): known working — used as the control.

Pipeline per branch:
  GET /reports/filters/902?branch=<id>          → count Z entries, show last 3
  POST /reports/result/?branch=<id> {ID_Z=last} → status code + body preview
  (No writes. PDF download is skipped to keep this fast & non-invasive.)
"""
import json
import os
import re
import sys
import time

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = 'https://bi1.aviv-pos.co.il:8443/avivbi/v2'
PROBE_BRANCHES = (1, 16, 8)
Z_LABEL_RE = re.compile(r'Z:\s*(\d+)\s*\|\s*(\d{1,2}/\d{1,2}/\d{2,4})')


def _login_chain() -> str:
    user = os.environ.get('AVIV_CHAIN_USER')
    pw = os.environ.get('AVIV_CHAIN_PASS')
    if not user or not pw:
        print('ERROR: AVIV_CHAIN_USER / AVIV_CHAIN_PASS not set', file=sys.stderr)
        sys.exit(2)
    r = requests.post(f'{BASE}/account/login',
                      json={'user': user, 'password': pw},
                      timeout=15, verify=False)
    r.raise_for_status()
    j = r.json() or {}
    tok = j.get('token') or j.get('value')
    if not tok:
        print('ERROR: login response missing token', file=sys.stderr)
        sys.exit(2)
    return tok


def _refresh(token: str) -> str:
    r = requests.post(f'{BASE}/account/refresh',
                      headers={'Authtoken': token, 'Content-Type': 'application/json'},
                      json={}, timeout=10, verify=False)
    if r.status_code != 200:
        return token
    j = r.json() or {}
    return j.get('token') or j.get('value') or token


def _extract_z_entries(filters_json) -> list[tuple[int, str]]:
    """Walk the filters response and return [(z_number, date_label_str), ...]."""
    out: list[tuple[int, str]] = []

    def _visit(node):
        if isinstance(node, dict):
            if len(node) == 1:
                (_, v), = node.items()
                if isinstance(v, str):
                    m = Z_LABEL_RE.search(v)
                    if m:
                        try:
                            out.append((int(m.group(1)), m.group(2)))
                        except ValueError:
                            pass
            for child in node.values():
                _visit(child)
        elif isinstance(node, list):
            for item in node:
                _visit(item)

    _visit(filters_json)
    return out


def probe_filters(aviv_id: int, token: str) -> dict:
    url = f'{BASE}/reports/filters/902?branch={aviv_id}'
    t0 = time.monotonic()
    r = requests.get(url, headers={'Authtoken': token}, timeout=30, verify=False)
    dt = time.monotonic() - t0
    out: dict = {'status': r.status_code, 'elapsed_s': round(dt, 2),
                 'body_preview': r.text[:240]}
    if r.status_code == 200:
        try:
            entries = _extract_z_entries(r.json())
            entries.sort(key=lambda e: e[0])
            out['z_count'] = len(entries)
            out['z_first3'] = entries[:3]
            out['z_last3'] = entries[-3:]
        except Exception as e:
            out['parse_error'] = str(e)[:160]
    return out


def probe_result(aviv_id: int, z_number: int, token: str) -> dict:
    """POST /reports/result/?branch=<id> with the given Z. Read-only: we do NOT
    download the resulting PDF, just observe whether Aviv accepts the request.
    """
    url = f'{BASE}/reports/result/?branch={aviv_id}'
    body = {
        'id': 902,
        'outputType': 'PDF',
        'filters': [
            {'id': 1, 'name': 'ID_Z', 'filterType': 'INTEGER', 'value': z_number},
            {'id': 2, 'name': 'TO_Z', 'filterType': 'INTEGER', 'value': z_number},
        ],
    }
    t0 = time.monotonic()
    r = requests.post(url, json=body,
                      headers={'Authtoken': token, 'Content-Type': 'application/json'},
                      timeout=60, verify=False)
    dt = time.monotonic() - t0
    out: dict = {'status': r.status_code, 'elapsed_s': round(dt, 2),
                 'body_preview': r.text[:240]}
    if r.status_code == 200:
        try:
            out['has_url'] = bool((r.json() or {}).get('url'))
        except Exception:
            pass
    return out


def main() -> int:
    print(f'[login] POST {BASE}/account/login')
    token = _refresh(_login_chain())
    print('[login] ok\n')

    for aviv_id in PROBE_BRANCHES:
        label = {1: 'קדיש לוז (claimed: closed-day)',
                 16: 'קריית טבעון (claimed: 500 on result)',
                 8: 'תיכון (CONTROL — known working)'}.get(aviv_id, '')
        print(f'━━━ aviv_branch_id={aviv_id} — {label} ━━━')

        f = probe_filters(aviv_id, token)
        print(f'  filters/902 → status={f["status"]} elapsed={f["elapsed_s"]}s '
              f'z_count={f.get("z_count", "—")}')
        if f.get('z_first3'):
            print(f'    first 3 Z entries: {f["z_first3"]}')
        if f.get('z_last3') and f.get('z_count', 0) > 3:
            print(f'    last  3 Z entries: {f["z_last3"]}')
        if f['status'] != 200 or f.get('z_count', 0) == 0:
            print(f'    body preview: {f["body_preview"]!r}')

        # Pick the most recent Z for the result probe — that's what the agent
        # would actually submit on a "yesterday" pull when one exists.
        z_for_result = (f.get('z_last3') or [(None, None)])[-1][0]
        if z_for_result is None:
            print('  result/  → SKIPPED (no Z available to submit)')
        else:
            r = probe_result(aviv_id, z_for_result, token)
            print(f'  result/ Z={z_for_result} → status={r["status"]} '
                  f'elapsed={r["elapsed_s"]}s has_url={r.get("has_url", "—")}')
            if r['status'] != 200:
                print(f'    body preview: {r["body_preview"]!r}')
        print()

    return 0


if __name__ == '__main__':
    sys.exit(main())

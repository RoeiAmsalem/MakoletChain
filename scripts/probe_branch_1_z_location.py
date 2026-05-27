"""Read-only diagnostic: WHERE is branch 1's Z actually exposed in Aviv?

Ground truth (from a printed Z): branch 1 (קדיש לוז) DID produce a Z for
2026-05-26 — daily total ₪21,429.36, 460 transactions, cumulative Z
₪52,338,653.55. But filters/902?branch=1 returns z_count=0.

So the Z exists in Aviv; it's just not in the 902 feed for this branch. This
script enumerates what /is/ available for branch 1 to pinpoint where to read
it from. No writes, no submit, no PDF downloads.

Probes (all under chain account auth):
  1. GET  /avivbi/v2/reports/filters/902?branch=1   — confirm raw 200 empty
  2. GET  /avivbi/v2/reports/filters/902?branch=8   — control diff
  3. GET  /avivbi/v2/reports?branch=1               — full report catalog
  4. GET  /avivbi/v2/reports?branch=8               — control catalog
  5. Diff the two catalogs by report id/name
  6. GET  :65010/raw/status/plain?branches=1        — live status (should
     surface today's running total + z field for the open shift)
  7. GET  :65010/raw/status/plain?branches=8        — control for shape

Output is verbose by design — this is the artifact Roei reads to decide the
next move.
"""
import json
import os
import sys
from urllib.parse import quote

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = 'https://bi1.aviv-pos.co.il:8443/avivbi/v2'
STATUS_BASE = 'https://bi1.aviv-pos.co.il:65010'
TARGET_BRANCH = 1
CONTROL_BRANCH = 8


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


def _fmt_status(r) -> str:
    return f'status={r.status_code} bytes={len(r.content)} ctype={r.headers.get("Content-Type","?")}'


def _maybe_json(r):
    try:
        return r.json()
    except Exception:
        return None


# ── Probe 1+2: filters/902 raw bodies for branch 1 and 8 ──────────────────

def probe_filters_902(branch: int, token: str) -> dict:
    url = f'{BASE}/reports/filters/902?branch={branch}'
    print(f'\n━━━ GET {url}')
    r = requests.get(url, headers={'Authtoken': token}, timeout=30, verify=False)
    print(f'  {_fmt_status(r)}')
    j = _maybe_json(r)
    if j is None:
        print(f'  body (non-JSON): {r.text[:500]!r}')
        return {'ok': False}
    # Pretty-print but truncated.
    print(f'  body: {json.dumps(j, ensure_ascii=False)[:1200]}')
    # Count Z entries by inspecting ID_Z.possibleValues across the filters tree.
    n_z = 0
    if isinstance(j, list):
        for f in j:
            if isinstance(f, dict) and f.get('name') == 'ID_Z':
                pv = f.get('possibleValues')
                if isinstance(pv, list):
                    n_z = len(pv)
                elif pv is None:
                    n_z = 0
                break
    print(f'  → ID_Z.possibleValues count = {n_z}')
    return {'ok': True, 'z_count': n_z, 'body': j}


# ── Probe 3+4+5: report catalog per branch ───────────────────────────────

def probe_reports_catalog(branch: int, token: str) -> list[dict]:
    url = f'{BASE}/reports?branch={branch}'
    print(f'\n━━━ GET {url}')
    r = requests.get(url, headers={'Authtoken': token}, timeout=30, verify=False)
    print(f'  {_fmt_status(r)}')
    j = _maybe_json(r)
    if j is None:
        print(f'  body (non-JSON): {r.text[:500]!r}')
        return []
    if not isinstance(j, list):
        print(f'  unexpected shape: {type(j).__name__} preview={str(j)[:300]!r}')
        return []
    print(f'  count: {len(j)}')
    rows: list[dict] = []
    for item in j:
        if not isinstance(item, dict):
            continue
        rid = item.get('id')
        name = item.get('name') or item.get('title') or ''
        rows.append({'id': rid, 'name': str(name), 'raw': item})
    rows.sort(key=lambda x: (x['id'] is None, x['id']))
    for row in rows:
        print(f'    id={row["id"]:>4}  name={row["name"]!r}')
    return rows


def diff_catalogs(branch_a: int, rows_a: list[dict],
                  branch_b: int, rows_b: list[dict]) -> None:
    a_ids = {r['id']: r for r in rows_a}
    b_ids = {r['id']: r for r in rows_b}
    only_a = sorted(set(a_ids) - set(b_ids), key=lambda x: (x is None, x))
    only_b = sorted(set(b_ids) - set(a_ids), key=lambda x: (x is None, x))
    common = sorted(set(a_ids) & set(b_ids), key=lambda x: (x is None, x))
    print(f'\n━━━ catalog diff: branch {branch_a} vs branch {branch_b}')
    print(f'  common report ids: {common}')
    if only_a:
        print(f'  ONLY in branch {branch_a} ({len(only_a)}):')
        for rid in only_a:
            print(f'    id={rid}  name={a_ids[rid]["name"]!r}')
    else:
        print(f'  ONLY in branch {branch_a}: (none)')
    if only_b:
        print(f'  ONLY in branch {branch_b} ({len(only_b)}):')
        for rid in only_b:
            print(f'    id={rid}  name={b_ids[rid]["name"]!r}')
    else:
        print(f'  ONLY in branch {branch_b}: (none)')

    # Is 902 in branch 1's catalog at all?
    has_902_a = 902 in a_ids
    has_902_b = 902 in b_ids
    print(f'  → branch {branch_a} has report 902? {has_902_a}')
    print(f'  → branch {branch_b} has report 902? {has_902_b}')


# ── Probe 6+7: live status endpoint ──────────────────────────────────────

def probe_live_status(branch: int, token: str) -> None:
    """Hit the :65010/raw/status/plain endpoint for one branch. The shape used
    in production is `branches=<id>` as a query param (per CLAUDE.md), but we
    also try the JSON-body form in case the chain account uses a different
    shape. Read-only — no DB writes.
    """
    # Form A: query-string ?branches=<id>
    url_a = f'{STATUS_BASE}/raw/status/plain?branches={branch}'
    print(f'\n━━━ GET {url_a}')
    try:
        r = requests.get(url_a, headers={'Authtoken': token}, timeout=15, verify=False)
        print(f'  {_fmt_status(r)}')
        print(f'  body[:1200]: {r.text[:1200]!r}')
    except Exception as e:
        print(f'  ERROR: {e}')

    # Form B: POST with body — some Aviv install variants want this.
    url_b = f'{STATUS_BASE}/raw/status/plain'
    print(f'\n━━━ POST {url_b}  body={{"branches":[{branch}]}}')
    try:
        r = requests.post(url_b, json={'branches': [branch]},
                          headers={'Authtoken': token, 'Content-Type': 'application/json'},
                          timeout=15, verify=False)
        print(f'  {_fmt_status(r)}')
        print(f'  body[:1200]: {r.text[:1200]!r}')
    except Exception as e:
        print(f'  ERROR: {e}')


def main() -> int:
    print(f'[login] POST {BASE}/account/login')
    token = _refresh(_login_chain())
    print('[login] ok')

    # 1+2: raw filters/902 for both branches
    probe_filters_902(TARGET_BRANCH, token)
    probe_filters_902(CONTROL_BRANCH, token)

    # 3+4+5: report catalogs + diff
    rows_a = probe_reports_catalog(TARGET_BRANCH, token)
    rows_b = probe_reports_catalog(CONTROL_BRANCH, token)
    diff_catalogs(TARGET_BRANCH, rows_a, CONTROL_BRANCH, rows_b)

    # 6+7: live status endpoint
    probe_live_status(TARGET_BRANCH, token)
    probe_live_status(CONTROL_BRANCH, token)

    return 0


if __name__ == '__main__':
    sys.exit(main())

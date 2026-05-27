"""Read-only diagnostic: does /reports/filters/902/possible-values populate the
Z list for branches the main /reports/filters/902 leaves empty under the chain
account (Y25165)?

Discovery (browser network trace): the BI website's Z dropdown is fed by a
SEPARATE lazy-load endpoint:

  GET /avivbi/v2/reports/filters/902/possible-values?filter=ID_Z&branch=<id>

Our agent reads /reports/filters/902 only — which returns
ID_Z.possibleValues=null with lazy=true for branch 1 under the chain account.
If the lazy-load endpoint returns the Z list under the same chain token, the
fix is just to point the agent at it.

Probes (all chain-token, read-only — no /reports/result/ submits, no PDFs):
  1. CONTROL — GET /reports/filters/902?branch=1  → confirm possibleValues=null
  2. THE TEST — GET /reports/filters/902/possible-values?filter=ID_Z&branch=1
     → populated? show Z count + top 3 (expect 3036 / 26-05-2026 at top).
  3. Same possible-values call for branch 16 (the persistent 500-on-render branch).
  4. CONTROL — same call for branch 8 (known working) for shape comparison.
  5. BONUS — explore whether other filters (TO_Z, possible date filter) are
     reachable via the same possible-values pattern.
"""
import json
import os
import re
import sys

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = 'https://bi1.aviv-pos.co.il:8443/avivbi/v2'
POSSIBLE_VALUES_PATH = '/reports/filters/902/possible-values'
Z_LABEL_RE = re.compile(r'Z:\s*(\d+)\s*\|\s*(\d{1,2}/\d{1,2}/\d{2,4})')

PROBE_BRANCHES = (1, 16, 8)


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


def _fmt(r) -> str:
    return f'status={r.status_code} bytes={len(r.content)} ctype={r.headers.get("Content-Type","?")}'


def _maybe_json(r):
    try:
        return r.json()
    except Exception:
        return None


def _extract_z_entries(node) -> list[tuple[int, str]]:
    """Walk anywhere-nested 'Z: <num>|DD/MM/YYYY' labels."""
    out: list[tuple[int, str]] = []

    def visit(n):
        if isinstance(n, dict):
            if len(n) == 1:
                (_, v), = n.items()
                if isinstance(v, str):
                    m = Z_LABEL_RE.search(v)
                    if m:
                        try:
                            out.append((int(m.group(1)), m.group(2)))
                        except ValueError:
                            pass
            for child in n.values():
                visit(child)
        elif isinstance(n, list):
            for item in n:
                visit(item)

    visit(node)
    return out


def control_filters_902(branch: int, token: str) -> None:
    url = f'{BASE}/reports/filters/902?branch={branch}'
    print(f'\n━━━ CONTROL: GET {url}')
    r = requests.get(url, headers={'Authtoken': token}, timeout=30, verify=False)
    print(f'  {_fmt(r)}')
    j = _maybe_json(r)
    if isinstance(j, list):
        for f in j:
            if isinstance(f, dict) and f.get('name') == 'ID_Z':
                pv = f.get('possibleValues')
                lazy = f.get('lazy')
                if pv is None:
                    print(f'  → ID_Z.possibleValues = null  (lazy={lazy})  ← matches expectation')
                elif isinstance(pv, list):
                    print(f'  → ID_Z.possibleValues populated: {len(pv)} entries (lazy={lazy})')
                break


def probe_possible_values(branch: int, filter_name: str, token: str) -> dict:
    url = f'{BASE}{POSSIBLE_VALUES_PATH}?filter={filter_name}&branch={branch}'
    print(f'\n━━━ TEST: GET {url}')
    r = requests.get(url, headers={'Authtoken': token}, timeout=30, verify=False)
    print(f'  {_fmt(r)}')
    out: dict = {'status': r.status_code, 'body_preview': r.text[:400]}
    if r.status_code != 200:
        print(f'  body: {r.text[:600]!r}')
        return out
    j = _maybe_json(r)
    if j is None:
        print(f'  body (non-JSON): {r.text[:300]!r}')
        return out
    # Show structure preview.
    serialized = json.dumps(j, ensure_ascii=False)
    print(f'  body[:600]: {serialized[:600]}')

    entries = _extract_z_entries(j)
    entries.sort(key=lambda e: e[0], reverse=True)
    out['z_count'] = len(entries)
    if entries:
        out['top3'] = entries[:3]
        out['bottom3'] = entries[-3:]
        print(f'  → Z count: {len(entries)}')
        print(f'    top 3 (most recent): {entries[:3]}')
        print(f'    bottom 3 (oldest):   {entries[-3:]}')
    else:
        print('  → no Z entries parsed from response (could be unexpected shape)')
    return out


def main() -> int:
    print(f'[login] POST {BASE}/account/login (chain account from env)')
    token = _refresh(_login_chain())
    print('[login] ok — token will NEVER be printed')

    # ── Step 1: control on branch 1 (expect possibleValues=null) ──
    control_filters_902(1, token)

    # ── Step 2: THE TEST — possible-values for branch 1 ──
    print('\n' + '═' * 60)
    print('STEP 2: possible-values endpoint for branch 1 (THE TEST)')
    print('═' * 60)
    r1 = probe_possible_values(1, 'ID_Z', token)

    # ── Step 3: same for branch 16 (the 500-on-render branch) ──
    print('\n' + '═' * 60)
    print('STEP 3: possible-values endpoint for branch 16')
    print('═' * 60)
    r16 = probe_possible_values(16, 'ID_Z', token)

    # ── Step 4: control on branch 8 for shape comparison ──
    print('\n' + '═' * 60)
    print('STEP 4: possible-values endpoint for branch 8 (control)')
    print('═' * 60)
    r8 = probe_possible_values(8, 'ID_Z', token)

    # ── Step 5: BONUS — try TO_Z and any plausible "date" filters ──
    # Only run if step 2 succeeded — saves Aviv side load otherwise.
    if r1.get('z_count'):
        print('\n' + '═' * 60)
        print('STEP 5 (bonus): is TO_Z also lazy-loadable the same way?')
        print('═' * 60)
        probe_possible_values(1, 'TO_Z', token)

    # ── Summary ──
    print('\n' + '═' * 60)
    print('SUMMARY')
    print('═' * 60)
    for label, res in (('branch=1 (קדיש לוז, expected fix)', r1),
                        ('branch=16 (קריית טבעון, 500-on-render)', r16),
                        ('branch=8 (תיכון, control)', r8)):
        status = res.get('status')
        count = res.get('z_count')
        top = res.get('top3')
        print(f'  {label}: status={status} z_count={count} top={top}')
    return 0


if __name__ == '__main__':
    sys.exit(main())

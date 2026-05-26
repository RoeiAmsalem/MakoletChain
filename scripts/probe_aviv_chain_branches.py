"""Read-only diagnostic: chain account branch list + filters/902 probe.

Logs in once with AVIV_CHAIN_USER / AVIV_CHAIN_PASS, then:
  1. POST /account/branches → list every Aviv branch under this chain account
  2. GET /reports/filters/902?branch=<id> for each branch in branches table that
     has aviv_branch_id set — print status code + (if 200) the count of Z entries.

Use to verify aviv_branch_id mapping and whether filters/902 is available right
now per branch. No writes, no submits.
"""
import os
import sqlite3
import sys
import urllib3
from pathlib import Path

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = 'https://bi1.aviv-pos.co.il:8443/avivbi/v2'
DB_PATH = Path(__file__).resolve().parent.parent / 'db' / 'makolet_chain.db'


def main() -> int:
    user = os.environ.get('AVIV_CHAIN_USER')
    pw = os.environ.get('AVIV_CHAIN_PASS')
    if not user or not pw:
        print('ERROR: AVIV_CHAIN_USER / AVIV_CHAIN_PASS not set', file=sys.stderr)
        return 2

    print(f'[login] POST {BASE}/account/login as {user!r}')
    r = requests.post(f'{BASE}/account/login',
                      json={'user': user, 'password': pw},
                      timeout=15, verify=False)
    print(f'[login] status={r.status_code}')
    r.raise_for_status()
    data = r.json() or {}
    token = data.get('token') or data.get('value')
    inline_branches = data.get('branches') or []
    print(f'[login] token={"yes" if token else "MISSING"} '
          f'inline_branches={len(inline_branches)}')
    for b in inline_branches:
        print(f'  inline-branch id={b.get("id")} name={b.get("name")!r}')

    print('\n[branches] POST /account/branches (explicit list)')
    rb = requests.post(f'{BASE}/account/branches', json={},
                       headers={'Authtoken': token, 'Content-Type': 'application/json'},
                       timeout=15, verify=False)
    print(f'[branches] status={rb.status_code}')
    if rb.status_code == 200:
        try:
            body = rb.json()
        except Exception as e:
            print(f'[branches] non-JSON body: {e} raw={rb.text[:200]!r}')
            body = None
        if isinstance(body, list):
            print(f'[branches] count={len(body)}')
            for b in body:
                print(f'  branch id={b.get("id")} name={b.get("name")!r} '
                      f'extra_keys={[k for k in b.keys() if k not in ("id","name")]}')
        else:
            print(f'[branches] unexpected shape: {type(body).__name__} '
                  f'preview={str(body)[:200]!r}')
    else:
        print(f'[branches] body={rb.text[:300]!r}')

    print('\n[db] reading branches table for aviv_branch_id map')
    conn = sqlite3.connect(f'file:{DB_PATH}?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, name, aviv_branch_id FROM branches "
        "WHERE active=1 AND aviv_branch_id IS NOT NULL ORDER BY id"
    ).fetchall()
    conn.close()
    print(f'[db] {len(rows)} active branches with aviv_branch_id set')
    for r in rows:
        print(f'  makolet-branch id={r["id"]} name={r["name"]!r} '
              f'-> aviv_branch_id={r["aviv_branch_id"]}')

    print('\n[probe] GET /reports/filters/902?branch=<id> for each')
    for r in rows:
        ab = r['aviv_branch_id']
        url = f'{BASE}/reports/filters/902?branch={ab}'
        resp = requests.get(url, headers={'Authtoken': token},
                            timeout=30, verify=False)
        n_entries = ''
        if resp.status_code == 200:
            try:
                j = resp.json()
                if isinstance(j, list):
                    # Count Z entries (possibleValues across all filters of name ID_Z)
                    n = 0
                    for f in j:
                        if isinstance(f, dict) and f.get('name') == 'ID_Z':
                            n = len(f.get('possibleValues') or [])
                            break
                    n_entries = f' z_count={n}'
            except Exception:
                pass
        print(f'  branch={ab} (makolet {r["id"]} {r["name"]!r}) '
              f'status={resp.status_code}{n_entries} '
              f'body_preview={resp.text[:120]!r}')

    return 0


if __name__ == '__main__':
    sys.exit(main())

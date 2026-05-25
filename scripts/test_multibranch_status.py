"""READ-ONLY test: does :65010/raw/status/plain return MULTIPLE branches in one call?

Same endpoint prod's aviv_live already hits daily; we're just sending a longer
branches list. No writes, no report submission. Credentials from env vars
YANIV_AVIV_USER / YANIV_AVIV_PASS — never logged.
"""
import json
import os
import sys
import time

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_BASE = "https://bi1.aviv-pos.co.il:8443/avivbi/v2"
API_PLAIN = "https://bi1.aviv-pos.co.il:65010"
TIMEOUT = 20


def die(msg: str) -> None:
    print(f"STOP: {msg}", file=sys.stderr)
    sys.exit(2)


def login(user: str, password: str) -> str:
    r = requests.post(
        f"{API_BASE}/account/login",
        json={"user": user, "password": password},
        timeout=15, verify=False,
    )
    r.raise_for_status()
    data = r.json()
    token = data.get("token") or data.get("value")
    if not token:
        die("login response missing token")
    return token


def fetch_status(token: str, branch_ids: list[int]) -> requests.Response:
    return requests.post(
        f"{API_PLAIN}/raw/status/plain",
        json={"branches": branch_ids},
        headers={"Authtoken": token, "Content-Type": "application/json"},
        timeout=TIMEOUT, verify=False,
    )


def hr(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def summarize(rows, branch_ids):
    """Print structural summary: list/dict, length, ids found, sample of each row."""
    print(f"  type: {type(rows).__name__}")
    if isinstance(rows, list):
        print(f"  list length: {len(rows)}")
        # Per-row key/id detection.
        id_fields = ["branchId", "branch_id", "branch", "id", "shopId", "shop_id"]
        found_ids = []
        for i, row in enumerate(rows):
            if not isinstance(row, dict):
                print(f"    [{i}] non-dict: {row!r}")
                continue
            row_id = next((row[k] for k in id_fields if k in row), None)
            found_ids.append(row_id)
            keys = sorted(row.keys())
            print(f"    [{i}] id_field={row_id!r}  keys({len(keys)})={keys[:10]}{'…' if len(keys)>10 else ''}")
        print(f"  ids returned: {found_ids}")
        print(f"  ids requested: {branch_ids}")
        print(f"  match (set): {set(filter(None, found_ids)) == set(branch_ids)}")
    elif isinstance(rows, dict):
        print(f"  dict keys: {sorted(rows.keys())[:20]}")
    else:
        print(f"  unexpected: {rows!r}")


def print_row_essentials(row, branch_id):
    """Compact view: branch id + the financial fields aviv_live cares about."""
    if not isinstance(row, dict):
        print(f"    branch {branch_id}: non-dict row")
        return
    interesting = ["dealTotal", "dealCount", "tmUpdate",
                   "totalEmployeeHours", "currentEmployeeHours",
                   "cancellationTotal", "discountTotal"]
    snippet = {k: row.get(k) for k in interesting if k in row}
    print(f"    branch {branch_id}: {snippet}")


def main():
    user = os.environ.get("YANIV_AVIV_USER")
    pw = os.environ.get("YANIV_AVIV_PASS")
    if not user or not pw:
        die("YANIV_AVIV_USER / YANIV_AVIV_PASS must be set in the env.")

    print(f"login as user={user!r} (password redacted)")
    token = login(user, pw)
    print(f"  token OK (length={len(token)})")
    time.sleep(0.5)

    # ----- 1) Single-branch baseline -----
    hr("1. SINGLE branch baseline — {'branches': [3]}")
    r = fetch_status(token, [3])
    print(f"  HTTP {r.status_code}")
    if r.status_code != 200:
        die(f"baseline failed: body={(r.text or '')[:300]!r}")
    rows = r.json()
    summarize(rows, [3])
    if isinstance(rows, list) and rows:
        print_row_essentials(rows[0], 3)

    time.sleep(0.6)

    # ----- 2) Two-branch test -----
    hr("2. TWO branches — {'branches': [3, 8]}")
    r = fetch_status(token, [3, 8])
    print(f"  HTTP {r.status_code}")
    if r.status_code != 200:
        print(f"  body[:500]: {(r.text or '')[:500]!r}")
        die("multi-branch [3,8] did not return 200 — stopping per safety rules.")
    rows = r.json()
    summarize(rows, [3, 8])
    if isinstance(rows, list):
        for row in rows:
            bid = row.get("branchId") or row.get("branch_id") or row.get("branch") or row.get("id")
            print_row_essentials(row, bid)
        print(f"\n  RAW first row sample (full keys):")
        if rows:
            print(json.dumps(rows[0], ensure_ascii=False, default=str)[:800])

    time.sleep(0.6)

    # ----- 3) Four-branch test -----
    hr("3. FOUR branches — {'branches': [3, 8, 9, 13]}")
    r = fetch_status(token, [3, 8, 9, 13])
    print(f"  HTTP {r.status_code}")
    if r.status_code != 200:
        print(f"  body[:500]: {(r.text or '')[:500]!r}")
        die("4-branch payload did not return 200 — stopping per safety rules.")
    rows = r.json()
    summarize(rows, [3, 8, 9, 13])
    if isinstance(rows, list):
        for row in rows:
            bid = row.get("branchId") or row.get("branch_id") or row.get("branch") or row.get("id")
            print_row_essentials(row, bid)

    hr("CONFIRMATION")
    print("  Calls made: POST /account/login (auth) + 3× POST :65010/raw/status/plain (read-only status).")
    print("  No report submission. No writes. No DB touched.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

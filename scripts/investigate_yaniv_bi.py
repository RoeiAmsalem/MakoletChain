"""READ-ONLY investigation of the Aviv BI API under a chain-wide account.

Maps everything we can READ from one account — branches, report catalog, and
filter metadata for key reports (902 Z, 301 attendance). Submits nothing.

Credentials come from env vars:
  YANIV_AVIV_USER, YANIV_AVIV_PASS

Safety:
- Only GET endpoints + login/refresh (auth). NO POST to /reports/result/.
- Prints findings; never writes the DB; never logs the password.
- 0.4s sleep between calls to avoid hammering the account.
"""
import json
import os
import sys
import time
from collections import Counter

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = "https://bi1.aviv-pos.co.il:8443/avivbi/v2"
STATUS_BASE = "https://bi1.aviv-pos.co.il:65010"
SLEEP = 0.4
TIMEOUT = 20


def die(msg: str) -> None:
    print(f"STOP: {msg}", file=sys.stderr)
    sys.exit(2)


def login(user: str, password: str) -> dict:
    r = requests.post(
        f"{BASE}/account/login",
        json={"user": user, "password": password},
        timeout=15, verify=False,
    )
    r.raise_for_status()
    return r.json()


def refresh(token: str) -> str:
    time.sleep(0.3)
    r = requests.post(
        f"{BASE}/account/refresh",
        headers={"Authtoken": token, "Content-Type": "application/json"},
        json={}, timeout=10, verify=False,
    )
    j = r.json() if r.content else {}
    return j.get("token") or j.get("value") or token


def get(path_or_url: str, token: str):
    url = path_or_url if path_or_url.startswith("http") else f"{BASE}{path_or_url}"
    time.sleep(SLEEP)
    return requests.get(url, headers={"Authtoken": token}, timeout=TIMEOUT, verify=False)


def hr(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def parse_z_list(filters_json) -> tuple[int, str | None, int | None]:
    """Return (count, latest_label_date, latest_z) from filters/902 response."""
    import re
    label_re = re.compile(r"Z:\s*(\d+)\s*\|\s*(\d{1,2}/\d{1,2}/\d{2,4})")
    entries: list[tuple[int, str]] = []

    def visit(node):
        if isinstance(node, dict):
            if len(node) == 1:
                (_, v), = node.items()
                if isinstance(v, str):
                    m = label_re.search(v)
                    if m:
                        entries.append((int(m.group(1)), m.group(2)))
            for c in node.values():
                visit(c)
        elif isinstance(node, list):
            for it in node:
                visit(it)

    visit(filters_json)
    if not entries:
        return 0, None, None
    entries.sort(key=lambda t: t[0], reverse=True)
    return len(entries), entries[0][1], entries[0][0]


def main() -> int:
    user = os.environ.get("YANIV_AVIV_USER")
    pw = os.environ.get("YANIV_AVIV_PASS")
    if not user or not pw:
        die("YANIV_AVIV_USER / YANIV_AVIV_PASS must be set in the env.")

    # ---- AUTH ------------------------------------------------------------
    hr("1. AUTH")
    print(f"  user: {user}  (password redacted)")
    try:
        data = login(user, pw)
    except Exception as e:
        die(f"login failed: {e}")
    token = data.get("token") or data.get("value")
    branches = data.get("branches", []) or []
    print(f"  token returned? {bool(token)}  (length={len(token) if token else 0})")
    print(f"  branches in login response: {len(branches)}")
    other_keys = [k for k in data.keys() if k not in ("token", "value", "branches")]
    if other_keys:
        # Show top-level metadata keys (but not values; could include secrets).
        print(f"  other top-level keys in login response: {other_keys}")
    token = refresh(token)
    print(f"  token refreshed OK")

    # ---- BRANCHES --------------------------------------------------------
    hr("2. BRANCHES")
    print(f"  total: {len(branches)}")
    # Header for table.
    print(f"  {'id':<6}{'name':<28}{'address':<28}{'startHour':>10}{'finishHour':>12}")
    print(f"  {'-'*6}{'-'*28}{'-'*28}{'-'*10}{'-'*12}")
    sorted_branches = sorted(branches, key=lambda b: b.get("id", 0))
    seen_extra_keys: set[str] = set()
    for b in sorted_branches:
        bid = b.get("id")
        name = (b.get("name") or "")[:26]
        addr = (b.get("address") or "")[:26]
        sh = b.get("startHour", "")
        fh = b.get("finishHour", "")
        print(f"  {bid!s:<6}{name:<28}{addr:<28}{sh!s:>10}{fh!s:>12}")
        seen_extra_keys.update(k for k in b.keys()
                               if k not in ("id", "name", "address", "startHour", "finishHour"))
    if seen_extra_keys:
        print(f"\n  extra keys ever seen on branch objects: {sorted(seen_extra_keys)}")

    if not branches:
        die("no branches visible to this account — cannot continue.")

    # ---- REPORTS CATALOG -------------------------------------------------
    hr("3. REPORTS CATALOG (per sampled branch)")
    # Sample across id range.
    by_id = {b["id"]: b for b in sorted_branches}
    ids = sorted(by_id.keys())
    sample_ids: list[int] = []
    for want in (3, 8):  # known stores
        if want in by_id:
            sample_ids.append(want)
    # Add 4 more spread across the id range.
    others = [i for i in ids if i not in sample_ids]
    if others:
        step = max(1, len(others) // 4)
        for i in range(0, len(others), step):
            if len(sample_ids) >= 6:
                break
            sample_ids.append(others[i])
    sample_ids = sorted(set(sample_ids))
    print(f"  sampling branches: {sample_ids}")

    catalogs: dict[int, list[tuple]] = {}
    for bid in sample_ids:
        r = get(f"/reports?branch={bid}", token)
        print(f"\n  branch {bid} ({by_id[bid].get('name')!r})  /reports -> HTTP {r.status_code}")
        if r.status_code != 200:
            print(f"    body[:200]: {(r.text or '')[:200]!r}")
            continue
        try:
            j = r.json()
        except Exception as e:
            print(f"    json parse failed: {e}")
            continue
        # Walk known shapes to collect (id, name) pairs.
        items: list[tuple] = []

        def walk(node, category=None):
            if isinstance(node, dict):
                rid = node.get("id")
                rname = node.get("name") or node.get("title")
                # Heuristic: report items have BOTH id and name and aren't categories.
                # We collect anything looking like an int id with a name.
                if isinstance(rid, int) and isinstance(rname, str):
                    items.append((rid, rname, category))
                # Some shapes use "reports" or "items" arrays for the children of a category.
                cat_name = node.get("categoryName") or node.get("groupName") or node.get("name")
                for k, v in node.items():
                    if k in ("reports", "items", "children", "list"):
                        walk(v, cat_name)
                    elif isinstance(v, (dict, list)):
                        walk(v, category)
            elif isinstance(node, list):
                for it in node:
                    walk(it, category)

        walk(j)
        # Dedup by (id, name).
        seen = set()
        uniq = []
        for rid, rname, cat in items:
            key = (rid, rname)
            if key in seen:
                continue
            seen.add(key)
            uniq.append((rid, rname, cat))
        uniq.sort(key=lambda t: t[0])
        catalogs[bid] = uniq
        print(f"    catalog size: {len(uniq)} entries")
        # Print first 30 entries to keep output reasonable.
        for rid, rname, cat in uniq[:30]:
            cat_s = f"  [{cat}]" if cat else ""
            print(f"    {rid:>6}  {rname}{cat_s}")
        if len(uniq) > 30:
            print(f"    ... ({len(uniq)-30} more)")

    # Cross-branch comparison.
    if len(catalogs) > 1:
        print("\n  cross-branch comparison (does every sampled branch see the same catalog?):")
        sets = {bid: frozenset((rid, rname) for rid, rname, _ in entries)
                for bid, entries in catalogs.items()}
        ref_bid, ref_set = next(iter(sets.items()))
        identical = all(s == ref_set for s in sets.values())
        print(f"    identical across {len(sets)} sampled branches? {identical}")
        if not identical:
            # Show what each branch has that ref doesn't, and vice versa.
            for bid, s in sets.items():
                if bid == ref_bid:
                    continue
                only_here = sorted(s - ref_set)
                only_ref = sorted(ref_set - s)
                if only_here or only_ref:
                    print(f"    branch {bid}: +{len(only_here)} unique vs ref, -{len(only_ref)} missing vs ref")

    # ---- 902 + 301 FILTER SHAPES (read-only metadata) --------------------
    hr("4. KEY REPORTS — filter metadata only (NO submission)")

    print("\n  902 (Z report) — does EVERY sampled branch return a Z-list?")
    print(f"  {'branch':<10}{'http':<6}{'Z count':>10}  latest Z / date")
    print(f"  {'-'*10}{'-'*6}{'-'*10}  {'-'*30}")
    for bid in sample_ids:
        r = get(f"/reports/filters/902?branch={bid}", token)
        if r.status_code != 200:
            print(f"  {bid!s:<10}{r.status_code:<6}{'-':>10}  body={(r.text or '')[:60]!r}")
            continue
        try:
            j = r.json()
        except Exception:
            print(f"  {bid!s:<10}{r.status_code:<6}{'-':>10}  (non-JSON)")
            continue
        n, latest_date, latest_z = parse_z_list(j)
        latest_s = f"z={latest_z} / {latest_date}" if latest_z else "(no Z labels parsed)"
        print(f"  {bid!s:<10}{r.status_code:<6}{n:>10}  {latest_s}")

    print("\n  301 (attendance / employer report) — filter shape:")
    for bid in sample_ids[:2]:
        r = get(f"/reports/filters/301?branch={bid}", token)
        print(f"    branch {bid}  HTTP {r.status_code}")
        if r.status_code == 200:
            try:
                j = r.json()
                # Strip down to top-level filter list with id + name + type.
                if isinstance(j, list):
                    print(f"      filters returned: {len(j)}")
                    for filt in j[:8]:
                        if isinstance(filt, dict):
                            keys_meta = {k: filt.get(k) for k in
                                         ("id", "name", "filterType", "type", "defaultValue")
                                         if k in filt}
                            print(f"      - {keys_meta}")
                else:
                    print(f"      shape: {type(j).__name__}")
            except Exception as e:
                print(f"      json parse failed: {e}")

    # ---- USEFUL REPORTS FROM CATALOG ------------------------------------
    hr("5. CANDIDATE USEFUL REPORTS (from catalog, name-based heuristics)")
    if catalogs:
        # Pick a reference branch (first sampled) and find financial-looking reports.
        ref_bid = sample_ids[0]
        ref = catalogs.get(ref_bid, [])
        keywords = [
            ("Z", ["Z", "ז"]),  # Z-report
            ("attendance/employees", ["נוכחות", "עובד", "שכר", "שעות"]),
            ("sales/revenue", ["מכירות", "הכנסה", "מחזור"]),
            ("payments/breakdown", ["תשלום", "אשראי", "מזומן", "הקפה"]),
            ("departments/items", ["מחלקה", "פריט", "מוצר", "ספק"]),
            ("inventory", ["מלאי", "ספירה"]),
            ("hourly/by-hour", ["שעה", "שעתי", "שעות פתיחה"]),
        ]
        for label, kws in keywords:
            matched = [(rid, rname, cat) for rid, rname, cat in ref
                       if any(kw in rname for kw in kws)]
            if matched:
                print(f"\n  {label}:")
                for rid, rname, cat in matched:
                    cat_s = f"  [{cat}]" if cat else ""
                    print(f"    {rid:>6}  {rname}{cat_s}")

    # ---- OTHER READ ENDPOINTS -------------------------------------------
    hr("6. OTHER READ ENDPOINTS")
    print(f"  noting (NOT calling unless clearly read-only):")
    print(f"  - {BASE}/account/branches        — POST, returns branches (we got them in /login)")
    print(f"  - {BASE}/dashboard/query         — POST SQL-like queries (read-only by use, but POST: SKIP this pass)")
    print(f"  - {BASE}/raw/deals/list          — POST list receipts (read-only by use, but POST: SKIP)")
    print(f"  - {STATUS_BASE}/raw/status/plain — GET live shift status, already used by aviv_live; safe.")

    print("\n  live status endpoint (read-only GET, already wired in prod):")
    try:
        time.sleep(SLEEP)
        r = requests.get(f"{STATUS_BASE}/raw/status/plain",
                         headers={"Authtoken": token}, timeout=10, verify=False)
        print(f"    HTTP {r.status_code}  body length={len(r.text or '')}")
        # Print only the first line or two so we don't dump full status.
        if r.status_code == 200 and r.text:
            head = r.text.splitlines()[:3]
            for line in head:
                # Avoid dumping anything sensitive — show first 80 chars only.
                print(f"    | {line[:80]}")
    except Exception as e:
        print(f"    skipped: {e}")

    # ---- FINAL ATTESTATION ----------------------------------------------
    hr("CONFIRMATION")
    print("  NOTHING was submitted, generated, or written.")
    print("  Calls made: POST /account/login, POST /account/refresh (auth only),")
    print("              GET /reports?branch=...,")
    print("              GET /reports/filters/902?branch=...,")
    print("              GET /reports/filters/301?branch=...,")
    print("              GET :65010/raw/status/plain.")
    print("  No POST to /reports/result/. No mutating endpoints touched. No DB writes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

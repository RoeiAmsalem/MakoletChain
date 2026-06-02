#!/usr/bin/env python3
"""Before/after harness for the electricity overlap double-count fix.

Reimplements the OLD proration (sum every intersecting invoice) and the NEW
per-day shortest-span dedup, reads the DB directly (no Flask import), and prints
before/after totals for a set of (branch, year, month) cases.

Usage: python3 scripts/verify_electricity_overlap.py [path-to-db]
Default DB: db/makolet_chain.db
"""
import calendar
import json
import sqlite3
import sys
from datetime import date, timedelta


def _load(db, branch_id, year, month):
    month_start = date(year, month, 1)
    month_end = date(year, month, calendar.monthrange(year, month)[1])
    rows = db.execute(
        "SELECT id, amount, raw_json FROM electricity_invoices WHERE branch_id = ?",
        (branch_id,),
    ).fetchall()
    cands = []  # (from_d, to_d, span, amount, id)
    for r in rows:
        try:
            rj = json.loads(r["raw_json"])
        except (json.JSONDecodeError, TypeError):
            continue
        fs, ts = rj.get("from_date", ""), rj.get("to_date", "")
        if not fs or not ts:
            continue
        fd, td = date.fromisoformat(fs[:10]), date.fromisoformat(ts[:10])
        span = (td - fd).days
        if span <= 0 or span > 90:
            continue
        if td < month_start or fd > month_end:
            continue
        cands.append((fd, td, span, r["amount"], r["id"]))
    return cands, month_start, month_end


def old_total(db, branch_id, year, month):
    cands, ms, me = _load(db, branch_id, year, month)
    total = 0.0
    for fd, td, span, amount, _ in cands:
        ov_start, ov_end = max(fd, ms), min(td, me)
        ov_days = (ov_end - ov_start).days + 1
        if ov_days <= 0:
            continue
        total += amount * ov_days / span  # span = (to-from).days, matches old code
    return round(total, 2)


def new_total(db, branch_id, year, month):
    cands, ms, me = _load(db, branch_id, year, month)
    if not cands:
        return 0.0
    assigned = {}
    day = ms
    while day <= me:
        covering = [c for c in cands if c[0] <= day <= c[1]]
        if covering:
            w = min(covering, key=lambda c: (c[2], -c[0].toordinal(), -c[4]))
            assigned[w[4]] = assigned.get(w[4], 0) + 1
        day += timedelta(days=1)
    by_id = {c[4]: c for c in cands}
    total = 0.0
    for inv_id, days in assigned.items():
        _, _, span, amount, _ = by_id[inv_id]
        total += amount * days / span
    return round(total, 2)


CASES = [
    ("9009 Jun 2025 (overlap)", 9009, 2025, 6),
    ("9009 Jul 2025 (1636 only)", 9009, 2025, 7),
    ("9009 Jun 2026 (estimate basis)", 9009, 2026, 6),  # estimate path -> last yr Jun
    ("9009 May 2025 (boundary)", 9009, 2025, 5),
    ("126 normal months (each)", 126, None, None),  # expanded below
    ("9017 normal months (each)", 9017, None, None),
]


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "db/makolet_chain.db"
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row

    print(f"DB: {path}\n")
    print(f"{'case':<34}{'OLD':>12}{'NEW':>12}{'Δ':>12}")
    print("-" * 70)

    def line(label, bid, y, m):
        o, n = old_total(db, bid, y, m), new_total(db, bid, y, m)
        d = round(n - o, 2)
        flag = "  <-- changed" if abs(d) > 0.01 else ""
        print(f"{label:<34}{o:>12}{n:>12}{d:>12}{flag}")

    # explicit 9009 cases
    for label, bid, y, m in CASES[:4]:
        line(label, bid, y, m)

    # all months with data for two normal branches (single-bill sanity)
    for bid in (126, 9017):
        months = db.execute(
            """SELECT DISTINCT json_extract(raw_json,'$.from_date') fd
               FROM electricity_invoices WHERE branch_id=? AND fd IS NOT NULL
               ORDER BY fd""",
            (bid,),
        ).fetchall()
        seen = set()
        for row in months:
            d0 = date.fromisoformat(row["fd"][:10])
            for delta in (0, 1):  # the start month and the month it bleeds into
                y, m = (d0.year, d0.month) if delta == 0 else (
                    d0.year + (d0.month == 12), (d0.month % 12) + 1)
                if (y, m) in seen:
                    continue
                seen.add((y, m))
                line(f"  {bid} {y}-{m:02d}", bid, y, m)


if __name__ == "__main__":
    main()

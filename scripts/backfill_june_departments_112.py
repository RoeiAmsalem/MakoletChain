"""One-time June-2026 department gap-fill from Aviv report 112.

GAP-FILL ONLY. For each active branch and each calendar day in June 2026, this
script INSERTS z_department_sales rows ONLY for (branch, date) combinations that
currently have NO department rows at all. It NEVER updates or overwrites an
existing row — any store/day that already has dept data is left completely
untouched. This is how stores the 902 section missed (9018/9019/9016) and any
day before the 112 cutover get backfilled without disturbing what's already there.

Default is DRY-RUN: it prints, per branch and per date, how many rows it WOULD
insert plus a small sample, and writes nothing. Pass --apply to actually write.

Branch 9011 (ויצמן) is skipped — report 112 404s there; it's left to the Aviv
agent + the existing 902 backfill.

Usage (staging):
  venv/bin/python scripts/backfill_june_departments_112.py            # dry-run
  venv/bin/python scripts/backfill_june_departments_112.py --apply    # write
  venv/bin/python scripts/backfill_june_departments_112.py --branch-id 127
"""
import argparse
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import agents.aviv_z_report as zr  # noqa: E402

IL_TZ = ZoneInfo('Asia/Jerusalem')
DB_PATH = zr.DB_PATH

# Report 112 404s for this branch — leave it to the Aviv agent + 902 backfill.
SKIP_LOCAL_BRANCH_IDS = {9011}

MONTH = '2026-06'


def _june_days() -> list[str]:
    """June-2026 calendar days from the 1st through today (IL), capped at the
    30th. Future days have no data, so there's nothing to pull for them."""
    today = datetime.now(IL_TZ).date()
    start = date(2026, 6, 1)
    end = date(2026, 6, 30)
    if today < end:
        end = today
    if end < start:
        return []
    days = []
    d = start
    while d <= end:
        days.append(d.isoformat())
        d += timedelta(days=1)
    return days


def _has_dept_rows(conn, branch_id: int, day: str) -> bool:
    row = conn.execute(
        'SELECT 1 FROM z_department_sales WHERE branch_id=? AND date=? LIMIT 1',
        (branch_id, day)).fetchone()
    return row is not None


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--apply', action='store_true',
                    help='Actually INSERT rows. Default is dry-run (no writes).')
    ap.add_argument('--branch-id', type=int,
                    help='Limit to a single local branch id (for spot checks).')
    args = ap.parse_args()

    mode = 'APPLY (writing)' if args.apply else 'DRY-RUN (no writes)'
    days = _june_days()
    print(f'June-2026 dept gap-fill from report 112 — {mode}')
    print(f'days: {days[0]}..{days[-1]} ({len(days)} day(s))' if days
          else 'no June days to process')

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    where = 'active=1 AND aviv_branch_id IS NOT NULL'
    params: tuple = ()
    if args.branch_id is not None:
        where += ' AND id=?'
        params = (args.branch_id,)
    branches = conn.execute(
        f'SELECT id, name, aviv_branch_id FROM branches WHERE {where} ORDER BY id',
        params).fetchall()

    try:
        token = zr._refresh(zr._login_chain_account())
    except Exception as e:
        print(f'FATAL: chain login failed: {e}')
        sys.exit(2)

    total_skipped_existing = 0   # (branch,date) already had dept rows
    total_filled = 0             # (branch,date) we (would) insert for
    total_rows = 0               # dept rows (would be) inserted
    total_empty = 0              # (branch,date) 112 returned nothing

    for b in branches:
        local_id = b['id']
        if local_id in SKIP_LOCAL_BRANCH_IDS:
            print(f'\n[branch {local_id} {b["name"]}] SKIP (report 112 unsupported)')
            continue
        aviv_id = b['aviv_branch_id']
        if aviv_id in zr.EXCLUDED_CHAIN_AVIV_IDS:
            continue

        b_skipped = b_filled = b_rows = b_empty = 0
        lines: list[str] = []
        for day in days:
            # GUARD: never touch a (branch,date) that already has dept rows.
            if _has_dept_rows(conn, local_id, day):
                b_skipped += 1
                continue
            try:
                departments = zr.fetch_112_departments(aviv_id, day, token)
            except zr.AuthExpired:
                token = zr._refresh(zr._login_chain_account())
                try:
                    departments = zr.fetch_112_departments(aviv_id, day, token)
                except Exception as e:
                    lines.append(f'    {day}: ERROR after re-auth: {str(e)[:120]}')
                    continue
            except Exception as e:
                lines.append(f'    {day}: ERROR: {str(e)[:120]}')
                continue

            if not departments:
                b_empty += 1
                continue

            b_filled += 1
            b_rows += len(departments)
            sample = departments[0]
            lines.append(
                f'    {day}: {"INSERT" if args.apply else "would insert"} '
                f'{len(departments)} rows '
                f'(e.g. {sample["dept_code"]} {sample["dept_name"]} '
                f'= {sample["sale_incl_vat"]})')

            if args.apply:
                # Safe: guard above proved zero existing rows, so this is a
                # pure insert (upsert's DELETE is a no-op on an empty key).
                zr.upsert_department_sales(conn, local_id, day, departments)

        print(f'\n[branch {local_id} {b["name"]}] '
              f'filled={b_filled} rows={b_rows} '
              f'skipped_existing={b_skipped} empty={b_empty}')
        for ln in lines:
            print(ln)

        total_skipped_existing += b_skipped
        total_filled += b_filled
        total_rows += b_rows
        total_empty += b_empty

    conn.close()
    print(f'\n=== TOTAL — {mode} ===')
    print(f'branch/date filled        : {total_filled}')
    print(f'dept rows {"inserted" if args.apply else "to insert"}      : {total_rows}')
    print(f'branch/date skipped (data): {total_skipped_existing}')
    print(f'branch/date empty (no 112): {total_empty}')
    if not args.apply:
        print('\nDRY-RUN — nothing written. Re-run with --apply to insert.')


if __name__ == '__main__':
    main()

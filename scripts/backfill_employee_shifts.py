"""Backfill employee_shifts by re-pulling the Aviv employer report (301).

WHEN TO USE: an employee was added via the pending-match UI AFTER the report
was first pulled, so they have employee_hours but ZERO employee_shifts (the
pending-promote path writes aggregate hours only — shift rows are written only
on the agent's matched path at parse time). The nightly runs self-heal the
CURRENT + PREVIOUS month automatically; use this ONLY for months OLDER than
that window (a mid-history backfill).

Re-pulling under the now-existing employee cards full-overwrites employee_shifts
per (branch, month, 'aviv_report') and runs the migration-023 regular/OT/Shabbat
classification. employee_hours.total_hours (the sole salary input) is unchanged.

Chain auth (same as nightly); notify_anomalies=False — no brrr spam.

USAGE (from the app dir, venv active + .env sourced):
    python3 scripts/backfill_employee_shifts.py --branch-id 9009 2026-04 2026-05 2026-06
"""
import argparse
import calendar
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from agents.aviv_employees_report import (  # noqa: E402
    run_for_branch, _login_chain_account, _refresh, USE_CHAIN_AUTH,
)


def _last_day(month_str: str) -> date:
    """YYYY-MM -> date of the last day of that month."""
    y, m = (int(p) for p in month_str.split('-'))
    return date(y, m, calendar.monthrange(y, m)[1])


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--branch-id', type=int, required=True)
    ap.add_argument('months', nargs='+', help='one or more YYYY-MM to backfill')
    args = ap.parse_args()

    assert USE_CHAIN_AUTH, (
        "AVIV_EMP_USE_CHAIN must be 1 — chain branches (e.g. 9009) have no "
        "per-store Aviv creds, only aviv_branch_id; this run requires chain auth."
    )
    token = _refresh(_login_chain_account())

    # One run per month: set `today` to the last day of the target month so the
    # current-month window resolves to the FULL month, then pull current-only.
    for month_str in args.months:
        res = run_for_branch(
            args.branch_id, today=_last_day(month_str),
            include_current_month=True, include_previous_month=False,
            chain_token=token, notify_anomalies=False)
        print(f"{month_str}: {res}")


if __name__ == '__main__':
    main()

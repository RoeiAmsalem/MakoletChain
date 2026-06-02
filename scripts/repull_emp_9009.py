"""One-off: re-pull Aviv employer report (301) for branch 9009 (שבטי ישראל)
for 2026-04, 2026-05, 2026-06.

Why: איה נוריאל + גיא בלקר were added via the pending-match UI AFTER the report
was first pulled. api_pending_add_new promotes aggregate hours into
employee_hours but never writes per-shift rows — shifts are only written on the
agent's matched path at parse time. So they have hours but ZERO employee_shifts.
Re-pulling under the now-existing employee cards full-overwrites employee_shifts
(+ migration-023 regular/OT/Shabbat buckets) and links them by canonical name.

Chain auth (same as nightly). notify_anomalies=False — manual run, no brrr spam.
employee_hours.total_hours (the sole salary input) is unchanged by design;
this only (re)writes the display-only shift drill-down.
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from agents.aviv_employees_report import (  # noqa: E402
    run_for_branch, _login_chain_account, _refresh, USE_CHAIN_AUTH,
)

BRANCH = 9009


def main():
    assert USE_CHAIN_AUTH, (
        "AVIV_EMP_USE_CHAIN must be 1 — branch 9009 has no per-store Aviv creds, "
        "only aviv_branch_id; this run requires chain auth."
    )
    token = _refresh(_login_chain_account())

    # June 2026 (current) + May 2026 (previous), relative to today = 2026-06-02.
    r1 = run_for_branch(BRANCH, include_previous_month=True,
                        today=date(2026, 6, 2), chain_token=token,
                        notify_anomalies=False)
    print("JUN+MAY:", r1)

    # April 2026 only — shift `today` into mid-May so the previous-month window
    # resolves to April, and skip the current (May) month (already done above).
    r2 = run_for_branch(BRANCH, include_previous_month=True,
                        include_current_month=False, today=date(2026, 5, 15),
                        chain_token=token, notify_anomalies=False)
    print("APR:", r2)


if __name__ == '__main__':
    main()

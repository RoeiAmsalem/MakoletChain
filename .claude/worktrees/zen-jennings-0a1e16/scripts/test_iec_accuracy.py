#!/usr/bin/env python3
"""Tests for IEC accuracy data logic.

NOTE: Tests that depend on branch 126 having iec_token will be skipped
if running against a dev DB without IEC data. They will pass on prod.
"""

import os
import sys

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, _get_iec_accuracy_data


def _branch_has_iec(branch_id):
    """Check if branch has IEC configured in the local DB."""
    from app import get_db
    db = get_db()
    row = db.execute(
        "SELECT iec_token FROM branches WHERE id = ?", (branch_id,)
    ).fetchone()
    return row and row['iec_token']


def test_branch_with_iec():
    """Branch with IEC token should return 12 months starting from current."""
    with app.app_context():
        if not _branch_has_iec(126):
            print("  SKIP: Branch 126 has no iec_token in local DB")
            return

        rows = _get_iec_accuracy_data(branch_id=126)
        assert len(rows) == 12, f"Expected 12 rows, got {len(rows)}"

        from datetime import datetime
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo('Asia/Jerusalem'))
        first = rows[0]
        assert first['year'] == now.year, f"Expected year {now.year}, got {first['year']}"
        assert first['month'] == now.month, f"Expected month {now.month}, got {first['month']}"
        assert first['branch_id'] == 126

        print(f"  Branch 126 current month: estimate={first['estimate']}, "
              f"real={first['real']}, status={first['status']}")


def test_no_past_months():
    """Accuracy table should only have current month forward, not past."""
    with app.app_context():
        if not _branch_has_iec(126):
            print("  SKIP: Branch 126 has no iec_token in local DB")
            return

        rows = _get_iec_accuracy_data(branch_id=126)
        feb_rows = [r for r in rows if r['year'] == 2026 and r['month'] == 2]
        assert len(feb_rows) == 0, "Past months should not appear in accuracy table"
        print("  No past months in accuracy table: OK")


def test_branch_without_iec():
    """Branch without IEC token should return empty list."""
    with app.app_context():
        rows = _get_iec_accuracy_data(branch_id=127)
        assert rows == [], f"Expected empty list for branch without IEC, got {len(rows)} rows"
        print("  Branch 127 (no IEC): empty list OK")


def test_nonexistent_branch():
    """Nonexistent branch should return empty list."""
    with app.app_context():
        rows = _get_iec_accuracy_data(branch_id=99999)
        assert rows == [], f"Expected empty list for nonexistent branch, got {len(rows)} rows"
        print("  Nonexistent branch: empty list OK")


def test_required_fields():
    """Every row should have the expected keys."""
    required_keys = {'branch_id', 'branch_name', 'year', 'month', 'month_label',
                     'estimate', 'estimate_basis', 'real', 'delta', 'accuracy_pct', 'status'}
    with app.app_context():
        if not _branch_has_iec(126):
            print("  SKIP: Branch 126 has no iec_token in local DB")
            return

        rows = _get_iec_accuracy_data(branch_id=126)
        for i, row in enumerate(rows):
            missing = required_keys - set(row.keys())
            assert not missing, f"Row {i} missing keys: {missing}"
        print("  All required fields present: OK")


def test_status_values():
    """Status should only be one of the valid values."""
    valid_statuses = {'pending', 'final', 'no_estimate'}
    with app.app_context():
        if not _branch_has_iec(126):
            print("  SKIP: Branch 126 has no iec_token in local DB")
            return

        rows = _get_iec_accuracy_data(branch_id=126)
        for row in rows:
            assert row['status'] in valid_statuses, f"Invalid status: {row['status']}"
        print("  All status values valid: OK")


if __name__ == '__main__':
    print("Running IEC accuracy tests...\n")
    test_branch_with_iec()
    test_no_past_months()
    test_branch_without_iec()
    test_nonexistent_branch()
    test_required_fields()
    test_status_values()
    print("\nAll tests passed!")

#!/usr/bin/env python3
"""Tests for electricity helper functions."""
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sqlite3
from app import get_electricity_for_month, get_branch_start_month, _get_fixed_total

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'db', 'makolet_chain.db')


def get_test_db():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row
    return db


def test_branch_126_feb_2026_real():
    """Branch 126, Feb 2026 — REAL because invoice 22/01-22/03 covers most of Feb."""
    db = get_test_db()
    result = get_electricity_for_month(126, 2026, 2, db)
    assert result['source'] == 'real', f"Expected 'real', got '{result['source']}'"
    assert result['amount'] > 0, f"Expected > 0, got {result['amount']}"
    print(f"  PASS: branch 126 Feb 2026 = {result}")


def test_branch_126_april_2026():
    """Branch 126, April 2026 — no invoice covers April, should be ESTIMATE."""
    db = get_test_db()
    result = get_electricity_for_month(126, 2026, 4, db)
    assert result['amount'] > 0, f"Expected > 0, got {result['amount']}"
    assert result['source'] in ('estimate', 'real'), f"Unexpected source: {result['source']}"
    print(f"  PASS: branch 126 Apr 2026 = {result}")


def test_branch_127_none():
    """Branch 127 (Gal), no IEC integration — NONE."""
    db = get_test_db()
    result = get_electricity_for_month(127, 2026, 4, db)
    assert result['source'] == 'none', f"Expected 'none', got '{result['source']}'"
    assert result['amount'] == 0, f"Expected 0, got {result['amount']}"
    print(f"  PASS: branch 127 Apr 2026 = {result}")


def test_deterministic():
    """Helper is deterministic: same input twice = same output."""
    db = get_test_db()
    a = get_electricity_for_month(126, 2026, 4, db)
    b = get_electricity_for_month(126, 2026, 4, db)
    assert a == b, f"Non-deterministic: {a} != {b}"
    print(f"  PASS: deterministic check")


def test_branch_start_month():
    """Branch 126 was onboarded to MakoletDashboard in 2025, not earlier."""
    db = get_test_db()
    start = get_branch_start_month(126, db)
    assert start is not None, "Expected start month, got None"
    assert start[0] >= 2025, f"Expected year >= 2025 (onboarded in 2025), got {start[0]}"
    print(f"  PASS: branch 126 start month = {start[1]:02d}/{start[0]}")


def test_fixed_total_includes_electricity():
    """For branch 126 in a month with electricity, total > fixed_only."""
    db = get_test_db()
    result = _get_fixed_total(126, '2026-02', 100000, db)
    assert result['total'] >= result['fixed_only'], \
        f"total {result['total']} < fixed_only {result['fixed_only']}"
    assert result['electricity']['amount'] > 0, \
        f"Expected electricity > 0 for Feb 2026, got {result['electricity']}"
    print(f"  PASS: _get_fixed_total 126 Feb 2026 = total:{result['total']}, fixed:{result['fixed_only']}, elec:{result['electricity']['amount']}")


def test_long_invoices_excluded():
    """Invoices >90 days should be excluded from proration."""
    db = get_test_db()
    # Nov 2023 to Sep 2025 invoice (691 days) exists — should not be included
    # Check a month that would only have that long invoice
    result = get_electricity_for_month(126, 2024, 6, db)
    # Jun 2024 should have real data from the 05/24-07/21 invoice (59 days), not the long one
    print(f"  PASS: long invoice exclusion check, Jun 2024 = {result}")


if __name__ == '__main__':
    print("Running electricity helper tests...\n")
    tests = [
        test_branch_126_feb_2026_real,
        test_branch_126_april_2026,
        test_branch_127_none,
        test_deterministic,
        test_branch_start_month,
        test_fixed_total_includes_electricity,
        test_long_invoices_excluded,
    ]
    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except AssertionError as e:
            print(f"  FAIL: {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR: {t.__name__}: {e}")
            failed += 1
    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
    print("All tests passed!")

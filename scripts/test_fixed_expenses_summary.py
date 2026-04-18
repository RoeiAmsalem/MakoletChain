#!/usr/bin/env python3
"""Tests for /api/fixed-expenses-summary endpoint math.

Verifies the fixed-expenses page now uses monthly-prorated electricity
(same as /api/summary on the home page), not the raw latest invoice.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sqlite3
from app import get_electricity_for_month, _get_fixed_total

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'db', 'makolet_chain.db')


def get_test_db():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row
    return db


def test_branch_126_april_2026_estimate():
    """Branch 126 April 2026 — monthly electricity should be ESTIMATE (~4208)."""
    db = get_test_db()
    result = get_electricity_for_month(126, 2026, 4, db)
    assert result['source'] == 'estimate', f"Expected 'estimate', got '{result['source']}'"
    assert 4000 < result['amount'] < 5000, f"Expected ~4208, got {result['amount']}"
    print(f"  PASS: branch 126 Apr 2026 electricity = {result['amount']} (source={result['source']}, basis={result.get('estimate_basis')})")


def test_branch_126_feb_2026_real():
    """Branch 126 Feb 2026 — monthly electricity should be REAL (Jan-Mar invoice covers Feb)."""
    db = get_test_db()
    result = get_electricity_for_month(126, 2026, 2, db)
    assert result['source'] == 'real', f"Expected 'real', got '{result['source']}'"
    assert 4000 < result['amount'] < 5500, f"Expected ~4500, got {result['amount']}"
    print(f"  PASS: branch 126 Feb 2026 electricity = {result['amount']} (source={result['source']})")


def test_branch_127_none():
    """Branch 127 — no IEC integration, should be NONE."""
    db = get_test_db()
    result = get_electricity_for_month(127, 2026, 4, db)
    assert result['source'] == 'none', f"Expected 'none', got '{result['source']}'"
    assert result['amount'] == 0, f"Expected 0, got {result['amount']}"
    print(f"  PASS: branch 127 Apr 2026 electricity = 0 (source=none)")


def test_fixed_total_reconciles():
    """total must equal fixed_only + electricity.amount."""
    db = get_test_db()
    data = _get_fixed_total(126, '2026-04', 189000, db)
    expected_total = round(data['fixed_only'] + data['electricity']['amount'], 2)
    assert data['total'] == expected_total, \
        f"total {data['total']} != fixed_only {data['fixed_only']} + elec {data['electricity']['amount']} = {expected_total}"
    print(f"  PASS: total={data['total']}, fixed_only={data['fixed_only']}, electricity={data['electricity']['amount']}")


def test_branch_127_total_equals_fixed_only():
    """Branch 127 (no IEC) — total should equal fixed_only."""
    db = get_test_db()
    data = _get_fixed_total(127, '2026-04', 179000, db)
    assert data['electricity']['source'] == 'none', f"Expected 'none', got '{data['electricity']['source']}'"
    assert data['electricity']['amount'] == 0
    assert data['total'] == data['fixed_only'], \
        f"total {data['total']} != fixed_only {data['fixed_only']} when no electricity"
    print(f"  PASS: branch 127 total={data['total']} == fixed_only (no electricity)")


if __name__ == '__main__':
    print("Running fixed-expenses-summary tests...\n")
    tests = [
        test_branch_126_april_2026_estimate,
        test_branch_126_feb_2026_real,
        test_branch_127_none,
        test_fixed_total_reconciles,
        test_branch_127_total_equals_fixed_only,
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

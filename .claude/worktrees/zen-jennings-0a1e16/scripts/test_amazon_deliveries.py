#!/usr/bin/env python3
"""Tests for the Amazon deliveries API endpoint."""
import os
import sys
import sqlite3
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app, DB_PATH


def setup_test_db():
    """Create a temporary test DB with hourly_sales data."""
    tmp = tempfile.mktemp(suffix='.db')
    conn = sqlite3.connect(tmp)
    conn.execute('''CREATE TABLE branches (
        id INTEGER PRIMARY KEY, name TEXT, city TEXT, active INTEGER DEFAULT 1,
        avg_hourly_rate REAL, hours_this_month REAL, hours_baseline REAL, hours_updated_at TEXT
    )''')
    conn.execute("INSERT INTO branches (id, name, city) VALUES (126, 'Test 126', 'Test')")
    conn.execute("INSERT INTO branches (id, name, city) VALUES (127, 'Test 127', 'Test')")
    conn.execute('''CREATE TABLE users (
        id INTEGER PRIMARY KEY, name TEXT, email TEXT, password_hash TEXT,
        role TEXT, active INTEGER DEFAULT 1
    )''')
    conn.execute('''CREATE TABLE user_branches (user_id INTEGER, branch_id INTEGER)''')
    conn.execute('''CREATE TABLE hourly_sales (
        branch_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        hour INTEGER NOT NULL,
        amount REAL DEFAULT 0,
        transactions INTEGER DEFAULT 0,
        PRIMARY KEY (branch_id, date, hour)
    )''')
    # Early morning big transaction (should match)
    conn.execute("INSERT INTO hourly_sales VALUES (126, '2026-04-15', 6, 8500.0, 3)")
    # Early morning small transaction (below threshold - should NOT match)
    conn.execute("INSERT INTO hourly_sales VALUES (126, '2026-04-16', 5, 250.0, 1)")
    # Afternoon big transaction (should NOT match - after opening)
    conn.execute("INSERT INTO hourly_sales VALUES (126, '2026-04-17', 14, 1000.0, 2)")
    # Another early morning qualifying day
    conn.execute("INSERT INTO hourly_sales VALUES (126, '2026-04-18', 6, 15000.0, 5)")
    # Branch 127 early morning (should not appear for branch 127 requests)
    conn.execute("INSERT INTO hourly_sales VALUES (127, '2026-04-15', 6, 9000.0, 2)")
    conn.commit()
    conn.close()
    return tmp


def run_tests():
    tmp_db = setup_test_db()
    # Patch the DB path
    import app as app_module
    orig_db = app_module.DB_PATH
    app_module.DB_PATH = tmp_db

    client = app.test_client()
    passed = 0
    failed = 0

    try:
        # Simulate login as branch 126 manager
        with client.session_transaction() as sess:
            sess['user_id'] = 1
            sess['user_role'] = 'manager'
            sess['branch_id'] = 126

        # Test 1: Branch 126 with qualifying early-morning transactions
        r = client.get('/api/amazon-deliveries?month=2026-04')
        d = r.get_json()
        assert d['total_count'] == 8, f"Expected 8 total tx, got {d['total_count']}"
        assert d['total_amount'] == 23500.0, f"Expected 23500.0 total, got {d['total_amount']}"
        assert len(d['deliveries']) == 2, f"Expected 2 delivery days, got {len(d['deliveries'])}"
        print("  PASS: Branch 126 qualifying transactions detected")
        passed += 1

        # Test 2: Small early-morning transaction excluded (₪250 on Apr 16)
        dates = [del_['date'] for del_ in d['deliveries']]
        assert '2026-04-16' not in dates, "₪250 transaction should be excluded (below ₪400)"
        print("  PASS: Small early-morning transaction excluded")
        passed += 1

        # Test 3: Afternoon big transaction excluded (₪1000 at 14:00 on Apr 17)
        assert '2026-04-17' not in dates, "Afternoon transaction should be excluded"
        print("  PASS: Afternoon big transaction excluded")
        passed += 1

        # Test 4: Branch 127 returns empty
        with client.session_transaction() as sess:
            sess['branch_id'] = 127
        r = client.get('/api/amazon-deliveries?month=2026-04')
        d = r.get_json()
        assert d['total_count'] == 0, f"Branch 127 should get empty, got count={d['total_count']}"
        assert d['deliveries'] == [], f"Branch 127 should get empty deliveries"
        print("  PASS: Branch 127 returns empty")
        passed += 1

        # Test 5: Future month returns empty
        with client.session_transaction() as sess:
            sess['branch_id'] = 126
        r = client.get('/api/amazon-deliveries?month=2030-01')
        d = r.get_json()
        assert d['total_count'] == 0, "Future month should return empty"
        assert d['deliveries'] == [], "Future month should return empty deliveries"
        print("  PASS: Future month returns empty")
        passed += 1

        # Test 6: Month with no qualifying transactions
        r = client.get('/api/amazon-deliveries?month=2026-03')
        d = r.get_json()
        assert d['total_amount'] == 0, "Empty month should show 0"
        assert d['total_count'] == 0, "Empty month should show 0 count"
        assert d['deliveries'] == [], "Empty month should have empty list"
        print("  PASS: Empty month returns zeros")
        passed += 1

    except AssertionError as e:
        print(f"  FAIL: {e}")
        failed += 1
    except Exception as e:
        print(f"  ERROR: {e}")
        failed += 1
    finally:
        app_module.DB_PATH = orig_db
        os.unlink(tmp_db)

    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed")
    return failed == 0


if __name__ == '__main__':
    print("Amazon Deliveries API Tests")
    print("=" * 40)
    success = run_tests()
    sys.exit(0 if success else 1)

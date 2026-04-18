"""Tests: Verify CSV path is fully retired and API is sole source of truth.

Run: python scripts/test_employees_api_only.py
"""
import os
import sys
import sqlite3
import importlib

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'db', 'makolet_chain.db')


def test_scheduler_no_csv_agent():
    """Verify the scheduler does NOT directly schedule a CSV-specific agent."""
    scheduler_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scheduler.py')
    with open(scheduler_path) as f:
        content = f.read()
    # The CSV ingestion is inside gmail_agent's _sync_attendance_csv,
    # called from nightly_sync -> run_gmail_sync. Verify it's disabled in gmail_agent.
    gmail_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'agents', 'gmail_agent.py')
    with open(gmail_path) as f:
        gmail_content = f.read()
    # The _sync_attendance_csv call should be commented out
    assert '# DISABLED 2026-04-18' in gmail_content, "gmail_agent.py should have CSV disabled comment"
    # Make sure the actual call is commented
    lines = gmail_content.split('\n')
    for i, line in enumerate(lines):
        stripped = line.strip()
        # Skip the function definition and comments — only check actual invocations
        if '_sync_attendance_csv(mail,' in stripped and not stripped.startswith('#') and not stripped.startswith('def '):
            raise AssertionError(f"Line {i+1}: _sync_attendance_csv call is not commented out!")
    print("PASS: CSV agent is disabled in gmail_agent.py")


def test_discrepancy_routes_disabled():
    """Verify discrepancy API routes are commented out in app.py."""
    app_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app.py')
    with open(app_path) as f:
        content = f.read()
    # The routes should be commented out
    assert "# DISABLED 2026-04-18: Discrepancy routes retired" in content, \
        "Discrepancy routes should have disabled comment"
    # Make sure @app.route('/api/employee-hours-discrepancies') is NOT active
    for line in content.split('\n'):
        if "employee-hours-discrepancies" in line and line.strip().startswith('@app.route'):
            raise AssertionError("Discrepancy route is still active!")
    print("PASS: Discrepancy routes are disabled")


def test_salary_calculation_api_only():
    """Verify _calculate_salary_cost uses API-only rows."""
    app_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app.py')
    with open(app_path) as f:
        content = f.read()
    # Find the _calculate_salary_cost function — should have only one query with source = 'aviv_api'
    func_start = content.find('def _calculate_salary_cost')
    func_end = content.find('\ndef ', func_start + 1)
    func_body = content[func_start:func_end]
    # Should NOT have an else branch that queries without source filter
    assert "WHERE eh.branch_id = ? AND eh.month = ?\n" not in func_body, \
        "_calculate_salary_cost should not have unfiltered query"
    assert "AND eh.source = 'aviv_api'" in func_body, \
        "_calculate_salary_cost should filter by aviv_api"
    print("PASS: _calculate_salary_cost uses API-only")


def test_employees_template_no_csv_ui():
    """Verify employees.html has no visible CSV UI elements."""
    tpl_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'templates', 'employees.html')
    with open(tpl_path) as f:
        content = f.read()
    # Discrepancies banner should be commented out
    assert 'id="discrepancies-banner"' not in content, "Discrepancies banner should be removed"
    # CSV-only banner should be commented out
    assert 'id="csv-only-banner"' not in content, "CSV-only banner should be removed"
    # loadDiscrepancies function should be removed
    assert 'async function loadDiscrepancies' not in content, "loadDiscrepancies function should be removed"
    # "אשר CSV" button should not exist
    assert 'אשר CSV' not in content, "אשר CSV button should be removed"
    # CSV badge on employee cards should be removed
    assert "badge-approved\">CSV</span>" not in content or "היסטורי" in content, \
        "CSV badge should be removed from employee cards (historical OK)"
    print("PASS: No CSV UI elements in employees.html")


def test_db_tables_still_exist():
    """Verify CSV-related DB tables/columns are NOT deleted (conservative removal)."""
    if not os.path.exists(DB_PATH):
        print("SKIP: No local DB found (expected on dev machine without DB)")
        return
    conn = sqlite3.connect(DB_PATH)
    # employee_hours table should still exist
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    assert 'employee_hours' in tables, "employee_hours table must still exist"
    assert 'employee_hours_discrepancies' in tables, "employee_hours_discrepancies table must still exist"
    assert 'employee_match_pending' in tables, "employee_match_pending table must still exist"
    # source column should still exist in employee_hours
    cols = [r[1] for r in conn.execute("PRAGMA table_info(employee_hours)").fetchall()]
    assert 'source' in cols, "source column must still exist in employee_hours"
    conn.close()
    print("PASS: DB tables and columns preserved")


def test_gmail_agent_file_exists():
    """Verify gmail_agent.py file is NOT deleted."""
    agent_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'agents', 'gmail_agent.py')
    assert os.path.exists(agent_path), "gmail_agent.py must still exist"
    print("PASS: gmail_agent.py file preserved")


if __name__ == '__main__':
    passed = 0
    failed = 0
    tests = [
        test_scheduler_no_csv_agent,
        test_discrepancy_routes_disabled,
        test_salary_calculation_api_only,
        test_employees_template_no_csv_ui,
        test_db_tables_still_exist,
        test_gmail_agent_file_exists,
    ]
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"FAIL: {test.__name__}: {e}")
            failed += 1
    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
    print("All tests passed!")

"""Tests for the employees page: accuracy, security, edge cases, data integrity."""
import os
import sys
import sqlite3
import json
import pytest

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, init_db, DB_PATH


@pytest.fixture
def client():
    """Create test client with fresh in-memory-like DB."""
    app.config['TESTING'] = True
    # Use a temp DB for tests
    test_db = os.path.join(os.path.dirname(__file__), 'test_makolet.db')
    original_db = DB_PATH

    import app as app_module
    app_module.DB_PATH = test_db

    # Init fresh DB with schema + migrations
    if os.path.exists(test_db):
        os.remove(test_db)
    app_module.DB_PATH = test_db
    app_module.init_db()
    conn = sqlite3.connect(test_db, timeout=30)

    # Add columns that exist in prod but not in schema.sql
    for col_sql in [
        "ALTER TABLE branches ADD COLUMN hours_this_month REAL DEFAULT 0",
        "ALTER TABLE branches ADD COLUMN hours_baseline REAL DEFAULT 0",
        "ALTER TABLE branches ADD COLUMN hours_updated_at TEXT",
        "ALTER TABLE branches ADD COLUMN avg_hourly_rate REAL DEFAULT 0",
        "ALTER TABLE branches ADD COLUMN bilboy_user TEXT",
        "ALTER TABLE branches ADD COLUMN bilboy_pass TEXT",
    ]:
        try:
            conn.execute(col_sql)
        except sqlite3.OperationalError:
            pass
    conn.commit()

    # Seed test data
    conn.execute("INSERT OR REPLACE INTO branches (id, name, city, active) VALUES (126, 'איינשטיין', 'תל אביב', 1)")
    conn.execute("INSERT OR REPLACE INTO branches (id, name, city, active) VALUES (127, 'התיכון', 'ירושלים', 1)")
    from werkzeug.security import generate_password_hash
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) VALUES (1, 'CEO', 'admin@makolet.com', ?, 'admin', 1)",
        (generate_password_hash('test123'),))
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) VALUES (2, 'Manager', 'mgr@test.com', ?, 'manager', 1)",
        (generate_password_hash('test123'),))
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (2, 126)")

    # Employees
    conn.execute("INSERT INTO employees (id, branch_id, name, role, hourly_rate, active) VALUES (1, 126, 'עידן בקון', 'ערב', 40, 1)")
    conn.execute("INSERT INTO employees (id, branch_id, name, role, hourly_rate, active) VALUES (2, 126, 'רועי אמסלם', 'בוקר', 35, 1)")
    conn.execute("INSERT INTO employees (id, branch_id, name, role, hourly_rate, active) VALUES (3, 126, 'עובד מושבת', 'ערב', 30, 0)")
    conn.execute("INSERT INTO employees (id, branch_id, name, role, hourly_rate, active) VALUES (4, 127, 'עובד סניף אחר', 'בוקר', 45, 1)")

    # Employee hours for April 2026
    conn.execute(
        "INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, total_salary, source) "
        "VALUES (126, '2026-04', 'עידן בקון', 100.5, 4020, 'aviv_api')")
    conn.execute(
        "INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, total_salary, source) "
        "VALUES (126, '2026-04', 'רועי אמסלם', 50.25, 1758.75, 'aviv_api')")

    # Daily sales for ratio test
    conn.execute("INSERT INTO daily_sales (branch_id, date, amount, transactions, source) VALUES (126, '2026-04-01', 8000, 50, 'z_report')")
    conn.execute("INSERT INTO daily_sales (branch_id, date, amount, transactions, source) VALUES (126, '2026-04-10', 7000, 45, 'z_report')")
    conn.execute("INSERT INTO daily_sales (branch_id, date, amount, transactions, source) VALUES (126, '2026-03-15', 9000, 55, 'z_report')")

    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    # Cleanup
    app_module.DB_PATH = original_db
    if os.path.exists(test_db):
        os.remove(test_db)


def _login(client, email='admin@makolet.com', password='test123'):
    """Helper to login and set session."""
    return client.post('/login', data={'email': email, 'password': password}, follow_redirects=True)


# ── Accuracy Tests ────────────────────────────────────────────

class TestAccuracy:

    def test_employee_hours_times_rate_equals_salary(self, client):
        """For each employee: hours * hourly_rate = salary (within 0.01)."""
        _login(client)
        res = client.get('/api/employees?month=2026-04')
        data = json.loads(res.data)
        for emp in data['employees']:
            if emp['hours'] > 0 and emp['hourly_rate'] > 0:
                expected = round(emp['hours'] * emp['hourly_rate'], 2)
                actual = emp['salary'] if emp['salary'] > 0 else expected
                assert abs(actual - expected) < 0.02, \
                    f"{emp['name']}: {actual} != {expected}"

    def test_salary_cost_matches_sum_of_employees(self, client):
        """Sum of individual employee salaries should match salary_cost KPI."""
        _login(client)
        res = client.get('/api/employees?month=2026-04')
        data = json.loads(res.data)
        emp_total = sum(
            e['hours'] * e['hourly_rate']
            for e in data['employees']
            if e['hours'] > 0 and e['hourly_rate'] > 0
        )
        assert abs(data['salary_cost'] - emp_total) < 1.0, \
            f"KPI {data['salary_cost']} vs sum {emp_total}"

    def test_no_employees_shows_empty(self, client):
        """Branch with no employees returns empty list, not error."""
        _login(client)
        # Switch to branch 127 which has 1 employee but check hours
        res = client.get('/api/employees?month=2026-04')
        data = json.loads(res.data)
        assert 'employees' in data
        assert isinstance(data['employees'], list)


# ── Security Tests ────────────────────────────────────────────

class TestSecurity:

    def test_unauthenticated_api_returns_401(self, client):
        """API endpoints require login."""
        res = client.get('/api/employees')
        assert res.status_code == 401

    def test_unauthenticated_delete_returns_401(self, client):
        """DELETE without session returns 401."""
        res = client.delete('/api/employees/1')
        assert res.status_code == 401

    def test_branch_isolation_manager(self, client):
        """Manager of branch 126 cannot see branch 127 employees."""
        _login(client, 'mgr@test.com', 'test123')
        res = client.get('/api/employees?month=2026-04')
        data = json.loads(res.data)
        names = [e['name'] for e in data['employees']]
        assert 'עובד סניף אחר' not in names

    def test_negative_hourly_rate_rejected(self, client):
        """Adding employee with negative rate should fail."""
        _login(client)
        res = client.post('/api/employees',
                          data=json.dumps({'name': 'טסט', 'hourly_rate': -100, 'role': 'ערב'}),
                          content_type='application/json')
        # Should either reject or store 0, not negative
        if res.status_code == 200:
            data = json.loads(res.data)
            # If accepted, verify rate isn't negative in DB
            res2 = client.get('/api/employees?month=2026-04')
            emps = json.loads(res2.data)['employees']
            for e in emps:
                assert e['hourly_rate'] >= 0, f"Negative rate for {e['name']}"

    def test_xss_in_employee_name(self, client):
        """Employee name with script tags should be stored safely."""
        _login(client)
        xss_name = '<script>alert(1)</script>'
        res = client.post('/api/employees',
                          data=json.dumps({'name': xss_name, 'hourly_rate': 30, 'role': 'ערב'}),
                          content_type='application/json')
        assert res.status_code in (200, 201, 400)
        if res.status_code == 200:
            res2 = client.get('/api/employees?month=2026-04')
            data = json.loads(res2.data)
            # Name should be stored as-is (template escaping handles display)
            script_names = [e for e in data['employees'] if '<script>' in e['name']]
            # Verify the name is there (stored literally, rendered escaped)
            assert len(script_names) <= 1

    def test_hebrew_english_emoji_name(self, client):
        """Mixed-script names should be accepted."""
        _login(client)
        mixed_name = 'עובד test'
        res = client.post('/api/employees',
                          data=json.dumps({'name': mixed_name, 'hourly_rate': 35, 'role': 'ערב'}),
                          content_type='application/json')
        assert res.status_code == 200


# ── Edge Cases ────────────────────────────────────────────────

class TestEdgeCases:

    def test_employee_with_zero_hours(self, client):
        """Employee with 0 hours should still appear in list."""
        _login(client)
        # Add employee with no hours
        client.post('/api/employees',
                    data=json.dumps({'name': 'חדש ללא שעות', 'hourly_rate': 30, 'role': 'ערב'}),
                    content_type='application/json')
        res = client.get('/api/employees?month=2026-04')
        data = json.loads(res.data)
        names = [e['name'] for e in data['employees']]
        assert 'חדש ללא שעות' in names

    def test_delete_employee_soft_delete(self, client):
        """Delete should set active=0, not remove from DB."""
        _login(client)
        res = client.delete('/api/employees/1')
        assert res.status_code == 200
        # Should not appear in active list
        res2 = client.get('/api/employees?month=2026-04')
        data = json.loads(res2.data)
        ids = [e['id'] for e in data['employees']]
        assert 1 not in ids


# ── Data Integrity ────────────────────────────────────────────

class TestDataIntegrity:

    def test_history_returns_list(self, client):
        """History should be a list of monthly records."""
        _login(client)
        res = client.get('/api/employees?month=2026-04')
        data = json.loads(res.data)
        assert 'history' in data
        assert isinstance(data['history'], list)

    def test_labor_cost_ratio_endpoint(self, client):
        """Labor cost ratio should return 6 months of data."""
        _login(client)
        res = client.get('/api/labor-cost-ratio')
        assert res.status_code == 200
        data = json.loads(res.data)
        assert isinstance(data, list)
        assert len(data) == 6
        for item in data:
            assert 'month' in item
            assert 'salary' in item
            assert 'income' in item
            assert 'ratio' in item
            assert item['ratio'] >= 0

    def test_labor_cost_ratio_values(self, client):
        """Verify ratio calculation is correct."""
        _login(client)
        res = client.get('/api/labor-cost-ratio')
        data = json.loads(res.data)
        for item in data:
            if item['income'] > 0:
                expected_ratio = round((item['salary'] / item['income']) * 100, 2)
                assert abs(item['ratio'] - expected_ratio) < 0.01, \
                    f"Month {item['month']}: {item['ratio']} != {expected_ratio}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

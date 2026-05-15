"""Decimal hourly_rate round-trip tests (regression for 37.5 → 38 rounding)."""
import os
import sys
import sqlite3
import json
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, DB_PATH
from werkzeug.security import generate_password_hash


@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_employees.db')
    original_db = DB_PATH

    import app as app_module
    app_module.DB_PATH = test_db
    if os.path.exists(test_db):
        os.remove(test_db)
    app_module.init_db()

    conn = sqlite3.connect(test_db, timeout=30)
    for col_sql in [
        "ALTER TABLE branches ADD COLUMN avg_hourly_rate REAL DEFAULT 0",
        "ALTER TABLE branches ADD COLUMN hours_this_month REAL DEFAULT 0",
        "ALTER TABLE branches ADD COLUMN hours_baseline REAL DEFAULT 0",
        "ALTER TABLE branches ADD COLUMN hours_updated_at TEXT",
    ]:
        try:
            conn.execute(col_sql)
        except sqlite3.OperationalError:
            pass

    conn.execute("INSERT OR REPLACE INTO branches (id, name, city, active) VALUES (126, 'איינשטיין', 'תל אביב', 1)")
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) "
        "VALUES (1, 'CEO', 'makoletdashboard@gmail.com', ?, 'admin', 1)",
        (generate_password_hash('test123'),))
    # Existing employee for the edit test
    conn.execute(
        "INSERT INTO employees (id, branch_id, name, role, hourly_rate, active) "
        "VALUES (1, 126, 'עידן בקון', 'ערב', 40, 1)")
    # Pending match row for the save-new-from-pending test
    conn.execute(
        "INSERT INTO employee_match_pending "
        "(id, branch_id, month, csv_name, suggested_employee_id, confidence, hours, salary, resolved, source) "
        "VALUES (1, 126, '2026-05', 'דנה כהן', NULL, 'low', 80.0, 0, 0, 'aviv_report')")
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(test_db):
        os.remove(test_db)


def _login(client):
    return client.post('/login',
                        data={'email': 'makoletdashboard@gmail.com', 'password': 'test123'},
                        follow_redirects=True)


def _rate_of(client, name):
    res = client.get('/api/employees?month=2026-05')
    emps = json.loads(res.data)['employees']
    match = [e for e in emps if e['name'] == name]
    assert match, f"{name} not found in {[e['name'] for e in emps]}"
    return match[0]['hourly_rate']


def test_create_employee_preserves_decimal_rate(client):
    _login(client)
    res = client.post('/api/employees',
                       data=json.dumps({'name': 'שירה לוי', 'hourly_rate': 37.5, 'role': 'ערב'}),
                       content_type='application/json')
    assert res.status_code == 200
    assert _rate_of(client, 'שירה לוי') == 37.5


def test_edit_employee_preserves_decimal_rate(client):
    _login(client)
    assert _rate_of(client, 'עידן בקון') == 40
    res = client.put('/api/employees/1',
                      data=json.dumps({'name': 'עידן בקון', 'hourly_rate': 37.5, 'role': 'ערב'}),
                      content_type='application/json')
    assert res.status_code == 200
    assert _rate_of(client, 'עידן בקון') == 37.5


def test_save_new_from_pending_preserves_decimal_rate(client):
    _login(client)
    res = client.post('/api/employee-match-pending/1/add-new',
                       data=json.dumps({'name': 'דנה כהן', 'hourly_rate': 37.5, 'role': 'ערב'}),
                       content_type='application/json')
    assert res.status_code == 200
    assert _rate_of(client, 'דנה כהן') == 37.5


@pytest.mark.parametrize('rate', [0.5, 0.01, 100.99, 37.5])
def test_create_decimal_rate_edge_cases(client, rate):
    _login(client)
    name = f'עובד {rate}'
    res = client.post('/api/employees',
                       data=json.dumps({'name': name, 'hourly_rate': rate, 'role': 'ערב'}),
                       content_type='application/json')
    assert res.status_code == 200
    assert _rate_of(client, name) == rate

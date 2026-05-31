"""Global-salary employee type: flat monthly cost, hours ignored, no proration.

_calculate_salary_cost = hourly (hours×rate) + sum(active global_salary).
Verifies globals flow through the single salary source and never double-count
against any Aviv hours that happen to exist for them.
"""
import os
import sys
import sqlite3
import json
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, DB_PATH, _calculate_salary_cost
from werkzeug.security import generate_password_hash

MONTH = '2026-05'


@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_global_salary.db')
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


def _salary(client):
    with app.app_context():
        return _calculate_salary_cost(126, MONTH)['amount']


def _add_global(client, name, amount):
    return client.post('/api/employees',
                       data=json.dumps({'name': name, 'role': 'מנהל',
                                        'salary_type': 'global', 'global_salary': amount}),
                       content_type='application/json')


def _add_hourly(client, name, rate):
    return client.post('/api/employees',
                       data=json.dumps({'name': name, 'role': 'ערב', 'hourly_rate': rate}),
                       content_type='application/json')


def _add_hours(db_path, name, hours, rate):
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute(
        "INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, total_salary, source) "
        "VALUES (126, ?, ?, ?, ?, 'aviv_report')",
        (MONTH, name, hours, hours * rate))
    conn.commit()
    conn.close()


def _get_emp(client, name):
    res = client.get(f'/api/employees?month={MONTH}')
    emps = json.loads(res.data)['employees']
    return next((e for e in emps if e['name'] == name), None)


def test_global_matched_hours_shown_but_cost_flat(client):
    """Global employee with matched Aviv hours: hours DISPLAYED on the card,
    cost stays the flat amount (hours never costed)."""
    import app as app_module
    _login(client)
    _add_global(client, 'משה מנהל', 10000)
    # Agent matched hours for this global (rate stored 0, total_salary 0).
    _add_hours(app_module.DB_PATH, 'משה מנהל', 180, 0)

    emp = _get_emp(client, 'משה מנהל')
    assert emp is not None
    assert emp['salary_type'] == 'global'
    assert emp['hours'] == 180          # hours are displayed
    assert emp['global_salary'] == 10000
    assert _salary(client) == 10000     # but cost stays flat

    # Pile on more hours — cost is unchanged.
    res = json.loads(client.get(f'/api/employees?month={MONTH}').data)
    assert res['salary_cost'] == 10000


def test_global_employee_adds_flat_amount(client):
    _login(client)
    assert _salary(client) == 0
    assert _add_global(client, 'משה מנהל', 10000).status_code == 200
    # Exactly the flat amount, no proration.
    assert _salary(client) == 10000


def test_global_plus_hourly_sum(client):
    import app as app_module
    _login(client)
    _add_global(client, 'משה מנהל', 10000)
    _add_hourly(client, 'דנה כהן', 50)
    _add_hours(app_module.DB_PATH, 'דנה כהן', 100, 50)  # 100h × 50 = 5000
    assert _salary(client) == 15000


def test_global_hours_do_not_change_cost(client):
    import app as app_module
    _login(client)
    _add_global(client, 'משה מנהל', 10000)
    # Aviv hours exist for the global employee — must NOT add hours×rate or
    # resurface via the rate=0 → total_salary fallback.
    _add_hours(app_module.DB_PATH, 'משה מנהל', 200, 60)  # would be 12000 if hourly
    assert _salary(client) == 10000


def test_edit_global_amount_updates_cost(client):
    _login(client)
    _add_global(client, 'משה מנהל', 10000)
    emp_id = json.loads(client.get(f'/api/employees?month={MONTH}').data)['employees'][0]['id']
    res = client.put(f'/api/employees/{emp_id}',
                     data=json.dumps({'salary_type': 'global', 'global_salary': 12500}),
                     content_type='application/json')
    assert res.status_code == 200
    assert _salary(client) == 12500


def test_switch_hourly_to_global(client):
    import app as app_module
    _login(client)
    _add_hourly(client, 'דנה כהן', 50)
    _add_hours(app_module.DB_PATH, 'דנה כהן', 100, 50)
    emp_id = json.loads(client.get(f'/api/employees?month={MONTH}').data)['employees'][0]['id']
    assert _salary(client) == 5000  # hourly
    client.put(f'/api/employees/{emp_id}',
               data=json.dumps({'salary_type': 'global', 'global_salary': 8000}),
               content_type='application/json')
    assert _salary(client) == 8000  # now flat, hours ignored


def test_global_requires_positive_amount(client):
    _login(client)
    res = client.post('/api/employees',
                      data=json.dumps({'name': 'בלי סכום', 'salary_type': 'global', 'global_salary': 0}),
                      content_type='application/json')
    assert res.status_code == 400


def test_hourly_only_branch_unchanged(client):
    import app as app_module
    _login(client)
    _add_hourly(client, 'דנה כהן', 45)
    _add_hours(app_module.DB_PATH, 'דנה כהן', 80, 45)
    assert _salary(client) == 3600  # 80 × 45, no global interference

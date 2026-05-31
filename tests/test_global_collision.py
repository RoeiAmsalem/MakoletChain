"""POST /api/employees no longer silently swallows name collisions, and the
unmatched-link flow can create a global employee.

Covers the Bug-1 collision fix:
  - brand-new name -> created
  - active duplicate name -> 409, nothing created (no silent {ok:true})
  - soft-deleted duplicate name -> revived with the new (global) details
  - convert active hourly -> global via PUT
And Bug-2: api_pending_add_new accepts salary_type='global'.
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
    test_db = os.path.join(os.path.dirname(__file__), 'test_global_collision.db')
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
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, active) "
                 "VALUES (1, 'CEO', 'makoletdashboard@gmail.com', ?, 'admin', 1)",
                 (generate_password_hash('test123'),))
    conn.commit()
    conn.close()

    with app.test_client() as c:
        c.post('/login', data={'email': 'makoletdashboard@gmail.com', 'password': 'test123'},
               follow_redirects=True)
        yield c

    if os.path.exists(test_db):
        os.remove(test_db)


def _list(client):
    return json.loads(client.get(f'/api/employees?month={MONTH}').data)['employees']


def _salary(client):
    with app.app_context():
        return _calculate_salary_cost(126, MONTH)['amount']


def test_new_name_creates_global(client):
    r = client.post('/api/employees', data=json.dumps(
        {'name': 'גלובל גדי', 'role': 'מנהל', 'salary_type': 'global', 'global_salary': 10000}),
        content_type='application/json')
    assert r.status_code == 200
    emps = _list(client)
    assert any(e['name'] == 'גלובל גדי' and e['salary_type'] == 'global' for e in emps)
    assert _salary(client) == 10000


def test_active_duplicate_returns_409(client):
    client.post('/api/employees', data=json.dumps({'name': 'דנה כהן', 'role': 'ערב', 'hourly_rate': 50}),
                content_type='application/json')
    r = client.post('/api/employees', data=json.dumps(
        {'name': 'דנה כהן', 'role': 'מנהל', 'salary_type': 'global', 'global_salary': 9000}),
        content_type='application/json')
    assert r.status_code == 409
    body = json.loads(r.data)
    assert 'error' in body and 'כבר' in body['error']
    # Nothing changed — still hourly, no global created.
    emps = _list(client)
    dana = [e for e in emps if e['name'] == 'דנה כהן']
    assert len(dana) == 1 and dana[0]['salary_type'] == 'hourly'


def test_soft_deleted_name_revived_as_global(client):
    # create hourly, then soft-delete
    client.post('/api/employees', data=json.dumps({'name': 'רון לוי', 'role': 'ערב', 'hourly_rate': 40}),
                content_type='application/json')
    emp_id = [e for e in _list(client) if e['name'] == 'רון לוי'][0]['id']
    assert client.delete(f'/api/employees/{emp_id}').status_code == 200
    assert not any(e['name'] == 'רון לוי' for e in _list(client))  # gone from active list

    # re-add same name as global -> revived
    r = client.post('/api/employees', data=json.dumps(
        {'name': 'רון לוי', 'role': 'מנהל', 'salary_type': 'global', 'global_salary': 11000}),
        content_type='application/json')
    assert r.status_code == 200
    revived = [e for e in _list(client) if e['name'] == 'רון לוי']
    assert len(revived) == 1
    assert revived[0]['salary_type'] == 'global'
    assert revived[0]['global_salary'] == 11000
    assert _salary(client) == 11000


def test_convert_active_hourly_to_global_via_put(client):
    client.post('/api/employees', data=json.dumps({'name': 'מאיה כהן', 'role': 'ערב', 'hourly_rate': 50}),
                content_type='application/json')
    emp_id = [e for e in _list(client) if e['name'] == 'מאיה כהן'][0]['id']
    r = client.put(f'/api/employees/{emp_id}', data=json.dumps(
        {'salary_type': 'global', 'global_salary': 8500}), content_type='application/json')
    assert r.status_code == 200
    e = [x for x in _list(client) if x['name'] == 'מאיה כהן'][0]
    assert e['salary_type'] == 'global' and e['global_salary'] == 8500
    assert _salary(client) == 8500


def _seed_pending(name, hours, source='aviv_report'):
    conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), 'test_global_collision.db'), timeout=30)
    cur = conn.execute(
        "INSERT INTO employee_match_pending "
        "(branch_id, month, csv_name, suggested_employee_id, confidence, hours, salary, resolved, source, is_new_employee) "
        "VALUES (126, ?, ?, NULL, 'low', ?, 0, 0, ?, 1)", (MONTH, name, hours, source))
    pid = cur.lastrowid
    conn.commit()
    conn.close()
    return pid


def test_pending_link_as_global(client):
    pid = _seed_pending('אורח חדש', 95.0)
    r = client.post(f'/api/employee-match-pending/{pid}/add-new', data=json.dumps(
        {'name': 'אורח חדש', 'role': 'מנהל', 'salary_type': 'global', 'global_salary': 12000}),
        content_type='application/json')
    assert r.status_code == 200
    e = [x for x in _list(client) if x['name'] == 'אורח חדש']
    assert len(e) == 1 and e[0]['salary_type'] == 'global'
    # Flat amount contributes; the 95 pending hours do NOT become cost.
    assert _salary(client) == 12000
    # Pending row resolved (banner clears), and no employee_hours row written.
    conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), 'test_global_collision.db'), timeout=30)
    unresolved = conn.execute("SELECT COUNT(*) FROM employee_match_pending WHERE resolved=0").fetchone()[0]
    hours_rows = conn.execute("SELECT COUNT(*) FROM employee_hours WHERE branch_id=126 AND employee_name='אורח חדש'").fetchone()[0]
    conn.close()
    assert unresolved == 0
    assert hours_rows == 0


def test_pending_link_as_hourly_still_works(client):
    pid = _seed_pending('עובד שעתי', 80.0)
    r = client.post(f'/api/employee-match-pending/{pid}/add-new', data=json.dumps(
        {'name': 'עובד שעתי', 'role': 'ערב', 'hourly_rate': 50}), content_type='application/json')
    assert r.status_code == 200
    assert _salary(client) == 4000  # 80 × 50 promoted

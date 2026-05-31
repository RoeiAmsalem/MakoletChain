"""End-to-end HTTP probe on staging: create a global employee via the real
POST /api/employees, then GET /api/employees, and report whether it appears.

Uses the Flask test client against the staging DB (real schema), with a logged
admin session on a sandbox branch. Exercises the exact code the form hits.
Cleans up the sandbox rows afterwards.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module
from app import app, get_db

BID = 999992
MONTH = '2026-05'


def names_in_get(client):
    r = client.get(f'/api/employees?month={MONTH}&branch_id={BID}')
    body = r.get_json()
    return [(e['name'], e.get('salary_type'), e.get('hours'), e.get('salary')) for e in body['employees']], body.get('salary_cost')


app.config['TESTING'] = True
with app.app_context():
    db = get_db()
    db.execute("DELETE FROM employees WHERE branch_id=?", (BID,))
    db.execute("DELETE FROM branches WHERE id=?", (BID,))
    db.execute("INSERT INTO branches (id, name, city, active) VALUES (?, 'DBG-HTTP', 'x', 1)", (BID,))
    db.commit()

with app.test_client() as c:
    with c.session_transaction() as s:
        s['user_id'] = 1
        s['user_role'] = 'admin'
        s['branch_id'] = BID

    # 1) Create a global employee exactly as the form does.
    r1 = c.post('/api/employees', json={'name': 'גלובל גדי', 'role': 'מנהל',
                                         'salary_type': 'global', 'global_salary': 10000})
    print("POST global ->", r1.status_code, r1.get_json())

    # 2) Create a zero-hour hourly employee.
    r2 = c.post('/api/employees', json={'name': 'הדס שעתי', 'role': 'ערב', 'hourly_rate': 45})
    print("POST hourly ->", r2.status_code, r2.get_json())

    emps, salary_cost = names_in_get(c)
    print("\nGET /api/employees returned salary_cost =", salary_cost)
    for n in emps:
        print("   ", n)

    appeared = any(e[0] == 'גלובל גדי' and e[1] == 'global' for e in emps)
    print("\nGLOBAL EMPLOYEE APPEARS:", appeared)

with app.app_context():
    db = get_db()
    db.execute("DELETE FROM employees WHERE branch_id=?", (BID,))
    db.execute("DELETE FROM branches WHERE id=?", (BID,))
    db.commit()
    print("(cleaned up)")

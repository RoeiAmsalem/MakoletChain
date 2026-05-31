"""Probe: does a global employee come back from GET /api/employees?

Creates an admin session for a sandbox branch on the staging DB, inserts one
global + one zero-hour hourly + one hourly-with-hours employee, calls the real
api_employees_list handler, prints which employees the JSON contains, then
deletes the sandbox rows. Read-through of the actual shipped code path.
"""
import os
import sys
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module
from app import app, get_db

BID = 999991
MONTH = '2026-05'


def _cleanup(db):
    db.execute("DELETE FROM employees WHERE branch_id=?", (BID,))
    db.execute("DELETE FROM employee_hours WHERE branch_id=?", (BID,))
    db.execute("DELETE FROM branches WHERE id=?", (BID,))
    db.commit()


with app.test_request_context():
    db = get_db()
    _cleanup(db)
    db.execute("INSERT INTO branches (id, name, city, active) VALUES (?, 'DBG', 'x', 1)", (BID,))
    db.execute("INSERT INTO employees (branch_id, name, role, hourly_rate, salary_type, global_salary, active) "
               "VALUES (?, 'גלובל גדי', 'מנהל', 0, 'global', 10000, 1)", (BID,))
    db.execute("INSERT INTO employees (branch_id, name, role, hourly_rate, salary_type, active) "
               "VALUES (?, 'שעתי בלי שעות', 'ערב', 45, 'hourly', 1)", (BID,))
    db.execute("INSERT INTO employees (branch_id, name, role, hourly_rate, salary_type, active) "
               "VALUES (?, 'שעתי עם שעות', 'ערב', 50, 'hourly', 1)", (BID,))
    db.execute("INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, total_salary, source) "
               "VALUES (?, ?, 'שעתי עם שעות', 100, 5000, 'aviv_report')", (BID, MONTH))
    db.commit()

    from flask import session
    session['user_id'] = 1
    session['user_role'] = 'admin'
    session['branch_id'] = BID

    from app import api_employees_list
    import flask
    # call handler within a request context carrying our query args
    with app.test_request_context(f'/api/employees?month={MONTH}&branch_id={BID}'):
        flask.session.update({'user_id': 1, 'user_role': 'admin', 'branch_id': BID})
        resp = api_employees_list()
        body = resp.get_json() if hasattr(resp, 'get_json') else json.loads(resp[0])

    print("salary_cost:", body.get('salary_cost'))
    print("employees returned:")
    for e in body.get('employees', []):
        print(f"  - {e['name']:20s} type={e.get('salary_type'):7s} hours={e.get('hours')} "
              f"salary={e.get('salary')} global_salary={e.get('global_salary')}")

    _cleanup(db)
    print("\n(cleaned up sandbox)")

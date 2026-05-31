"""Test the INSERT OR IGNORE + UNIQUE(branch_id,name) collision hypothesis.

Scenario A: a same-name ACTIVE hourly employee already exists, manager tries to
            add them as global -> does the global appear / does anything change?
Scenario B: a same-name SOFT-DELETED (active=0) employee exists, manager adds a
            global with that name -> does the global appear in the active list?
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from app import app, get_db

BID = 999993
MONTH = '2026-05'


def get_list(c):
    b = c.get(f'/api/employees?month={MONTH}&branch_id={BID}').get_json()
    return [(e['name'], e['salary_type'], e.get('global_salary')) for e in b['employees']]


app.config['TESTING'] = True
for scenario, active in (('A active-collision', 1), ('B softdeleted-collision', 0)):
    with app.app_context():
        db = get_db()
        db.execute("DELETE FROM employees WHERE branch_id=?", (BID,))
        db.execute("DELETE FROM branches WHERE id=?", (BID,))
        db.execute("INSERT INTO branches (id,name,city,active) VALUES (?, 'COL', 'x', 1)", (BID,))
        db.execute("INSERT INTO employees (branch_id,name,role,hourly_rate,salary_type,active) "
                   "VALUES (?, 'דנה כהן', 'ערב', 50, 'hourly', ?)", (BID, active))
        db.commit()
    with app.test_client() as c:
        with c.session_transaction() as s:
            s.update({'user_id': 1, 'user_role': 'admin', 'branch_id': BID})
        r = c.post('/api/employees', json={'name': 'דנה כהן', 'role': 'מנהל',
                                           'salary_type': 'global', 'global_salary': 9000})
        lst = get_list(c)
        is_global = any(n == 'דנה כהן' and t == 'global' for n, t, g in lst)
        print(f"[{scenario}] POST={r.status_code}{r.get_json()}  active_list={lst}  GLOBAL_PRESENT={is_global}")
    with app.app_context():
        db = get_db()
        db.execute("DELETE FROM employees WHERE branch_id=?", (BID,))
        db.execute("DELETE FROM branches WHERE id=?", (BID,))
        db.commit()
print("(cleaned up)")

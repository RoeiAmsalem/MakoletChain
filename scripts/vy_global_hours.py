"""Staging VY: global employees show worked hours but cost stays flat.

Sandbox branch, real HTTP routes (test client) against staging DB, cleanup.
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from app import app, get_db, _calculate_salary_cost

BID = 999995
MONTH = '2026-05'
results = []


def check(label, cond):
    results.append((bool(cond), label))
    print(f"{'PASS' if cond else 'FAIL'} — {label}")


def cleanup():
    with app.app_context():
        db = get_db()
        for t in ('employees', 'employee_hours'):
            db.execute(f"DELETE FROM {t} WHERE branch_id=?", (BID,))
        db.execute("DELETE FROM branches WHERE id=?", (BID,))
        db.commit()


def add_hours(name, hours):
    with app.app_context():
        get_db().execute(
            "INSERT OR REPLACE INTO employee_hours (branch_id, month, employee_name, total_hours, total_salary, source) "
            "VALUES (?, ?, ?, ?, 0, 'aviv_report')", (BID, MONTH, name, hours))
        get_db().commit()


def emp(c, name):
    body = c.get(f'/api/employees?month={MONTH}&branch_id={BID}').get_json()
    return next((e for e in body['employees'] if e['name'] == name), None), body['salary_cost']


app.config['TESTING'] = True
cleanup()
with app.app_context():
    get_db().execute("INSERT INTO branches (id,name,city,active) VALUES (?, 'VY-HRS','x',1)", (BID,))
    get_db().commit()

with app.test_client() as c:
    with c.session_transaction() as s:
        s.update({'user_id': 1, 'user_role': 'admin', 'branch_id': BID})

    c.post('/api/employees', json={'name': 'גלובל גדי', 'role': 'מנהל',
                                   'salary_type': 'global', 'global_salary': 10000})
    # also an hourly employee with hours, to confirm they're unaffected
    c.post('/api/employees', json={'name': 'שעתי דנה', 'role': 'ערב', 'hourly_rate': 50})
    add_hours('שעתי דנה', 100)   # 100 × 50 = 5000

    # global with matched hours
    add_hours('גלובל גדי', 180)
    g, cost = emp(c, 'גלובל גדי')
    check('global hours displayed (180)', g and g['hours'] == 180)
    check('global salary_type=global', g and g['salary_type'] == 'global')
    check('branch cost = 10000 global + 5000 hourly = 15000', cost == 15000)

    # pile on more hours -> cost unchanged
    add_hours('גלובל גדי', 400)
    g2, cost2 = emp(c, 'גלובל גדי')
    check('global hours now 400', g2 and g2['hours'] == 400)
    check('cost still 15000 (hours never costed)', cost2 == 15000)
    with app.app_context():
        check('_calculate_salary_cost = 15000', _calculate_salary_cost(BID, MONTH)['amount'] == 15000)

    # hourly employee unchanged
    h, _ = emp(c, 'שעתי דנה')
    check('hourly unchanged (100h, salary 5000)', h and h['hours'] == 100 and h['salary'] == 5000)

cleanup()
failed = [r for r in results if not r[0]]
print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
sys.exit(1 if failed else 0)

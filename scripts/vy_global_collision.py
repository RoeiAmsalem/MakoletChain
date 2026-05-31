"""Staging VY for the global-collision fix + unmatched-link-as-global.

Drives the real HTTP routes (test client) against the staging DB on a sandbox
branch, asserts each behaviour, then deletes all sandbox rows. PASS/FAIL lines.
"""
import os
import sys
import sqlite3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, get_db, _calculate_salary_cost

BID = 999994
MONTH = '2026-05'
results = []


def check(label, cond):
    results.append((bool(cond), label))
    print(f"{'PASS' if cond else 'FAIL'} — {label}")


def cleanup():
    with app.app_context():
        db = get_db()
        db.execute("DELETE FROM employees WHERE branch_id=?", (BID,))
        db.execute("DELETE FROM employee_hours WHERE branch_id=?", (BID,))
        db.execute("DELETE FROM employee_match_pending WHERE branch_id=?", (BID,))
        db.execute("DELETE FROM branches WHERE id=?", (BID,))
        db.commit()


def emp_list(c):
    return c.get(f'/api/employees?month={MONTH}&branch_id={BID}').get_json()['employees']


def salary():
    with app.app_context():
        return _calculate_salary_cost(BID, MONTH)['amount']


app.config['TESTING'] = True
cleanup()
with app.app_context():
    get_db().execute("INSERT INTO branches (id,name,city,active) VALUES (?, 'VY-COL','x',1)", (BID,))
    get_db().commit()

with app.test_client() as c:
    with c.session_transaction() as s:
        s.update({'user_id': 1, 'user_role': 'admin', 'branch_id': BID})

    # 1) brand-new global name -> appears
    r = c.post('/api/employees', json={'name': 'גלובל גדי', 'role': 'מנהל',
                                       'salary_type': 'global', 'global_salary': 10000})
    check('new global POST ok', r.status_code == 200)
    check('new global appears', any(e['name'] == 'גלובל גדי' and e['salary_type'] == 'global' for e in emp_list(c)))
    check('new global cost = 10000', salary() == 10000)

    # 2) active duplicate -> 409, nothing created
    c.post('/api/employees', json={'name': 'דנה כהן', 'role': 'ערב', 'hourly_rate': 50})
    r = c.post('/api/employees', json={'name': 'דנה כהן', 'role': 'מנהל',
                                       'salary_type': 'global', 'global_salary': 9000})
    check('active duplicate -> 409', r.status_code == 409)
    check('409 has clear error', 'כבר' in (r.get_json() or {}).get('error', ''))
    dana = [e for e in emp_list(c) if e['name'] == 'דנה כהן']
    check('active duplicate unchanged (still hourly, single row)', len(dana) == 1 and dana[0]['salary_type'] == 'hourly')

    # 3) soft-deleted same name -> revived as global
    c.post('/api/employees', json={'name': 'רון לוי', 'role': 'ערב', 'hourly_rate': 40})
    rid = [e for e in emp_list(c) if e['name'] == 'רון לוי'][0]['id']
    c.delete(f'/api/employees/{rid}')
    check('soft-deleted gone from list', not any(e['name'] == 'רון לוי' for e in emp_list(c)))
    r = c.post('/api/employees', json={'name': 'רון לוי', 'role': 'מנהל',
                                       'salary_type': 'global', 'global_salary': 11000})
    revived = [e for e in emp_list(c) if e['name'] == 'רון לוי']
    check('revived as global', r.status_code == 200 and len(revived) == 1
          and revived[0]['salary_type'] == 'global' and revived[0]['global_salary'] == 11000)

    # 4) convert active hourly -> global via PUT
    c.post('/api/employees', json={'name': 'מאיה כהן', 'role': 'ערב', 'hourly_rate': 50})
    mid = [e for e in emp_list(c) if e['name'] == 'מאיה כהן'][0]['id']
    r = c.put(f'/api/employees/{mid}', json={'salary_type': 'global', 'global_salary': 8500})
    maya = [e for e in emp_list(c) if e['name'] == 'מאיה כהן'][0]
    check('convert hourly->global via PUT', r.status_code == 200 and maya['salary_type'] == 'global' and maya['global_salary'] == 8500)

    # 5) link an unmatched name as global
    with app.app_context():
        get_db().execute(
            "INSERT INTO employee_match_pending (branch_id, month, csv_name, suggested_employee_id, "
            "confidence, hours, salary, resolved, source, is_new_employee) "
            "VALUES (?, ?, 'אורח חדש', NULL, 'low', 95.0, 0, 0, 'aviv_report', 1)", (BID, MONTH))
        get_db().commit()
        pid = get_db().execute("SELECT id FROM employee_match_pending WHERE branch_id=? AND csv_name='אורח חדש'", (BID,)).fetchone()[0]
    r = c.post(f'/api/employee-match-pending/{pid}/add-new',
               json={'name': 'אורח חדש', 'role': 'מנהל', 'salary_type': 'global', 'global_salary': 12000})
    guest = [e for e in emp_list(c) if e['name'] == 'אורח חדש']
    check('unmatched linked as global', r.status_code == 200 and len(guest) == 1 and guest[0]['salary_type'] == 'global')
    with app.app_context():
        unresolved = get_db().execute("SELECT COUNT(*) FROM employee_match_pending WHERE branch_id=? AND resolved=0", (BID,)).fetchone()[0]
        ghours = get_db().execute("SELECT COUNT(*) FROM employee_hours WHERE branch_id=? AND employee_name='אורח חדש'", (BID,)).fetchone()[0]
    check('pending resolved (banner clears)', unresolved == 0)
    check('global link writes no hours row', ghours == 0)

cleanup()
failed = [r for r in results if not r[0]]
print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
sys.exit(1 if failed else 0)

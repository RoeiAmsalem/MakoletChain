"""Aviv Employee Hours Agent — fetches per-employee shift data from Aviv BI API.

Endpoint: POST /avivbi/v2/employees/sales?type=all
Date format: YYYYMMDDHHMMSS (no separators)
Returns per-employee shifts with check-in/out times and hours.
"""

import logging
import os
import sqlite3
import time

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)

BASE = 'https://bi1.aviv-pos.co.il:8443/avivbi/v2'
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')


def _login(username, password):
    r = requests.post(f'{BASE}/account/login',
                      json={'user': username, 'password': password},
                      timeout=15, verify=False)
    r.raise_for_status()
    data = r.json()
    token = data.get('token') or data.get('value')
    branches = data.get('branches', [])
    aviv_branch_id = branches[0]['id'] if branches else None
    return token, aviv_branch_id


def _refresh(token):
    time.sleep(0.5)
    r = requests.post(f'{BASE}/account/refresh',
                      headers={'Authtoken': token, 'Content-Type': 'application/json'},
                      json={}, timeout=10, verify=False)
    return r.json().get('token') or r.json().get('value') or token


def _parse_hours(s):
    """Parse 'HH:MM' string to float hours. e.g. '97:08' → 97.133"""
    if not s or ':' not in str(s):
        return 0.0
    parts = str(s).split(':')
    try:
        return int(parts[0]) + int(parts[1]) / 60
    except (ValueError, IndexError):
        return 0.0


def _friendly_error(e):
    """Lightweight error message formatter."""
    msg = str(e)
    if '500' in msg:
        return "Aviv BI server error — their server is having issues."
    if '401' in msg:
        return "Aviv BI login failed — credentials may have changed."
    if 'Timeout' in msg or 'timed out' in msg:
        return "Aviv BI request timed out."
    return msg[:120] if msg else "Unknown error."


def _match_employee(aviv_name, aviv_emp_id, db_employees, branch_name=''):
    """Match Aviv employee to DB employee. Returns (emp_id, confidence).

    Priority:
    1. Match by aviv_employee_id (exact link from previous approval)
    2. Fuzzy name matching (reuses gmail_agent logic)
    """
    # First: match by stored Aviv ID
    for emp in db_employees:
        if emp['aviv_employee_id'] and emp['aviv_employee_id'] == aviv_emp_id:
            return emp['id'], 'exact'

    # Second: fuzzy name match
    try:
        from agents.gmail_agent import _match_employee_name
        emp_list = [{'id': e['id'], 'name': e['name'], 'hourly_rate': e['hourly_rate']}
                    for e in db_employees]
        emp_id, confidence, _, _ = _match_employee_name(aviv_name, emp_list, branch_name)
        return emp_id, confidence
    except Exception as e:
        log.warning("Fuzzy match failed for '%s': %s", aviv_name, e)
        return None, 'none'


def run_aviv_employees(branch_id):
    """Fetch per-employee hours from Aviv BI and save to employee_hours."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    branch = None
    run_id = None

    try:
        # Create agent_runs entry
        cur = conn.execute(
            "INSERT INTO agent_runs (branch_id, agent, status, started_at) "
            "VALUES (?, 'aviv_employees', 'running', datetime('now'))",
            (branch_id,))
        run_id = cur.lastrowid
        conn.commit()

        branch = conn.execute('SELECT * FROM branches WHERE id=?', (branch_id,)).fetchone()
        if not branch or not branch['aviv_user_id']:
            msg = 'No Aviv credentials'
            conn.execute(
                "UPDATE agent_runs SET status='error', message=?, finished_at=datetime('now') WHERE id=?",
                (msg, run_id))
            conn.commit()
            return {'success': False, 'message': msg}

        # Login + refresh
        token, aviv_branch_id = _login(branch['aviv_user_id'], branch['aviv_password'])
        token = _refresh(token)

        # Fetch current month's employee data
        from datetime import date
        today = date.today()
        from_date = today.replace(day=1).strftime('%Y%m%d') + '000000'
        to_date = today.strftime('%Y%m%d') + '235959'
        month_str = today.strftime('%Y-%m')

        r = requests.post(
            f'{BASE}/employees/sales?type=all',
            headers={'Authtoken': token, 'Content-Type': 'application/json'},
            json={'branches': [aviv_branch_id], 'from': from_date, 'to': to_date, 'employees': []},
            timeout=30, verify=False)
        r.raise_for_status()
        data = r.json()

        # Ensure new columns exist
        for col_sql in [
            "ALTER TABLE employees ADD COLUMN aviv_employee_id INTEGER",
            "ALTER TABLE employee_match_pending ADD COLUMN aviv_employee_id INTEGER",
            "ALTER TABLE employee_match_pending ADD COLUMN source TEXT DEFAULT 'csv'",
        ]:
            try:
                conn.execute(col_sql)
                conn.commit()
            except sqlite3.OperationalError:
                pass

        # Get all known employees for this branch
        db_employees = [dict(r) for r in conn.execute(
            'SELECT id, name, hourly_rate, aviv_employee_id FROM employees WHERE branch_id=?',
            (branch_id,)).fetchall()]

        branch_name = branch['name'] or ''
        saved = 0
        flagged = 0

        for group in data:
            title = group.get('title', '')
            # Skip "Unknown user" group — that's unlinked POS transactions
            if title == '---' or title == 'Unknown user':
                continue

            for emp in group.get('employees', []):
                aviv_name = (emp.get('name') or '').strip()
                aviv_emp_id = emp.get('id')
                hours = _parse_hours(emp.get('workingHours', '0:00'))

                if hours == 0 or not aviv_name:
                    continue

                emp_id, confidence = _match_employee(aviv_name, aviv_emp_id, db_employees, branch_name)

                if confidence in ('exact', 'high') and emp_id:
                    # Save aviv_employee_id link for future exact matches
                    conn.execute(
                        'UPDATE employees SET aviv_employee_id=? WHERE id=? AND (aviv_employee_id IS NULL OR aviv_employee_id != ?)',
                        (aviv_emp_id, emp_id, aviv_emp_id))

                    # Get hourly rate for salary calculation
                    db_emp = conn.execute('SELECT hourly_rate FROM employees WHERE id=?', (emp_id,)).fetchone()
                    rate = db_emp['hourly_rate'] if db_emp and db_emp['hourly_rate'] else 0
                    salary = round(hours * rate, 2)

                    # Save to employee_hours using employee_name for compatibility
                    emp_row = conn.execute('SELECT name FROM employees WHERE id=?', (emp_id,)).fetchone()
                    emp_name = emp_row['name'] if emp_row else aviv_name

                    conn.execute('''
                        INSERT INTO employee_hours (branch_id, month, employee_name, total_hours, total_salary, source)
                        VALUES (?, ?, ?, ?, ?, 'aviv_api')
                        ON CONFLICT(branch_id, month, employee_name) DO UPDATE SET
                            total_hours=excluded.total_hours,
                            total_salary=excluded.total_salary,
                            source='aviv_api'
                    ''', (branch_id, month_str, emp_name, round(hours, 2), salary))
                    saved += 1
                    log.info("  Saved: %s → %s (%.1f hrs, ₪%.0f)", aviv_name, emp_name, hours, salary)
                else:
                    # Flag for manual review
                    existing = conn.execute(
                        'SELECT id FROM employee_match_pending WHERE branch_id=? AND month=? AND csv_name=? AND resolved=0',
                        (branch_id, month_str, aviv_name)).fetchone()

                    if not existing:
                        conn.execute('''
                            INSERT INTO employee_match_pending
                            (branch_id, month, csv_name, aviv_employee_id, suggested_employee_id,
                             confidence, hours, salary, source)
                            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'aviv_api')
                        ''', (branch_id, month_str, aviv_name, aviv_emp_id,
                              emp_id, confidence, round(hours, 2)))
                        flagged += 1
                        log.info("  Flagged: %s (aviv_id=%s, %.1f hrs, confidence=%s)",
                                 aviv_name, aviv_emp_id, hours, confidence)
                    else:
                        conn.execute(
                            'UPDATE employee_match_pending SET hours=?, aviv_employee_id=? WHERE id=?',
                            (round(hours, 2), aviv_emp_id, existing['id']))

        conn.commit()
        msg = f'{saved} employees updated, {flagged} flagged for review'

        if flagged > 0:
            try:
                from utils.notify import notify
                notify(f'Attendance — {branch_name}',
                       f'{flagged} employees from Aviv need review on the employees page.')
            except Exception:
                pass

        conn.execute(
            "UPDATE agent_runs SET status='success', docs_count=?, message=?, finished_at=datetime('now') WHERE id=?",
            (saved, msg, run_id))
        conn.commit()
        log.info("aviv_employees branch %d: %s", branch_id, msg)
        return {'success': True, 'message': msg, 'saved': saved, 'flagged': flagged}

    except Exception as e:
        log.exception('aviv_employees failed for branch %d', branch_id)
        pass  # use local _friendly_error
        msg = _friendly_error(e)
        try:
            if run_id:
                conn.execute(
                    "UPDATE agent_runs SET status='error', message=?, finished_at=datetime('now') WHERE id=?",
                    (msg, run_id))
                conn.commit()
        except Exception:
            pass
        try:
            from utils.notify import notify
            bname = branch['name'] if branch else f'Branch {branch_id}'
            notify(f'Aviv Employees — {bname}', msg)
        except Exception:
            pass
        return {'success': False, 'message': msg}
    finally:
        conn.close()


if __name__ == '__main__':
    import sys
    # Allow running as standalone script
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    bid = int(sys.argv[1]) if len(sys.argv) > 1 else 126
    print(run_aviv_employees(bid))

"""VY: employee-delete cleans orphaned employee_hours + history table active=1 join.

Isolated on a throwaway branch (99999) so real branch data does not affect the
numbers. Exercises the REAL api_employees_delete endpoint via the test client.
Self-cleans. Read-only against real branches.
"""
import sqlite3
import app as A
from app import app, _calculate_salary_cost

DB = A.DB_PATH
B = 99999
T = 'ZZ_VY_DELETE_TEST'      # gets deleted
C = 'ZZ_VY_CONTROL'          # stays — must keep counting
MONTH = A._now_il().strftime('%Y-%m')

# The exact history-table query after Fix 2 (active=1 join).
HIST_SQL = (
    "SELECT COALESCE(SUM(eh.total_hours),0) AS hours, "
    "COALESCE(SUM(eh.total_salary),0) AS salary, COUNT(*) AS cnt "
    "FROM employee_hours eh JOIN employees e ON "
    "(e.branch_id = eh.branch_id AND e.name = eh.employee_name AND e.active = 1) "
    "WHERE eh.branch_id=? AND eh.month=? AND eh.source IN ('aviv_api','aviv_report')"
)


def conn():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c


def run(sql, args=()):
    c = conn()
    c.execute(sql, args)
    c.commit()
    c.close()


def hist_salary():
    c = conn()
    r = c.execute(HIST_SQL, (B, MONTH)).fetchone()
    c.close()
    return round(r['salary'], 2)


def calc_salary():
    with app.app_context():
        return _calculate_salary_cost(B, MONTH)['amount']


def orphan_hours_count(name):
    c = conn()
    n = c.execute("SELECT COUNT(*) FROM employee_hours WHERE branch_id=? AND employee_name=?",
                  (B, name)).fetchone()[0]
    c.close()
    return n


def cleanup():
    run("DELETE FROM employee_hours WHERE branch_id=?", (B,))
    run("DELETE FROM employees WHERE branch_id=?", (B,))


results = []


def check(label, ok, evidence):
    results.append((label, ok, evidence))


cleanup()  # clear any prior residue

# ── seed ──────────────────────────────────────────────────────────
run("INSERT INTO employees (branch_id,name,role,hourly_rate,active) VALUES (?,?,?,?,1)",
    (B, T, 'ערב', 12.7))
run("INSERT INTO employees (branch_id,name,role,hourly_rate,active) VALUES (?,?,?,?,1)",
    (B, C, 'ערב', 10.0))
c = conn()
TID = c.execute("SELECT id FROM employees WHERE branch_id=? AND name=?", (B, T)).fetchone()['id']
c.close()
run("INSERT INTO employee_hours (branch_id,month,employee_name,total_hours,total_salary,source) "
    "VALUES (?,?,?,?,?, 'aviv_report')", (B, MONTH, T, 24.0, 304.8))
run("INSERT INTO employee_hours (branch_id,month,employee_name,total_hours,total_salary,source) "
    "VALUES (?,?,?,?,?, 'aviv_report')", (B, MONTH, C, 10.0, 100.0))

# ── BEFORE delete: both readers count test+control = 404.8 ─────────
before_calc = calc_salary()
before_hist = hist_salary()
check("BEFORE both readers agree = 404.8",
      abs(before_calc - 404.8) < 0.01 and abs(before_hist - 404.8) < 0.01,
      f"calc={before_calc} hist={before_hist}")

# ── DELETE via the real endpoint ──────────────────────────────────
client = app.test_client()
with client.session_transaction() as s:
    s['user_id'] = 1
    s['user_role'] = 'admin'
    s['user_branches'] = [B]
    s['branch_id'] = B
resp = client.delete(f'/api/employees/{TID}')
check("DELETE endpoint 200", resp.status_code == 200, f"status={resp.status_code} body={resp.get_data(as_text=True)[:120]}")

# ── AFTER delete ──────────────────────────────────────────────────
after_calc = calc_salary()
after_hist = hist_salary()
orphans = orphan_hours_count(T)

# Home KPI via real /api/summary endpoint
sresp = client.get(f'/api/summary?month={MONTH}')
summary_salary = round(sresp.get_json().get('salary', -1), 2) if sresp.status_code == 200 else 'ERR'

check("Fix1: no orphaned employee_hours for deleted employee", orphans == 0, f"orphan_rows={orphans}")
check("KPI (_calculate_salary_cost) = 100.0 (control only)", abs(after_calc - 100.0) < 0.01, f"calc={after_calc}")
check("History table (Fix2 active=1 join) = 100.0", abs(after_hist - 100.0) < 0.01, f"hist={after_hist}")
check("Home /api/summary salary = 100.0", summary_salary == 100.0, f"summary={summary_salary}")
check("All three readers agree post-delete",
      abs(after_calc - after_hist) < 0.01 and summary_salary == after_calc,
      f"calc={after_calc} hist={after_hist} summary={summary_salary}")
# control employee untouched
c = conn()
ctrl_active = c.execute("SELECT active FROM employees WHERE branch_id=? AND name=?", (B, C)).fetchone()['active']
ctrl_hours = c.execute("SELECT COUNT(*) FROM employee_hours WHERE branch_id=? AND employee_name=?", (B, C)).fetchone()[0]
c.close()
check("Control employee still active + hours intact (no over-filter)",
      ctrl_active == 1 and ctrl_hours == 1, f"active={ctrl_active} hours_rows={ctrl_hours}")

cleanup()

print(f"VY employee-delete  (branch {B}, month {MONTH})")
allpass = True
for label, ok, ev in results:
    print(f"  {'PASS' if ok else 'FAIL'} — {label} [{ev}]")
    allpass = allpass and ok
print("RESULT:", "ALL PASS" if allpass else "FAILURES PRESENT")

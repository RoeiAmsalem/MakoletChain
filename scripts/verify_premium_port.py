"""Read-only verification for the OT/Shabbat premium-pay port.

Runs under the live Flask app context and uses the SAME functions the
endpoints use (_calculate_salary_cost = branch KPI; _employee_premium_costs =
the per-employee list pass) so the Σ(per-employee)==KPI reconcile is checked at
the source both /api/summary and /api/employees consume.

Usage: python scripts/verify_premium_port.py [BRANCH:MONTH ...]
Default targets: 126 & 127 for 2026-05 and 2026-06, plus a flat check.
Writes NOTHING.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root
from app import app, get_db, _calculate_salary_cost, _employee_premium_costs  # noqa: E402


def _global_total(db, branch_id):
    row = db.execute(
        "SELECT COALESCE(SUM(global_salary),0) AS g FROM employees "
        "WHERE branch_id=? AND active=1 AND salary_type='global'",
        (branch_id,)).fetchone()
    return round(row['g'] or 0, 2)


def check(branch_id, month):
    db = get_db()
    kpi = _calculate_salary_cost(branch_id, month)
    per_emp = _employee_premium_costs(branch_id, month, db)
    sum_hourly = round(sum(v['salary'] for v in per_emp.values()), 2)
    gtot = _global_total(db, branch_id)
    recon = round(sum_hourly + gtot, 2)
    kpi_amt = round(kpi['amount'], 2)
    ok = abs(recon - kpi_amt) < 0.01
    print(f"  branch {branch_id} {month}: KPI=₪{kpi_amt:,.2f} | "
          f"Σper-emp(₪{sum_hourly:,.2f})+globals(₪{gtot:,.2f})=₪{recon:,.2f} "
          f"-> {'RECONCILE OK' if ok else 'MISMATCH ✗'}  ({len(per_emp)} hourly emps)")
    return ok, kpi_amt, per_emp


def detail(branch_id, month):
    """Per-employee premium vs flat hours×rate, to show who got a premium."""
    db = get_db()
    per_emp = _employee_premium_costs(branch_id, month, db)
    rates = {r['name']: (r['hourly_rate'] or 0) for r in db.execute(
        "SELECT name, hourly_rate FROM employees WHERE branch_id=? AND active=1",
        (branch_id,)).fetchall()}
    for name, v in sorted(per_emp.items(), key=lambda kv: -kv[1]['salary']):
        rate = rates.get(name, 0)
        flat = round((v['hours'] or 0) * rate, 2)
        delta = round(v['salary'] - flat, 2)
        tag = f"  +₪{delta:,.2f} PREMIUM" if delta > 0.01 else "  (flat)"
        print(f"      {name}: {v['hours']:.2f}h  flat=₪{flat:,.2f}  "
              f"paid=₪{v['salary']:,.2f}{tag}")


if __name__ == '__main__':
    targets = sys.argv[1:] or ['126:2026-05', '126:2026-06',
                               '127:2026-05', '127:2026-06']
    with app.app_context():
        print("=== reconcile (Σ per-employee + globals == branch KPI) ===")
        all_ok = True
        for t in targets:
            b, m = t.split(':')
            ok, _, _ = check(int(b), m)
            all_ok = all_ok and ok
        print(f"\n=== per-employee premium detail (127 May, 126 June) ===")
        detail(127, '2026-05')
        print("    ---")
        detail(126, '2026-06')
        print(f"\nRECONCILE ALL: {'PASS' if all_ok else 'FAIL'}")
        sys.exit(0 if all_ok else 1)

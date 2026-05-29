"""End-to-end check: hit the real /api/department-sales-monthly route via
Flask's test client (logged-in admin session) for a few branches and print
the JSON. Compare against scripts/verify_dept_monthly.py output.
"""
import json
from app import app

CHECK = [(126, '2026-05'), (127, '2026-05'), (9002, '2026-05'), (9014, '2026-05')]

with app.test_client() as c:
    for bid, month in CHECK:
        with c.session_transaction() as s:
            s['user_id'] = 1
            s['user_role'] = 'admin'
            s['branch_id'] = bid
            s['selected_month'] = month
        r = c.get(f'/api/department-sales-monthly?month={month}')
        d = r.get_json()
        print(f"\nbranch {bid} {month}  HTTP {r.status_code}  days={d['days_counted']}")
        for t in d['tiles']:
            print(f"  {t['code']:>2} {t['label']:<8} avg_pct={t['avg_pct']}  total=₪{t['total']}  accent={t['accent']}")

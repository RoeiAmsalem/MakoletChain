"""Verify /network/employees-v2 on staging via Flask test client.

Forges admin + manager sessions (no password needed, no auth/data mutation) and
asserts: page loads, role scoping, ranked list, click-through to real /employees
content, greyed missing rows, honest coverage. Run on the server via SSH.
"""
import json
import app as A

client = A.app.test_client()


def login_as(user_id, role, branches):
    with client.session_transaction() as s:
        s['user_id'] = user_id
        s['user_role'] = role
        s['user_name'] = f'test-{role}'
        s['user_branches'] = branches
        if branches:
            s['branch_id'] = branches[0]


def find_users():
    with A.app.app_context():
        db = A.get_db()
        admin = db.execute(
            "SELECT id FROM users WHERE role IN ('admin','ceo') AND active=1 ORDER BY id LIMIT 1"
        ).fetchone()
        # a manager whose user_branches points at 127 (has employee data)
        mgr = db.execute(
            "SELECT u.id, ub.branch_id FROM users u "
            "JOIN user_branches ub ON ub.user_id=u.id "
            "WHERE u.role='manager' AND u.active=1 AND ub.branch_id=127 LIMIT 1"
        ).fetchone()
        return admin['id'], (mgr['id'] if mgr else None)


PASS, FAIL = [], []
def check(label, cond, detail=''):
    (PASS if cond else FAIL).append(f"{label}: {'PASS' if cond else 'FAIL'} {detail}")


admin_id, mgr127_id = find_users()

# ── ADMIN — sees all 18 ───────────────────────────────────────
login_as(admin_id, 'admin', [])

r = client.get('/network/employees-v2?mode=network')
html = r.get_data(as_text=True)
check('admin network page 200', r.status_code == 200, f'(got {r.status_code})')
check('toggle present', 'הסניפים שלי' in html and 'סניף בודד' in html)
check('evBody mount present', 'id="evBody"' in html)

r = client.get('/api/network/employees-v2?month=2026-05')
d = json.loads(r.get_data(as_text=True))
check('admin api total_branches=18', d.get('total_branches') == 18, f"(got {d.get('total_branches')})")
check('admin api reported=3', d.get('reported') == 3, f"(got {d.get('reported')})")
rep_ids = sorted(b['branch_id'] for b in d.get('per_branch', []))
check('admin api reported ids = 126,127,9001', rep_ids == [126, 127, 9001], f'(got {rep_ids})')
check('admin api ranked desc', [b['salary'] for b in d['per_branch']] ==
      sorted((b['salary'] for b in d['per_branch']), reverse=True))
check('admin api chain total > 0', d.get('chain_salary_total', 0) > 0,
      f"(₪{d.get('chain_salary_total')})")
check('admin api avg = total/reported', d.get('avg_per_store') ==
      round(d['chain_salary_total'] / d['reported'], 2))
miss = d.get('missing', [])
check('admin api missing = 15', len(miss) == 15, f'(got {len(miss)})')
check('admin api missing carry pending counts', any(m.get('pending', 0) > 0 for m in miss))

# ── ADMIN single mode — real /employees content for a store WITH data ──
r = client.get('/network/employees-v2?mode=single&store=126&month=2026-05')
html = r.get_data(as_text=True)
check('admin single 200', r.status_code == 200, f'(got {r.status_code})')
check('single reuses /employees content (emp-grid)', 'id="emp-grid"' in html)
check('single reuses /employees charts (chart-hours)', 'id="chart-hours"' in html)
check('single BRANCH_ID = picked store 126', 'const BRANCH_ID = 126;' in html,
      '(BRANCH_ID line)')
check('single store picker present', 'id="evStore"' in html)

# ── MANAGER (branch 127) — scoping, no leak ───────────────────
if mgr127_id:
    login_as(mgr127_id, 'manager', [127])
    r = client.get('/api/network/employees-v2?month=2026-05')
    d = json.loads(r.get_data(as_text=True))
    ids = [b['branch_id'] for b in d.get('per_branch', [])]
    check('mgr127 api total_branches=1', d.get('total_branches') == 1, f"(got {d.get('total_branches')})")
    check('mgr127 api sees only 127', ids == [127], f'(got {ids})')

    # Attempt to view another store via URL → must fall back to own (no leak)
    r = client.get('/network/employees-v2?mode=single&store=126&month=2026-05')
    html = r.get_data(as_text=True)
    check('mgr127 cannot open store 126 (forced to own 127)',
          'const BRANCH_ID = 127;' in html and 'const BRANCH_ID = 126;' not in html,
          '(no leak)')
    # Single-store manager → no toggle
    check('mgr127 single-store: no toggle shown', 'הסניפים שלי' not in html)
else:
    check('manager 127 lookup', False, '(no manager bound to branch 127 — skipped scoping test)')

# ── /employees itself unchanged (still renders after refactor) ──
login_as(admin_id, 'admin', [])
r = client.get('/employees?month=2026-05')
check('/employees still 200 after partial split', r.status_code == 200, f'(got {r.status_code})')
check('/employees has emp-grid + chart-hours', 'id="emp-grid"' in r.get_data(as_text=True)
      and 'id="chart-hours"' in r.get_data(as_text=True))

print('\n'.join(PASS))
print('\n'.join(FAIL) if FAIL else '')
print(f"\n{len(PASS)}/{len(PASS)+len(FAIL)} passed")

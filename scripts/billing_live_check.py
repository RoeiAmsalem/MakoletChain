"""Live paywall enforcement check against the REAL app + DB (read-only).

Uses Flask's test_client with a minted session (no passwords touched) to
prove, on the deployed code and production data, that:
  - a LOCKED manager's page request redirects to /account (and API → 402)
  - a paid-up manager gets 200 with NO warning banner

Usage: venv/bin/python scripts/billing_live_check.py <locked_uid> <paid_uid>
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app import app, get_db  # noqa: E402


def _check(client, uid, name, role, email, branch_id):
    with client.session_transaction() as s:
        s['user_id'] = uid
        s['user_name'] = name
        s['user_role'] = role
        s['user_email'] = email
        s['branch_id'] = branch_id
    page = client.get('/sales')
    api = client.get('/api/summary')
    banner = ('billing-warning-banner'
              in client.get('/sales', follow_redirects=True).get_data(as_text=True))
    return {'uid': uid, 'name': name, 'page_status': page.status_code,
            'page_location': page.headers.get('Location'),
            'api_status': api.status_code, 'banner': banner}


def main(locked_uid, paid_uid):
    app.config['TESTING'] = True
    with app.app_context():
        db = get_db()
        rows = {r['id']: r for r in db.execute(
            "SELECT u.id, u.name, u.role, u.email, "
            "(SELECT branch_id FROM user_branches ub WHERE ub.user_id=u.id "
            " LIMIT 1) AS branch_id FROM users u WHERE u.id IN (?,?)",
            (locked_uid, paid_uid)).fetchall()}
    for uid, label in ((locked_uid, 'locked'), (paid_uid, 'paid')):
        r = rows[uid]
        with app.test_client() as c:
            res = _check(c, r['id'], r['name'], r['role'], r['email'],
                         r['branch_id'])
        print(f"[{label}] {res}")


if __name__ == '__main__':
    main(int(sys.argv[1]), int(sys.argv[2]))

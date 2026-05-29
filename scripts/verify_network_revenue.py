"""E2E verification for the chain-wide daily revenue headline.

Uses the Flask test client with simulated sessions (login_required only checks
session['user_id']) to exercise /api/network/revenue and /network/revenue as
admin, ceo, and manager. Reconciles the API totals against an independent
daily_sales SUM. Read-only — no DB writes. Run on staging:

    ssh makolet-chain "cd /opt/makolet-chain-staging && venv/bin/python scripts/verify_network_revenue.py"
"""
import sys
from app import app, get_db

PROBE_DATE = '2026-05-28'  # known good day on staging (17/18 reported)


def line(step, ok, detail):
    print(f"{step}: {'PASS' if ok else 'FAIL'} — {detail}")
    return ok


def main():
    results = []
    with app.app_context():
        db = get_db()
        active = db.execute("SELECT id, name FROM branches WHERE active=1 ORDER BY id").fetchall()
        active_ids = [r['id'] for r in active]
        total_branches = len(active)
        mgr = db.execute(
            "SELECT u.id FROM users u JOIN user_branches ub ON ub.user_id=u.id "
            "WHERE u.role='manager' LIMIT 1").fetchone()
        admin = db.execute("SELECT id FROM users WHERE role='admin' LIMIT 1").fetchone()
        admin_id = admin['id'] if admin else 1
        mgr_id = mgr['id'] if mgr else None

        # Independent reconciliation total for PROBE_DATE
        ph = ','.join('?' * len(active_ids))
        recon = db.execute(
            f"SELECT COALESCE(SUM(amount),0) t, COUNT(DISTINCT branch_id) c "
            f"FROM daily_sales WHERE date=? AND branch_id IN ({ph})",
            [PROBE_DATE] + active_ids).fetchone()
        recon_total = round(float(recon['t'] or 0), 2)
        recon_count = recon['c']

    client = app.test_client()

    def as_role(role, user_id):
        with client.session_transaction() as s:
            s.clear()
            s['user_id'] = user_id
            s['user_role'] = role

    # 1. Admin API, explicit probe date
    as_role('admin', admin_id)
    r = client.get(f'/api/network/revenue?date={PROBE_DATE}')
    d = r.get_json()
    results.append(line("STEP 1 (admin API 200)", r.status_code == 200, f"status={r.status_code}"))

    sum_rows = round(sum(b['amount'] for b in d['per_branch']), 2)
    results.append(line("STEP 2 (hero = sum of per-branch rows)",
                        d['chain_total'] == sum_rows,
                        f"chain_total={d['chain_total']} sum(rows)={sum_rows}"))

    results.append(line("STEP 3 (chain_total reconciles to daily_sales SUM)",
                        d['chain_total'] == recon_total,
                        f"api={d['chain_total']} db={recon_total}"))

    results.append(line("STEP 4 (coverage count accurate)",
                        d['reported'] == recon_count and d['reported'] == len(d['per_branch'])
                        and d['reported'] + len(d['missing']) == total_branches == d['total_branches'],
                        f"reported={d['reported']} db_count={recon_count} missing={len(d['missing'])} total={total_branches}"))

    amounts = [b['amount'] for b in d['per_branch']]
    sorted_desc = amounts == sorted(amounts, reverse=True)
    results.append(line("STEP 5 (ranked strip sorted desc)", sorted_desc,
                        f"first={amounts[0] if amounts else None} last={amounts[-1] if amounts else None}"))

    top_ok = d['top'] == d['per_branch'][0] and d['bottom'] == d['per_branch'][-1]
    results.append(line("STEP 6 (top/bottom callouts correct)", top_ok,
                        f"top={d['top']['branch_name']}({d['top']['amount']}) bottom={d['bottom']['branch_name']}({d['bottom']['amount']})"))

    missing_names = [m['branch_name'] for m in d['missing']]
    results.append(line("STEP 7 (missing branches named)", len(missing_names) > 0 or recon_count == total_branches,
                        f"missing={missing_names}"))

    series_ok = len(d['series_7d']) == 7 and d['series_7d'][-1]['date'] == PROBE_DATE
    results.append(line("STEP 8 (7-day sparkline series)", series_ok,
                        f"len={len(d['series_7d'])} last={d['series_7d'][-1]['date'] if d['series_7d'] else None}"))

    # 9. Default date = most recent day with data (no date param)
    r2 = client.get('/api/network/revenue')
    d2 = r2.get_json()
    results.append(line("STEP 9 (default date = latest with data, not blank today)",
                        d2['date'] is not None and d2['reported'] > 0,
                        f"date={d2['date']} reported={d2['reported']}"))

    # 10. CEO can access
    as_role('ceo', admin_id)
    rc = client.get(f'/api/network/revenue?date={PROBE_DATE}')
    results.append(line("STEP 10 (ceo API 200)", rc.status_code == 200, f"status={rc.status_code}"))

    # 11. Manager blocked from API (403)
    if mgr_id:
        as_role('manager', mgr_id)
        rm = client.get(f'/api/network/revenue?date={PROBE_DATE}')
        results.append(line("STEP 11 (manager API 403)", rm.status_code == 403, f"status={rm.status_code}"))

        # 12. Manager blocked from page (302 redirect to /)
        rp = client.get('/network/revenue')
        loc = rp.headers.get('Location', '')
        results.append(line("STEP 12 (manager page redirected)",
                            rp.status_code == 302 and loc.endswith('/'),
                            f"status={rp.status_code} loc={loc}"))
    else:
        results.append(line("STEP 11-12 (manager checks)", False, "no manager user found"))

    # 13. Admin page renders
    as_role('admin', admin_id)
    rpg = client.get('/network/revenue')
    body = rpg.get_data(as_text=True)
    results.append(line("STEP 13 (admin page 200 + renders)",
                        rpg.status_code == 200 and 'הכנסות רשת' in body and 'nrBody' in body,
                        f"status={rpg.status_code}"))

    print()
    failed = [i for i, ok in enumerate(results) if not ok]
    if failed:
        print(f"ANOMALIES: {len(failed)} step(s) failed")
        sys.exit(1)
    print(f"ALL {len(results)} CHECKS PASSED")


if __name__ == '__main__':
    main()

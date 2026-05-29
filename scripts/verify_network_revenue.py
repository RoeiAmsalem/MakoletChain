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
        admin = db.execute("SELECT id FROM users WHERE role='admin' LIMIT 1").fetchone()
        ceo = db.execute("SELECT id FROM users WHERE role='ceo' LIMIT 1").fetchone()
        admin_id = admin['id'] if admin else 1
        ceo_id = ceo['id'] if ceo else None

        # Branch-count per manager → pick a multi-store and a single-store one.
        mgr_rows = db.execute(
            "SELECT u.id, COUNT(ub.branch_id) n FROM users u "
            "JOIN user_branches ub ON ub.user_id=u.id "
            "WHERE u.role='manager' GROUP BY u.id").fetchall()
        multi_mgr = next((r['id'] for r in mgr_rows if r['n'] >= 2), None)
        single_mgr = next((r['id'] for r in mgr_rows if r['n'] == 1), None)
        mgr_id = (multi_mgr or single_mgr or
                  (mgr_rows[0]['id'] if mgr_rows else None))

        def _mgr_branch_ids(uid):
            return {r['branch_id'] for r in db.execute(
                "SELECT branch_id FROM user_branches WHERE user_id=?", (uid,)).fetchall()}
        multi_branches = _mgr_branch_ids(multi_mgr) if multi_mgr else set()
        single_branches = _mgr_branch_ids(single_mgr) if single_mgr else set()

        # Independent reconciliation totals for PROBE_DATE
        ph = ','.join('?' * len(active_ids))
        recon = db.execute(
            f"SELECT COALESCE(SUM(amount),0) t, COALESCE(SUM(transactions),0) txn, "
            f"COUNT(DISTINCT branch_id) c "
            f"FROM daily_sales WHERE date=? AND branch_id IN ({ph})",
            [PROBE_DATE] + active_ids).fetchone()
        recon_total = round(float(recon['t'] or 0), 2)
        recon_txn = int(recon['txn'] or 0)
        recon_count = recon['c']
        # Month-to-date (2026-05-01 → PROBE_DATE)
        recon_mtd = round(float(db.execute(
            f"SELECT COALESCE(SUM(amount),0) t FROM daily_sales "
            f"WHERE date BETWEEN '2026-05-01' AND ? AND branch_id IN ({ph})",
            [PROBE_DATE] + active_ids).fetchone()['t'] or 0), 2)

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

    series_ok = len(d['series_14d']) == 14 and d['series_14d'][-1]['date'] == PROBE_DATE
    results.append(line("STEP 8 (14-day trend series)", series_ok,
                        f"len={len(d['series_14d'])} last={d['series_14d'][-1]['date'] if d['series_14d'] else None}"))

    # 8b. New metrics reconcile
    results.append(line("STEP 8b (transactions reconcile)",
                        d['total_transactions'] == recon_txn,
                        f"api={d['total_transactions']} db={recon_txn}"))
    exp_basket = round(d['chain_total'] / recon_txn, 2) if recon_txn else 0
    results.append(line("STEP 8c (avg basket = total/txns)",
                        d['avg_basket'] == exp_basket,
                        f"api={d['avg_basket']} expected={exp_basket}"))
    exp_avg_store = round(d['chain_total'] / d['reported'], 2) if d['reported'] else 0
    results.append(line("STEP 8d (avg per store = total/reporting)",
                        d['avg_per_store'] == exp_avg_store,
                        f"api={d['avg_per_store']} expected={exp_avg_store}"))
    results.append(line("STEP 8e (month-to-date reconciles)",
                        d['month_to_date_total'] == recon_mtd,
                        f"api={d['month_to_date_total']} db={recon_mtd}"))

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

    # ── /network/revenue-v2 (experimental toggle page) ──
    # 14. Admin v2 API: 200, scoped to ALL active branches.
    as_role('admin', admin_id)
    rv = client.get('/api/network/revenue-v2').get_json()
    results.append(line("STEP 14 (v2 API admin = all branches)",
                        rv.get('total_branches') == total_branches,
                        f"total_branches={rv.get('total_branches')} active={total_branches}"))

    # 15. CEO v2 API: 200, all branches.
    if ceo_id:
        as_role('ceo', ceo_id)
        rvc = client.get('/api/network/revenue-v2')
        results.append(line("STEP 15 (v2 API ceo 200 + all branches)",
                            rvc.status_code == 200 and rvc.get_json().get('total_branches') == total_branches,
                            f"status={rvc.status_code} total={rvc.get_json().get('total_branches')}"))

    # 16. Multi-store manager v2 API: 200 (NOT 403), scoped to ONLY his stores.
    if multi_mgr:
        as_role('manager', multi_mgr)
        rmv = client.get('/api/network/revenue-v2')
        dm = rmv.get_json()
        seen = {b['branch_id'] for b in dm['per_branch']} | {m['branch_id'] for m in dm['missing']}
        results.append(line("STEP 16 (v2 API manager scoped to own stores only)",
                            rmv.status_code == 200 and dm['total_branches'] == len(multi_branches)
                            and seen.issubset(multi_branches),
                            f"total={dm['total_branches']} own={len(multi_branches)} leak={seen - multi_branches}"))

        # 17. Multi-store manager page → toggle shown.
        rmp = client.get('/network/revenue-v2').get_data(as_text=True)
        toggle_in_rmp = 'class="rev2-toggle"' in rmp
        results.append(line("STEP 17 (multi-store manager sees toggle)",
                            toggle_in_rmp and 'הרשת שלי' in rmp and 'סניף בודד' in rmp,
                            f"toggle_present={toggle_in_rmp}"))

    # 18. Single-store manager page → NO toggle, lands on single store w/ reused /sales content.
    if single_mgr:
        as_role('manager', single_mgr)
        rsp = client.get('/network/revenue-v2').get_data(as_text=True)
        toggle_absent = 'class="rev2-toggle"' not in rsp
        results.append(line("STEP 18 (single-store manager: no toggle, single mode)",
                            toggle_absent and 'sales-tfoot' in rsp,
                            f"toggle_absent={toggle_absent} sales_content={'sales-tfoot' in rsp}"))

        # 19. Single manager v2 API still scoped to his 1 store.
        dsv = client.get('/api/network/revenue-v2').get_json()
        results.append(line("STEP 19 (single manager v2 API = 1 store)",
                            dsv['total_branches'] == len(single_branches) == 1,
                            f"total={dsv['total_branches']} own={len(single_branches)}"))

    # 20. EXISTING /sales still renders (include refactor didn't break it).
    as_role('admin', admin_id)
    rs = client.get('/sales')
    rsb = rs.get_data(as_text=True)
    results.append(line("STEP 20 (/sales unchanged: 200 + Z content)",
                        rs.status_code == 200 and 'sales-tfoot' in rsb,
                        f"status={rs.status_code} sales_content={'sales-tfoot' in rsb}"))

    # 21. v2 admin network mode renders aggregate dashboard + toggle.
    as_role('admin', admin_id)
    rv2p = client.get('/network/revenue-v2?mode=network').get_data(as_text=True)
    results.append(line("STEP 21 (v2 admin network mode renders)",
                        'nrBody' in rv2p and 'class="rev2-toggle"' in rv2p and 'revenue-v2' in rv2p,
                        f"dashboard={'nrBody' in rv2p}"))

    # 22. Network-mode rows are wired to single-store links (clickable).
    results.append(line("STEP 22 (ranked rows link to single mode)",
                        'mode=single&store=' in rv2p and 'a.nr-row' in rv2p,
                        f"link_wiring={'mode=single&store=' in rv2p}"))

    # 23. Clicking a reporting branch → single mode for THAT exact branch (real /sales content).
    target = d['per_branch'][0]['branch_id']  # top reporting store
    rclick = client.get(f'/network/revenue-v2?mode=single&store={target}').get_data(as_text=True)
    results.append(line("STEP 23 (click branch → its single-store detail)",
                        f'const BRANCH_ID = {target};' in rclick and 'sales-tfoot' in rclick,
                        f"branch_id_rendered={f'const BRANCH_ID = {target};' in rclick}"))

    # 24. Manager can open OWN store; a foreign store falls back to one of theirs (no cross-tenant).
    if multi_mgr:
        own = sorted(multi_branches)[0]
        foreign = next((b for b in active_ids if b not in multi_branches), None)
        as_role('manager', multi_mgr)
        rown = client.get(f'/network/revenue-v2?mode=single&store={own}').get_data(as_text=True)
        rforeign = client.get(f'/network/revenue-v2?mode=single&store={foreign}').get_data(as_text=True)
        foreign_rendered = f'const BRANCH_ID = {foreign};' in rforeign
        results.append(line("STEP 24 (manager opens own store, foreign store blocked)",
                            f'const BRANCH_ID = {own};' in rown and not foreign_rendered,
                            f"own_ok={f'const BRANCH_ID = {own};' in rown} foreign_leaked={foreign_rendered}"))

    print()
    failed = [i for i, ok in enumerate(results) if not ok]
    if failed:
        print(f"ANOMALIES: {len(failed)} step(s) failed")
        sys.exit(1)
    print(f"ALL {len(results)} CHECKS PASSED")


if __name__ == '__main__':
    main()

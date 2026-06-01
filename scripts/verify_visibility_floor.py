#!/usr/bin/env python3
"""Verify the per-branch visibility FLOOR against the live (staging) DB.

Forges an admin session (admin can switch branches via ?branch_id=) and
exercises the real endpoints for:
  - a FLOORED chain branch that actually has pre-June data, and
  - branch 126 (NULL visible_from = no floor).

Run on the server:  /opt/makolet-chain-staging/venv/bin/python \
                     /opt/makolet-chain-staging/scripts/verify_visibility_floor.py
"""
import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as m

FLOOR_DATE = '2026-06-01'
FLOOR_MONTH = '2026-06'
PRE = '2026-05'   # a pre-floor month to probe

results = []


def check(label, ok, detail=''):
    results.append(ok)
    tag = 'PASS' if ok else 'FAIL'
    print(f'{tag} — {label}{(": " + detail) if detail else ""}')


def main():
    db = sqlite3.connect(m.DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row

    # 1. Schema + data setup
    cols = [r['name'] for r in db.execute('PRAGMA table_info(branches)')]
    check('branches.visible_from column exists', 'visible_from' in cols)

    floored = [r['id'] for r in db.execute(
        "SELECT id FROM branches WHERE visible_from IS NOT NULL ORDER BY id")]
    exempt = [r['id'] for r in db.execute(
        "SELECT id FROM branches WHERE visible_from IS NULL ORDER BY id")]
    print(f'  floored branches : {floored}')
    print(f'  exempt (NULL)    : {exempt}')
    check('126 is exempt (NULL floor)', 126 in exempt)
    check('127 is exempt (NULL floor)', 127 in exempt)
    check('no chain branch (9001-9020) is exempt',
          not any(9001 <= b <= 9020 for b in exempt))

    # Pick a floored branch that genuinely has pre-June revenue to prove hiding.
    probe = db.execute(
        "SELECT b.id AS id, COUNT(*) AS n FROM branches b "
        "JOIN daily_sales d ON d.branch_id = b.id "
        "WHERE b.visible_from IS NOT NULL AND d.date < ? "
        "GROUP BY b.id ORDER BY n DESC LIMIT 1", (FLOOR_DATE,)).fetchone()
    admin = db.execute(
        "SELECT id, email FROM users WHERE role = 'admin' ORDER BY id LIMIT 1").fetchone()
    db.close()

    if not admin:
        check('found an admin user to forge a session', False)
        return
    if not probe:
        print('  (no floored branch has pre-June daily_sales — hiding check skipped)')

    m.app.config['TESTING'] = True
    c = m.app.test_client()
    with c.session_transaction() as s:
        s['user_id'] = admin['id']
        s['user_email'] = admin['email']
        s['user_role'] = 'admin'
        s['user_branches'] = []

    def get(path, bid, month=None):
        q = f'?branch_id={bid}' + (f'&month={month}' if month else '')
        return json.loads(c.get(path + q).data)

    # 2. FLOORED branch hides pre-June, shows June+
    if probe:
        fb = probe['id']
        print(f'  probing floored branch {fb} (has {probe["n"]} pre-June Z rows)')
        s_may = get('/api/summary', fb, PRE)
        check(f'floored {fb}: /api/summary May income=0',
              s_may['income'] == 0, f"income={s_may['income']}")
        check(f'floored {fb}: /api/summary May goods=0',
              s_may['goods'] == 0, f"goods={s_may['goods']}")
        sales_may = get('/api/sales', fb, PRE)
        check(f'floored {fb}: /api/sales May empty',
              sales_may['days'] == 0, f"days={sales_may['days']}")
        hist = get('/api/history', fb, FLOOR_MONTH)
        below = [h['month'] for h in hist if h['month'] < FLOOR_MONTH]
        check(f'floored {fb}: /api/history has no pre-June month',
              not below, f'leaked={below}')
        fx_may = get('/api/fixed-expenses', fb, PRE)
        check(f'floored {fb}: /api/fixed-expenses May empty',
              fx_may == [], f'rows={len(fx_may)}')

    # 3. 126 keeps full history (NULL floor)
    s126 = db_has_pre = None
    d126 = sqlite3.connect(m.DB_PATH)
    pre126 = d126.execute(
        "SELECT COUNT(*) FROM daily_sales WHERE branch_id=126 AND date < ?",
        (FLOOR_DATE,)).fetchone()[0]
    d126.close()
    if pre126:
        sales126 = get('/api/sales', 126, PRE)
        check('126: pre-June /api/sales still visible',
              sales126['days'] > 0, f"days={sales126['days']}")
    else:
        print('  (126 has no pre-June daily_sales in this DB — visibility check skipped)')

    print()
    total, passed = len(results), sum(results)
    print(f'{"ALL PASS" if total == passed else "SOME FAILED"}: {passed}/{total}')
    sys.exit(0 if total == passed else 1)


if __name__ == '__main__':
    main()

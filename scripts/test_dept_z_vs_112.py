# -*- coding: utf-8 -*-
"""READ-ONLY accuracy test: report 112 (current dept source) vs the 902 Z report's
department section, all 18 chain branches, over a common period.

We switched dept sourcing 902 → 112 because 902 lacked departments for some stores
(9016/9018/9019). This validates that switch: where the 902 DOES carry a dept
section, 112 must agree to the cent (both incl-VAT per our controls).

Per branch, over a period BOTH sources cover — prefer completed May 2026, fall back
to June MTD if May has no data — SAME dates both sides:
  • Z side  : 902 chain-Z over the period's Z-range (min..max Z), parsed by the
              existing dormant parse_902_xls_departments → {dept_code: amount}.
  • 112 side: report 112 over the same date range → {dept_code: sale_incl_vat}.
  • Compare by dept_code; flag only-in-Z / only-in-112; per matched dept Z.amount
    vs 112.sale_incl_vat (BOTH incl-VAT). Tolerance ₪1.

Verdicts: MATCH · MISMATCH · Z-EMPTY (902 no dept section → 112-only) · BROKEN
(Aviv 404 / no data either side).

100% READ-ONLY: Aviv GET/POST report-result (generates a report file, no DB/state
write) + SELECT only. Reuses the existing fetch path + dormant parser; changes NO
agent code. Throttled.

Usage (prod):
  venv/bin/python scripts/test_dept_z_vs_112.py                 # all 18
  venv/bin/python scripts/test_dept_z_vs_112.py --branch-id 126 # one branch
  venv/bin/python scripts/test_dept_z_vs_112.py --month 2026-05
"""
import argparse
import calendar
import os
import sqlite3
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import requests                                  # noqa: E402
import agents.aviv_z_report as zr                # noqa: E402

IL = ZoneInfo('Asia/Jerusalem')
TOL = 1.0                                         # ₪1 rounding tolerance
THROTTLE = 1.2                                    # seconds between branches


def conn_ro():
    c = sqlite3.connect('file:' + os.path.abspath(zr.DB_PATH) + '?mode=ro',
                        uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def norm(s):
    return ' '.join((s or '').split()).strip()


def submit_902_range(aviv_id, from_z, to_z, token):
    """Mirror zr.submit_902 but for a Z-RANGE (month aggregation). Read-only:
    posts to reports/result which generates a report file, writes no state."""
    body = zr.build_submit_body(from_z, to_z, output_type='XLS')
    url = f'{zr.BASE}/reports/result/?branch={aviv_id}'
    r = requests.post(url, json=body,
                      headers={'Authtoken': token, 'Content-Type': 'application/json'},
                      timeout=60, verify=False)
    if r.status_code == 401:
        raise zr.AuthExpired('reports/result 401')
    r.raise_for_status()
    j = r.json()
    if not j.get('url'):
        raise RuntimeError(f'reports/result missing url: {j}')
    return j['url']


def zs_in_period(aviv_id, token, from_date, to_date):
    """Resolve the 902 Z-list and return the (z_number, date) entries whose date
    falls in [from_date, to_date]. Raises on Aviv error (caller → BROKEN)."""
    filters = zr.fetch_902_z_list(aviv_id, token)
    out = [e for e in zr._iter_z_entries(filters)
           if e.get('date') and from_date <= e['date'] <= to_date]
    out.sort(key=lambda e: e['date'])
    return out


def depts_902(aviv_id, token, zlist):
    """902 dept section over the Z-range min..max → {dept_code: (name, amount)}."""
    if not zlist:
        return {}
    from_z = zlist[0]['z_number']
    to_z = zlist[-1]['z_number']
    url = submit_902_range(aviv_id, from_z, to_z, token)
    time.sleep(0.4)
    xls = zr.download_xls(url, token)
    rows = zr.parse_902_xls_departments(xls)
    return {d['dept_code']: (d['dept_name'], round(d['amount'], 2)) for d in rows}


def depts_112(aviv_id, token, from_date, to_date):
    """Report 112 over the date range → {dept_code: (name, sale_incl_vat)}."""
    url = zr.submit_112(aviv_id, from_date, to_date, token)
    time.sleep(0.4)
    xls = zr.download_xls(url, token)
    rows = zr.parse_112_departments(xls)
    return {d['dept_code']: (d['dept_name'], round(d['sale_incl_vat'], 2)) for d in rows}


def compare(z, m):
    """Return (matched, mism_list, only_z, only_112, sum_z, sum_112)."""
    codes = sorted(set(z) | set(m))
    matched = 0
    mism = []
    only_z = []
    only_m = []
    for c in codes:
        zr_ = z.get(c)
        mr = m.get(c)
        if zr_ and not mr:
            only_z.append((c, zr_[0], zr_[1]))
        elif mr and not zr_:
            only_m.append((c, mr[0], mr[1]))
        else:
            matched += 1
            delta = round(mr[1] - zr_[1], 2)
            if abs(delta) > TOL:
                mism.append((c, mr[0], zr_[1], mr[1], delta))
    sum_z = round(sum(v[1] for v in z.values()), 2)
    sum_m = round(sum(v[1] for v in m.values()), 2)
    return matched, mism, only_z, only_m, sum_z, sum_m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--branch-id', type=int)
    ap.add_argument('--month', default='2026-05')
    a = ap.parse_args()

    y, mo = int(a.month[:4]), int(a.month[5:7])
    may_from = f'{a.month}-01'
    may_to = f'{a.month}-{calendar.monthrange(y, mo)[1]:02d}'
    today = datetime.now(IL).strftime('%Y-%m-%d')
    jun_from, jun_to = '2026-06-01', today

    c = conn_ro()
    q = ("SELECT id, name, aviv_branch_id FROM branches "
         "WHERE active=1 AND id NOT IN (9998,9999) AND aviv_branch_id IS NOT NULL ")
    if a.branch_id:
        q += "AND id=? "
        rows = c.execute(q + "ORDER BY id", (a.branch_id,)).fetchall()
    else:
        rows = c.execute(q + "ORDER BY id").fetchall()
    c.close()

    excl = zr.EXCLUDED_CHAIN_AVIV_IDS
    branches = [r for r in rows if r['aviv_branch_id'] not in excl]
    print(f"branches: {len(branches)} | tol ₪{TOL:.0f} | May={may_from}..{may_to} "
          f"fallback June MTD={jun_from}..{jun_to}\n")

    token = zr._refresh(zr._login_chain_account())
    results = []
    mismatch_detail = []

    hdr = (f"{'branch':<22} {'period':<14} {'Zdpt':>4} {'112dpt':>6} "
           f"{'ΣZ':>11} {'Σ112':>11} {'Δ':>9}  verdict")
    print(hdr)
    print('-' * len(hdr))

    for b in branches:
        bid, aviv, name = b['id'], b['aviv_branch_id'], b['name']
        label = f"{bid} {name[:16]}"
        try:
            token = zr._refresh(token)
        except zr.AuthExpired:
            token = zr._refresh(zr._login_chain_account())

        def run(period_label, fd, td):
            zlist = zs_in_period(aviv, token, fd, td)
            z = depts_902(aviv, token, zlist) if zlist else {}
            m = depts_112(aviv, token, fd, td)
            return period_label, fd, td, zlist, z, m

        try:
            period, fd, td, zlist, z, m = run(a.month, may_from, may_to)
            # May empty both sides → fall back to June MTD.
            if not zlist and not m:
                time.sleep(THROTTLE)
                period, fd, td, zlist, z, m = run('2026-06-MTD', jun_from, jun_to)

            if not zlist and not m:
                verdict = 'BROKEN'
            elif not z and not zlist:
                verdict = 'Z-EMPTY' if m else 'BROKEN'
            elif not z and zlist:
                verdict = 'Z-EMPTY'      # Z exists but no dept section
            else:
                matched, mism, only_z, only_m, sz, sm = compare(z, m)
                tot_ok = abs(sz - sm) <= TOL
                verdict = 'MATCH' if (not mism and tot_ok) else 'MISMATCH'
                if verdict == 'MISMATCH':
                    mismatch_detail.append((label, period, mism, only_z, only_m, sz, sm))

            sz = round(sum(v[1] for v in z.values()), 2)
            sm = round(sum(v[1] for v in m.values()), 2)
            d = round(sm - sz, 2)
            zdates = f"{zlist[0]['date'][5:]}..{zlist[-1]['date'][5:]}" if zlist else '—'
            print(f"{label:<22} {period:<14} {len(z):>4} {len(m):>6} "
                  f"{sz:>11,.2f} {sm:>11,.2f} {d:>9,.2f}  {verdict}  (Z {zdates})")
            results.append((label, verdict))
        except zr.AuthExpired:
            print(f"{label:<22} {'—':<14} {'—':>4} {'—':>6} {'—':>11} {'—':>11} "
                  f"{'—':>9}  BROKEN (401)")
            results.append((label, 'BROKEN'))
            token = zr._refresh(zr._login_chain_account())
        except Exception as e:
            print(f"{label:<22} {'—':<14} {'—':>4} {'—':>6} {'—':>11} {'—':>11} "
                  f"{'—':>9}  BROKEN ({type(e).__name__}: {str(e)[:40]})")
            results.append((label, 'BROKEN'))
        time.sleep(THROTTLE)

    # Per-dept detail for mismatches.
    if mismatch_detail:
        print("\n=== MISMATCH detail (dept_code | name | Z amount | 112 amount | Δ) ===")
        for label, period, mism, only_z, only_m, sz, sm in mismatch_detail:
            print(f"\n■ {label}  [{period}]  ΣZ {sz:,.2f}  Σ112 {sm:,.2f}")
            for code, nm, za, ma, d in mism:
                print(f"   dept {code:>4} {norm(nm)[:24]:<24} Z {za:>11,.2f}  "
                      f"112 {ma:>11,.2f}  Δ {d:>9,.2f}")
            if only_z:
                print(f"   only-in-Z : {[(c, norm(n)[:16]) for c, n, v in only_z]}")
            if only_m:
                print(f"   only-in-112: {[(c, norm(n)[:16]) for c, n, v in only_m]}")

    # Roll-up.
    from collections import Counter
    tally = Counter(v for _, v in results)
    print("\n=== ROLL-UP ===")
    for k in ('MATCH', 'MISMATCH', 'Z-EMPTY', 'BROKEN'):
        names = [lbl for lbl, v in results if v == k]
        print(f"  {k:<9} {tally.get(k,0):>2}  {names}")
    print("\nNote: 9019 dept named '1' is a real Aviv config (not an error); "
          "name variants across sources are expected — matching is by dept_code.")


if __name__ == '__main__':
    main()

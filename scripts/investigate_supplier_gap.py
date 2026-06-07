"""READ-ONLY: explain the budget-list supplier gap (few shown vs BilBoy's 100+).

A) our DB, all active non-demo branches: budget_list_count (the EXACT list
   _goal_data builds — roster ∪ current-month ∪ budgeted), roster_count,
   distinct non-franchise suppliers over the prior-2-month roster window, and
   all-time.
B) verdict: flag any branch where budget_list_count < suppliers_last_2mo
   (= we'd be dropping recently-active suppliers → real bug).
C) live BilBoy /customer/suppliers master count per branch (count only, throttled,
   N/A on stale token).
D) deep dive 9018 + 126: suppliers that BILLED in BilBoy over the window (same
   status/type/amount/franchise filters the sync applies) but are ABSENT from our
   goods_documents for the same window — the real sync-gap test.

No writes anywhere. Live BilBoy calls are GET-only and throttled. Does NOT modify
bilboy.py — only imports its existing auth/fetch helpers.
"""
import os
import sys
import time
import sqlite3
import calendar
from datetime import date
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as app_module                                  # noqa: E402
from app import _goal_data                                # noqa: E402
from agents.supplier_roster import prior_two_months       # noqa: E402
from agents.bilboy import (                               # noqa: E402  (read-only import)
    _get_branch_config, _branch_session, _api_get,
    ALLOWED_DOC_TYPES, KNOWN_STATUSES, EXCLUDED_STATUSES,
)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
DEEP = [9018, 126]
OLDER, NEWER = prior_two_months()           # e.g. ('2026-04', '2026-05')


def conn_ro():
    c = sqlite3.connect('file:' + os.path.abspath(DB_PATH) + '?mode=ro', uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def is_franchise(name, franchise):
    n = (name or '').strip()
    return (not n) or n == '—' or (franchise and franchise in n)


def distinct_suppliers(c, bid, franchise, months=None):
    if months:
        q = ("SELECT DISTINCT supplier FROM goods_documents WHERE branch_id=? "
             "AND strftime('%Y-%m', doc_date) IN (%s)" % ','.join('?' * len(months)))
        rows = c.execute(q, [bid, *months]).fetchall()
    else:
        rows = c.execute("SELECT DISTINCT supplier FROM goods_documents WHERE branch_id=?",
                         (bid,)).fetchall()
    return {r['supplier'] for r in rows if not is_franchise(r['supplier'], franchise)}


def main():
    c = conn_ro()
    branches = c.execute(
        "SELECT id, name, franchise_supplier, bilboy_branch_id FROM branches "
        "WHERE active=1 AND id NOT IN (9998,9999) ORDER BY id").fetchall()

    print(f"roster window (prior 2 months) = {OLDER} + {NEWER}\n")
    print("PART A — our DB")
    print(f"{'branch':<7} {'name':<22} {'budget_list':>11} {'roster':>7} {'last2mo':>8} {'all_time':>9}")
    partA = {}
    for b in branches:
        bid, fr = b['id'], (b['franchise_supplier'] or '').strip()
        blist = len(_goal_data(bid, c)['suppliers'])
        roster = c.execute("SELECT COUNT(*) k FROM supplier_roster WHERE branch_id=?",
                           (bid,)).fetchone()['k']
        last2 = len(distinct_suppliers(c, bid, fr, [OLDER, NEWER]))
        allt = len(distinct_suppliers(c, bid, fr, None))
        partA[bid] = (blist, roster, last2, allt)
        print(f"{bid:<7} {b['name'][:22]:<22} {blist:>11} {roster:>7} {last2:>8} {allt:>9}")

    print("\nPART B — verdict (budget_list_count < suppliers_last_2mo ⇒ dropping active = BUG)")
    bugs = [bid for bid, (bl, r, l2, at) in partA.items() if bl < l2]
    eq = [bid for bid, (bl, r, l2, at) in partA.items() if bl >= l2]
    if bugs:
        for bid in bugs:
            bl, r, l2, at = partA[bid]
            print(f"  ⚠ branch {bid}: budget_list={bl} < last2mo={l2}  (dropping {l2-bl})")
    else:
        print(f"  OK — budget_list >= suppliers_last_2mo for ALL {len(eq)} branches "
              f"(no recently-active supplier dropped).")

    c.close()

    # ---- live BilBoy helpers ----
    def master_suppliers(bid):
        """(count, [(id,name)] non-franchise) from /customer/suppliers, or (None, [])."""
        b = _get_branch_config(bid)
        bb = b.get('bilboy_branch_id')
        if not bb:
            return None, []
        sess = _branch_session(b, bid)
        raw = _api_get(sess, '/customer/suppliers',
                       params={'customerBranchId': str(bb), 'all': 'true'}, timeout=30)
        sup = raw.get('suppliers') if isinstance(raw, dict) else raw
        sup = sup or []
        fr = (b.get('franchise_supplier') or '').strip()
        keep = []
        for s in sup:
            nm = s.get('title') or s.get('name') or s.get('supplierName') or ''
            sid = str(s.get('id') or s.get('supplierId') or '')
            if sid and not is_franchise(nm, fr):
                keep.append((sid, nm))
        return len(sup), keep

    print("\nPART C — live BilBoy master supplier count (/customer/suppliers, all=true)")
    print(f"{'branch':<7} {'name':<22} {'master_count':>12}")
    master = {}
    for b in branches:
        bid = b['id']
        try:
            cnt, keep = master_suppliers(bid)
            master[bid] = (cnt, keep)
            print(f"{bid:<7} {b['name'][:22]:<22} {('N/A' if cnt is None else cnt):>12}")
        except Exception as e:
            master[bid] = (None, [])
            print(f"{bid:<7} {b['name'][:22]:<22} {'N/A':>12}  ({type(e).__name__})")
        time.sleep(0.6)

    # ---- PART D ----
    def billed_in_window(bid, keep):
        """{supplierName: (doc_count, amount)} for headers in the window that pass
        the sync's status/type/amount/franchise filters."""
        b = _get_branch_config(bid)
        bb = str(b.get('bilboy_branch_id'))
        fr = (b.get('franchise_supplier') or '').strip()
        sess = _branch_session(b, bid)
        ids = [sid for sid, _ in keep]
        last_day = calendar.monthrange(int(NEWER[:4]), int(NEWER[5:7]))[1]
        frm = f"{OLDER}-01T00:00:00"
        to = f"{NEWER}-{last_day:02d}T00:00:00"
        agg = defaultdict(lambda: [0, 0.0])
        for i in range(0, len(ids), 30):
            batch = ids[i:i + 30]
            docs = _api_get(sess, '/customer/docs/headers', params={
                'suppliers': ','.join(batch), 'branches': bb, 'from': frm, 'to': to,
            }, timeout=60)
            docs = docs if isinstance(docs, list) else (
                docs.get('data') or docs.get('docs') or docs.get('headers') or [])
            for d in docs:
                m = str(d.get('date') or d.get('documentDate') or '')[:7]
                if m not in (OLDER, NEWER):
                    continue
                st = d.get('status')
                if st in EXCLUDED_STATUSES or (st is not None and st not in KNOWN_STATUSES):
                    continue
                if d.get('type') not in ALLOWED_DOC_TYPES:
                    continue
                nm = d.get('supplierName') or ''
                if is_franchise(nm, fr):
                    continue
                amt = float(d.get('totalWithVat') or 0)
                if amt == 0:
                    continue
                agg[nm][0] += 1
                agg[nm][1] += amt
            time.sleep(0.5)
        return agg

    print("\nPART D — billed-in-BilBoy-window but ABSENT from our goods_documents (window "
          f"{OLDER}+{NEWER}, franchise + status=9/type/zero excluded)")
    c2 = conn_ro()
    for bid in DEEP:
        b = _get_branch_config(bid)
        fr = (b.get('franchise_supplier') or '').strip()
        ours = distinct_suppliers(c2, bid, fr, [OLDER, NEWER])
        cnt, keep = master.get(bid, (None, []))
        if cnt is None or not keep:
            print(f"  branch {bid}: BilBoy master N/A — skipped")
            continue
        try:
            billed = billed_in_window(bid, keep)
        except Exception as e:
            print(f"  branch {bid}: header fetch failed ({type(e).__name__}) — skipped")
            continue
        missing = {nm: v for nm, v in billed.items() if nm not in ours}
        print(f"  branch {bid} ({b.get('name')}): billed_suppliers={len(billed)} "
              f"ours={len(ours)} MISSING-ACTIVE={len(missing)}")
        if not missing:
            print("     ✓ none — no sync gap; the rest are dormant/configured.")
        for nm, (dc, amt) in sorted(missing.items(), key=lambda kv: -kv[1][1])[:15]:
            print(f"     - {nm[:34]:<34} docs={dc} ₪{amt:,.0f}")
    c2.close()


if __name__ == '__main__':
    main()

# -*- coding: utf-8 -*-
"""READ-ONLY: resolve Dennis's "missing suppliers" complaint, chain-wide.

Dennis (manager of 9015 הגנה + 9018 דפנה) reports a list of suppliers MISSING
from his /goods תקציב budget list, and "מרינה appears twice in 9018". This script
finds the ROOT CAUSE per supplier across all 18 chain branches.

The /goods budget list (app._goal_data) = supplier_roster (distinct BilBoy goods
suppliers over the PRIOR 2 calendar months, franchise-excluded, floor-ignoring)
  ∪ current-month spenders  ∪  budgeted suppliers.
So a supplier is in the list iff it has a goods doc under that EXACT name in the
window (older/newer/current month), is in supplier_roster, or has a saved budget.

TIER 1 — prod goods_documents, all branches: per named supplier, LIKE/fuzzy match,
   bucket ACTIVE / DORMANT / VARIANT / ABSENT, show stores, latest date, amount,
   exact stored name(s), and whether it lands in the budget list.
TIER 2 — prod dedup: normalize names, find variants that collapse to one supplier
   (esp. מרינה in 9018) + a chain-wide duplicate-row count.
TIER 3 — staging product catalog (if present): products filed under זיכיונות whose
   suggested_supplier is one of Dennis's named suppliers = real goods hidden under
   the franchise (the franchise-exclusion smoking gun).

100% READ-ONLY: both DBs opened mode=ro. No writes, no migrate, no deploy, no
BilBoy/network calls. Does NOT import or modify bilboy.py / the sync.

Usage (on the server, which holds prod + staging DBs):
  venv/bin/python scripts/investigate_dennis_suppliers.py
  venv/bin/python scripts/investigate_dennis_suppliers.py \
      --prod-db /opt/makolet-chain/db/makolet_chain.db \
      --staging-db /opt/makolet-chain-staging/db/makolet_chain.db
"""
import argparse
import os
import sqlite3
import sys
from collections import defaultdict

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from agents.supplier_roster import prior_two_months  # read-only helper

HERE = os.path.dirname(__file__)
DEFAULT_PROD = os.path.join(HERE, '..', 'db', 'makolet_chain.db')
DEFAULT_STAGING = '/opt/makolet-chain-staging/db/makolet_chain.db'
DENNIS_BRANCHES = {9015, 9018}

# Dennis's named suppliers → normalized match-cores (spelling variants he gave +
# the way we might store them). Matching is substring-on-normalized (spaces/
# quotes/punct stripped), so 'קפוא זן' and 'קפואזן' both reduce to 'קפואזן'.
DENNIS_SUPPLIERS = [
    ('מילועוף / מילעוף',        ['מילעוף', 'מילועוף']),
    ('פלקו (גלידות פלדמן)',     ['פלקו', 'פלדמן']),
    ('קפוא זן / קפואזן',        ['קפואזן']),
    ('נטו',                     ['נטו']),
    ('רד בול',                  ['רדבול', 'redbull']),
    ('דבאח',                    ['דבאח']),
    ('פארם אקספרס',             ['פארםאקספרס', 'פארמאקספרס']),
    ('ויסוצקי',                 ['ויסוצקי']),
    ('אייס דילר',               ['אייסדילר']),
    ('פורמולה',                 ['פורמולה']),
    ('אינטרנשיונל',             ['אינטרנשיונל']),
    ('גלובל ויין / ווין',       ['גלובלווין', 'גלובלויין']),
    ('מרינה',                   ['מרינה']),
]

# Latin lowercasing helps redbull; everything else is Hebrew.
_STRIP = ' \t\r\n"\'״׳.־-–—_()[]{}/*,'


def norm(s):
    """Collapse a supplier string to a comparison core: strip spaces, quotes,
    punctuation, a trailing בע"מ, and lowercase latin."""
    s = (s or '').strip().lower()
    for junk in ('בעמ', 'בע"מ', 'בע״מ'):
        s = s.replace(junk, '')
    out = []
    for ch in s:
        if ch in _STRIP:
            continue
        out.append(ch)
    return ''.join(out)


def conn_ro(path):
    uri = 'file:' + os.path.abspath(path) + '?mode=ro'
    c = sqlite3.connect(uri, uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def is_franchise(name, franchise):
    n = (name or '').strip()
    return (not n) or n == '—' or (franchise and franchise in n)


# ─────────────────────────────────────────────────────────────────────────────
# TIER 1
# ─────────────────────────────────────────────────────────────────────────────
def tier1(c, branches, older, newer, current):
    window = {older, newer}                     # roster window (prior 2 months)
    list_window = window | {current}            # roster ∪ current-month spenders

    # Pull every goods row once: (branch, supplier) aggregates.
    rows = c.execute(
        "SELECT branch_id, supplier, COUNT(*) docs, SUM(amount) amt, "
        "MIN(doc_date) mn, MAX(doc_date) mx, "
        "MAX(strftime('%Y-%m', doc_date)) mxmonth "
        "FROM goods_documents "
        "WHERE supplier IS NOT NULL AND TRIM(supplier) NOT IN ('','—') "
        "GROUP BY branch_id, supplier").fetchall()

    # supplier_roster table membership (the literal budget-list source).
    roster = defaultdict(set)                    # branch -> {supplier_name}
    for r in c.execute("SELECT branch_id, supplier_name FROM supplier_roster"):
        roster[r['branch_id']].add(r['supplier_name'])

    # saved budgets (also force a name into the list).
    budgeted = defaultdict(set)
    try:
        for r in c.execute("SELECT branch_id, supplier_name FROM supplier_budgets"):
            budgeted[r['branch_id']].add(r['supplier_name'])
    except sqlite3.OperationalError:
        pass

    bname = {b['id']: b['name'] for b in branches}
    franchise = {b['id']: (b['franchise_supplier'] or '').strip() for b in branches}
    valid = set(bname)

    # Index normalized supplier-name → list of aggregate rows.
    agg = []
    for r in rows:
        if r['branch_id'] not in valid:
            continue
        agg.append({
            'branch': r['branch_id'], 'supplier': r['supplier'],
            'ncore': norm(r['supplier']), 'docs': r['docs'],
            'amt': r['amt'] or 0.0, 'mn': r['mn'], 'mx': r['mx'],
            'mxmonth': r['mxmonth'],
        })

    print("=" * 78)
    print(f"TIER 1 — per-supplier resolution  (roster window {older}+{newer}, "
          f"current {current})")
    print("=" * 78)

    summary = []
    for typed, cores in DENNIS_SUPPLIERS:
        matches = [a for a in agg if any(core in a['ncore'] for core in cores)]
        print(f"\n■ {typed}")
        if not matches:
            print("   → ABSENT — no goods_documents row under any spelling, chain-wide.")
            summary.append((typed, 'ABSENT', '—'))
            continue

        # distinct stored spellings actually matched.
        spellings = sorted({a['supplier'] for a in matches})

        # group by store
        by_store = defaultdict(list)
        for a in matches:
            by_store[a['branch']].append(a)

        any_active = any(a['mxmonth'] in list_window for a in matches)
        bucket = 'ACTIVE' if any_active else 'DORMANT'

        print(f"   stored spelling(s): {spellings}")
        print(f"   {'store':<26} {'docs':>5} {'amount':>11} {'latest':<11} "
              f"{'in_list?':<9} franchise?")
        for bid in sorted(by_store):
            recs = by_store[bid]
            docs = sum(a['docs'] for a in recs)
            amt = sum(a['amt'] for a in recs)
            mx = max(a['mx'] for a in recs)
            names = {a['supplier'] for a in recs}
            in_roster = bool(names & roster.get(bid, set()))
            in_cur = any(a['mxmonth'] == current for a in recs)
            in_bud = bool(names & budgeted.get(bid, set()))
            in_list = in_roster or in_cur or in_bud
            is_fr = any(is_franchise(a['supplier'], franchise.get(bid, '')) for a in recs)
            star = '  ← DENNIS' if bid in DENNIS_BRANCHES else ''
            label = f"{bid} {bname.get(bid, '?')[:18]}"
            print(f"   {label:<26} {docs:>5} {amt:>11,.0f} {str(mx):<11} "
                  f"{('YES' if in_list else 'no'):<9} {'YES' if is_fr else '-'}{star}")

        # spelling-variant flag vs what Dennis typed
        typed_core = norm(typed.split(' / ')[0].split(' (')[0])
        is_variant = all(typed_core not in norm(s) and norm(s) not in typed_core
                         for s in spellings)
        notes = []
        if is_variant:
            notes.append('VARIANT (stored spelling ≠ typed)')
        # Dennis-store specific verdict
        d_recs = [a for a in matches if a['branch'] in DENNIS_BRANCHES]
        if d_recs:
            d_in_list = False
            for bid in DENNIS_BRANCHES:
                recs = by_store.get(bid, [])
                if not recs:
                    continue
                names = {a['supplier'] for a in recs}
                if (names & roster.get(bid, set())) or (names & budgeted.get(bid, set())) \
                        or any(a['mxmonth'] == current for a in recs):
                    d_in_list = True
            if not d_in_list:
                notes.append("present in Dennis's store(s) but NOT in his budget list")
        else:
            notes.append("ABSENT from Dennis's own stores (only other branches)")

        print(f"   ⇒ bucket: {bucket}" + (f"   [{'; '.join(notes)}]" if notes else ''))
        summary.append((typed, bucket, '; '.join(notes) or 'in list'))

    print("\n" + "-" * 78)
    print("TIER 1 SUMMARY")
    for typed, bucket, note in summary:
        print(f"   {typed:<26} {bucket:<8} {note}")
    return summary


# ─────────────────────────────────────────────────────────────────────────────
# TIER 2  — dedup collisions
# ─────────────────────────────────────────────────────────────────────────────
def tier2(c, branches, older, newer, current):
    list_window = {older, newer, current}
    bname = {b['id']: b['name'] for b in branches}
    valid = set(bname)

    # name → set(raw spellings) per branch, restricted to names that are in the
    # budget list (roster window or current month) so we count REAL budget rows.
    rows = c.execute(
        "SELECT branch_id, supplier, strftime('%Y-%m', doc_date) m "
        "FROM goods_documents "
        "WHERE supplier IS NOT NULL AND TRIM(supplier) NOT IN ('','—')").fetchall()
    bynorm = defaultdict(lambda: defaultdict(set))   # branch -> ncore -> {raw}
    for r in rows:
        if r['branch_id'] not in valid or r['m'] not in list_window:
            continue
        bynorm[r['branch_id']][norm(r['supplier'])].add(r['supplier'])

    print("\n" + "=" * 78)
    print("TIER 2 — name-dedup collisions (same supplier under >1 spelling, "
          "in-list window)")
    print("=" * 78)

    chain_dupes = 0
    collision_rows = []
    for bid in sorted(bynorm):
        for ncore, raws in bynorm[bid].items():
            if len(raws) > 1:
                chain_dupes += len(raws) - 1
                collision_rows.append((bid, sorted(raws)))

    # מרינה in 9018 first
    print("\n● מרינה / 9018 focus:")
    marina_9018 = [(bid, raws) for bid, raws in collision_rows
                   if bid == 9018 and any('מרינה' in norm(x) for x in raws)]
    marina_any = c.execute(
        "SELECT branch_id, supplier, COUNT(*) d, MAX(doc_date) mx "
        "FROM goods_documents WHERE branch_id=9018 AND supplier LIKE '%מרינה%' "
        "GROUP BY supplier").fetchall()
    if marina_any:
        for r in marina_any:
            print(f"   9018  {r['supplier']!r:<40} docs={r['d']} latest={r['mx']}")
        distinct = {norm(r['supplier']) for r in marina_any}
        print(f"   → {len(marina_any)} stored spelling(s) collapse to "
              f"{len(distinct)} supplier(s) → "
              f"{'DUPLICATE rows in budget list' if len(marina_any) > 1 else 'single row'}")
    else:
        print("   no מרינה rows in 9018 goods_documents.")

    print(f"\n● All collisions (branch: spellings that collapse to one supplier):")
    if not collision_rows:
        print("   none in the in-list window.")
    for bid, raws in sorted(collision_rows):
        print(f"   {bid} {bname.get(bid,'?')[:16]:<16} {raws}")

    print(f"\n   chain-wide duplicate budget rows (Σ extra spellings) = {chain_dupes}")
    return chain_dupes


# ─────────────────────────────────────────────────────────────────────────────
# TIER 3  — staging product catalog (franchise-exclusion smoking gun)
# ─────────────────────────────────────────────────────────────────────────────
def find_catalog_table(sc):
    """Locate a staging table whose columns include a raw-supplier and a
    suggested-supplier field. Returns (table, raw_col, sugg_col, name_col,
    branch_col) or None."""
    tbls = [r['name'] for r in sc.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    for t in tbls:
        cols = [r[1] for r in sc.execute(f"PRAGMA table_info({t})").fetchall()]
        low = {col.lower(): col for col in cols}
        raw = next((low[k] for k in low if 'raw' in k and 'supp' in k), None)
        sugg = next((low[k] for k in low
                     if ('sugg' in k or 'mapped' in k or 'resolv' in k) and 'supp' in k), None)
        if raw and sugg:
            name_col = next((low[k] for k in low
                             if k in ('product_name', 'name', 'item_name', 'product')), None)
            branch_col = next((low[k] for k in low if k in ('branch_id', 'branch')), None)
            return (t, raw, sugg, name_col, branch_col)
    return None


def tier3(staging_path):
    print("\n" + "=" * 78)
    print("TIER 3 — staging product catalog: Dennis's suppliers hidden under זיכיונות")
    print("=" * 78)
    if not staging_path or not os.path.exists(staging_path):
        print(f"   staging DB not found at {staging_path!r} — Tier 3 SKIPPED.")
        print("   → follow-up: run a fresh prod זיכיונות line-item pull for 9015/9018.")
        return
    sc = conn_ro(staging_path)
    found = find_catalog_table(sc)
    if not found:
        print("   No product-catalog table with raw/suggested supplier columns on "
              "staging.")
        print("   → follow-up: build a fresh זיכיונות line-item catalog for 9015/9018.")
        sc.close()
        return
    tbl, raw_c, sugg_c, name_c, branch_c = found
    print(f"   catalog table = {tbl}  (raw={raw_c}, suggested={sugg_c}, "
          f"name={name_c}, branch={branch_c})")

    sel = f"SELECT {raw_c} raw, {sugg_c} sugg"
    if name_c:
        sel += f", {name_c} pname"
    if branch_c:
        sel += f", {branch_c} branch"
    q = f"{sel} FROM {tbl} WHERE {raw_c} LIKE '%זיכיונות%'"
    try:
        rows = sc.execute(q).fetchall()
    except sqlite3.OperationalError as e:
        print(f"   query failed: {e}")
        sc.close()
        return

    print(f"   products filed under זיכיונות: {len(rows)}")
    branches_seen = set()
    hits = []
    for r in rows:
        if branch_c:
            branches_seen.add(r['branch'])
        sugg = r['sugg'] or ''
        nc = norm(sugg)
        for typed, cores in DENNIS_SUPPLIERS:
            if any(core in nc for core in cores):
                pname = r['pname'] if name_c else '?'
                br = r['branch'] if branch_c else '?'
                hits.append((typed, sugg, pname, br))

    if branch_c:
        cov = branches_seen & DENNIS_BRANCHES
        if not cov:
            print(f"   ⚠ catalog has NO 9015/9018 coverage (branches present: "
                  f"{sorted(branches_seen)[:10]}).")
            print("   → follow-up: fresh prod זיכיונות pull for 9015/9018 to confirm.")

    if not hits:
        print("   No Dennis-named supplier appears as a suggested_supplier under "
              "זיכיונות in the catalog.")
    else:
        print(f"   SMOKING-GUN matches ({len(hits)}): זiכ-filed product → real supplier")
        for typed, sugg, pname, br in hits[:40]:
            print(f"     [{br}] {str(pname)[:34]:<34} → {sugg[:24]:<24} "
                  f"(Dennis: {typed})")
    sc.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--prod-db', default=os.environ.get('PROD_DB', DEFAULT_PROD))
    ap.add_argument('--staging-db', default=os.environ.get('STAGING_DB', DEFAULT_STAGING))
    a = ap.parse_args()

    older, newer = prior_two_months()
    # current month = the month AFTER newer.
    cy, cm = int(newer[:4]), int(newer[5:7]) + 1
    if cm == 13:
        cy, cm = cy + 1, 1
    current = f'{cy:04d}-{cm:02d}'

    print(f"prod DB    : {os.path.abspath(a.prod_db)}  (mode=ro)")
    print(f"staging DB : {a.staging_db}  (mode=ro)")
    print(f"windows    : roster={older}+{newer}  current={current}\n")

    c = conn_ro(a.prod_db)
    branches = c.execute(
        "SELECT id, name, franchise_supplier FROM branches "
        "WHERE active=1 AND id NOT IN (9998,9999) ORDER BY id").fetchall()
    print(f"branches in scope: {len(branches)}  "
          f"({', '.join(str(b['id']) for b in branches)})")

    tier1(c, branches, older, newer, current)
    tier2(c, branches, older, newer, current)
    c.close()
    tier3(a.staging_db)


if __name__ == '__main__':
    main()

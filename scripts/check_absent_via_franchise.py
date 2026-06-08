# -*- coding: utf-8 -*-
"""READ-ONLY: are Dennis's "ABSENT" budget suppliers actually billed through the
זיכיונות franchise (which goods_documents excludes by design), or truly never bought?

goods_documents NEVER stores the franchise supplier (bilboy.py drops it), so a
supplier whose goods arrive on a franchise invoice looks "absent" to /goods even
though the store buys from it. This checks that, per branch (9018 דפנה + 9015 הגנה).

TIER 1 (chain-wide signal): cross-ref the ABSENT names against the EXISTING staging
products catalog — franchise rows (raw_supplier ~ זיכיונות) whose suggested_supplier
maps to an ABSENT name.

TIER 2 (authoritative, per branch): pull the זיכיונות franchise docs live from BilBoy
(~last 3 months), read each doc's line items, map every barcode → real supplier via
the catalog's NON-franchise barcode→supplier mapping, and report which ABSENT
suppliers show up (₪ gross + doc count) + the TOTAL franchise goods ₪ per store that
maps to a real supplier (= goods spend currently EXCLUDED from /goods).

Also confirms the typo: his ע.ס.ג vs our 'ע.ס.נ סחר בע"מ' — same supplier?

READ-ONLY: BilBoy GETs + SELECT only. No writes/migrations/deploy. Does NOT modify
bilboy.py — only imports its existing read helpers. Throttled.
"""
import os
import re
import sqlite3
import sys
import time
from collections import defaultdict

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import requests                                         # noqa: E402
from agents.bilboy import (                             # noqa: E402  (read-only)
    _get_branch_config, _branch_session, _api_get,
    EXCLUDED_STATUSES, API_BASE,
)

STAGING_DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
ZIK_MATCH = 'זיכיונות המכולת'
FROM_DATE, TO_DATE = '2026-04-01', '2026-06-09'         # ~last 3 months
THROTTLE = 0.35
JUNK_NAMES = {'פיקדון', 'פריט חסום'}
JUNK_CODES = {'', '0', '32323232'}

# Per-branch ABSENT sets from the coverage result (DORMANT excluded).
ABSENT = {
    9018: ['דובק', 'שסטוביץ', 'אורנים', 'חוגלה', 'גלובל יין', 'בן אנד גריס',
           'היכל היין', 'שמאי', 'ויסוצקי', 'פלדמן', 'קפולסקי', 'ערטול', 'אמילי',
           'מילעוף', 'קפואזן', 'נטו', 'דבאח', 'סלטי מונטנה', 'פארם אקספרס', 'ע.ס.ג'],
    9015: ['אורנים', 'ע.ס.ג', 'בן אנד גריס', 'פארם אקספרס', 'רד בול', 'פלדמן',
           'מילעוף', 'קפואזן', 'דבאח', 'נטו', 'סלטי מונטנה'],
}

# ── fuzzy matching (same scheme as dennis_budget_coverage.py) ──
_PUNCT = re.compile(r'["\'״׳()\[\]/.,\-–—_*]')
_WS = re.compile(r'\s+')
GENERIC = {'אינטרנשיונל', 'עלית', 'סחר', 'שיווק', 'יבוא', 'הפצה', 'מפיצים',
           'ישראל', 'מוצרי', 'קבוצת', 'המרכזית', 'החברה'}


def normalize(s):
    s = (s or '').lower()
    for b in ('בע"מ', 'בע״מ', 'בעמ'):
        s = s.replace(b, ' ')
    return _WS.sub(' ', _PUNCT.sub(' ', s)).strip()


def toks(s):
    return [t for t in normalize(s).split() if len(t) >= 3 and t not in GENERIC]


def score(dname, oname):
    on, dn = normalize(oname), normalize(dname)
    sc = 0
    for t in set(toks(dname)):
        if t in on:
            sc += len(t)
    for t in set(toks(oname)):
        if t in dn:
            sc += len(t)
    return sc


def best_absent(real_supplier, absent_names):
    best, bsc = None, 0
    for d in absent_names:
        sc = score(d, real_supplier)
        if sc > bsc:
            best, bsc = d, sc
    return (best, bsc) if bsc >= 3 else (None, 0)


def norm_bc(bc):
    bc = str(bc or '').strip()
    return bc.lstrip('0') or bc


def conn_ro(path):
    c = sqlite3.connect('file:' + os.path.abspath(path) + '?mode=ro', uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def build_barcode_map(staging):
    """barcode→real supplier + name→real supplier, from NON-franchise catalog rows."""
    rows = staging.execute(
        "SELECT barcode, name, supplier, doc_count FROM products "
        "WHERE supplier IS NOT NULL AND supplier NOT LIKE '%זיכיונות%'").fetchall()
    bc_map, bc_best = {}, {}
    nm_map, nm_best = {}, {}
    for r in rows:
        sup = r['supplier']
        dc = r['doc_count'] or 0
        b = norm_bc(r['barcode'])
        if b and b not in JUNK_CODES and dc >= bc_best.get(b, -1):
            bc_map[b], bc_best[b] = sup, dc
        nm = normalize(r['name'])
        if nm and dc >= nm_best.get(nm, -1):
            nm_map[nm], nm_best[nm] = sup, dc
    return bc_map, nm_map


def get_franchise_doc_ids(session, bb_id):
    """Resolve זיכיונות supplier id(s), then headers in the window → [doc dicts]."""
    raw = _api_get(session, '/customer/suppliers',
                   params={'customerBranchId': str(bb_id), 'all': 'true'}, timeout=30)
    sup = raw.get('suppliers') if isinstance(raw, dict) else raw
    zik_ids = []
    for s in (sup or []):
        nm = s.get('title') or s.get('name') or s.get('supplierName') or ''
        if ZIK_MATCH in nm:
            sid = str(s.get('id') or s.get('supplierId') or '')
            if sid:
                zik_ids.append(sid)
    if not zik_ids:
        return []
    docs = _api_get(session, '/customer/docs/headers', params={
        'suppliers': ','.join(zik_ids), 'branches': str(bb_id),
        'from': f'{FROM_DATE}T00:00:00', 'to': f'{TO_DATE}T00:00:00'}, timeout=60)
    docs = docs if isinstance(docs, list) else (
        docs.get('data') or docs.get('docs') or docs.get('headers') or [])
    out = []
    for d in docs:
        if ZIK_MATCH not in (d.get('supplierName') or ''):
            continue
        if d.get('status') in EXCLUDED_STATUSES:
            continue
        out.append(d)
    return out


def main():
    staging = conn_ro(STAGING_DB)
    all_absent = sorted({n for v in ABSENT.values() for n in v})

    # ── TIER 1: catalog suggested_supplier cross-ref (chain-wide) ──
    print("=" * 78)
    print("TIER 1 — staging catalog: franchise products whose suggested_supplier "
          "maps to an ABSENT name")
    print("=" * 78)
    frows = staging.execute(
        "SELECT name, suggested_supplier FROM products "
        "WHERE raw_supplier LIKE '%זיכיונות%'").fetchall()
    n_sugg = sum(1 for r in frows if (r['suggested_supplier'] or '').strip())
    hits = []
    for r in frows:
        sg = (r['suggested_supplier'] or '').strip()
        if not sg:
            continue
        d, sc = best_absent(sg, all_absent)
        if d:
            hits.append((d, sg, r['name']))
    print(f"  franchise catalog rows: {len(frows)} | with suggested_supplier set: {n_sugg}")
    if hits:
        for d, sg, nm in hits:
            print(f"   ABSENT {d!r} ← suggested {sg!r}  (product {nm[:30]})")
    else:
        print("  no franchise catalog row maps (suggested_supplier) to an ABSENT name "
              "— suggested_supplier is mostly empty; rely on TIER 2 barcode mapping.")

    bc_map, nm_map = build_barcode_map(staging)
    print(f"\n  barcode→supplier map built from {len(bc_map)} non-franchise barcodes "
          f"({len(nm_map)} names).")
    staging.close()

    # ── TIER 2: live franchise docs per branch ──
    for bid in (9018, 9015):
        print("\n" + "=" * 78)
        print(f"TIER 2 — branch {bid}: זיכיונות franchise goods mapped to real suppliers "
              f"({FROM_DATE}..{TO_DATE})")
        print("=" * 78)
        branch = _get_branch_config(bid)
        bb_id = branch.get('bilboy_branch_id')
        if not bb_id:
            print(f"  branch {bid} has no bilboy_branch_id — skipped")
            continue
        session = _branch_session(branch, bid)
        try:
            docs = get_franchise_doc_ids(session, bb_id)
        except Exception as e:
            print(f"  franchise header fetch failed: {type(e).__name__}: {str(e)[:60]}")
            continue
        print(f"  franchise docs in window: {len(docs)}")

        per_real = defaultdict(lambda: [0.0, 0, set()])   # real supplier -> [gross, lines, docset]
        total_goods_gross = 0.0
        unmapped_gross = 0.0
        fetched = errs = 0
        for d in docs:
            did = d.get('id')
            try:
                raw = _api_get(session, '/customer/doc', params={'docId': did}, timeout=15)
            except Exception:
                errs += 1
                time.sleep(THROTTLE)
                continue
            fetched += 1
            items = (raw.get('body') or {}).get('items') if isinstance(raw, dict) else None
            for it in (items or []):
                nm = (it.get('name') or '').strip()
                bc = norm_bc(it.get('barcode') or it.get('catalogNumber') or '')
                if nm in JUNK_NAMES or (bc in JUNK_CODES and not nm):
                    continue
                if not bc or bc in JUNK_CODES:
                    continue                       # no barcode → a fee line, not goods
                net = float(it.get('total') or 0)
                gross = round(net * 1.18, 2) if it.get('hasVat') else round(net, 2)
                real = bc_map.get(bc) or nm_map.get(normalize(nm))
                if not real:
                    unmapped_gross += gross
                    continue
                total_goods_gross += gross
                per_real[real][0] += gross
                per_real[real][1] += 1
                per_real[real][2].add(did)
            time.sleep(THROTTLE)

        print(f"  docs read: {fetched}/{len(docs)} (errors {errs}) | "
              f"franchise GOODS (barcoded) mapped ₪{total_goods_gross:,.0f} gross, "
              f"unmapped-goods ₪{unmapped_gross:,.0f}")

        # classify each ABSENT name for this branch
        absent_here = ABSENT[bid]
        # roll mapped real suppliers up to the ABSENT name they match
        hidden = defaultdict(lambda: [0.0, set()])     # dennis name -> [gross, docset]
        for real, (g, lines, docset) in per_real.items():
            d, sc = best_absent(real, absent_here)
            if d:
                hidden[d][0] += g
                hidden[d][1] |= docset

        print(f"\n  ABSENT split for {bid} ({len(absent_here)} names):")
        fh, ta = [], []
        for d in absent_here:
            if d in hidden:
                fh.append((d, hidden[d][0], len(hidden[d][1])))
            else:
                ta.append(d)
        print(f"  ── FRANCHISE-HIDDEN ({len(fh)}) — billed via זיכיונות:")
        for d, g, nd in sorted(fh, key=lambda x: -x[1]):
            print(f"     {d:<16} ₪{g:>9,.0f} gross   {nd} doc(s)")
        if not fh:
            print("     (none)")
        print(f"  ── TRULY-ABSENT ({len(ta)}) — nothing under franchise either: {ta}")

    # ── ע.ס.ג vs ע.ס.נ typo check ──
    print("\n" + "=" * 78)
    print("ע.ס.ג (his) vs 'ע.ס.נ סחר בע\"מ' (ours) — same supplier?")
    print("=" * 78)
    for bid in (9018, 9015):
        branch = _get_branch_config(bid)
        bb_id = branch.get('bilboy_branch_id')
        try:
            session = _branch_session(branch, bid)
            raw = _api_get(session, '/customer/suppliers',
                           params={'customerBranchId': str(bb_id), 'all': 'true'}, timeout=30)
            sup = raw.get('suppliers') if isinstance(raw, dict) else raw
            names = [(s.get('title') or s.get('name') or '') for s in (sup or [])]
            hits = [n for n in names if 'ע.ס' in n or 'עס' in normalize(n)[:4]]
            print(f"  branch {bid} BilBoy suppliers matching ~'ע.ס': {hits}")
        except Exception as e:
            print(f"  branch {bid}: supplier list failed ({type(e).__name__})")
        time.sleep(THROTTLE)


if __name__ == '__main__':
    main()

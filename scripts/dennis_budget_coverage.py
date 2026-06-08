# -*- coding: utf-8 -*-
"""READ-ONLY: compare Dennis's real supplier-budget sheet against what our תקציב
page (_goal_data) actually lists, for his two branches — 9018 דפנה + 9015 הגנה —
to find coverage gaps. Prod DB only; no Aviv/BilBoy.

Fuzzy match: his names are brands, ours are legal names (ויסוצקי =
"תה ויסוצקי (ישראל) בע\"מ"). Normalize (strip בע"מ/quotes/punct/whitespace), then
GREEDY one-to-one assignment by shared-token score — so the 4 שטראוס / 3 דילר /
2 תנובה brand-variants each grab their own legal entity instead of colliding.

Per Dennis supplier → MATCHED (in our budget list) or MISSING. Each MISSING is
checked against goods_documents ALL-TIME for that branch: DORMANT (last doc date)
vs ABSENT (never a doc). Also lists OUR budget suppliers not on his sheet (extras)
and any low-confidence matches to eyeball.

READ-ONLY: SELECT + _goal_data (read path) only. No writes/migrations/deploy.
"""
import os
import re
import sqlite3
import sys

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import app as app_module                 # noqa: E402
from app import _goal_data               # noqa: E402

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')

# Dennis's sheet: name, 9018 (דפנה) ₪, 9015 (הגנה) ₪.
DENNIS = [
    ('פיליפ מוריס', 31000, 63000), ('גלוברנדס', 23000, 50000), ('דובק', 3700, 15000),
    ('פילטר טיפס', 1500, 10000), ('תנובה', 17600, 26000), ('תנובה בשר', 2900, 4500),
    ('שטראוס גרופ', 4700, 7000), ('שטראוס מצונן', 11000, 20000), ('שטראוס פריטולי', 2100, 3500),
    ('אחים מזרחי', 6600, 10000), ('טרה', 5200, 8300), ('קוקה קולה', 9300, 22000),
    ('טמפו', 9400, 22000), ('ויליפוד', 3300, 6900), ('אבאל', 2900, 4300),
    ('שסטוביץ', 3700, 6100), ('ע.ס.ג', 5400, 8900), ('אורנים', 3700, 9600),
    ('אגמי', 3300, 5900), ('רוסמן', 3000, 5700), ('אביקם זינגר', 2100, 4000),
    ('אסם', 3700, 10900), ('גלידות שטראוס', 3100, 4900), ('גלידות נסטלה', 2300, 4300),
    ('פלדמן', 380, 600), ('בן אנד גריס', 1500, 2300), ('קפולסקי', 0, 800),
    ('חוגלה', 2000, 4700), ('מן הטבע', 4500, 8000), ('דיפלומט', 5700, 8800),
    ('ויסוצקי', 700, 900), ('שמאי', 1100, 2300), ('ערטול', 0, 570),
    ('זוגלובק', 1800, 5400), ('אמילי', 0, 1900), ('מילעוף', 0, 500),
    ('עוף טוב', 650, 1300), ('מרינה', 6700, 15000), ('קפואזן', 0, 500),
    ('קריסטל אייס', 300, 600), ('נטו', 0, 0), ('רד בול', 600, 650),
    ('גלוברנדס מזון', 1000, 3400), ('אייס דילר', 1000, 1200), ('דילר פורמולה', 0, 900),
    ('דילר אינטרנשיונל', 0, 1200), ('גלובל יין', 1700, 3800), ('דבאח', 0, 450),
    ('סלטי מונטנה', 0, 0), ('פארם אקספרס', 0, 950), ('היכל היין', 1200, 1300),
]
BRANCHES = [(9018, 'דפנה', 1), (9015, 'הגנה', 2)]   # (id, label, amt-index)

_PUNCT = re.compile(r'["\'״׳()\[\]/.,\-–—_*]')
_WS = re.compile(r'\s+')

# Generic descriptor tokens that recur across DIFFERENT legal entities, so they
# must NOT by themselves carry a fuzzy match (e.g. 'אינטרנשיונל' appears in BOTH
# ויליפוד אינטרנשיונל and דילר בי.אמ.די אינטרנשיונל). A match needs a brand token.
GENERIC = {
    'אינטרנשיונל', 'גלידות', 'מזון', 'עלית', 'סחר', 'שיווק', 'יבוא', 'הפצה',
    'מפיצים', 'ישראל', 'מוצרי', 'קבוצת', 'המרכזית', 'החברה',
}


def normalize(s):
    s = (s or '').lower()
    for b in ('בע"מ', 'בע״מ', 'בעמ'):
        s = s.replace(b, ' ')
    s = _PUNCT.sub(' ', s)
    return _WS.sub(' ', s).strip()


def toks(s):
    return [t for t in normalize(s).split() if len(t) >= 3 and t not in GENERIC]


def pair_score(dname, oname):
    """(score, matched_dtoken_count). Dennis tokens (≥3) found as substrings of the
    normalized our-name, plus our tokens found in Dennis (covers short brand names).
    Score = Σ matched-token length; coverage uses Dennis-token hits."""
    on, dn = normalize(oname), normalize(dname)
    dts = set(toks(dname))
    matched = [t for t in dts if t in on]
    sc = sum(len(t) for t in matched)
    for t in set(toks(oname)):
        if t in dn and t not in matched:
            sc += len(t)
    return sc, len(matched), len(dts)


def assign(dennis_names, our_names):
    """Greedy one-to-one: each Dennis name ↔ at most one our-name. Returns
    {dennis_idx: (our_name, score, coverage)} and the set of matched our-names."""
    pairs = []
    for di, dn in enumerate(dennis_names):
        for on in our_names:
            sc, mc, dt = pair_score(dn, on)
            if sc > 0:
                cov = mc / dt if dt else 0
                pairs.append((sc, cov, di, on))
    # Best score first; then best coverage (full-name match beats partial).
    pairs.sort(key=lambda p: (p[0], p[1]), reverse=True)
    dmatch, taken = {}, set()
    for sc, cov, di, on in pairs:
        if di in dmatch or on in taken:
            continue
        dmatch[di] = (on, sc, cov)
        taken.add(on)
    return dmatch, taken


def conn_ro():
    c = sqlite3.connect('file:' + os.path.abspath(DB_PATH) + '?mode=ro', uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def main():
    c = conn_ro()
    for bid, label, idx in BRANCHES:
        our = [s['supplier_name'] for s in _goal_data(bid, c)['suppliers']]
        # all-time goods suppliers for DORMANT/ABSENT check.
        goods = c.execute(
            "SELECT supplier, MAX(doc_date) mx FROM goods_documents "
            "WHERE branch_id=? AND supplier IS NOT NULL AND TRIM(supplier) NOT IN ('','—') "
            "GROUP BY supplier", (bid,)).fetchall()
        goods_names = [g['supplier'] for g in goods]
        goods_last = {g['supplier']: g['mx'] for g in goods}

        dnames = [d[0] for d in DENNIS]
        dmatch, taken = assign(dnames, our)

        matched, missing, lowconf = [], [], []
        for di, (name, *_amts) in enumerate(DENNIS):
            amt = DENNIS[di][idx]
            if di in dmatch:
                on, sc, cov = dmatch[di]
                matched.append((name, on))
                # weak: the match covers less than half of Dennis's brand tokens
                # (a partial-name hit worth eyeballing). Full short-name hits
                # (cov 1.0, e.g. טרה→החברה המרכזית טרה) are confident, not flagged.
                if cov < 0.5:
                    lowconf.append((name, on, sc, round(cov, 2)))
            else:
                # MISSING from budget list → check goods all-time (fuzzy, best score).
                best, bestsc = None, 0
                for gn in goods_names:
                    sc, mc, dt = pair_score(name, gn)
                    if sc > bestsc:
                        best, bestsc = gn, sc
                if best and bestsc > 3:
                    missing.append((name, amt, 'DORMANT', goods_last[best], best))
                else:
                    missing.append((name, amt, 'ABSENT', None, None))

        extras = sorted(set(our) - taken)

        print("=" * 78)
        print(f"BRANCH {bid} {label}  — Dennis N={len(DENNIS)}  ours M={len(our)}  "
              f"matched K={len(matched)}  MISSING={len(missing)}  extras={len(extras)}")
        print("=" * 78)

        print(f"\nMISSING ({len(missing)}) — Dennis supplier | {label} ₪ | status | last doc / our-name")
        for name, amt, status, last, gn in sorted(missing, key=lambda m: -m[1]):
            if status == 'DORMANT':
                print(f"   {name:<18} ₪{amt:>6} DORMANT   last {last}  ({gn[:30]})")
            else:
                print(f"   {name:<18} ₪{amt:>6} ABSENT    —")

        print(f"\nEXTRAS ({len(extras)}) — our budget suppliers NOT on Dennis's sheet:")
        for e in extras:
            print(f"   {e}")

        if lowconf:
            print(f"\nLOW-CONFIDENCE matches ({len(lowconf)}) — eyeball (Dennis → ours | score/cov):")
            for name, on, sc, cov in lowconf:
                print(f"   {name:<18} → {on[:38]:<38} (sc={sc} cov={cov})")

        # one-line read
        n_dorm = sum(1 for m in missing if m[2] == 'DORMANT')
        n_abs = sum(1 for m in missing if m[2] == 'ABSENT')
        print(f"\nREAD {bid} {label}: {len(matched)}/{len(DENNIS)} matched; "
              f"MISSING {len(missing)} = {n_dorm} DORMANT + {n_abs} ABSENT.")
        print()
    c.close()


if __name__ == '__main__':
    main()

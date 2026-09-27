#!/usr/bin/env python3
"""READ-ONLY זיכיונות line audit — what a line-router WOULD do for one month.

Fetches every franchise (זיכיונות המכולת) doc + line items for every active
branch (excl. demo 9998/9999), caches the raw JSON, then classifies each line
and compares against what the dashboard already counts. Never writes the DB
(opened mode=ro); only GET calls to BilBoy, hard-capped.

Usage:
  python3 scripts/zik_audit.py 2026-08 fetch      # BilBoy calls → /tmp cache
  python3 scripts/zik_audit.py 2026-08 report     # classify from cache, 0 calls
Options: --cap N (default 450), --branches 9015,9018
"""
import json
import os
import re
import sqlite3
import sys
import time
from calendar import monthrange
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

DB = os.path.join(ROOT, 'db', 'makolet_chain.db')
STAGING_DB = '/opt/makolet-chain-staging/db/makolet_chain.db'
ZIK = 'זיכיונות המכולת'
EXCLUDED_BRANCHES = (9998, 9999)
THROTTLE = 0.35


def ro(path):
    conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def cache_path(month):
    return f'/tmp/zik_audit_{month}.json'


# ───────────────────────────── fetch ─────────────────────────────

class Budget:
    def __init__(self, cap):
        self.cap, self.used = cap, 0

    def get(self, api_get, session, path, **kw):
        if self.used >= self.cap:
            raise RuntimeError(f'BilBoy call cap {self.cap} reached')
        self.used += 1
        time.sleep(THROTTLE)
        return api_get(session, path, **kw)


def _hlist(h):
    return h if isinstance(h, list) else (
        h.get('data') or h.get('docs') or h.get('headers') or [])


def fetch(month, cap, only):
    from agents.bilboy import _api_get, _branch_session, EXCLUDED_STATUSES
    y, m = (int(x) for x in month.split('-'))
    frm = f'{month}-01T00:00:00'
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    to = f'{ny}-{nm:02d}-01T00:00:00'   # exclusive-ish; filter by date prefix below

    conn = ro(DB)
    branches = [dict(r) for r in conn.execute(
        'SELECT * FROM branches WHERE active=1 AND id NOT IN (?,?) ORDER BY id',
        EXCLUDED_BRANCHES)]
    conn.close()
    if only:
        branches = [b for b in branches if b['id'] in only]

    budget = Budget(cap)
    out = {'month': month, 'branches': {}}
    # PHASE A — headers only
    for b in branches:
        bid, bb = b['id'], str(b.get('bilboy_branch_id') or '')
        rec = {'name': b['name'], 'bb': bb, 'franchise_ids': [], 'headers': [],
               'error': None}
        out['branches'][str(bid)] = rec
        if not bb:
            rec['error'] = 'no bilboy_branch_id'
            continue
        try:
            s = _branch_session(b, bid)
            sup = budget.get(_api_get, s, '/customer/suppliers',
                             params={'customerBranchId': bb, 'all': 'true'})
            slist = sup.get('suppliers') if isinstance(sup, dict) else sup
            fids = [str(x.get('id') or x.get('supplierId'))
                    for x in (slist or [])
                    if ZIK in (x.get('title') or x.get('name') or x.get('supplierName') or '')]
            rec['franchise_ids'] = fids
            if not fids:
                continue
            h = budget.get(_api_get, s, '/customer/docs/headers', params={
                'suppliers': ','.join(fids), 'branches': bb, 'from': frm, 'to': to})
            rec['headers'] = [x for x in _hlist(h)
                              if str(x.get('date') or '')[:7] == month]
        except Exception as e:
            rec['error'] = f'{type(e).__name__}: {str(e)[:160]}'
        n = len(rec['headers'])
        print(f"A {bid:>5} {b['name']:<22} fids={rec['franchise_ids']} docs={n} "
              f"err={rec['error']}", flush=True)

    todo = [(bid, h) for bid, r in out['branches'].items() for h in r['headers']
            if h.get('status') not in EXCLUDED_STATUSES]
    projected = budget.used + len(todo)
    print(f'PHASE A done: calls={budget.used}  detail docs={len(todo)}  '
          f'projected={projected}/{cap}', flush=True)
    if projected > cap:
        json.dump(out, open(cache_path(month), 'w'), ensure_ascii=False)
        sys.exit(f'STOP: projected {projected} > cap {cap} — ask before continuing')

    # PHASE B — details
    sessions = {}
    for bid, h in todo:
        b = next(x for x in branches if str(x['id']) == bid)
        s = sessions.get(bid) or _branch_session(b, int(bid))
        sessions[bid] = s
        try:
            raw = budget.get(_api_get, s, '/customer/doc',
                             params={'docId': h.get('id')}, timeout=20)
            h['_detail'] = raw
        except Exception as e:
            h['_detail_error'] = f'{type(e).__name__}: {str(e)[:160]}'
    out['calls'] = budget.used
    json.dump(out, open(cache_path(month), 'w'), ensure_ascii=False)
    print(f'PHASE B done: BilBoy calls used {budget.used}/{cap} → {cache_path(month)}')


# ─────────────────────────── classify ────────────────────────────
# First rule that matches wins; every line carries its rule id.

FEE_RULES = [  # (fee type, regex on line name)
    ('rent', r'שכר דירה'),
    ('ארנונה', r'ארנונה'),
    ('חשמל', r'חיוב חשמל|^\s*חשמל'),
    ('מים', r'מיסי עיריה|^\s*מים\s'),
    ('catalog', r'ניהול קטלוג'),
    ('פרסום', r'קרן פרסום'),
    ('דיוור', r'דיוור'),
    ('אקו"ם', r'(?<![א-ת])אקו"?ם'),
    ('שילוט', r'שילוט'),
    ('כספומט', r'כספומט'),
]
# fee type → substrings of fixed_expenses.name that already count it
FEE_FIXED_NAMES = {
    'rent': ['שכר דירה'], 'ארנונה': ['ארנונה'], 'חשמל': ['חשמל'],
    'מים': ['מים'], 'catalog': ['קטלוג'], 'פרסום': ['פרסום'],
    'דיוור': ['דיוור'], 'אקו"ם': ['אקו'], 'שילוט': ['שילוט'],
    'כספומט': ['כספומט'],
}
EQUIPMENT_RE = r'iPad|Tablet|ארגז|מקודד מחירים|חולצ|פליז'
# consolidated-purchase line → token that must appear in a direct supplier name
REBILL_RE = r'^\s*(קניות|החזרות|ריכוז קניות)\s+(\S+)'
REBILL_SUPPLIER_TOKEN = {'תנובה': 'תנובה', 'שטראוס': 'שטראוס',
                         'יוניליוור': 'יוניליוור', 'דבאח': 'דבאח'}


def _barcode(it):
    cat = str(it.get('catalogNumber') or '').strip()
    if re.fullmatch(r'\d{7,14}', cat):
        return cat
    bc = str(it.get('barcode') or '').strip()
    return bc if re.fullmatch(r'\d{7,14}', bc) else ''


def classify(it, h, ctx):
    """→ (category, rule, note)."""
    from agents.bilboy import is_wolt_fee_line
    name = (it.get('name') or '').strip()
    total = float(it.get('total') or 0)
    cat = str(it.get('catalogNumber') or '').strip()
    if it.get('referencedNoticeId') and not name:
        return 'SKIP_SUMMARY', 'R1 pointer line (referencedNoticeId, no name)', ''
    if is_wolt_fee_line(it):
        return 'SKIP_WOLT', f'R2 is_wolt_fee_line (cat {cat})', ''
    m = re.match(REBILL_RE, name)
    if m and not _barcode(it):
        tok = next((t for k, t in REBILL_SUPPLIER_TOKEN.items() if k in name), None)
        shape = (it.get('qty') == it.get('total') and it.get('priceWithoutVat') == 1)
        if tok and ctx['direct_supplier_month'](tok):
            return ('SKIP_REBILL',
                    f"R3 consolidated '{m.group(1)}' + direct {tok} docs exist this month",
                    f"cat {cat}; roei-shape={'yes' if shape else 'no'}")
        return 'UNKNOWN', f"R3x consolidated '{m.group(1)}' but NO direct {tok or '?'} docs", ''
    if 'תמלוגים' in name:
        return 'ROYALTY', 'R4 name contains תמלוגים', ''
    for fee, rx in FEE_RULES:
        if h.get('type') in (3, 4) and re.search(rx, name):   # never on delivery notes
            return f'FEE:{fee}', f'R5 fee keyword /{rx}/ on invoice/credit', ''
    if 'הפרשי מחיר' in name or 'זיכוי' in name or 'מימוש' in name:
        if total <= 0:
            return 'CREDIT', 'R6 הפרשי מחיר/מימוש/זיכוי, amount ≤ 0', ''
        return 'UNKNOWN', 'R6x credit-type name but POSITIVE amount', ''
    if re.search(EQUIPMENT_RE, name):
        return 'EQUIPMENT', f'R7 equipment keyword /{EQUIPMENT_RE}/', ''
    if h.get('type') == 2 and name and 'ריכוז' not in name:
        bc = _barcode(it)
        sup = ctx['real_supplier'](bc, name)
        if sup and ctx['direct_same_day'](sup, str(h.get('date'))[:10]):
            return 'GOODS_DUP', 'R8 product line; same-day direct doc from real supplier', sup
        return ('GOODS_NEW', 'R8 product line; no same-day direct doc',
                sup or ('supplier_unknown' if bc else 'no_barcode'))
    return 'UNKNOWN', 'R9 no rule matched', ''


# ──────────────────────────── report ─────────────────────────────

def report(month):
    from agents.bilboy import extract_wolt_fee_total, EXCLUDED_STATUSES
    data = json.load(open(cache_path(month)))
    db = ro(DB)
    # barcode → real (non-franchise) supplier, from staging products
    bc_sup = {}
    try:
        sdb = ro(STAGING_DB)
        for r in sdb.execute('SELECT barcode, supplier FROM products '
                             'WHERE barcode IS NOT NULL AND barcode != ""'):
            if ZIK not in (r['supplier'] or ''):
                bc_sup.setdefault(r['barcode'], r['supplier'])
        sdb.close()
    except Exception as e:
        print(f'(products map unavailable: {e})')

    lines, per_branch = [], {}
    for bid, rec in data['branches'].items():
        b = int(bid)
        gdocs = db.execute(
            'SELECT supplier, doc_date FROM goods_documents '
            'WHERE branch_id=? AND substr(doc_date,1,7)=?', (b, month)).fetchall()
        sup_month = {r['supplier'] for r in gdocs}
        sup_days = {(r['supplier'], str(r['doc_date'])[:10]) for r in gdocs}
        ctx = {
            'direct_supplier_month': lambda tok, s=sup_month: any(tok in x for x in s),
            'real_supplier': lambda bc, nm: bc_sup.get(bc) if bc else None,
            'direct_same_day': lambda sup, day, s=sup_days: (sup, day) in s,
        }
        wolt_docs = []
        for h in rec['headers']:
            if h.get('status') in EXCLUDED_STATUSES or not h.get('_detail'):
                continue
            items = (h['_detail'].get('body') or {}).get('items') or []
            twv, two = float(h.get('totalWithVat') or 0), float(h.get('totalWithoutVat') or 0)
            ratio = (twv / two) if two else 1.18
            if h.get('type') == 3:
                wolt_docs.append({'totalWithVat': twv, 'totalWithoutVat': two,
                                  'items': [{'name': i.get('name'),
                                             'catalogNumber': i.get('catalogNumber') or i.get('barcode'),
                                             'total': i.get('total')} for i in items]})
            for it in items:
                c, rule, note = classify(it, h, ctx)
                net = float(it.get('total') or 0)
                gross = net * ratio if it.get('hasVat', True) else net
                lines.append({'branch': b, 'bname': rec['name'], 'ref': h.get('refNumber'),
                              'date': str(h.get('date'))[:10], 'type': h.get('type'),
                              'status': h.get('status'), 'name': (it.get('name') or '').strip(),
                              'cat': str(it.get('catalogNumber') or ''), 'qty': it.get('qty'),
                              'price': it.get('priceWithoutVat'), 'net': round(net, 2),
                              'gross': round(gross, 2), 'class': c, 'rule': rule, 'note': note})
        per_branch[b] = {'name': rec['name'], 'wolt_extractor': extract_wolt_fee_total(wolt_docs)}

    def gsum(rows):
        return round(sum(r['gross'] for r in rows), 2)

    # ── TASK 2 per-branch category table ──
    cats = ['ROYALTY', 'FEE', 'SKIP_WOLT', 'SKIP_REBILL', 'SKIP_SUMMARY', 'CREDIT',
            'GOODS_NEW', 'GOODS_DUP', 'EQUIPMENT', 'UNKNOWN']
    print(f'TASK 2 — {month} gross ₪ (lines)')
    print('branch                     ' + ' '.join(f'{c[:11]:>17}' for c in cats))
    for b in sorted(per_branch):
        bl = [l for l in lines if l['branch'] == b]
        cells = []
        for c in cats:
            rows = [l for l in bl if l['class'].split(':')[0] == c]
            cells.append(f'{gsum(rows):>11,.2f}({len(rows):>3})')
        print(f"{b:>5} {per_branch[b]['name'][:20]:<20} " + ' '.join(cells))
    cells = []
    for c in cats:
        rows = [l for l in lines if l['class'].split(':')[0] == c]
        cells.append(f'{gsum(rows):>11,.2f}({len(rows):>3})')
    print(f"{'CHAIN':<26} " + ' '.join(cells))

    print('\nFEE breakdown (gross):')
    fees = sorted({l['class'] for l in lines if l['class'].startswith('FEE:')})
    for f in fees:
        rows = [l for l in lines if l['class'] == f]
        print(f'  {f:<14} ₪{gsum(rows):>12,.2f}  lines={len(rows)}')

    for c in ('UNKNOWN', 'EQUIPMENT'):
        print(f'\n{c} lines (all):')
        for l in sorted([l for l in lines if l['class'] == c], key=lambda x: (x['branch'], x['ref'])):
            print(f"  {l['branch']} ref={l['ref']} {l['date']} t{l['type']} cat={l['cat']} "
                  f"qty={l['qty']} net={l['net']:,.2f} gross={l['gross']:,.2f} "
                  f"'{l['name'][:60]}'  [{l['rule']}]")

    print('\nGOODS_NEW lines (aggregated by branch+doc; full list in JSON):')
    agg = defaultdict(lambda: [0, 0.0, 0.0, set()])
    for l in lines:
        if l['class'] == 'GOODS_NEW':
            a = agg[(l['branch'], l['ref'], l['date'])]
            a[0] += 1; a[1] += l['net']; a[2] += l['gross']; a[3].add(l['note'])
    for (b, ref, day), (n, net, gross, notes) in sorted(agg.items()):
        print(f'  {b} ref={ref} {day} lines={n} net={net:,.2f} gross={gross:,.2f} {sorted(notes)}')
    dup = [l for l in lines if l['class'] == 'GOODS_DUP']
    print(f'GOODS_DUP: {len(dup)} lines ₪{gsum(dup):,.2f}')

    # ── TASK 3 already counted ──
    print('\nTASK 3 — already counted?')
    money = {}
    for b in sorted(per_branch):
        bl = [l for l in lines if l['branch'] == b]
        fx = db.execute('SELECT name, amount, pct_value, source FROM fixed_expenses '
                        'WHERE branch_id=? AND month=?', (b, month)).fetchall()
        sales = db.execute('SELECT COALESCE(SUM(amount),0) FROM daily_sales '
                           'WHERE branch_id=? AND substr(date,1,7)=?', (b, month)).fetchone()[0]
        pct = sum(r['pct_value'] or 0 for r in fx if r['name'] == 'זיכיונות')
        model = round(sales * pct / 100, 2)
        roy = gsum([l for l in bl if l['class'] == 'ROYALTY'])
        out = [f"{b} {per_branch[b]['name']}"]
        roy_gap = round(roy - model, 2) if roy else 0.0
        st = 'ALREADY_COUNTED' if roy and abs(roy_gap) <= 1 else ('PARTIAL' if roy else 'n/a')
        out.append(f"  ROYALTY real ₪{roy:,.2f} vs model {pct:g}%×sales ₪{sales:,.2f} = ₪{model:,.2f} "
                   f"→ gap ₪{roy_gap:,.2f} [{st}]")
        fee_missing = 0.0
        for f in sorted({l['class'] for l in bl if l['class'].startswith('FEE:')}):
            real = gsum([l for l in bl if l['class'] == f])
            keys = FEE_FIXED_NAMES[f[4:]]
            have = [r for r in fx if any(k in r['name'] for k in keys) and not r['pct_value']]
            have_amt = round(sum(r['amount'] or 0 for r in have), 2)
            if not have:
                st = 'MISSING'; miss = real
            elif abs(have_amt - real) <= 1:
                st = 'ALREADY_COUNTED'; miss = 0.0
            else:
                st = 'PARTIAL'; miss = round(real - have_amt, 2)
            fee_missing += miss
            names = ','.join(f"{r['name']}({r['source']})" for r in have) or '-'
            out.append(f'  {f:<13} real ₪{real:>10,.2f} | fixed_expenses ₪{have_amt:>10,.2f} [{names}] → {st} {miss:,.2f}')
        wolt = gsum([l for l in bl if l['class'] == 'SKIP_WOLT'])
        wrow = [r for r in fx if r['name'] == 'עמלות Wolt']
        wamt = round(sum(r['amount'] or 0 for r in wrow), 2)
        wx = per_branch[b]['wolt_extractor']
        wst = ('n/a' if not wolt else 'ALREADY_COUNTED' if abs(wamt - wx) <= 1 and wamt
               else 'PARTIAL' if wamt else 'MISSING')
        out.append(f'  WOLT lines ₪{wolt:,.2f} | extractor would write ₪{wx:,.2f} | row ₪{wamt:,.2f} → {wst}')
        gnew = gsum([l for l in bl if l['class'] == 'GOODS_NEW'])
        adj = db.execute('SELECT COALESCE(SUM(amount),0) FROM goods_adjustments '
                         'WHERE branch_id=? AND month=?', (b, month)).fetchone()[0]
        gst = 'n/a' if not gnew else ('PARTIAL' if adj else 'MISSING')
        out.append(f'  GOODS_NEW ₪{gnew:,.2f} | goods_adjustments ₪{adj:,.2f} → {gst}')
        print('\n'.join(out))
        money[b] = {'name': per_branch[b]['name'], 'royalty_gap': roy_gap,
                    'fees_missing': round(fee_missing, 2),
                    'wolt_missing': round(wx - wamt, 2) if wolt else 0.0,
                    'goods_new': round(gnew - adj, 2),
                    'credit': gsum([l for l in bl if l['class'] == 'CREDIT']),
                    'equipment': gsum([l for l in bl if l['class'] == 'EQUIPMENT']),
                    'unknown': gsum([l for l in bl if l['class'] == 'UNKNOWN'])}

    # ── TASK 4 money ──
    print('\nTASK 4 — MISSING money that should hit profit (gross ₪; + = more cost)')
    cols = ['royalty_gap', 'fees_missing', 'wolt_missing', 'goods_new', 'equipment', 'credit', 'unknown']
    print(f"{'branch':<26}" + ''.join(f'{c:>14}' for c in cols) + f"{'TOTAL*':>14}")
    tot = defaultdict(float)
    for b, mrow in sorted(money.items(), key=lambda kv: -sum(kv[1][c] for c in cols[:-1])):
        t = sum(mrow[c] for c in cols[:-1])
        for c in cols:
            tot[c] += mrow[c]
        tot['T'] += t
        print(f"{b:>5} {mrow['name'][:20]:<20}" + ''.join(f'{mrow[c]:>14,.2f}' for c in cols) + f'{t:>14,.2f}')
    print(f"{'CHAIN':<26}" + ''.join(f'{tot[c]:>14,.2f}' for c in cols) + f"{tot['T']:>14,.2f}")
    print('* TOTAL excludes UNKNOWN (never guessed into a bucket)')

    json.dump({'lines': lines, 'money': money}, open(f'/tmp/zik_audit_{month}_report.json', 'w'),
              ensure_ascii=False)
    print(f'\nfull line JSON → /tmp/zik_audit_{month}_report.json  (BilBoy calls this mode: 0; '
          f"fetch used {data.get('calls')})")


if __name__ == '__main__':
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    if len(args) < 2 or not re.fullmatch(r'\d{4}-\d{2}', args[0]):
        sys.exit(__doc__)
    month, mode = args[0], args[1]
    cap = 450
    only = None
    for a in sys.argv[1:]:
        if a.startswith('--cap='):
            cap = int(a.split('=', 1)[1])
        if a.startswith('--branches='):
            only = {int(x) for x in a.split('=', 1)[1].split(',')}
    if mode == 'fetch':
        fetch(month, cap, only)
    elif mode == 'report':
        report(month)
    else:
        sys.exit(__doc__)

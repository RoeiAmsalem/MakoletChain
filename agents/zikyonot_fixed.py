"""
זיכיונות fixed-expense capture (branch-aware, ISOLATED from the goods agent).

Supplier "זיכיונות המכולת בע\"מ" (BilBoy id=13) is the franchisor. For a small
set of branches it bills SOME real fixed expenses (rent etc.) through BilBoy.
The goods agent (bilboy.py) correctly excludes this supplier from
goods_documents — that behavior is LOCKED and untouched here.

This module is a SEPARATE path: it reads the זiכ docs' LINE ITEMS, matches a
fixed, explicit set of named fixed-expense items, and upserts them into
fixed_expenses for the branch+month. It never writes goods_documents and never
calls into the goods pipeline, so a failure here cannot break the goods sync.

What we pull (by line-item NAME match — the only reliable signal) — and ONLY:
  1. שכר דירה                       (rent)
  2. ניהול קטלוג והקלדות מלאי        (catalog / inventory-entry mgmt)
  3. קרן פרסום + מועדון חודשי        (advertising fund + monthly club)
  4. השתתפות בדיוור חברי מועדון      (member-club mailing participation)

What we DELIBERATELY do NOT pull:
  - תמלוגים (royalty) — already modeled as the existing 5%-of-sales 'זיכיונות'
    fixed_expenses row; pulling it would DOUBLE-COUNT. Hard-excluded.
  - ארנונה / חשמל / מים — out of scope per Roei (also billed via franchise but
    not requested). Hard-excluded.
  - Real goods (תנובה, member redemptions, products with barcodes) — handled by
    the goods filter; never touched here.
  - Everything else from supplier 13.

Anything fee-like that matches NEITHER a managed item NOR a known-exclude and has
no barcode is treated as AMBIGUOUS: logged + brrr-surfaced, NOT written. We never
guess.

Scope: ONLY the branches in SCOPE_BRANCHES. All other branches → no-op.

Idempotent: each run does a scoped delete+reinsert of ONLY the managed item names
for (branch, month). Re-running cannot duplicate rows; disappearing docs self-heal.
"""

import logging
import os
import sqlite3
import time
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests

from utils.notify import notify

API_BASE = "https://app.billboy.co.il:5050/api"
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
CHAIN_TOKEN_ENV = 'BILBOY_CHAIN_TOKEN'

# Only these branches route fixed expenses through the franchise (Roei confirmed).
SCOPE_BRANCHES = {9018, 9015}

ZIK_SUPPLIER_MATCH = 'זיכיונות המכולת'

# Whether to store the with-VAT (gross) amount the manager actually pays, vs the
# net line total. Gross matches how rent/electricity are entered elsewhere and how
# the 5% royalty (computed on gross sales) behaves. One flag to flip if needed.
STORE_WITH_VAT = True

# Managed items: (canonical fixed_expenses name, [name-keywords that identify it]).
# Canonical name is the stable fixed_expenses.name (the UNIQUE upsert key part).
MANAGED_ITEMS = [
    ('שכר דירה', ['שכר דירה']),
    ('ניהול קטלוג והקלדות מלאי', ['ניהול קטלוג']),
    ('קרן פרסום + מועדון חודשי', ['קרן פרסום']),
    ('השתתפות בדיוור חברי מועדון', ['השתתפות בדיוור']),
]
MANAGED_NAMES = [c for c, _ in MANAGED_ITEMS]

# Known line-item names under supplier 13 that we intentionally skip (not goods,
# not ambiguous): royalty (double-count), out-of-scope utilities, and goods-ish
# summary/redemption lines. Keeps steady-state "ambiguous" surfacing quiet.
KNOWN_EXCLUDE_KEYWORDS = [
    'תמלוגים',        # royalty — already the 5% row
    'ארנונה',         # municipal property tax — out of scope
    'חיוב חשמל',      # electricity — out of scope
    'מיסי עיריה',     # municipal / water — out of scope
    'מים',            # water — out of scope
    'מימוש',          # member-club redemptions (goods promo credits)
    'תנובה',          # Tnuva goods
    'קניות',          # purchase summary lines (goods)
    'החזרות',         # return summary lines (goods)
]


def _get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _setup_logger(branch_id: int) -> logging.Logger:
    logger = logging.getLogger(f'zik_fixed_{branch_id}')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        log_dir = Path(__file__).parent.parent / 'logs'
        log_dir.mkdir(exist_ok=True)
        fh = logging.FileHandler(log_dir / f'zik_fixed_{branch_id}.log', encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(sh)
    return logger


def _api_get(session, path, params=None, timeout=30):
    resp = session.get(f"{API_BASE}{path}", params=params, timeout=timeout)
    if resp.status_code == 401:
        raise PermissionError("BilBoy token expired")
    resp.raise_for_status()
    return resp.json()


def _classify_line(name: str, barcode: str):
    """Return (managed_canonical_name | None, reason).

    reason ∈ {'managed','known_exclude','goods','zero','ambiguous'}.
    """
    nm = name or ''
    for canon, kws in MANAGED_ITEMS:
        if any(kw in nm for kw in kws):
            return canon, 'managed'
    if any(kw in nm for kw in KNOWN_EXCLUDE_KEYWORDS):
        return None, 'known_exclude'
    if (barcode or '').strip():
        return None, 'goods'
    return None, 'ambiguous'


def run_zikyonot_fixed(branch_id: int, year: int = None, month: int = None) -> dict:
    """Capture the managed זiכ fixed-expense items into fixed_expenses for
    branch+month. No-op for branches outside SCOPE_BRANCHES.

    year/month override the target month (for backfill/testing); default = today.
    Returns {success, branch_id, month, written: {name: amount}, ambiguous: [...]}.
    """
    if branch_id not in SCOPE_BRANCHES:
        return {'success': True, 'skipped': 'out_of_scope', 'branch_id': branch_id}

    log = _setup_logger(branch_id)
    t0 = time.time()
    today = date.today()
    y = year or today.year
    m = month or today.month
    month_str = f'{y:04d}-{m:02d}'
    # Window: first of target month .. end of target month (or today if current).
    from calendar import monthrange
    last_day = monthrange(y, m)[1]
    from_date = f'{y:04d}-{m:02d}-01'
    to_date = f'{y:04d}-{m:02d}-{last_day:02d}'
    log.info("zik-fixed start branch=%d month=%s", branch_id, month_str)

    try:
        conn = _get_db()
        row = conn.execute('SELECT * FROM branches WHERE id=?', (branch_id,)).fetchone()
        conn.close()
        if not row:
            raise ValueError(f"branch {branch_id} not found")
        branch = dict(row)
        bb_id = branch['bilboy_branch_id']
        if not bb_id:
            raise ValueError(f"branch {branch_id} has no bilboy_branch_id")

        token = os.environ.get(CHAIN_TOKEN_ENV) or ''
        if not token:
            raise ValueError("BILBOY_CHAIN_TOKEN not set")
        session = requests.Session()
        session.headers.update({'Authorization': f'Bearer {token}'})

        # Resolve זiכ supplier id(s) for this branch.
        raw = _api_get(session, '/customer/suppliers',
                       params={'customerBranchId': bb_id, 'all': 'true'})
        suppliers = raw.get('suppliers') if isinstance(raw, dict) else raw
        zik_ids = []
        for s in (suppliers or []):
            nm = s.get('title') or s.get('name') or s.get('supplierName') or ''
            if ZIK_SUPPLIER_MATCH in nm:
                sid = str(s.get('id') or s.get('supplierId') or '')
                if sid:
                    zik_ids.append(sid)
        if not zik_ids:
            log.info("no זiכ supplier for branch %d — nothing to capture", branch_id)
            return {'success': True, 'branch_id': branch_id, 'month': month_str,
                    'written': {}, 'ambiguous': []}

        # Fetch זiכ doc headers for the month.
        headers = _api_get(session, '/customer/docs/headers', params={
            'suppliers': ','.join(zik_ids),
            'branches': str(bb_id),
            'from': f'{from_date}T00:00:00',
            'to': f'{to_date}T00:00:00',
        })
        docs = headers if isinstance(headers, list) else (
            headers.get('data') or headers.get('docs') or headers.get('headers') or [])
        docs = [d for d in docs if ZIK_SUPPLIER_MATCH in (d.get('supplierName') or '')]

        buckets = {c: 0.0 for c in MANAGED_NAMES}
        ambiguous = []
        n_goods = n_known = n_managed_lines = 0

        for d in docs:
            did = d.get('id')
            ref = d.get('refNumber') or d.get('number')
            # per-doc VAT rate from header totals (lines here are net `total`)
            twv = float(d.get('totalWithVat') or 0)
            two = float(d.get('totalWithoutVat') or 0)
            vat_rate = (twv / two - 1.0) if (two and twv) else 0.18
            try:
                detail = _api_get(session, '/customer/doc', params={'docId': did}, timeout=15)
            except Exception as e:
                log.warning("doc detail failed ref=%s: %s — skipping doc", ref, e)
                continue
            items = (detail.get('body') or {}).get('items') if isinstance(detail, dict) else None
            for ln in (items or []):
                nm = ln.get('name') or ''
                net = float(ln.get('total') or 0)
                barcode = ln.get('barcode') or ''
                has_vat = bool(ln.get('hasVat'))
                canon, reason = _classify_line(nm, barcode)
                if reason == 'managed':
                    amt = net * (1.0 + vat_rate) if (STORE_WITH_VAT and has_vat) else net
                    buckets[canon] += amt
                    n_managed_lines += 1
                    log.info("  matched %r → %s net=%.2f -> %.2f (ref=%s)",
                             nm, canon, net, amt, ref)
                elif reason == 'known_exclude':
                    n_known += 1
                elif reason == 'goods':
                    n_goods += 1
                elif net == 0:
                    continue
                else:  # ambiguous
                    ambiguous.append({'ref': ref, 'name': nm, 'net': round(net, 2)})

        # Round + drop near-zero buckets.
        written = {c: round(v, 2) for c, v in buckets.items() if round(v, 2) != 0}

        # ── DB: scoped delete+reinsert of ONLY managed names for this branch+month
        conn = _get_db()
        try:
            placeholders = ','.join('?' * len(MANAGED_NAMES))
            conn.execute(
                f"DELETE FROM fixed_expenses WHERE branch_id=? AND month=? "
                f"AND name IN ({placeholders})",
                (branch_id, month_str, *MANAGED_NAMES))
            for name, amt in written.items():
                conn.execute(
                    "INSERT INTO fixed_expenses (branch_id, month, name, amount, "
                    "expense_type, pct_value) VALUES (?,?,?,?, 'monthly', NULL)",
                    (branch_id, month_str, name, amt))
            conn.commit()
        finally:
            conn.close()

        if ambiguous:
            log.warning("AMBIGUOUS זiכ items (NOT written) branch=%d: %s", branch_id, ambiguous)
            notify(
                f"⚠️ זiכ fixed — {branch.get('name', f'Branch {branch_id}')}",
                f"{len(ambiguous)} unrecognized זiכ line item(s) for {month_str} "
                f"— not classified, please review: "
                + "; ".join(f"{a['name']} ₪{a['net']}" for a in ambiguous[:5]),
                dedup_key=f"zik_fixed_ambiguous_{branch_id}_{month_str}")

        dur = round(time.time() - t0, 1)
        log.info("zik-fixed done branch=%d month=%s written=%s "
                 "(managed_lines=%d goods=%d known=%d ambiguous=%d) %.1fs",
                 branch_id, month_str, written, n_managed_lines, n_goods, n_known,
                 len(ambiguous), dur)
        return {'success': True, 'branch_id': branch_id, 'month': month_str,
                'written': written, 'ambiguous': ambiguous}

    except PermissionError:
        log.error("zik-fixed token expired branch=%d", branch_id)
        return {'success': False, 'branch_id': branch_id, 'error': 'token_expired'}
    except Exception as e:
        log.error("zik-fixed failed branch=%d: %s", branch_id, e, exc_info=True)
        return {'success': False, 'branch_id': branch_id, 'error': str(e)}


if __name__ == '__main__':
    import argparse
    import json
    p = argparse.ArgumentParser(description='זiכ fixed-expense capture (isolated)')
    p.add_argument('branch_id', nargs='?', type=int, help='Branch ID (9018/9015)')
    p.add_argument('--year', type=int)
    p.add_argument('--month', type=int)
    p.add_argument('--all-scope', action='store_true', help='Run all SCOPE_BRANCHES')
    a = p.parse_args()
    targets = sorted(SCOPE_BRANCHES) if a.all_scope else [a.branch_id]
    for bid in targets:
        print(json.dumps(run_zikyonot_fixed(bid, a.year, a.month),
                         ensure_ascii=False, default=str))

"""
זיכיונות fixed-expense capture (branch-aware, ISOLATED from the goods agent).

Supplier "זיכיונות המכולת בע\"מ" (BilBoy id=13) is the franchisor. For a small
set of branches it bills SOME real fixed expenses (rent, arnona, utilities, fees)
through BilBoy. The goods agent (bilboy.py) correctly excludes this supplier from
goods_documents — that behavior is LOCKED and untouched here.

This module is a SEPARATE path: it reads the זiכ docs' LINE ITEMS, matches a
fixed, explicit set of named fixed-expense items, and upserts them into
fixed_expenses for the branch+month. It never writes goods_documents and never
calls into the goods pipeline, so a failure here cannot break the goods sync.

What we pull (by line-item NAME match — the only reliable signal):
  1. שכר דירה                       (rent)
  2. ניהול קטלוג והקלדות מלאי        (catalog / inventory-entry mgmt)
  3. קרן פרסום + מועדון חודשי        (advertising fund + monthly club)
  4. השתתפות בדיוור חברי מועדון      (member-club mailing participation)
  5. ארנונה                          (municipal property tax)
  6. חשמל                            ("חיוב חשמל" — electricity)
  7. מים וביוב                       ("מיסי עיריה מים" — municipal water)
  (5–7 added 2026-06: 9018/9015 have NO IEC integration, so their electricity/
   water + arnona are billed via the franchise and were otherwise missing.)

What we DELIBERATELY do NOT pull:
  - תמלוגים (royalty) — already modeled as the existing 5%-of-sales 'זיכיונות'
    fixed_expenses row; pulling it would DOUBLE-COUNT. Hard-excluded.
  - Real goods (תנובה, member redemptions, products with barcodes) — handled by
    the goods filter; never touched here.

Anything fee-like that matches NEITHER a managed item NOR a known-exclude and has
no barcode is treated as UNRECOGNIZED: it is persisted to zik_unclassified (so it
is never silently dropped) and a brrr alert fires for each NEW distinct item name.
We never guess it into fixed_expenses or goods.

Scope: ONLY the branches in SCOPE_BRANCHES. All other branches → no-op.

Data-loss guard: the scoped delete+reinsert of managed names runs ONLY on a
RELIABLE read (at least one invoice/credit doc read with no per-doc-detail
failures). On an empty/partial read we KEEP existing rows, upsert only what we
resolved, and alert — we never zero out good data on a transient glitch.
"""

import logging
import os
import sqlite3
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests

from utils.notify import notify

API_BASE = "https://app.billboy.co.il:5050/api"
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
CHAIN_TOKEN_ENV = 'BILBOY_CHAIN_TOKEN'
IL_TZ = ZoneInfo('Asia/Jerusalem')

# Only these branches route fixed expenses through the franchise (Roei confirmed).
SCOPE_BRANCHES = {9018, 9015}

ZIK_SUPPLIER_MATCH = 'זיכיונות המכולת'

# Whether to store the with-VAT (gross) amount the manager actually pays, vs the
# net line total. Gross matches how rent/electricity are entered and how the 5%
# royalty (computed on gross sales) behaves. One flag to flip if needed.
STORE_WITH_VAT = True

# Managed items: (canonical fixed_expenses name, [name-keywords that identify it]).
# Canonical name is the stable fixed_expenses.name (the UNIQUE upsert key part).
MANAGED_ITEMS = [
    ('שכר דירה', ['שכר דירה']),
    ('ניהול קטלוג והקלדות מלאי', ['ניהול קטלוג']),
    ('קרן פרסום + מועדון חודשי', ['קרן פרסום']),
    ('השתתפות בדיוור חברי מועדון', ['השתתפות בדיוור']),
    ('ארנונה', ['ארנונה']),
    ('חשמל', ['חיוב חשמל']),
    ('מים וביוב', ['מיסי עיריה']),
]
MANAGED_NAMES = [c for c, _ in MANAGED_ITEMS]

# Known line-item names under supplier 13 that we intentionally skip (not goods,
# not unrecognized): royalty (double-count) + goods-ish summary/redemption lines.
KNOWN_EXCLUDE_KEYWORDS = [
    'תמלוגים',        # royalty — already the 5% row
    'מימוש',          # member-club redemptions (goods promo credits)
    'תנובה',          # Tnuva goods
    'קניות',          # purchase summary lines (goods)
    'החזרות',         # return summary lines (goods)
]


def _il_today() -> date:
    return datetime.now(IL_TZ).date()


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

    reason ∈ {'managed','known_exclude','goods','zero','unrecognized'}.
    """
    nm = name or ''
    for canon, kws in MANAGED_ITEMS:
        if any(kw in nm for kw in kws):
            return canon, 'managed'
    if any(kw in nm for kw in KNOWN_EXCLUDE_KEYWORDS):
        return None, 'known_exclude'
    if (barcode or '').strip():
        return None, 'goods'
    return None, 'unrecognized'


def _persist_unclassified(conn, branch_id, month_str, unrecognized, log):
    """Upsert unrecognized items into zik_unclassified. Returns the list of NEW
    distinct item names (not previously recorded for this branch+month)."""
    if not unrecognized:
        return []
    try:
        existing = {r['item_name'] for r in conn.execute(
            "SELECT item_name FROM zik_unclassified WHERE branch_id=? AND month=?",
            (branch_id, month_str)).fetchall()}
    except Exception as e:
        # Table missing / migration not yet run — degrade safely, don't crash capture.
        log.warning("zik_unclassified read failed (%s) — skipping persistence", e)
        return []
    new_names = []
    # Aggregate by item name (a name may appear on several docs in the month).
    agg = {}
    for u in unrecognized:
        a = agg.setdefault(u['name'], {'amount': 0.0, 'ref': u['ref']})
        a['amount'] += u['net']
    for name, a in agg.items():
        if name not in existing:
            new_names.append(name)
        conn.execute(
            "INSERT INTO zik_unclassified "
            "(branch_id, month, item_name, amount, doc_ref, first_seen, last_seen, status) "
            "VALUES (?,?,?,?,?, datetime('now'), datetime('now'), 'pending') "
            "ON CONFLICT(branch_id, month, item_name) DO UPDATE SET "
            "amount=excluded.amount, doc_ref=excluded.doc_ref, last_seen=datetime('now')",
            (branch_id, month_str, name, round(a['amount'], 2), str(a['ref'])))
    conn.commit()
    return new_names


def run_zikyonot_fixed(branch_id: int, year: int = None, month: int = None) -> dict:
    """Capture the managed זiכ fixed-expense items into fixed_expenses for
    branch+month. No-op for branches outside SCOPE_BRANCHES.

    year/month override the target month (for backfill/testing); default = today (IL).
    Returns {success, branch_id, month, mode, written, new_unclassified, ...}.
    """
    if branch_id not in SCOPE_BRANCHES:
        return {'success': True, 'skipped': 'out_of_scope', 'branch_id': branch_id}

    log = _setup_logger(branch_id)
    t0 = time.time()
    today = _il_today()
    y = year or today.year
    m = month or today.month
    month_str = f'{y:04d}-{m:02d}'
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
        bname = branch.get('name', f'Branch {branch_id}')
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
            # Supplier renamed / not found → do NOT touch existing rows. Surface it.
            conn = _get_db()
            had = conn.execute(
                f"SELECT COUNT(*) FROM fixed_expenses WHERE branch_id=? AND month=? "
                f"AND name IN ({','.join('?'*len(MANAGED_NAMES))})",
                (branch_id, month_str, *MANAGED_NAMES)).fetchone()[0]
            conn.close()
            log.warning("no זiכ supplier for branch %d (had %d managed rows) — kept, not wiped",
                        branch_id, had)
            if had:
                notify(f"⚠️ זiכ supplier missing — {bname}",
                       f"Franchise supplier not found for {month_str} but {had} managed "
                       f"row(s) exist — kept existing, capture skipped. Possible rename.")
            return {'success': True, 'branch_id': branch_id, 'month': month_str,
                    'mode': 'no_supplier', 'written': {}, 'new_unclassified': []}

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
        unrecognized = []
        n_goods = n_known = n_managed_lines = 0
        n_fee_docs = 0          # type 3/4 docs successfully read
        detail_failures = 0     # per-doc detail calls that errored

        for d in docs:
            did = d.get('id')
            ref = d.get('refNumber') or d.get('number')
            # Fees/rent/utilities are invoices (type=3) or credits (type=4). Goods
            # arrive as delivery notes (type=2) whose lines carry no barcode —
            # scanning them would mis-flag every product. Restrict to invoices/credits.
            try:
                dtype = int(d.get('type'))
            except (TypeError, ValueError):
                dtype = None
            if dtype not in (3, 4):
                continue
            twv = float(d.get('totalWithVat') or 0)
            two = float(d.get('totalWithoutVat') or 0)
            vat_rate = (twv / two - 1.0) if (two and twv) else 0.18
            try:
                detail = _api_get(session, '/customer/doc', params={'docId': did}, timeout=15)
            except Exception as e:
                detail_failures += 1
                log.warning("doc detail failed ref=%s: %s — skipping doc", ref, e)
                continue
            n_fee_docs += 1
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
                else:  # unrecognized
                    unrecognized.append({'ref': ref, 'name': nm, 'net': round(net, 2)})

        written = {c: round(v, 2) for c, v in buckets.items() if round(v, 2) != 0}

        # ── Reliability gate: only the destructive delete+reinsert on a trusted
        # read (≥1 fee doc read, zero detail failures). Otherwise preserve + alert.
        reliable = (n_fee_docs > 0) and (detail_failures == 0)

        conn = _get_db()
        try:
            placeholders = ','.join('?' * len(MANAGED_NAMES))
            had_existing = conn.execute(
                f"SELECT COUNT(*) FROM fixed_expenses WHERE branch_id=? AND month=? "
                f"AND name IN ({placeholders})",
                (branch_id, month_str, *MANAGED_NAMES)).fetchone()[0]

            if reliable:
                # Authoritative snapshot of the month: clear managed namespace,
                # reinsert what's currently billed (handles genuinely-removed items).
                conn.execute(
                    f"DELETE FROM fixed_expenses WHERE branch_id=? AND month=? "
                    f"AND name IN ({placeholders})",
                    (branch_id, month_str, *MANAGED_NAMES))
                for name, amt in written.items():
                    conn.execute(
                        "INSERT INTO fixed_expenses (branch_id, month, name, amount, "
                        "expense_type, pct_value) VALUES (?,?,?,?, 'monthly', NULL)",
                        (branch_id, month_str, name, amt))
                mode = 'authoritative'
            else:
                # Unreliable read — NEVER wipe. Upsert only what we resolved.
                for name, amt in written.items():
                    conn.execute(
                        "INSERT INTO fixed_expenses (branch_id, month, name, amount, "
                        "expense_type, pct_value) VALUES (?,?,?,?, 'monthly', NULL) "
                        "ON CONFLICT(branch_id, month, name) DO UPDATE SET amount=excluded.amount",
                        (branch_id, month_str, name, amt))
                mode = 'preserved'
            conn.commit()

            # Persist unrecognized items (own try inside; commits separately).
            new_unclassified = _persist_unclassified(conn, branch_id, month_str, unrecognized, log)
        finally:
            conn.close()

        # ── Alerts ────────────────────────────────────────────────
        if not reliable and (had_existing > 0 or detail_failures > 0):
            if n_fee_docs == 0 and had_existing > 0:
                log.warning("EMPTY read: 0 fee docs but %d managed rows exist — kept, NOT wiped",
                            had_existing)
                notify(f"⚠️ זiכ docs missing — {bname}",
                       f"No franchise invoice docs returned for {month_str} but "
                       f"{had_existing} managed row(s) exist — kept existing, did NOT "
                       f"zero out. Possible BilBoy glitch.")
            if detail_failures > 0:
                log.warning("PARTIAL read: %d doc-detail failures — kept existing, upserted resolved",
                            detail_failures)
                notify(f"⚠️ זiכ partial read — {bname}",
                       f"{detail_failures} doc detail call(s) failed for {month_str} — "
                       f"kept existing rows, upserted only what resolved.")

        if new_unclassified:
            log.warning("NEW unrecognized זiכ items branch=%d month=%s: %s",
                        branch_id, month_str, new_unclassified)
            notify(f"⚠️ זiכ unrecognized — {bname}",
                   f"{len(new_unclassified)} new unrecognized franchise item(s) for "
                   f"{month_str}: " + "; ".join(new_unclassified[:5])
                   + " — review on /admin/franchise-classifier")

        dur = round(time.time() - t0, 1)
        log.info("zik-fixed done branch=%d month=%s mode=%s written=%s "
                 "(fee_docs=%d detail_fail=%d managed_lines=%d goods=%d known=%d "
                 "unrecognized=%d new=%d) %.1fs",
                 branch_id, month_str, mode, written, n_fee_docs, detail_failures,
                 n_managed_lines, n_goods, n_known, len(unrecognized),
                 len(new_unclassified), dur)
        return {'success': True, 'branch_id': branch_id, 'month': month_str,
                'mode': mode, 'written': written, 'n_fee_docs': n_fee_docs,
                'detail_failures': detail_failures, 'new_unclassified': new_unclassified}

    except PermissionError:
        log.error("zik-fixed token expired branch=%d", branch_id)
        return {'success': False, 'branch_id': branch_id, 'error': 'token_expired'}
    except Exception as e:
        log.error("zik-fixed failed branch=%d: %s", branch_id, e, exc_info=True)
        return {'success': False, 'branch_id': branch_id, 'error': str(e)}


def run_zikyonot_fixed_nightly(branch_id: int) -> list:
    """Nightly entry point. Always refreshes the CURRENT month; on days 1–7 (IL)
    also refreshes the PREVIOUS month, since franchise invoices post late
    (e.g. May's rent posted June 2) — this avoids needing manual backfill."""
    results = [run_zikyonot_fixed(branch_id)]
    t = _il_today()
    if t.day <= 7:
        py, pm = (t.year, t.month - 1) if t.month > 1 else (t.year - 1, 12)
        results.append(run_zikyonot_fixed(branch_id, year=py, month=pm))
    return results


if __name__ == '__main__':
    import argparse
    import json
    p = argparse.ArgumentParser(description='זiכ fixed-expense capture (isolated)')
    p.add_argument('branch_id', nargs='?', type=int, help='Branch ID (9018/9015)')
    p.add_argument('--year', type=int)
    p.add_argument('--month', type=int)
    p.add_argument('--all-scope', action='store_true', help='Run all SCOPE_BRANCHES')
    p.add_argument('--nightly', action='store_true',
                   help='Use the nightly path (current + prev month on days 1–7)')
    a = p.parse_args()
    targets = sorted(SCOPE_BRANCHES) if a.all_scope else [a.branch_id]
    for bid in targets:
        if a.nightly:
            print(json.dumps(run_zikyonot_fixed_nightly(bid), ensure_ascii=False, default=str))
        else:
            print(json.dumps(run_zikyonot_fixed(bid, a.year, a.month),
                             ensure_ascii=False, default=str))

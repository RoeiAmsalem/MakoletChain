"""Wolt commission fees → automatic system fixed-expense row (source='bilboy_wolt').

Covers: fee-line pattern extraction (incl. the audited 9020 July shape), MTD
growth across two simulated syncs, idempotent re-run, manager edit/delete
rejection, manual rows unaffected, no-Wolt branch has no row, carry-forward
ignores system rows, MTD pro-ration excludes the system row.
"""
import json
import os
import sys
import sqlite3

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from agents.bilboy import (
    extract_wolt_fee_total, is_wolt_fee_line, upsert_wolt_fee_expense,
    WOLT_EXPENSE_NAME, WOLT_EXPENSE_SOURCE,
)
from app import app, _now_il
import app as app_module


# ── extraction unit tests ─────────────────────────────────────────────────

def _doc(twv, two, items):
    return {'totalWithVat': twv, 'totalWithoutVat': two, 'items': items}


def _line(name, cat, total):
    return {'name': name, 'catalogNumber': cat, 'total': total}


def test_wolt_line_patterns():
    assert is_wolt_fee_line(_line('עמלת וולט 17% + מע"מ', '61', 100))
    assert is_wolt_fee_line(_line('וואלט + עמלה 17%+ מעמ', '63', 100))
    assert is_wolt_fee_line(_line('דמי שירות קבועים וולט + 3.8 למשלוח', '67', 100))
    assert is_wolt_fee_line(_line('קמפיין וולט', '69', 100))
    # cat code alone (the 10% service-fee line carries no וולט in the name)
    assert is_wolt_fee_line(_line('דמי שירות עמלה 10%', '62', 100))
    # name alone, unknown cat
    assert is_wolt_fee_line(_line('קמפיין וולט מיוחד', '0', 100))
    # NOT Wolt: royalty, catalog, rent, product EAN
    assert not is_wolt_fee_line(_line('תמלוגים חודשיים לחודש 07/2026', '30', 100))
    assert not is_wolt_fee_line(_line('ניהול קטלוג והקלדות מלאי', '10', 100))
    assert not is_wolt_fee_line(_line('שכר דירה', '40', 100))
    assert not is_wolt_fee_line(_line('חלב 3% קרטון מהדרין', '7290004131074', 100))


def test_extract_pure_wolt_doc_uses_incl_vat_total():
    """The audited 9020 July doc 4321 shape: pure-Wolt invoice — the row must
    carry the doc's incl-VAT total exactly (₪19,236)."""
    doc = _doc(19236.0, 16301.73, [
        _line('עמלת וולט 17% + מע"מ', '61', 8449.44),
        _line('וואלט + עמלה 17%+ מעמ', '63', 5899.03),
        _line('דמי שירות קבועים וולט + 3.8 למשלוח', '67', 672.60),
        _line('קמפיין וולט', '69', 1280.66),
    ])
    assert extract_wolt_fee_total([doc]) == 19236.0


def test_extract_mixed_doc_scales_by_vat_ratio():
    """A doc mixing a Wolt line with a non-Wolt fee contributes only the Wolt
    share, VAT-scaled."""
    doc = _doc(1180.0, 1000.0, [
        _line('עמלת וולט 17% + מע"מ', '61', 600.0),
        _line('ניהול קטלוג והקלדות מלאי', '10', 400.0),
    ])
    assert extract_wolt_fee_total([doc]) == round(600.0 * 1.18, 2)


def test_extract_no_wolt_lines_is_zero():
    doc = _doc(19135.0, 16216.10, [
        _line('תמלוגים חודשיים לחודש 06/2026', '30', 15662.60),
        _line('תמלוגים חודשיים ירקות ופירות לחודש 06/2026', '30', 553.50),
    ])
    assert extract_wolt_fee_total([doc]) == 0.0


# ── app-level fixture ─────────────────────────────────────────────────────

@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_wolt_fees.db')

    original_db = app_module.DB_PATH
    if os.path.exists(test_db):
        os.remove(test_db)
    app_module.DB_PATH = test_db
    app_module.init_db()

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
    import migrate as _migrate
    _migrate.DB_PATH = test_db
    mconn = _migrate.get_connection()
    _migrate.ensure_migrations_table(mconn)
    _migrate.cmd_apply(mconn)
    mconn.close()

    conn = sqlite3.connect(test_db, timeout=30)
    conn.execute("DELETE FROM branches")
    conn.execute("DELETE FROM users")
    conn.execute("DELETE FROM user_branches")
    conn.execute("INSERT INTO branches (id, name, city, active) VALUES (1, 'אלפא', 'עיר', 1)")
    pw = generate_password_hash('test123')
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, active) "
                 "VALUES (2, 'Mgr', 'mgr@t.com', ?, 'manager', 1)", (pw,))
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, active) "
                 "VALUES (3, 'Admin', 'admin@t.com', ?, 'admin', 1)", (pw,))
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (2, 1)")
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(test_db):
        os.remove(test_db)


def _db():
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _login(client, who='mgr'):
    email = 'admin@t.com' if who == 'admin' else 'mgr@t.com'
    return client.post('/login', data={'email': email, 'password': 'test123'})


def _cur_month():
    return _now_il().strftime('%Y-%m')


# ── upsert semantics ──────────────────────────────────────────────────────

def test_mtd_growth_across_two_syncs(client):
    """Simulated nightly recomputes: the row grows during the month, one row only."""
    m = _cur_month()
    conn = _db()
    assert upsert_wolt_fee_expense(conn, 1, m, 500.0) == 'upserted'
    assert upsert_wolt_fee_expense(conn, 1, m, 1961.0) == 'upserted'   # later sync, more fees
    rows = conn.execute(
        "SELECT amount, source, locked, expense_type FROM fixed_expenses "
        "WHERE branch_id=1 AND month=? AND name=?", (m, WOLT_EXPENSE_NAME)).fetchall()
    conn.close()
    assert len(rows) == 1
    assert rows[0]['amount'] == 1961.0
    assert rows[0]['source'] == WOLT_EXPENSE_SOURCE
    assert rows[0]['locked'] == 1


def test_idempotent_rerun(client):
    m = _cur_month()
    conn = _db()
    upsert_wolt_fee_expense(conn, 1, m, 19236.0)
    upsert_wolt_fee_expense(conn, 1, m, 19236.0)
    rows = conn.execute("SELECT COUNT(*) c FROM fixed_expenses WHERE branch_id=1 AND month=?",
                        (m,)).fetchone()
    assert rows['c'] == 1
    conn.close()


def test_zero_amount_removes_row_no_wolt_branch_shows_nothing(client):
    m = _cur_month()
    conn = _db()
    upsert_wolt_fee_expense(conn, 1, m, 500.0)
    assert upsert_wolt_fee_expense(conn, 1, m, 0.0) == 'deleted'
    assert upsert_wolt_fee_expense(conn, 1, m, 0.0) == 'none'
    c = conn.execute("SELECT COUNT(*) c FROM fixed_expenses WHERE branch_id=1").fetchone()['c']
    conn.close()
    assert c == 0


def test_manual_row_with_reserved_name_never_hijacked(client):
    m = _cur_month()
    conn = _db()
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount, expense_type, source) "
                 "VALUES (1, ?, ?, 777, 'monthly', 'manual')", (m, WOLT_EXPENSE_NAME))
    conn.commit()
    assert upsert_wolt_fee_expense(conn, 1, m, 19236.0) == 'blocked_by_manual_row'
    row = conn.execute("SELECT amount, source FROM fixed_expenses WHERE branch_id=1 AND month=?",
                       (m,)).fetchone()
    conn.close()
    assert row['amount'] == 777 and row['source'] == 'manual'


# ── endpoint guards ───────────────────────────────────────────────────────

def _seed_system_row(m, amount=1961.0):
    conn = _db()
    upsert_wolt_fee_expense(conn, 1, m, amount)
    rid = conn.execute("SELECT id FROM fixed_expenses WHERE branch_id=1 AND month=? AND name=?",
                       (m, WOLT_EXPENSE_NAME)).fetchone()['id']
    conn.close()
    return rid


def test_manager_cannot_edit_or_delete_system_row(client):
    m = _cur_month()
    rid = _seed_system_row(m)
    _login(client)
    r = client.put(f'/api/fixed-expenses/{rid}', json={'amount': 1})
    assert r.status_code == 403
    r = client.delete(f'/api/fixed-expenses/{rid}')
    assert r.status_code == 403
    conn = _db()
    amt = conn.execute("SELECT amount FROM fixed_expenses WHERE id=?", (rid,)).fetchone()['amount']
    conn.close()
    assert amt == 1961.0


def test_manager_cannot_create_reserved_name(client):
    _login(client)
    r = client.post('/api/fixed-expenses',
                    json={'name': WOLT_EXPENSE_NAME, 'amount': 5, 'month': _cur_month()})
    assert r.status_code == 400


def test_admin_can_delete_system_row(client):
    m = _cur_month()
    rid = _seed_system_row(m)
    _login(client, 'admin')
    r = client.delete(f'/api/fixed-expenses/{rid}')
    assert r.status_code == 200


def test_manual_rows_unaffected(client):
    """Managers still fully control their own manual rows."""
    m = _cur_month()
    _login(client)
    assert client.post('/api/fixed-expenses',
                       json={'name': 'שכירות', 'amount': 5000, 'month': m}).status_code == 200
    conn = _db()
    rid = conn.execute("SELECT id FROM fixed_expenses WHERE name='שכירות'").fetchone()['id']
    conn.close()
    assert client.put(f'/api/fixed-expenses/{rid}', json={'amount': 5500}).status_code == 200
    assert client.delete(f'/api/fixed-expenses/{rid}').status_code == 200


def test_list_exposes_source(client):
    m = _cur_month()
    _seed_system_row(m)
    _login(client)
    rows = json.loads(client.get(f'/api/fixed-expenses?month={m}').data)
    wolt = [r for r in rows if r['name'] == WOLT_EXPENSE_NAME]
    assert wolt and wolt[0]['source'] == WOLT_EXPENSE_SOURCE


# ── carry-forward + MTD semantics ─────────────────────────────────────────

def test_carry_forward_ignores_system_rows(client):
    """A system row in the new month must not block manual carry-forward, and
    system rows are never copied forward."""
    conn = _db()
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount, expense_type, source) "
                 "VALUES (1, '2026-01', 'שכירות', 5000, 'monthly', 'manual')")
    conn.commit()
    upsert_wolt_fee_expense(conn, 1, '2026-02', 800.0)   # system row lands first
    app_module._ensure_monthly_expenses(1, '2026-02', conn)
    rows = {r['name']: dict(r) for r in conn.execute(
        "SELECT name, amount, source FROM fixed_expenses WHERE branch_id=1 AND month='2026-02'")}
    conn.close()
    assert 'שכירות' in rows, 'manual carry-forward must not be blocked by the system row'
    assert rows['שכירות']['amount'] == 5000
    # and the JAN system row never existed, so only Feb's own wolt row is there
    assert rows[WOLT_EXPENSE_NAME]['source'] == WOLT_EXPENSE_SOURCE


def test_mtd_does_not_prorate_system_row(client):
    """MTD mode halves חודשי rows but must keep the Wolt row at its actual value."""
    conn = _db()
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount, expense_type, source) "
                 "VALUES (1, '2026-03', 'שכירות', 1000, 'monthly', 'manual')")
    conn.commit()
    upsert_wolt_fee_expense(conn, 1, '2026-03', 500.0)
    fx = app_module._get_fixed_total(1, '2026-03', 0, conn, mtd_factor=0.5)
    conn.close()
    assert fx['fixed_only'] == 1500.0
    # MTD: manual monthly 1000×0.5 + system wolt 500 (NOT pro-rated) = 1000
    assert fx['fixed_only_mtd'] == 1000.0


# ── window + edge cases (sync_wolt_fees_window against a fake BilBoy) ─────

import logging
from datetime import date

import agents.bilboy as bilboy_module
from agents.bilboy import sync_wolt_fees_window

_LOG = logging.getLogger('test_wolt')
_W61 = ('עמלת וולט 17% + מע"מ', '61')
_W67 = ('דמי שירות קבועים וולט + 3.8 למשלוח', '67')
_IPAD = ('iPad Wi-Fi + cellular, serial dyjkxltwm6', '65')
_ROYAL = ('תמלוגים חודשיים לחודש 08/2026', '30')


def _hdr(doc_id, dt, twv, two, lines, dtype=3, status=5):
    return {'id': doc_id, 'refNumber': doc_id, 'date': f'{dt}T00:00:00',
            'type': dtype, 'status': status, 'totalWithVat': twv,
            'totalWithoutVat': two,
            '_items': [{'name': n, 'catalogNumber': c, 'total': t}
                       for (n, c), t in lines]}


class _FakeBilBoy:
    """Serves /customer/docs/headers windowed by DATE (like BilBoy) and
    /customer/doc details; records every call."""
    def __init__(self, docs):
        self.docs = docs
        self.calls = []

    def __call__(self, session, path, params=None, timeout=30):
        self.calls.append((path, dict(params or {})))
        if path == '/customer/docs/headers':
            frm, to = params['from'][:10], params['to'][:10]
            return [{k: v for k, v in d.items() if k != '_items'}
                    for d in self.docs if frm <= d['date'][:10] <= to]
        if path == '/customer/doc':
            d = next(d for d in self.docs if d['id'] == params['docId'])
            return {'body': {'items': d['_items']}}
        raise AssertionError(path)

    def header_windows(self):
        return [(p['from'][:10], p['to'][:10]) for path, p in self.calls
                if path == '/customer/docs/headers']


@pytest.fixture
def wolt_env(client, monkeypatch):
    monkeypatch.setattr(bilboy_module, 'DB_PATH', app_module.DB_PATH)
    monkeypatch.delenv('BILBOY_PREV_MONTH_DAYS', raising=False)
    sent = []
    monkeypatch.setattr(bilboy_module, 'notify',
                        lambda title, msg, **kw: sent.append((title, msg)))

    def run(docs, today):
        fake = _FakeBilBoy(docs)
        monkeypatch.setattr(bilboy_module, '_api_get', fake)
        res = sync_wolt_fees_window(None, '170', 1, ['13'], _LOG,
                                    today_il=today, branch_name='אלפא')
        return fake, res
    return run, sent


def _rows(month=None):
    conn = _db()
    q = "SELECT month, name, amount, source FROM fixed_expenses WHERE branch_id=1"
    rows = [dict(r) for r in conn.execute(q + (" AND month=?" if month else ""),
                                          ((month,) if month else ()))]
    conn.close()
    return rows


def _wolt(month):
    r = [x for x in _rows(month) if x['source'] == WOLT_EXPENSE_SOURCE]
    return r[0]['amount'] if r else None


# the real August shape: 9020 invoice 4570, dated the 31st, issued in September
AUG_9020 = _hdr('4570', '2026-08-31', 20207.0, 17124.37,
                [(_W61, 16000.00), (_W67, 1124.37)], status=7)


def test_invoice_dated_31st_lands_in_its_dates_month_on_day_3(wolt_env):
    run, _ = wolt_env
    fake, res = run([AUG_9020], date(2026, 9, 3))
    assert fake.header_windows() == [('2026-08-01', '2026-08-31'),
                                     ('2026-09-01', '2026-09-30')]
    assert _wolt('2026-08') == 20207.0
    assert _wolt('2026-09') is None


def test_day_10_does_not_touch_previous_month(wolt_env):
    run, sent = wolt_env
    conn = _db()
    upsert_wolt_fee_expense(conn, 1, '2026-08', 20207.0)
    conn.close()
    fake, _ = run([AUG_9020, _hdr('x', '2026-08-20', 99999.0, 84745.76,
                                  [(_W61, 84745.76)])], date(2026, 9, 10))
    # only September is synced; August row present → the late check is a no-op
    assert fake.header_windows() == [('2026-09-01', '2026-09-30')]
    assert _wolt('2026-08') == 20207.0
    assert sent == []


def test_idempotent_upsert_never_a_second_row(wolt_env):
    run, _ = wolt_env
    run([AUG_9020], date(2026, 9, 3))
    run([AUG_9020], date(2026, 9, 4))
    run([AUG_9020], date(2026, 9, 5))
    aug = [r for r in _rows('2026-08') if r['name'] == WOLT_EXPENSE_NAME]
    assert len(aug) == 1 and aug[0]['amount'] == 20207.0


def test_no_wolt_invoice_no_row_and_own_row_deleted_at_zero(wolt_env):
    run, _ = wolt_env
    conn = _db()
    upsert_wolt_fee_expense(conn, 1, '2026-09', 500.0)
    conn.close()
    royalty_only = _hdr('r1', '2026-09-05', 1180.0, 1000.0, [(_ROYAL, 1000.0)])
    _, res = run([royalty_only], date(2026, 9, 20))
    assert _wolt('2026-09') is None
    assert res[0]['action'] == 'deleted'
    assert _rows('2026-09') == []


def test_type4_credit_note_reduces_the_row(wolt_env):
    run, _ = wolt_env
    inv = _hdr('i1', '2026-09-30', 1180.0, 1000.0, [(_W61, 1000.0)])
    credit = _hdr('c1', '2026-09-30', -236.0, -200.0, [(_W61, -200.0)], dtype=4)
    run([inv, credit], date(2026, 10, 2))
    assert _wolt('2026-09') == 944.0


def test_only_wolt_codes_or_names_count_vat_scaled_never_65(wolt_env):
    run, _ = wolt_env
    # mixed doc: Wolt 61 + Wolt-by-name (other cat) + iPad 65 + royalty
    mixed = _hdr('m1', '2026-09-30', 2360.0, 2000.0,
                 [(_W61, 600.0), (('קמפיין וולט מיוחד', '0'), 100.0),
                  (_IPAD, 500.0), (_ROYAL, 800.0)])
    run([mixed], date(2026, 10, 1))
    assert _wolt('2026-09') == round(700.0 * 2360.0 / 2000.0, 2)   # 826.0
    assert not is_wolt_fee_line({'name': 'iPad וולט', 'catalogNumber': '65'})


def test_two_wolt_invoices_in_one_month_are_summed(wolt_env):
    run, _ = wolt_env
    # the real 9001 August pair (4572 + 4573)
    a = _hdr('4572', '2026-08-31', 2408.0, 2040.51, [(_W61, 2040.51)], status=3)
    b = _hdr('4573', '2026-08-31', 4068.0, 3447.59, [(_W61, 3447.59)], status=3)
    run([a, b], date(2026, 9, 2))
    assert _wolt('2026-08') == 6476.0


def test_statuses_3_5_7_11_count_9_excluded(wolt_env):
    run, _ = wolt_env
    docs = [_hdr(f's{st}', '2026-09-15', 118.0, 100.0, [(_W61, 100.0)], status=st)
            for st in (3, 5, 7, 9, 11)]
    fake, _ = run(docs, date(2026, 9, 20))
    assert _wolt('2026-09') == 4 * 118.0
    detail_ids = [p['docId'] for path, p in fake.calls if path == '/customer/doc']
    assert 's9' not in detail_ids


def test_manager_row_never_touched(wolt_env):
    run, _ = wolt_env
    conn = _db()
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount, "
                 "expense_type, source) VALUES (1, '2026-09', 'שכירות', 5000, 'monthly', 'manual')")
    conn.commit()
    conn.close()
    run([_hdr('i', '2026-09-10', 118.0, 100.0, [(_W61, 100.0)])], date(2026, 9, 20))
    run([], date(2026, 9, 21))          # → 0: deletes OUR row only
    rows = _rows('2026-09')
    assert rows == [{'month': '2026-09', 'name': 'שכירות', 'amount': 5000,
                     'source': 'manual'}]


@pytest.mark.parametrize('manual_name', ['וולט', 'עמלות מכירה', 'משלוחים', 'עמלות Wolt',
                                         'Wolt עמלה'])
def test_fuzzy_manager_row_blocks_write_and_alerts(wolt_env, manual_name):
    run, sent = wolt_env
    conn = _db()
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount, "
                 "expense_type, source) VALUES (1, '2026-09', ?, 777, 'monthly', 'manual')",
                 (manual_name,))
    conn.commit()
    conn.close()
    _, res = run([_hdr('i', '2026-09-10', 118.0, 100.0, [(_W61, 100.0)])],
                 date(2026, 9, 20))
    assert res[0]['action'] == 'blocked_by_manual_row'
    assert _wolt('2026-09') is None
    rows = _rows('2026-09')
    assert rows == [{'month': '2026-09', 'name': manual_name, 'amount': 777,
                     'source': 'manual'}]
    assert len(sent) == 1 and 'NOT written' in sent[0][1]


def test_prev_month_still_zero_after_day_7_with_invoice_alerts(wolt_env):
    run, sent = wolt_env
    fake, res = run([AUG_9020], date(2026, 9, 8))
    assert _wolt('2026-08') is None            # outside window: never written
    assert res[-1]['checked'] and res[-1]['amount'] == 20207.0
    assert len(sent) == 1 and '2026-08' in sent[0][1] and '20,207.00' in sent[0][1]


def test_prev_month_zero_no_invoice_no_alert(wolt_env):
    run, sent = wolt_env
    run([], date(2026, 9, 8))
    assert sent == []


def test_partial_read_writes_nothing(wolt_env, monkeypatch):
    run, _ = wolt_env
    conn = _db()
    upsert_wolt_fee_expense(conn, 1, '2026-09', 500.0)
    conn.close()

    class _Flaky(_FakeBilBoy):
        def __call__(self, session, path, params=None, timeout=30):
            if path == '/customer/doc':
                raise RuntimeError('boom')
            return super().__call__(session, path, params, timeout)
    monkeypatch.setattr(bilboy_module, '_api_get',
                        _Flaky([_hdr('i', '2026-09-10', 118.0, 100.0, [(_W61, 100.0)])]))
    res = sync_wolt_fees_window(None, '170', 1, ['13'], _LOG,
                                today_il=date(2026, 9, 20))
    assert res[0]['action'] == 'skipped_partial_read'
    assert _wolt('2026-09') == 500.0


# ── backfill anchor gate ──────────────────────────────────────────────────

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
import backfill_wolt_fees as backfill  # noqa: E402


def test_backfill_anchors_always_fetched_even_out_of_scope():
    targets = backfill.fetch_targets([9001], ['2026-09'])
    assert (9001, '2026-09') in targets
    for bid, ms in backfill.ANCHORS.items():
        for m in ms:
            assert (bid, m) in targets
    assert backfill.ANCHORS[9020]['2026-08'][0] == 20207.00
    assert backfill.ANCHORS[9017]['2026-08'][0] == 5728.00


def test_backfill_missing_anchor_is_a_fail_never_skipped():
    good = {(b, m): want for b, ms in backfill.ANCHORS.items()
            for m, (want, _) in ms.items()}
    lines, failed = backfill.check_anchors(good)
    assert not failed and all('OK' in ln for ln in lines)
    missing = dict(good)
    missing[(9017, '2026-08')] = None
    lines, failed = backfill.check_anchors(missing)
    assert failed and any('MISSING' in ln for ln in lines)
    del missing[(9017, '2026-08')]
    assert backfill.check_anchors(missing)[1]
    off = dict(good)
    off[(9020, '2026-08')] = 20207.01          # August anchors are to the agora
    assert backfill.check_anchors(off)[1]


# ── zikyonot_fixed: Wolt lines are KNOWN, never zik_unclassified ──────────

def test_zik_classifies_wolt_lines_as_known():
    from agents.zikyonot_fixed import _classify_line
    for (name, cat) in (_W61, _W67, ('קמפיין וולט', '63'), ('הנחות על משלוח (מימון)', '69'),
                        ('דמי שירות עמלה 10%', '62')):
        assert _classify_line(name, '', cat) == (None, 'wolt'), name
    # unchanged: managed items, the iPad line stays unrecognized
    assert _classify_line('שכר דירה', '', '40')[1] == 'managed'
    assert _classify_line(_IPAD[0], '', '65')[1] == 'unrecognized'


def test_zik_nightly_uses_months_to_sync(monkeypatch):
    import agents.zikyonot_fixed as zf
    seen = []
    monkeypatch.setattr(zf, 'run_zikyonot_fixed',
                        lambda bid, year=None, month=None: seen.append((year, month)))
    monkeypatch.delenv('BILBOY_PREV_MONTH_DAYS', raising=False)
    monkeypatch.setattr(zf, '_il_today', lambda: date(2027, 1, 3))
    zf.run_zikyonot_fixed_nightly(9018)
    assert seen == [(2027, 1), (2026, 12)]
    seen.clear()
    monkeypatch.setattr(zf, '_il_today', lambda: date(2026, 9, 8))
    zf.run_zikyonot_fixed_nightly(9018)
    assert seen == [(2026, 9)]

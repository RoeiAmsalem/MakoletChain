"""Event-driven billing rework — SUMIT webhook receiver, sweep skip-path,
API-call accounting.

Webhook: the payload is UNTRUSTED and OPAQUE — it can only ever schedule the
read-only sync (rate-limited globally), never write state. Always 200.
Skip-path: when payments/list shows nothing new since the last successful run
this month (and that run resolved every tag), the sync stops at 1 SUMIT call.
Call accounting: every logged run records api_calls; /admin/billing sums the
month; one 🟠 when the total crosses the threshold.
"""
import os
import sqlite3
import sys
import time as _time
from datetime import datetime

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from app import app  # noqa: E402
import app as app_module  # noqa: E402
import utils.notify as notify_module  # noqa: E402
from utils import sumit  # noqa: E402

TEST_DB = os.path.join(os.path.dirname(__file__), 'test_billing_webhook.db')
BRANCH = 126
U_MGR, U_ADMIN = 41, 43
TODAY_REAL = app_module._now_il().strftime('%Y-%m-%d')
MONTH_REAL = TODAY_REAL[:7]
NOW_PREFIX = MONTH_REAL + '-01 08:00'


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(app_module, 'BILLING_START_DATE', '2026-07-05')
    monkeypatch.setattr(app_module, 'BILLING_GRACE_DAYS', 5)
    app_module._payment_sync_last.clear()
    app_module._webhook_sync_last['ts'] = 0.0

    app.config['TESTING'] = True
    original_db = app_module.DB_PATH
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    app_module.DB_PATH = TEST_DB
    app_module.init_db()

    import migrate as _migrate
    _migrate.DB_PATH = TEST_DB
    mconn = _migrate.get_connection()
    _migrate.ensure_migrations_table(mconn)
    _migrate.cmd_apply(mconn)
    mconn.close()

    conn = sqlite3.connect(TEST_DB, timeout=30)
    conn.execute('DELETE FROM branches')
    conn.execute('DELETE FROM users')
    conn.execute('DELETE FROM user_branches')
    conn.execute('DELETE FROM manager_billing')
    conn.execute(
        "INSERT INTO branches (id, name, city, active) VALUES (?, 'המכולת אינשטיין', 'חיפה', 1)",
        (BRANCH,))
    pw = generate_password_hash('test123')
    for uid, email, role in [(U_MGR, 'mgr@test.com', 'manager'),
                             (U_ADMIN, 'admin@test.com', 'admin')]:
        conn.execute(
            "INSERT INTO users (id, name, email, password_hash, role, active) "
            "VALUES (?, ?, ?, ?, ?, 1)", (uid, f'user{uid}', email, pw, role))
        if role == 'manager':
            conn.execute(
                'INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)',
                (uid, BRANCH))
    conn.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, "
        "last_status, updated_at) VALUES (?, ?, 179, 1, 'unpaid', ?)",
        (U_MGR, str(U_MGR), NOW_PREFIX))
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def _db():
    conn = sqlite3.connect(TEST_DB, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _sync_runs():
    conn = _db()
    rows = conn.execute('SELECT * FROM billing_sync_runs ORDER BY id').fetchall()
    conn.close()
    return rows


def _wait_for_runs(n, timeout=2.0):
    deadline = _time.monotonic() + timeout
    while _time.monotonic() < deadline:
        if len(_sync_runs()) >= n:
            return True
        _time.sleep(0.02)
    return len(_sync_runs()) >= n


def _fake_sync(monkeypatch, effect=None, result=None):
    calls = []

    def fake(db, allow_skip=False):
        calls.append(allow_skip)
        if effect:
            effect(db)
        return result or {'connected': True, 'payments_seen': 1,
                          'paid_managers': 1, 'customers': 1}

    monkeypatch.setattr(app_module, '_run_billing_sync', fake)
    return calls


# ── webhook receiver ─────────────────────────────────────────

def test_webhook_valid_hit_triggers_sync(client, monkeypatch):
    calls = _fake_sync(monkeypatch)
    r = client.post('/api/billing/sumit-webhook', json={'EntityID': 123})
    assert r.status_code == 200 and r.get_json()['sync'] == 'started'
    assert _wait_for_runs(1)
    runs = _sync_runs()
    assert runs[-1]['source'] == 'webhook' and runs[-1]['ok'] == 1
    assert calls == [True]          # webhook sync may take the skip path


def test_webhook_needs_no_login(client, monkeypatch):
    # no session at all — SUMIT's server has none
    _fake_sync(monkeypatch)
    r = client.post('/api/billing/sumit-webhook', data=b'x')
    assert r.status_code == 200


def test_webhook_malformed_payload_no_crash(client, monkeypatch):
    calls = _fake_sync(monkeypatch)
    r = client.post('/api/billing/sumit-webhook', data=b'\xff\xfe garbage \x00',
                    content_type='application/octet-stream')
    assert r.status_code == 200
    assert _wait_for_runs(1)
    assert len(calls) == 1


def test_webhook_replay_rate_limited(client, monkeypatch):
    calls = _fake_sync(monkeypatch)
    r1 = client.post('/api/billing/sumit-webhook', json={'a': 1})
    r2 = client.post('/api/billing/sumit-webhook', json={'a': 1})   # replay
    r3 = client.post('/api/billing/sumit-webhook', json={'b': 2})
    assert r1.get_json()['sync'] == 'started'
    assert r2.get_json()['sync'] == 'rate-limited'
    assert r3.get_json()['sync'] == 'rate-limited'
    assert r2.status_code == r3.status_code == 200
    _wait_for_runs(1)
    assert len(calls) == 1          # one sync for three hits


def test_webhook_payload_never_writes_state(client, monkeypatch):
    # a hostile payload that *names* billing fields — the receiver must ignore
    # it entirely; state only ever changes via the read-only sync itself.
    _fake_sync(monkeypatch, result={'connected': True, 'payments_seen': 0,
                                    'paid_managers': 0})
    evil = {'user_id': U_MGR, 'last_paid_date': TODAY_REAL,
            'last_status': 'paid', 'active': 1, 'fee': 0}
    r = client.post('/api/billing/sumit-webhook', json=evil)
    assert r.status_code == 200
    _wait_for_runs(1)
    conn = _db()
    row = conn.execute('SELECT last_status, last_paid_date, fee FROM '
                       'manager_billing WHERE user_id=?', (U_MGR,)).fetchone()
    conn.close()
    assert row['last_status'] == 'unpaid'
    assert row['last_paid_date'] is None
    assert row['fee'] == 179


# ── skip-path (metered calls) ────────────────────────────────

def _mock_sumit(monkeypatch, payments, docs_forbidden=False):
    monkeypatch.setenv('SUMIT_API_KEY', 'k')
    monkeypatch.setenv('SUMIT_ORG_ID', '1')

    def _payments(since):
        sumit._count_call()
        return payments

    def _documents(since):
        assert not docs_forbidden, 'documents/list called on the skip path'
        sumit._count_call()
        return [{'CustomerID': 500, 'DocumentID': 9}]

    def _detail(doc_id):
        sumit._count_call()
        return {'Customer': {'ID': 500, 'ExternalIdentifier': str(U_MGR)}}

    monkeypatch.setattr(sumit, 'list_payments', _payments)
    monkeypatch.setattr(sumit, 'list_documents', _documents)
    monkeypatch.setattr(sumit, 'get_document', _detail)


PAYMENT = {'ID': 7, 'CustomerID': 500,
           'Date': f'{MONTH_REAL}-02T10:00:00+03:00', 'ValidPayment': True}


def _seed_prev_run(payments_seen=1, last_payment_id=7, unmatched=0,
                   month=MONTH_REAL, ok=1):
    conn = _db()
    conn.execute(
        "INSERT INTO billing_sync_runs (started_at, finished_at, source, ok, "
        "payments_seen, paid_managers, api_calls, last_payment_id, "
        "last_payment_date, unmatched, skipped) VALUES (?,?,?,1,?,?,?,?,?,?,0)",
        (f'{month}-02 09:10:00', f'{month}-02 09:10:02', 'auto',
         payments_seen, 1, 3, last_payment_id, f'{month}-02', unmatched))
    conn.commit()
    conn.close()


def test_skip_path_nothing_new(client, monkeypatch):
    _seed_prev_run()
    _mock_sumit(monkeypatch, [PAYMENT], docs_forbidden=True)
    conn = _db()
    sumit.reset_call_count()
    res = app_module._run_billing_sync(conn, allow_skip=True)
    assert res.get('skipped') is True
    assert sumit.call_count() == 1          # payments/list only
    row = conn.execute('SELECT last_status, updated_at FROM manager_billing '
                       'WHERE user_id=?', (U_MGR,)).fetchone()
    assert row['last_status'] == 'unpaid'   # state untouched
    assert row['updated_at'] != NOW_PREFIX  # staleness guard still satisfied
    conn.close()


def test_no_skip_when_new_payment(client, monkeypatch):
    _seed_prev_run(last_payment_id=6, payments_seen=0)
    _mock_sumit(monkeypatch, [PAYMENT])
    conn = _db()
    res = app_module._run_billing_sync(conn, allow_skip=True)
    assert not res.get('skipped')
    assert res['paid_managers'] == 1
    assert conn.execute('SELECT last_status FROM manager_billing WHERE user_id=?',
                        (U_MGR,)).fetchone()[0] == 'paid'
    conn.close()


def test_no_skip_when_prev_unmatched(client, monkeypatch):
    # previous run failed to resolve a tag (receipt not created yet) —
    # skipping would strand that payment, so the full path must run.
    _seed_prev_run(unmatched=1)
    _mock_sumit(monkeypatch, [PAYMENT])
    conn = _db()
    res = app_module._run_billing_sync(conn, allow_skip=True)
    assert not res.get('skipped') and res.get('unmatched') == 0
    conn.close()


def test_no_skip_across_month_rollover(client, monkeypatch):
    _seed_prev_run(month='2001-01')         # prev run belongs to another month
    _mock_sumit(monkeypatch, [PAYMENT])
    conn = _db()
    res = app_module._run_billing_sync(conn, allow_skip=True)
    assert not res.get('skipped')
    conn.close()


def test_manual_sync_never_skips(client, monkeypatch):
    _seed_prev_run()
    _mock_sumit(monkeypatch, [PAYMENT])
    conn = _db()
    app_module._run_billing_sync_logged(conn, 'manual')
    run = _sync_runs()[-1]
    assert run['source'] == 'manual' and run['skipped'] == 0
    conn.close()


# ── call accounting ──────────────────────────────────────────

def test_logged_run_records_api_calls(client, monkeypatch):
    _mock_sumit(monkeypatch, [PAYMENT])
    conn = _db()
    app_module._run_billing_sync_logged(conn, 'auto')
    run = _sync_runs()[-1]
    # payments/list + documents/list + 1 document detail
    assert run['api_calls'] == 3
    assert run['last_payment_id'] == 7 and run['unmatched'] == 0
    conn.close()


def test_admin_header_shows_month_call_total(client, monkeypatch):
    _seed_prev_run()                        # api_calls=3
    _seed_prev_run()                        # api_calls=3
    resp = client.post('/login', data={'email': 'admin@test.com',
                                       'password': 'test123'})
    assert resp.status_code == 302
    html = client.get('/admin/billing').get_data(as_text=True)
    assert 'קריאות API החודש' in html
    assert '<b>6</b>' in html


def test_call_budget_alert_fires_once_on_crossing(client, monkeypatch):
    monkeypatch.setattr(app_module, 'BILLING_CALL_ALERT_THRESHOLD', 8)
    alerts = []
    monkeypatch.setattr(notify_module, 'notify',
                        lambda t, m, **kw: alerts.append((t, kw)))
    _seed_prev_run()                        # month total 3
    _mock_sumit(monkeypatch, [PAYMENT])
    conn = _db()
    app_module._run_billing_sync_logged(conn, 'auto')    # skip path: +1 → 4
    assert _sync_runs()[-1]['api_calls'] == 1
    app_module._run_billing_sync_logged(conn, 'manual')  # full: +3 → 7, below 8
    assert alerts == []
    app_module._run_billing_sync_logged(conn, 'manual')  # +3 → 10, crosses 8
    assert len(alerts) == 1 and alerts[0][1].get('medium') is True
    app_module._run_billing_sync_logged(conn, 'manual')  # already over — quiet
    assert len(alerts) == 1
    conn.close()


# ── alerts on the daily cadence ──────────────────────────────

def test_alert_transitions_survive_one_run_per_day(client, monkeypatch):
    """With the sweep at once daily, a state can JUMP (ok→warning_final,
    warning→locked) between runs — each daily pass must still alert the
    transition it lands on (day-4 'locks tomorrow' and the lock itself)."""
    alerts = []
    monkeypatch.setattr(notify_module, 'notify',
                        lambda t, m, **kw: alerts.append((t, kw)))
    conn = _db()

    # day 4 of grace (2026-07-05 start, grace 5): days_left=1 → warning_final
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-09')
    app_module._billing_alert_pass(conn)
    assert len(alerts) == 1 and 'TOMORROW' in alerts[0][0]
    assert alerts[0][1].get('medium') is True

    # next daily run: locked
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-10')
    app_module._billing_alert_pass(conn)
    assert len(alerts) == 2 and alerts[1][1].get('critical') is True
    conn.close()

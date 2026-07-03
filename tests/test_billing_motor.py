"""Billing motor (layers A/B/C) — sync-on-return, scheduled sweep, alerts.

Layer A: OG-* return params trigger the read-only sync (rate-limited, never
writing state themselves); fast path renders already-flipped, slow path
renders the מתעדכן hint + one auto-refresh.
Layer B: run_sweep gates (own flag, IL window), retry-then-🟠, run-log.
Layer C: one alert per state transition, deduped across repeated runs.
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
import billing_sweep  # noqa: E402

REPO_ROOT = os.path.join(os.path.dirname(__file__), '..')
TEST_DB = os.path.join(os.path.dirname(__file__), 'test_billing_motor.db')

BRANCH = 126
U_MGR, U_OFF, U_ADMIN = 41, 42, 43
START = '2026-07-05'
TODAY_REAL = app_module._now_il().strftime('%Y-%m-%d')
MONTH_REAL = TODAY_REAL[:7]


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(app_module, 'BILLING_START_DATE', START)
    monkeypatch.setattr(app_module, 'BILLING_GRACE_DAYS', 5)
    monkeypatch.setattr(app_module, 'SUMIT_PAYMENT_URL_SET', True)
    monkeypatch.setattr(app_module, 'SUMIT_PAYMENT_URL',
                        'https://pay.sumit.example/prod179/')
    app_module._payment_sync_last.clear()

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
    for uid, email, role in [
        (U_MGR, 'mgr@test.com', 'manager'),
        (U_OFF, 'off@test.com', 'manager'),
        (U_ADMIN, 'admin@test.com', 'admin'),
    ]:
        conn.execute(
            "INSERT INTO users (id, name, email, password_hash, role, active) "
            "VALUES (?, ?, ?, ?, ?, 1)", (uid, f'user{uid}', email, pw, role))
        if role == 'manager':
            conn.execute(
                'INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)',
                (uid, BRANCH))
    # U_MGR billed-active + unpaid, row already touched this (real) month
    conn.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, "
        "last_status, updated_at) VALUES (?, ?, 179, 1, 'unpaid', ?)",
        (U_MGR, str(U_MGR), f'{MONTH_REAL}-01 08:00'))
    conn.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active) "
        "VALUES (?, ?, 179, 0)", (U_OFF, str(U_OFF)))
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


def _row(uid=U_MGR):
    conn = _db()
    r = conn.execute('SELECT * FROM manager_billing WHERE user_id=?',
                     (uid,)).fetchone()
    conn.close()
    return r


def _sync_runs():
    conn = _db()
    rows = conn.execute('SELECT * FROM billing_sync_runs ORDER BY id').fetchall()
    conn.close()
    return rows


def _login(client, email):
    resp = client.post('/login', data={'email': email, 'password': 'test123'})
    assert resp.status_code == 302


def _fake_sync(monkeypatch, effect=None, result=None, delay=0.0):
    """Replace _run_billing_sync with a recorder. effect(db) runs inside."""
    calls = []

    def fake(db):
        calls.append(1)
        if delay:
            _time.sleep(delay)
        if effect:
            effect(db)
        return result or {'connected': True, 'payments_seen': 1,
                          'paid_managers': 1, 'customers': 1}

    monkeypatch.setattr(app_module, '_run_billing_sync', fake)
    return calls


def _mark_paid(db):
    db.execute("UPDATE manager_billing SET last_paid_date=?, last_status='paid' "
               "WHERE user_id=?", (TODAY_REAL, U_MGR))
    db.commit()


# ── layer A: sync-on-return ──────────────────────────────────

def test_og_triggers_sync_fast_path(client, monkeypatch):
    calls = _fake_sync(monkeypatch, effect=_mark_paid)
    _login(client, 'mgr@test.com')
    html = client.get('/account?OG-PaymentID=p1&OG-DocumentNumber=40005') \
        .get_data(as_text=True)
    assert len(calls) == 1
    assert 'התשלום התקבל' in html and 'קבלה מס\' 40005' in html
    assert 'המנוי פעיל' in html          # state re-read after inline sync
    assert 'מתעדכן' not in html          # fast path — no pending hint
    runs = _sync_runs()
    assert len(runs) == 1 and runs[0]['source'] == 'payment' and runs[0]['ok'] == 1


def test_og_rate_limited_per_user(client, monkeypatch):
    calls = _fake_sync(monkeypatch, effect=_mark_paid)
    _login(client, 'mgr@test.com')
    client.get('/account?OG-PaymentID=p1')
    client.get('/account?OG-PaymentID=p1')   # replay within 60s
    assert len(calls) == 1


def test_no_og_param_no_sync(client, monkeypatch):
    calls = _fake_sync(monkeypatch)
    _login(client, 'mgr@test.com')
    client.get('/account')
    assert calls == []


def test_og_inactive_row_no_sync(client, monkeypatch):
    calls = _fake_sync(monkeypatch)
    _login(client, 'off@test.com')
    client.get('/account?OG-PaymentID=p1')
    assert calls == []


def test_og_already_paid_no_sync(client, monkeypatch):
    conn = _db()
    _mark_paid(conn)
    conn.close()
    calls = _fake_sync(monkeypatch)
    _login(client, 'mgr@test.com')
    html = client.get('/account?OG-PaymentID=p1').get_data(as_text=True)
    assert calls == [] and 'המנוי פעיל' in html


def test_slow_path_pending_hint_and_forged_og_writes_nothing(client, monkeypatch):
    # forged OG: the sync runs but finds nothing → state must stay unpaid.
    monkeypatch.setattr(app_module, '_PAYMENT_SYNC_INLINE_WAIT', 0.05)
    calls = _fake_sync(monkeypatch, delay=0.3)   # no effect — nothing to find
    _login(client, 'mgr@test.com')
    html = client.get('/account?OG-PaymentID=FORGED"><script>x</script>') \
        .get_data(as_text=True)
    assert len(calls) == 1
    assert 'מתעדכן' in html and 'location.reload' in html
    assert 'ממתין לתשלום החודש' in html      # hero still amber, not flipped
    assert '<script>x</script>' not in html   # autoescaped
    _time.sleep(0.4)                          # let the background sync finish
    assert _row()['last_status'] == 'unpaid'  # OG param wrote NOTHING
    runs = _sync_runs()
    assert len(runs) == 1 and runs[0]['source'] == 'payment'


# ── layer B: scheduled sweep ─────────────────────────────────

def _noon(monkeypatch):
    monkeypatch.setattr(app_module, '_now_il',
                        lambda: datetime(2026, 7, 10, 12, 0, 0))


def test_sweep_flag_off_skips_but_manual_sync_works(client, monkeypatch):
    monkeypatch.setenv('BILLING_SYNC_ENABLED', 'false')
    calls = _fake_sync(monkeypatch)
    assert billing_sweep.run_sweep(retry_delay=0) == 'disabled'
    assert calls == []
    # רענן is NOT gated by the sweep flag
    _login(client, 'admin@test.com')
    r = client.post('/api/admin/billing/sync')
    assert r.status_code == 200 and len(calls) == 1
    assert _sync_runs()[-1]['source'] == 'manual'


def test_sweep_outside_il_window(client, monkeypatch):
    monkeypatch.setattr(app_module, '_now_il',
                        lambda: datetime(2026, 7, 10, 3, 0, 0))
    calls = _fake_sync(monkeypatch)
    assert billing_sweep.run_sweep(retry_delay=0) == 'outside-window'
    assert calls == []


def test_sweep_retry_then_medium_alert(client, monkeypatch):
    _noon(monkeypatch)
    calls = _fake_sync(monkeypatch, result={'connected': True, 'error': 'boom'})
    alerts = []
    monkeypatch.setattr(notify_module, 'notify',
                        lambda t, m, **kw: alerts.append((t, kw)))
    assert billing_sweep.run_sweep(retry_delay=0) == 'failed'
    assert len(calls) == 2                          # first try + one retry
    assert len(alerts) == 1 and alerts[0][1].get('medium') is True
    runs = _sync_runs()
    assert len(runs) == 2 and all(r['ok'] == 0 and r['source'] == 'auto'
                                  for r in runs)


def test_sweep_ok_runs_alert_pass_with_dedup(client, monkeypatch):
    _noon(monkeypatch)
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-07')   # warning (3 left)
    _fake_sync(monkeypatch)
    alerts = []
    monkeypatch.setattr(notify_module, 'notify',
                        lambda t, m, **kw: alerts.append((t, m, kw)))
    assert billing_sweep.run_sweep(retry_delay=0) == 'ok'
    assert len(alerts) == 1 and 'unpaid' in alerts[0][0]
    r = _row()
    assert r['alert_state'] == 'warning' and r['alert_date'] == '2026-07-07'
    # second sweep, same state → no new alert
    assert billing_sweep.run_sweep(retry_delay=0) == 'ok'
    assert len(alerts) == 1


# ── layer C: transition alerts ───────────────────────────────

def test_alert_transitions_full_lifecycle(client, monkeypatch):
    alerts = []
    monkeypatch.setattr(notify_module, 'notify',
                        lambda t, m, **kw: alerts.append((t, kw)))
    conn = _db()

    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-07')   # days_left=3
    app_module._billing_alert_pass(conn)
    app_module._billing_alert_pass(conn)                     # dedup
    assert len(alerts) == 1 and alerts[0][1] == {}

    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-09')   # days_left=1
    app_module._billing_alert_pass(conn)
    assert len(alerts) == 2 and alerts[1][1].get('medium') is True
    assert 'TOMORROW' in alerts[1][0]

    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-10')   # locked
    app_module._billing_alert_pass(conn)
    assert len(alerts) == 3 and alerts[2][1].get('critical') is True

    conn.execute("UPDATE manager_billing SET last_paid_date='2026-07-10', "
                 "last_status='paid' WHERE user_id=?", (U_MGR,))
    conn.commit()
    app_module._billing_alert_pass(conn)                     # locked → paid
    assert len(alerts) == 4 and 'paid' in alerts[3][0]
    conn.close()


def test_alert_paid_without_prior_warning_is_silent(client, monkeypatch):
    alerts = []
    monkeypatch.setattr(notify_module, 'notify',
                        lambda t, m, **kw: alerts.append(t))
    conn = _db()
    conn.execute("UPDATE manager_billing SET last_paid_date='2026-07-08', "
                 "last_status='paid' WHERE user_id=?", (U_MGR,))
    conn.commit()
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-08')
    app_module._billing_alert_pass(conn)
    assert alerts == []
    assert _row()['alert_state'] == 'ok'    # tracked silently
    conn.close()


# ── month boundary (the sweep's rollover correctness) ────────

def _mock_sumit(monkeypatch, pdate):
    monkeypatch.setenv('SUMIT_API_KEY', 'k')
    monkeypatch.setenv('SUMIT_ORG_ID', '1')
    monkeypatch.setattr(sumit, 'list_payments', lambda since: [
        {'ID': 1, 'CustomerID': 500, 'Date': pdate, 'ValidPayment': True}])
    monkeypatch.setattr(sumit, 'list_documents', lambda since: [
        {'CustomerID': 500, 'DocumentID': 9, 'DocumentNumber': 40010}])
    monkeypatch.setattr(sumit, 'get_document', lambda doc_id: {
        'Customer': {'ID': 500, 'ExternalIdentifier': str(U_MGR)}})


def test_sync_ignores_previous_month_payment(client, monkeypatch):
    _mock_sumit(monkeypatch, '2026-06-15T10:00:00+03:00')
    monkeypatch.setattr(app_module, '_now_il',
                        lambda: datetime(2026, 7, 10, 12, 0, 0))
    conn = _db()
    res = app_module._run_billing_sync(conn)
    assert res['paid_managers'] == 0
    assert conn.execute('SELECT last_status FROM manager_billing WHERE user_id=?',
                        (U_MGR,)).fetchone()[0] == 'unpaid'
    conn.close()


def test_sync_counts_current_month_payment(client, monkeypatch):
    _mock_sumit(monkeypatch, '2026-07-02T10:00:00+03:00')
    monkeypatch.setattr(app_module, '_now_il',
                        lambda: datetime(2026, 7, 10, 12, 0, 0))
    conn = _db()
    res = app_module._run_billing_sync(conn)
    assert res['paid_managers'] == 1
    row = conn.execute('SELECT last_status, last_paid_date FROM manager_billing '
                       'WHERE user_id=?', (U_MGR,)).fetchone()
    assert row[0] == 'paid' and row[1] == '2026-07-02'
    conn.close()


# ── admin visibility (task 5) ────────────────────────────────

def test_admin_header_shows_last_sync_layer_and_states(client, monkeypatch):
    conn = _db()
    conn.execute(
        "INSERT INTO billing_sync_runs (started_at, finished_at, source, ok, "
        "payments_seen, paid_managers) VALUES "
        "('2026-07-10 09:00:00', '2026-07-10 09:00:02', 'auto', 1, 5, 3)")
    conn.commit()
    conn.close()
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-07')
    _login(client, 'admin@test.com')
    html = client.get('/admin/billing').get_data(as_text=True)
    assert 'סונכרן לאחרונה' in html and '09:00' in html and 'אוטומטי' in html
    assert '<th>מצב</th>' in html
    assert 'state-chip warning' in html      # U_MGR unpaid → warning chip
    assert 'רענן סטטוס' in html              # the manual button stays

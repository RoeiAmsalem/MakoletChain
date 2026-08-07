"""Manager-facing subscription cancel (the FIRST intentional SUMIT write).

Under test (docs/billing_cancel_spec.md):
- POST /api/account/cancel-subscription: resolves THE SESSION USER's tag →
  matched payment resolution → SUMIT customer → their ONE live recurring item,
  cancels it, VERIFIES with a fresh read, and only then writes cancelled_at.
  Forged request bodies are ignored by construction.
- State machine: cancelled + within paid_until = 'cancelled' (full access);
  past paid_until = direct lock (no grace), never warning.
- Cancelled managers are never nagged: no layer-C alerts, no reminder email,
  no lock email.
- Re-subscribe: a payment dated on/after cancelled_at clears the cancel.
- utils/sumit.py guards: the generic _post still refuses the cancel endpoint;
  _post_recurring only allows its two endpoints.

All SUMIT calls are mocked — nothing here talks to the real API.
"""
import os
import sqlite3
import sys

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from app import app  # noqa: E402
import app as app_module  # noqa: E402
import utils.sumit as sumit  # noqa: E402
import utils.notify as notify_module  # noqa: E402
import billing_reminder  # noqa: E402

REPO_ROOT = os.path.join(os.path.dirname(__file__), '..')
TEST_DB = os.path.join(os.path.dirname(__file__), 'test_billing_cancel.db')

BRANCH = 126
U_MGR, U_NORES, U_ADMIN = 61, 62, 63
START = '2026-07-05'
TODAY = '2026-07-25'          # inside U_MGR's paid window
PAID = '2026-07-20'           # → paid_until 2026-08-20
PAID_UNTIL = '2026-08-20'
CUSTOMER = 555001
ITEM = 999001

LIVE_ITEM = {'ID': ITEM, 'Status': 0, 'UnitPrice': 179.9, 'Quantity': 1.0,
             'Item': {'ID': 2087758337, 'Name': 'קופה שקופה'}}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(app_module, 'BILLING_START_DATE', START)
    monkeypatch.setattr(app_module, 'BILLING_GRACE_DAYS', 5)
    monkeypatch.setattr(app_module, 'BILLING_STALE_DAYS', 365)
    monkeypatch.setattr(app_module, 'SUMIT_PAYMENT_URL_SET', True)
    monkeypatch.setattr(app_module, 'SUMIT_PAYMENT_URL',
                        'https://pay.sumit.example/prod179/')
    monkeypatch.setenv('BILLING_FAKE_TODAY', TODAY)
    app_module._cancel_attempt_last.clear()

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
    conn.execute('DELETE FROM billing_payment_resolutions')
    conn.execute(
        "INSERT INTO branches (id, name, city, active) "
        "VALUES (?, 'המכולת אינשטיין', 'חיפה', 1)", (BRANCH,))
    pw = generate_password_hash('test123')
    for uid, email, role in [
        (U_MGR, 'mgr@test.com', 'manager'),
        (U_NORES, 'nores@test.com', 'manager'),
        (U_ADMIN, 'admin@test.com', 'admin'),
    ]:
        conn.execute(
            "INSERT INTO users (id, name, email, password_hash, role, active) "
            "VALUES (?, ?, ?, ?, ?, 1)", (uid, f'user{uid}', email, pw, role))
        if role == 'manager':
            conn.execute(
                'INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)',
                (uid, BRANCH))
    for uid in (U_MGR, U_NORES):
        conn.execute(
            "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, "
            "last_status, last_paid_date, updated_at) "
            "VALUES (?, ?, 179, 1, 'paid', ?, '2026-07-25 08:00')",
            (uid, str(uid), PAID))
    # U_MGR's SUMIT customer is resolvable (matched receipt-join); U_NORES not
    conn.execute(
        "INSERT INTO billing_payment_resolutions (payment_id, customer_id, "
        "resolution, tag) VALUES (111, ?, 'matched', ?)", (CUSTOMER, str(U_MGR)))
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


def _cancel_log(uid=U_MGR):
    conn = _db()
    rows = conn.execute(
        'SELECT * FROM billing_cancellations WHERE user_id=? ORDER BY id',
        (uid,)).fetchall()
    conn.close()
    return rows


def _login(client, email):
    resp = client.post('/login', data={'email': email, 'password': 'test123'})
    assert resp.status_code == 302


def _mock_sumit(monkeypatch, items_before=None, items_after=None,
                cancel_result=None, notify_spy=None):
    """Wire the three SUMIT touchpoints. list_recurring returns items_before
    on the first call and items_after afterwards (the verification read)."""
    calls = {'list': 0, 'cancel': []}

    def fake_list(customer_id, include_inactive=False):
        calls['list'] += 1
        if calls['list'] == 1:
            return list(items_before or [])
        return list(items_after or [])

    def fake_cancel(customer_id, recurring_item_id):
        calls['cancel'].append((customer_id, recurring_item_id))
        return dict(cancel_result or {'Status': 0})

    monkeypatch.setattr(sumit, 'list_recurring_for_customer', fake_list)
    monkeypatch.setattr(sumit, 'cancel_recurring', fake_cancel)
    if notify_spy is not None:
        monkeypatch.setattr(notify_module, 'notify',
                            lambda *a, **kw: notify_spy.append((a, kw)))
    return calls


# ── endpoint: happy path + guards ────────────────────────────

def test_cancel_happy_path(client, monkeypatch):
    alerts = []
    calls = _mock_sumit(monkeypatch, items_before=[LIVE_ITEM], items_after=[],
                        notify_spy=alerts)
    _login(client, 'mgr@test.com')
    resp = client.post('/api/account/cancel-subscription')
    assert resp.status_code == 200
    assert resp.get_json()['ok'] is True
    assert resp.get_json()['paid_until'] == PAID_UNTIL
    # exactly one cancel, aimed at the session user's own customer + item
    assert calls['cancel'] == [(CUSTOMER, ITEM)]
    row = _row()
    assert row['cancelled_at'] == TODAY
    assert row['cancelled_recurring_id'] == ITEM
    log = _cancel_log()
    assert len(log) == 1 and log[0]['ok'] == 1 and log[0]['api_status'] == 0
    assert log[0]['sumit_customer_id'] == CUSTOMER
    assert [a[0][0] for a in alerts] == ['Subscription cancelled']


def test_forged_body_ignored(client, monkeypatch):
    """A forged request body (someone else's ids) can never redirect the
    cancel — targeting comes from the session only."""
    calls = _mock_sumit(monkeypatch, items_before=[LIVE_ITEM], items_after=[],
                        notify_spy=[])
    _login(client, 'mgr@test.com')
    resp = client.post('/api/account/cancel-subscription',
                       json={'user_id': U_NORES, 'customer_id': 666666,
                             'recurring_item_id': 777777, 'tag': '62'})
    assert resp.status_code == 200
    assert calls['cancel'] == [(CUSTOMER, ITEM)]   # session-resolved, not forged
    assert _row(U_NORES)['cancelled_at'] is None


def test_no_resolvable_customer_404_no_write(client, monkeypatch):
    alerts = []
    calls = _mock_sumit(monkeypatch, notify_spy=alerts)
    _login(client, 'nores@test.com')
    resp = client.post('/api/account/cancel-subscription')
    assert resp.status_code == 404
    assert calls['cancel'] == []
    assert _row(U_NORES)['cancelled_at'] is None
    log = _cancel_log(U_NORES)
    assert len(log) == 1 and log[0]['ok'] == 0
    assert [a[0][0] for a in alerts] == ['Subscription cancel failed']


def test_zero_live_items_404(client, monkeypatch):
    cancelled_item = dict(LIVE_ITEM, Status=1)
    calls = _mock_sumit(monkeypatch, items_before=[cancelled_item],
                        notify_spy=[])
    _login(client, 'mgr@test.com')
    resp = client.post('/api/account/cancel-subscription')
    assert resp.status_code == 404
    assert calls['cancel'] == []
    assert _row()['cancelled_at'] is None


def test_two_live_items_409_no_cancel(client, monkeypatch):
    second = dict(LIVE_ITEM, ID=ITEM + 1)
    calls = _mock_sumit(monkeypatch, items_before=[LIVE_ITEM, second],
                        notify_spy=[])
    _login(client, 'mgr@test.com')
    resp = client.post('/api/account/cancel-subscription')
    assert resp.status_code == 409
    assert calls['cancel'] == []
    assert _row()['cancelled_at'] is None


def test_sumit_refusal_no_local_write(client, monkeypatch):
    _mock_sumit(monkeypatch, items_before=[LIVE_ITEM],
                cancel_result={'Status': 1, 'UserErrorMessage': 'nope'},
                notify_spy=[])
    _login(client, 'mgr@test.com')
    resp = client.post('/api/account/cancel-subscription')
    assert resp.status_code == 502
    assert _row()['cancelled_at'] is None
    assert _cancel_log()[-1]['ok'] == 0


def test_verify_read_still_live_no_local_write(client, monkeypatch):
    _mock_sumit(monkeypatch, items_before=[LIVE_ITEM],
                items_after=[LIVE_ITEM], notify_spy=[])
    _login(client, 'mgr@test.com')
    resp = client.post('/api/account/cancel-subscription')
    assert resp.status_code == 502
    assert _row()['cancelled_at'] is None


def test_already_cancelled_friendly_idempotent(client, monkeypatch):
    calls = _mock_sumit(monkeypatch, notify_spy=[])
    conn = _db()
    conn.execute("UPDATE manager_billing SET cancelled_at=? WHERE user_id=?",
                 (TODAY, U_MGR))
    conn.commit()
    conn.close()
    _login(client, 'mgr@test.com')
    resp = client.post('/api/account/cancel-subscription')
    assert resp.status_code == 200
    assert resp.get_json()['already_cancelled'] is True
    assert calls['cancel'] == []


def test_rate_limited_second_attempt(client, monkeypatch):
    _mock_sumit(monkeypatch, items_before=[], notify_spy=[])
    _login(client, 'mgr@test.com')
    assert client.post('/api/account/cancel-subscription').status_code == 404
    assert client.post('/api/account/cancel-subscription').status_code == 429


def test_admin_cannot_cancel(client, monkeypatch):
    calls = _mock_sumit(monkeypatch, items_before=[LIVE_ITEM], notify_spy=[])
    _login(client, 'admin@test.com')
    resp = client.post('/api/account/cancel-subscription')
    assert resp.status_code == 403
    assert calls['cancel'] == []
    # and the confirm page bounces admins back to /account
    resp = client.get('/account/cancel')
    assert resp.status_code == 302 and resp.headers['Location'].endswith('/account')


# ── /account UI ──────────────────────────────────────────────

def test_account_shows_cancel_link_when_eligible(client, monkeypatch):
    _login(client, 'mgr@test.com')
    html = client.get('/account').get_data(as_text=True)
    assert '/account/cancel' in html
    assert 'ביטול מנוי' in html


def test_account_cancelled_hero_and_renew(client, monkeypatch):
    conn = _db()
    conn.execute("UPDATE manager_billing SET cancelled_at=? WHERE user_id=?",
                 (TODAY, U_MGR))
    conn.commit()
    conn.close()
    _login(client, 'mgr@test.com')
    html = client.get('/account').get_data(as_text=True)
    assert 'המנוי בוטל' in html
    assert 'חידוש מנוי' in html
    assert '/account/cancel' not in html      # no cancel link once cancelled


def test_confirm_page_shows_paid_until(client, monkeypatch):
    _login(client, 'mgr@test.com')
    html = client.get('/account/cancel').get_data(as_text=True)
    assert PAID_UNTIL in html
    assert 'המנוי יבוטל' in html


# ── state machine ────────────────────────────────────────────

def _state(monkeypatch, today, uid=U_MGR, email='mgr@test.com'):
    monkeypatch.setenv('BILLING_FAKE_TODAY', today)
    conn = _db()
    st = app_module._billing_state(uid, 'manager', email, db=conn)
    conn.close()
    return st


def test_cancelled_within_window_full_access(client, monkeypatch):
    conn = _db()
    conn.execute("UPDATE manager_billing SET cancelled_at='2026-07-25' "
                 "WHERE user_id=?", (U_MGR,))
    conn.commit()
    conn.close()
    st = _state(monkeypatch, '2026-08-10')
    assert st == {'state': 'cancelled', 'paid_until': PAID_UNTIL}
    # page access still works (no lock redirect)
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-08-10')
    _login(client, 'mgr@test.com')
    assert client.get('/goods').status_code == 200


def test_cancelled_past_paid_until_locks_directly_no_grace(client, monkeypatch):
    conn = _db()
    conn.execute("UPDATE manager_billing SET cancelled_at='2026-07-25' "
                 "WHERE user_id=?", (U_MGR,))
    conn.commit()
    conn.close()
    # day paid_until+1: straight to locked — never 'warning'
    st = _state(monkeypatch, '2026-08-21')
    assert st['state'] == 'locked' and st['cancelled'] is True
    assert st['days_unpaid'] == 1
    # month rollover never unlocks
    st = _state(monkeypatch, '2026-09-05')
    assert st['state'] == 'locked' and st['days_unpaid'] == 16
    # lock enforced on routes
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-08-21')
    _login(client, 'mgr@test.com')
    resp = client.get('/goods')
    assert resp.status_code == 302 and resp.headers['Location'].endswith('/account')
    assert client.get('/api/summary').status_code == 402


def test_cancelled_lock_ignores_staleness_failopen(client, monkeypatch):
    """cancelled_at is our own local data — a stale sync row must NOT flip a
    lapsed cancelled manager back to exempt."""
    monkeypatch.setattr(app_module, 'BILLING_STALE_DAYS', 3)
    conn = _db()
    conn.execute("UPDATE manager_billing SET cancelled_at='2026-07-25', "
                 "updated_at='2026-07-25 08:00' WHERE user_id=?", (U_MGR,))
    conn.commit()
    conn.close()
    st = _state(monkeypatch, '2026-09-05')
    assert st['state'] == 'locked' and st['cancelled'] is True


# ── never nagged: alerts + emails skip cancelled rows ────────

def test_layer_c_alerts_skip_cancelled(client, monkeypatch):
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-09-05')   # deep in lapse
    alerts = []
    monkeypatch.setattr(notify_module, 'notify',
                        lambda *a, **kw: alerts.append(a))
    conn = _db()
    conn.execute("UPDATE manager_billing SET cancelled_at='2026-07-25', "
                 "updated_at='2026-09-05 08:00'")
    conn.commit()
    with app.app_context():
        sent = app_module._billing_alert_pass(conn)
    assert sent == [] and alerts == []
    # sanity: without cancelled_at the same rows DO alert
    conn.execute("UPDATE manager_billing SET cancelled_at=NULL")
    conn.commit()
    with app.app_context():
        sent = app_module._billing_alert_pass(conn)
    conn.close()
    assert len(sent) > 0


def test_reminder_and_lock_emails_skip_cancelled(client, monkeypatch):
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-09-05')
    conn = _db()
    conn.execute("UPDATE manager_billing SET cancelled_at='2026-07-25', "
                 "updated_at='2026-09-05 08:00'")
    conn.commit()
    with app.app_context():
        rem = billing_reminder.run_pass(conn)
        lock = billing_reminder.run_lock_pass(conn)
    assert rem['sent'] == [] and rem['would_send'] == []
    assert lock['sent'] == [] and lock['would_send'] == []
    # sanity: clearing cancelled_at makes the locked manager selectable again
    conn.execute("UPDATE manager_billing SET cancelled_at=NULL")
    conn.commit()
    with app.app_context():
        lock = billing_reminder.run_lock_pass(conn)
    conn.close()
    assert len(lock['would_send']) > 0


# ── re-subscribe ─────────────────────────────────────────────

def test_resubscribe_payment_clears_cancel(client, monkeypatch):
    """A payment dated on/after cancelled_at (paying again through the same
    link) clears the cancel; an older payment never does."""
    month = app_module._now_il().strftime('%Y-%m')
    cancelled = f'{month}-05'
    new_payment_date = f'{month}-06'
    conn = _db()
    conn.execute("UPDATE manager_billing SET cancelled_at=?, "
                 "cancelled_recurring_id=? WHERE user_id=?",
                 (cancelled, ITEM, U_MGR))
    conn.execute("INSERT INTO billing_payment_resolutions (payment_id, "
                 "customer_id, resolution, tag) VALUES (222, ?, 'matched', ?)",
                 (CUSTOMER, str(U_MGR)))
    conn.commit()

    monkeypatch.setattr(sumit, 'is_connected', lambda: True)
    monkeypatch.setattr(sumit, 'list_payments', lambda since: [
        {'ID': 222, 'CustomerID': CUSTOMER,
         'Date': f'{new_payment_date}T10:00:00', 'Amount': 179.9,
         'ValidPayment': True}])
    monkeypatch.setattr(sumit, 'list_documents', lambda since: [])
    with app.app_context():
        res = app_module._run_billing_sync(conn, allow_skip=False)
    assert res['paid_managers'] == 1
    row = conn.execute('SELECT * FROM manager_billing WHERE user_id=?',
                       (U_MGR,)).fetchone()
    assert row['cancelled_at'] is None
    assert row['cancelled_recurring_id'] is None
    assert row['last_paid_date'] == new_payment_date

    # an OLDER payment (before the cancel) must NOT clear it
    conn.execute("UPDATE manager_billing SET cancelled_at=?, "
                 "cancelled_recurring_id=? WHERE user_id=?",
                 (f'{month}-07', ITEM, U_MGR))
    conn.commit()
    with app.app_context():
        app_module._run_billing_sync(conn, allow_skip=False)
    row = conn.execute('SELECT * FROM manager_billing WHERE user_id=?',
                       (U_MGR,)).fetchone()
    conn.close()
    assert row['cancelled_at'] == f'{month}-07'


# ── utils/sumit guard rails ──────────────────────────────────

def test_generic_post_still_refuses_cancel_endpoint(monkeypatch):
    monkeypatch.setenv('SUMIT_API_KEY', 'k')
    monkeypatch.setenv('SUMIT_ORG_ID', '1')
    with pytest.raises(RuntimeError):
        sumit._post('/billing/recurring/cancel/')
    with pytest.raises(RuntimeError):
        sumit._post('/billing/recurring/listforcustomer/')


def test_post_recurring_refuses_other_endpoints(monkeypatch):
    monkeypatch.setenv('SUMIT_API_KEY', 'k')
    monkeypatch.setenv('SUMIT_ORG_ID', '1')
    for ep in ('/billing/payments/list/', '/billing/recurring/charge/',
               '/billing/recurring/update/', '/accounting/documents/list/'):
        with pytest.raises(RuntimeError):
            sumit._post_recurring(ep)


def test_cancel_recurring_hits_only_the_cancel_endpoint(monkeypatch):
    monkeypatch.setenv('SUMIT_API_KEY', 'k')
    monkeypatch.setenv('SUMIT_ORG_ID', '1')
    posted = []

    class _Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return {'Status': 0}

    def fake_post(url, json=None, headers=None, timeout=None):
        posted.append((url, json))
        return _Resp()

    monkeypatch.setattr(sumit.requests, 'post', fake_post)
    sumit.cancel_recurring(CUSTOMER, ITEM)
    assert len(posted) == 1
    url, body = posted[0]
    assert url.endswith('/billing/recurring/cancel/')
    # Customer is {ID} ONLY — Typed_Customer auto-creates entities on
    # name/email, so nothing else may ever appear in that object.
    assert body['Customer'] == {'ID': CUSTOMER}
    assert body['RecurringCustomerItemID'] == ITEM


def test_recurring_item_is_live_statuses():
    assert sumit.recurring_item_is_live({'Status': 0})            # Active
    assert sumit.recurring_item_is_live({'Status': 11})           # GracePeriod
    assert sumit.recurring_item_is_live({'Status': 14})           # PendingRetry
    assert not sumit.recurring_item_is_live({'Status': 1})        # Cancelled
    assert not sumit.recurring_item_is_live({'Status': 9})        # FinishedExpired
    assert not sumit.recurring_item_is_live({'Status': 13})       # CancelledByCustomer
    assert not sumit.recurring_item_is_live({'Status': 'Cancelled'})

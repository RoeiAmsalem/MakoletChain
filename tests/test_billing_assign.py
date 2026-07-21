"""Manual payment→manager assignment (/api/admin/billing/assign-payment).

For payments whose receipt-join failed (pending/unmatchable in
billing_payment_resolutions): admin assigns by hand → manager flips to paid
with the PAYMENT's date, the payment becomes matched with tag=str(user_id) so
every future sync re-derives the same result (survives), and an
already-matched payment can never be re-assigned. Zero SUMIT calls.
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
import utils.notify as notify_module  # noqa: E402
from utils import sumit  # noqa: E402

TEST_DB = os.path.join(os.path.dirname(__file__), 'test_billing_assign.db')
U_MGR, U_MGR_OFF, U_ADMIN = 51, 52, 53
MONTH = app_module._now_il().strftime('%Y-%m')
PAY_ID, PAY_DATE = 990001, f'{MONTH}-04'


@pytest.fixture
def client():
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

    conn = _db()
    conn.execute('DELETE FROM users')
    conn.execute('DELETE FROM manager_billing')
    pw = generate_password_hash('test123')
    for uid, email, role in [(U_MGR, 'mgr@t.com', 'manager'),
                             (U_MGR_OFF, 'off@t.com', 'manager'),
                             (U_ADMIN, 'admin@t.com', 'admin')]:
        conn.execute(
            "INSERT INTO users (id, name, email, password_hash, role, active) "
            "VALUES (?, ?, ?, ?, ?, 1)", (uid, f'user{uid}', email, pw, role))
    conn.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, "
        "last_status) VALUES (?, ?, 179, 1, 'unpaid')", (U_MGR, str(U_MGR)))
    conn.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, "
        "last_status) VALUES (?, ?, 179, 0, 'unpaid')",
        (U_MGR_OFF, str(U_MGR_OFF)))
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


def _login(client, email):
    return client.post('/login', data={'email': email, 'password': 'test123'})


def _seed_payment(resolution='unmatchable'):
    conn = _db()
    conn.execute(
        "INSERT INTO billing_payment_resolutions (payment_id, customer_id, "
        "resolution, seen_days, first_seen_date, last_seen_date, "
        "payment_date, amount) VALUES (?, 600, ?, 5, ?, ?, ?, 179.0)",
        (PAY_ID, resolution, PAY_DATE, PAY_DATE, PAY_DATE))
    conn.commit()
    conn.close()


def _assign(client, payment_id=PAY_ID, user_id=U_MGR):
    return client.post('/api/admin/billing/assign-payment',
                       json={'payment_id': payment_id, 'user_id': user_id})


def _mgr_row(uid=U_MGR):
    conn = _db()
    row = conn.execute(
        'SELECT last_paid_date, last_status FROM manager_billing '
        'WHERE user_id=?', (uid,)).fetchone()
    conn.close()
    return row


def test_assign_flips_manager_and_records_audit(client, monkeypatch):
    notifies = []
    monkeypatch.setattr(notify_module, 'notify',
                        lambda *a, **k: notifies.append((a, k)))
    _seed_payment()
    _login(client, 'admin@t.com')
    r = _assign(client)
    assert r.status_code == 200 and r.get_json()['ok'] is True

    row = _mgr_row()
    assert (row['last_paid_date'], row['last_status']) == (PAY_DATE, 'paid')
    conn = _db()
    res = conn.execute(
        "SELECT resolution, tag, assigned_by, assigned_at "
        "FROM billing_payment_resolutions WHERE payment_id=?",
        (PAY_ID,)).fetchone()
    conn.close()
    assert res['resolution'] == 'matched' and res['tag'] == str(U_MGR)
    assert res['assigned_by'] == U_ADMIN and res['assigned_at']
    assert len(notifies) == 1               # one 🟡 on assignment
    assert str(PAY_ID) in notifies[0][0][1]


def test_assignment_survives_future_syncs(client, monkeypatch):
    monkeypatch.setattr(notify_module, 'notify', lambda *a, **k: None)
    _seed_payment(resolution='pending')
    _login(client, 'admin@t.com')
    assert _assign(client).status_code == 200

    # Full sync: the payment is STILL receipt-less in SUMIT, but the stored
    # matched tag re-derives paid for the assigned manager — and no doc fetch.
    monkeypatch.setattr(sumit, 'is_connected', lambda: True)

    def _payments(since):
        sumit._count_call()
        return [{'ID': PAY_ID, 'CustomerID': 600,
                 'Date': f'{PAY_DATE}T10:00:00+03:00', 'ValidPayment': True}]

    def _documents(since):
        sumit._count_call()
        return []
    monkeypatch.setattr(sumit, 'list_payments', _payments)
    monkeypatch.setattr(sumit, 'list_documents', _documents)

    def boom(doc_id):
        raise AssertionError('assigned payment must not fetch documents')
    monkeypatch.setattr(sumit, 'get_document', boom)

    conn = _db()
    res = app_module._run_billing_sync(conn, allow_skip=False)
    assert res['unmatched'] == 0 and res['paid_managers'] == 1
    row = _mgr_row()
    assert (row['last_paid_date'], row['last_status']) == (PAY_DATE, 'paid')

    # and the skip path is eligible again (all ids terminal-resolved)
    conn.execute(
        "INSERT INTO billing_sync_runs (started_at, source, ok, payments_seen, "
        "api_calls, last_payment_id, unmatched, skipped) "
        "VALUES (?, 'auto', 1, 1, 2, ?, 0, 0)",
        (f'{MONTH}-04 09:10:00', PAY_ID))
    conn.commit()
    sumit.reset_call_count()
    res2 = app_module._run_billing_sync(conn, allow_skip=True)
    assert res2.get('skipped') is True and sumit.call_count() == 1
    conn.close()


def test_cannot_assign_already_matched(client, monkeypatch):
    notifies = []
    monkeypatch.setattr(notify_module, 'notify',
                        lambda *a, **k: notifies.append(1))
    _seed_payment()
    _login(client, 'admin@t.com')
    assert _assign(client).status_code == 200
    r2 = _assign(client, user_id=U_MGR)     # second attempt — already matched
    assert r2.status_code == 409
    res = _mgr_row()
    assert res['last_status'] == 'paid'     # unchanged
    assert len(notifies) == 1               # no second alert


def test_assign_validations(client, monkeypatch):
    monkeypatch.setattr(notify_module, 'notify', lambda *a, **k: None)
    _seed_payment()
    _login(client, 'admin@t.com')
    assert _assign(client, payment_id=123456).status_code == 404
    assert _assign(client, user_id=U_MGR_OFF).status_code == 400   # inactive
    assert _assign(client, user_id=99999).status_code == 400       # no row
    assert _mgr_row()['last_status'] == 'unpaid'


def test_assign_admin_only(client):
    # off@t.com: manager with billing active=0 → billing-exempt, so the
    # request reaches the route and fails on ROLE (not on the paywall).
    _seed_payment()
    _login(client, 'off@t.com')
    assert _assign(client).status_code == 403

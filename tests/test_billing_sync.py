"""SUMIT sync — the two live-payment fixes (2026-07-02 ₪1 e2e test).

1. Date window: SUMIT's Date_To/DateTo is a midnight cutoff, so list_payments /
   list_documents must send TOMORROW or same-day payments are invisible.
2. Join: payment → receipt document (payment.CustomerID == doc.Customer.ID)
   → doc.Customer.ExternalIdentifier == manager_billing.sumit_tag. The CRM
   entity path returns null properties and must not be relied on.
"""
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app  # noqa: E402
import app as app_module  # noqa: E402
from utils import sumit  # noqa: E402

REPO_ROOT = os.path.join(os.path.dirname(__file__), '..')
TEST_DB = os.path.join(os.path.dirname(__file__), 'test_billing_sync.db')

MONTH = app_module._now_il().strftime('%Y-%m')
PDATE = f'{MONTH}-02'


# ── fix 1: date window includes today ────────────────────────

def _capture_post(monkeypatch):
    captured = {}

    class FakeResp:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {'Status': 0, 'Data': {'Payments': [], 'Documents': []}}

    def fake_post(url, json=None, **kw):
        captured['url'] = url
        captured['body'] = json
        return FakeResp()

    monkeypatch.setattr(sumit.requests, 'post', fake_post)
    monkeypatch.setenv('SUMIT_API_KEY', 'test-key')
    monkeypatch.setenv('SUMIT_ORG_ID', '123')
    return captured


def test_list_payments_window_ends_tomorrow(monkeypatch):
    captured = _capture_post(monkeypatch)
    sumit.list_payments('2026-07-01')
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime('%Y-%m-%d')
    assert captured['body']['Date_To'] == tomorrow
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    assert captured['body']['Date_To'] > today  # same-day payments stay visible


def test_list_documents_window_ends_tomorrow(monkeypatch):
    captured = _capture_post(monkeypatch)
    sumit.list_documents('2026-07-01')
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime('%Y-%m-%d')
    assert captured['body']['DateTo'] == tomorrow


# ── fix 2: document-based join ───────────────────────────────

@pytest.fixture
def db():
    app.config['TESTING'] = True
    original_db = app_module.DB_PATH
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    app_module.DB_PATH = TEST_DB
    app_module.init_db()

    sys.path.insert(0, os.path.join(REPO_ROOT, 'scripts'))
    import migrate as _migrate
    _migrate.DB_PATH = TEST_DB
    mconn = _migrate.get_connection()
    _migrate.ensure_migrations_table(mconn)
    _migrate.cmd_apply(mconn)
    mconn.close()

    conn = sqlite3.connect(TEST_DB, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('DELETE FROM manager_billing')
    for uid in (26, 27):
        conn.execute(
            "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, last_status) "
            "VALUES (?, ?, 179, 1, 'unpaid')", (uid, str(uid)))
    conn.commit()
    yield conn
    conn.close()
    app_module.DB_PATH = original_db
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def _fake_sumit(monkeypatch, payments, documents, details):
    monkeypatch.setattr(sumit, 'is_connected', lambda: True)
    monkeypatch.setattr(sumit, 'list_payments', lambda since: payments)
    monkeypatch.setattr(sumit, 'list_documents', lambda since: documents)
    monkeypatch.setattr(sumit, 'get_document', lambda doc_id: details[doc_id])


def _row(db, uid):
    return db.execute(
        'SELECT last_paid_date, last_status FROM manager_billing WHERE user_id=?',
        (uid,)).fetchone()


def test_payment_joined_to_manager_via_document(db, monkeypatch):
    _fake_sumit(
        monkeypatch,
        payments=[{'ID': 1, 'CustomerID': 555, 'Amount': 1.0,
                   'Date': f'{PDATE}T08:47:39+03:00', 'ValidPayment': True}],
        documents=[{'DocumentID': 999, 'CustomerID': 555, 'DocumentNumber': 40000}],
        details={999: {'Customer': {'ID': 555, 'ExternalIdentifier': '26'}}})
    with app.test_request_context():
        result = app_module._run_billing_sync(db)
    assert result == {'connected': True, 'payments_seen': 1,
                      'paid_managers': 1, 'customers': 1}
    paid = _row(db, 26)
    assert (paid['last_paid_date'], paid['last_status']) == (PDATE, 'paid')
    other = _row(db, 27)
    assert other['last_status'] == 'unpaid' and other['last_paid_date'] is None


def test_mismatched_document_customer_does_not_join(db, monkeypatch):
    # receipt embeds a DIFFERENT customer than the payer → no mapping, nobody paid
    _fake_sumit(
        monkeypatch,
        payments=[{'ID': 1, 'CustomerID': 555, 'Amount': 1.0,
                   'Date': f'{PDATE}T08:47:39+03:00', 'ValidPayment': True}],
        documents=[{'DocumentID': 999, 'CustomerID': 555, 'DocumentNumber': 40000}],
        details={999: {'Customer': {'ID': 777, 'ExternalIdentifier': '26'}}})
    with app.test_request_context():
        result = app_module._run_billing_sync(db)
    assert result['paid_managers'] == 0
    assert _row(db, 26)['last_status'] == 'unpaid'


def test_invalid_payment_ignored(db, monkeypatch):
    _fake_sumit(
        monkeypatch,
        payments=[{'ID': 1, 'CustomerID': 555, 'Amount': 1.0,
                   'Date': f'{PDATE}T08:47:39+03:00', 'ValidPayment': False}],
        documents=[{'DocumentID': 999, 'CustomerID': 555, 'DocumentNumber': 40000}],
        details={999: {'Customer': {'ID': 555, 'ExternalIdentifier': '26'}}})
    with app.test_request_context():
        result = app_module._run_billing_sync(db)
    assert result['payments_seen'] == 0
    assert _row(db, 26)['last_status'] == 'unpaid'


def test_two_payments_same_customer_fetch_document_once(db, monkeypatch):
    calls = []

    def counting_get_document(doc_id):
        calls.append(doc_id)
        return {'Customer': {'ID': 555, 'ExternalIdentifier': '26'}}

    monkeypatch.setattr(sumit, 'is_connected', lambda: True)
    monkeypatch.setattr(sumit, 'list_payments', lambda since: [
        {'ID': 1, 'CustomerID': 555, 'Date': f'{PDATE}T08:47:39+03:00', 'ValidPayment': True},
        {'ID': 2, 'CustomerID': 555, 'Date': f'{PDATE}T08:50:01+03:00', 'ValidPayment': True}])
    monkeypatch.setattr(sumit, 'list_documents', lambda since: [
        {'DocumentID': 999, 'CustomerID': 555}])
    monkeypatch.setattr(sumit, 'get_document', counting_get_document)
    with app.test_request_context():
        result = app_module._run_billing_sync(db)
    assert result['paid_managers'] == 1
    assert calls == [999]
    assert _row(db, 26)['last_status'] == 'paid'

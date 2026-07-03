"""Billing paywall (stage 2) — state machine + route enforcement.

Policy under test: billing starts BILLING_START_DATE (2026-07-05 here);
active-billed unpaid managers get a warning banner for BILLING_GRACE_DAYS
days counted from max(start, 1st of month, activated_at), then are locked to
/account until a payment lands. admin/demo/active=0 are never affected; a
toggled-ON ceo goes through the same warning→lock machine as a manager
(2026-07-03), a toggled-OFF ceo stays exempt.
Fail-open: unreadable rows or a row the sync hasn't touched this month must
never lock anyone.

Dates are simulated via the BILLING_FAKE_TODAY env override (read per call by
_billing_today) — no real data or clocks are edited.
"""
import os
import sqlite3
import sys

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app  # noqa: E402
import app as app_module  # noqa: E402

REPO_ROOT = os.path.join(os.path.dirname(__file__), '..')
TEST_DB = os.path.join(os.path.dirname(__file__), 'test_billing_paywall.db')

BRANCH = 126
U_MGR, U_OFF, U_ADMIN, U_DEMO = 31, 32, 33, 34
U_CEO, U_CEO_NOROW = 35, 36
START = '2026-07-05'
GRACE = 5


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(app_module, 'BILLING_START_DATE', START)
    monkeypatch.setattr(app_module, 'BILLING_GRACE_DAYS', GRACE)
    monkeypatch.setattr(app_module, 'SUMIT_PAYMENT_URL_SET', True)
    monkeypatch.setattr(app_module, 'SUMIT_PAYMENT_URL',
                        'https://pay.sumit.example/prod179/')

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
        (U_DEMO, app_module.DEMO_ACCOUNT_EMAIL, 'manager'),
        (U_CEO, 'ceo@test.com', 'ceo'),
        (U_CEO_NOROW, 'ceo2@test.com', 'ceo'),
    ]:
        conn.execute(
            "INSERT INTO users (id, name, email, password_hash, role, active) "
            "VALUES (?, ?, ?, ?, ?, 1)", (uid, f'user{uid}', email, pw, role))
        if role == 'manager':
            conn.execute(
                'INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)',
                (uid, BRANCH))
    # U_MGR: billed-active, unpaid, row synced within July (trustworthy)
    conn.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, last_status, updated_at) "
        "VALUES (?, ?, 179, 1, 'unpaid', '2026-07-05 08:00')", (U_MGR, str(U_MGR)))
    # U_OFF: not billed
    conn.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active) "
        "VALUES (?, ?, 179, 0)", (U_OFF, str(U_OFF)))
    # U_CEO: billed-active ceo, unpaid, row synced within July (trustworthy) —
    # goes through the same machine as U_MGR. U_CEO_NOROW deliberately has no
    # row (materialization + no-row exemption are tested).
    conn.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, last_status, updated_at) "
        "VALUES (?, ?, 179, 1, 'unpaid', '2026-07-05 08:00')", (U_CEO, str(U_CEO)))
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


def _set_row(uid=U_MGR, **cols):
    conn = _db()
    sets = ', '.join(f'{k}=?' for k in cols)
    conn.execute(f'UPDATE manager_billing SET {sets} WHERE user_id={uid}',
                 tuple(cols.values()))
    conn.commit()
    conn.close()


def _state(monkeypatch, today, uid=U_MGR, role='manager', email='mgr@test.com'):
    monkeypatch.setenv('BILLING_FAKE_TODAY', today)
    conn = _db()
    st = app_module._billing_state(uid, role, email, db=conn)
    conn.close()
    return st


def _login(client, email):
    resp = client.post('/login', data={'email': email, 'password': 'test123'})
    assert resp.status_code == 302


# ── state machine ────────────────────────────────────────────

def test_before_start_date_exempt(client, monkeypatch):
    assert _state(monkeypatch, '2026-07-04')['state'] == 'exempt'


def test_far_future_start_everyone_exempt(client, monkeypatch):
    monkeypatch.setattr(app_module, 'BILLING_START_DATE', '2027-01-01')
    assert _state(monkeypatch, '2026-07-20')['state'] == 'exempt'


def test_warning_day1_through_day5(client, monkeypatch):
    st = _state(monkeypatch, '2026-07-05')
    assert (st['state'], st['days_unpaid'], st['days_left']) == ('warning', 1, 5)
    st = _state(monkeypatch, '2026-07-09')
    assert (st['state'], st['days_unpaid'], st['days_left']) == ('warning', 5, 1)


def test_locked_from_day6(client, monkeypatch):
    st = _state(monkeypatch, '2026-07-10')
    assert st['state'] == 'locked'
    assert st['days_unpaid'] == 6


def test_paid_this_month_ok(client, monkeypatch):
    _set_row(last_paid_date='2026-07-08', last_status='paid')
    assert _state(monkeypatch, '2026-07-20')['state'] == 'ok'


def test_month_rollover_restarts_grace(client, monkeypatch):
    # paid July → ok in July; unpaid again in August, counted from Aug 1
    _set_row(last_paid_date='2026-07-20', last_status='paid',
             updated_at='2026-08-01 06:00')
    st = _state(monkeypatch, '2026-08-03')
    assert (st['state'], st['days_unpaid'], st['days_left']) == ('warning', 3, 3)
    st = _state(monkeypatch, '2026-08-08')
    assert st['state'] == 'locked'


def test_stale_row_fails_open(client, monkeypatch):
    # row never touched this month (sync didn't run after rollover) → exempt
    _set_row(last_paid_date='2026-07-20', last_status='paid',
             updated_at='2026-07-30 23:00')
    assert _state(monkeypatch, '2026-08-10')['state'] == 'exempt'


def test_toggled_mid_month_counts_from_toggle(client, monkeypatch):
    _set_row(activated_at='2026-07-20', updated_at='2026-07-20 10:00')
    st = _state(monkeypatch, '2026-07-21')
    assert (st['state'], st['days_unpaid']) == ('warning', 2)
    assert _state(monkeypatch, '2026-07-26')['state'] == 'locked'


def test_admin_demo_inactive_norow_exempt(client, monkeypatch):
    assert _state(monkeypatch, '2026-07-20', uid=U_ADMIN, role='admin',
                  email='admin@test.com')['state'] == 'exempt'
    assert _state(monkeypatch, '2026-07-20', uid=U_DEMO, role='manager',
                  email=app_module.DEMO_ACCOUNT_EMAIL)['state'] == 'exempt'
    assert _state(monkeypatch, '2026-07-20', uid=U_OFF,
                  email='off@test.com')['state'] == 'exempt'
    assert _state(monkeypatch, '2026-07-20', uid=999,
                  email='ghost@test.com')['state'] == 'exempt'


def test_ceo_active_unpaid_goes_through_machine(client, monkeypatch):
    # A toggled-ON ceo is billable exactly like a manager: warning inside
    # grace, locked after it.
    st = _state(monkeypatch, '2026-07-06', uid=U_CEO, role='ceo',
                email='ceo@test.com')
    assert (st['state'], st['days_unpaid']) == ('warning', 2)
    st = _state(monkeypatch, '2026-07-10', uid=U_CEO, role='ceo',
                email='ceo@test.com')
    assert st['state'] == 'locked'


def test_ceo_toggled_off_exempt(client, monkeypatch):
    # Toggled OFF (or no row at all) → the ceo sees nothing, like any
    # unbilled user.
    _set_row(uid=U_CEO, active=0)
    assert _state(monkeypatch, '2026-07-20', uid=U_CEO, role='ceo',
                  email='ceo@test.com')['state'] == 'exempt'
    assert _state(monkeypatch, '2026-07-20', uid=U_CEO_NOROW, role='ceo',
                  email='ceo2@test.com')['state'] == 'exempt'


def test_admin_always_exempt_even_with_active_row(client, monkeypatch):
    # role='admin' short-circuits before the row is read — even a
    # (mis-)created active unpaid row can never warn or lock the admin.
    conn = _db()
    conn.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, last_status, updated_at) "
        "VALUES (?, ?, 179, 1, 'unpaid', '2026-07-05 08:00')",
        (U_ADMIN, str(U_ADMIN)))
    conn.commit()
    conn.close()
    assert _state(monkeypatch, '2026-07-20', uid=U_ADMIN, role='admin',
                  email='admin@test.com')['state'] == 'exempt'


def test_admin_billing_materializes_ceo_rows(client, monkeypatch):
    # /admin/billing includes role IN ('manager','ceo'): a ceo with no row
    # gets one auto-created OFF (active=0) and appears in the roster.
    _login(client, 'admin@test.com')
    resp = client.get('/admin/billing')
    assert resp.status_code == 200
    html = resp.data.decode('utf-8')
    assert 'ceo@test.com' in html
    assert 'ceo2@test.com' in html
    conn = _db()
    row = conn.execute(
        'SELECT active, sumit_tag FROM manager_billing WHERE user_id=?',
        (U_CEO_NOROW,)).fetchone()
    conn.close()
    assert row is not None
    assert row['active'] == 0
    assert row['sumit_tag'] == str(U_CEO_NOROW)


def test_unreadable_billing_data_fails_open(client, monkeypatch):
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-10')

    class BrokenDB:
        def execute(self, *a, **kw):
            raise sqlite3.OperationalError('no such table: manager_billing')

    st = app_module._billing_state(U_MGR, 'manager', 'mgr@test.com',
                                   db=BrokenDB())
    assert st['state'] == 'exempt'


# ── route enforcement ────────────────────────────────────────

def test_locked_pages_redirect_to_account(client, monkeypatch):
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-15')
    _login(client, 'mgr@test.com')
    for path in ('/sales', '/', '/goods', '/employees'):
        resp = client.get(path)
        assert resp.status_code == 302, path
        assert resp.headers['Location'].endswith('/account'), path


def test_locked_account_shows_lock_card_with_own_tag(client, monkeypatch):
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-15')
    _login(client, 'mgr@test.com')
    resp = client.get('/account')
    assert resp.status_code == 200
    html = resp.data.decode('utf-8')
    assert 'הגישה הושהתה עד להסדרת התשלום' in html
    assert 'kpi-card--loss' in html  # the hero IS the lock card (red accent)
    assert f'?customerexternalidentifier={U_MGR}' in html
    assert 'href="tel:0523455860"' in html


def test_locked_logout_reachable(client, monkeypatch):
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-15')
    _login(client, 'mgr@test.com')
    resp = client.get('/logout')
    assert resp.status_code == 302
    assert '/account' not in resp.headers['Location']


def test_locked_api_returns_402(client, monkeypatch):
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-15')
    _login(client, 'mgr@test.com')
    resp = client.get('/api/summary')
    assert resp.status_code == 402
    assert resp.get_json() == {'error': 'payment_required'}


def test_payment_while_locked_unlocks_without_admin(client, monkeypatch):
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-15')
    _login(client, 'mgr@test.com')
    assert client.get('/sales').status_code == 302
    # sync lands a payment (what /api/admin/billing/sync writes on success)
    _set_row(last_paid_date='2026-07-15', last_status='paid')
    resp = client.get('/sales')
    assert resp.status_code == 200


def test_warning_banner_days_remaining(client, monkeypatch):
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-06')  # day 2 of 5
    _login(client, 'mgr@test.com')
    resp = client.get('/')
    assert resp.status_code == 200
    html = resp.data.decode('utf-8')
    assert 'המנוי טרם שולם החודש' in html
    assert 'בעוד 4 ימים' in html
    assert 'billing-warning-banner' in html


def test_exempt_manager_sees_no_banner(client, monkeypatch):
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-15')
    _login(client, 'off@test.com')
    resp = client.get('/')
    assert resp.status_code == 200
    html = resp.data.decode('utf-8')
    assert 'המנוי טרם שולם החודש' not in html
    assert 'billing-warning-banner' not in html


def test_admin_toggle_sets_activated_at(client, monkeypatch):
    monkeypatch.setenv('BILLING_FAKE_TODAY', '2026-07-20')
    _login(client, 'admin@test.com')
    resp = client.post(f'/api/admin/billing/{U_OFF}', json={'active': True})
    assert resp.status_code == 200
    conn = _db()
    row = conn.execute('SELECT activated_at FROM manager_billing WHERE user_id=?',
                       (U_OFF,)).fetchone()
    conn.close()
    assert row['activated_at'] == '2026-07-20'

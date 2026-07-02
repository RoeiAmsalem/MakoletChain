"""/account — manager-facing billing status page (SUMIT stage 1).

Locks:
- auth: anonymous request redirects to /login
- status rendering for active+paid / active+unpaid / inactive / no-row / admin
- the SUMIT pay link carries the LOGGED-IN user's tag and no other user's
- exact contact hrefs (tel / wa.me / mailto)
- missing SUMIT_PAYMENT_URL hides the pay button and shows the placeholder
"""
import os
import re
import sqlite3
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app  # noqa: E402
import app as app_module  # noqa: E402

REPO_ROOT = os.path.join(os.path.dirname(__file__), '..')
TEST_DB = os.path.join(os.path.dirname(__file__), 'test_account_page.db')

BRANCH = 126
CUR_MONTH = datetime.now(ZoneInfo('Asia/Jerusalem')).strftime('%Y-%m')
PAID_DATE = f'{CUR_MONTH}-05'

# user_id → (email, billing row) — ids are distinct so the tag-leak assertion
# below can prove the rendered link holds exactly one id.
U_PAID, U_UNPAID, U_INACTIVE, U_NOROW, U_ADMIN = 21, 22, 23, 24, 25


@pytest.fixture
def client():
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
        (U_PAID, 'paid@test.com', 'manager'),
        (U_UNPAID, 'unpaid@test.com', 'manager'),
        (U_INACTIVE, 'inactive@test.com', 'manager'),
        (U_NOROW, 'norow@test.com', 'manager'),
        (U_ADMIN, 'admin@test.com', 'admin'),
    ]:
        conn.execute(
            "INSERT INTO users (id, name, email, password_hash, role, active) "
            "VALUES (?, ?, ?, ?, ?, 1)", (uid, f'user{uid}', email, pw, role))
        if role == 'manager':
            conn.execute(
                'INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)',
                (uid, BRANCH))
    conn.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, last_paid_date, last_status) "
        "VALUES (?, ?, 179, 1, ?, 'paid')", (U_PAID, str(U_PAID), PAID_DATE))
    conn.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, last_status) "
        "VALUES (?, ?, 179, 1, 'unpaid')", (U_UNPAID, str(U_UNPAID)))
    conn.execute(
        "INSERT INTO manager_billing (user_id, sumit_tag, fee, active) "
        "VALUES (?, ?, 179, 0)", (U_INACTIVE, str(U_INACTIVE)))
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def _login(client, email):
    resp = client.post('/login', data={'email': email, 'password': 'test123'})
    assert resp.status_code == 302, f'login failed for {email}'


def _get_account(client, email, monkeypatch=None, url_set=True):
    if monkeypatch is not None:
        monkeypatch.setattr(app_module, 'SUMIT_PAYMENT_URL_SET', url_set)
        monkeypatch.setattr(app_module, 'SUMIT_PAYMENT_URL',
                            'https://pay.sumit.example/prod179/')
    _login(client, email)
    resp = client.get('/account')
    assert resp.status_code == 200
    return resp.data.decode('utf-8')


def test_anonymous_redirects_to_login(client):
    resp = client.get('/account')
    assert resp.status_code == 302
    assert '/login' in resp.headers['Location']


def test_active_paid_shows_active_status(client, monkeypatch):
    html = _get_account(client, 'paid@test.com', monkeypatch)
    assert 'המנוי פעיל' in html
    assert PAID_DATE in html
    assert 'ממתין לתשלום החודש' not in html
    assert 'המנוי אינו פעיל' not in html
    assert '₪179' in html


def test_active_unpaid_shows_waiting(client, monkeypatch):
    html = _get_account(client, 'unpaid@test.com', monkeypatch)
    assert 'ממתין לתשלום החודש' in html
    assert 'המנוי פעיל' not in html


def test_inactive_shows_not_active_and_no_pay_link(client, monkeypatch):
    html = _get_account(client, 'inactive@test.com', monkeypatch)
    assert 'המנוי אינו פעיל' in html
    assert 'customerexternalidentifier' not in html
    assert 'לתשלום / עדכון אמצעי תשלום' not in html


def test_no_billing_row_renders_inactive(client, monkeypatch):
    html = _get_account(client, 'norow@test.com', monkeypatch)
    assert 'המנוי אינו פעיל' in html
    assert 'customerexternalidentifier' not in html


def test_admin_without_row_renders_neutral(client, monkeypatch):
    html = _get_account(client, 'admin@test.com', monkeypatch)
    assert 'אין מנוי לחשבון אדמין' in html
    assert 'customerexternalidentifier' not in html


def test_pay_link_carries_only_session_users_tag(client, monkeypatch):
    html = _get_account(client, 'paid@test.com', monkeypatch)
    assert f'?customerexternalidentifier={U_PAID}' in html
    tags = re.findall(r'customerexternalidentifier=(\d+)', html)
    assert tags == [str(U_PAID)], f'expected only tag {U_PAID}, got {tags}'
    assert 'target="_blank"' in html


def test_missing_payment_url_hides_button(client, monkeypatch):
    html = _get_account(client, 'paid@test.com', monkeypatch, url_set=False)
    assert 'קישור תשלום יוגדר בקרוב' in html
    assert 'customerexternalidentifier' not in html
    assert 'לתשלום / עדכון אמצעי תשלום' not in html


def test_contact_hrefs_exact(client, monkeypatch):
    html = _get_account(client, 'norow@test.com', monkeypatch)
    assert 'href="tel:0523455860"' in html
    assert 'href="https://wa.me/972523455860"' in html
    assert 'href="mailto:KupaShkufa@gmail.com"' in html


def test_payment_return_banner_with_doc_number(client, monkeypatch):
    _login(client, 'unpaid@test.com')
    html = client.get(
        '/account?OG-PaymentID=abc123&OG-PaymentType=CreditCard'
        '&OG-DocumentNumber=40002').data.decode('utf-8')
    assert 'התשלום התקבל' in html
    assert "קבלה מס' 40002" in html


def test_payment_return_banner_without_doc_number(client, monkeypatch):
    _login(client, 'unpaid@test.com')
    html = client.get('/account?OG-PaymentID=abc123').data.decode('utf-8')
    assert 'התשלום התקבל' in html
    assert "קבלה מס'" not in html
    assert 'קבלה נשלחה למייל' in html


def test_no_return_params_no_banner(client, monkeypatch):
    html = _get_account(client, 'unpaid@test.com', monkeypatch)
    assert 'התשלום התקבל' not in html


def test_return_params_are_escaped(client, monkeypatch):
    _login(client, 'unpaid@test.com')
    html = client.get('/account', query_string={
        'OG-PaymentID': 'x', 'OG-DocumentNumber': '<script>alert(1)</script>',
    }).data.decode('utf-8')
    assert '<script>alert(1)' not in html
    assert '&lt;script&gt;' in html


def test_return_params_never_flip_state(client, monkeypatch):
    # inactive-in-DB manager returning with params: banner shows (sync lag UX)
    # but the subscription state stays exactly as the DB says.
    _login(client, 'inactive@test.com')
    html = client.get(
        '/account?OG-PaymentID=abc&OG-DocumentNumber=40002').data.decode('utf-8')
    assert 'התשלום התקבל' in html
    assert 'המנוי אינו פעיל' in html
    assert 'המנוי פעיל ✓' not in html


def test_nav_shows_account_link_for_manager_and_admin(client, monkeypatch):
    for email in ('norow@test.com', 'admin@test.com'):
        html = _get_account(client, email, monkeypatch)
        assert 'החשבון שלי' in html
        assert 'href="/account"' in html
        client.get('/logout')

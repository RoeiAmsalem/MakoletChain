"""Payment-reminder email job (scripts/billing_reminder.py).

Policy under test: daily job emails ACTIVE-billed managers whose paywall state
is 'warning' AND days_left <= 2 (the 2-days-before-lock morning; '<=' catches
a manager who crossed the threshold while the job was down) —
locked/exempt/paid/ok and early-warning managers get nothing. ONE email per
manager per month via manager_billing.reminder_sent_month, set only on SMTP
success (a failed send retries next morning + fires one 🟠 brrr). Dry-run
(missing creds or BILLING_REMINDER_DRY_RUN != 'false') logs would-sends,
touches no SMTP and no flag. The job makes ZERO SUMIT calls.

Dates are simulated via BILLING_FAKE_TODAY (read per call by _billing_today):
START=2026-07-05, GRACE=5, fake today=2026-07-12 → days_left = 6 - days_unpaid:
  activated 2026-07-09  → days_unpaid=4 → warning, days_left=2 → selected
  activated 2026-07-08  → days_unpaid=5 → warning, days_left=1 → selected (missed)
  activated 2026-07-11  → days_unpaid=2 → warning, days_left=4 → NOT selected
  no activated_at        → days_unpaid=8 → locked → never
"""
import os
import smtplib
import sqlite3
import sys

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from app import app  # noqa: E402
import app as app_module  # noqa: E402
import billing_reminder  # noqa: E402
import utils.notify  # noqa: E402
import utils.sumit  # noqa: E402

REPO_ROOT = os.path.join(os.path.dirname(__file__), '..')
TEST_DB = os.path.join(os.path.dirname(__file__), 'test_billing_reminder.db')

BRANCH = 126
U_WARN, U_LOCKED, U_PAID, U_OFF, U_DEMO, U_INACT = 41, 42, 43, 44, 45, 46
U_MISSED, U_EARLY = 47, 48
START = '2026-07-05'
GRACE = 5
FAKE_TODAY = '2026-07-12'
MONTH = '2026-07'
SYNCED = '2026-07-12 06:00'


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setattr(app_module, 'BILLING_START_DATE', START)
    monkeypatch.setattr(app_module, 'BILLING_GRACE_DAYS', GRACE)
    monkeypatch.setenv('BILLING_FAKE_TODAY', FAKE_TODAY)

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
    conn.row_factory = sqlite3.Row
    conn.execute('DELETE FROM branches')
    conn.execute('DELETE FROM users')
    conn.execute('DELETE FROM user_branches')
    conn.execute('DELETE FROM manager_billing')
    conn.execute(
        "INSERT INTO branches (id, name, city, active) "
        "VALUES (?, 'המכולת אינשטיין', 'חיפה', 1)", (BRANCH,))
    pw = generate_password_hash('test123')
    for uid, name, email, active in [
        (U_WARN, 'מנהל אזהרה', 'warn@test.com', 1),
        (U_LOCKED, 'מנהל נעול', 'locked@test.com', 1),
        (U_PAID, 'מנהל שילם', 'paid@test.com', 1),
        (U_OFF, 'מנהל כבוי', 'off@test.com', 1),
        (U_DEMO, 'דמו', app_module.DEMO_ACCOUNT_EMAIL, 1),
        (U_INACT, 'מנהל לא פעיל', 'inactive@test.com', 0),
        (U_MISSED, 'מנהל פוספס', 'missed@test.com', 1),
        (U_EARLY, 'מנהל מוקדם', 'early@test.com', 1),
    ]:
        conn.execute(
            "INSERT INTO users (id, name, email, password_hash, role, active) "
            "VALUES (?, ?, ?, ?, 'manager', ?)", (uid, name, email, pw, active))
        conn.execute(
            'INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)',
            (uid, BRANCH))

    def mb(uid, active=1, activated=None, paid=None):
        conn.execute(
            "INSERT INTO manager_billing (user_id, sumit_tag, fee, active, "
            "last_status, last_paid_date, activated_at, updated_at) "
            "VALUES (?, ?, 179, ?, ?, ?, ?, ?)",
            (uid, str(uid), active, 'paid' if paid else 'unpaid', paid,
             activated, SYNCED))

    mb(U_WARN, activated='2026-07-09')      # warning, days_left=2 → selected
    mb(U_LOCKED)                            # locked (day 8 > grace) → never
    mb(U_PAID, paid='2026-07-06')           # ok
    mb(U_OFF, active=0)                     # billing off → exempt
    mb(U_DEMO, activated='2026-07-09')      # demo email → exempt
    mb(U_INACT, activated='2026-07-09')     # user inactive → excluded by join
    mb(U_MISSED, activated='2026-07-08')    # warning, days_left=1 → selected
    mb(U_EARLY, activated='2026-07-11')     # warning, days_left=4 → too early
    conn.commit()

    yield conn

    conn.close()
    app_module.DB_PATH = original_db
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


@pytest.fixture
def live_mode(monkeypatch):
    """Creds present + dry-run off → real-send code path (SMTP mocked)."""
    monkeypatch.setenv('BILLING_GMAIL_USER', 'kupashkufaa@gmail.com')
    monkeypatch.setenv('BILLING_GMAIL_APP_PASSWORD', 'test-app-password')
    monkeypatch.setenv('BILLING_REMINDER_DRY_RUN', 'false')


def _flag(db, uid):
    return db.execute(
        'SELECT reminder_sent_month FROM manager_billing WHERE user_id=?',
        (uid,)).fetchone()[0]


def test_only_final_stretch_warning_selected(db, live_mode, monkeypatch):
    sends = []
    monkeypatch.setattr(billing_reminder, '_send_email',
                        lambda to, name, **kw: sends.append((to, name)))
    res = billing_reminder.run_pass(db)
    assert res['dry_run'] is False
    # days_left=2 (today's threshold) AND days_left=1 (missed yesterday) send;
    # days_left=4 waits, locked/exempt/paid/off/inactive never.
    assert sends == [('warn@test.com', 'מנהל אזהרה'),
                     ('missed@test.com', 'מנהל פוספס')]
    assert [u for u, _, _ in res['sent']] == [U_WARN, U_MISSED]
    assert res['failed'] == [] and res['would_send'] == []
    assert _flag(db, U_WARN) == MONTH
    assert _flag(db, U_MISSED) == MONTH
    for uid in (U_EARLY, U_LOCKED, U_PAID, U_OFF, U_DEMO, U_INACT):
        assert _flag(db, uid) is None


def test_once_per_month_dedup(db, live_mode, monkeypatch):
    sends = []
    monkeypatch.setattr(billing_reminder, '_send_email',
                        lambda to, name, **kw: sends.append(to))
    billing_reminder.run_pass(db)
    res2 = billing_reminder.run_pass(db)
    assert sends == ['warn@test.com', 'missed@test.com']  # once each, not twice
    assert res2['sent'] == []
    assert res2['skipped_already_sent'] == 2


def test_smtp_failure_no_flag_one_brrr(db, live_mode, monkeypatch):
    def boom(to, name, **kw):
        raise smtplib.SMTPAuthenticationError(535, b'bad credentials')
    monkeypatch.setattr(billing_reminder, '_send_email', boom)
    alerts = []
    monkeypatch.setattr(utils.notify, 'notify',
                        lambda *a, **kw: alerts.append((a, kw)))
    res = billing_reminder.run_pass(db)
    assert [u for u, _, _ in res['failed']] == [U_WARN, U_MISSED]
    assert res['sent'] == []
    assert _flag(db, U_WARN) is None           # retries tomorrow
    assert _flag(db, U_MISSED) is None
    assert len(alerts) == 1                    # ONE brrr for the whole run
    assert alerts[0][1].get('medium') is True
    # next-morning retry actually reselects the managers
    sends = []
    monkeypatch.setattr(billing_reminder, '_send_email',
                        lambda to, name, **kw: sends.append(to))
    billing_reminder.run_pass(db)
    assert sends == ['warn@test.com', 'missed@test.com']
    assert _flag(db, U_WARN) == MONTH


def test_dry_run_no_smtp_no_flag(db, monkeypatch):
    monkeypatch.delenv('BILLING_GMAIL_USER', raising=False)
    monkeypatch.delenv('BILLING_GMAIL_APP_PASSWORD', raising=False)

    def no_smtp(*a, **kw):
        raise AssertionError('SMTP must not be touched in dry-run')
    monkeypatch.setattr(smtplib, 'SMTP', no_smtp)
    alerts = []
    monkeypatch.setattr(utils.notify, 'notify',
                        lambda *a, **kw: alerts.append(a))
    res = billing_reminder.run_pass(db)
    assert res['dry_run'] is True
    assert [u for u, _, _ in res['would_send']] == [U_WARN, U_MISSED]
    assert res['sent'] == [] and res['failed'] == []
    assert _flag(db, U_WARN) is None
    assert alerts == []


def test_dry_run_forced_even_with_creds(db, monkeypatch):
    # creds present but DRY_RUN not explicitly 'false' → still dry
    monkeypatch.setenv('BILLING_GMAIL_USER', 'kupashkufaa@gmail.com')
    monkeypatch.setenv('BILLING_GMAIL_APP_PASSWORD', 'x')
    monkeypatch.delenv('BILLING_REMINDER_DRY_RUN', raising=False)
    res = billing_reminder.run_pass(db)
    assert res['dry_run'] is True


def test_zero_sumit_calls(db, live_mode, monkeypatch):
    monkeypatch.setattr(billing_reminder, '_send_email', lambda to, name, **kw: None)
    for fn in ('list_payments', 'list_documents', 'get_document',
               'list_customers', 'ping', '_post'):
        monkeypatch.setattr(
            utils.sumit, fn,
            lambda *a, _fn=fn, **kw: (_ for _ in ()).throw(
                AssertionError(f'SUMIT call {_fn} from reminder job')))
    utils.sumit.reset_call_count()
    billing_reminder.run_pass(db)
    assert utils.sumit.call_count() == 0


def test_html_body_rtl():
    h = billing_reminder._body_html('מנהל בדיקה')
    assert h.startswith('<div dir="rtl"')
    assert 'text-align:right' in h
    assert 'שלום מנהל בדיקה,' in h
    assert (f'<a href="{billing_reminder.ACCOUNT_URL}">'
            f'{billing_reminder.ACCOUNT_URL}</a>') in h
    assert '<img' not in h


def test_email_is_multipart_alternative(monkeypatch):
    monkeypatch.setenv('BILLING_GMAIL_USER', 'kupashkufaa@gmail.com')
    monkeypatch.setenv('BILLING_GMAIL_APP_PASSWORD', 'x')
    captured = {}

    class FakeSMTP:
        def __init__(self, *a, **kw):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *a):
            pass
        def starttls(self):
            pass
        def login(self, *a):
            pass
        def send_message(self, msg):
            captured['msg'] = msg

    monkeypatch.setattr(smtplib, 'SMTP', FakeSMTP)
    billing_reminder._send_email('to@test.com', 'מנהל')
    msg = captured['msg']
    assert msg.get_content_type() == 'multipart/alternative'
    parts = {p.get_content_type(): p.get_content() for p in msg.iter_parts()}
    assert 'שלום מנהל,' in parts['text/plain']
    assert '<div dir="rtl"' in parts['text/html']


def test_kill_switch_and_hour_gate(db, monkeypatch):
    monkeypatch.setenv('BILLING_REMINDER_ENABLED', 'false')
    assert billing_reminder.run_reminder() == 'disabled'
    monkeypatch.setenv('BILLING_REMINDER_ENABLED', 'true')
    wrong_hour = (app_module._now_il().hour + 1) % 24
    monkeypatch.setenv('BILLING_REMINDER_HOUR', str(wrong_hour))
    assert billing_reminder.run_reminder() == 'outside-window'


# ── Lock-notification email (run_lock_pass, fired by the 09:10 sweep) ──────

def _lock_flag(db, uid):
    return db.execute(
        'SELECT locked_email_sent_month FROM manager_billing WHERE user_id=?',
        (uid,)).fetchone()[0]


def test_lock_email_only_locked_selected(db, live_mode, monkeypatch):
    sends = []
    monkeypatch.setattr(billing_reminder, '_send_email',
                        lambda to, name, **kw: sends.append((to, kw['subject'])))
    res = billing_reminder.run_lock_pass(db)
    # locked selected with the lock subject; warning/paid/exempt/off/inactive never
    assert sends == [('locked@test.com', billing_reminder.LOCKED_SUBJECT)]
    assert [u for u, _, _ in res['sent']] == [U_LOCKED]
    assert _lock_flag(db, U_LOCKED) == MONTH
    for uid in (U_WARN, U_MISSED, U_EARLY, U_PAID, U_OFF, U_DEMO, U_INACT):
        assert _lock_flag(db, uid) is None
    # the lock pass never touches the reminder flag and vice versa
    assert _flag(db, U_LOCKED) is None


def test_lock_email_once_then_relock_next_month(db, live_mode, monkeypatch):
    sends = []
    monkeypatch.setattr(billing_reminder, '_send_email',
                        lambda to, name, **kw: sends.append(to))
    billing_reminder.run_lock_pass(db)
    res2 = billing_reminder.run_lock_pass(db)
    assert sends == ['locked@test.com']        # staying locked ≠ more mail
    assert res2['sent'] == [] and res2['skipped_already_sent'] == 1
    # paid in July, re-locked in August: the July flag no longer matches →
    # exactly one more mail (simulated by backdating the flag a month)
    db.execute("UPDATE manager_billing SET locked_email_sent_month='2026-06' "
               "WHERE user_id=?", (U_LOCKED,))
    db.commit()
    res3 = billing_reminder.run_lock_pass(db)
    assert [u for u, _, _ in res3['sent']] == [U_LOCKED]
    assert _lock_flag(db, U_LOCKED) == MONTH


def test_lock_email_smtp_fail_no_flag_one_brrr(db, live_mode, monkeypatch):
    def boom(to, name, **kw):
        raise smtplib.SMTPAuthenticationError(535, b'bad credentials')
    monkeypatch.setattr(billing_reminder, '_send_email', boom)
    alerts = []
    monkeypatch.setattr(utils.notify, 'notify',
                        lambda *a, **kw: alerts.append((a, kw)))
    res = billing_reminder.run_lock_pass(db)
    assert [u for u, _, _ in res['failed']] == [U_LOCKED]
    assert _lock_flag(db, U_LOCKED) is None    # retries next sweep
    assert len(alerts) == 1
    assert alerts[0][1].get('medium') is True
    sends = []
    monkeypatch.setattr(billing_reminder, '_send_email',
                        lambda to, name, **kw: sends.append(to))
    billing_reminder.run_lock_pass(db)
    assert sends == ['locked@test.com']


def test_lock_email_dry_run(db, monkeypatch):
    monkeypatch.delenv('BILLING_GMAIL_USER', raising=False)
    monkeypatch.delenv('BILLING_GMAIL_APP_PASSWORD', raising=False)

    def no_smtp(*a, **kw):
        raise AssertionError('SMTP must not be touched in dry-run')
    monkeypatch.setattr(smtplib, 'SMTP', no_smtp)
    res = billing_reminder.run_lock_pass(db)
    assert res['dry_run'] is True
    assert [u for u, _, _ in res['would_send']] == [U_LOCKED]
    assert res['sent'] == [] and res['failed'] == []
    assert _lock_flag(db, U_LOCKED) is None


def test_lock_email_html_body():
    h = billing_reminder._body_html('מנהל', billing_reminder.LOCKED_BODY)
    assert h.startswith('<div dir="rtl"')
    assert 'הגישה למערכת קופה שקופה הושהתה זמנית' in h
    assert (f'<a href="{billing_reminder.ACCOUNT_URL}">'
            f'{billing_reminder.ACCOUNT_URL}</a>') in h
    assert '<img' not in h

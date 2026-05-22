"""branch_id resolution + live tile store-closed read-time rule.

get_branch_id() must honor ?branch_id= URL param only if the user is
entitled to that branch, must not mutate session, and must fall back to
existing precedence otherwise.

/api/live-sales and /api/summary must show live data only for the current
calendar day (Asia/Jerusalem). When the calendar date has rolled over and
no fresh pull exists yet for the new day, return is_closed with
last_amount/last_date — never resurface the past day's number as live.
Z-report (daily_sales row for today) always wins.
"""
import json
import os
import sys
import sqlite3
from datetime import timedelta

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, _now_il


@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_branch_id_resolution.db')

    import app as app_module
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
    conn.execute("INSERT INTO branches (id, name, city, active) "
                 "VALUES (126, 'אלפא', 'עיר', 1)")
    conn.execute("INSERT INTO branches (id, name, city, active) "
                 "VALUES (127, 'בית', 'עיר', 1)")
    pw = generate_password_hash('test123')
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, "
                 "active) VALUES (1, 'Admin', 'admin@t.com', ?, 'admin', 1)",
                 (pw,))
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, "
                 "active) VALUES (2, 'Mgr', 'mgr@t.com', ?, 'manager', 1)",
                 (pw,))
    conn.execute("INSERT INTO user_branches (user_id, branch_id) "
                 "VALUES (2, 126)")
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(test_db):
        os.remove(test_db)


def _login_admin(client):
    return client.post('/login', data={'email': 'admin@t.com',
                                        'password': 'test123'})


def _login_manager(client):
    return client.post('/login', data={'email': 'mgr@t.com',
                                        'password': 'test123'})


def _db():
    import app as app_module
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    return conn


def _today():
    return _now_il().date()


# ── Fix 1: get_branch_id() honors ?branch_id= with access control ──

def test_get_branch_id_honors_url_param_for_admin(client):
    _login_admin(client)
    # Seed distinct data so we can tell the branches apart.
    today = _today().isoformat()
    conn = _db()
    conn.execute(
        "INSERT INTO daily_sales (branch_id, date, amount, transactions, "
        "source) VALUES (126, ?, 1111, 5, 'z_report')", (today,))
    conn.execute(
        "INSERT INTO daily_sales (branch_id, date, amount, transactions, "
        "source) VALUES (127, ?, 2222, 9, 'z_report')", (today,))
    conn.commit()
    conn.close()

    d126 = json.loads(client.get('/api/summary?branch_id=126').data)
    d127 = json.loads(client.get('/api/summary?branch_id=127').data)
    assert d126['branch_id'] == 126
    assert d127['branch_id'] == 127
    assert d126['income'] == 1111
    assert d127['income'] == 2222


def test_get_branch_id_ignores_url_param_for_unauthorized_manager(client):
    _login_manager(client)
    today = _today().isoformat()
    conn = _db()
    conn.execute(
        "INSERT INTO daily_sales (branch_id, date, amount, transactions, "
        "source) VALUES (126, ?, 1111, 5, 'z_report')", (today,))
    conn.execute(
        "INSERT INTO daily_sales (branch_id, date, amount, transactions, "
        "source) VALUES (127, ?, 2222, 9, 'z_report')", (today,))
    conn.commit()
    conn.close()

    # Manager owns only branch 126. Asking for 127 must fall through, not honor it.
    d = json.loads(client.get('/api/summary?branch_id=127').data)
    assert d['branch_id'] == 126
    assert d['income'] == 1111


def test_get_branch_id_no_session_mutation(client):
    _login_admin(client)
    today = _today().isoformat()
    conn = _db()
    conn.execute(
        "INSERT INTO daily_sales (branch_id, date, amount, transactions, "
        "source) VALUES (126, ?, 1111, 5, 'z_report')", (today,))
    conn.execute(
        "INSERT INTO daily_sales (branch_id, date, amount, transactions, "
        "source) VALUES (127, ?, 2222, 9, 'z_report')", (today,))
    conn.commit()
    conn.close()

    # Hit 127 via URL param.
    d127 = json.loads(client.get('/api/summary?branch_id=127').data)
    assert d127['branch_id'] == 127
    # Then a plain call (no param) must NOT see 127 in session — admin
    # falls back to "first branch by id" (126), proving no side-effect write.
    d_plain = json.loads(client.get('/api/summary').data)
    assert d_plain['branch_id'] == 126


# ── Fix 2: store-closed read-time rule ────────────────────────

def test_live_shows_today_when_fresh(client):
    """live_sales row for today → normal live number, no is_closed."""
    _login_manager(client)
    today = _today().isoformat()
    conn = _db()
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, "
        "last_updated, fetched_at) VALUES (126, ?, 8888, 250, '14:00:00', ?)",
        (today, today + 'T14:00:00'))
    conn.commit()
    conn.close()

    d = json.loads(client.get('/api/live-sales').data)
    assert d['is_closed'] is False
    assert d['amount'] == 8888
    assert d['transactions'] == 250


def test_live_keeps_number_same_day_evening(client):
    """Late-night row dated today → still shows it, not is_closed."""
    _login_manager(client)
    today = _today().isoformat()
    conn = _db()
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, "
        "last_updated, fetched_at) VALUES (126, ?, 12345, 380, '23:00:00', ?)",
        (today, today + 'T23:00:00'))
    conn.commit()
    conn.close()

    d = json.loads(client.get('/api/live-sales').data)
    assert d['is_closed'] is False
    assert d['amount'] == 12345


def test_live_closed_when_latest_is_past_day(client):
    """Most recent live row is yesterday → is_closed=true, NOT yesterday's
    number rendered as live. live_amount_today is 0 in /api/summary."""
    _login_manager(client)
    yesterday = (_today() - timedelta(days=1)).isoformat()
    conn = _db()
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, "
        "last_updated, fetched_at) VALUES (126, ?, 13721.98, 400, '23:00:00', ?)",
        (yesterday, yesterday + 'T23:00:00'))
    conn.commit()
    conn.close()

    d = json.loads(client.get('/api/live-sales').data)
    assert d['is_closed'] is True
    assert d['amount'] is None
    assert d['last_amount'] == 13721.98
    assert d['last_date'] == yesterday

    s = json.loads(client.get('/api/summary').data)
    assert s['live'] is not None
    assert s['live']['is_closed'] is True
    assert s['live']['amount'] is None
    assert s['live']['last_amount'] == 13721.98
    assert s['live']['last_date'] == yesterday
    assert s['live_amount_today'] == 0


def test_live_z_report_overrides(client):
    """Today has a Z (daily_sales row) → Z wins, no is_closed."""
    _login_manager(client)
    today = _today().isoformat()
    yesterday = (_today() - timedelta(days=1)).isoformat()
    conn = _db()
    # Yesterday has a stale live row (would otherwise trigger is_closed).
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, "
        "last_updated, fetched_at) VALUES (126, ?, 9999, 300, '23:00:00', ?)",
        (yesterday, yesterday + 'T23:00:00'))
    # Today has a Z.
    conn.execute(
        "INSERT INTO daily_sales (branch_id, date, amount, transactions, "
        "source) VALUES (126, ?, 4444, 150, 'z_report')", (today,))
    conn.commit()
    conn.close()

    d = json.loads(client.get('/api/live-sales').data)
    assert d['is_closed'] is False

    s = json.loads(client.get('/api/summary').data)
    # has_z true and no live row for today → live tile empty, no is_closed.
    if s['live'] is not None:
        assert s['live']['is_closed'] is False


def test_live_first_pull_clears_closed(client):
    """After is_closed state, inserting a fresh today row → is_closed gone,
    shows the fresh number."""
    _login_manager(client)
    yesterday = (_today() - timedelta(days=1)).isoformat()
    today = _today().isoformat()
    conn = _db()
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, "
        "last_updated, fetched_at) VALUES (126, ?, 13721.98, 400, '23:00:00', ?)",
        (yesterday, yesterday + 'T23:00:00'))
    conn.commit()

    d1 = json.loads(client.get('/api/live-sales').data)
    assert d1['is_closed'] is True

    # First fresh pull of the new day.
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, "
        "last_updated, fetched_at) VALUES (126, ?, 250, 7, '07:00:00', ?)",
        (today, today + 'T07:00:00'))
    conn.commit()
    conn.close()

    d2 = json.loads(client.get('/api/live-sales').data)
    assert d2['is_closed'] is False
    assert d2['amount'] == 250

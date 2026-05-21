"""branch_id resolution + stale-fallback closed-day filter.

get_branch_id() must honor ?branch_id= URL param only if the user is
entitled to that branch, must not mutate session, and must fall back to
existing precedence otherwise.

Stale fallback in /api/live-sales and /api/summary must skip dates that
already have a daily_sales row — once a day is closed, its day-end live
cumulative must not resurface on the next morning.
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


# ── Fix 2: stale fallback skips closed days ───────────────────

def test_stale_skips_closed_day(client):
    """live_sales row for May-20-equivalent + daily_sales row for same day,
    nothing for today → /api/live-sales returns empty (not the closed day)."""
    _login_manager(client)
    closed_day = (_today() - timedelta(days=1)).isoformat()
    conn = _db()
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, "
        "last_updated, fetched_at) VALUES (126, ?, 13721.98, 400, '23:00:00', ?)",
        (closed_day, closed_day + 'T23:00:00'))
    conn.execute(
        "INSERT INTO daily_sales (branch_id, date, amount, transactions, "
        "source) VALUES (126, ?, 13721.98, 400, 'z_report')", (closed_day,))
    conn.commit()
    conn.close()

    d = json.loads(client.get('/api/live-sales').data)
    assert d['is_stale'] is False
    assert d['amount'] is None


def test_stale_returns_open_day_only(client):
    """live_sales row from a date with NO daily_sales row → stale fallback returns it."""
    _login_manager(client)
    open_day = (_today() - timedelta(days=2)).isoformat()
    conn = _db()
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, "
        "last_updated, fetched_at) VALUES (126, ?, 7777, 200, '23:00:00', ?)",
        (open_day, open_day + 'T23:00:00'))
    conn.commit()
    conn.close()

    d = json.loads(client.get('/api/live-sales').data)
    assert d['is_stale'] is True
    assert d['amount'] == 7777
    assert d['stale_date'] == open_day


def test_stale_same_fix_in_summary(client):
    """Same closed-day filter applies to /api/summary.live stale fallback."""
    _login_manager(client)
    closed_day = (_today() - timedelta(days=1)).isoformat()
    open_day = (_today() - timedelta(days=3)).isoformat()
    conn = _db()
    # Closed: has both live AND a daily_sales row → must be skipped.
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, "
        "last_updated, fetched_at) VALUES (126, ?, 13721.98, 400, '23:00:00', ?)",
        (closed_day, closed_day + 'T23:00:00'))
    conn.execute(
        "INSERT INTO daily_sales (branch_id, date, amount, transactions, "
        "source) VALUES (126, ?, 13721.98, 400, 'z_report')", (closed_day,))
    # Older open day with only a live row → that one should surface.
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, "
        "last_updated, fetched_at) VALUES (126, ?, 5555, 100, '23:00:00', ?)",
        (open_day, open_day + 'T23:00:00'))
    conn.commit()
    conn.close()

    d = json.loads(client.get('/api/summary').data)
    assert d['live'] is not None
    assert d['live']['is_stale'] is True
    assert d['live']['amount'] == 5555
    assert d['live']['stale_date'] == open_day
    # Sanity: closed day must not be the one resurfaced.
    assert d['live']['amount'] != 13721.98

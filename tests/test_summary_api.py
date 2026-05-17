"""/api/summary live-tile stale policy.

POLICY (display-only separation):
  - live.{amount,transactions,last_updated,is_stale,stale_date} feeds the
    tile and MAY be a prior-day value (is_stale=true).
  - live_amount_today feeds income math and is ONLY ever the fresh
    today value, else 0 — a stale value is NEVER added to income (that
    would double-count a day whose Z-report already landed).
  - Z-report for today always wins: no stale fallback when has_z.
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
    test_db = os.path.join(os.path.dirname(__file__), 'test_summary_api.db')

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
                 "VALUES (1, 'אלפא', 'עיר', 1)")
    pw = generate_password_hash('test123')
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, "
                 "active) VALUES (2, 'Mgr', 'mgr@t.com', ?, 'manager', 1)",
                 (pw,))
    conn.execute("INSERT INTO user_branches (user_id, branch_id) "
                 "VALUES (2, 1)")
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(test_db):
        os.remove(test_db)


def _login(client):
    return client.post('/login', data={'email': 'mgr@t.com',
                                        'password': 'test123'})


def _db():
    import app as app_module
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    return conn


def _today():
    return _now_il().date()


def _summary(client):
    _login(client)
    return json.loads(client.get('/api/summary').data)


# ── stale fallback ────────────────────────────────────────────

def test_summary_live_stale_when_no_today_row(client):
    old = (_today() - timedelta(days=2)).isoformat()
    conn = _db()
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, "
        "last_updated, fetched_at) VALUES (1, ?, 2000, 80, '23:00:00', ?)",
        (old, old + 'T23:00:00'))
    conn.commit()
    conn.close()

    d = _summary(client)
    assert d['live'] is not None
    assert d['live']['is_stale'] is True
    assert d['live']['amount'] == 2000
    assert d['live']['stale_date'] == old
    # Display-only: stale value must NOT enter income math.
    assert d['live_amount_today'] == 0
    assert d['has_z'] is False


def test_summary_live_fresh_when_today_row_exists(client):
    today = _today().isoformat()
    conn = _db()
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, "
        "last_updated, fetched_at) VALUES (1, ?, 555, 12, '12:00:00', ?)",
        (today, today + 'T12:00:00'))
    conn.commit()
    conn.close()

    d = _summary(client)
    assert d['live'] is not None
    assert d['live']['is_stale'] is False
    assert d['live']['amount'] == 555
    assert d['live_amount_today'] == 555


def test_summary_live_zero_when_no_history(client):
    d = _summary(client)
    assert d['live'] is None
    assert d['live_amount_today'] == 0
    assert d['has_z'] is False


def test_summary_live_overridden_by_z(client):
    today = _today().isoformat()
    old = (_today() - timedelta(days=2)).isoformat()
    conn = _db()
    # A prior live row exists, but a Z-report today must suppress stale.
    conn.execute(
        "INSERT INTO live_sales (branch_id, date, amount, transactions, "
        "last_updated, fetched_at) VALUES (1, ?, 2000, 80, '23:00:00', ?)",
        (old, old + 'T23:00:00'))
    conn.execute(
        "INSERT INTO daily_sales (branch_id, date, amount, transactions, "
        "source) VALUES (1, ?, 999, 30, 'z_report')", (today,))
    conn.commit()
    conn.close()

    d = _summary(client)
    assert d['has_z'] is True
    assert d['live'] is None
    assert d['live_amount_today'] == 0

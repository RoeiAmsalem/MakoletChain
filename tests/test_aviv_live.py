"""aviv_live manual force-run.

The store-hours guard must stay in place for SCHEDULED runs (silent skip,
no DB write) but a manual /ops trigger passes force=True and bypasses it —
the admin clicked the button on purpose. force must NOT swallow Aviv API
errors (auth failures still surface), but that path needs no network here:
we prove the bypass by letting it fall through to the credential check.
"""
import os
import sys
import sqlite3

import pytest
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import agents.aviv_live as aviv_live
import agents.bilboy as bilboy
import utils.notify as notify_mod
from app import app


# ── run_aviv_live() guard ─────────────────────────────────────

def test_aviv_live_skip_when_outside_hours_default(monkeypatch):
    """Scheduled call (no force) outside store hours → silent skip, no work."""
    monkeypatch.setattr(aviv_live, '_is_store_hours', lambda: False)

    def _boom(*a, **k):
        raise AssertionError("must not touch DB / scrape when skipping")

    monkeypatch.setattr(aviv_live, '_get_branch_config', _boom)
    monkeypatch.setattr(aviv_live, '_get_db', _boom)

    result = aviv_live.run_aviv_live(1)
    assert result == {'success': True, 'amount': 0,
                      'transactions': 0, 'skipped': 'outside_hours'}


def test_aviv_live_force_runs_outside_hours(monkeypatch):
    """force=True outside store hours → bypasses guard, proceeds past it.

    We stub _get_branch_config to return no credentials so the run stops
    at the credential check — reaching it proves the store-hours guard was
    bypassed (it would otherwise have returned 'outside_hours' first).
    """
    monkeypatch.setattr(aviv_live, '_is_store_hours', lambda: False)
    monkeypatch.setattr(aviv_live, '_get_branch_config', lambda bid: {})

    result = aviv_live.run_aviv_live(1, force=True)
    assert result == {'success': True, 'skipped': 'no_credentials'}
    assert result.get('skipped') != 'outside_hours'


# ── /ops/run-agent passes force only for aviv_live ────────────

@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_aviv_live.db')

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
                 "active) VALUES (1, 'Admin', 'admin@t.com', ?, 'admin', 1)",
                 (pw,))
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


def test_run_agent_endpoint_passes_force_for_aviv_live(client, monkeypatch):
    monkeypatch.setattr(notify_mod, 'notify', lambda *a, **k: None)
    captured = {}

    def fake_aviv(branch_id, force=False):
        captured['force'] = force
        return {'success': True, 'amount': 0, 'transactions': 0}

    monkeypatch.setattr(aviv_live, 'run_aviv_live', fake_aviv)
    _login_admin(client)
    r = client.post('/ops/run-agent',
                     json={'branch_id': 1, 'agent': 'aviv_live'})
    assert r.status_code == 200
    assert captured.get('force') is True


def test_run_agent_endpoint_no_force_for_bilboy(client, monkeypatch):
    monkeypatch.setattr(notify_mod, 'notify', lambda *a, **k: None)
    captured = {}

    def fake_bilboy(branch_id, *args, **kwargs):
        captured['kwargs'] = kwargs
        return {'success': True, 'docs_count': 0, 'total_amount': 0}

    monkeypatch.setattr(bilboy, 'run_bilboy', fake_bilboy)
    _login_admin(client)
    r = client.post('/ops/run-agent',
                     json={'branch_id': 1, 'agent': 'bilboy'})
    assert r.status_code == 200
    assert 'force' not in captured.get('kwargs', {})

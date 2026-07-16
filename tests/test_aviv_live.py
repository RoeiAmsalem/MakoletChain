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


# ── chain-account path (run_aviv_live_chain) ──────────────────

@pytest.fixture(autouse=True)
def _fresh_chain_gate():
    """The outage gate is module-global (persists across scheduler ticks);
    reset it so tests don't leak streak state into each other."""
    aviv_live._chain_gate = aviv_live._ChainFailureGate()

def _chain_db():
    """In-memory DB with the columns the chain path touches."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE branches (
            id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 1,
            aviv_branch_id INTEGER,
            hours_this_month REAL, hours_updated_at TEXT
        );
        CREATE TABLE live_sales (
            branch_id INTEGER, date TEXT, amount REAL, transactions INTEGER,
            last_updated TEXT, fetched_at TEXT,
            cancellation_total REAL, discount_total REAL,
            running_total REAL, running_count INTEGER,
            PRIMARY KEY (branch_id, date)
        );
        CREATE TABLE hourly_sales (
            branch_id INTEGER, date TEXT, hour INTEGER, amount REAL,
            PRIMARY KEY (branch_id, date, hour)
        );
        CREATE TABLE agent_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER, agent TEXT, started_at TEXT, finished_at TEXT,
            status TEXT, amount REAL, message TEXT, duration_seconds REAL,
            dismissed INTEGER DEFAULT 0
        );
    ''')
    conn.execute("INSERT INTO branches (id, name, active, aviv_branch_id) "
                 "VALUES (126, 'Einstein', 1, 3)")
    conn.execute("INSERT INTO branches (id, name, active, aviv_branch_id) "
                 "VALUES (127, 'Tichon', 1, 8)")
    conn.commit()
    return conn


def _silence_notify(monkeypatch):
    monkeypatch.setattr(aviv_live, 'notify', lambda *a, **k: None)


def _live_status_row(branch_id, deal_total, deal_count=10,
                     monthly_hours=100.0, shift_hours=2.0):
    return {
        'branch': branch_id, 'apiKey': '25165',
        'dealTotal': deal_total, 'dealCount': deal_count,
        'runningDealTotal': deal_total + 1000, 'runningDealCount': deal_count + 50,
        'cancellationTotal': None, 'cancellationCount': 0,
        'discountTotal': None, 'discountCount': 0,
        'totalEmployeeHours': monthly_hours,
        'currentEmployeeHours': shift_hours,
        'currentEmployeeCount': 1, 'totalEmployeeCount': 30,
        'tmUpdate': '2026-05-25 12:00:00',
        'payments': [], 'z': 0, 'zCreate': None,
    }


def test_multibranch_maps_rows(monkeypatch):
    """Response rows are mapped to local branches via aviv_branch_id, not order."""
    conn = _chain_db()
    _silence_notify(monkeypatch)
    monkeypatch.setattr(aviv_live, '_is_store_hours', lambda: True)
    monkeypatch.setattr(aviv_live, '_login_chain_account', lambda: 'tok')

    # Deliberately swap order in the response.
    response = [_live_status_row(8, 500.0),  # branch 127
                _live_status_row(3, 1234.56)]  # branch 126
    monkeypatch.setattr(aviv_live, '_fetch_multi_status', lambda t, ids: response)

    out = aviv_live.run_aviv_live_chain(conn=conn)
    assert out['success'] is True
    assert out['ok'] == 2
    assert out['failed'] == 0

    # Verify mapping by amount → correct local branch.
    row_126 = conn.execute("SELECT amount FROM live_sales WHERE branch_id=126").fetchone()
    row_127 = conn.execute("SELECT amount FROM live_sales WHERE branch_id=127").fetchone()
    assert row_126['amount'] == 1234.56, 'branch 126 must get aviv_branch_id=3 row'
    assert row_127['amount'] == 500.0, 'branch 127 must get aviv_branch_id=8 row'


def test_missing_branch_in_response(monkeypatch):
    """Branch absent from response → that branch fails; others still process."""
    conn = _chain_db()
    _silence_notify(monkeypatch)
    monkeypatch.setattr(aviv_live, '_is_store_hours', lambda: True)
    monkeypatch.setattr(aviv_live, '_login_chain_account', lambda: 'tok')
    # Only return branch 8; branch 3 is missing.
    monkeypatch.setattr(aviv_live, '_fetch_multi_status',
                        lambda t, ids: [_live_status_row(8, 500.0)])

    out = aviv_live.run_aviv_live_chain(conn=conn)
    assert out['ok'] == 1
    assert out['failed'] == 1

    # 127 got its row; 126 did not.
    assert conn.execute("SELECT amount FROM live_sales WHERE branch_id=127"
                        ).fetchone()['amount'] == 500.0
    assert conn.execute("SELECT amount FROM live_sales WHERE branch_id=126"
                        ).fetchone() is None
    # 126 has an agent_runs error row.
    err = conn.execute("SELECT status, message FROM agent_runs WHERE branch_id=126"
                       ).fetchone()
    assert err['status'] == 'error'
    assert 'missing' in err['message']


def test_single_login_one_call(monkeypatch):
    """N branches must trigger exactly ONE login + ONE multi-branch POST."""
    conn = _chain_db()
    _silence_notify(monkeypatch)
    monkeypatch.setattr(aviv_live, '_is_store_hours', lambda: True)

    login_calls = {'n': 0}
    def fake_login():
        login_calls['n'] += 1
        return 'tok'
    monkeypatch.setattr(aviv_live, '_login_chain_account', fake_login)

    fetch_calls: list[list[int]] = []
    def fake_fetch(token, aviv_branch_ids):
        fetch_calls.append(list(aviv_branch_ids))
        return [_live_status_row(b, 100.0 * b) for b in aviv_branch_ids]
    monkeypatch.setattr(aviv_live, '_fetch_multi_status', fake_fetch)

    aviv_live.run_aviv_live_chain(conn=conn)

    assert login_calls['n'] == 1, f'expected 1 login, got {login_calls["n"]}'
    assert len(fetch_calls) == 1, f'expected 1 fetch, got {len(fetch_calls)}'
    assert sorted(fetch_calls[0]) == [3, 8]


def test_live_storage_unchanged(monkeypatch):
    """The fields written to live_sales match the legacy per-branch shape."""
    conn = _chain_db()
    _silence_notify(monkeypatch)
    monkeypatch.setattr(aviv_live, '_is_store_hours', lambda: True)
    monkeypatch.setattr(aviv_live, '_login_chain_account', lambda: 'tok')

    raw = {
        'branch': 3, 'dealTotal': 9999.99, 'dealCount': 42,
        'runningDealTotal': 12345.67, 'runningDealCount': 200,
        'cancellationTotal': 10.0, 'discountTotal': 5.0,
        'totalEmployeeHours': 250.5, 'currentEmployeeHours': 3.25,
        'tmUpdate': '2026-05-25 14:30:00',
    }
    monkeypatch.setattr(aviv_live, '_fetch_multi_status', lambda t, ids: [raw])

    aviv_live.run_aviv_live_chain(conn=conn)

    row = conn.execute(
        "SELECT amount, transactions, last_updated, "
        "cancellation_total, discount_total, running_total, running_count "
        "FROM live_sales WHERE branch_id=126"
    ).fetchone()
    assert row['amount'] == 9999.99
    assert row['transactions'] == 42
    # _fmt_last_updated converts "YYYY-MM-DD HH:MM:SS" → "HH:MM dd/mm/yy"
    assert row['last_updated'] == '14:30 25/05/26'
    assert row['cancellation_total'] == 10.0
    assert row['discount_total'] == 5.0
    assert row['running_total'] == 12345.67
    assert row['running_count'] == 200
    # branches.hours_this_month updated when monthly_hours > 0
    h = conn.execute("SELECT hours_this_month FROM branches WHERE id=126").fetchone()
    assert h['hours_this_month'] == 250.5


def test_total_rest_failure_does_not_fallback_to_playwright(monkeypatch):
    """If the chain REST call fails entirely, NO Playwright is launched."""
    conn = _chain_db()
    _silence_notify(monkeypatch)
    monkeypatch.setattr(aviv_live, '_is_store_hours', lambda: True)

    def boom_login():
        raise Exception('connection refused')
    monkeypatch.setattr(aviv_live, '_login_chain_account', boom_login)

    def boom_pw(*a, **k):
        raise AssertionError('chain path must NOT fall back to Playwright')
    monkeypatch.setattr(aviv_live, '_scrape_playwright', boom_pw)

    out = aviv_live.run_aviv_live_chain(conn=conn)
    assert out['success'] is False
    # No agent_runs were created for any branch.
    assert conn.execute("SELECT COUNT(*) FROM agent_runs").fetchone()[0] == 0
    # No live_sales rows either.
    assert conn.execute("SELECT COUNT(*) FROM live_sales").fetchone()[0] == 0


def test_chain_outside_store_hours_silent_skip(monkeypatch):
    """Chain path obeys the store-hours guard like the per-branch path."""
    monkeypatch.setattr(aviv_live, '_is_store_hours', lambda: False)
    def boom(*a, **k):
        raise AssertionError('must not work when outside hours')
    monkeypatch.setattr(aviv_live, '_login_chain_account', boom)
    out = aviv_live.run_aviv_live_chain()
    assert out == {'success': True, 'skipped': 'outside_hours'}


# ── chain outage gate (page after N consecutive ticks) ────────

def _capture_notify(monkeypatch):
    calls = []
    monkeypatch.setattr(aviv_live, 'notify',
                        lambda title, msg, **kw: calls.append((title, msg, kw)))
    return calls


def _chain_tick(conn, monkeypatch, *, fail):
    if fail:
        def boom():
            raise Exception('Read timed out. (read timeout=30)')
        monkeypatch.setattr(aviv_live, '_login_chain_account', boom)
    else:
        monkeypatch.setattr(aviv_live, '_login_chain_account', lambda: 'tok')
        monkeypatch.setattr(aviv_live, '_fetch_multi_status',
                            lambda t, ids: [_live_status_row(b, 100.0) for b in ids])
    return aviv_live.run_aviv_live_chain(conn=conn)


def test_chain_single_blip_does_not_page(monkeypatch):
    """One failed tick then a good one → no ❌ page, no ✅ recovery."""
    conn = _chain_db()
    monkeypatch.setattr(aviv_live, '_is_store_hours', lambda: True)
    calls = _capture_notify(monkeypatch)

    out = _chain_tick(conn, monkeypatch, fail=True)
    assert out['success'] is False and out['fail_streak'] == 1
    _chain_tick(conn, monkeypatch, fail=False)

    chain_alerts = [c for c in calls if 'Aviv Live (chain)' in c[0]]
    assert chain_alerts == [], f'blip must not page: {chain_alerts}'


def test_chain_pages_once_after_threshold(monkeypatch):
    """3 consecutive failed ticks → exactly one ❌ page; a 4th adds none."""
    conn = _chain_db()
    monkeypatch.setattr(aviv_live, '_is_store_hours', lambda: True)
    calls = _capture_notify(monkeypatch)

    for _ in range(2):
        _chain_tick(conn, monkeypatch, fail=True)
    assert [c for c in calls if '❌' in c[0]] == [], 'must not page before threshold'

    _chain_tick(conn, monkeypatch, fail=True)   # 3rd — crosses threshold
    _chain_tick(conn, monkeypatch, fail=True)   # 4th — already alerted

    pages = [c for c in calls if '❌ Aviv Live (chain)' in c[0]]
    assert len(pages) == 1, f'expected exactly one page, got {len(pages)}'
    assert '3 ticks in a row' in pages[0][1]
    assert pages[0][2].get('critical') is True


def test_chain_recovery_after_page(monkeypatch):
    """First good tick after a page → one ✅ recovery; next good tick silent."""
    conn = _chain_db()
    monkeypatch.setattr(aviv_live, '_is_store_hours', lambda: True)
    calls = _capture_notify(monkeypatch)

    for _ in range(3):
        _chain_tick(conn, monkeypatch, fail=True)
    _chain_tick(conn, monkeypatch, fail=False)
    _chain_tick(conn, monkeypatch, fail=False)

    recoveries = [c for c in calls if '✅ Aviv Live (chain)' in c[0]]
    assert len(recoveries) == 1, f'expected one recovery, got {len(recoveries)}'
    assert '15 minutes' in recoveries[0][1]


def test_chain_no_recovery_without_page(monkeypatch):
    """2 failed ticks (below threshold) then success → fully silent."""
    conn = _chain_db()
    monkeypatch.setattr(aviv_live, '_is_store_hours', lambda: True)
    calls = _capture_notify(monkeypatch)

    for _ in range(2):
        _chain_tick(conn, monkeypatch, fail=True)
    _chain_tick(conn, monkeypatch, fail=False)

    assert [c for c in calls if 'Aviv Live (chain)' in c[0]] == []


def test_chain_timeout_is_30s():
    """Chain path uses the widened timeout; legacy per-branch stays at 15."""
    assert aviv_live.CHAIN_API_TIMEOUT == 30
    assert aviv_live.API_TIMEOUT == 15


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

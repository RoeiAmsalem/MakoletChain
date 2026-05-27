"""Tests for the /z-status diagnostic page.

Covers admin-gate, default-date-is-yesterday-IL, status derivation
(got / closed / missing), and one row per active branch with aviv_branch_id.
"""
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app
from werkzeug.security import generate_password_hash


TEST_DB = os.path.join(os.path.dirname(__file__), 'test_z_status.db')


@pytest.fixture
def client():
    app.config['TESTING'] = True
    import app as app_module
    original_db = app_module.DB_PATH
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    app_module.DB_PATH = TEST_DB
    app_module.init_db()

    conn = sqlite3.connect(TEST_DB, timeout=30)
    # Ensure aviv_branch_id column exists in this fresh DB (migration 011 path).
    cols = [r[1] for r in conn.execute("PRAGMA table_info(branches)").fetchall()]
    if 'aviv_branch_id' not in cols:
        conn.execute("ALTER TABLE branches ADD COLUMN aviv_branch_id INTEGER")
    # Ensure z_report_902 table exists (migrations 010 + 013).
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS z_report_902 (
            branch_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            z_number INTEGER,
            amount REAL,
            transactions INTEGER,
            avg_per_txn REAL,
            payment_breakdown TEXT,
            fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
            trigger_type TEXT,
            auth_source TEXT,
            UNIQUE(branch_id, date)
        );
    ''')
    # Three branches: 126 (mapped), 127 (mapped), 9001 (autoseeded chain).
    # Plus one inactive branch and one without aviv_branch_id — both must
    # be excluded from /z-status.
    conn.execute("INSERT OR REPLACE INTO branches "
                 "(id, name, active, aviv_branch_id) VALUES (126, 'איינשטיין', 1, 3)")
    conn.execute("INSERT OR REPLACE INTO branches "
                 "(id, name, active, aviv_branch_id) VALUES (127, 'התיכון', 1, 8)")
    conn.execute("INSERT OR REPLACE INTO branches "
                 "(id, name, active, aviv_branch_id) VALUES (9001, 'Branch One', 1, 1)")
    conn.execute("INSERT OR REPLACE INTO branches "
                 "(id, name, active, aviv_branch_id) VALUES (200, 'Inactive', 0, 99)")
    conn.execute("INSERT OR REPLACE INTO branches "
                 "(id, name, active, aviv_branch_id) VALUES (300, 'NoAvivId', 1, NULL)")
    pw = generate_password_hash('test123')
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, active) "
                 "VALUES (1, 'Admin', 'admin@test.com', ?, 'admin', 1)", (pw,))
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, active) "
                 "VALUES (2, 'Manager', 'mgr@test.com', ?, 'manager', 1)", (pw,))
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (2, 126)")
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def _login(client, email='admin@test.com', password='test123'):
    return client.post('/login', data={'email': email, 'password': password},
                       follow_redirects=False)


def _seed_z(branch_id, target_date, z_number=None, amount=None, transactions=None,
            fetched_at='2026-05-27 23:00:00', trigger_type=None, auth_source=None):
    conn = sqlite3.connect(TEST_DB, timeout=30)
    conn.execute(
        "INSERT INTO z_report_902 (branch_id, date, z_number, amount, "
        "transactions, fetched_at, trigger_type, auth_source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (branch_id, target_date, z_number, amount, transactions, fetched_at,
         trigger_type, auth_source))
    conn.commit()
    conn.close()


def test_manager_gets_403(client):
    _login(client, 'mgr@test.com')
    res = client.get('/z-status')
    assert res.status_code == 403


def test_anon_redirects_to_login(client):
    res = client.get('/z-status')
    assert res.status_code in (302, 401)


def test_renders_one_row_per_eligible_branch(client):
    """Active branches with aviv_branch_id get a row; inactive + no-aviv excluded."""
    _login(client)
    res = client.get('/z-status?date=2026-05-26')
    assert res.status_code == 200
    html = res.get_data(as_text=True)
    # The three eligible branches are present.
    assert 'איינשטיין' in html
    assert 'התיכון' in html
    assert 'Branch One' in html
    # The two excluded branches are NOT present.
    assert 'Inactive' not in html
    assert 'NoAvivId' not in html


def test_status_derivation_got_closed_missing(client):
    """Real Z → 'קיבלנו'; closed-day sentinel → 'סגור'; no row → 'חסר'."""
    _login(client)
    date_str = '2026-05-26'
    # 126: real Z
    _seed_z(126, date_str, z_number=2525, amount=13721.98, transactions=234)
    # 127: closed-day sentinel (z_number IS NULL)
    _seed_z(127, date_str, z_number=None, amount=None, transactions=None)
    # 9001: no row → missing

    res = client.get(f'/z-status?date={date_str}')
    assert res.status_code == 200
    html = res.get_data(as_text=True)

    # Got, closed, and missing badges all present (CSS class + Hebrew label).
    assert 'zs-pill got' in html and 'קיבלנו' in html
    assert 'zs-pill closed' in html and 'סגור' in html
    assert 'zs-pill missing' in html and 'חסר' in html

    # Amount formatted with thousands separator + ₪.
    assert '₪13,721.98' in html
    # Z number visible.
    assert '2525' in html


def test_default_date_is_yesterday_il(client):
    """No ?date param → default to yesterday in Israel time."""
    _login(client)
    res = client.get('/z-status')
    assert res.status_code == 200
    # Compute expected yesterday-IL.
    from zoneinfo import ZoneInfo
    yesterday = (datetime.now(ZoneInfo('Asia/Jerusalem')).date()
                 - timedelta(days=1)).isoformat()
    html = res.get_data(as_text=True)
    # The date input is pre-populated to yesterday.
    assert f'value="{yesterday}"' in html


def test_invalid_date_falls_back_to_yesterday(client):
    """Garbage in ?date doesn't crash — falls back to yesterday IL."""
    _login(client)
    res = client.get('/z-status?date=not-a-date')
    assert res.status_code == 200


def test_metadata_columns_render_with_correct_tags(client):
    """trigger_type + auth_source map to the labeled tags in the table."""
    _login(client)
    date_str = '2026-05-26'
    # Auto + chain (typical 02:00 IL run via chain account).
    _seed_z(126, date_str, z_number=2525, amount=100.0, transactions=1,
            trigger_type='auto', auth_source='chain')
    # Manual + per_store (an admin doing a one-off probe from CLI without chain).
    _seed_z(127, date_str, z_number=1318, amount=200.0, transactions=2,
            trigger_type='manual', auth_source='per_store')
    # Pre-migration row: NULL metadata → renders the dashed "—" tag.
    _seed_z(9001, date_str, z_number=1, amount=50.0, transactions=1,
            trigger_type=None, auth_source=None)

    res = client.get(f'/z-status?date={date_str}')
    html = res.get_data(as_text=True)

    # Hebrew labels for each enum value present.
    assert 'אוטומטי' in html
    assert 'ידני' in html
    assert 'חשבון רשת' in html
    assert 'לפי סניף' in html
    # All four enum-tag CSS classes used.
    for cls in ('zs-tag auto', 'zs-tag manual', 'zs-tag chain', 'zs-tag per_store'):
        assert cls in html, f'missing tag class: {cls}'
    # Pre-migration row's NULL metadata renders the "empty" placeholder tag.
    assert 'zs-tag empty' in html


def test_nav_link_visible_for_admin(client):
    """Admin sees the סטטוס Z nav entry in base.html."""
    _login(client)
    res = client.get('/')
    html = res.get_data(as_text=True)
    assert 'href="/z-status"' in html
    assert 'סטטוס Z' in html


def test_nav_link_hidden_for_manager(client):
    """Non-admin (manager) does NOT see the סטטוס Z nav entry."""
    _login(client, 'mgr@test.com')
    res = client.get('/')
    html = res.get_data(as_text=True)
    assert 'href="/z-status"' not in html


def test_fetched_at_displayed_in_il_time(client):
    """A SQLite UTC fetched_at is rendered in Israel time (IDT in May, UTC+3)."""
    _login(client)
    date_str = '2026-05-26'
    # 00:30 UTC → 03:30 IDT.
    _seed_z(126, date_str, z_number=2525, amount=100.0, transactions=1,
            fetched_at='2026-05-27 00:30:00')
    res = client.get(f'/z-status?date={date_str}')
    html = res.get_data(as_text=True)
    # The page replaces 'T' with ' ' for readability — match either.
    assert '03:30:00' in html

"""Tests for /admin/analytics dashboard (Phase 2).

Covers admin-gate, range filtering, user_id filtering, session computation,
empty state and cache behavior.
"""
import os
import sys
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, _compute_sessions, format_duration_he
from werkzeug.security import generate_password_hash


TEST_DB = os.path.join(os.path.dirname(__file__), 'test_admin_analytics.db')


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
    # user_events + analytics_cache live in migrations — create them inline.
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS user_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            page TEXT,
            branch_id INTEGER,
            duration_seconds INTEGER,
            user_agent TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS analytics_cache (
            range TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            computed_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    conn.execute("INSERT OR REPLACE INTO branches (id, name, city, active) VALUES (126, 'איינשטיין', 'תל אביב', 1)")
    conn.execute("INSERT OR REPLACE INTO branches (id, name, city, active) VALUES (127, 'התיכון', 'תל אביב', 1)")
    pw = generate_password_hash('test123')
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, active) "
                 "VALUES (1, 'Admin', 'admin@test.com', ?, 'admin', 1)", (pw,))
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, active) "
                 "VALUES (2, 'Manager A', 'mgr_a@test.com', ?, 'manager', 1)", (pw,))
    conn.execute("INSERT INTO users (id, name, email, password_hash, role, active) "
                 "VALUES (3, 'Manager B', 'mgr_b@test.com', ?, 'manager', 1)", (pw,))
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (2, 126)")
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (3, 127)")
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def _login(client, email, password='test123'):
    return client.post('/login', data={'email': email, 'password': password},
                       follow_redirects=False)


def _utc(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def _seed_event(user_id, when_utc, event_type='page_view', page='/',
                branch_id=126, ua='Mozilla/5.0'):
    conn = sqlite3.connect(TEST_DB, timeout=30)
    conn.execute(
        "INSERT INTO user_events (user_id, event_type, page, branch_id, "
        "user_agent, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, event_type, page, branch_id, ua, _utc(when_utc))
    )
    conn.commit()
    conn.close()


def _clear_cache():
    conn = sqlite3.connect(TEST_DB, timeout=30)
    conn.execute("DELETE FROM analytics_cache")
    conn.commit()
    conn.close()


# ── Admin gate ────────────────────────────────────────────────

class TestAdminGate:

    def test_manager_gets_403(self, client):
        _login(client, 'mgr_a@test.com')
        res = client.get('/admin/analytics')
        assert res.status_code == 403

    def test_admin_gets_200(self, client):
        _login(client, 'admin@test.com')
        res = client.get('/admin/analytics')
        assert res.status_code == 200

    def test_subnav_renders(self, client):
        """Admin sub-toggle must include all three tabs."""
        _login(client, 'admin@test.com')
        res = client.get('/admin/analytics')
        body = res.get_data(as_text=True)
        assert 'ניתוח שימוש' in body
        assert 'סניפים' in body
        assert 'משתמשים' in body

    def test_anonymous_redirects(self, client):
        res = client.get('/admin/analytics')
        assert res.status_code in (301, 302)


# ── Empty state ───────────────────────────────────────────────

class TestEmptyState:

    def test_no_events_renders_empty_box(self, client):
        _login(client, 'admin@test.com')
        res = client.get('/admin/analytics?range=7d')
        assert res.status_code == 200
        body = res.get_data(as_text=True)
        assert 'אוסף נתונים' in body


# ── Range filter ──────────────────────────────────────────────

class TestRangeFilter:

    def test_7d_excludes_old_events(self, client):
        now = datetime.now(timezone.utc)
        # 2 events inside 7d window
        _seed_event(2, now - timedelta(days=1), event_type='login')
        _seed_event(2, now - timedelta(days=2), event_type='page_view')
        # 1 event well outside 7d window
        _seed_event(2, now - timedelta(days=20), event_type='login')
        _clear_cache()

        _login(client, 'admin@test.com')
        res = client.get('/admin/analytics?range=7d')
        body = res.get_data(as_text=True)
        # Only 1 login inside 7d
        assert '>1<' in body or '">1<' in body
        # Confirm the "20 days ago" event isn't in the window
        # by also asserting login_count in cache payload
        conn = sqlite3.connect(TEST_DB, timeout=30)
        row = conn.execute("SELECT payload FROM analytics_cache WHERE range='7d'").fetchone()
        conn.close()
        assert row is not None
        payload = json.loads(row[0])
        assert payload['login_count'] == 1


# ── User filter ───────────────────────────────────────────────

class TestUserFilter:

    def test_user_id_narrows_results(self, client):
        now = datetime.now(timezone.utc)
        _seed_event(2, now - timedelta(hours=1), event_type='login')
        _seed_event(2, now - timedelta(hours=1, minutes=30), event_type='page_view')
        _seed_event(3, now - timedelta(hours=1), event_type='login')
        _seed_event(3, now - timedelta(hours=1, minutes=30), event_type='page_view')
        _clear_cache()

        _login(client, 'admin@test.com')
        res = client.get('/admin/analytics?range=7d&user_id=2')
        assert res.status_code == 200
        body = res.get_data(as_text=True)
        # JSON endpoint for the same filter
        res2 = client.get('/api/admin/analytics/recent-activity?range=7d&user_id=2')
        data = res2.get_json()
        assert len(data['users_table']) == 1
        assert data['users_table'][0]['user_id'] == 2
        assert data['users_table'][0]['logins'] == 1


# ── Session computation ───────────────────────────────────────

class TestSessionComputation:

    def test_two_events_20min_apart_is_one_session(self):
        t0 = datetime(2026, 5, 13, 10, 0, 0)
        events = [
            {'user_id': 2, 'event_type': 'login',
             'created_at': t0.strftime('%Y-%m-%d %H:%M:%S')},
            {'user_id': 2, 'event_type': 'page_view',
             'created_at': (t0 + timedelta(minutes=20)).strftime('%Y-%m-%d %H:%M:%S')},
        ]
        sessions = _compute_sessions(events)
        assert len(sessions) == 1

    def test_two_events_40min_apart_is_two_sessions(self):
        t0 = datetime(2026, 5, 13, 10, 0, 0)
        events = [
            {'user_id': 2, 'event_type': 'login',
             'created_at': t0.strftime('%Y-%m-%d %H:%M:%S')},
            {'user_id': 2, 'event_type': 'page_view',
             'created_at': (t0 + timedelta(minutes=40)).strftime('%Y-%m-%d %H:%M:%S')},
        ]
        sessions = _compute_sessions(events)
        assert len(sessions) == 2

    def test_login_always_starts_new_session(self):
        t0 = datetime(2026, 5, 13, 10, 0, 0)
        events = [
            {'user_id': 2, 'event_type': 'login',
             'created_at': t0.strftime('%Y-%m-%d %H:%M:%S')},
            {'user_id': 2, 'event_type': 'page_view',
             'created_at': (t0 + timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')},
            {'user_id': 2, 'event_type': 'login',
             'created_at': (t0 + timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')},
        ]
        sessions = _compute_sessions(events)
        assert len(sessions) == 2


# ── Cache behavior ────────────────────────────────────────────

class TestCache:

    def test_second_call_hits_cache(self, client):
        now = datetime.now(timezone.utc)
        _seed_event(2, now - timedelta(hours=2), event_type='login')
        _clear_cache()

        _login(client, 'admin@test.com')

        with patch('app._analytics_aggregate',
                   wraps=__import__('app')._analytics_aggregate) as spy:
            r1 = client.get('/admin/analytics?range=7d')
            r2 = client.get('/admin/analytics?range=7d')
            assert r1.status_code == 200
            assert r2.status_code == 200
            # Only the first request should have called the aggregator;
            # the second should be served from analytics_cache.
            assert spy.call_count == 1

    def test_user_filter_bypasses_cache(self, client):
        now = datetime.now(timezone.utc)
        _seed_event(2, now - timedelta(hours=2), event_type='login')
        _clear_cache()

        _login(client, 'admin@test.com')

        with patch('app._analytics_aggregate',
                   wraps=__import__('app')._analytics_aggregate) as spy:
            client.get('/admin/analytics?range=7d&user_id=2')
            client.get('/admin/analytics?range=7d&user_id=2')
            assert spy.call_count == 2


# ── Polish pass ───────────────────────────────────────────────

class TestFormatDurationHe:

    def test_zero(self):
        assert format_duration_he(0) == '—'

    def test_under_minute(self):
        assert format_duration_he(30) == 'פחות מדקה'

    def test_minutes(self):
        assert format_duration_he(90) == '1 דקות'

    def test_exact_hour(self):
        assert format_duration_he(3600) == '1 שעות'

    def test_hour_and_minutes(self):
        assert format_duration_he(3700) == '1 שעות 1 דקות'

    def test_two_hours(self):
        assert format_duration_he(7200) == '2 שעות'


class TestPolishRendering:

    def test_page_labels_mapping(self, client):
        """Top pages must render Hebrew labels (e.g. 'בית'), not raw paths."""
        now = datetime.now(timezone.utc)
        for i in range(3):
            _seed_event(2, now - timedelta(hours=i + 1),
                        event_type='page_view', page='/')
        _clear_cache()
        _login(client, 'admin@test.com')
        res = client.get('/admin/analytics?range=7d')
        body = res.get_data(as_text=True)
        assert 'בית' in body
        # The raw '/' path should not appear as a popular-page list-item label.
        # We assert the .name span around the page label uses Hebrew.
        assert '<span class="name">בית</span>' in body

    def test_active_days_subtitle_full(self, client):
        """When distinct_days == days_in_window the tile shows 'פעיל בכל יום'."""
        now = datetime.now(timezone.utc)
        # 7 events spread across 7 distinct days to match 7d window.
        for i in range(7):
            _seed_event(2, now - timedelta(days=i, hours=2),
                        event_type='page_view')
        _clear_cache()
        _login(client, 'admin@test.com')
        res = client.get('/admin/analytics?range=7d')
        body = res.get_data(as_text=True)
        assert 'פעיל בכל יום' in body

    def test_sessions_per_day_rounding(self, client):
        """4.0 should render as '4', not '4.0'."""
        now = datetime.now(timezone.utc)
        # 4 logins per day for 1 day → 4 sessions / 7 days ≈ 0.6 (not integer)
        # To get exactly 4.0 ses/day, we'd need 28 sessions in 7d. Use 7
        # distinct logins (one per day, >30min apart) → 7 sessions / 7 days = 1.0.
        for i in range(7):
            _seed_event(2, now - timedelta(days=i, hours=2),
                        event_type='login')
        _clear_cache()
        _login(client, 'admin@test.com')
        res = client.get('/admin/analytics?range=7d')
        body = res.get_data(as_text=True)
        # The string '1.0' must not appear in the subtitle; '1 ליום' should.
        assert '1.0 ליום בממוצע' not in body
        assert '1 ליום בממוצע' in body

    def test_hour_chart_subtitle(self, client):
        """The hour chart must render the explanatory subtitle."""
        now = datetime.now(timezone.utc)
        _seed_event(2, now - timedelta(hours=2), event_type='page_view')
        _clear_cache()
        _login(client, 'admin@test.com')
        res = client.get('/admin/analytics?range=7d')
        body = res.get_data(as_text=True)
        assert 'כמות פעולות בכל שעה ביום' in body
        assert 'מקסימום' in body

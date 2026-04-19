"""Tests for /api/sales-by-hour: bucket definitions, empty state, math."""
import os
import sys
import sqlite3
import json
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, init_db, DB_PATH


@pytest.fixture
def client():
    """Create test client with fresh DB."""
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_hourly.db')
    original_db = DB_PATH

    import app as app_module
    app_module.DB_PATH = test_db

    if os.path.exists(test_db):
        os.remove(test_db)
    app_module.init_db()
    conn = sqlite3.connect(test_db, timeout=30)

    # Seed branch + user
    conn.execute("INSERT OR REPLACE INTO branches (id, name, city, active) VALUES (126, 'איינשטיין', 'תל אביב', 1)")
    from werkzeug.security import generate_password_hash
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) VALUES (1, 'CEO', 'admin@makolet.com', ?, 'admin', 1)",
        (generate_password_hash('test123'),))
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (1, 126)")
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(test_db):
        os.remove(test_db)


def _login(client):
    return client.post('/login', data={'email': 'admin@makolet.com', 'password': 'test123'}, follow_redirects=True)


def _seed_hourly_data(client):
    """Insert sample hourly_sales rows for testing."""
    import app as app_module
    conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
    # Data across 2 days, various hours
    rows = [
        (126, '2026-04-20', 7, 500, 10),
        (126, '2026-04-20', 8, 600, 12),
        (126, '2026-04-20', 13, 1200, 20),
        (126, '2026-04-20', 14, 800, 15),
        (126, '2026-04-20', 21, 300, 5),
        (126, '2026-04-20', 22, 400, 8),
        (126, '2026-04-20', 23, 200, 4),
        (126, '2026-04-21', 9, 700, 14),
        (126, '2026-04-21', 10, 900, 18),
        # Hour 6 — should NOT appear in any bucket
        (126, '2026-04-20', 6, 5000, 50),
    ]
    conn.executemany(
        "INSERT INTO hourly_sales (branch_id, date, hour, amount, transactions) VALUES (?, ?, ?, ?, ?)",
        rows)
    conn.commit()
    conn.close()


class TestBucketDefinitions:

    def test_returns_8_buckets(self, client):
        _login(client)
        resp = client.get('/api/sales-by-hour?month=2026-04')
        data = json.loads(resp.data)
        assert len(data['buckets']) == 8

    def test_first_bucket_starts_7(self, client):
        _login(client)
        resp = client.get('/api/sales-by-hour?month=2026-04')
        data = json.loads(resp.data)
        assert data['buckets'][0]['start'] == '7:00'

    def test_last_bucket_ends_2330(self, client):
        _login(client)
        resp = client.get('/api/sales-by-hour?month=2026-04')
        data = json.loads(resp.data)
        assert data['buckets'][-1]['end'] == '23:30'

    def test_hour_6_not_in_any_bucket(self, client):
        _login(client)
        _seed_hourly_data(client)
        resp = client.get('/api/sales-by-hour?month=2026-04')
        data = json.loads(resp.data)
        bucket_total = sum(b['total'] for b in data['buckets'])
        # Hour 6 has 5000 — should NOT be in bucket total
        hourly_7_to_23 = sum(data['hourly'][h]['total'] for h in range(7, 24))
        assert abs(bucket_total - hourly_7_to_23) < 0.01
        # Verify hour 6 is excluded
        assert data['hourly'][6]['total'] == 5000
        assert bucket_total < sum(h['total'] for h in data['hourly'])

    def test_bucket_totals_equal_hourly_7_to_23(self, client):
        _login(client)
        _seed_hourly_data(client)
        resp = client.get('/api/sales-by-hour?month=2026-04')
        data = json.loads(resp.data)
        bucket_total = sum(b['total'] for b in data['buckets'])
        hourly_sum = sum(data['hourly'][h]['total'] for h in range(7, 24))
        assert abs(bucket_total - hourly_sum) < 0.01


class TestEmptyState:

    def test_empty_returns_200(self, client):
        _login(client)
        resp = client.get('/api/sales-by-hour?month=2026-04')
        assert resp.status_code == 200

    def test_empty_has_8_zero_buckets(self, client):
        _login(client)
        resp = client.get('/api/sales-by-hour?month=2026-04')
        data = json.loads(resp.data)
        assert len(data['buckets']) == 8
        assert all(b['total'] == 0 for b in data['buckets'])

    def test_empty_total_days_is_zero(self, client):
        _login(client)
        resp = client.get('/api/sales-by-hour?month=2026-04')
        data = json.loads(resp.data)
        assert data['stats']['total_days_data'] == 0

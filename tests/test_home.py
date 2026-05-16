"""Tests for the home page (/) monthly cumulative-revenue chart.

The home chart reuses the exact /sales helper (_build_cumulative_chart_data)
and the same JS init (initSalesCumulativeChart). These tests lock:
  - the canvas renders for a logged-in manager,
  - the home payload is byte-identical to the /sales cumulative payload,
  - a zero-data month shows the empty state, not a broken canvas.
"""
import json
import os
import re
import sys
import sqlite3
from datetime import date, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app, _build_cumulative_chart_data
from werkzeug.security import generate_password_hash


# Seed inside the current UI month (May 2026 per project clock) so the home
# route — which always shows the selected month — picks them up.
MONTH = '2026-05'
D0 = date(2026, 5, 4)
SEED = [
    (D0, 1000, 10),
    (D0 + timedelta(days=1), 2000, 20),
    (D0 + timedelta(days=2), 750, 8),
]


@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_home.db')

    import app as app_module
    original_db = app_module.DB_PATH
    if os.path.exists(test_db):
        os.remove(test_db)
    app_module.DB_PATH = test_db
    app_module.init_db()

    conn = sqlite3.connect(test_db, timeout=30)
    conn.execute("INSERT OR REPLACE INTO branches (id, name, city, active) "
                 "VALUES (126, 'איינשטיין', 'תל אביב', 1)")
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) "
        "VALUES (2, 'Manager', 'mgr@test.com', ?, 'manager', 1)",
        (generate_password_hash('test123'),))
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (2, 126)")
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) "
        "VALUES (1, 'Admin', 'admin@test.com', ?, 'admin', 1)",
        (generate_password_hash('test123'),))
    for d, amt, txn in SEED:
        conn.execute(
            "INSERT INTO daily_sales (branch_id, date, amount, transactions, source) "
            "VALUES (?, ?, ?, ?, 'z_report')",
            (126, d.strftime('%Y-%m-%d'), amt, txn))
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(test_db):
        os.remove(test_db)


def _login(client, email='mgr@test.com', password='test123'):
    return client.post('/login', data={'email': email, 'password': password},
                        follow_redirects=False)


def _home_cumulative(body):
    """The array passed to initSalesCumulativeChart on the home page."""
    m = re.search(
        r"initSalesCumulativeChart\('home-cumulative-chart',\s*(\[.*?\])\)",
        body, re.S)
    assert m, 'home page does not inject the cumulative payload'
    return json.loads(m.group(1))


def _sales_cumulative(body):
    """The .cumulative slice of SALES_CHARTS on the /sales page."""
    m = re.search(r'const SALES_CHARTS = (\{.*\});', body)
    assert m, 'no SALES_CHARTS on /sales response'
    return json.loads(m.group(1))['cumulative']


def test_home_renders_cumulative_canvas(client):
    _login(client)
    res = client.get(f'/?month={MONTH}')
    assert res.status_code == 200
    assert '<canvas id="home-cumulative-chart"' in res.get_data(as_text=True)


def test_home_cumulative_data_matches_sales(client):
    _login(client, 'admin@test.com')
    home = _home_cumulative(client.get(f'/?month={MONTH}').get_data(as_text=True))
    sales = _sales_cumulative(
        client.get(f'/sales?month={MONTH}').get_data(as_text=True))

    expected = _build_cumulative_chart_data(
        [{'date': d.strftime('%Y-%m-%d'), 'amount': a} for d, a, _ in SEED])
    assert home == sales == expected
    assert [c['value'] for c in home] == [1000, 3000, 3750]


def test_home_cumulative_empty_state(client):
    _login(client)
    # A month with zero z-reports → empty state, no canvas.
    body = client.get('/?month=2099-01').get_data(as_text=True)
    assert 'אין נתונים עדיין החודש' in body
    assert '<canvas id="home-cumulative-chart"' not in body

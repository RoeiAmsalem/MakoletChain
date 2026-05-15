"""Tests for the /sales charts (daily revenue / day-of-week / cumulative).

All red-vs-blue logic lives in the backend helpers, so most of these are
pure unit tests on the helpers; one render test asserts the three canvases
reach the page.

Weekend rule recap:
  - Friday is ALWAYS blue (it is NOT the red weekend).
  - CASE A (branch HAS Saturday Z): Saturday is red; a Sunday is red only
    when the preceding Saturday also has a Z in range.
  - CASE B (branch has NO Saturday Z): a Sunday is the merged "שבת+ראשון"
    red bar only when it has other Z days in its week; a lone isolated
    Sunday stays a normal blue ראשון.
"""
import os
import sys
import sqlite3
from datetime import date, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import (
    app,
    _build_daily_chart_data,
    _build_dow_chart_data,
    _build_cumulative_chart_data,
)
from werkzeug.security import generate_password_hash


# ── date helpers (no hard-coded weekday math) ─────────────────

def _on_or_after(start, target_weekday):
    """First date >= start whose .weekday() == target_weekday."""
    d = start
    while d.weekday() != target_weekday:
        d += timedelta(days=1)
    return d


def _z(d, amount):
    return {'date': d.strftime('%Y-%m-%d'), 'amount': amount}


# Anchor inside a normal week somewhere in May 2026.
SAT = _on_or_after(date(2026, 5, 4), 5)   # a Saturday
SUN = SAT + timedelta(days=1)             # the Sunday after it
FRI = SAT - timedelta(days=1)             # the Friday before it
MON = SUN + timedelta(days=1)
TUE = SUN + timedelta(days=2)


def _find(daily, ddmm):
    return next(b for b in daily if b['date'] == ddmm)


# ── CASE A — branch has Saturday Z reports ────────────────────

def test_build_daily_chart_has_saturday():
    daily = _build_daily_chart_data([
        _z(FRI, 1000), _z(SAT, 2000), _z(SUN, 3000),
    ])
    sat = _find(daily, SAT.strftime('%d/%m'))
    sun = _find(daily, SUN.strftime('%d/%m'))
    fri = _find(daily, FRI.strftime('%d/%m'))

    assert sat['color'] == 'red' and sat['label_secondary'] == 'שבת'
    # Sunday is red because the preceding Saturday also has a Z.
    assert sun['color'] == 'red' and sun['label_secondary'] == 'ראשון'
    # Friday is NEVER the red weekend.
    assert fri['color'] == 'blue' and fri['label_secondary'] == 'שישי'


def test_sunday_blue_when_no_preceding_saturday_case_a():
    """CASE A but THIS Sunday's own preceding Saturday has no Z → blue."""
    other_sat = SAT + timedelta(days=7)        # keeps has_saturday_z True
    lone_sun = SUN + timedelta(days=14)        # its Saturday (lone_sun-1)
    daily = _build_daily_chart_data([          # is NOT in the data set
        _z(other_sat, 500), _z(lone_sun, 900),
    ])
    s = _find(daily, lone_sun.strftime('%d/%m'))
    assert s['color'] == 'blue' and s['label_secondary'] == 'ראשון'


# ── CASE B — branch has NO Saturday Z reports ─────────────────

def test_build_daily_chart_no_saturday():
    # Sunday with neighbour weekday data in its week → merged red bar.
    daily = _build_daily_chart_data([
        _z(SUN, 4000), _z(MON, 1200), _z(TUE, 1100),
    ])
    sun = _find(daily, SUN.strftime('%d/%m'))
    assert sun['color'] == 'red'
    assert sun['label_secondary'] == 'שבת+ראשון'
    assert _find(daily, MON.strftime('%d/%m'))['color'] == 'blue'


def test_build_daily_chart_lone_sunday_blue():
    """The clarified edge case: an isolated Sunday (no other Z days in its
    week) stays a normal blue ראשון even though has_saturday_z is False."""
    daily = _build_daily_chart_data([_z(SUN, 4000)])
    sun = _find(daily, SUN.strftime('%d/%m'))
    assert sun['color'] == 'blue'
    assert sun['label_secondary'] == 'ראשון'


# ── Day-of-week chart ─────────────────────────────────────────

def test_build_dow_chart_has_saturday():
    dow = _build_dow_chart_data([
        _z(FRI, 1000), _z(SAT, 2000), _z(SUN, 3000), _z(MON, 500),
    ])
    assert len(dow) == 7
    assert [b['label'] for b in dow] == \
        ['ראשון', 'שני', 'שלישי', 'רביעי', 'חמישי', 'שישי', 'שבת']
    by = {b['label']: b for b in dow}
    assert by['שבת']['color'] == 'red'
    assert by['ראשון']['color'] == 'red'
    assert by['שישי']['color'] == 'blue'        # Friday never red


def test_build_dow_chart_no_saturday():
    dow = _build_dow_chart_data([
        _z(SUN, 4000), _z(SUN + timedelta(days=7), 2000),  # two Sundays
        _z(MON, 1000), _z(FRI, 800),
    ])
    assert len(dow) == 6
    assert dow[0]['label'] == 'שבת+ראשון'
    assert dow[0]['color'] == 'red'
    assert dow[0]['value'] == 3000              # mean(4000, 2000)
    by = {b['label']: b for b in dow}
    assert by['שישי']['color'] == 'blue'        # Friday never red
    assert 'שבת' not in by                       # no standalone Saturday bar


# ── Cumulative ────────────────────────────────────────────────

def test_build_cumulative():
    d0 = date(2026, 5, 4)
    cum = _build_cumulative_chart_data([
        _z(d0, 100),
        _z(d0 + timedelta(days=1), 50),
        _z(d0 + timedelta(days=2), 75),
    ])
    assert [c['value'] for c in cum] == [100, 150, 225]
    assert cum[0]['date'] == d0.strftime('%d/%m')


def test_cumulative_sorts_unordered_input():
    d0 = date(2026, 5, 4)
    cum = _build_cumulative_chart_data([
        _z(d0 + timedelta(days=2), 75),
        _z(d0, 100),
        _z(d0 + timedelta(days=1), 50),
    ])
    assert [c['value'] for c in cum] == [100, 150, 225]


# ── Friday is never red, in every scenario ────────────────────

def test_friday_never_red():
    # daily, CASE A
    a = _build_daily_chart_data([_z(FRI, 1), _z(SAT, 2), _z(SUN, 3)])
    assert _find(a, FRI.strftime('%d/%m'))['color'] == 'blue'
    # daily, CASE B
    b = _build_daily_chart_data([_z(SUN, 1), _z(FRI, 2), _z(MON, 3)])
    assert _find(b, FRI.strftime('%d/%m'))['color'] == 'blue'
    # dow, CASE A
    da = _build_dow_chart_data([_z(FRI, 1), _z(SAT, 2), _z(SUN, 3)])
    assert {x['label']: x for x in da}['שישי']['color'] == 'blue'
    # dow, CASE B
    db = _build_dow_chart_data([_z(FRI, 1), _z(SUN, 2), _z(MON, 3)])
    assert {x['label']: x for x in db}['שישי']['color'] == 'blue'


# ── Page render ───────────────────────────────────────────────

@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_sales_charts.db')

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
    for d, amt in [(FRI, 1000), (SAT, 2000), (SUN, 3000), (MON, 1500)]:
        conn.execute(
            "INSERT INTO daily_sales (branch_id, date, amount, source) "
            "VALUES (?, ?, ?, 'z_report')",
            (126, d.strftime('%Y-%m-%d'), amt))
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


def test_sales_page_renders_three_canvases(client):
    _login(client)
    res = client.get(f"/sales?month={SAT.strftime('%Y-%m')}")
    assert res.status_code == 200
    body = res.get_data(as_text=True)
    assert '<canvas id="salesDailyChart"' in body
    assert '<canvas id="salesDowChart"' in body
    assert '<canvas id="salesCumChart"' in body

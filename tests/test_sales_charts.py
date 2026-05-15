"""Tests for the /sales charts + 6-cell table footer.

Colour rule (single source of truth in the backend helpers):
  red  = a bar that represents COMBINED Saturday+Sunday revenue.
  blue = any single-day bar (Saturday alone, Sunday alone, anything else).
  Friday is ALWAYS blue.

  - has_saturday_z True  → every bar is its own day, all blue, no red.
  - has_saturday_z False → a Sunday with no preceding Saturday Z is the
    combined שבת+ראשון bar (red); day-of-week chart collapses Sat+Sun
    into one red bar.
"""
import os
import re
import sys
import sqlite3
from datetime import date, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import (
    app,
    _has_saturday_z,
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


# ── _has_saturday_z ───────────────────────────────────────────

def test_has_saturday_z_true():
    assert _has_saturday_z([_z(FRI, 1), _z(SAT, 2), _z(SUN, 3)]) is True


def test_has_saturday_z_false():
    assert _has_saturday_z([_z(SUN, 1), _z(MON, 2), _z(FRI, 3)]) is False


# ── Daily chart ───────────────────────────────────────────────

def test_daily_chart_with_saturday_z_no_red():
    """Branch runs Saturday Zs → every bar is its own day, all blue."""
    daily = _build_daily_chart_data([
        _z(FRI, 1000), _z(SAT, 2000), _z(SUN, 3000), _z(MON, 1500),
    ])
    assert all(b['color'] == 'blue' for b in daily)
    assert all(b['label_secondary'] is None for b in daily)


def test_daily_chart_without_saturday_z():
    """No Saturday Z → the Sunday becomes the combined שבת+ראשון red bar."""
    daily = _build_daily_chart_data([
        _z(SUN, 4000), _z(MON, 1200), _z(TUE, 1100),
    ])
    sun = _find(daily, SUN.strftime('%d/%m'))
    assert sun['color'] == 'red'
    assert sun['label_secondary'] == 'שבת+ראשון'
    assert _find(daily, MON.strftime('%d/%m'))['color'] == 'blue'
    assert _find(daily, MON.strftime('%d/%m'))['label_secondary'] is None


def test_daily_chart_without_saturday_z_clean_sunday():
    """One Saturday + its Sunday: has_saturday_z is True (a weekday=5 row
    exists), so the Sunday is a single-day bar and stays blue."""
    reports = [_z(SAT, 2000), _z(SUN, 3000)]
    assert _has_saturday_z(reports) is True
    daily = _build_daily_chart_data(reports)
    sun = _find(daily, SUN.strftime('%d/%m'))
    assert sun['color'] == 'blue'
    assert sun['label_secondary'] is None


def test_daily_friday_always_blue():
    # CASE A
    a = _build_daily_chart_data([_z(FRI, 1), _z(SAT, 2), _z(SUN, 3)])
    assert _find(a, FRI.strftime('%d/%m'))['color'] == 'blue'
    # CASE B
    b = _build_daily_chart_data([_z(FRI, 1), _z(SUN, 2), _z(MON, 3)])
    assert _find(b, FRI.strftime('%d/%m'))['color'] == 'blue'


# ── Day-of-week chart ─────────────────────────────────────────

def test_dow_chart_with_saturday_z_seven_bars_all_blue():
    dow = _build_dow_chart_data([
        _z(FRI, 1000), _z(SAT, 2000), _z(SUN, 3000), _z(MON, 500),
    ])
    assert len(dow) == 7
    assert [b['label'] for b in dow] == \
        ['ראשון', 'שני', 'שלישי', 'רביעי', 'חמישי', 'שישי', 'שבת']
    assert all(b['color'] == 'blue' for b in dow)


def test_dow_chart_without_saturday_z_six_bars():
    dow = _build_dow_chart_data([
        _z(SUN, 4000), _z(SUN + timedelta(days=7), 2000),  # two Sundays
        _z(MON, 1000), _z(FRI, 800),
    ])
    assert len(dow) == 6
    assert dow[0]['label'] == 'שבת+ראשון'
    assert dow[0]['color'] == 'red'
    assert dow[0]['value'] == 3000               # mean(4000, 2000)
    assert all(b['color'] == 'blue' for b in dow[1:])
    by = {b['label']: b for b in dow}
    assert by['שישי']['color'] == 'blue'         # Friday never red
    assert 'שבת' not in by                        # no standalone Saturday bar


# ── Cumulative ────────────────────────────────────────────────

def test_cumulative_running_sum():
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


# ── Page render + footer ──────────────────────────────────────

# (date, amount, transactions)
SEED = [(FRI, 1000, 10), (SAT, 2000, 20), (SUN, 3000, 25), (MON, 1500, 12)]
TOTAL_REV = sum(a for _, a, _ in SEED)            # 7500
TOTAL_TXN = sum(t for _, _, t in SEED)            # 67
AVG_BASKET = round(TOTAL_REV / TOTAL_TXN)         # 112


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


def _footer_cells(body):
    """Return the stripped text of each <td> in the server-rendered tfoot."""
    foot = re.search(r'<tfoot[^>]*>(.*?)</tfoot>', body, re.S)
    assert foot, 'no <tfoot> in /sales response'
    cells = re.findall(r'<td[^>]*>(.*?)</td>', foot.group(1), re.S)
    return [re.sub(r'<[^>]+>', '', c).strip() for c in cells]


def test_sales_page_renders_three_canvases(client):
    _login(client)
    res = client.get(f"/sales?month={SAT.strftime('%Y-%m')}")
    assert res.status_code == 200
    body = res.get_data(as_text=True)
    assert '<canvas id="salesDailyChart"' in body
    assert '<canvas id="salesDowChart"' in body
    assert '<canvas id="salesCumChart"' in body


def test_sales_footer_six_cells(client):
    _login(client)
    res = client.get(f"/sales?month={SAT.strftime('%Y-%m')}")
    cells = _footer_cells(res.get_data(as_text=True))
    assert len(cells) == 6, f"expected 6 footer cells, got {len(cells)}: {cells}"


def test_sales_footer_source_order(client):
    """The footer <td>s must be in the SAME source order as the <thead>
    columns (תאריך | סכום | עסקאות | ממוצע | מקור | PDF), so the RTL
    browser lays the label adjacent to the totals it labels.

    Catches a regression where the 6 cells exist but are reordered /
    reversed (e.g. סה"כ rendered last instead of first).
    """
    _login(client)
    res = client.get(f"/sales?month={SAT.strftime('%Y-%m')}")
    cells = _footer_cells(res.get_data(as_text=True))
    assert len(cells) == 6
    assert 'סה"כ' in cells[0]                       # תאריך column
    assert '₪' in cells[1] and any(c.isdigit() for c in cells[1])  # סכום
    assert cells[2].replace(',', '').isdigit()      # עסקאות (txn count)
    assert '₪' in cells[3]                          # ממוצע לעסקה
    assert cells[4] == '' and cells[5] == ''        # מקור / PDF: no aggregate


def test_sales_footer_transactions_sum(client):
    _login(client)
    res = client.get(f"/sales?month={SAT.strftime('%Y-%m')}")
    cells = _footer_cells(res.get_data(as_text=True))
    assert int(cells[2]) == TOTAL_TXN


def test_sales_footer_avg_basket(client):
    _login(client)
    res = client.get(f"/sales?month={SAT.strftime('%Y-%m')}")
    cells = _footer_cells(res.get_data(as_text=True))
    basket = int(cells[3].replace('₪', '').replace(',', '').strip())
    assert basket == AVG_BASKET


def test_sales_footer_browser_equivalent_six_distinct_cells(client):
    """Regression for the 'tests pass but the browser shows 3 cells' bug.

    The footer must be ONE source of truth — server-rendered — with six
    DISTINCT <td>s, none spanning multiple columns. A colspan>1 cell is
    exactly the old broken layout (סה"כ | total | <td colspan=4>), so we
    assert there is no colspan>1 AND that the effective column count the
    browser would lay out is exactly 6.

    NOTE: footer correctness is server-side-only. There is no JS test
    runner in this repo; the fix removes all client-side <tfoot> writes
    so the rendered HTML *is* what the browser sees (no runtime rebuild).
    """
    _login(client)
    res = client.get(f"/sales?month={SAT.strftime('%Y-%m')}")
    body = res.get_data(as_text=True)

    foot = re.search(r'<tfoot[^>]*>(.*?)</tfoot>', body, re.S)
    assert foot, 'no <tfoot> in /sales response'
    foot_html = foot.group(1)

    td_tags = re.findall(r'<td([^>]*)>', foot_html)
    assert len(td_tags) == 6, \
        f"expected 6 <td> tags, got {len(td_tags)}"

    # The browser lays out columns by summing colspans. The old broken
    # footer was 3 <td>s with one colspan=4 (still 6 columns) but only
    # 3 *visible* cells — so also assert no cell spans.
    spans = []
    for attrs in td_tags:
        m = re.search(r'colspan\s*=\s*["\']?(\d+)', attrs)
        spans.append(int(m.group(1)) if m else 1)
    assert all(s == 1 for s in spans), \
        f"footer has a spanning cell (old 3-cell layout): colspans={spans}"
    assert sum(spans) == 6

"""Fetch + cache Shabbat/holiday candle-lighting → havdalah windows from Hebcal
for the chain's area (Haifa, geonameid 294801).

Used to CLASSIFY shift hours as Shabbat/chag — display only, never costing.
Fail-soft: Hebcal is external; on any error we log and leave the cache as-is so
the nightly pipeline never crashes.

API: https://www.hebcal.com/shabbat?cfg=json&geonameid=294801&M=on
  items[].category == 'candles'   → window start (candle lighting)
  items[].category == 'havdalah'  → window end
  items[].category == 'holiday' (yomtov) → chag title (labelled is_holiday=1)

/shabbat returns one week; pass a date (gy/gm/gd) to get that week. The weekly
refresh job walks a few weeks back + ahead so recent + upcoming shifts always
have their window cached.
"""

import json
import logging
import os
import sqlite3
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta

log = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')
HAIFA_GEONAMEID = 294801
HEBCAL_SHABBAT_URL = 'https://www.hebcal.com/shabbat'


def _parse_iso_local(s):
    """ISO8601 → naive Israel-local datetime (drops the offset), else None."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s)).replace(tzinfo=None)
    except ValueError:
        return None


def fetch_shabbat_items(geonameid=HAIFA_GEONAMEID, on_date=None, timeout=15):
    """Fetch Hebcal /shabbat items for the week containing on_date (or current)."""
    params = {'cfg': 'json', 'geonameid': geonameid, 'M': 'on'}
    if on_date is not None:
        params.update({'gy': on_date.year, 'gm': on_date.month, 'gd': on_date.day})
    url = f'{HEBCAL_SHABBAT_URL}?{urllib.parse.urlencode(params)}'
    req = urllib.request.Request(url, headers={'User-Agent': 'MakoletChain/1.0'})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.loads(resp.read().decode('utf-8'))
    return data.get('items', [])


def _pair_windows(items):
    """Pair candles→havdalah into windows.

    Multiple candle-lightings before a single havdalah (a chag adjacent to
    Shabbat) collapse into ONE window spanning from the first candle-lighting to
    the havdalah. Friday candles → 'שבת'; any other weekday → holiday (chag)."""
    windows = []
    pending = None  # [candle_dt, label, is_holiday]
    last_holiday_title = None
    for it in items:
        cat = it.get('category')
        if cat == 'holiday' and it.get('yomtov'):
            last_holiday_title = it.get('title')
        elif cat == 'candles':
            dt = _parse_iso_local(it.get('date'))
            if dt and pending is None:
                is_fri = dt.weekday() == 4  # Mon=0 … Fri=4
                label = 'שבת' if is_fri else (last_holiday_title or 'חג')
                pending = [dt, label, 0 if is_fri else 1]
        elif cat == 'havdalah':
            dt = _parse_iso_local(it.get('date'))
            if dt and pending is not None:
                windows.append({
                    'date': pending[0].strftime('%Y-%m-%d'),
                    'candle': pending[0],
                    'havdalah': dt,
                    'label': pending[1],
                    'is_holiday': pending[2],
                })
                pending = None
    return windows


def _store_windows(windows, geonameid, conn):
    stored = 0
    for w in windows:
        conn.execute('''
            INSERT INTO shabbat_times
            (date, candle_lighting_ts, havdalah_ts, is_holiday, label, geonameid, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(date, geonameid) DO UPDATE SET
                candle_lighting_ts=excluded.candle_lighting_ts,
                havdalah_ts=excluded.havdalah_ts,
                is_holiday=excluded.is_holiday,
                label=excluded.label,
                updated_at=datetime('now')
        ''', (w['date'], w['candle'].strftime('%Y-%m-%d %H:%M:%S'),
              w['havdalah'].strftime('%Y-%m-%d %H:%M:%S'),
              w['is_holiday'], w['label'], geonameid))
        stored += 1
    conn.commit()
    return stored


def fetch_and_store(geonameid=HAIFA_GEONAMEID, weeks_back=6, weeks_ahead=4,
                    today=None) -> dict:
    """Refresh shabbat_times for a window of weeks around today (fail-soft).

    Walks one /shabbat fetch per week from weeks_back ago to weeks_ahead ahead,
    pairs candle→havdalah, and upserts. A single week failing is logged and
    skipped; a total failure (e.g. Hebcal down at the very first call with no
    data) returns ok=False without raising.
    """
    today = today or date.today()
    all_windows = {}
    weeks_fetched = 0
    errors = 0
    for w in range(-weeks_back, weeks_ahead + 1):
        on_date = today + timedelta(weeks=w)
        try:
            items = fetch_shabbat_items(geonameid, on_date=on_date)
            for win in _pair_windows(items):
                all_windows[win['date']] = win  # dedup by candle date
            weeks_fetched += 1
        except Exception as e:
            errors += 1
            log.warning("Hebcal fetch failed for week of %s (fail-soft): %s",
                        on_date.isoformat(), str(e)[:120])

    if not all_windows:
        log.warning("shabbat_times: no windows fetched (Hebcal unreachable?) — "
                    "leaving cache unchanged")
        return {'ok': False, 'stored': 0, 'weeks_fetched': weeks_fetched,
                'errors': errors}

    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        stored = _store_windows(list(all_windows.values()), geonameid, conn)
    finally:
        conn.close()
    log.info("shabbat_times: stored/updated %d windows (geonameid=%d, %d weeks, %d errors)",
             stored, geonameid, weeks_fetched, errors)
    return {'ok': True, 'stored': stored, 'weeks_fetched': weeks_fetched, 'errors': errors}


if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    result = fetch_and_store()
    print(result)
    sys.exit(0 if result.get('ok') else 1)

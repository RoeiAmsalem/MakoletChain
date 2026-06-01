"""Classify a shift's hours into regular / overtime / Shabbat buckets.

DISPLAY ONLY — this never touches salary. _calculate_salary_cost is unchanged;
these buckets are labels for the shift drill-down + monthly summary.

Rules (locked):
  - Overtime: DAILY basis. Cumulative hours over 8 in a single day are overtime.
    Allocated across the day's shifts in start-time order: the first 8 cumulative
    hours are regular, the rest overtime. (regular + overtime = the shift hours.)
  - Shabbat: hours that fall inside a Shabbat/chag window (candle-lighting →
    havdalah, from Hebcal). ORTHOGONAL to the regular/overtime split — a shift
    can be partly Shabbat AND partly overtime. Capped at the shift's hours.
  - Global-salary employees are not classified (flat pay): regular = hours,
    overtime = 0, shabbat = 0 — shown plain.
"""

import logging
import sqlite3
from collections import defaultdict
from datetime import datetime

log = logging.getLogger(__name__)

DAILY_REGULAR_CAP = 8.0  # hours/day before overtime begins


def _parse_ts(s):
    """'YYYY-MM-DD HH:MM:SS' → datetime, else None."""
    if not s:
        return None
    try:
        return datetime.strptime(str(s), '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None


def _parse_iso_local(s):
    """ISO8601 (with or without offset) → naive Israel-local datetime, else None.

    Hebcal emits e.g. '2026-05-29T19:14:00+03:00'; the wall-clock is already
    Israel local, so we drop the offset and compare against the (also-local,
    naive) shift timestamps."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s)).replace(tzinfo=None)
    except ValueError:
        return None


def load_shabbat_windows(conn):
    """Return [(start_dt, end_dt), ...] from shabbat_times (havdalah known).

    Fail-soft: missing table / bad rows yield an empty list, so classification
    degrades to regular/overtime only (shabbat_hours=0) rather than crashing."""
    try:
        rows = conn.execute(
            "SELECT candle_lighting_ts, havdalah_ts FROM shabbat_times "
            "WHERE havdalah_ts IS NOT NULL"
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    windows = []
    for r in rows:
        s = _parse_ts(r[0]) or _parse_iso_local(r[0])
        e = _parse_ts(r[1]) or _parse_iso_local(r[1])
        if s and e and e > s:
            windows.append((s, e))
    windows.sort()
    return windows


def _shabbat_overlap_hours(start_ts, end_ts, windows):
    """Total hours of [start, end] that fall inside any Shabbat/chag window.

    Windows are non-overlapping (paired candle→havdalah), so per-window overlaps
    sum cleanly."""
    s = _parse_ts(start_ts)
    e = _parse_ts(end_ts)
    if not s or not e or e <= s:
        return 0.0
    total = 0.0
    for ws, we in windows:
        overlap = (min(e, we) - max(s, ws)).total_seconds()
        if overlap > 0:
            total += overlap / 3600.0
    return total


def classify_shifts(shifts, shabbat_windows, is_global=False):
    """Annotate each shift dict with regular_hours / overtime_hours / shabbat_hours.

    Mutates and returns `shifts`. Daily overtime is allocated across each day's
    shifts in start-time order (the day's first 8 cumulative hours are regular).
    Open shifts (hours=0, no clock-out) get all-zero buckets.
    """
    if is_global:
        for s in shifts:
            h = round(float(s.get('hours') or 0), 4)
            s['regular_hours'] = h
            s['overtime_hours'] = 0.0
            s['shabbat_hours'] = 0.0
        return shifts

    by_date = defaultdict(list)
    for s in shifts:
        by_date[s.get('shift_date')].append(s)

    for _date, day_shifts in by_date.items():
        # Stable order so the daily-8 allocation is deterministic; shifts with no
        # start_ts (rare orphan rows) sort last.
        day_shifts.sort(key=lambda s: (s.get('start_ts') is None, s.get('start_ts') or ''))
        cumulative = 0.0
        for s in day_shifts:
            h = float(s.get('hours') or 0)
            regular_room = max(0.0, DAILY_REGULAR_CAP - cumulative)
            regular = min(h, regular_room)
            overtime = h - regular
            cumulative += h
            shabbat = min(h, _shabbat_overlap_hours(
                s.get('start_ts'), s.get('end_ts'), shabbat_windows))
            s['regular_hours'] = round(regular, 4)
            s['overtime_hours'] = round(overtime, 4)
            s['shabbat_hours'] = round(shabbat, 4)
    return shifts

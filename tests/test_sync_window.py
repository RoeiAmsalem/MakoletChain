"""utils.sync_window.months_to_sync — the one BilBoy re-read window."""
import os
import sys
from datetime import date, datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.sync_window import (
    il_today, month_bounds, months_to_sync, prev_month, prev_month_days,
)


def test_day_1_reads_prev_and_current():
    assert months_to_sync(date(2026, 9, 1)) == ['2026-08', '2026-09']


def test_day_7_still_reads_prev():
    assert months_to_sync(date(2026, 9, 7)) == ['2026-08', '2026-09']


def test_day_8_current_only():
    assert months_to_sync(date(2026, 9, 8)) == ['2026-09']


def test_day_31_current_only():
    assert months_to_sync(date(2026, 8, 31)) == ['2026-08']


def test_jan_1_year_rollover():
    assert months_to_sync(date(2027, 1, 1)) == ['2026-12', '2027-01']


def test_utc_il_boundary_2300_utc_is_next_day_in_israel():
    # The 02:00 IL nightly run on Sep 1 is Aug 31 23:00 UTC (IDT, UTC+3).
    now = datetime(2026, 8, 31, 23, 0, tzinfo=timezone.utc)
    assert il_today(now) == date(2026, 9, 1)
    assert months_to_sync(il_today(now)) == ['2026-08', '2026-09']
    # the UTC date would have missed August entirely
    assert months_to_sync(now.date()) == ['2026-08']


def test_utc_il_boundary_winter_new_year():
    # IST (UTC+2): Dec 31 23:00 UTC = Jan 1 01:00 IL
    now = datetime(2026, 12, 31, 23, 0, tzinfo=timezone.utc)
    assert il_today(now) == date(2027, 1, 1)
    assert months_to_sync(il_today(now)) == ['2026-12', '2027-01']


def test_naive_now_is_treated_as_utc():
    assert il_today(datetime(2026, 8, 31, 23, 0)) == date(2026, 9, 1)


def test_env_configurable(monkeypatch):
    monkeypatch.setenv('BILBOY_PREV_MONTH_DAYS', '3')
    assert prev_month_days() == 3
    assert months_to_sync(date(2026, 9, 3)) == ['2026-08', '2026-09']
    assert months_to_sync(date(2026, 9, 4)) == ['2026-09']
    monkeypatch.setenv('BILBOY_PREV_MONTH_DAYS', '0')
    assert months_to_sync(date(2026, 9, 1)) == ['2026-09']


def test_env_default_and_garbage(monkeypatch):
    monkeypatch.delenv('BILBOY_PREV_MONTH_DAYS', raising=False)
    assert prev_month_days() == 7
    monkeypatch.setenv('BILBOY_PREV_MONTH_DAYS', 'seven')
    assert prev_month_days() == 7


def test_month_helpers():
    assert prev_month('2027-01') == '2026-12'
    assert month_bounds('2026-08') == ('2026-08-01', '2026-08-31')
    assert month_bounds('2028-02') == ('2028-02-01', '2028-02-29')

"""Which BilBoy months a nightly sync must (re)read — the ONE window mechanism.

Root cause this exists for: the BilBoy sync pinned its window to the CURRENT
month, so anything BilBoy adds or reissues for a month after that month's
last night is never seen:
  - Wolt fee invoices are dated the 31st but ISSUED in the first days of the
    next month (all 8 Wolt branches' August invoices: dated 2026-08-31);
  - goods reissues (status 7 replacements) and late supplier invoices.

Rule: on days 1..BILBOY_PREV_MONTH_DAYS (default 7) of a month, re-read the
PREVIOUS month too; from the day after, only the current month. "Today" is
Israel time, never the server's UTC date — the 02:00 IL nightly run is
23:00 UTC the previous day, so a UTC date would be one day (and on the 1st,
one whole month) behind.

Users: Wolt fee rows (agents/bilboy.py sync_wolt_fees_window) and the
זיכיונות fixed-expense capture (agents/zikyonot_fixed.py). The goods sync
moves onto this same helper once migration 046 is on prod (see the TODO in
agents/bilboy.py run_bilboy).
"""
import calendar
import os
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

IL_TZ = ZoneInfo('Asia/Jerusalem')
PREV_MONTH_DAYS_ENV = 'BILBOY_PREV_MONTH_DAYS'
DEFAULT_PREV_MONTH_DAYS = 7


def il_today(now: datetime = None) -> date:
    """Israel calendar date for `now` (default: the current instant). A naive
    `now` is treated as UTC (what the server clock gives)."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now.astimezone(IL_TZ).date()


def prev_month_days() -> int:
    """BILBOY_PREV_MONTH_DAYS, falling back to 7 on a missing/garbled value."""
    raw = os.environ.get(PREV_MONTH_DAYS_ENV, '')
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_PREV_MONTH_DAYS
    return max(n, 0)


def month_str(d: date) -> str:
    return f'{d.year:04d}-{d.month:02d}'


def prev_month(month: str) -> str:
    y, m = map(int, month.split('-'))
    return f'{y - 1:04d}-12' if m == 1 else f'{y:04d}-{m - 1:02d}'


def month_bounds(month: str) -> tuple:
    """('YYYY-MM-01', 'YYYY-MM-<last>') — always the FULL month, so a past
    month's re-read never truncates at today's day-of-month."""
    y, m = map(int, month.split('-'))
    return f'{month}-01', f'{month}-{calendar.monthrange(y, m)[1]:02d}'


def months_to_sync(today_il: date = None, prev_days: int = None) -> list:
    """['YYYY-MM', ...] oldest first: [prev, current] on days 1..prev_days,
    else [current]. today_il defaults to il_today()."""
    today_il = today_il or il_today()
    prev_days = prev_month_days() if prev_days is None else prev_days
    cur = month_str(today_il)
    if today_il.day <= prev_days:
        return [prev_month(cur), cur]
    return [cur]

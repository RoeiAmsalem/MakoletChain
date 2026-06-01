"""Hourly sales data-health monitor — read-only checks, never mutates DB."""

import logging
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)


def _ok_from_status(status: str) -> bool:
    """ok is True iff the check passed or cannot be evaluated yet.
    Green = passed. Pending = cannot yet evaluate (not a failure).
    Amber / Red = attention needed.
    """
    return status in ('green', 'pending')

IL_TZ = ZoneInfo('Asia/Jerusalem')

# ── Tunable thresholds ──────────────────────────────────────
HEARTBEAT_MAX_AGE_MIN = 30           # max minutes since last write during store hours
COVERAGE_GREEN = 16                  # hours 7-22 inclusive
COVERAGE_AMBER = 14                  # amber threshold
RECONCILIATION_AMBER_PCT = 5.0       # delta % for amber
RECONCILIATION_RED_PCT = 10.0        # delta % for red
SPIKE_THRESHOLD = 5000               # hourly amount that triggers spike alert
AMAZON_STALE_DAYS = 3                # days without delivery before amber
AMAZON_BRANCH_ID = 126

# Store hours (Israel time)
STORE_OPEN_HOUR = 6
STORE_OPEN_MIN = 30
STORE_CLOSE_HOUR = 23
STORE_CLOSE_MIN = 0


def _is_store_hours(now: datetime) -> bool:
    """Check if current time is within store operating hours."""
    open_time = now.replace(hour=STORE_OPEN_HOUR, minute=STORE_OPEN_MIN, second=0, microsecond=0)
    close_time = now.replace(hour=STORE_CLOSE_HOUR, minute=STORE_CLOSE_MIN, second=0, microsecond=0)
    return open_time <= now <= close_time


def check_heartbeat(branch_id: int, conn: sqlite3.Connection) -> dict:
    """Did a row appear in hourly_sales in the last 30 min during store hours?"""
    now = datetime.now(IL_TZ)

    if not _is_store_hours(now):
        status = 'green'
        return {
            'ok': _ok_from_status(status), 'status': status,
            'last_write_at': None,
            'message': 'מחוץ לשעות פעילות'
        }

    today = now.date().isoformat()
    row = conn.execute(
        '''SELECT MAX(date || 'T' || printf('%02d', hour) || ':00') as last_ts
           FROM hourly_sales WHERE branch_id = ? AND date = ?''',
        (branch_id, today)
    ).fetchone()

    if not row or not row['last_ts']:
        status = 'red'
        return {
            'ok': _ok_from_status(status), 'status': status,
            'last_write_at': None,
            'message': 'אין נתונים היום'
        }

    # Approximate last write time from the hour bucket
    last_hour = conn.execute(
        'SELECT MAX(hour) as h FROM hourly_sales WHERE branch_id = ? AND date = ?',
        (branch_id, today)
    ).fetchone()['h']

    # The last write was approximately at the end of that hour bucket
    last_write = now.replace(hour=last_hour, minute=55, second=0, microsecond=0)
    if last_write > now:
        last_write = last_write.replace(minute=0)

    age_min = (now - last_write).total_seconds() / 60

    if age_min <= HEARTBEAT_MAX_AGE_MIN:
        status = 'green'
    elif age_min <= HEARTBEAT_MAX_AGE_MIN * 2:
        status = 'amber'
    else:
        status = 'red'

    message = f'עדכון אחרון לפני {int(age_min)} דקות'
    if status == 'red':
        message += ' — ייתכן תקלה'

    return {
        'ok': _ok_from_status(status), 'status': status,
        'last_write_at': last_write.isoformat(),
        'message': message
    }


def check_hour_coverage(branch_id: int, date: str, conn: sqlite3.Connection) -> dict:
    """For a given date, how many of hours 7-22 have amount > 0?"""
    now = datetime.now(IL_TZ)
    target_date = datetime.strptime(date, '%Y-%m-%d').date()

    rows = conn.execute(
        '''SELECT hour FROM hourly_sales
           WHERE branch_id = ? AND date = ? AND hour BETWEEN 7 AND 22 AND amount > 0''',
        (branch_id, date)
    ).fetchall()

    covered = len(rows)
    covered_hours = sorted(r['hour'] for r in rows)
    all_hours = set(range(7, 23))
    missing = sorted(all_hours - set(covered_hours))

    # Only judge coverage after store closes (23:30)
    is_today = target_date == now.date()
    after_close = now.hour >= 23 and now.minute >= 30

    if is_today and not after_close:
        # Day still in progress — don't flag
        expected = now.hour - 6  # rough expected coverage
        status = 'green'
        message = f'{covered} שעות עד כה (היום עוד לא נגמר)'
    elif covered >= COVERAGE_GREEN:
        status = 'green'
        message = f'{covered}/{COVERAGE_GREEN} שעות — כיסוי מלא'
    elif covered >= COVERAGE_AMBER:
        status = 'amber'
        message = f'{covered}/{COVERAGE_GREEN} שעות — חסרות: {", ".join(str(h) for h in missing[:4])}'
    else:
        status = 'red'
        message = f'{covered}/{COVERAGE_GREEN} שעות — כיסוי חלקי'

    return {
        'ok': _ok_from_status(status), 'status': status,
        'covered': covered, 'total': COVERAGE_GREEN,
        'missing_hours': missing,
        'message': message
    }


def check_daily_reconciliation(branch_id: int, date: str, conn: sqlite3.Connection) -> dict:
    """Compare sum(hourly_sales) vs daily_sales for a given date."""
    hourly_row = conn.execute(
        'SELECT SUM(amount) as total FROM hourly_sales WHERE branch_id = ? AND date = ?',
        (branch_id, date)
    ).fetchone()
    hourly_total = float(hourly_row['total'] or 0)

    daily_row = conn.execute(
        'SELECT amount, source FROM daily_sales WHERE branch_id = ? AND date = ?',
        (branch_id, date)
    ).fetchone()

    if not daily_row:
        return {
            'ok': _ok_from_status('pending'), 'status': 'pending',
            'hourly_total': round(hourly_total, 2),
            'daily_total': None,
            'delta_pct': None,
            'message': 'Z-report טרם התקבל'
        }

    daily_total = float(daily_row['amount'] or 0)
    if daily_total == 0:
        return {
            'ok': _ok_from_status('pending'), 'status': 'pending',
            'hourly_total': round(hourly_total, 2),
            'daily_total': 0,
            'delta_pct': None,
            'message': 'סכום יומי 0 — ממתין לנתונים'
        }

    delta_pct = abs(hourly_total - daily_total) / daily_total * 100

    if delta_pct < RECONCILIATION_AMBER_PCT:
        status = 'green'
    elif delta_pct < RECONCILIATION_RED_PCT:
        status = 'amber'
    else:
        status = 'red'

    return {
        'ok': _ok_from_status(status), 'status': status,
        'hourly_total': round(hourly_total, 2),
        'daily_total': round(daily_total, 2),
        'delta_pct': round(delta_pct, 1),
        'message': f'Δ {delta_pct:.1f}% (שעתי: ₪{hourly_total:,.0f} / יומי: ₪{daily_total:,.0f})'
    }


def check_suspicious_spikes(branch_id: int, date: str, conn: sqlite3.Connection) -> list:
    """Flag any hourly_sales row with amount > threshold in hours 7-22."""
    rows = conn.execute(
        '''SELECT hour, amount, transactions FROM hourly_sales
           WHERE branch_id = ? AND date = ? AND hour BETWEEN 7 AND 22 AND amount > ?''',
        (branch_id, date, SPIKE_THRESHOLD)
    ).fetchall()

    return [{
        'hour': r['hour'],
        'amount': round(float(r['amount']), 2),
        'transactions': int(r['transactions'] or 0),
    } for r in rows]


def check_amazon_activity(branch_id: int, conn: sqlite3.Connection) -> dict:
    """Track Amazon delivery activity for branch 126."""
    if branch_id != AMAZON_BRANCH_ID:
        return {'ok': _ok_from_status('green'), 'status': 'green', 'message': 'לא רלוונטי לסניף זה', 'skipped': True}

    now = datetime.now(IL_TZ)
    row = conn.execute(
        '''SELECT date, SUM(amount) as total
           FROM hourly_sales
           WHERE branch_id = ? AND hour <= 6 AND amount > 0
           GROUP BY date
           HAVING total >= 400
           ORDER BY date DESC LIMIT 1''',
        (branch_id,)
    ).fetchone()

    if not row:
        return {
            'ok': _ok_from_status('amber'), 'status': 'amber',
            'last_delivery_date': None,
            'last_delivery_amount': None,
            'days_since': None,
            'message': 'אין משלוחים מתועדים עדיין'
        }

    last_date = datetime.strptime(row['date'], '%Y-%m-%d').date()
    days_since = (now.date() - last_date).days
    amount = round(float(row['total']), 2)

    if days_since <= AMAZON_STALE_DAYS:
        status = 'green'
    else:
        status = 'amber'

    return {
        'ok': _ok_from_status(status), 'status': status,
        'last_delivery_date': row['date'],
        'last_delivery_amount': amount,
        'days_since': days_since,
        'message': f'משלוח אחרון: {row["date"]} (₪{amount:,.0f}) — לפני {days_since} ימים'
    }


def run_all_checks(branch_id: int, date: str, conn: sqlite3.Connection) -> dict:
    """Run all health checks and return combined result."""
    conn.row_factory = sqlite3.Row

    checks = {
        'heartbeat': check_heartbeat(branch_id, conn),
        'hour_coverage': check_hour_coverage(branch_id, date, conn),
        'daily_reconciliation': check_daily_reconciliation(branch_id, date, conn),
        'suspicious_spikes': check_suspicious_spikes(branch_id, date, conn),
    }

    if branch_id == AMAZON_BRANCH_ID:
        checks['amazon_activity'] = check_amazon_activity(branch_id, conn)

    # Sanity: each check's ok flag should match its status
    for name, val in checks.items():
        if isinstance(val, dict) and 'status' in val and 'ok' in val:
            assert val['ok'] == _ok_from_status(val['status']), \
                f"Inconsistent ok/status in check {name}: ok={val['ok']} status={val['status']}"

    # Overall status = worst of individual statuses
    priority = {'red': 3, 'amber': 2, 'pending': 1, 'green': 0}
    worst = 'green'
    for key, val in checks.items():
        if isinstance(val, list):
            if val:  # spikes found
                worst = max(worst, 'amber', key=lambda s: priority.get(s, 0))
        elif priority.get(val.get('status', 'green'), 0) > priority.get(worst, 0):
            worst = val['status']

    return {
        'branch_id': branch_id,
        'date': date,
        'checks': checks,
        'overall_status': worst,
    }

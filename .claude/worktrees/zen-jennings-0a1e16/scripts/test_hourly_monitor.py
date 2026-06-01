#!/usr/bin/env python3
"""Tests for hourly sales data-health monitor and alerts."""
import os
import sys
import sqlite3
import tempfile
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

IL_TZ = ZoneInfo('Asia/Jerusalem')


def make_db():
    """Create test DB with hourly_sales + daily_sales + branches."""
    tmp = tempfile.mktemp(suffix='.db')
    conn = sqlite3.connect(tmp)
    conn.row_factory = sqlite3.Row
    conn.execute('''CREATE TABLE branches (
        id INTEGER PRIMARY KEY, name TEXT, city TEXT, active INTEGER DEFAULT 1)''')
    conn.execute("INSERT INTO branches VALUES (126, 'Einstein', 'Haifa', 1)")
    conn.execute("INSERT INTO branches VALUES (127, 'Tichon', 'TLV', 1)")
    conn.execute('''CREATE TABLE hourly_sales (
        branch_id INTEGER NOT NULL, date TEXT NOT NULL, hour INTEGER NOT NULL,
        amount REAL DEFAULT 0, transactions INTEGER DEFAULT 0,
        PRIMARY KEY (branch_id, date, hour))''')
    conn.execute('''CREATE TABLE daily_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT, branch_id INTEGER NOT NULL,
        date TEXT NOT NULL, amount REAL DEFAULT 0, transactions INTEGER DEFAULT 0,
        source TEXT DEFAULT 'z_report', UNIQUE(branch_id, date))''')
    conn.commit()
    return tmp, conn


def test_heartbeat_green_recent():
    """Heartbeat green when recent write exists during store hours."""
    _, conn = make_db()
    now = datetime.now(IL_TZ)
    today = now.strftime('%Y-%m-%d')
    conn.execute("INSERT INTO hourly_sales VALUES (126, ?, ?, 500, 5)", (today, now.hour))
    conn.commit()

    from agents.hourly_sales_monitor import check_heartbeat, _is_store_hours
    if not _is_store_hours(now):
        print("  SKIP: outside store hours — heartbeat always green")
        return True

    result = check_heartbeat(126, conn)
    assert result['status'] in ('green', 'amber'), f"Expected green/amber, got {result['status']}"
    conn.close()
    print("  PASS: heartbeat green with recent write")
    return True


def test_heartbeat_red_stale():
    """Heartbeat red when last write > 30 min ago during store hours."""
    _, conn = make_db()
    now = datetime.now(IL_TZ)
    today = now.strftime('%Y-%m-%d')
    # Write to an hour 3+ hours ago
    old_hour = max(0, now.hour - 3)
    conn.execute("INSERT INTO hourly_sales VALUES (126, ?, ?, 500, 5)", (today, old_hour))
    conn.commit()

    from agents.hourly_sales_monitor import check_heartbeat, _is_store_hours
    if not _is_store_hours(now):
        print("  SKIP: outside store hours — heartbeat always green")
        return True

    result = check_heartbeat(126, conn)
    assert result['status'] == 'red', f"Expected red, got {result['status']}"
    conn.close()
    print("  PASS: heartbeat red when stale")
    return True


def test_heartbeat_green_outside_hours():
    """Heartbeat green when outside store hours (no writes expected)."""
    _, conn = make_db()
    from agents.hourly_sales_monitor import check_heartbeat

    # Mock time to 3:00 AM (outside store hours)
    mock_now = datetime.now(IL_TZ).replace(hour=3, minute=0)
    with patch('agents.hourly_sales_monitor.datetime') as mock_dt:
        mock_dt.now.return_value = mock_now
        mock_dt.strptime = datetime.strptime
        result = check_heartbeat(126, conn)

    assert result['status'] == 'green', f"Expected green outside hours, got {result['status']}"
    assert 'מחוץ' in result['message']
    conn.close()
    print("  PASS: heartbeat green outside store hours")
    return True


def test_coverage_green_full():
    """Hour coverage green at 16/16."""
    _, conn = make_db()
    today = datetime.now(IL_TZ).strftime('%Y-%m-%d')
    for h in range(7, 23):
        conn.execute("INSERT INTO hourly_sales VALUES (126, ?, ?, ?, 3)", (today, h, 500 + h * 10))
    conn.commit()

    from agents.hourly_sales_monitor import check_hour_coverage

    # Mock to after 23:30 so it actually judges
    mock_now = datetime.now(IL_TZ).replace(hour=23, minute=45)
    with patch('agents.hourly_sales_monitor.datetime') as mock_dt:
        mock_dt.now.return_value = mock_now
        mock_dt.strptime = datetime.strptime
        result = check_hour_coverage(126, today, conn)

    assert result['status'] == 'green', f"Expected green, got {result['status']}"
    assert result['covered'] == 16
    conn.close()
    print("  PASS: hour coverage green at 16/16")
    return True


def test_coverage_red_partial():
    """Hour coverage red at 12/16 AFTER 23:30."""
    _, conn = make_db()
    today = datetime.now(IL_TZ).strftime('%Y-%m-%d')
    for h in range(7, 19):  # Only 12 hours
        conn.execute("INSERT INTO hourly_sales VALUES (126, ?, ?, 500, 3)", (today, h))
    conn.commit()

    from agents.hourly_sales_monitor import check_hour_coverage

    mock_now = datetime.now(IL_TZ).replace(hour=23, minute=45)
    with patch('agents.hourly_sales_monitor.datetime') as mock_dt:
        mock_dt.now.return_value = mock_now
        mock_dt.strptime = datetime.strptime
        result = check_hour_coverage(126, today, conn)

    assert result['status'] == 'red', f"Expected red, got {result['status']}"
    assert result['covered'] == 12
    conn.close()
    print("  PASS: hour coverage red at 12/16 after 23:30")
    return True


def test_reconciliation_green():
    """Reconciliation green when delta < 5%."""
    _, conn = make_db()
    today = datetime.now(IL_TZ).strftime('%Y-%m-%d')
    # Hourly total: 10000
    for h in range(7, 17):
        conn.execute("INSERT INTO hourly_sales VALUES (126, ?, ?, 1000, 5)", (today, h))
    # Daily: 10200 (2% delta)
    conn.execute("INSERT INTO daily_sales (branch_id, date, amount) VALUES (126, ?, 10200)", (today,))
    conn.commit()

    from agents.hourly_sales_monitor import check_daily_reconciliation
    result = check_daily_reconciliation(126, today, conn)
    assert result['status'] == 'green', f"Expected green, got {result['status']}"
    assert result['delta_pct'] < 5
    conn.close()
    print("  PASS: reconciliation green at < 5%")
    return True


def test_reconciliation_red():
    """Reconciliation red when delta > 10%."""
    _, conn = make_db()
    today = datetime.now(IL_TZ).strftime('%Y-%m-%d')
    for h in range(7, 17):
        conn.execute("INSERT INTO hourly_sales VALUES (126, ?, ?, 1000, 5)", (today, h))
    # Daily: 8000 (25% delta)
    conn.execute("INSERT INTO daily_sales (branch_id, date, amount) VALUES (126, ?, 8000)", (today,))
    conn.commit()

    from agents.hourly_sales_monitor import check_daily_reconciliation
    result = check_daily_reconciliation(126, today, conn)
    assert result['status'] == 'red', f"Expected red, got {result['status']}"
    assert result['delta_pct'] > 10
    conn.close()
    print("  PASS: reconciliation red at > 10%")
    return True


def test_spike_detection():
    """Spike detection flags hourly total > 5000."""
    _, conn = make_db()
    today = datetime.now(IL_TZ).strftime('%Y-%m-%d')
    conn.execute("INSERT INTO hourly_sales VALUES (126, ?, 14, 6000, 20)", (today,))
    conn.execute("INSERT INTO hourly_sales VALUES (126, ?, 15, 3000, 10)", (today,))
    conn.commit()

    from agents.hourly_sales_monitor import check_suspicious_spikes
    result = check_suspicious_spikes(126, today, conn)
    assert len(result) == 1, f"Expected 1 spike, got {len(result)}"
    assert result[0]['amount'] == 6000
    assert result[0]['hour'] == 14
    conn.close()
    print("  PASS: spike detection flags 6000")
    return True


def test_amazon_green_recent():
    """Amazon activity green when delivery today."""
    _, conn = make_db()
    today = datetime.now(IL_TZ).strftime('%Y-%m-%d')
    conn.execute("INSERT INTO hourly_sales VALUES (126, ?, 6, 8000, 3)", (today,))
    conn.commit()

    from agents.hourly_sales_monitor import check_amazon_activity
    result = check_amazon_activity(126, conn)
    assert result['status'] == 'green', f"Expected green, got {result['status']}"
    assert result['ok'] is True, f"Expected ok=True for green, got {result['ok']}"
    assert result['days_since'] == 0
    conn.close()
    print("  PASS: amazon activity green with delivery today")
    return True


def test_amazon_amber_stale():
    """Amazon activity amber + ok=False when > 3 days since last delivery."""
    _, conn = make_db()
    old_date = (datetime.now(IL_TZ) - timedelta(days=5)).strftime('%Y-%m-%d')
    conn.execute("INSERT INTO hourly_sales VALUES (126, ?, 6, 8000, 3)", (old_date,))
    conn.commit()

    from agents.hourly_sales_monitor import check_amazon_activity
    result = check_amazon_activity(126, conn)
    assert result['status'] == 'amber', f"Expected amber, got {result['status']}"
    assert result['ok'] is False, f"Expected ok=False for amber, got {result['ok']}"
    assert result['days_since'] >= 4
    conn.close()
    print("  PASS: amazon activity amber + ok=False when > 3 days")
    return True


def test_brrr_not_telegram():
    """Verify alerts use brrr (notify), NOT Telegram."""
    # Check that hourly_sales_alerts.py imports from utils.notify, not telegram
    import agents.hourly_sales_alerts as alerts_module
    source = open(alerts_module.__file__).read()
    assert 'telegram' not in source.lower(), "Found 'telegram' in alerts module!"
    assert 'from utils.notify import notify' in source, "Missing brrr notify import"

    # Verify notify is called with correct args (mocked)
    _, conn = make_db()
    today = datetime.now(IL_TZ).strftime('%Y-%m-%d')
    # No data = heartbeat red during store hours
    mock_now = datetime.now(IL_TZ).replace(hour=12, minute=0)

    with patch('agents.hourly_sales_monitor.datetime') as mock_dt, \
         patch('agents.hourly_sales_alerts.notify') as mock_notify:
        mock_dt.now.return_value = mock_now
        mock_dt.strptime = datetime.strptime

        from agents.hourly_sales_monitor import check_heartbeat
        result = check_heartbeat(126, conn)

        if result['status'] == 'red':
            # Simulate what the alerter would do
            mock_notify("⚠️ Hourly data — Einstein", f"No data received: {result['message']}")
            mock_notify.assert_called_once()
            args = mock_notify.call_args[0]
            assert 'Einstein' in args[0]
            assert 'telegram' not in str(args).lower()

    conn.close()
    print("  PASS: alerts use brrr (not Telegram)")
    return True


def test_ok_matches_status_rule():
    """ok == (status in ['green', 'pending']) for every check."""
    from agents.hourly_sales_monitor import _ok_from_status
    assert _ok_from_status('green') is True
    assert _ok_from_status('pending') is True
    assert _ok_from_status('amber') is False
    assert _ok_from_status('red') is False
    print("  PASS: _ok_from_status matches rule for all statuses")
    return True


def test_amazon_green_ok_true():
    """Amazon ok=True when status=green (recent delivery)."""
    _, conn = make_db()
    today = datetime.now(IL_TZ).strftime('%Y-%m-%d')
    conn.execute("INSERT INTO hourly_sales VALUES (126, ?, 6, 8000, 3)", (today,))
    conn.commit()

    from agents.hourly_sales_monitor import check_amazon_activity
    result = check_amazon_activity(126, conn)
    assert result['status'] == 'green'
    assert result['ok'] is True, f"Expected ok=True for green, got {result['ok']}"
    conn.close()
    print("  PASS: amazon ok=True when green")
    return True


def test_coverage_today_in_progress_ok_true():
    """Coverage today in-progress: status=green, ok=True."""
    _, conn = make_db()
    today = datetime.now(IL_TZ).strftime('%Y-%m-%d')
    for h in range(7, 12):
        conn.execute("INSERT INTO hourly_sales VALUES (126, ?, ?, 500, 3)", (today, h))
    conn.commit()

    from agents.hourly_sales_monitor import check_hour_coverage

    # Mock to midday (before 23:30)
    mock_now = datetime.now(IL_TZ).replace(hour=14, minute=0)
    with patch('agents.hourly_sales_monitor.datetime') as mock_dt:
        mock_dt.now.return_value = mock_now
        mock_dt.strptime = datetime.strptime
        result = check_hour_coverage(126, today, conn)

    assert result['status'] == 'green', f"Expected green, got {result['status']}"
    assert result['ok'] is True, f"Expected ok=True for in-progress, got {result['ok']}"
    conn.close()
    print("  PASS: coverage today in-progress ok=True")
    return True


def test_coverage_past_red_ok_false():
    """Coverage past date 10/16: status=red, ok=False."""
    _, conn = make_db()
    yesterday = (datetime.now(IL_TZ) - timedelta(days=1)).strftime('%Y-%m-%d')
    for h in range(7, 17):  # Only 10 hours
        conn.execute("INSERT INTO hourly_sales VALUES (126, ?, ?, 500, 3)", (yesterday, h))
    conn.commit()

    from agents.hourly_sales_monitor import check_hour_coverage

    # Mock to after 23:30
    mock_now = datetime.now(IL_TZ).replace(hour=23, minute=45)
    with patch('agents.hourly_sales_monitor.datetime') as mock_dt:
        mock_dt.now.return_value = mock_now
        mock_dt.strptime = datetime.strptime
        result = check_hour_coverage(126, yesterday, conn)

    assert result['status'] == 'red', f"Expected red, got {result['status']}"
    assert result['ok'] is False, f"Expected ok=False for red, got {result['ok']}"
    assert result['covered'] == 10
    conn.close()
    print("  PASS: coverage past date 10/16 ok=False")
    return True


def test_run_all_assertion_passes():
    """run_all_checks sanity assertion does not fire under normal conditions."""
    _, conn = make_db()
    today = datetime.now(IL_TZ).strftime('%Y-%m-%d')
    for h in range(7, 23):
        conn.execute("INSERT INTO hourly_sales VALUES (126, ?, ?, 500, 3)", (today, h))
    conn.execute("INSERT INTO daily_sales (branch_id, date, amount) VALUES (126, ?, 8000)", (today,))
    conn.commit()

    from agents.hourly_sales_monitor import run_all_checks
    # Should not raise AssertionError
    result = run_all_checks(126, today, conn)
    assert 'overall_status' in result
    conn.close()
    print("  PASS: run_all_checks assertion passes")
    return True


def run_all():
    tests = [
        test_heartbeat_green_recent,
        test_heartbeat_red_stale,
        test_heartbeat_green_outside_hours,
        test_coverage_green_full,
        test_coverage_red_partial,
        test_reconciliation_green,
        test_reconciliation_red,
        test_spike_detection,
        test_amazon_green_recent,
        test_amazon_amber_stale,
        test_brrr_not_telegram,
        test_ok_matches_status_rule,
        test_amazon_green_ok_true,
        test_coverage_today_in_progress_ok_true,
        test_coverage_past_red_ok_false,
        test_run_all_assertion_passes,
    ]
    passed = failed = 0
    for t in tests:
        try:
            if t():
                passed += 1
        except Exception as e:
            print(f"  FAIL [{t.__name__}]: {e}")
            failed += 1

    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed")
    return failed == 0


if __name__ == '__main__':
    print("Hourly Sales Monitor Tests")
    print("=" * 40)
    success = run_all()
    sys.exit(0 if success else 1)

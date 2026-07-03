"""Tests for the Z-agent alerting fixes (s1/s2/s3, July 2026).

s1 — backfill passes re-probe closed-day sentinels (9009 2026-07-01: the
     store closed Z 1728 after the 02:00 IL primary run and the old
     sentinel-blocks-backfill design turned that into a permanent miss).
s2 — check_missing_z: post-backfill completeness check with per-branch
     expected-closed weekday exclusion (Saturday varies by branch).
s3 — alert_run_failures: hard failures brrr once per branch/day across
     separate cron processes (persistent dedup via z_alert_log).

All offline: no HTTP, in-memory SQLite.
"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import agents.aviv_z_report as zr


WEDNESDAY = '2026-07-01'
WED_LOOKBACKS = ['2026-06-24', '2026-06-17', '2026-06-10']
SATURDAY = '2026-06-27'
SAT_LOOKBACKS = ['2026-06-20', '2026-06-13', '2026-06-06']


@pytest.fixture
def db():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE branches (
            id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 1,
            aviv_user_id TEXT, aviv_password TEXT, aviv_branch_id INTEGER
        );
        CREATE TABLE daily_sales (
            branch_id INTEGER, date TEXT, amount REAL,
            transactions INTEGER DEFAULT 0, source TEXT, fetched_at TEXT,
            UNIQUE(branch_id, date)
        );
        CREATE TABLE z_report_902 (
            branch_id INTEGER NOT NULL, date TEXT NOT NULL,
            z_number INTEGER, amount REAL, transactions INTEGER,
            avg_per_txn REAL, payment_breakdown TEXT,
            fetched_at TEXT DEFAULT (datetime('now')),
            trigger_type TEXT, auth_source TEXT,
            UNIQUE(branch_id, date)
        );
        CREATE TABLE z_alert_log (
            branch_id INTEGER NOT NULL, date TEXT NOT NULL, kind TEXT NOT NULL,
            sent_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(branch_id, date, kind)
        );
    ''')
    conn.execute(
        "INSERT INTO branches (id, name, aviv_user_id, aviv_branch_id) "
        "VALUES (126, 'איינשטיין', 'einstein_user', 3)")
    conn.commit()
    return conn


@pytest.fixture
def sent(monkeypatch):
    """Capture brrr sends as (title, message) tuples."""
    calls = []
    monkeypatch.setattr(
        zr, 'notify',
        lambda title, message, **kw: calls.append((title, message)))
    return calls


@pytest.fixture(autouse=True)
def _per_branch_mode(monkeypatch):
    """check_missing_z iterates via USE_CHAIN_AUTH; pin to per-branch mode so
    the fixture's aviv_user_id drives the branch universe."""
    monkeypatch.setattr(zr, 'USE_CHAIN_AUTH', False)


def _real_z(conn, branch_id, d, z=100, amount=1000.0):
    conn.execute(
        'INSERT INTO z_report_902 (branch_id, date, z_number, amount) '
        'VALUES (?, ?, ?, ?)', (branch_id, d, z, amount))
    conn.commit()


def _sentinel(conn, branch_id, d):
    zr.record_closed_day(conn, branch_id, d)


# ── s1: backfill re-probes sentinels ────────────────────────────────────────

def test_backfill_reprobes_sentinel(db):
    _sentinel(db, 126, WEDNESDAY)
    assert zr._branch_ids_for_date(db, WEDNESDAY, missing_only=True) == [126]


def test_backfill_skips_real_z(db):
    _real_z(db, 126, WEDNESDAY)
    assert zr._branch_ids_for_date(db, WEDNESDAY, missing_only=True) == []


def test_backfill_includes_branch_with_no_row(db):
    assert zr._branch_ids_for_date(db, WEDNESDAY, missing_only=True) == [126]


def test_real_z_overwrites_sentinel(db):
    """The recovery path: sentinel first, real Z later — upsert must win."""
    _sentinel(db, 126, WEDNESDAY)
    zr.upsert_z_report(db, 126, WEDNESDAY, 1728,
                       {'total': 12584.07, 'transactions': 239})
    row = db.execute('SELECT z_number, amount FROM z_report_902 '
                     'WHERE branch_id=126 AND date=?', (WEDNESDAY,)).fetchone()
    assert row['z_number'] == 1728
    assert row['amount'] == 12584.07


# ── s3: give-up alert once per branch/day ───────────────────────────────────

def test_alert_run_failures_fires_once_per_day(db, sent):
    fail = {'ok': False, 'branch_id': 126, 'date': WEDNESDAY,
            'error': 'Z-list fetch transient-give-up after 11 attempts'}
    assert zr.alert_run_failures(db, [fail], WEDNESDAY) == 1
    assert len(sent) == 1
    assert '126' in sent[0][1] and 'transient-give-up' in sent[0][1]
    # Same failure on the next backfill pass (separate process in prod,
    # same z_alert_log) — silent.
    assert zr.alert_run_failures(db, [fail], WEDNESDAY) == 0
    assert len(sent) == 1


def test_alert_run_failures_new_day_fires_again(db, sent):
    fail = {'ok': False, 'branch_id': 126, 'error': '500 Server Error'}
    zr.alert_run_failures(db, [fail], WEDNESDAY)
    zr.alert_run_failures(db, [fail], '2026-07-02')
    assert len(sent) == 2


def test_alert_run_failures_ignores_closed_day_and_ok(db, sent):
    results = [
        {'ok': True, 'branch_id': 126, 'date': WEDNESDAY},
        {'ok': False, 'branch_id': 126, 'date': WEDNESDAY,
         'error': 'no Z for date'},
    ]
    assert zr.alert_run_failures(db, results, WEDNESDAY) == 0
    assert sent == []


def test_run_all_branches_alerts_only_on_auto(db, sent, monkeypatch):
    fail = {'ok': False, 'branch_id': 126, 'date': WEDNESDAY, 'error': 'boom'}
    monkeypatch.setattr(zr, 'run_for_branch', lambda *a, **kw: dict(fail))
    zr.run_all_branches(WEDNESDAY, conn=db, trigger_type='manual')
    assert sent == []
    zr.run_all_branches(WEDNESDAY, conn=db, trigger_type='auto')
    assert len(sent) == 1


# ── s2: missing-Z completeness check ────────────────────────────────────────

def test_missing_z_alerts_open_weekday(db, sent):
    for d in WED_LOOKBACKS:
        _real_z(db, 126, d)
    out = zr.check_missing_z(WEDNESDAY, conn=db)
    assert out['missing'] == [126]
    assert out['alerted'] == [126]
    assert len(sent) == 1
    assert WEDNESDAY in sent[0][0]
    assert '126' in sent[0][1]


def test_missing_z_covered_by_real_z(db, sent):
    _real_z(db, 126, WEDNESDAY)
    out = zr.check_missing_z(WEDNESDAY, conn=db)
    assert out['missing'] == []
    assert sent == []


def test_missing_z_covered_by_gmail_daily_sales(db, sent):
    db.execute("INSERT INTO daily_sales (branch_id, date, amount, source) "
               "VALUES (126, ?, 9999.0, 'z_report')", (WEDNESDAY,))
    db.commit()
    out = zr.check_missing_z(WEDNESDAY, conn=db)
    assert out['missing'] == []
    assert sent == []


def test_missing_z_sentinel_alone_is_not_coverage(db, sent):
    """The 9009 case: a sentinel row must still count as missing."""
    for d in WED_LOOKBACKS:
        _real_z(db, 126, d)
    _sentinel(db, 126, WEDNESDAY)
    out = zr.check_missing_z(WEDNESDAY, conn=db)
    assert out['alerted'] == [126]


def test_missing_z_excludes_saturday_closed_branch(db, sent):
    for d in SAT_LOOKBACKS:
        _sentinel(db, 126, d)
    out = zr.check_missing_z(SATURDAY, conn=db)
    assert out['expected_closed'] == [126]
    assert out['alerted'] == []
    assert sent == []


def test_missing_z_alerts_saturday_trading_branch(db, sent):
    _sentinel(db, 126, SAT_LOOKBACKS[0])
    _real_z(db, 126, SAT_LOOKBACKS[1])  # traded a recent Saturday
    out = zr.check_missing_z(SATURDAY, conn=db)
    assert out['alerted'] == [126]
    assert len(sent) == 1


def test_missing_z_new_branch_without_history_alerts(db, sent):
    """No same-weekday history at all → not silenced (probing gap ≠ closed)."""
    out = zr.check_missing_z(WEDNESDAY, conn=db)
    assert out['expected_closed'] == []
    assert out['alerted'] == [126]


def test_missing_z_deduped_on_rerun(db, sent):
    for d in WED_LOOKBACKS:
        _real_z(db, 126, d)
    first = zr.check_missing_z(WEDNESDAY, conn=db)
    second = zr.check_missing_z(WEDNESDAY, conn=db)
    assert first['alerted'] == [126]
    assert second['alerted'] == []
    assert second['missing'] == [126]  # still reported, just not re-paged
    assert len(sent) == 1

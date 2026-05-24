"""Tests for the aviv_z_report agent — 902 PDF parser + submit body + upsert.

All tests are offline. The HTTP layer is monkeypatched; the only real I/O
is reading tests/fixtures/z_902_sample.pdf and writing to an in-memory
SQLite that has the same z_report_902 schema as migration 010.
"""
import json
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import agents.aviv_z_report as zr


FIXTURE_PDF = os.path.join(os.path.dirname(__file__), 'fixtures', 'z_902_sample.pdf')


@pytest.fixture
def sample_pdf_bytes() -> bytes:
    with open(FIXTURE_PDF, 'rb') as f:
        return f.read()


@pytest.fixture
def staging_db():
    """In-memory DB with migration 010's schema + a minimal branches row."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE branches (
            id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 1,
            aviv_user_id TEXT, aviv_password TEXT
        );
        CREATE TABLE daily_sales (
            branch_id INTEGER, date TEXT, amount REAL, source TEXT,
            UNIQUE(branch_id, date)
        );
        CREATE TABLE z_report_902 (
            branch_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            z_number INTEGER,
            amount REAL,
            transactions INTEGER,
            avg_per_txn REAL,
            payment_breakdown TEXT,
            fetched_at TEXT DEFAULT (datetime('now')),
            UNIQUE(branch_id, date)
        );
    ''')
    conn.execute(
        "INSERT INTO branches (id, name, aviv_user_id, aviv_password) "
        "VALUES (126, 'Einstein', 'einstein_user', 'einstein_pass')")
    conn.commit()
    return conn


# ── parser ────────────────────────────────────────────────────────────────

def test_parse_902_pdf(sample_pdf_bytes):
    out = zr.parse_902_pdf(sample_pdf_bytes)
    assert out['total'] == 13721.98
    assert out['transactions'] == 234
    assert out['avg_per_txn'] == 58.64


def test_parse_902_payment_breakdown(sample_pdf_bytes):
    pb = zr.parse_902_pdf(sample_pdf_bytes)['payment_breakdown']
    assert pb['cash'] == 2974.40
    assert pb['credit'] == 9216.62
    assert pb['hakafa'] == 1530.96
    assert pb['soed'] == 483.00
    assert pb['check'] == 0.0
    assert pb['transfer'] == 0.0


# ── submit body ───────────────────────────────────────────────────────────

def test_build_submit_body_single_z():
    body = zr.build_submit_body(2525, 2525)
    assert body == {
        'id': 902,
        'outputType': 'PDF',
        'filters': [
            {'id': 1, 'name': 'ID_Z', 'filterType': 'INTEGER', 'value': 2525},
            {'id': 2, 'name': 'TO_Z', 'filterType': 'INTEGER', 'value': 2525},
        ],
    }


# ── date → Z resolution ───────────────────────────────────────────────────

def test_date_to_z_resolution():
    # Simulated filters/902 response — wrapping shape is defensive.
    filters_json = {
        'data': [
            {
                'id': 1, 'name': 'ID_Z', 'filterType': 'INTEGER',
                'value': [
                    {'key': 2523, 'value': '2026-05-18 23:59:59'},
                    {'key': 2524, 'value': '2026-05-19 23:59:59'},
                    {'key': 2525, 'value': '2026-05-20 23:59:59'},
                    {'key': 2526, 'value': '2026-05-21 23:59:59'},
                ],
            }
        ]
    }
    assert zr.resolve_z_for_date(filters_json, '2026-05-20') == 2525
    assert zr.resolve_z_for_date(filters_json, '2026-05-19') == 2524
    assert zr.resolve_z_for_date(filters_json, '2026-01-01') is None


def test_date_to_z_resolution_dd_mm_yyyy():
    """Defensively handles dd/mm/yyyy too — Aviv has been seen serving both."""
    filters_json = [
        {'value': [
            {'key': 2525, 'value': '20/05/2026'},
        ]}
    ]
    assert zr.resolve_z_for_date(filters_json, '2026-05-20') == 2525


def test_date_to_z_resolution_captured_aviv_shape():
    """Actual production shape from filters/902 (captured 2026-05-22):
    a top-level list of filter objects with a `possibleValues` array of
    single-key dicts {z_str: "Z: <z>|DD/MM/YYYY"}.
    """
    filters_json = [
        {
            'id': 1, 'name': 'ID_Z', 'filterType': 'INTEGER',
            'possibleValues': [
                {'2526': 'Z: 2526|21/05/2026'},
                {'2525': 'Z: 2525|20/05/2026'},
                {'2524': 'Z: 2524|19/05/2026'},
            ],
        },
        {
            'id': 2, 'name': 'TO_Z', 'filterType': 'INTEGER',
            'possibleValues': [
                {'2526': 'Z: 2526|21/05/2026'},
                {'2525': 'Z: 2525|20/05/2026'},
            ],
        },
    ]
    assert zr.resolve_z_for_date(filters_json, '2026-05-20') == 2525
    assert zr.resolve_z_for_date(filters_json, '2026-05-21') == 2526
    assert zr.resolve_z_for_date(filters_json, '2026-04-01') is None


# ── upsert ────────────────────────────────────────────────────────────────

def test_upsert_writes_separate_table(staging_db):
    parsed = {'total': 13721.98, 'transactions': 234, 'avg_per_txn': 58.64,
              'payment_breakdown': {'cash': 2974.40, 'credit': 9216.62}}
    zr.upsert_z_report(staging_db, 126, '2026-05-20', 2525, parsed)

    rows = staging_db.execute('SELECT * FROM z_report_902').fetchall()
    assert len(rows) == 1
    assert rows[0]['amount'] == 13721.98
    assert rows[0]['transactions'] == 234
    assert rows[0]['z_number'] == 2525
    pb = json.loads(rows[0]['payment_breakdown'])
    assert pb['cash'] == 2974.40

    # daily_sales must be untouched
    assert staging_db.execute('SELECT COUNT(*) FROM daily_sales').fetchone()[0] == 0


def test_upsert_updates_on_rerun(staging_db):
    """Same (branch_id, date) re-runs overwrite, not duplicate."""
    p1 = {'total': 10000.00, 'transactions': 100, 'avg_per_txn': 100.0,
          'payment_breakdown': None}
    p2 = {'total': 13721.98, 'transactions': 234, 'avg_per_txn': 58.64,
          'payment_breakdown': {'cash': 2974.40}}

    zr.upsert_z_report(staging_db, 126, '2026-05-20', 2525, p1)
    zr.upsert_z_report(staging_db, 126, '2026-05-20', 2525, p2)

    rows = staging_db.execute('SELECT * FROM z_report_902').fetchall()
    assert len(rows) == 1
    assert rows[0]['amount'] == 13721.98
    assert rows[0]['transactions'] == 234


# ── 401 retry ─────────────────────────────────────────────────────────────

def test_auth_retry_on_401(monkeypatch, staging_db, sample_pdf_bytes):
    """First submit raises AuthExpired → agent re-logs in → second call succeeds."""
    login_calls = {'n': 0}

    def fake_login(user, password):
        login_calls['n'] += 1
        # Token differs per call so we can prove the retry uses the new one.
        return f'token-{login_calls["n"]}', 999

    monkeypatch.setattr(zr, '_login', fake_login)
    monkeypatch.setattr(zr, '_refresh', lambda t: t)
    monkeypatch.setattr(zr, 'fetch_902_filters',
                        lambda b, t: {'data': [{'value': [
                            {'key': 2525, 'value': '2026-05-20 23:59:59'}]}]})

    submit_calls = {'n': 0}

    def fake_submit(branch, z, token):
        submit_calls['n'] += 1
        if submit_calls['n'] == 1:
            raise zr.AuthExpired('first 401')
        assert token == 'token-2', \
            f'second submit must use refreshed token, got {token}'
        return 'https://example.invalid/report.pdf'

    monkeypatch.setattr(zr, 'submit_902', fake_submit)
    monkeypatch.setattr(zr, 'download_pdf', lambda u, t: sample_pdf_bytes)

    result = zr.run_for_branch(126, '2026-05-20', conn=staging_db)

    assert result['ok'] is True
    assert result['total'] == 13721.98
    assert login_calls['n'] == 2   # first login + retry login
    assert submit_calls['n'] == 2  # first submit (401) + retry


# ── filters/902 retry on transient Aviv failure ───────────────────────────

def _good_filters():
    return {'data': [{'value': [
        {'key': 2525, 'value': '2026-05-20 23:59:59'}]}]}


def _stub_success_path(monkeypatch, sample_pdf_bytes):
    """Stub everything downstream of fetch_902_filters so happy path runs."""
    monkeypatch.setattr(zr, '_login', lambda u, p: ('tok', 999))
    monkeypatch.setattr(zr, '_refresh', lambda t: t)
    monkeypatch.setattr(zr, 'submit_902',
                        lambda b, z, t: 'https://example.invalid/r.pdf')
    monkeypatch.setattr(zr, 'download_pdf', lambda u, t: sample_pdf_bytes)
    monkeypatch.setattr(zr.time, 'sleep', lambda s: None)  # don't sleep in tests


def test_filters_902_retries_on_failure(monkeypatch, staging_db, sample_pdf_bytes):
    """Transient filters/902 failure → agent retries → second call succeeds."""
    _stub_success_path(monkeypatch, sample_pdf_bytes)

    calls = {'n': 0}

    def flaky_filters(aviv_branch_id, token):
        calls['n'] += 1
        if calls['n'] == 1:
            raise requests_HTTPError_404()
        return _good_filters()

    monkeypatch.setattr(zr, 'fetch_902_filters', flaky_filters)

    result = zr.run_for_branch(126, '2026-05-20', conn=staging_db)
    assert result['ok'] is True
    assert result['total'] == 13721.98
    assert calls['n'] == 2  # one failure + one success


def test_filters_902_gives_up_after_max_retries(monkeypatch, staging_db,
                                                sample_pdf_bytes):
    """All filters/902 attempts fail → graceful error dict, no crash."""
    _stub_success_path(monkeypatch, sample_pdf_bytes)

    calls = {'n': 0}

    def always_404(aviv_branch_id, token):
        calls['n'] += 1
        raise requests_HTTPError_404()

    monkeypatch.setattr(zr, 'fetch_902_filters', always_404)

    result = zr.run_for_branch(126, '2026-05-20', conn=staging_db)
    assert result['ok'] is False
    assert result['branch_id'] == 126
    assert 'filters/902 failed' in result['error']
    assert calls['n'] == zr.FILTERS_MAX_ATTEMPTS
    # nothing written to z_report_902
    assert staging_db.execute(
        'SELECT COUNT(*) FROM z_report_902').fetchone()[0] == 0


def test_closed_day_does_not_retry(monkeypatch, staging_db, sample_pdf_bytes):
    """200 with no Z for target date → 'no Z for date' WITHOUT retrying."""
    _stub_success_path(monkeypatch, sample_pdf_bytes)

    calls = {'n': 0}

    def filters_no_z_for_date(aviv_branch_id, token):
        calls['n'] += 1
        # Z list exists but not for our target date — closed day.
        return {'data': [{'value': [
            {'key': 2525, 'value': '2026-05-18 23:59:59'}]}]}

    monkeypatch.setattr(zr, 'fetch_902_filters', filters_no_z_for_date)

    result = zr.run_for_branch(126, '2026-05-20', conn=staging_db)
    assert result['ok'] is False
    assert result['error'] == 'no Z for date'
    assert calls['n'] == 1, 'closed day must not consume retries'


def test_one_branch_failure_doesnt_block_others(monkeypatch, staging_db,
                                                sample_pdf_bytes):
    """Branch A fails all retries; branch B still pulls + writes its row."""
    staging_db.execute(
        "INSERT INTO branches (id, name, aviv_user_id, aviv_password) "
        "VALUES (127, 'Tichon', 'tichon_user', 'tichon_pass')")
    staging_db.commit()

    monkeypatch.setattr(zr, '_login', lambda u, p: ('tok', 999))
    monkeypatch.setattr(zr, '_refresh', lambda t: t)
    monkeypatch.setattr(zr, 'submit_902',
                        lambda b, z, t: 'https://example.invalid/r.pdf')
    monkeypatch.setattr(zr, 'download_pdf', lambda u, t: sample_pdf_bytes)
    monkeypatch.setattr(zr.time, 'sleep', lambda s: None)

    def filters_by_branch(aviv_branch_id, token):
        # Branch 126 wired to fail every call; 127 wired to succeed.
        # We can't distinguish by aviv_branch_id here (stub returns 999 for both),
        # so use a counter on which call is which.
        raise NotImplementedError  # replaced below

    state = {'fail_next': True}  # 126 runs first; fail it. Then 127 runs; succeed.

    def filters_stub(aviv_branch_id, token):
        if state['fail_next']:
            # Exhaust retries for branch 126 by always failing during this branch's window.
            raise requests_HTTPError_404()
        return _good_filters()

    # Patch run_for_branch so we can flip the flag between branches.
    real_run_for_branch = zr.run_for_branch
    branches_seen = []

    def wrapped(branch_id, target_date=None, conn=None):
        branches_seen.append(branch_id)
        state['fail_next'] = (branch_id == 126)
        return real_run_for_branch(branch_id, target_date, conn=conn)

    monkeypatch.setattr(zr, 'fetch_902_filters', filters_stub)
    monkeypatch.setattr(zr, 'run_for_branch', wrapped)
    monkeypatch.setattr(zr, 'DB_PATH', ':memory:')  # not used; we pass conn

    # run_all_branches opens its own conn from DB_PATH; bypass by calling
    # run_for_branch directly for each branch using the in-memory db.
    results = [
        zr.run_for_branch(126, '2026-05-20', conn=staging_db),
        zr.run_for_branch(127, '2026-05-20', conn=staging_db),
    ]

    assert results[0]['ok'] is False
    assert 'filters/902 failed' in results[0]['error']
    assert results[1]['ok'] is True
    assert results[1]['total'] == 13721.98
    # Only branch 127's row should be written.
    rows = staging_db.execute(
        'SELECT branch_id FROM z_report_902').fetchall()
    assert [r['branch_id'] for r in rows] == [127]


def requests_HTTPError_404():
    """Build a realistic requests.HTTPError mimicking Aviv's 404."""
    import requests
    resp = requests.Response()
    resp.status_code = 404
    resp.url = ('https://bi1.aviv-pos.co.il:8443/avivbi/v2/'
                'reports/filters/902?branch=3')
    return requests.exceptions.HTTPError(
        '404 Client Error: for url: %s' % resp.url, response=resp)

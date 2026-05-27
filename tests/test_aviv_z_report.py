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


@pytest.fixture(autouse=True)
def _isolate_pdf_base(monkeypatch, tmp_path):
    """Redirect 902-agent PDF writes to a per-test tmp dir so happy-path tests
    don't write into <repo>/data/pdfs/. The dedicated preview test re-points
    PDF_BASE to its own root before asserting."""
    monkeypatch.setattr(zr, 'PDF_BASE', str(tmp_path / '_default_pdfs'))


@pytest.fixture(autouse=True)
def _stub_possible_values_empty_by_default(monkeypatch):
    """fetch_902_z_list now tries the possible-values endpoint first. In
    tests we don't want real HTTP traffic, so default it to "no entries"
    which makes the wrapper fall back to fetch_902_filters. Tests that want
    to exercise the possible-values path explicitly override this stub.
    """
    monkeypatch.setattr(zr, 'fetch_902_id_z_possible_values',
                        lambda b, t: [])


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
            branch_id INTEGER, date TEXT, amount REAL,
            transactions INTEGER DEFAULT 0,
            source TEXT,
            fetched_at TEXT,
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
            trigger_type TEXT,
            auth_source TEXT,
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


# Shape captured from the live probe of /reports/filters/902/possible-values
# for branch 1: a flat list of single-key dicts {<z_number_str>: "Z: <z>|DD/MM/YYYY"}.
def _branch_1_possible_values_body():
    return [
        {"3036": "Z: 3036|26/05/2026"},
        {"3035": "Z: 3035|25/05/2026"},
        {"3034": "Z: 3034|24/05/2026"},
    ]


def test_possible_values_endpoint_parses_branch_1_z_3036(monkeypatch):
    """The captured possible-values body for branch 1 must:
       (a) parse via _iter_z_entries with the existing logic, and
       (b) resolve Z 3036 for target_date='2026-05-26'.

    This was the missing piece — branch 1's main filters/902 returns
    possibleValues=null under chain auth, so without this endpoint there's
    no way to find Z 3036."""
    body = _branch_1_possible_values_body()
    entries = zr._iter_z_entries(body)
    assert {'z_number': 3036, 'date': '2026-05-26'} in entries
    # resolve_z_for_date picks the Z that matches the date we ask for.
    assert zr.resolve_z_for_date(body, '2026-05-26') == 3036
    # And it returns None for a date not in the list.
    assert zr.resolve_z_for_date(body, '2026-05-27') is None


def test_fetch_902_z_list_prefers_possible_values_when_populated(monkeypatch):
    """fetch_902_z_list returns the possible-values body when it has entries —
    fetch_902_filters must NOT be called at all in that path."""
    monkeypatch.setattr(zr, 'fetch_902_id_z_possible_values',
                        lambda b, t: _branch_1_possible_values_body())
    def boom(b, t):
        raise AssertionError(
            'fetch_902_filters must not be called when possible-values has entries')
    monkeypatch.setattr(zr, 'fetch_902_filters', boom)

    body = zr.fetch_902_z_list(1, 'TOK')
    assert zr.resolve_z_for_date(body, '2026-05-26') == 3036


def test_fetch_902_z_list_falls_back_when_possible_values_empty(monkeypatch):
    """possible-values returns [] (no entries) → fall back to filters/902."""
    monkeypatch.setattr(zr, 'fetch_902_id_z_possible_values', lambda b, t: [])
    called = {'filters': 0}
    def filters_stub(b, t):
        called['filters'] += 1
        return _good_filters()
    monkeypatch.setattr(zr, 'fetch_902_filters', filters_stub)

    body = zr.fetch_902_z_list(126, 'TOK')
    assert called['filters'] == 1, 'fallback to filters/902 must fire on empty'
    assert zr.resolve_z_for_date(body, '2026-05-20') == 2525


def test_fetch_902_z_list_falls_back_when_possible_values_raises(monkeypatch):
    """Non-200 / transport error on possible-values → fall back, not crash."""
    def boom(b, t):
        raise RuntimeError('simulated 500 from possible-values')
    monkeypatch.setattr(zr, 'fetch_902_id_z_possible_values', boom)
    called = {'filters': 0}
    def filters_stub(b, t):
        called['filters'] += 1
        return _good_filters()
    monkeypatch.setattr(zr, 'fetch_902_filters', filters_stub)

    body = zr.fetch_902_z_list(126, 'TOK')
    assert called['filters'] == 1
    assert zr.resolve_z_for_date(body, '2026-05-20') == 2525


def test_fetch_902_z_list_propagates_auth_expired(monkeypatch):
    """AuthExpired from possible-values must propagate so run_for_branch's
    re-auth retry kicks in (and is not silently swallowed by the fallback)."""
    def auth_expired(b, t):
        raise zr.AuthExpired('possible-values 401')
    monkeypatch.setattr(zr, 'fetch_902_id_z_possible_values', auth_expired)
    # If fallback were reached, this would prove it (we don't want that).
    monkeypatch.setattr(zr, 'fetch_902_filters',
                        lambda b, t: (_ for _ in ()).throw(
                            AssertionError('fallback must not run on 401')))

    with pytest.raises(zr.AuthExpired):
        zr.fetch_902_z_list(1, 'TOK')


def test_run_for_branch_with_chain_token_handles_branch_without_per_store_creds(
        monkeypatch, sample_pdf_bytes):
    """Autoseeded chain rows have NULL aviv_user_id. When invoked with a
    chain_token, run_for_branch must use it and ignore the missing per-store
    creds — this is what the single-branch CLI path relies on."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE branches (
            id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 1,
            aviv_user_id TEXT, aviv_password TEXT, aviv_branch_id INTEGER
        );
        CREATE TABLE z_report_902 (
            branch_id INTEGER NOT NULL, date TEXT NOT NULL,
            z_number INTEGER, amount REAL, transactions INTEGER,
            avg_per_txn REAL, payment_breakdown TEXT,
            fetched_at TEXT DEFAULT (datetime('now')),
            trigger_type TEXT, auth_source TEXT,
            UNIQUE(branch_id, date)
        );
    ''')
    # Autoseeded row: NO aviv_user_id / aviv_password — only aviv_branch_id.
    conn.execute("INSERT INTO branches (id, name, aviv_branch_id) "
                 "VALUES (9001, 'קדיש לוז', 1)")
    conn.commit()

    monkeypatch.setattr(zr, 'fetch_902_id_z_possible_values',
                        lambda b, t: _branch_1_possible_values_body())
    monkeypatch.setattr(zr, 'submit_902', lambda b, z, t: 'https://x.invalid/r.pdf')
    monkeypatch.setattr(zr, 'download_pdf', lambda u, t: sample_pdf_bytes)
    monkeypatch.setattr(zr.time, 'sleep', lambda s: None)

    result = zr.run_for_branch(9001, '2026-05-26', conn=conn,
                               chain_token='CHAIN_TOK', trigger_type='manual')
    assert result['ok'] is True, f'expected success, got {result}'
    assert result['z_number'] == 3036
    row = conn.execute(
        "SELECT trigger_type, auth_source FROM z_report_902 "
        "WHERE branch_id=9001 AND date='2026-05-26'").fetchone()
    assert row['trigger_type'] == 'manual'
    assert row['auth_source'] == 'chain'


def test_run_for_branch_uses_possible_values_for_lazy_branch(
        monkeypatch, staging_db, sample_pdf_bytes):
    """End-to-end: when possible-values returns branch 1's Z list, the full
    pipeline lands Z 3036 in z_report_902 — filters/902 is never called."""
    monkeypatch.setattr(zr, 'fetch_902_id_z_possible_values',
                        lambda b, t: _branch_1_possible_values_body())
    def boom(b, t):
        raise AssertionError('fetch_902_filters must not be hit in the lazy path')
    monkeypatch.setattr(zr, 'fetch_902_filters', boom)
    _stub_success_path(monkeypatch, sample_pdf_bytes)

    # Re-point branch 126 in staging_db to aviv 1 for the test (the agent
    # reads aviv creds + writes to local branch_id 126 here).
    result = zr.run_for_branch(126, '2026-05-26', conn=staging_db)
    assert result['ok'] is True
    assert result['z_number'] == 3036
    assert result['total'] == 13721.98  # from the parsed sample fixture PDF


def _stub_success_path(monkeypatch, sample_pdf_bytes):
    """Stub everything downstream of fetch_902_filters so happy path runs."""
    monkeypatch.setattr(zr, '_login', lambda u, p: ('tok', 999))
    monkeypatch.setattr(zr, '_refresh', lambda t: t)
    monkeypatch.setattr(zr, 'submit_902',
                        lambda b, z, t: 'https://example.invalid/r.pdf')
    monkeypatch.setattr(zr, 'download_pdf', lambda u, t: sample_pdf_bytes)
    monkeypatch.setattr(zr.time, 'sleep', lambda s: None)  # don't sleep in tests


def test_902_stores_pdf_for_preview(monkeypatch, staging_db, sample_pdf_bytes,
                                    tmp_path):
    """A successful 902 pull writes the PDF to <PDF_BASE>/<branch_id>/z_<date>.pdf
    so the /sales 'צפה' preview reads it the same way it reads Gmail-Z PDFs.
    """
    monkeypatch.setattr(zr, 'PDF_BASE', str(tmp_path))
    _stub_success_path(monkeypatch, sample_pdf_bytes)
    monkeypatch.setattr(zr, 'fetch_902_filters', lambda b, t: _good_filters())

    result = zr.run_for_branch(126, '2026-05-20', conn=staging_db)
    assert result['ok'] is True

    pdf_path = tmp_path / '126' / 'z_2026-05-20.pdf'
    assert pdf_path.is_file(), f'expected PDF at {pdf_path}'
    assert pdf_path.read_bytes() == sample_pdf_bytes


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
    assert 'Z-list fetch failed' in result['error']
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
    assert 'Z-list fetch failed' in results[0]['error']
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


# ── backfill mode (missing-only) ──────────────────────────────────────────

def _multi_branch_db():
    """Two active branches (126 + 127) with the migration-010 schema."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE branches (
            id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 1,
            aviv_user_id TEXT, aviv_password TEXT
        );
        CREATE TABLE z_report_902 (
            branch_id INTEGER NOT NULL, date TEXT NOT NULL,
            z_number INTEGER, amount REAL, transactions INTEGER,
            avg_per_txn REAL, payment_breakdown TEXT,
            fetched_at TEXT DEFAULT (datetime('now')),
            trigger_type TEXT,
            auth_source TEXT,
            UNIQUE(branch_id, date)
        );
    ''')
    conn.execute("INSERT INTO branches (id, name, aviv_user_id, aviv_password) "
                 "VALUES (126, 'Einstein', 'e_u', 'e_p')")
    conn.execute("INSERT INTO branches (id, name, aviv_user_id, aviv_password) "
                 "VALUES (127, 'Tichon', 't_u', 't_p')")
    conn.commit()
    return conn


def test_branch_ids_for_date_full_run():
    """Full run lists every active branch with aviv creds."""
    conn = _multi_branch_db()
    assert zr._branch_ids_for_date(conn, '2026-05-24', missing_only=False) == [126, 127]


def test_branch_ids_for_date_missing_only():
    """missing_only filters out branches that already have ANY row for the date,
    including closed-day sentinels (z_number IS NULL)."""
    conn = _multi_branch_db()
    # 126 has a real row; 127 has a closed-day sentinel — both must be skipped.
    conn.execute("INSERT INTO z_report_902 (branch_id, date, z_number, amount) "
                 "VALUES (126, '2026-05-24', 1234, 10000.00)")
    zr.record_closed_day(conn, 127, '2026-05-24')
    assert zr._branch_ids_for_date(conn, '2026-05-24', missing_only=True) == []
    # Different date → both still missing
    assert zr._branch_ids_for_date(conn, '2026-05-25', missing_only=True) == [126, 127]


def test_record_closed_day_does_not_overwrite_real_row():
    """If a real Z already exists, INSERT OR IGNORE must not blank it out."""
    conn = _multi_branch_db()
    conn.execute(
        "INSERT INTO z_report_902 (branch_id, date, z_number, amount, transactions) "
        "VALUES (126, '2026-05-24', 1234, 10000.00, 200)")
    zr.record_closed_day(conn, 126, '2026-05-24')  # must be a no-op
    row = conn.execute(
        "SELECT z_number, amount FROM z_report_902 "
        "WHERE branch_id=126 AND date='2026-05-24'").fetchone()
    assert row['z_number'] == 1234
    assert row['amount'] == 10000.00


def test_closed_day_writes_sentinel_row(monkeypatch, sample_pdf_bytes):
    """run_for_branch's closed-day path writes a sentinel so later passes skip."""
    conn = _multi_branch_db()
    _stub_success_path(monkeypatch, sample_pdf_bytes)
    # Filters returns 200 but no Z matching the target date → closed-day.
    monkeypatch.setattr(zr, 'fetch_902_filters',
                        lambda b, t: {'data': [{'value': [
                            {'key': 2520, 'value': '2026-05-15 23:59:59'}]}]})
    result = zr.run_for_branch(126, '2026-05-24', conn=conn)
    assert result['ok'] is False
    assert result['error'] == 'no Z for date'
    row = conn.execute(
        "SELECT z_number, amount FROM z_report_902 "
        "WHERE branch_id=126 AND date='2026-05-24'").fetchone()
    assert row is not None
    assert row['z_number'] is None
    assert row['amount'] is None


def _spy_run_for_branch(monkeypatch):
    """Wrap zr.run_for_branch with a list that records which branch ids it sees."""
    seen: list[int] = []
    real = zr.run_for_branch
    def spy(bid, td=None, conn=None, **kw):
        seen.append(bid)
        return real(bid, td, conn=conn, **kw)
    monkeypatch.setattr(zr, 'run_for_branch', spy)
    return seen


def test_backfill_only_pulls_missing(monkeypatch, sample_pdf_bytes):
    """run_all_branches(missing_only=True) attempts ONLY branches with no row."""
    conn = _multi_branch_db()
    _stub_success_path(monkeypatch, sample_pdf_bytes)
    monkeypatch.setattr(zr, 'fetch_902_filters', lambda b, t: _good_filters())

    # 127 already has a row for the date → skip it.
    conn.execute("INSERT INTO z_report_902 (branch_id, date, z_number, amount) "
                 "VALUES (127, '2026-05-20', 1318, 12401.86)")
    conn.commit()

    seen = _spy_run_for_branch(monkeypatch)
    out = zr.run_all_branches('2026-05-20', missing_only=True, conn=conn)

    assert seen == [126], f'backfill must skip 127 (already has row), got {seen}'
    assert len(out) == 1
    assert out[0]['ok'] is True
    assert out[0]['branch_id'] == 126


def test_backfill_skips_closed_day_branch(monkeypatch, sample_pdf_bytes):
    """A branch marked closed-day on an earlier pass is skipped on later passes."""
    conn = _multi_branch_db()
    _stub_success_path(monkeypatch, sample_pdf_bytes)
    monkeypatch.setattr(zr, 'fetch_902_filters', lambda b, t: _good_filters())

    # Simulate 02:00 pass having recorded 126 as closed-day for 2026-05-20.
    zr.record_closed_day(conn, 126, '2026-05-20')

    seen = _spy_run_for_branch(monkeypatch)
    zr.run_all_branches('2026-05-20', missing_only=True, conn=conn)

    assert 126 not in seen, 'closed-day branch must not be re-probed in backfill'
    assert seen == [127]


def test_902_chain_auth_uses_chain_token_and_db_aviv_branch_id(monkeypatch, staging_db, sample_pdf_bytes):
    """Chain mode: skips per-branch login, uses chain_token + branches.aviv_branch_id."""
    # Wire 126 with aviv_branch_id=3 in the test DB.
    staging_db.execute("ALTER TABLE branches ADD COLUMN aviv_branch_id INTEGER")
    staging_db.execute("UPDATE branches SET aviv_branch_id=3 WHERE id=126")
    staging_db.commit()

    # _login must NEVER be called in chain mode.
    def boom_login(*a, **kw):
        raise AssertionError('per-branch _login must not be called in chain mode')
    monkeypatch.setattr(zr, '_login', boom_login)
    monkeypatch.setattr(zr, '_refresh', lambda t: t)

    seen_branch_param: list = []
    def fake_filters(aviv_branch_id, token):
        seen_branch_param.append((aviv_branch_id, token))
        return {'data': [{'value': [
            {'key': 2525, 'value': '2026-05-20 23:59:59'}]}]}
    monkeypatch.setattr(zr, 'fetch_902_filters', fake_filters)
    monkeypatch.setattr(zr, 'submit_902', lambda b, z, t: 'https://example.invalid/r.pdf')
    monkeypatch.setattr(zr, 'download_pdf', lambda u, t: sample_pdf_bytes)
    monkeypatch.setattr(zr.time, 'sleep', lambda s: None)

    result = zr.run_for_branch(126, '2026-05-20', conn=staging_db,
                               chain_token='CHAIN_TOK')
    assert result['ok'] is True
    assert result['total'] == 13721.98
    # URL branch param must be 3 (from DB), not 999 (login response shape).
    assert seen_branch_param == [(3, 'CHAIN_TOK')], \
        f'fetch_902_filters must be called with (3, CHAIN_TOK), got {seen_branch_param}'


def test_902_chain_mode_one_login_for_all_branches(monkeypatch, sample_pdf_bytes):
    """USE_CHAIN_AUTH on: run_all_branches does ONE chain login, not per-branch."""
    conn = _multi_branch_db()
    # Both branches get aviv_branch_id.
    conn.execute("ALTER TABLE branches ADD COLUMN aviv_branch_id INTEGER")
    conn.execute("UPDATE branches SET aviv_branch_id=3 WHERE id=126")
    conn.execute("UPDATE branches SET aviv_branch_id=8 WHERE id=127")
    conn.commit()

    monkeypatch.setattr(zr, 'USE_CHAIN_AUTH', True)

    chain_logins = {'n': 0}
    def fake_chain_login():
        chain_logins['n'] += 1
        return f'CHAIN_TOK_{chain_logins["n"]}'
    monkeypatch.setattr(zr, '_login_chain_account', fake_chain_login)

    def boom_login(*a, **kw):
        raise AssertionError('per-branch _login must not be called in chain mode')
    monkeypatch.setattr(zr, '_login', boom_login)
    monkeypatch.setattr(zr, '_refresh', lambda t: t)

    aviv_ids_seen: list[int] = []
    tokens_seen: list[str] = []
    def fake_filters(aviv_branch_id, token):
        aviv_ids_seen.append(aviv_branch_id)
        tokens_seen.append(token)
        return {'data': [{'value': [
            {'key': 2525, 'value': '2026-05-20 23:59:59'}]}]}
    monkeypatch.setattr(zr, 'fetch_902_filters', fake_filters)
    monkeypatch.setattr(zr, 'submit_902', lambda b, z, t: 'https://example.invalid/r.pdf')
    monkeypatch.setattr(zr, 'download_pdf', lambda u, t: sample_pdf_bytes)
    monkeypatch.setattr(zr.time, 'sleep', lambda s: None)

    out = zr.run_all_branches('2026-05-20', conn=conn)

    assert chain_logins['n'] == 1, 'exactly 1 chain login expected'
    assert sorted(aviv_ids_seen) == [3, 8], \
        f'must call filters with both aviv_branch_ids, got {aviv_ids_seen}'
    # Same token reused for both branches.
    assert tokens_seen == ['CHAIN_TOK_1', 'CHAIN_TOK_1']
    assert len(out) == 2
    assert all(r['ok'] for r in out)


def test_902_chain_skips_branches_without_aviv_branch_id(monkeypatch, sample_pdf_bytes):
    """Chain mode: branches without aviv_branch_id are filtered out at SELECT time."""
    conn = _multi_branch_db()
    conn.execute("ALTER TABLE branches ADD COLUMN aviv_branch_id INTEGER")
    conn.execute("UPDATE branches SET aviv_branch_id=3 WHERE id=126")
    # 127 has aviv_branch_id NULL — should be skipped in chain mode.
    conn.commit()

    monkeypatch.setattr(zr, 'USE_CHAIN_AUTH', True)
    monkeypatch.setattr(zr, '_login_chain_account', lambda: 'CHAIN')
    monkeypatch.setattr(zr, '_refresh', lambda t: t)
    monkeypatch.setattr(zr, 'fetch_902_filters', lambda b, t: _good_filters())
    monkeypatch.setattr(zr, 'submit_902', lambda b, z, t: 'u')
    monkeypatch.setattr(zr, 'download_pdf', lambda u, t: sample_pdf_bytes)
    monkeypatch.setattr(zr.time, 'sleep', lambda s: None)

    seen = _spy_run_for_branch(monkeypatch)
    zr.run_all_branches('2026-05-20', conn=conn)
    assert seen == [126]


def test_902_chain_login_failure_records_error_per_branch(monkeypatch):
    """If the single chain login fails, every branch gets a graceful error dict."""
    conn = _multi_branch_db()
    conn.execute("ALTER TABLE branches ADD COLUMN aviv_branch_id INTEGER")
    conn.execute("UPDATE branches SET aviv_branch_id=3 WHERE id=126")
    conn.execute("UPDATE branches SET aviv_branch_id=8 WHERE id=127")
    conn.commit()

    monkeypatch.setattr(zr, 'USE_CHAIN_AUTH', True)
    def boom():
        raise Exception('chain login refused')
    monkeypatch.setattr(zr, '_login_chain_account', boom)
    # If we reach branch processing, this would raise — must not be called.
    def must_not(*a, **kw):
        raise AssertionError('branches must not be processed after chain login failure')
    monkeypatch.setattr(zr, 'fetch_902_filters', must_not)

    out = zr.run_all_branches('2026-05-20', conn=conn)
    assert len(out) == 2
    for r in out:
        assert r['ok'] is False
        assert 'chain login failed' in r['error']


def test_bridge_mirrors_real_row_to_daily_sales(monkeypatch, staging_db, sample_pdf_bytes):
    """Successful 902 + MIRROR_TO_DAILY_SALES=True → daily_sales gets a z_report row."""
    monkeypatch.setattr(zr, 'MIRROR_TO_DAILY_SALES', True)
    _stub_success_path(monkeypatch, sample_pdf_bytes)
    monkeypatch.setattr(zr, 'fetch_902_filters', lambda b, t: _good_filters())

    result = zr.run_for_branch(126, '2026-05-20', conn=staging_db)
    assert result['ok'] is True

    row = staging_db.execute(
        "SELECT amount, transactions, source, fetched_at FROM daily_sales "
        "WHERE branch_id=126 AND date='2026-05-20'"
    ).fetchone()
    assert row is not None, 'daily_sales must have a mirrored row'
    assert row['amount'] == 13721.98
    assert row['transactions'] == 234
    assert row['source'] == 'z_report'
    # /sales surfaces this in the "שעת משיכה" column. ISO-ish "YYYY-MM-DD HH:MM:SS".
    assert row['fetched_at'] and len(row['fetched_at']) >= 16, \
        f'fetched_at not populated: {row["fetched_at"]!r}'


def test_bridge_closed_day_no_daily_sales_row(monkeypatch, staging_db, sample_pdf_bytes):
    """Closed-day sentinel must NEVER write to daily_sales."""
    monkeypatch.setattr(zr, 'MIRROR_TO_DAILY_SALES', True)
    _stub_success_path(monkeypatch, sample_pdf_bytes)
    # Filters return 200 but no Z for our date → closed-day path.
    monkeypatch.setattr(zr, 'fetch_902_filters',
                        lambda b, t: {'data': [{'value': [
                            {'key': 2400, 'value': '2026-01-01 23:59:59'}]}]})

    result = zr.run_for_branch(126, '2026-05-20', conn=staging_db)
    assert result['ok'] is False
    assert result['error'] == 'no Z for date'

    # daily_sales must NOT have a row — no zero/NULL overwrite.
    rows = staging_db.execute(
        "SELECT * FROM daily_sales WHERE branch_id=126 AND date='2026-05-20'"
    ).fetchall()
    assert rows == [], f'closed-day must not write daily_sales, got {rows}'


def test_bridge_insert_or_ignore_no_overwrite(monkeypatch, staging_db, sample_pdf_bytes):
    """Pre-existing daily_sales row (e.g. Gmail-Z) survives the 902 mirror."""
    # Seed an existing daily_sales row with a different amount.
    staging_db.execute(
        "INSERT INTO daily_sales (branch_id, date, amount, source) "
        "VALUES (126, '2026-05-20', 99999.99, 'z_report')")
    staging_db.commit()

    monkeypatch.setattr(zr, 'MIRROR_TO_DAILY_SALES', True)
    _stub_success_path(monkeypatch, sample_pdf_bytes)
    monkeypatch.setattr(zr, 'fetch_902_filters', lambda b, t: _good_filters())

    result = zr.run_for_branch(126, '2026-05-20', conn=staging_db)
    assert result['ok'] is True

    row = staging_db.execute(
        "SELECT amount FROM daily_sales WHERE branch_id=126 AND date='2026-05-20'"
    ).fetchone()
    assert row['amount'] == 99999.99, \
        '902 mirror must NOT overwrite an existing daily_sales row'


def test_bridge_disabled_when_flag_off(monkeypatch, staging_db, sample_pdf_bytes):
    """MIRROR_TO_DAILY_SALES=False → no daily_sales write even on success."""
    monkeypatch.setattr(zr, 'MIRROR_TO_DAILY_SALES', False)
    _stub_success_path(monkeypatch, sample_pdf_bytes)
    monkeypatch.setattr(zr, 'fetch_902_filters', lambda b, t: _good_filters())

    zr.run_for_branch(126, '2026-05-20', conn=staging_db)
    rows = staging_db.execute("SELECT COUNT(*) FROM daily_sales").fetchone()
    assert rows[0] == 0, 'flag off → daily_sales must stay empty'


def test_primary_run_pulls_all_branches(monkeypatch, sample_pdf_bytes):
    """02:00 full run attempts every active branch regardless of state."""
    conn = _multi_branch_db()
    _stub_success_path(monkeypatch, sample_pdf_bytes)
    monkeypatch.setattr(zr, 'fetch_902_filters', lambda b, t: _good_filters())

    # 127 already has a row — full run still attempts it.
    conn.execute("INSERT INTO z_report_902 (branch_id, date, z_number, amount) "
                 "VALUES (127, '2026-05-20', 1318, 12401.86)")
    conn.commit()

    seen = _spy_run_for_branch(monkeypatch)
    zr.run_all_branches('2026-05-20', missing_only=False, conn=conn)
    assert sorted(seen) == [126, 127]


def test_backfill_interval_skips_resolved(monkeypatch, sample_pdf_bytes):
    """Interval backfill: a branch with a row (real or sentinel) is skipped on
    later 30-min ticks. Simulates ticks N, N+1, N+2 — branch never re-probed.
    """
    conn = _multi_branch_db()
    _stub_success_path(monkeypatch, sample_pdf_bytes)
    monkeypatch.setattr(zr, 'fetch_902_filters', lambda b, t: _good_filters())

    # Tick 1: 126 lands a real row, 127 still missing.
    conn.execute("INSERT INTO z_report_902 (branch_id, date, z_number, amount) "
                 "VALUES (126, '2026-05-20', 2525, 5000.0)")
    # 127 already marked closed-day (sentinel) on the primary run.
    zr.record_closed_day(conn, 127, '2026-05-20')
    conn.commit()

    seen = _spy_run_for_branch(monkeypatch)
    # Tick 2 and Tick 3 (both --missing-only) — neither branch should be touched.
    zr.run_all_branches('2026-05-20', missing_only=True, conn=conn)
    zr.run_all_branches('2026-05-20', missing_only=True, conn=conn)
    assert seen == [], \
        f'resolved branches (real + sentinel) must not be re-probed; got {seen}'


def test_backfill_interval_retries_missing(monkeypatch, sample_pdf_bytes):
    """Interval backfill: a missing branch is attempted on every tick until it
    lands, then stops being attempted. Models Aviv's late-morning window for 126.
    """
    # Single-branch DB to keep retry accounting simple. The internal
    # FILTERS_MAX_ATTEMPTS retries are real — what we test is the OUTER tick loop.
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE branches (
            id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 1,
            aviv_user_id TEXT, aviv_password TEXT
        );
        CREATE TABLE z_report_902 (
            branch_id INTEGER NOT NULL, date TEXT NOT NULL,
            z_number INTEGER, amount REAL, transactions INTEGER,
            avg_per_txn REAL, payment_breakdown TEXT,
            fetched_at TEXT DEFAULT (datetime('now')),
            trigger_type TEXT,
            auth_source TEXT,
            UNIQUE(branch_id, date)
        );
    ''')
    conn.execute("INSERT INTO branches (id, name, aviv_user_id, aviv_password) "
                 "VALUES (126, 'Einstein', 'e_u', 'e_p')")
    conn.commit()

    _stub_success_path(monkeypatch, sample_pdf_bytes)

    # Tick-aware filters: ticks 1+2 fail every filters call (simulates Aviv's
    # 404 window still being closed); tick 3 succeeds. The OUTER tick loop is
    # what should keep retrying — internal FILTERS_MAX_ATTEMPTS won't bridge
    # the multi-hour window.
    tick = {'n': 0}

    def flaky_filters(aviv_bid, token):
        if tick['n'] < 3:
            raise RuntimeError('Aviv 404 / window not open yet')
        return _good_filters()

    monkeypatch.setattr(zr, 'fetch_902_filters', flaky_filters)
    seen = _spy_run_for_branch(monkeypatch)

    for n in range(1, 5):
        tick['n'] = n
        zr.run_all_branches('2026-05-20', missing_only=True, conn=conn)

    # Ticks 1, 2, 3 attempt 126 (3x). Tick 4 skips it (row exists).
    assert seen == [126, 126, 126], \
        f'missing branch must be retried each tick until it lands; saw {seen}'
    row = conn.execute("SELECT amount FROM z_report_902 "
                       "WHERE branch_id=126 AND date='2026-05-20'").fetchone()
    assert row is not None and row['amount'] is not None, \
        'tick 3 should have written a real row for 126'


def _autoseed_db():
    """In-memory DB whose branches table has the chain-mode columns the
    autoseed path needs (id, name, active, aviv_branch_id)."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE branches (
            id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 1,
            aviv_user_id TEXT, aviv_password TEXT, aviv_branch_id INTEGER
        );
        CREATE TABLE z_report_902 (
            branch_id INTEGER NOT NULL, date TEXT NOT NULL,
            z_number INTEGER, amount REAL, transactions INTEGER,
            avg_per_txn REAL, payment_breakdown TEXT,
            fetched_at TEXT DEFAULT (datetime('now')),
            trigger_type TEXT,
            auth_source TEXT,
            UNIQUE(branch_id, date)
        );
    ''')
    # Existing mappings: 126 → aviv 3, 127 → aviv 8.
    conn.execute("INSERT INTO branches (id, name, aviv_branch_id) "
                 "VALUES (126, 'Einstein', 3)")
    conn.execute("INSERT INTO branches (id, name, aviv_branch_id) "
                 "VALUES (127, 'Tichon', 8)")
    conn.commit()
    return conn


def test_autoseed_chain_branches_inserts_new_rows_only():
    """Existing aviv_branch_id mappings are preserved; missing ones get
    synthetic rows at id=9000+aviv_branch_id with the API-provided name."""
    conn = _autoseed_db()
    chain = [
        {'id': 3, 'name': 'איינשטיין'},   # already mapped (126) — skip
        {'id': 8, 'name': 'תיכון'},       # already mapped (127) — skip
        {'id': 1, 'name': 'Branch One'},  # new → local 9001
        {'id': 7, 'name': 'Branch Seven'},  # new → local 9007
    ]
    seeded = zr.autoseed_chain_branches(conn, chain)
    assert sorted(seeded) == [9001, 9007]

    # Existing 126/127 rows untouched.
    assert conn.execute("SELECT name FROM branches WHERE id=126").fetchone()['name'] == 'Einstein'

    # New synthetic rows present with the API name and active=1.
    row9001 = conn.execute("SELECT name, active, aviv_branch_id "
                           "FROM branches WHERE id=9001").fetchone()
    assert row9001['name'] == 'Branch One'
    assert row9001['active'] == 1
    assert row9001['aviv_branch_id'] == 1


def test_autoseed_excludes_hq_and_legacy_aviv_ids():
    """aviv_branch_id 90 (HQ) and 900 (legacy) must NEVER be seeded, even when
    /account/branches returns them. They aren't operating stores."""
    conn = _autoseed_db()
    chain = [
        {'id': 1, 'name': 'Real Branch'},
        {'id': 90, 'name': 'בשכונה HO'},          # HQ → excluded
        {'id': 900, 'name': 'שבטי ישראל - ישן'},  # legacy → excluded
    ]
    seeded = zr.autoseed_chain_branches(conn, chain)
    # Only the real branch was inserted; HQ + legacy never get a local row.
    assert seeded == [9001]
    # Defense in depth: those aviv ids must not exist in branches AT ALL
    # afterwards (independent of whether they pre-existed).
    rows = conn.execute(
        "SELECT aviv_branch_id FROM branches WHERE aviv_branch_id IN (90, 900)"
    ).fetchall()
    assert rows == [], 'HQ/legacy must not be present after autoseed'


def test_branch_ids_for_date_excludes_hq_and_legacy(monkeypatch):
    """If an HQ/legacy row somehow exists, iteration still skips it."""
    conn = _autoseed_db()
    # Force-insert an HQ row (simulating a pre-exclusion database).
    conn.execute("INSERT INTO branches (id, name, active, aviv_branch_id) "
                 "VALUES (9090, 'HQ', 1, 90)")
    conn.execute("INSERT INTO branches (id, name, active, aviv_branch_id) "
                 "VALUES (9900, 'Legacy', 1, 900)")
    conn.commit()
    bids = zr._branch_ids_for_date(conn, '2026-05-20', missing_only=False,
                                   chain_mode=True)
    assert 9090 not in bids
    assert 9900 not in bids
    # The legitimately mapped 126/127 still come through.
    assert 126 in bids and 127 in bids


def test_autoseed_is_idempotent():
    """Re-running autoseed with the same chain list adds zero rows."""
    conn = _autoseed_db()
    chain = [{'id': 7, 'name': 'Seven'}]
    assert zr.autoseed_chain_branches(conn, chain) == [9007]
    assert zr.autoseed_chain_branches(conn, chain) == []


def test_run_all_branches_autoseed_widens_iteration(monkeypatch, sample_pdf_bytes):
    """USE_CHAIN_AUTH + AUTOSEED_CHAIN: agent fetches chain list, seeds missing
    rows, and run_for_branch fires for the newly seeded local ids too."""
    conn = _autoseed_db()
    monkeypatch.setattr(zr, 'USE_CHAIN_AUTH', True)
    monkeypatch.setattr(zr, 'AUTOSEED_CHAIN', True)
    monkeypatch.setattr(zr, '_login_chain_account', lambda: 'CHAIN_TOK')
    monkeypatch.setattr(zr, '_refresh', lambda t: t)
    monkeypatch.setattr(zr, 'fetch_chain_branches', lambda t: [
        {'id': 3, 'name': 'איינשטיין'},
        {'id': 8, 'name': 'תיכון'},
        {'id': 1, 'name': 'Branch One'},
        {'id': 7, 'name': 'Branch Seven'},
        # HQ (90) + legacy (900) present in chain list but must NOT be iterated.
        {'id': 90, 'name': 'בשכונה HO'},
        {'id': 900, 'name': 'שבטי ישראל - ישן'},
    ])
    monkeypatch.setattr(zr, 'fetch_902_filters', lambda b, t: _good_filters())
    monkeypatch.setattr(zr, 'submit_902', lambda b, z, t: 'u')
    monkeypatch.setattr(zr, 'download_pdf', lambda u, t: sample_pdf_bytes)
    monkeypatch.setattr(zr.time, 'sleep', lambda s: None)

    seen = _spy_run_for_branch(monkeypatch)
    zr.run_all_branches('2026-05-20', conn=conn)
    # Existing 126/127 + the two non-excluded autoseeds (9001, 9007).
    # The HQ/legacy entries are silently dropped by EXCLUDED_CHAIN_AVIV_IDS.
    assert sorted(seen) == [126, 127, 9001, 9007], \
        f'autoseed must skip HQ/legacy aviv ids; saw {seen}'


def test_yesterday_il_anchors_on_israel_time_not_utc(monkeypatch):
    """At 23:00 UTC = 02:00 IL the next day, "yesterday" must be yesterday-IL,
    not 2-days-ago-IL.

    UTC: 2026-05-27 23:00  →  IL: 2026-05-28 02:00 (IDT, UTC+3).
    The agent fires from cron at 02:00 IL — so yesterday should be 2026-05-27,
    not 2026-05-26 (which is what UTC's date.today()-1 would have returned).
    """
    import agents.aviv_z_report as _zr_mod
    from datetime import datetime as _real_dt, timezone as _tz

    class _FrozenDT(_real_dt):
        @classmethod
        def now(cls, tz=None):
            # 23:00 UTC on 2026-05-27 — UTC clock still says "today=27"
            # but Israel is already 02:00 on 2026-05-28.
            utc = _real_dt(2026, 5, 27, 23, 0, 0, tzinfo=_tz.utc)
            if tz is None:
                return utc.replace(tzinfo=None)
            return utc.astimezone(tz)

    monkeypatch.setattr(_zr_mod, 'datetime', _FrozenDT)
    assert _zr_mod._yesterday_il() == '2026-05-27', \
        'yesterday-IL at 02:00 IL is the IL calendar date one before — not two before'


# ── pull metadata (trigger_type + auth_source) ────────────────────────────

def test_upsert_records_auto_chain_metadata(staging_db, sample_pdf_bytes):
    """Default auto + chain (token passed) → row stores ('auto','chain')."""
    parsed = zr.parse_902_pdf(sample_pdf_bytes)
    zr.upsert_z_report(staging_db, 126, '2026-05-20', 2525, parsed,
                       trigger_type='auto', auth_source='chain')
    row = staging_db.execute(
        "SELECT trigger_type, auth_source FROM z_report_902 "
        "WHERE branch_id=126 AND date='2026-05-20'").fetchone()
    assert row['trigger_type'] == 'auto'
    assert row['auth_source'] == 'chain'


def test_upsert_records_manual_per_store_metadata(staging_db, sample_pdf_bytes):
    """Manual CLI invocation without chain token → ('manual','per_store')."""
    parsed = zr.parse_902_pdf(sample_pdf_bytes)
    zr.upsert_z_report(staging_db, 126, '2026-05-20', 2525, parsed,
                       trigger_type='manual', auth_source='per_store')
    row = staging_db.execute(
        "SELECT trigger_type, auth_source FROM z_report_902 "
        "WHERE branch_id=126 AND date='2026-05-20'").fetchone()
    assert row['trigger_type'] == 'manual'
    assert row['auth_source'] == 'per_store'


def test_upsert_coerces_unknown_trigger_to_auto(staging_db, sample_pdf_bytes):
    """Bogus trigger_type values are coerced to 'auto' so /z-status only ever
    sees the documented enum."""
    parsed = zr.parse_902_pdf(sample_pdf_bytes)
    zr.upsert_z_report(staging_db, 126, '2026-05-20', 2525, parsed,
                       trigger_type='garbage', auth_source='also_garbage')
    row = staging_db.execute(
        "SELECT trigger_type, auth_source FROM z_report_902 "
        "WHERE branch_id=126 AND date='2026-05-20'").fetchone()
    assert row['trigger_type'] == 'auto'
    assert row['auth_source'] is None


def test_closed_day_sentinel_records_metadata():
    """Closed-day sentinel rows also carry trigger_type + auth_source so
    /z-status can tell that an admin manually probed and confirmed the day
    was closed."""
    conn = _multi_branch_db()
    zr.record_closed_day(conn, 126, '2026-05-20',
                         trigger_type='manual', auth_source='chain')
    row = conn.execute(
        "SELECT z_number, trigger_type, auth_source FROM z_report_902 "
        "WHERE branch_id=126 AND date='2026-05-20'").fetchone()
    assert row['z_number'] is None  # sentinel
    assert row['trigger_type'] == 'manual'
    assert row['auth_source'] == 'chain'


def test_run_for_branch_chain_token_implies_auth_source_chain(
        monkeypatch, staging_db, sample_pdf_bytes):
    """When chain_token is provided, the row's auth_source MUST be 'chain'
    regardless of how trigger_type is set."""
    staging_db.execute("ALTER TABLE branches ADD COLUMN aviv_branch_id INTEGER")
    staging_db.execute("UPDATE branches SET aviv_branch_id=3 WHERE id=126")
    staging_db.commit()

    monkeypatch.setattr(zr, 'fetch_902_filters',
                        lambda b, t: _good_filters())
    monkeypatch.setattr(zr, 'submit_902', lambda b, z, t: 'u')
    monkeypatch.setattr(zr, 'download_pdf', lambda u, t: sample_pdf_bytes)
    monkeypatch.setattr(zr.time, 'sleep', lambda s: None)

    zr.run_for_branch(126, '2026-05-20', conn=staging_db,
                      chain_token='CHAIN_TOK', trigger_type='manual')
    row = staging_db.execute(
        "SELECT trigger_type, auth_source FROM z_report_902 "
        "WHERE branch_id=126 AND date='2026-05-20'").fetchone()
    assert row['trigger_type'] == 'manual'
    assert row['auth_source'] == 'chain'


def test_run_for_branch_no_chain_token_implies_per_store(
        monkeypatch, staging_db, sample_pdf_bytes):
    """No chain_token → auth_source='per_store'."""
    _stub_success_path(monkeypatch, sample_pdf_bytes)
    monkeypatch.setattr(zr, 'fetch_902_filters', lambda b, t: _good_filters())

    zr.run_for_branch(126, '2026-05-20', conn=staging_db, trigger_type='auto')
    row = staging_db.execute(
        "SELECT trigger_type, auth_source FROM z_report_902 "
        "WHERE branch_id=126 AND date='2026-05-20'").fetchone()
    assert row['trigger_type'] == 'auto'
    assert row['auth_source'] == 'per_store'


def test_fetched_at_displays_israel_time():
    """app._utc_str_to_il_iso converts a SQLite UTC string to Israel-local ISO.

    DST-safe via zoneinfo. May 2026 is IDT (UTC+3): 00:30 UTC → 03:30 IL.
    Winter sample (Jan): UTC+2: 22:15 UTC → 00:15 IL the next day.
    """
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from app import _utc_str_to_il_iso

    # IDT (UTC+3)
    assert _utc_str_to_il_iso('2026-05-27 00:30:00') == '2026-05-27T03:30:00'
    # IST (UTC+2) — winter
    assert _utc_str_to_il_iso('2026-01-15 22:15:00') == '2026-01-16T00:15:00'
    # None / empty input
    assert _utc_str_to_il_iso(None) is None
    assert _utc_str_to_il_iso('') is None

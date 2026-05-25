"""Chain-account auth tests for aviv_employees_report (report 301)."""
import os
import sys
import sqlite3

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import agents.aviv_employees_report as emp


def _two_branch_db(tmp_path):
    """On-disk DB so emp.DB_PATH can point at it (the agent opens its own conn)."""
    db = tmp_path / 'makolet.db'
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE branches (
            id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 1,
            aviv_user_id TEXT, aviv_password TEXT, aviv_branch_id INTEGER
        );
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER, name TEXT, role TEXT,
            hourly_rate REAL, active INTEGER DEFAULT 1
        );
        CREATE TABLE employee_hours (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER, month TEXT, employee_name TEXT,
            total_hours REAL, total_salary REAL, source TEXT,
            employee_id INTEGER, confidence TEXT, raw_total_hours REAL,
            open_shifts_count INTEGER DEFAULT 0,
            UNIQUE(branch_id, month, employee_name)
        );
        CREATE TABLE employee_match_pending (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER, month TEXT, source_name TEXT,
            total_hours REAL, suggested_match_id INTEGER,
            confidence TEXT, raw_total_hours REAL,
            open_shifts_count INTEGER, status TEXT DEFAULT 'pending', source TEXT
        );
        CREATE TABLE agent_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER, agent TEXT, started_at TEXT, finished_at TEXT,
            status TEXT DEFAULT 'running', docs_count INTEGER DEFAULT 0,
            amount REAL DEFAULT 0, message TEXT, duration_seconds REAL DEFAULT 0,
            dismissed INTEGER DEFAULT 0
        );
    ''')
    conn.execute("INSERT INTO branches (id, name, aviv_user_id, aviv_password, aviv_branch_id) "
                 "VALUES (126, 'Einstein', 'e_u', 'e_p', 3)")
    conn.execute("INSERT INTO branches (id, name, aviv_user_id, aviv_password, aviv_branch_id) "
                 "VALUES (127, 'Tichon', 't_u', 't_p', 8)")
    conn.commit()
    conn.close()
    return str(db)


def _stub_report_internals(monkeypatch):
    """Stub everything past the auth step so we can isolate chain-auth behaviour."""
    monkeypatch.setattr(emp, 'fetch_report_list',
                        lambda aviv_branch_id, token: [{'id': 301, 'name': 'x'}])
    monkeypatch.setattr(emp, 'find_employer_report_id', lambda reports: 301)
    monkeypatch.setattr(emp, 'fetch_employer_report',
                        lambda aviv_branch_id, fd, td, token: b'XLS')
    monkeypatch.setattr(emp, 'parse_employer_report',
                        lambda xls_bytes: {'employees': [], 'total_hours': 0.0,
                                           'open_shifts_total': 0})
    monkeypatch.setattr(emp, 'update_employee_hours',
                        lambda branch_id, month, parsed, conn: {
                            'matched': 0, 'unmatched': 0,
                            'open_shifts_total': 0, 'total_hours': 0.0})


def test_emp_chain_auth_uses_chain_token_and_db_aviv_branch_id(monkeypatch, tmp_path):
    db = _two_branch_db(tmp_path)
    monkeypatch.setattr(emp, 'DB_PATH', db)

    # _login MUST NOT be called in chain mode.
    def boom_login(*a, **kw):
        raise AssertionError('per-branch _login must not be called in chain mode')
    monkeypatch.setattr(emp, '_login', boom_login)
    monkeypatch.setattr(emp, '_refresh', lambda t: t)
    _stub_report_internals(monkeypatch)

    seen_branch_params: list = []
    def spy_fetch(aviv_branch_id, fd, td, token):
        seen_branch_params.append((aviv_branch_id, token))
        return b'XLS'
    monkeypatch.setattr(emp, 'fetch_employer_report', spy_fetch)

    out = emp.run_for_branch(126, chain_token='CHAIN_TOK')
    assert out['ok'] is True
    # aviv_branch_id from DB (=3), token=CHAIN_TOK
    assert seen_branch_params and seen_branch_params[0] == (3, 'CHAIN_TOK')


def test_emp_chain_mode_one_login_for_all_branches(monkeypatch, tmp_path):
    db = _two_branch_db(tmp_path)
    monkeypatch.setattr(emp, 'DB_PATH', db)
    monkeypatch.setattr(emp, 'USE_CHAIN_AUTH', True)

    chain_logins = {'n': 0}
    def fake_chain_login():
        chain_logins['n'] += 1
        return f'CHAIN_TOK_{chain_logins["n"]}'
    monkeypatch.setattr(emp, '_login_chain_account', fake_chain_login)
    monkeypatch.setattr(emp, '_refresh', lambda t: t)

    def boom_login(*a, **kw):
        raise AssertionError('per-branch _login must not be called in chain mode')
    monkeypatch.setattr(emp, '_login', boom_login)
    _stub_report_internals(monkeypatch)

    aviv_ids_seen: list[int] = []
    tokens_seen: list[str] = []
    def spy(aviv_branch_id, fd, td, token):
        aviv_ids_seen.append(aviv_branch_id)
        tokens_seen.append(token)
        return b'XLS'
    monkeypatch.setattr(emp, 'fetch_employer_report', spy)

    out = emp.run_all_branches()
    assert chain_logins['n'] == 1, 'exactly 1 chain login expected'
    assert sorted(aviv_ids_seen) == [3, 8]
    assert tokens_seen == ['CHAIN_TOK_1', 'CHAIN_TOK_1']
    assert len(out) == 2 and all(r['ok'] for r in out)


def test_emp_chain_fallback_when_flag_off(monkeypatch, tmp_path):
    """USE_CHAIN_AUTH off → per-branch _login is called, chain login is not."""
    db = _two_branch_db(tmp_path)
    monkeypatch.setattr(emp, 'DB_PATH', db)
    monkeypatch.setattr(emp, 'USE_CHAIN_AUTH', False)

    def boom_chain():
        raise AssertionError('chain login must not be called when flag off')
    monkeypatch.setattr(emp, '_login_chain_account', boom_chain)

    per_branch_logins: list = []
    def fake_login(user, pw):
        per_branch_logins.append(user)
        return f'TOK_{user}', 999
    monkeypatch.setattr(emp, '_login', fake_login)
    monkeypatch.setattr(emp, '_refresh', lambda t: t)
    _stub_report_internals(monkeypatch)

    out = emp.run_all_branches()
    assert sorted(per_branch_logins) == ['e_u', 't_u']
    assert len(out) == 2 and all(r['ok'] for r in out)


def test_emp_chain_mode_skips_branch_with_null_aviv_branch_id(monkeypatch, tmp_path):
    """A branch with aviv_branch_id NULL is filtered out at SELECT time in chain mode."""
    db = _two_branch_db(tmp_path)
    # Null out 127's aviv_branch_id.
    c = sqlite3.connect(db)
    c.execute("UPDATE branches SET aviv_branch_id=NULL WHERE id=127")
    c.commit()
    c.close()

    monkeypatch.setattr(emp, 'DB_PATH', db)
    monkeypatch.setattr(emp, 'USE_CHAIN_AUTH', True)
    monkeypatch.setattr(emp, '_login_chain_account', lambda: 'CHAIN')
    monkeypatch.setattr(emp, '_refresh', lambda t: t)
    _stub_report_internals(monkeypatch)

    aviv_ids_seen: list[int] = []
    monkeypatch.setattr(emp, 'fetch_employer_report',
                        lambda b, fd, td, t: aviv_ids_seen.append(b) or b'XLS')
    out = emp.run_all_branches()
    assert aviv_ids_seen == [3]   # only 126 (aviv=3), 127 skipped
    assert len(out) == 1 and out[0]['ok'] is True

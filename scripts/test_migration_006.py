"""Tests for migrations/006_aviv_report_pending_columns.sql.

The migration adds 4 columns to employee_match_pending. Production DBs already
have them via runtime ALTER from agent startup paths, so this migration must
be a no-op there. Fresh DBs must end up with all 4 columns and correct
defaults after the migration runs.

Also exercises the dup-column-tolerance path in scripts/migrate.py.
"""

import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from scripts.migrate import _apply_sql, _split_statements


MIGRATION_PATH = os.path.join(os.path.dirname(__file__), '..',
                               'migrations', '006_aviv_report_pending_columns.sql')

BASE_TABLE = '''
CREATE TABLE employee_match_pending (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id INTEGER,
    month TEXT,
    csv_name TEXT,
    suggested_employee_id INTEGER,
    confidence TEXT,
    hours REAL,
    salary REAL,
    created_at TEXT,
    resolved INTEGER DEFAULT 0
);
'''


def _read_migration():
    with open(MIGRATION_PATH) as f:
        return f.read()


def _column_info(conn):
    """Return {column_name: (type, dflt_value)} for employee_match_pending."""
    rows = conn.execute("PRAGMA table_info(employee_match_pending)").fetchall()
    # PRAGMA returns: cid, name, type, notnull, dflt_value, pk
    return {r[1]: (r[2], r[4]) for r in rows}


class FreshDBTest(unittest.TestCase):
    """Migration applied to a DB without any of the 4 columns."""

    def setUp(self):
        self.db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.db.close()
        self.conn = sqlite3.connect(self.db.name)
        self.conn.executescript(BASE_TABLE)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        os.unlink(self.db.name)

    def test_all_four_columns_added(self):
        _apply_sql(self.conn, _read_migration())
        cols = _column_info(self.conn)
        self.assertIn('aviv_employee_id', cols)
        self.assertEqual(cols['aviv_employee_id'][0], 'INTEGER')
        self.assertIn('source', cols)
        self.assertEqual(cols['source'][0], 'TEXT')
        self.assertIn('is_new_employee', cols)
        self.assertEqual(cols['is_new_employee'][0], 'INTEGER')
        self.assertIn('is_csv_only', cols)
        self.assertEqual(cols['is_csv_only'][0], 'INTEGER')

    def test_defaults_applied_on_insert(self):
        _apply_sql(self.conn, _read_migration())
        self.conn.execute(
            "INSERT INTO employee_match_pending (branch_id, month, csv_name, hours) "
            "VALUES (127, '2026-05', 'אגם', 10.0)")
        self.conn.commit()
        row = self.conn.execute(
            "SELECT source, is_new_employee, is_csv_only, aviv_employee_id "
            "FROM employee_match_pending"
        ).fetchone()
        self.assertEqual(row[0], 'csv')
        self.assertEqual(row[1], 0)
        self.assertEqual(row[2], 0)
        self.assertIsNone(row[3])


class IdempotencyTest(unittest.TestCase):
    """Migration applied to a DB that already has the columns must not error."""

    def setUp(self):
        self.db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.db.close()
        self.conn = sqlite3.connect(self.db.name)
        self.conn.executescript(BASE_TABLE)
        # Simulate prod state: runtime ALTER already added these.
        self.conn.execute("ALTER TABLE employee_match_pending ADD COLUMN aviv_employee_id INTEGER")
        self.conn.execute("ALTER TABLE employee_match_pending ADD COLUMN source TEXT DEFAULT 'csv'")
        self.conn.execute("ALTER TABLE employee_match_pending ADD COLUMN is_new_employee INTEGER DEFAULT 0")
        self.conn.execute("ALTER TABLE employee_match_pending ADD COLUMN is_csv_only INTEGER DEFAULT 0")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        os.unlink(self.db.name)

    def test_no_error_on_reapply(self):
        # Should not raise — every ALTER hits dup-column → skipped.
        _apply_sql(self.conn, _read_migration())
        cols = _column_info(self.conn)
        # Still exactly one of each (sanity).
        self.assertEqual(sum(1 for k in cols if k == 'aviv_employee_id'), 1)
        self.assertEqual(sum(1 for k in cols if k == 'source'), 1)

    def test_run_twice(self):
        _apply_sql(self.conn, _read_migration())
        _apply_sql(self.conn, _read_migration())  # second pass also no-op
        cols = _column_info(self.conn)
        self.assertIn('aviv_employee_id', cols)
        self.assertIn('source', cols)
        self.assertIn('is_new_employee', cols)
        self.assertIn('is_csv_only', cols)


class PartiallyAppliedTest(unittest.TestCase):
    """One column already exists, three don't — the migration must add the
    three and skip the one. This is the actual prod state for some columns
    (added at different times by different agents)."""

    def setUp(self):
        self.db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.db.close()
        self.conn = sqlite3.connect(self.db.name)
        self.conn.executescript(BASE_TABLE)
        self.conn.execute("ALTER TABLE employee_match_pending ADD COLUMN source TEXT DEFAULT 'csv'")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        os.unlink(self.db.name)

    def test_missing_columns_added(self):
        _apply_sql(self.conn, _read_migration())
        cols = _column_info(self.conn)
        for c in ('aviv_employee_id', 'source', 'is_new_employee', 'is_csv_only'):
            self.assertIn(c, cols)


class MigrateRunnerHelpersTest(unittest.TestCase):
    """Direct unit tests for the SQL splitter — guards against regressions
    that would break migrations 001-005."""

    def test_strips_line_comments(self):
        sql = "-- a comment\nSELECT 1;\n-- another"
        self.assertEqual(_split_statements(sql), ['SELECT 1'])

    def test_inline_comment_after_statement(self):
        sql = "ALTER TABLE x ADD COLUMN y INTEGER; -- trailing"
        self.assertEqual(_split_statements(sql), ['ALTER TABLE x ADD COLUMN y INTEGER'])

    def test_multiple_statements(self):
        sql = "ALTER TABLE a ADD COLUMN b INT;\nUPDATE a SET b=1;\nALTER TABLE a ADD COLUMN c INT;"
        stmts = _split_statements(sql)
        self.assertEqual(len(stmts), 3)
        self.assertIn('UPDATE a SET b=1', stmts)

    def test_empty_lines_ignored(self):
        sql = "\n\n\nSELECT 1;\n\n"
        self.assertEqual(_split_statements(sql), ['SELECT 1'])

    def test_non_dup_error_propagates(self):
        """Non-dup-column errors must NOT be silently skipped."""
        conn = sqlite3.connect(':memory:')
        with self.assertRaises(sqlite3.OperationalError):
            _apply_sql(conn, "ALTER TABLE nonexistent ADD COLUMN x INTEGER;")


if __name__ == '__main__':
    unittest.main()

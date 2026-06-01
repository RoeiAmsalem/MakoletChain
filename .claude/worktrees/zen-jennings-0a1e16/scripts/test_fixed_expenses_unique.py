"""Tests for the UNIQUE constraint on fixed_expenses and the cleanup script."""

import sqlite3
import subprocess
import sys
import tempfile
import os

PASS = 0
FAIL = 0

def test(name, condition):
    global PASS, FAIL
    if condition:
        print(f'  PASS: {name}')
        PASS += 1
    else:
        print(f'  FAIL: {name}')
        FAIL += 1


def setup_db(path):
    """Create a minimal fixed_expenses table with the UNIQUE index."""
    conn = sqlite3.connect(path)
    conn.execute('''
        CREATE TABLE fixed_expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER,
            month TEXT,
            name TEXT,
            amount REAL DEFAULT 0,
            expense_type TEXT DEFAULT 'חודשי',
            pct_value REAL DEFAULT 0
        )
    ''')
    conn.execute('''
        CREATE UNIQUE INDEX uq_fixed_branch_month_name
          ON fixed_expenses(branch_id, month, name)
    ''')
    conn.commit()
    return conn


print('=== Test 1: INSERT succeeds for unique row ===')
with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
    db_path = f.name
try:
    conn = setup_db(db_path)
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount) VALUES (127, '2026-05', 'שכירות', 5000)")
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM fixed_expenses").fetchone()[0]
    test('single insert works', count == 1)
    conn.close()
finally:
    os.unlink(db_path)


print('=== Test 2: Duplicate INSERT raises IntegrityError ===')
with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
    db_path = f.name
try:
    conn = setup_db(db_path)
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount) VALUES (127, '2026-05', 'שכירות', 5000)")
    conn.commit()
    raised = False
    try:
        conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount) VALUES (127, '2026-05', 'שכירות', 9999)")
        conn.commit()
    except sqlite3.IntegrityError:
        raised = True
    test('duplicate INSERT raises IntegrityError', raised)
    count = conn.execute("SELECT COUNT(*) FROM fixed_expenses").fetchone()[0]
    test('still only 1 row after failed insert', count == 1)
    conn.close()
finally:
    os.unlink(db_path)


print('=== Test 3: INSERT OR IGNORE silently skips duplicate ===')
with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
    db_path = f.name
try:
    conn = setup_db(db_path)
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount) VALUES (127, '2026-05', 'שכירות', 5000)")
    conn.commit()
    conn.execute("INSERT OR IGNORE INTO fixed_expenses (branch_id, month, name, amount) VALUES (127, '2026-05', 'שכירות', 9999)")
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM fixed_expenses").fetchone()[0]
    test('INSERT OR IGNORE keeps 1 row', count == 1)
    amt = conn.execute("SELECT amount FROM fixed_expenses").fetchone()[0]
    test('original amount preserved (5000)', amt == 5000)
    conn.close()
finally:
    os.unlink(db_path)


print('=== Test 4: Cleanup script with no dupes does nothing ===')
with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
    db_path = f.name
try:
    conn = sqlite3.connect(db_path)
    conn.execute('''
        CREATE TABLE fixed_expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER, month TEXT, name TEXT, amount REAL DEFAULT 0
        )
    ''')
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount) VALUES (127, '2026-05', 'שכירות', 5000)")
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount) VALUES (127, '2026-05', 'חשמל', 2000)")
    conn.commit()
    conn.close()

    script = os.path.join(os.path.dirname(__file__), 'cleanup_fixed_expenses_dupes.py')
    result = subprocess.run([sys.executable, script, db_path], capture_output=True, text=True)
    test('cleanup reports 0 duplicate groups', 'Found 0 duplicate groups' in result.stdout)
    test('cleanup deletes 0 rows', 'Deleted 0 duplicate rows' in result.stdout)

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM fixed_expenses").fetchone()[0]
    test('still 2 rows after cleanup', count == 2)
    conn.close()
finally:
    os.unlink(db_path)


print('=== Test 5: Cleanup script removes dupes, keeps lowest ID ===')
with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
    db_path = f.name
try:
    conn = sqlite3.connect(db_path)
    conn.execute('''
        CREATE TABLE fixed_expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER, month TEXT, name TEXT, amount REAL DEFAULT 0
        )
    ''')
    # Original rows (IDs 1, 2)
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount) VALUES (127, '2026-05', 'שכירות', 5000)")
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount) VALUES (127, '2026-05', 'חשמל', 2000)")
    # Duplicate rows (IDs 3, 4)
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount) VALUES (127, '2026-05', 'שכירות', 5000)")
    conn.execute("INSERT INTO fixed_expenses (branch_id, month, name, amount) VALUES (127, '2026-05', 'חשמל', 2000)")
    conn.commit()
    conn.close()

    script = os.path.join(os.path.dirname(__file__), 'cleanup_fixed_expenses_dupes.py')
    result = subprocess.run([sys.executable, script, db_path], capture_output=True, text=True)
    test('cleanup reports 2 duplicate groups', 'Found 2 duplicate groups' in result.stdout)
    test('cleanup deletes 2 rows', 'Deleted 2 duplicate rows' in result.stdout)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id, name FROM fixed_expenses ORDER BY id").fetchall()
    test('2 rows remain after cleanup', len(rows) == 2)
    test('kept lowest IDs (1, 2)', [r['id'] for r in rows] == [1, 2])
    conn.close()
finally:
    os.unlink(db_path)


print(f'\n{"="*40}')
print(f'Results: {PASS} passed, {FAIL} failed')
sys.exit(1 if FAIL else 0)

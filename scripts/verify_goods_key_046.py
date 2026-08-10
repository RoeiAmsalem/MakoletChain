"""Verify migration 046 on scratch DBs — eviction repro + hard-gate proof.

Self-contained: builds throwaway SQLite files in a temp dir, touches no real
database and no server. Two things it proves:

A. EVICTION REPRO — the same two documents under the OLD key
   UNIQUE(branch_id, ref_number) vs the NEW key
   UNIQUE(branch_id, ref_number, supplier, doc_date), driven through the exact
   INSERT OR REPLACE the agent uses. Old key: the May document disappears when
   July is synced. New key: both survive.

B. HARD-GATE PROOF — runs the real migrations/046 file the way
   scripts/migrate.py runs it (BEGIN, statements, COMMIT, rollback on error),
   then runs a SABOTAGED copy whose row-copy silently drops one document. The
   sabotaged run must abort on the CHECK and leave the original table intact.

usage:  python3 scripts/verify_goods_key_046.py
"""
import os
import re
import sqlite3
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MIGRATION = os.path.join(ROOT, 'migrations', '046_goods_documents_unique_key.sql')

OLD_TABLE = """
CREATE TABLE goods_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL,
  doc_date TEXT, supplier TEXT, ref_number TEXT, amount REAL,
  doc_type INTEGER, total_without_vat REAL, paid INTEGER,
  bilboy_status INTEGER, bilboy_doc_id TEXT,
  UNIQUE(branch_id, ref_number)
)"""

NEW_TABLE = OLD_TABLE.replace("UNIQUE(branch_id, ref_number)",
                              "UNIQUE(branch_id, ref_number, supplier, doc_date)")

# Two REAL documents that collide only on ref_number: different supplier,
# different month, different amount. This is the shape that was being lost.
MAY = (126, '2026-05-07', 'סופר א', '2283', 1250.50)
JUL = (126, '2026-07-05', 'סופר ב', '2283', 990.25)

INSERT = ("INSERT OR REPLACE INTO goods_documents "
          "(branch_id, doc_date, supplier, ref_number, amount) VALUES (?, ?, ?, ?, ?)")


def _split_statements(sql):
    """Same splitter scripts/migrate.py uses — strip -- comments, split on ';'."""
    cleaned = []
    for line in sql.split('\n'):
        idx = line.find('--')
        if idx >= 0:
            line = line[:idx]
        cleaned.append(line)
    return [s.strip() for s in '\n'.join(cleaned).split(';') if s.strip()]


def _apply_migration(db_path, sql):
    """Apply `sql` exactly as migrate.py would. Returns (ok, error_text)."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("BEGIN")
        for stmt in _split_statements(sql):
            conn.execute(stmt)
        conn.commit()
        return True, None
    except Exception as e:
        conn.rollback()
        return False, f'{type(e).__name__}: {e}'
    finally:
        conn.close()


def part_a(tmp):
    print('── A. EVICTION REPRO (INSERT OR REPLACE, May then July)')
    results = {}
    for label, ddl in (('OLD key (branch_id, ref_number)', OLD_TABLE),
                       ('NEW key (+ supplier, doc_date)', NEW_TABLE)):
        path = os.path.join(tmp, f'repro_{len(results)}.db')
        conn = sqlite3.connect(path)
        conn.execute(ddl)
        conn.execute(INSERT, MAY)          # month 1 sync
        conn.execute(INSERT, JUL)          # month 2 sync — the re-pull
        conn.commit()
        rows = conn.execute("SELECT doc_date, supplier, ref_number, amount "
                            "FROM goods_documents ORDER BY doc_date").fetchall()
        conn.close()
        total = sum(r[3] for r in rows)
        results[label] = (len(rows), total)
        print(f'   {label}: {len(rows)} row(s), ₪{total:,.2f}')
        for r in rows:
            print(f'      {r[0]}  {r[2]:>6}  {r[1]}  ₪{r[3]:,.2f}')
    old_n, old_sum = results['OLD key (branch_id, ref_number)']
    new_n, new_sum = results['NEW key (+ supplier, doc_date)']
    lost = new_sum - old_sum
    ok = old_n == 1 and new_n == 2
    print(f'   → OLD key silently lost 1 document worth ₪{lost:,.2f}; NEW key kept both.')
    print(f'   {"PASS" if ok else "FAIL"}\n')
    return ok


def part_b(tmp):
    print('── B. HARD-GATE PROOF (real 046 file, then a sabotaged copy)')
    with open(MIGRATION) as fh:
        real_sql = fh.read()
    # Sabotage: the row-copy drops one document, so the per-branch count/sum
    # check must fail and abort the migration.
    sabotaged_sql = real_sql.replace(
        "FROM goods_documents;",
        "FROM goods_documents WHERE id <> (SELECT MIN(id) FROM goods_documents);", 1)
    assert sabotaged_sql != real_sql, 'sabotage patch did not apply'

    ok = True
    for label, sql, expect_ok in (('honest copy', real_sql, True),
                                  ('sabotaged copy (1 row dropped)', sabotaged_sql, False)):
        path = os.path.join(tmp, f'gate_{"ok" if expect_ok else "bad"}.db')
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE branches (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO branches (id) VALUES (126)")
        conn.execute(OLD_TABLE)
        conn.execute(INSERT, MAY)
        conn.execute(INSERT, ('126', '2026-06-01', 'סופר ג', '4410', 777.77))
        before = conn.execute("SELECT COUNT(*), SUM(CAST(ROUND(amount*100) AS INTEGER)) "
                              "FROM goods_documents").fetchone()
        conn.commit()
        conn.close()

        applied, err = _apply_migration(path, sql)

        conn = sqlite3.connect(path)
        after = conn.execute("SELECT COUNT(*), SUM(CAST(ROUND(amount*100) AS INTEGER)) "
                             "FROM goods_documents").fetchone()
        ddl = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' "
                           "AND name='goods_documents'").fetchone()[0]
        conn.close()
        wide = 'supplier, doc_date)' in ddl.replace('\n', ' ')
        good = (applied == expect_ok) and after == before and (wide == expect_ok)
        ok = ok and good
        print(f'   {label}: applied={applied} rows {before[0]}→{after[0]} '
              f'agorot {before[1]}→{after[1]} wide_key={wide}')
        if err:
            print(f'      aborted with: {err}')
        print(f'   {"PASS" if good else "FAIL"}')
    print()
    return ok


def main():
    with tempfile.TemporaryDirectory() as tmp:
        a = part_a(tmp)
        b = part_b(tmp)
    print(f'VERDICT: {"all checks PASS" if a and b else "FAILURES ABOVE"}')
    return 0 if (a and b) else 1


if __name__ == '__main__':
    sys.exit(main())

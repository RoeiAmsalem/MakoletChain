-- 046: goods_documents — widen the UNIQUE key so a re-pull can no longer
-- evict a real document.
--
-- THE BUG. UNIQUE(branch_id, ref_number) spans ALL months, but the nightly
-- sync deletes+reinserts ONE month with INSERT OR REPLACE. A ref number reused
-- by a different supplier (BilBoy ref numbers are per-supplier counters, so
-- reuse is normal) silently REPLACED the older month's row. Measured loss:
-- 134 documents / 214,406.92 NIS.
--
-- THE KEY. (branch_id, ref_number, supplier, doc_date).
-- bilboy_doc_id was measured on prod 2026-08-10 and REJECTED as the key:
--   * 660 of 9,411 rows (7.01%) have no id at all (2026-03..2026-05 history)
--     and SQLite treats every NULL as distinct, so those rows would get no
--     protection whatsoever
--   * UUID 524b708b-fb50-43cc-97c0-fd531b6979a5 (branch 9006) is carried by
--     TWO genuinely different documents (2026-05-07 ref 2283 -225.14 vs
--     2026-07-05 ref 8791 -208.51) — as a key it would evict one of them,
--     which is the very bug being fixed here.
-- All four chosen columns are non-null and non-blank on all 9,411 prod rows,
-- and the key has 0 duplicate groups, so it can be enforced today.
--
-- THE GATE. Table rebuild, verified BEFORE the old table is dropped: per
-- branch, row count and SUM(amount) in whole agorot (integer math, exact to
-- the agora) must match on both sides. A mismatch violates the CHECK on
-- _mig046_gate, which aborts this file — and scripts/migrate.py wraps every
-- migration in BEGIN/COMMIT with rollback on any exception, so a failure
-- leaves goods_documents exactly as it was.

CREATE TABLE goods_documents_new (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL REFERENCES branches(id),
  doc_date TEXT,
  supplier TEXT,
  ref_number TEXT,
  amount REAL,
  doc_type INTEGER,
  total_without_vat REAL,
  paid INTEGER,
  bilboy_status INTEGER,
  bilboy_doc_id TEXT,
  UNIQUE(branch_id, ref_number, supplier, doc_date)
);

INSERT INTO goods_documents_new
  (id, branch_id, doc_date, supplier, ref_number, amount, doc_type,
   total_without_vat, paid, bilboy_status, bilboy_doc_id)
SELECT id, branch_id, doc_date, supplier, ref_number, amount, doc_type,
       total_without_vat, paid, bilboy_status, bilboy_doc_id
FROM goods_documents;

CREATE TEMP TABLE _mig046_gate (ok INTEGER NOT NULL CHECK (ok = 1));

INSERT INTO _mig046_gate (ok)
SELECT CASE WHEN
  (SELECT COUNT(*) FROM (
     SELECT branch_id, COUNT(*) AS c,
            SUM(CAST(ROUND(amount * 100) AS INTEGER)) AS agorot
       FROM goods_documents GROUP BY branch_id
     EXCEPT
     SELECT branch_id, COUNT(*) AS c,
            SUM(CAST(ROUND(amount * 100) AS INTEGER)) AS agorot
       FROM goods_documents_new GROUP BY branch_id)) = 0
  AND
  (SELECT COUNT(*) FROM (
     SELECT branch_id, COUNT(*) AS c,
            SUM(CAST(ROUND(amount * 100) AS INTEGER)) AS agorot
       FROM goods_documents_new GROUP BY branch_id
     EXCEPT
     SELECT branch_id, COUNT(*) AS c,
            SUM(CAST(ROUND(amount * 100) AS INTEGER)) AS agorot
       FROM goods_documents GROUP BY branch_id)) = 0
THEN 1 ELSE 0 END;

DROP TABLE goods_documents;

ALTER TABLE goods_documents_new RENAME TO goods_documents;

DROP TABLE _mig046_gate;

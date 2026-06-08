-- Migration 032: one-time cleanup of whitespace-dirty supplier names.
--
-- BilBoy returns some supplier names with a trailing newline (also CR/TAB/stray
-- spaces). Those raw values were stored in goods_documents, and a stripped copy
-- in supplier_roster, so the budget list unioned 'name' and 'name\n' as two
-- suppliers (duplicate-supplier bug, e.g. מרינה twice on 9018). The write-time
-- helper clean_supplier_name() now normalizes on the way in; this fixes the rows
-- already stored.
--
-- Normalization E(col): CR/LF/TAB → space, collapse double-spaces, then TRIM —
-- matching utils.text.clean_supplier_name for all realistic cases. Pure SQL, no
-- Hebrew literals, idempotent (only touches rows where cleaned <> current; a
-- second run is a no-op).

-- goods_documents — no UNIQUE on supplier, straight trim.
UPDATE goods_documents
   SET supplier = TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(supplier, CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' '), '  ', ' '), '  ', ' '))
 WHERE supplier IS NOT NULL
   AND supplier <> TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(supplier, CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' '), '  ', ' '), '  ', ' '));

-- supplier_budgets — UNIQUE(branch_id, supplier_name): drop a dirty row whose
-- cleaned name already exists (keep the clean twin), then trim the rest.
DELETE FROM supplier_budgets
 WHERE supplier_name IS NOT NULL
   AND supplier_name <> TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(supplier_name, CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' '), '  ', ' '), '  ', ' '))
   AND EXISTS (SELECT 1 FROM supplier_budgets t
                WHERE t.branch_id = supplier_budgets.branch_id
                  AND t.supplier_name = TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(supplier_budgets.supplier_name, CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' '), '  ', ' '), '  ', ' ')));
UPDATE supplier_budgets
   SET supplier_name = TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(supplier_name, CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' '), '  ', ' '), '  ', ' '))
 WHERE supplier_name IS NOT NULL
   AND supplier_name <> TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(supplier_name, CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' '), '  ', ' '), '  ', ' '));

-- supplier_roster — UNIQUE(branch_id, supplier_name): same collision-safe pattern.
DELETE FROM supplier_roster
 WHERE supplier_name IS NOT NULL
   AND supplier_name <> TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(supplier_name, CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' '), '  ', ' '), '  ', ' '))
   AND EXISTS (SELECT 1 FROM supplier_roster t
                WHERE t.branch_id = supplier_roster.branch_id
                  AND t.supplier_name = TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(supplier_roster.supplier_name, CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' '), '  ', ' '), '  ', ' ')));
UPDATE supplier_roster
   SET supplier_name = TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(supplier_name, CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' '), '  ', ' '), '  ', ' '))
 WHERE supplier_name IS NOT NULL
   AND supplier_name <> TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(supplier_name, CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' '), '  ', ' '), '  ', ' '));

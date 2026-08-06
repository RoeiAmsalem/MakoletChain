-- 044_fixed_expenses_source.sql
-- System-managed fixed-expense rows: provenance column.
--
-- source = 'manual'      -> user-entered row (default; all existing rows)
--          'bilboy_wolt' -> the auto "עמלות Wolt" row, written nightly by the
--                          BilBoy sync from זיכיונות המכולת Wolt fee lines.
--
-- Non-'manual' rows are NON-EDITABLE by managers (server-side reject in the
-- fixed-expenses PUT/DELETE endpoints; admin may delete, audited). They are
-- excluded from the monthly carry-forward (_ensure_monthly_expenses) and from
-- MTD pro-ration in _get_fixed_total (the amount is already actual MTD).
--
-- Design note: fixed_expenses already carries a dormant `locked` column (base
-- schema, never enforced). We deliberately add `source` instead of overloading
-- it — provenance ("who writes this row") is the semantic we need, and it
-- matches the electricity_invoices.source convention. The nightly writer also
-- sets locked=1 on its row for any future locked-aware UI.

ALTER TABLE fixed_expenses ADD COLUMN source TEXT DEFAULT 'manual';

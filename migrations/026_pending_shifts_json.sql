-- Migration 026: carry per-shift detail on the pending row so shifts can be
-- written INSTANTLY when a manager's employee is added from the pending UI
-- (no nightly wait, no synchronous Aviv re-pull in the web request).
--
-- The aviv_employees_report agent parses each unmatched employee's shift rows
-- but previously discarded them (only matched employees got employee_shifts).
-- It now stores the RAW shift list (date/in/out/hours/day/open) as JSON here.
-- On add (api_pending_add_new) the JSON is classified with the chosen
-- salary_type and written to employee_shifts under the final employee name with
-- source='aviv_report' — the exact key the nightly full-overwrite deletes, so a
-- later re-pull reconciles to identical rows (no orphans, no duplicates).
--
-- Additive, nullable: CSV/Gmail-source rows have no shift detail and leave this
-- NULL (no breakdown, as expected). No-op if the column already exists (the
-- migration runner tolerates "duplicate column name").

ALTER TABLE employee_match_pending ADD COLUMN shifts_json TEXT;

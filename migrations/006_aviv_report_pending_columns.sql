-- Migration 006: persist columns added at runtime by the
-- aviv_employees_report agent (and earlier by aviv_employees / gmail_agent).
-- These columns already exist in prod & staging via runtime ALTER from agent
-- startup paths (added 2026-04 to 2026-05). This migration formalizes them
-- for traceability and fresh-DB initialization.
--
-- The migration runner (scripts/migrate.py) tolerates "duplicate column name"
-- errors per-statement so this file is a no-op on DBs where the runtime ALTER
-- already added the columns.

ALTER TABLE employee_match_pending ADD COLUMN aviv_employee_id INTEGER;
ALTER TABLE employee_match_pending ADD COLUMN source TEXT DEFAULT 'csv';
ALTER TABLE employee_match_pending ADD COLUMN is_new_employee INTEGER DEFAULT 0;
ALTER TABLE employee_match_pending ADD COLUMN is_csv_only INTEGER DEFAULT 0;

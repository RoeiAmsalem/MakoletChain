-- ui_start_month: per-branch override for the earliest month shown in the UI.
-- The DB always retains full historical data (electricity invoices back to 2021,
-- test rows from Feb 2026, etc.); this column controls the user-visible window only.
-- Format: 'YYYY-MM'. NULL = use auto-detected start from operational data.
ALTER TABLE branches ADD COLUMN ui_start_month TEXT DEFAULT NULL;

UPDATE branches SET ui_start_month = '2026-03' WHERE id = 126;

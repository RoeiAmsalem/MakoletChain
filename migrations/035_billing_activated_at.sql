-- 035_billing_activated_at.sql
-- Paywall (billing stage 2): remember WHEN a manager was toggled active so the
-- unpaid grace countdown for a mid-month joiner starts at the toggle date, not
-- at BILLING_START_DATE / the 1st of the month. updated_at cannot serve as the
-- anchor because every read-only SUMIT sync refreshes it on all rows.
-- All pre-migration rows are active=0, so no backfill; NULL falls back to
-- max(BILLING_START_DATE, 1st of current month) in _billing_state().

ALTER TABLE manager_billing ADD COLUMN activated_at TEXT;

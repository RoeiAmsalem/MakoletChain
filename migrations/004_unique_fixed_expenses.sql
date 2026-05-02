-- Migration 004: prevent duplicate fixed_expenses rows from race conditions
-- _ensure_monthly_expenses does SELECT COUNT then INSERT which is not race-safe.
-- This UNIQUE index makes INSERT OR IGNORE work correctly at the DB level.
CREATE UNIQUE INDEX IF NOT EXISTS uq_fixed_branch_month_name
  ON fixed_expenses(branch_id, month, name);

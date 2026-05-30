-- Migration 017: agents_enabled flag for no-agent (demo) branches.
--
-- The demo branch "מכולת הדגמה" (id 9999) must be structurally invisible to
-- every scheduled agent: no agent_runs rows, no pulls/overwrites, no false
-- brrr alerts. NULL agent-config already hides it from the chain-keyed agents
-- (aviv_live / aviv_z / aviv_employees_report / iec — they filter
-- `... IS NOT NULL` in SQL), but bilboy + gmail + hourly_sales_alerts select
-- `WHERE active = 1` with no config filter and would still reach it per-branch.
--
-- This column is the explicit, self-documenting kill-switch. DEFAULT 1 +
-- the UPDATE below keep every existing real branch fully agent-enabled, so
-- behavior is unchanged for them. The demo branch is inserted with
-- agents_enabled = 0 by scripts/seed_demo_branch.py. The all-active selectors
-- (scheduler.get_active_branches, hourly_sales_alerts, bilboy --all-active)
-- add `AND agents_enabled = 1`.

ALTER TABLE branches ADD COLUMN agents_enabled INTEGER DEFAULT 1;

UPDATE branches SET agents_enabled = 1 WHERE agents_enabled IS NULL;

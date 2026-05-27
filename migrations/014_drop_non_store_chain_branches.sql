-- Remove non-store chain branches that autoseed pulled from /account/branches
-- but are NOT operating stores:
--   aviv_branch_id = 90  → 'בשכונה HO'        (headquarters)
--   aviv_branch_id = 900 → 'שבטי ישראל - ישן' (legacy/old)
-- These never produce a Z and only clutter /z-status with permanent "חסר".
--
-- This migration is a no-op on prod (those rows only exist on staging where
-- autoseed runs). The agent now also carries a hardcoded EXCLUDED_CHAIN_AVIV_IDS
-- set so autoseed will never re-add them on the next chain pull.

-- Drop any z_report_902 rows referencing the doomed branches first so we
-- don't leave orphans. (No FK enforcement in SQLite, but tidy.)
DELETE FROM z_report_902
 WHERE branch_id IN (
   SELECT id FROM branches WHERE aviv_branch_id IN (90, 900)
 );

DELETE FROM branches WHERE aviv_branch_id IN (90, 900);

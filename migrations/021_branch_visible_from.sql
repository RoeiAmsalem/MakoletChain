-- 021_branch_visible_from.sql
-- Per-branch visibility FLOOR.
--
-- When branches.visible_from is set, that branch can NEVER see its own
-- operational data from before that date. It is a rolling-forward FLOOR, not a
-- single-month window: a branch floored at 2026-06-01 sees June in June, then
-- June+July in July, and so on — there is no upper bound, but nothing before
-- the floor ever appears (home KPIs, revenue, goods, employees/hours, fixed
-- expenses, electricity, department sales, and the month picker).
--
-- NULL visible_from = NO floor = full history. Branches 126 (Shimon) and 127
-- (Gal) keep NULL and are completely untouched; the two demo stores
-- (9999, 9998) also stay NULL.
--
-- The column is left nullable with NO default on purpose: on prod the UPDATE
-- below floors the existing chain branches once, while any branch inserted
-- later (and every branch created in the test suites, which apply migrations
-- before seeding rows) defaults to NULL and is therefore unaffected.

ALTER TABLE branches ADD COLUMN visible_from DATE;

UPDATE branches
   SET visible_from = '2026-06-01'
 WHERE id NOT IN (126, 127, 9999, 9998);

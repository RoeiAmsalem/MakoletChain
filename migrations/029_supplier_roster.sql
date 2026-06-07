-- Migration 029: per-branch full supplier roster for the /goods תקציב page.
--
-- The budget page should list a branch's FULL supplier roster — not only
-- suppliers with current-month orders — so a manager can set a budget for any
-- supplier before ordering this month. The roster is rebuilt monthly (1st, IL)
-- from BilBoy goods over the PRIOR 2 calendar months, IGNORING the visible_from
-- display floor (the BilBoy data for new chain stores exists pre-floor), and
-- EXCLUDING the branch's franchise supplier (branches.franchise_supplier).
--
-- Replace-on-refresh: each build deletes the branch's rows and reinserts.
-- The budget page unions this roster with current-month spenders + budgeted
-- suppliers, so it degrades safely to current ∪ budgeted before the first build.

CREATE TABLE IF NOT EXISTS supplier_roster (
    branch_id     INTEGER NOT NULL,
    supplier_name TEXT    NOT NULL,
    updated_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(branch_id, supplier_name)
);

CREATE INDEX IF NOT EXISTS idx_supplier_roster_branch
    ON supplier_roster (branch_id);

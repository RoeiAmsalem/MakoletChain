-- 027_supplier_budgets.sql
-- Per-supplier monthly purchase budget, set by the manager on /goal.
--
-- One row per (branch_id, supplier_name): the manager's monthly purchase
-- budget (תקציב) for that supplier. The /goal page compares this against the
-- projected month-end spend (קצב = run-rate from BilBoy goods) to show the
-- remaining headroom (יתרה). Supplier names come ONLY from BilBoy goods data,
-- so they always match goods_documents.supplier exactly — never free text.
--
-- A single budget per supplier (not per month): the manager sets it once and
-- it carries forward every month until changed. Clearing the budget on /goal
-- deletes the row. updated_at tracks the last edit.

CREATE TABLE IF NOT EXISTS supplier_budgets (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id     INTEGER NOT NULL,
    supplier_name TEXT NOT NULL,
    monthly_budget REAL NOT NULL,
    updated_at    TEXT DEFAULT (datetime('now')),
    UNIQUE(branch_id, supplier_name)
);

CREATE INDEX IF NOT EXISTS idx_supplier_budgets_branch
    ON supplier_budgets(branch_id);

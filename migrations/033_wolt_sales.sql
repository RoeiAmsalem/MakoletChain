-- 033_wolt_sales.sql
-- Monthly Wolt revenue per branch, sourced from Aviv BI report 203
-- ("מכירות בחתך כרטיסי סועד") filtered to the Wolt tender (inDcType=20).
--
-- Wolt is a payment tender INSIDE total revenue — a slice of daily_sales,
-- never an addition to it. amount is incl-VAT, the same basis as
-- daily_sales.amount (verified: 203 day totals tie ₪-for-₪ to the 902 Z
-- payment lines, which tie to daily_sales).
--
-- One row per (branch_id, year_month); branches/months with no Wolt revenue
-- have NO row (the /sales tile only renders when a row > 0 exists). The
-- wolt_sales agent does a full-month overwrite on every run.

CREATE TABLE IF NOT EXISTS wolt_sales (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id  INTEGER NOT NULL,
    year_month TEXT NOT NULL,             -- 'YYYY-MM'
    amount     REAL NOT NULL,             -- incl-VAT, same basis as daily_sales
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(branch_id, year_month)
);

CREATE INDEX IF NOT EXISTS idx_wolt_sales_branch_month
    ON wolt_sales(branch_id, year_month);

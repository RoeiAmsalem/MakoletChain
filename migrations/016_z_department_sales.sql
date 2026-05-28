-- Migration 016: per-branch per-day per-department sales from Aviv BI 902 XLS.
-- Stores ALL ~35 departments from the Z report's "מכירות בחתך מחלקה" section.
-- Populated by aviv_z_report.py alongside the existing PDF pull (XLS is a
-- structured alternative to the same submit endpoint). Three departments are
-- surfaced on the home page (5=דאיירי, 83=טבק, 2=ירקות) but the table is
-- future-proof — adding another dept_code on the UI requires no DB change.
--
-- Idempotent / re-runnable. Backfills overwrite via INSERT OR REPLACE so a
-- corrected Z (rare manual re-issue) refreshes its dept rows cleanly.

CREATE TABLE IF NOT EXISTS z_department_sales (
    branch_id   INTEGER NOT NULL,
    date        TEXT    NOT NULL,      -- 'YYYY-MM-DD' anchored on Israel time
    dept_code   INTEGER NOT NULL,      -- Aviv department code (5, 83, 2, ...)
    dept_name   TEXT    NOT NULL,      -- Hebrew name as it appeared in the Z
    amount      REAL    NOT NULL,
    qty         REAL,                  -- fractional allowed (produce by weight)
    fetched_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (branch_id, date, dept_code)
);

CREATE INDEX IF NOT EXISTS idx_zdept_branch_date
    ON z_department_sales (branch_id, date);

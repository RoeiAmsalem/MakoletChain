-- 022_employee_shifts.sql
-- Per-shift drill-down captured from the Aviv employer report (report 301).
--
-- One row per shift: entry/exit timestamps, hours, day-of-week, and an
-- is_open flag for the "אין יציאה" (no clock-out) case. The monthly TOTAL
-- stays in employee_hours and remains the single salary source of truth
-- (_calculate_salary_cost). These rows are display-only drill-down — they are
-- NEVER summed for the monthly total (subtotals in report 301 can exceed 24h
-- per shift and open shifts carry no hours).
--
-- Written by aviv_employees_report.update_employee_hours alongside the monthly
-- row, full-overwrite per (branch_id, month, source) — same strategy as
-- employee_hours, so a re-sync replaces cleanly with no duplicate shifts.

CREATE TABLE IF NOT EXISTS employee_shifts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id INTEGER NOT NULL,
    month TEXT NOT NULL,
    employee_name TEXT NOT NULL,
    shift_date TEXT,
    start_ts TEXT,
    end_ts TEXT,
    hours REAL DEFAULT 0,
    day_of_week TEXT,
    is_open INTEGER NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'aviv_report',
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_employee_shifts_branch_month
    ON employee_shifts(branch_id, month, source);
CREATE INDEX IF NOT EXISTS idx_employee_shifts_emp
    ON employee_shifts(branch_id, month, employee_name);
CREATE INDEX IF NOT EXISTS idx_employee_shifts_open
    ON employee_shifts(branch_id, month, is_open);

-- 023_shabbat_times_and_shift_classification.sql
-- Overtime + Shabbat/chag hour CLASSIFICATION (display only — salary unchanged).
--
-- shabbat_times: cached Hebcal candle-lighting → havdalah windows for the
-- chain's area (Haifa, geonameid 294801). Refreshed by a weekly scheduler job;
-- never hit per request. Holidays (chag) come through the same /shabbat feed
-- and are stored with is_holiday=1 — they count the same as Shabbat.
--
-- employee_shifts gains three display-only buckets, computed at sync time by
-- the employer-report agent (agents/shift_classify.py) and full-overwritten on
-- every re-sync alongside the shift rows:
--   regular_hours + overtime_hours = the shift's hours (daily-8 OT split)
--   shabbat_hours = hours inside a Shabbat/chag window (ORTHOGONAL overlay —
--                   can coincide with regular or overtime; never summed into
--                   the regular/overtime partition, never into salary).
-- NULL on legacy rows / global-salary employees that aren't classified.

CREATE TABLE IF NOT EXISTS shabbat_times (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,                 -- candle-lighting date YYYY-MM-DD
    candle_lighting_ts TEXT NOT NULL,   -- 'YYYY-MM-DD HH:MM:SS' Israel local
    havdalah_ts TEXT,                   -- window end; NULL until known
    is_holiday INTEGER NOT NULL DEFAULT 0,
    label TEXT,                         -- 'שבת' or holiday title
    geonameid INTEGER NOT NULL DEFAULT 294801,
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(date, geonameid)
);

CREATE INDEX IF NOT EXISTS idx_shabbat_times_date ON shabbat_times(date);

ALTER TABLE employee_shifts ADD COLUMN regular_hours REAL;
ALTER TABLE employee_shifts ADD COLUMN overtime_hours REAL;
ALTER TABLE employee_shifts ADD COLUMN shabbat_hours REAL;

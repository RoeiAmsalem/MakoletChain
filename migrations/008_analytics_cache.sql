-- Migration 008: analytics aggregate cache.
-- /admin/analytics aggregates the full range (no user filter) once nightly at
-- 03:30 IL (right after the 03:00 user_events cleanup). Cache miss on page
-- load triggers a live compute + write. user_id-filtered queries bypass the
-- cache (they're cheap).

CREATE TABLE IF NOT EXISTS analytics_cache (
    range TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    computed_at TEXT NOT NULL DEFAULT (datetime('now'))
);

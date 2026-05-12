-- Migration 007: user activity event collection.
-- Records login, page_view, and heartbeat events for non-admin users.
-- Phase 1: collection only — admin UI (/admin/analytics) lands in a later commit.
-- 90-day retention enforced by scheduler.py (cleanup_old_user_events daily 03:00 IL).

CREATE TABLE IF NOT EXISTS user_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    page TEXT,
    branch_id INTEGER,
    duration_seconds INTEGER,
    user_agent TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_user_events_user_created ON user_events(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_user_events_type_created ON user_events(event_type, created_at);
CREATE INDEX IF NOT EXISTS idx_user_events_page ON user_events(page, created_at);

-- 036: persistent once-per-branch/day dedup for Z-agent brrr alerts.
--
-- The Z agent's cron passes are separate processes (23:00 UTC primary +
-- 16 half-hourly backfills), so in-memory dedup in utils/notify.py cannot
-- stop the same give-up failure from alerting on every pass. Rows here are
-- INSERT OR IGNOREd before sending; only a fresh insert may notify.
--
-- kind: 'z_fetch_fail' (per-run hard failure, s3) |
--       'missing_z'    (post-backfill completeness check, s2)

CREATE TABLE IF NOT EXISTS z_alert_log (
    branch_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    kind TEXT NOT NULL,
    sent_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(branch_id, date, kind)
);

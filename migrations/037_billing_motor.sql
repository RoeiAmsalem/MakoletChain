-- 037: billing motor — sync run-log + per-manager alert-state tracking.
--
-- billing_sync_runs: one row per SUMIT sync, whoever triggered it —
--   source 'auto' (scheduled sweep) / 'manual' (רענן button) / 'payment'
--   (sync-on-return from SUMIT). Feeds the /admin/billing "סונכרן לאחרונה"
--   header and the sweep's failure retry/alert logic.
--
-- manager_billing.alert_state/alert_date: the last ALERTED paywall state per
--   manager ('ok'/'warning'/'warning_final'/'locked') + when — transition
--   alerts fire only when the computed state differs, so repeated sweep runs
--   never re-send the same alert.

CREATE TABLE IF NOT EXISTS billing_sync_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,          -- IL time 'YYYY-MM-DD HH:MM:SS'
    finished_at TEXT,
    source TEXT NOT NULL,              -- 'auto' | 'manual' | 'payment'
    ok INTEGER,                        -- 1 success / 0 fail
    payments_seen INTEGER,
    paid_managers INTEGER,
    error TEXT
);

ALTER TABLE manager_billing ADD COLUMN alert_state TEXT;
ALTER TABLE manager_billing ADD COLUMN alert_date TEXT;

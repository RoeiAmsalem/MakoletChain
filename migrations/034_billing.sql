-- 034_billing.sql
-- Stage 1 of SUMIT billing: per-MANAGER billing config + last-known payment status.
--
-- Billing is per manager (one ₪179 charge per manager), NOT per branch — so the
-- table is keyed UNIQUE(user_id), one row per manager. sumit_tag = str(user_id):
-- the value we set as the SUMIT customer ExternalIdentifier on each manager's
-- payment link (?customerexternalidentifier=<user_id>). user_id was chosen over
-- branch_id because two managers can share a branch (e.g. דניס + דניס בדיקה both on
-- 9015/9018), which would collide a branch_id tag; user_id is unique per manager
-- and models "one charge per manager" exactly.
--
-- active=0 by default → NOBODY is billed until Roei toggles a manager on from
-- /admin/billing. Rows are created lazily (INSERT OR IGNORE) by the page, so this
-- migration only defines the shape — it never decides who gets billed.
--
-- last_paid_date / last_status are filled by the read-only sync (Task 5), which
-- pulls SUMIT payments for the current month and matches them to a manager via
-- the tag join. This migration creates no rows and touches no SUMIT data.

CREATE TABLE IF NOT EXISTS manager_billing (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id        INTEGER NOT NULL,
    sumit_tag      TEXT,
    fee            REAL DEFAULT 179,
    active         INTEGER DEFAULT 0,
    last_paid_date TEXT,
    last_status    TEXT,
    updated_at     TEXT DEFAULT (datetime('now')),
    UNIQUE(user_id)
);

CREATE INDEX IF NOT EXISTS idx_manager_billing_user
    ON manager_billing(user_id);

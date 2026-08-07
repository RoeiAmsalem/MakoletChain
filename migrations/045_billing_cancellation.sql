-- 045: manager-initiated subscription cancel (the cancel button on /account).
-- cancelled_at (IL date) marks a manager who cancelled their SUMIT standing
-- order: access stays until paid_until, then locks directly (no grace, no
-- reminder/lock emails, no layer-C alerts). A payment dated on/after
-- cancelled_at clears it (re-subscribe). cancelled_recurring_id keeps the
-- SUMIT RecurringCustomerItemID that was cancelled, for audit.
ALTER TABLE manager_billing ADD COLUMN cancelled_at TEXT;
ALTER TABLE manager_billing ADD COLUMN cancelled_recurring_id INTEGER;

-- Every cancel ATTEMPT (success or failure) is logged — this is our first
-- intentional SUMIT write, so the audit trail is non-negotiable.
CREATE TABLE IF NOT EXISTS billing_cancellations (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id           INTEGER NOT NULL,
    requested_at      TEXT NOT NULL,
    sumit_customer_id INTEGER,
    recurring_item_id INTEGER,
    ok                INTEGER NOT NULL DEFAULT 0,
    api_status        INTEGER,
    message           TEXT
);
CREATE INDEX IF NOT EXISTS idx_billing_cancellations_user
    ON billing_cancellations(user_id);

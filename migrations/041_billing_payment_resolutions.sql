-- 041: per-payment resolution memory for the SUMIT sweep.
-- A payment whose receipt-join failed used to block the 1-call skip path
-- FOREVER (unmatched>0 every run → full ~11-call sweep daily). Now each
-- payment converges to a terminal resolution:
--   pending     — join failed, receipt may still appear; blocks the skip
--   matched     — tag resolved; stored so future runs never re-fetch the doc
--   unmatchable — pending for BILLING_UNMATCHABLE_AFTER_DAYS distinct days;
--                 alerted once (🟡), never blocks or costs a call again
CREATE TABLE IF NOT EXISTS billing_payment_resolutions (
    payment_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    resolution TEXT NOT NULL DEFAULT 'pending',
    tag TEXT,
    seen_days INTEGER NOT NULL DEFAULT 1,
    first_seen_date TEXT,
    last_seen_date TEXT,
    resolved_at TEXT
);

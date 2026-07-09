-- 040_billing_lock_email.sql
-- Lock-notification email dedup: ONE email per manager per lock EVENT.
--
-- locked_email_sent_month holds the 'YYYY-MM' of the last lock email that
-- SMTP actually accepted. The sweep's lock-email pass (run_lock_pass in
-- scripts/billing_reminder.py) sends when _billing_state == 'locked' AND this
-- flag != current month — so the mail goes out the first sweep that sees the
-- manager locked (the transition morning), never repeats while they stay
-- locked that month, and a pay → re-lock in a later month gets exactly one
-- more (new month = new flag; same pattern as reminder_sent_month, mig 039).
-- Set ONLY on SMTP success: a failed send retries at the next sweep.

ALTER TABLE manager_billing ADD COLUMN locked_email_sent_month TEXT;

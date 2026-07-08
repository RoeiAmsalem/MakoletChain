-- 039_billing_reminder.sql
-- Payment-reminder email dedup: ONE reminder per manager per month.
--
-- reminder_sent_month holds the 'YYYY-MM' of the last reminder that SMTP
-- actually accepted. The reminder job (scripts/billing_reminder.py) skips any
-- manager whose value equals the current month, so a manager who stays unpaid
-- gets exactly one email — not daily spam. The flag is set ONLY on a
-- successful send: a failed SMTP attempt leaves it untouched, so the job
-- naturally retries the next morning. Month rollover needs no reset — an old
-- 'YYYY-MM' simply no longer matches.

ALTER TABLE manager_billing ADD COLUMN reminder_sent_month TEXT;

-- 038: billing event-driven rework — API-call accounting + sweep skip-path.
--
-- SUMIT confirmed API read calls are METERED (plan actions ×5 = monthly call
-- quota, overage billed), so every sync now records how many SUMIT calls it
-- made and what the newest payment it saw was:
--
--   api_calls          — SUMIT HTTP calls this sync made (thread-local counter
--                        in utils/sumit.py). /admin/billing sums the month.
--   last_payment_id    — max SUMIT payment ID among valid payments seen.
--   last_payment_date  — its date. Together with payments_seen these let the
--                        next sync detect "nothing new" and skip the
--                        documents/list + per-document detail reads entirely.
--   unmatched          — valid this-month payments whose receipt-document join
--                        failed (tag unresolved). Skip is only allowed when
--                        the previous run resolved everything (unmatched=0),
--                        so a late-appearing receipt is never skipped over.
--   skipped            — 1 when the run took the cheap skip path.

ALTER TABLE billing_sync_runs ADD COLUMN api_calls INTEGER;
ALTER TABLE billing_sync_runs ADD COLUMN last_payment_id INTEGER;
ALTER TABLE billing_sync_runs ADD COLUMN last_payment_date TEXT;
ALTER TABLE billing_sync_runs ADD COLUMN unmatched INTEGER;
ALTER TABLE billing_sync_runs ADD COLUMN skipped INTEGER;

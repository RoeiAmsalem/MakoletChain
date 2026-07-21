-- 042: manual payment→manager assignment (/admin/billing).
-- payment_date/amount are captured when the sweep first tracks an unresolved
-- payment, so the assign control can set last_paid_date without any SUMIT
-- call. assigned_by/assigned_at audit who resolved a payment by hand.
ALTER TABLE billing_payment_resolutions ADD COLUMN payment_date TEXT;
ALTER TABLE billing_payment_resolutions ADD COLUMN amount REAL;
ALTER TABLE billing_payment_resolutions ADD COLUMN assigned_by INTEGER;
ALTER TABLE billing_payment_resolutions ADD COLUMN assigned_at TEXT;

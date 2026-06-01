-- Migration 005: support manual electricity entry per branch
-- electricity_source ∈ ('iec', 'manual', NULL)
-- NULL = not configured yet (existing behavior preserved)
ALTER TABLE branches ADD COLUMN electricity_source TEXT DEFAULT NULL;

-- Set electricity_source='iec' for branches that already have IEC configured
UPDATE branches SET electricity_source = 'iec' WHERE iec_token IS NOT NULL;

-- Add month column to electricity_invoices for manual entries (YYYY-MM format)
-- IEC entries use invoice_number for uniqueness; manual entries use month
ALTER TABLE electricity_invoices ADD COLUMN month TEXT;

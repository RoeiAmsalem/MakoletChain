-- Migration: Add IEC (Israel Electric Company) integration tables
-- Adds iec_* columns to branches + electricity_invoices table

ALTER TABLE branches ADD COLUMN iec_token TEXT;
ALTER TABLE branches ADD COLUMN iec_user_id TEXT;
ALTER TABLE branches ADD COLUMN iec_bp_number TEXT;
ALTER TABLE branches ADD COLUMN iec_contract_id TEXT;
ALTER TABLE branches ADD COLUMN iec_last_sync_at TIMESTAMP;

CREATE TABLE IF NOT EXISTS electricity_invoices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id INTEGER NOT NULL,
    invoice_number TEXT,
    period_label TEXT,
    amount REAL NOT NULL,
    due_date DATE,
    is_paid INTEGER DEFAULT 0,
    source TEXT DEFAULT 'iec_api',
    raw_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (branch_id, invoice_number),
    FOREIGN KEY (branch_id) REFERENCES branches(id)
);

CREATE INDEX IF NOT EXISTS idx_electricity_invoices_branch ON electricity_invoices(branch_id);
CREATE INDEX IF NOT EXISTS idx_electricity_invoices_unpaid ON electricity_invoices(branch_id, is_paid);

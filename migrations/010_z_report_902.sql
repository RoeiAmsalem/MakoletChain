-- Migration 010: parallel Z-report capture from Aviv BI report 902.
-- Staging-only validation pipeline that runs alongside Gmail-Z on prod to
-- confirm BI 902 PDFs match the Gmail Z totals before any cutover.
-- Never written to by daily_sales or read by the home page; isolation is
-- the safety mechanism.

CREATE TABLE IF NOT EXISTS z_report_902 (
    branch_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    z_number INTEGER,
    amount REAL,
    transactions INTEGER,
    avg_per_txn REAL,
    payment_breakdown TEXT,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(branch_id, date)
);

CREATE INDEX IF NOT EXISTS idx_z_report_902_date ON z_report_902(date);

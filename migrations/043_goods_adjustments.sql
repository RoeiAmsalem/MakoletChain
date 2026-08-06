-- 043_goods_adjustments.sql
-- Manual monthly goods adjustments.
--
-- Carries a VERIFIED goods figure (e.g. franchise-delivered merchandise that the
-- BilBoy ingest filter excludes — Option B, no auto-ingest) into the goods total
-- that feeds גולמי / profit, WITHOUT faking documents. Admin-managed only.
-- Rendered as a clearly-labeled line ("התאמות ידניות"); NEVER shown in the
-- supplier/document/budget listings (those keep the raw goods_documents sum).

CREATE TABLE IF NOT EXISTS goods_adjustments (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id  INTEGER NOT NULL,
    month      TEXT    NOT NULL,               -- 'YYYY-MM'
    label      TEXT    NOT NULL,
    amount     REAL    NOT NULL,
    created_by INTEGER,                         -- users.id of the admin; NULL = system seed
    created_at TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(branch_id, month, label)
);

CREATE INDEX IF NOT EXISTS idx_goods_adjustments_branch_month
    ON goods_adjustments(branch_id, month);

-- Seed the audited July 2026 franchise-merchandise figures (verified read-only,
-- EAN-deduped against same-day direct-supplier docs):
--   9018 דפנה : net-new ₪3,679  -> July גולמי 100,692.68 → 97,013.68 (~₪97,014)
--   9015 הגנה : net-new ₪11,735 -> July גולמי 221,112.29 → 209,377.29 (~₪209,377)
INSERT OR IGNORE INTO goods_adjustments (branch_id, month, label, amount, created_by)
VALUES
    (9018, '2026-07', 'סחורה דרך זיכיונות המכולת', 3679,  NULL),
    (9015, '2026-07', 'סחורה דרך זיכיונות המכולת', 11735, NULL);

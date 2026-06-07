-- Migration 030: chain-wide product catalog (PROOF OF CONCEPT, staging).
--
-- Standalone — NOT wired into /goods, budget, or the doc view. Populated by
-- scripts/build_product_catalog.py from BilBoy invoice line-items
-- (agents.bilboy.fetch_doc_detail → GET /customer/doc). Anchored on the stable
-- product code (productId == line id). The supplier is NOT on the line — it
-- comes from the document (goods_documents.supplier) — so the same product can
-- arrive under different suppliers across stores; suppliers_seen > 1 flags that
-- mis-file mess.

CREATE TABLE IF NOT EXISTS products (
    product_id        TEXT PRIMARY KEY,   -- BilBoy line productId (stable code)
    barcode           TEXT,               -- GTIN / catalogNumber
    name              TEXT,
    supplier          TEXT,               -- most-common supplier across the chain
    suppliers_seen    INTEGER DEFAULT 0,  -- distinct supplier count (>1 = mis-file)
    latest_price      REAL,               -- newest observation chain-wide (ex-VAT)
    latest_price_date TEXT,
    last_seen         TEXT,               -- newest doc_date for this product
    doc_count         INTEGER DEFAULT 0,  -- distinct docs the product appeared in
    updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Raw rows feeding the catalog — price history + most-common-supplier source +
-- mis-file detection. One row per (product, branch, doc) so a re-run is
-- idempotent (INSERT OR IGNORE).
CREATE TABLE IF NOT EXISTS product_observations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id  TEXT NOT NULL,
    branch_id   INTEGER NOT NULL,
    doc_id      TEXT,                      -- bilboy_doc_id
    doc_date    TEXT,
    supplier    TEXT,                      -- from the document, not the line
    price       REAL,                      -- priceWithoutVat (unit cost ex-VAT)
    qty         REAL,
    UNIQUE(product_id, branch_id, doc_id)
);

CREATE INDEX IF NOT EXISTS idx_prodobs_product ON product_observations (product_id);
CREATE INDEX IF NOT EXISTS idx_prodobs_doc     ON product_observations (doc_id);

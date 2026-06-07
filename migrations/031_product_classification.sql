-- Migration 031: catalog classification of franchise-filed (זיכיונות) products.
--
-- The catalog (migration 030) now ALSO ingests line-items from the זיכיונות
-- franchise docs that the normal goods sync excludes (catalog-only — /goods and
-- budget still never count זיכיונות). These columns let us auto-map a
-- franchise-filed product to its REAL supplier when the same product_id also
-- appears under a real supplier elsewhere in the chain.

ALTER TABLE products ADD COLUMN raw_supplier          TEXT;  -- franchise name if seen under זיכיונות, else NULL
ALTER TABLE products ADD COLUMN suggested_supplier    TEXT;  -- most-common REAL supplier for this product_id
ALTER TABLE products ADD COLUMN classification_status TEXT;  -- 'auto' | 'needs-review' | NULL (not a זיכ product)

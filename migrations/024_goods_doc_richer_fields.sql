-- 024_goods_doc_richer_fields.sql
-- Capture additional FREE fields already present in each doc of the BilBoy
-- /customer/docs/headers response (NO extra API calls, NO dedup changes):
--   total_without_vat — authoritative pre-VAT total from BilBoy (was derived /1.17)
--   paid              — 1 = שולם, 0 = לא שולם
--   bilboy_status     — BilBoy lifecycle status (3/5/7 kept; 9 dropped upstream)
--   bilboy_doc_id     — BilBoy document UUID; needed for ON-DEMAND line-item
--                       detail via GET /customer/doc?docId=<uuid>
-- The 5-layer dedup + status=9-drop / keep-3/5/7 rules in agents/bilboy.py are
-- unchanged — these are display/enrichment columns only.

ALTER TABLE goods_documents ADD COLUMN total_without_vat REAL;
ALTER TABLE goods_documents ADD COLUMN paid INTEGER;
ALTER TABLE goods_documents ADD COLUMN bilboy_status INTEGER;
ALTER TABLE goods_documents ADD COLUMN bilboy_doc_id TEXT;

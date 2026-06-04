-- Migration 025: durable record of unrecognized זiכ (franchise) line items.
--
-- The זiכ fixed-expense capture (agents/zikyonot_fixed.py) matches a fixed list
-- of named items (rent, catalog, ad-fund, club, arnona, electricity, water).
-- Anything fee-like it does NOT recognize must never be silently dropped — it is
-- upserted here so Roei can see + classify it on /admin/franchise-classifier.
-- Nothing here is auto-added to fixed_expenses or goods; this is surface-only.
--
-- status: 'pending' (needs review) | 'classified' (handled) | 'ignored' (dismissed).
-- UNIQUE(branch_id, month, item_name) drives idempotent upserts and the
-- "alert once per NEW distinct item name" logic (a re-seen item is not new).

CREATE TABLE IF NOT EXISTS zik_unclassified (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL REFERENCES branches(id),
  month TEXT NOT NULL,
  item_name TEXT NOT NULL,
  amount REAL,
  doc_ref TEXT,
  first_seen TEXT DEFAULT (datetime('now')),
  last_seen TEXT DEFAULT (datetime('now')),
  status TEXT DEFAULT 'pending'
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_zik_unclassified
  ON zik_unclassified(branch_id, month, item_name);

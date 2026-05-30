-- Default franchise fee (זיכיונות) = 5% of revenue for every active branch.
--
-- The franchise fee applies chain-wide, so it's a DEFAULT row rather than a
-- per-store manual entry. Stored exactly like any other "% מהכנסות" expense:
--   name='זיכיונות', expense_type='monthly', pct_value=5.0, amount=0
-- (the % is driven by pct_value>0; amount stays 0 and is computed live from
-- income via _get_fixed_total: income * pct_value / 100).
--
-- Scope: CURRENT MONTH ONLY (no historical backfill — past P&L is left as-is;
-- branch 126 already has hand-entered זיכיונות rows for prior months).
-- Future months propagate automatically via _ensure_monthly_expenses()
-- carry-forward (expense_type='monthly'). It's a DEFAULT, not a lock: a branch
-- can edit/remove it, and removal sticks — next month's carry copies from the
-- month without the row.
--
-- INSERT OR IGNORE + UNIQUE(branch_id, month, name) make this idempotent and
-- dedup-safe: branches that already have a זיכיונות row this month (e.g. 126)
-- are skipped. id<>9999 is a safety net (no branch 9999 exists today).

INSERT OR IGNORE INTO fixed_expenses
  (branch_id, month, name, amount, expense_type, pct_value)
SELECT id, strftime('%Y-%m', 'now'), 'זיכיונות', 0, 'monthly', 5.0
FROM branches
WHERE active = 1 AND id <> 9999;

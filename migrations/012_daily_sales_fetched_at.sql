-- Track when a daily_sales row was first written. The 902 bridge populates
-- this on INSERT OR IGNORE so /sales can show "שעת משיכה" per row. Existing
-- rows and other writers (Gmail-Z, live_provisional) leave it NULL and the
-- UI renders "—" in that case.

ALTER TABLE daily_sales ADD COLUMN fetched_at TEXT;

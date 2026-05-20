-- Migration 009: Demo CEO user for the chain-wide aggregate view.
--
-- Used to demo the "תצוגה על כל הרשת" mode for tomorrow's meeting with the
-- chain CEO. Hash below is werkzeug.security.generate_password_hash('Demo2026')
-- — scrypt format, compatible with check_password_hash. Pre-computed because
-- the migration runner is pure SQL.
--
-- INSERT OR IGNORE keeps this safe to re-run and to cherry-pick to prod even
-- if the user already exists. The user gets NO rows in user_branches: the CEO
-- role grants visibility to all active branches via _list_visible_branches().

INSERT OR IGNORE INTO users (name, email, password_hash, role, active)
VALUES (
  'Demo CEO',
  'demo@makoletchain.com',
  'scrypt:32768:8:1$hmSBUKwcYlsfnAQH$9a8bc51c3fd2d69890a78061d6f43c8c1affbc4381d7d3435efab8de520e60ccaef9c7ac56bebb5acc0fce2e31aa2bd8a88665ac1366c021e556233b2062158a',
  'ceo',
  1
);

-- Migration 009: demo CEO user for the May 21 2026 demo.
-- CEO role grants all-branch access via _list_visible_branches /
-- ROLES_ALL_BRANCHES — DO NOT add user_branches rows for this user.
-- Idempotent: INSERT OR IGNORE on the UNIQUE email. password_hash is a
-- werkzeug generate_password_hash('Demo2026') digest (login uses
-- werkzeug check_password_hash, NOT bcrypt).
-- Columns match how managers are seeded in app.py
-- (name, email, password_hash, role, active).

INSERT OR IGNORE INTO users (name, email, password_hash, role, active)
VALUES (
  'Demo CEO',
  'demo@makoletchain.com',
  'scrypt:32768:8:1$TBrJK7ZOW8kHGD3s$a85cfd88449ca77c9fb0afa1132cb33d4e875957346b05cc80e06f27364fa6c66a024a76cb1038590997f2b194eb0c896ee5665de7aaa543efe1ae83bd3a55b7',
  'ceo',
  1
);

-- MakoletChain Schema — all tables are branch-aware

-- Core config
CREATE TABLE IF NOT EXISTS branches (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  city TEXT,
  active INTEGER DEFAULT 1,
  aviv_user_id TEXT,
  aviv_password TEXT,
  bilboy_user TEXT,
  bilboy_pass TEXT,
  gmail_label TEXT,
  franchise_supplier TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  role TEXT DEFAULT 'manager',
  active INTEGER DEFAULT 1,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS user_branches (
  user_id INTEGER REFERENCES users(id),
  branch_id INTEGER REFERENCES branches(id),
  PRIMARY KEY (user_id, branch_id)
);

-- Financial data (all branch-aware)
CREATE TABLE IF NOT EXISTS daily_sales (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL REFERENCES branches(id),
  date TEXT NOT NULL,
  amount REAL DEFAULT 0,
  transactions INTEGER DEFAULT 0,
  source TEXT DEFAULT 'z_report',
  UNIQUE(branch_id, date)
);

CREATE TABLE IF NOT EXISTS goods_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL REFERENCES branches(id),
  doc_date TEXT,
  supplier TEXT,
  ref_number TEXT,
  amount REAL,
  doc_type INTEGER,
  UNIQUE(branch_id, ref_number)
);

CREATE TABLE IF NOT EXISTS fixed_expenses (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL REFERENCES branches(id),
  month TEXT NOT NULL,
  name TEXT NOT NULL,
  amount REAL NOT NULL,
  expense_type TEXT DEFAULT 'monthly',
  pct_value REAL,
  locked INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS employee_hours (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL REFERENCES branches(id),
  month TEXT NOT NULL,
  employee_name TEXT NOT NULL,
  total_hours REAL DEFAULT 0,
  total_salary REAL DEFAULT 0,
  source TEXT DEFAULT 'csv',
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(branch_id, month, employee_name)
);

CREATE TABLE IF NOT EXISTS live_sales (
  branch_id INTEGER NOT NULL REFERENCES branches(id),
  date TEXT NOT NULL,
  amount REAL,
  transactions INTEGER,
  last_updated TEXT,
  fetched_at TEXT,
  PRIMARY KEY (branch_id, date)
);

CREATE TABLE IF NOT EXISTS agent_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL,
  agent TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  status TEXT DEFAULT 'running',
  docs_count INTEGER DEFAULT 0,
  amount REAL DEFAULT 0,
  message TEXT,
  duration_seconds REAL DEFAULT 0,
  dismissed INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS reset_tokens (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES users(id),
  token TEXT UNIQUE NOT NULL,
  expires_at TEXT NOT NULL,
  used INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now'))
);

-- Insert branch 126 (empty data, credentials TBD)
INSERT OR IGNORE INTO branches (id, name, city, aviv_user_id, gmail_label, franchise_supplier)
VALUES (126, 'מכולת אינשטיין', 'תל אביב', 'S33834', 'איינשטיין', 'זיכיונות המכולת בע"מ');

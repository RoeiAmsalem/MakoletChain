# MakoletChain — Claude Code Reference

## Project Overview
Multi-tenant financial BI dashboard for grocery store chains.
Each branch gets a manager login, sees only their own data.
CEO (Roei) sees all branches via the ops dashboard.

## Workflow
1. Claude (claude.ai) = Architect — thinks, plans, writes prompts
2. Claude Code = Contractor — writes and executes code
3. Roei = Project Manager — pastes prompts, reports results

Always start by reading this file.
Always commit when done.
Always deploy via GitHub — never rsync/scp.
Prompts use: Read CLAUDE.md first. and TASK N separators.

---

## Infrastructure

### Server
- Provider: Hetzner CPX32, Helsinki
- IP: 204.168.201.244
- SSH alias: ssh makolet-chain
- Path: /opt/makolet-chain
- Services: makolet-chain (Flask) + makolet-chain-scheduler (APScheduler)

### Deploy Command
git add -A && git commit -m 'message'
git push origin main
ssh makolet-chain 'cd /opt/makolet-chain && git pull origin main && pip install -r requirements.txt --break-system-packages && systemctl restart makolet-chain && systemctl restart makolet-chain-scheduler'

### Web Stack
- Nginx -> Cloudflare Tunnel -> Flask:8080
- Live URL: https://app.makoletdashboard.com
- Domain: makoletdashboard.com (Namecheap + Cloudflare DNS)
- Cloudflare Tunnel ID: 057e3fc0-a416-4f6d-97ba-39d982bcadf3
- Server IP hidden behind Cloudflare

### Database
- SQLite at db/makolet_chain.db
- All tables are branch-aware (branch_id column)

---

## Stack
- Python + Flask + SQLite + HTML/Chart.js
- Templates: RTL Hebrew, dark theme #0d1526, mobile-responsive 390px
- Auth: Flask session-based, login_required decorator
- Roles: ceo (sees all branches) | manager (locked to own branch)

---

## Database Schema (key tables)

branches:
  id, name, city, active
  aviv_user_id, aviv_password
  bilboy_user, bilboy_pass       -- JWT Bearer token (1yr expiry)
  gmail_label                    -- unique word in Z-report email subject
  franchise_supplier             -- supplier name to EXCLUDE from BilBoy
  avg_hourly_rate REAL           -- weighted avg from last month CSV
  hours_this_month REAL          -- updated by aviv scraper
  hours_baseline REAL            -- last night 23:30 authoritative total
  hours_updated_at TEXT

users:
  id, name, email, password_hash, role (ceo/manager), active

user_branches:
  user_id, branch_id

daily_sales:
  branch_id, date, amount, transactions, source (z_report/provisional)

goods_documents:
  branch_id, ref_number, supplier_id, supplier_name, doc_type, doc_date, amount, month

fixed_expenses:
  branch_id, name, amount, expense_type (חודשי/חד פעמי/% מהכנסות), month

employees:
  id, branch_id, name, role (בוקר/ערב/מנהל), hourly_rate, active

employee_hours:
  id, branch_id, month, employee_name, total_hours, total_salary, source (csv/estimate)
  UNIQUE(branch_id, month, employee_name)

agent_runs:
  id, branch_id, agent, started_at, finished_at, status, docs_count, amount, message, duration_seconds, dismissed

reset_tokens:
  id, user_id, token, expires_at, used

---

## Agents

### bilboy.py — Nightly goods sync (02:00)
- API: https://app.billboy.co.il:5050/api
- Auth: JWT Bearer token from branches.bilboy_pass (expires Mar 2027)
- Token obtained manually: log into BilBoy web app -> DevTools -> Network -> copy Authorization header
- On 401: brrr alert sent, agent_runs status=error
- Strategy: full month delete + reinsert nightly
- CRITICAL: NEVER include docs from branches.franchise_supplier
- 5-layer dedup: lstrip('0'), batch dedup, franchise filter, zero-amount filter, reconciliation verify
- Reconciliation diff > 500 -> brrr warning

### gmail_agent.py — Nightly email processing (02:00)
- IMAP to makoletdashboard@gmail.com using GMAIL_APP_PASSWORD from .env
- Old address makoletdeshboard@gmail.com (typo) forwards to new one permanently
- Shimon's Gmail (shimonmakolet@gmail.com) has forwarding filter:
  from:avivpost@avivpos.co.il + subject:נוכחות באקסל → forwards to makoletdashboard@gmail.com
- Branch 127 (Gal) sends attendance CSV directly to makoletdashboard@gmail.com
- Z-reports: finds emails from avivpost@avivpos.co.il matching branches.gmail_label
  - Downloads PDF, parses total with regex
  - Saves to daily_sales
- Attendance CSV: finds emails with 'נוכחות באקסל' + branch label in subject
  - CSV columns: עובד, יום בשבוע, תאריך כניסה, תאריך יציאה, הערות, כמות שעות
  - If email arrives on 1st-5th of month -> report belongs to PREVIOUS month
  - Employee name matching (see Employee Name Matching section below)
  - Calculates salary = hours x employee.hourly_rate from employees table
  - Saves to employee_hours, updates branches.avg_hourly_rate
  - Skips month if already processed

### aviv_live.py — Live revenue scraper (every 5 min during store hours)

**Primary: Aviv BI REST API**
- Base: https://bi1.aviv-pos.co.il:8443/avivbi/v2/
- Status: https://bi1.aviv-pos.co.il:65010/raw/status/plain
- Login: POST /account/login with {user, password} → returns token
- Auth: Authtoken: <token> header (single-use — must refresh before each call)
- Refresh: POST /account/refresh with Authtoken header → new token
- Branches: POST /account/branches → list of branches with IDs
- Query engine: POST /dashboard/query — SQL-like queries on tables:
  - deals: sum, branch, tip, tax, hour, type
  - items: branch, name, code, price, cost, sum, weight, count, tax, supplier, type, date, hour, family
  - employees: branch, id, name, position
- Bulk queries: POST /dashboard/query/envelope
- Receipts: POST /raw/deals/list
- 102 reports: GET /reports?branch=X
- Status fields: dealTotal, dealCount, cancellationTotal, discountTotal,
  runningDealTotal, runningDealCount, currentEmployeeHours, totalEmployeeHours,
  currentEmployeeCount, payments[], firstDealOpen, tmUpdate, z, zCreate
- Timing: ~1 second per branch (was 15-20s with Playwright)
- Employee hours: POST /employees/sales?type=all — per-employee breakdown
- Cancellations/Discounts: from :65010/raw/status/plain — cancellationTotal, discountTotal
- Running deals: runningDealTotal, runningDealCount — shown as secondary KPI row when values > 0

**Aviv BI REST API reference (full mapping):**
| Endpoint | Purpose | Our usage |
|---|---|---|
| POST /account/login | Get token | aviv_live, aviv_employees, sales-by-hour |
| POST /account/refresh | Refresh token | Before each call |
| POST /dashboard/query | SQL-like query | sales-by-hour (deals table) |
| POST /employees/sales?type=all | Employee hours | aviv_employees agent |
| GET :65010/raw/status/plain | Live status | aviv_live (revenue, hours, cancellations) |

**Fallback: Playwright scraper**
- Playwright headless Chromium -> bi-aviv.web.app/status
- bi-aviv.web.app uses Firebase — goes down when monthly bandwidth quota exceeded
- If REST API fails → falls back to Playwright automatically

**Common behavior:**
- Credentials: branches.aviv_user_id + branches.aviv_password
- Scrapes: daily revenue + transactions
- Zero detection: amount=0 after non-zero -> save provisional Z, brrr alert
- Store hours Israel time:
  Sun-Thu: 06:30-23:30
  Fri: 06:30-19:00
  Sat: 16:30-23:30
- Outside hours: SILENT SKIP — no DB write, no agent_runs entry
- Also scrapes TWO employee hours fields:
  'שעות עובדים מתחילת החודש' -> monthly total (authoritative)
  'שעות עובדים במשמרת' -> current shift only

### aviv_employees.py — Daily employee hours sync (23:45)
- API: POST /employees/sales?type=all via Aviv BI REST API
- Fetches per-employee hours breakdown (more accurate than Gmail CSV, daily updates vs monthly)
- Uses same login flow: POST /account/login → Authtoken header
- Same employee name matching logic as gmail_agent (exact → strip suffix → first name → fuzzy)
- Pending matches go to employee_match_pending table for manager review
- Supplements Gmail CSV attendance data

### Sales by Hour API
- GET /api/sales-by-hour?month=&branch_id= returns 12 buckets (2-hour windows starting 06:30)
- Data source: Aviv BI POST /dashboard/query on deals table grouped by hour
- Home page shows 3 summary cards: peak hour, slowest hour, avg per hour
- Only displays when Aviv API returns data

### Hours scraping (separate from revenue)
- 16:00 daily (scrape_hours_midday):
  - Scrapes 'שעות עובדים במשמרת' (current shift)
  - hours_this_month = hours_baseline + shift_hours
- 23:30 daily (scrape_hours_end_of_day):
  - Scrapes 'שעות עובדים מתחילת החודש' (authoritative total)
  - hours_this_month = authoritative_total
  - hours_baseline = authoritative_total (saved for tomorrow 16:00)

---

## Scheduler Jobs
- nightly_sync: 02:00 IL — BilBoy + Gmail for all active branches
- aviv_early: 06:30-06:55 IL — Aviv live revenue (early window)
- aviv_live: 07:00-22:55 IL every 5 min — Aviv live revenue
- hours_midday: 16:00 IL — hours estimate (baseline + shift)
- hours_end_of_day: 23:30 IL — authoritative hours total
- aviv_employees: 23:45 IL — per-employee hours from Aviv BI API

---

## Salary Calculation — Single Source of Truth

Function: _calculate_salary_cost(branch_id, current_month) in app.py
Used by: /api/summary (home page), /employees page, /api/history
BOTH PAGES ALWAYS SHOW THE SAME NUMBER.

Logic: Salary = SUM(employee_hours.total_hours × employees.hourly_rate) for the month.
No estimation. API is the source of truth, CSV is verification.

Sources (employee_hours.source):
- 'aviv_api' — daily from aviv_employees agent (source of truth)
- 'csv' — end-of-month Gmail CSV (verification only when API data exists)

CSV Verification:
- When CSV arrives and API data exists: compares hours, flags discrepancies > 0.5h
- Discrepancies stored in employee_hours_discrepancies table
- Manager resolves via UI: accept API, accept CSV, or ignore
- When no API data exists: CSV saved normally as fallback

---

## Employee Name Matching (CSV → employees table)
- CSV names often include employee ID prefix: "441 עידן בקון" → strip leading numbers
- CSV names often include branch suffix: "עידן בקון איינשטיין" → strip known suffixes
- Same employee can appear with different name variants in same CSV
  (e.g., "עידן בקון" for manual entries, "עידן בקון איינשטיין" for clock entries)
  → parser tracks by employee ID, keeps longest name variant
- Matching logic: exact → strip suffix → first name → fuzzy overlap
- Low confidence matches → saved to employee_match_pending table for manager review
- Pending matches UI on /employees page with approve/reject/reassign

---

## Fixed Expenses — Details
- 3 types: חודשי (monthly) / חד פעמי (one-time) / % מהכנסות (percent of income)
- % type expenses (pct_value > 0) store amount=0 in DB, calculated live from income
- _get_fixed_total(branch_id, month, income, db) helper calculates % types dynamically
- Monthly (חודשי) expenses auto-carry-forward to next month via _ensure_monthly_expenses()
- Only daily_sales and goods_documents count as "real data" for history start month

---

## Auth & Security
- Login: email + password (bcrypt hash)
- Session: Flask session, SECRET_KEY from .env
- Remember me: 30-day session
- Rate limiting: Nginx 5r/m on /login
- Branch isolation: ALL API routes use session.get('branch_id') ONLY — never from URL params
- Password reset: Resend -> noreply@makoletdashboard.com, 30min token, single-use
- HTTPS via Cloudflare Tunnel (server IP hidden)

---

## Users
- admin@makolet.com — CEO — Roei
- shimonmakolet@gmail.com — manager — Shimon (dad, branch 126)
- Branch 127 — המכולת תיכון (Gal) — onboarding in progress

### Branch 127 — המכולת תיכון Status
- Aviv credentials: Tichon123/Tichon123 ✅
- BilBoy token: set, expires Mar 2027 ✅
- Gmail label: התיכון ✅
- IEC contract: ⏳ pending
- Employees: ⏳ pending (Gal hasn't sent list yet)
- Fixed expenses: ⏳ pending

---

## Notifications (brrr)
- URL in .env as BRRR_URL
- HTTP GET with User-Agent: MakoletChain/1.0 (Cloudflare bypass)
- Helper: utils/notify.py -> notify(title, message)
- Free tier expires Apr 9 2026 — subscribe before then
- All notify() calls use plain English messages (no Hebrew technical jargon)
- Aviv Live: alerts after 6 consecutive failures (~30 min) via _check_consecutive_failures()
- Recovery alert fires on first success after 6+ consecutive failures
- Message format example: "Aviv BI is down — bandwidth quota exceeded on their end."
- BilBoy alerts: token expired, reconciliation gap, general errors
- Gmail alerts: auth failed, no Z-reports found, pending employee matches

---

## Email (Resend)
- API key in .env as RESEND_API_KEY
- From: noreply@makoletdashboard.com
- Region: eu-west-1 Ireland
- Used for: password reset emails only

---

## .env Variables (server: /opt/makolet-chain/.env)
SECRET_KEY
GMAIL_ADDRESS=makoletdashboard@gmail.com
GMAIL_APP_PASSWORD
RESEND_API_KEY
BRRR_URL

---

## Pages Built
- / — home: KPI tiles, revenue chart, P&L table
- /sales — Z-reports + PDF preview
- /goods — BilBoy docs, supplier badges
- /employees — employee cards CRUD, hours KPI, salary estimate, history table
- /fixed-expenses — 3 types: חודשי / חד פעמי / % מהכנסות
- /ops — CEO only: branch cards, agent timeline, alerts, server health, auto-refresh 60s
- /login, /forgot-password, /reset-password

---

## Onboarding Checklist (per new branch)
1. BilBoy JWT token: log in -> DevTools -> Network -> Authorization header
2. Aviv credentials: user_id + password (usually same value)
3. Gmail label: unique word in Z-report email subject from avivpost@avivpos.co.il
4. Franchise supplier name to exclude from BilBoy
5. Branch ID = franchise number
6. Create manager user + link to branch in user_branches table

---

## Lessons Learned / Key Bugs Fixed
- franchise_supplier must use straight double quote " not Hebrew typographic ״
- BilBoy suppliers must be batched in chunks of 30 (URL length limit)
- Aviv Live Playwright: use domcontentloaded + wait_for_timeout(3000), never networkidle
- % fixed expenses: store amount=0, calculate live — never store stale calculated amount
- Monthly fixed expenses: carry forward from last month with data automatically
- History table: start from first month with income/goods data (not employee hours)
- Aviv BI REST API (bi1.aviv-pos.co.il) is totally separate from Firebase (bi-aviv.web.app)

---

## Critical Rules
- NEVER include franchise_supplier docs in BilBoy sync
- NEVER get branch_id from URL — always from session
- NEVER rsync/scp — always deploy via GitHub
- NEVER write complex Python as inline SSH — write .py file, push, run via SSH
- Aviv outside store hours = silent skip, no DB write, no agent_runs entry
- Attendance CSV on 1st-5th = previous month report
- Salary = ONE function _calculate_salary_cost(), used everywhere
- All times stored as UTC, displayed as Israel time (Asia/Jerusalem)

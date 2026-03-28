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
- IMAP to makoletdeshboard@gmail.com using GMAIL_APP_PASSWORD from .env
- Z-reports: finds emails from avivpost@avivpos.co.il matching branches.gmail_label
  - Downloads PDF, parses total with regex
  - Saves to daily_sales
- Attendance CSV: finds emails with 'נוכחות באקסל' + branch label in subject
  - CSV columns: עובד, יום בשבוע, תאריך כניסה, תאריך יציאה, הערות, כמות שעות
  - If email arrives on 1st-5th of month -> report belongs to PREVIOUS month
  - Smart fuzzy name matching: strips store suffixes (איינשטיין etc.), handles middle names, token overlap
  - Calculates salary = hours x employee.hourly_rate from employees table
  - Saves to employee_hours, updates branches.avg_hourly_rate
  - Skips month if already processed

### aviv_live.py — Live revenue scraper (every 5 min during store hours)
- Playwright headless Chromium -> bi-aviv.web.app/status
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

---

## Salary Calculation — Single Source of Truth

Function: _calculate_salary_cost(branch_id, current_month) in app.py
Used by: /api/summary (home page), /employees page, /api/history
BOTH PAGES ALWAYS SHOW THE SAME NUMBER.

Priority:
1. CSV salary data for current month -> SUM(total_salary) from employee_hours
2. CSV hours but no salary -> hours x employee.hourly_rate weighted by last month distribution, saved back to employee_hours.total_salary
3. No CSV -> hours_this_month x avg_hourly_rate

avg_hourly_rate auto-recalculates when:
- Monthly CSV processed by Gmail agent
- Any employee rate changed via UI (POST/PUT/DELETE /api/employees)

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

---

## Notifications (brrr)
- URL in .env as BRRR_URL
- HTTP GET with User-Agent: MakoletChain/1.0 (Cloudflare bypass)
- Helper: utils/notify.py -> notify(title, message)
- Free tier expires Apr 9 2026 — subscribe before then

---

## Email (Resend)
- API key in .env as RESEND_API_KEY
- From: noreply@makoletdashboard.com
- Region: eu-west-1 Ireland
- Used for: password reset emails only

---

## .env Variables (server: /opt/makolet-chain/.env)
SECRET_KEY
GMAIL_ADDRESS=makoletdeshboard@gmail.com
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

## Critical Rules
- NEVER include franchise_supplier docs in BilBoy sync
- NEVER get branch_id from URL — always from session
- NEVER rsync/scp — always deploy via GitHub
- NEVER write complex Python as inline SSH — write .py file, push, run via SSH
- Aviv outside store hours = silent skip, no DB write, no agent_runs entry
- Attendance CSV on 1st-5th = previous month report
- Salary = ONE function _calculate_salary_cost(), used everywhere
- All times stored as UTC, displayed as Israel time (Asia/Jerusalem)

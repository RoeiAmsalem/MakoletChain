# MakoletChain — Claude Code Reference

Multi-tenant financial BI dashboard for grocery store chains.
Each branch has a manager login that sees only their data; the admin (Roei)
sees all branches via `/ops` and manages users/branches via `/admin/*`.

---

## CC Workflow Rules (read first)

1. Read `.claude-skills/lean-output/SKILL.md` at the start of every session for output formatting rules. Default to terse, structured output. Verbose only when explicitly requested.
2. Always read this file at the start of a task.
3. Prompts coming in from claude.ai will mark **PASTE INTO: [DEV/STAGING]** or
   **[PROD/MAIN]**. Respect that — never mix the two trees.
4. Wrap prompt bodies in a single fenced code block. Use `TASK N` separators.
5. **Deploy via git only** — never `rsync`/`scp`.
6. **Schema changes via migrations only** — never ad-hoc `sqlite3` ALTER on the
   server. (Migration 006 retroactively formalized the few cases where this rule
   was broken historically.)
7. After a deploy, verify by `grep`-ing the new code on the server and/or
   hitting `/health`.
8. Mixed Hebrew/English: keep a line break before/after each Hebrew chunk so
   diffs stay readable.
9. Always commit when done.

---

## Stack

- Python + Flask + SQLite + HTML/Chart.js
- Templates: RTL Hebrew, dark theme `#0d1526`, mobile-responsive @ 390px
- Auth: Flask session (`SECRET_KEY` from `.env`), bcrypt password hashes,
  `login_required` decorator, 30-day "remember me"
- Roles: `admin` (sees all branches, /ops, /admin/*) | `manager` (locked to
  their assigned branches via `user_branches`). A manager with 2+ branches
  gets a branch switcher in the header.
- Email is normalized to lowercase on insert/login (case-insensitive).

---

## Servers

### Prod — `ssh makolet-chain`  →  `204.168.201.244`  →  `/opt/makolet-chain`

- Provider: Hetzner CPX32, Helsinki
- Services: `makolet-chain` (Flask) + `makolet-chain-scheduler` (APScheduler)
- Gunicorn: `-w 1 --threads 4` (single worker + threads). Required because the
  IEC onboarding wizard holds session state in process memory
  (`_iec_wizard_sessions`). Do NOT increase `-w` without first moving wizard
  sessions to redis/sqlite.
- Deploy: `ssh makolet-chain "/opt/makolet-chain/deploy-prod.sh"`
  - `git pull origin main` → `pip install -r requirements.txt` →
    `scripts/migrate.py` → restart `makolet-chain` AND `makolet-chain-scheduler`
  - Why both restarts: scheduler.py imports change with code; without
    restarting the scheduler service, cron jobs run stale code.

### Staging — `ssh makolet-chain`  →  `/opt/makolet-chain-staging`

- Service: `makolet-chain-staging` (Flask only — no separate scheduler)
- URL: `https://staging.makoletdashboard.com` (basic auth `roei` / `makoletstaging2026`)
- Port: 8081
- Deploy: `ssh makolet-chain "/opt/makolet-chain-staging/deploy.sh"`
  - `git pull origin dev` → `pip install` → `scripts/migrate.py` →
    restart `makolet-chain-staging`
- Flags in `/opt/makolet-chain-staging/.env`:
  `ENABLE_AGENTS=false` (scheduler skips jobs),
  `DRY_RUN_GMAIL=true`,
  `BRRR_SILENT=true`,
  `PORT=8081`
- Refresh staging DB from prod: `ssh makolet-chain "/opt/makolet-chain-staging/refresh-staging-db.sh"`
- Run one agent on staging: `ssh makolet-chain "/opt/makolet-chain-staging/run-agent.sh bilboy"`
- See `STAGING.md` for the full reference.

### Legacy — `ssh makolet`  →  `/opt/makolet-dashboard`  (NOT active)

Single-tenant predecessor. No active dev. Kept around for historical reference;
do not deploy to it.

### Web stack

- Nginx → Cloudflare Tunnel → Flask:8080 (prod) / :8081 (staging)
- Live URL: `https://app.makoletdashboard.com`
- Domain: `makoletdashboard.com` (Namecheap registrar + Cloudflare DNS)
- Cloudflare Tunnel ID: `057e3fc0-a416-4f6d-97ba-39d982bcadf3`
- Server IP hidden behind Cloudflare. HTTPS terminated at Cloudflare.
- Login is rate-limited at Nginx: 5r/m on `/login`.

---

## Local Folders

- `~/Desktop/MakoletChain`     → `main` branch → prod
- `~/Desktop/MakoletChain-dev` → `dev`  branch → staging

Promote staging→prod by merging `dev` → `main` and pushing main, then run
`deploy-prod.sh` on the server.

---

## Database

- SQLite at `db/makolet_chain.db`
- All operational tables are branch-aware (`branch_id` column)
- Schema changes go through `migrations/NNN_description.sql` (see
  `migrations/README.md`). `scripts/migrate.py` is idempotent — it tracks
  applied migrations in the `_migrations` table and is safe to re-run.
- Both deploy scripts run `migrate.py` automatically.

### Per-branch source-of-truth tables

`branches`, `users`, `user_branches`, `daily_sales` (Z-reports),
`goods_documents` (BilBoy), `fixed_expenses`, `electricity_invoices`,
`employees`, `employee_hours`, `employee_match_pending`, `employee_shifts`,
`z_report_902`, `z_department_sales`, `agent_runs`, `alerts`, `_migrations`,
`reset_tokens`.

### Key columns

```
branches:
  id, name, city, active
  aviv_user_id, aviv_password
  bilboy_user, bilboy_pass             -- legacy per-store JWT (kept in DB but
                                       --   no longer collected by the UI; only
                                       --   used as fallback when chain mode off)
  bilboy_branch_id                     -- BilBoy chain customerBranchId (mig 015)
  gmail_label                          -- unique word in Z-report email subject
  franchise_supplier                   -- supplier name to EXCLUDE from BilBoy
  avg_hourly_rate REAL                 -- weighted avg from last month CSV
  hours_this_month REAL                -- updated by aviv scraper
  hours_baseline REAL                  -- last night 23:30 authoritative total
  hours_updated_at TEXT
  ui_start_month TEXT                  -- per-branch override for /history start

users:
  id, name, email, password_hash, role (admin/manager), active

user_branches:
  user_id, branch_id                   -- many-to-many; managers can have 2+

daily_sales:
  branch_id, date, amount, transactions, source (z_report/provisional)

goods_documents:
  id, branch_id, doc_date, supplier, ref_number, amount, doc_type
  UNIQUE(branch_id, ref_number)
  -- NOTE: no month/supplier_id/supplier_name/status columns. Month is derived
  --   as substr(doc_date,1,7). Status filtering happens in bilboy.py at sync
  --   time (drop 9, keep 7) — it is NOT persisted.

fixed_expenses:
  branch_id, name, amount, expense_type (חודשי/חד פעמי/% מהכנסות),
  pct_value, month
  UNIQUE(branch_id, name, month, expense_type)  -- migration 004

electricity_invoices:
  branch_id, period_start, period_end, amount, source (iec/manual), ...

employees:
  id, branch_id, name, role (בוקר/ערב/מנהל), hourly_rate, active

employee_hours:
  id, branch_id, month, employee_name, total_hours, total_salary,
  source (aviv_api/aviv_report/csv)
  UNIQUE(branch_id, month, employee_name)

employee_match_pending:
  -- low-confidence CSV/API name matches awaiting manager review
  -- formalized by migration 006

employee_shifts:                       -- migration 022
  id, branch_id, month, employee_name, shift_date, start_ts, end_ts,
  hours, day_of_week, is_open, source (aviv_report), created_at
  -- per-shift drill-down from report 301; DISPLAY-ONLY, never summed for salary.
  -- is_open=1 = "אין יציאה" (no clock-out). Full-overwrite per
  --   (branch_id, month, source), same strategy as employee_hours.

agent_runs:
  id, branch_id, agent, started_at, finished_at, status, docs_count, amount,
  message, duration_seconds, dismissed
```

---

## Pages

- `/`                  — home: KPI tiles (revenue, basket avg, expenses, salary), live
                         revenue tile with **סל ממוצע**, revenue chart, P&L table,
                         electricity tile, sales-by-hour summary cards
- `/sales`             — Z-reports + PDF preview/pages
- `/goods`             — BilBoy docs, supplier badges, totals
- `/employees`         — employee cards CRUD, hours KPI, salary, history table,
                         pending-match review UI (approve/reject/reassign)
- `/fixed-expenses`    — 3 expense types (`חודשי` / `חד פעמי` / `% מהכנסות`)
                         + electricity tile + auto-row sourced from IEC
- `/ops`               — admin only: branch tiles, agent timeline, alerts,
                         server health, IEC accuracy table, agent on-demand run,
                         60s auto-refresh
- `/admin/branches`    — admin only: branch CRUD + initial manager creation
- `/admin/users`       — admin only: user-to-branch assignments (NEW May 9)
- `/login`, `/forgot-password`, `/reset-password`

There is **no** standalone `/electricity-history` page. Electricity history is
exposed as JSON at `/api/electricity/history` and surfaced in the home tile +
`/fixed-expenses`.

Static asset cache busting via `static_v()` Jinja helper (mtime query param) —
prevents browsers from serving stale CSS/JS after deploy.

---

## Agents

### `bilboy.py` — Nightly goods sync (02:00)

- API: `https://app.billboy.co.il:5050/api`
- Auth: **chain mode** — one JWT in `.env` as `BILBOY_CHAIN_TOKEN`
  (userId=136 / יניב בן אלי, expires **2027-05-27**). Per-call branch via
  `branches.bilboy_branch_id` (BilBoy's internal store id).
- Per-store fallback: `branches.bilboy_pass` still works for branches without
  a `bilboy_branch_id` mapping (used when `BILBOY_USE_CHAIN=0` or chain token
  missing).
- Token obtained manually from BilBoy web app → DevTools → Network →
  Authorization header. Token NEVER goes in code, CLAUDE.md, or commits.
- Strategy: full-month delete + reinsert nightly
- 5-layer dedup: `lstrip('0')`, batch dedup, franchise filter, zero-amount
  filter, reconciliation verify
- **CRITICAL: NEVER include docs from `branches.franchise_supplier`**
- Status filtering: drop `status=9` (superseded) docs; keep `status=7`
  (replacement invoices). See commit `8c702e0`.
- Reconciliation diff > 500 → brrr warning. On 401 → brrr alert,
  `agent_runs.status=error`.
- Suppliers must be batched in chunks of 30 (URL length limit).

#### BilBoy chain mapping (one token, 18 branches)

Chain BilBoy is the **single source** for goods/docs across the chain. One JWT
in `.env` (`BILBOY_CHAIN_TOKEN`, 1yr, expires 2027-05-27, userId=136 Yaniv)
sees all 18 stores. Per-store BilBoy tokens are no longer required for any
store in the chain.

`/customer/docs/headers` requires `?branches=<bilboy_branch_id>` (the chain
endpoint did NOT accept `customerBranchId=N` — returns 400).
`/customer/suppliers` accepts `?customerBranchId=<bilboy_branch_id>`.

| bilboy_branch_id | local branch id | name                                      |
| ---------------- | --------------- | ----------------------------------------- |
| 99               | 9001            | קדיש לוז                                  |
| 106              | 9006            | נווה זיו                                  |
| 107              | 9011            | ויצמן                                     |
| 122              | 9013            | לימן                                      |
| 123              | 9012            | בצת                                       |
| 124              | 9010            | שומרת                                     |
| 125              | 9007            | ז'בוטינסקי                                |
| 126              | 126             | אינשטיין (Shimon)                         |
| 170              | 127             | גל ודרור / התיכון                         |
| 483              | 9015            | ההגנה                                     |
| 2267             | 9016            | קריית טבעון                               |
| 2337             | 9017            | רמת השרון                                 |
| 2653             | 9002            | קק"ל                                      |
| 3327             | 9014            | קרן היסוד                                 |
| 3606             | 9009            | שבטי ישראל                                |
| 3684             | 9018            | דפנה                                      |
| 4724             | 9019            | כפר סירקין                                |
| 4901             | 9020            | רמת גן                                    |

Token rotation: edit `.env`, restart `makolet-chain` + `makolet-chain-scheduler`.
The token NEVER goes into CLAUDE.md, git history, or logs.

### `gmail_agent.py` — Nightly email processing (02:00)

- IMAP to `makoletdashboard@gmail.com` using `GMAIL_APP_PASSWORD` from `.env`
- The old typo address `makoletdeshboard@gmail.com` permanently forwards to it
- Shimon's Gmail (`shimonmakolet@gmail.com`) has a forwarding filter:
  `from:avivpost@avivpos.co.il + subject:נוכחות באקסל` →
  `makoletdashboard@gmail.com`
- Branch 127 (Gal) sends attendance CSV directly to `makoletdashboard@gmail.com`
- **Z-reports**: emails from `avivpost@avivpos.co.il` matching
  `branches.gmail_label`. Downloads PDF, parses total, saves to `daily_sales`.
- **Attendance CSV** (`נוכחות באקסל` + branch label):
  - Columns: `עובד, יום בשבוע, תאריך כניסה, תאריך יציאה, הערות, כמות שעות`
  - **If email arrives 1st–5th of month → report belongs to PREVIOUS month**
  - Skips month if already processed
  - Saves to `employee_hours` (source=`csv`), updates `branches.avg_hourly_rate`
- Note (May 9): the UI no longer ingests CSV (commit `0ae8547`); CSV is now
  only verification when API data exists. CSV-only entry is still a fallback
  if no API data is present.

### `aviv_live.py` — Live revenue scraper (every 5 min during store hours)

**Primary: Aviv BI REST API** (`https://bi1.aviv-pos.co.il`)

- Base: `:8443/avivbi/v2/`  •  Status: `:65010/raw/status/plain`
- Login: `POST /account/login {user, password}` → token
- Auth: `Authtoken: <token>` header (single-use — refresh before each call)
- Refresh: `POST /account/refresh` (with current Authtoken)
- Branches: `POST /account/branches`
- Query: `POST /dashboard/query` — SQL-like over tables `deals`, `items`,
  `employees`. Bulk: `POST /dashboard/query/envelope`.
- Receipts: `POST /raw/deals/list`  •  102 reports: `GET /reports?branch=X`
- Status fields: `dealTotal, dealCount, cancellationTotal, discountTotal,
  runningDealTotal, runningDealCount, currentEmployeeHours,
  totalEmployeeHours, currentEmployeeCount, payments[], firstDealOpen,
  tmUpdate, z, zCreate`
- Timing: ~1s per branch (was 15–20s with Playwright)

| Endpoint                       | Purpose         | Used by                                       |
| ------------------------------ | --------------- | --------------------------------------------- |
| POST /account/login            | Get token       | aviv_live, aviv_employees, aviv_employees_report, sales-by-hour |
| POST /account/refresh          | Refresh token   | Before each call                              |
| POST /dashboard/query          | SQL-like query  | sales-by-hour (deals table)                   |
| POST /employees/sales?type=all | Per-employee    | aviv_employees agent (REMOVED from schedule)  |
| GET :65010/raw/status/plain    | Live status     | aviv_live (revenue, hours, cancellations)     |

**Fallback: Playwright headless Chromium → `bi-aviv.web.app/status`**.
Firebase-backed and goes down when monthly bandwidth quota is exceeded; if the
REST API fails, agent falls back. Use `domcontentloaded + wait_for_timeout(3000)`,
**never `networkidle`**.

**Common behavior**

- Credentials: `branches.aviv_user_id` + `branches.aviv_password`
- Scrapes: daily revenue + transactions + two hours fields
  - `שעות עובדים מתחילת החודש` → monthly authoritative total
  - `שעות עובדים במשמרת` → current shift only
- Zero detection: amount=0 after non-zero → save provisional Z + brrr alert
- **Store hours (Israel time) — live-scrape guard**: Sun–Thu 06:30–**23:00**,
  Fri 06:30–19:00, Sat 16:30–23:00 (`aviv_live.STORE_SCHEDULE`). NB: the
  `hours_end_of_day` job runs at 23:30 as a plain cron — it is NOT gated by this
  guard.
- **Outside hours: SILENT SKIP** — no DB write, no `agent_runs` entry
- Alerts after 6 consecutive failures (~30 min) via `_check_consecutive_failures()`;
  recovery alert on first success after 6+ failures.

### `aviv_employees.py` — REMOVED from the schedule (cutover 2026-05-10)

- Old per-employee agent (`POST /employees/sales?type=all`, `source='aviv_api'`).
  Its 15:00 + 23:45 jobs are **commented out** in `scheduler.py` and never run.
  The file may still sit in `agents/` but is dead — do not re-enable it (it
  clobbered `aviv_report` rows via the `employee_hours` UNIQUE constraint).

### `aviv_employees_report.py` — Employer-report agent (report 301)

- Pulls Aviv employer report (report 301), `employee_hours.source = 'aviv_report'`.
- Auth: **chain account** (`AVIV_EMP_USE_CHAIN=1` on prod) — one login reused
  across branches; per-branch via `aviv_branch_id`. 30s jitter between branches.
  Retry 3× / 30s backoff; 401 → re-login. POS offline (404) = silent skip.
- Schedule: Sun–Thu 16:00 (current month) • Sun–Thu 23:30 (current+prev) •
  Fri 20:00 (current) • Sat 23:30 (current+prev). See the consolidated table.
- Pending name matches → `employee_match_pending`; parser strips Aviv ID prefix
  (`441 עידן בקון` → `עידן בקון`).
- Alerts: unmatched>0 or open_shifts≥3 → brrr digest; chain-login fail →
  critical brrr (dedup `aviv_chain_auth`).

#### Shift breakdown + open-shift banner (migration 022)

- `parse_employer_report` also emits **per-shift** rows (date, entry/exit ts,
  hours, day-of-week, `is_open`) parsed from the existing report-301 columns —
  no extra Aviv call. Written to `employee_shifts` (full-overwrite per
  branch+month+source), keyed by the matched canonical name.
- UI: each `/employees` card has an expandable **פירוט משמרות** list
  (`/api/employee-shifts`). Open shifts (`אין יציאה` / no clock-out) drive a
  **red pulse banner** on `/employees` AND home (`/api/open-shifts`).
- **CRITICAL: shifts are NEVER summed for salary.** The monthly
  `employee_hours.total_hours` (from the report subtotal row) stays the sole
  salary input — `_calculate_salary_cost` is untouched. Shifts are drill-down
  only. The banner respects the per-branch visibility floor (`visible_from`).

#### Monthly reconciliation — `reconcile_previous_month` (10th, 23:00 IL)

- BilBoy-style: on the **10th** only (date-gated; `--force` bypasses), re-pulls
  the **previous month** for all branches and **silently full-overwrites**
  `employee_hours` + `employee_shifts` (source=`aviv_report`) so late Aviv
  corrections self-heal.
- Compares stored prev-month totals before vs after; if **hours Δ>0.5h or
  salary Δ>₪10** → logs `CHANGED` + brrr alert ("Hours changed — {branch}").
  Otherwise logs `OK`. Anomaly alerts suppressed during the re-pull.
- Dedicated log: `logs/hours_reconciliation.log`.
- Blast radius: previous month only; does not touch current month, `csv` rows,
  or other agents.

### `aviv_z_report.py` — Z report 902 (LIVE on prod)

- Reads Aviv **report 902** (העתק Z) PDF + XLS. Chain auth (`AVIV_Z_USE_CHAIN=1`).
- Writes `z_report_902` (z_number, amount, transactions, avg, payment_breakdown,
  trigger_type, auth_source; UPSERT on branch+date) and `z_department_sales`
  (DELETE+INSERT dept breakdown). Saves PDF to `data/pdfs/<branch>/z_<date>.pdf`
  (shared with the Gmail-Z preview on `/sales`). Records a closed-day sentinel
  so backfill passes don't re-probe.
- **`AVIV_Z_TO_DAILY_SALES=1` on prod** → also mirrors the Z total into
  `daily_sales` (`INSERT OR IGNORE`, source=`z_report`). So `daily_sales` now has
  two writers: `gmail_agent` (PDF) and `aviv_z_report` (902 mirror).
- **Trigger is HOST CRONTAB, not APScheduler**: `23:00` full pull +
  `02:00–09:00 every 30 min --missing-only` backfill
  (`/opt/makolet-chain/run_z_report_prod.sh`).
- Failure: Z-list fetch wall-clock budget 240s, capped-linear backoff
  (5/10/15/20/30s); 401 one-shot re-auth; dept-XLS errors swallowed (never fail
  the Z row); per-branch isolated.

### `iec_agent.py` / IEC integration — Electricity invoices

- Israeli Electric Corp API is **geo-blocked** outside Israel
- IEC sync runs on a Kamatera **Israeli VPS** (`185.253.75.56`) and POSTs
  results back to prod via `/api/internal/iec-sync`
- 06:00 IL daily — scheduler triggers via SSH:
  `ssh makolet-iec /opt/makolet-iec/venv/bin/python /opt/makolet-iec/iec_sync.py`
- Onboarding: 3-step web wizard at `/admin/branches` (and `/api/iec/onboard/*`).
  Auto-syncs after onboarding; manager can also trigger via `/api/iec/sync`.
- Manual entry mode (commit `4954363`): branches with no IEC integration can
  enter electricity bills directly. `electricity_invoices.source` is `iec` or
  `manual`.
- Internal endpoints: `/api/internal/iec-branches`, `/api/internal/iec-sync`,
  `/api/internal/iec-sync-error`, `/api/internal/iec-onboard`.

### `Sales by Hour API`

- `GET /api/sales-by-hour?month=&branch_id=` returns 12 buckets (2-hour windows
  starting 06:30)
- Source: `POST /dashboard/query` on `deals` table grouped by hour
- Home page renders 3 summary cards: peak hour, slowest hour, avg per hour;
  only shown when Aviv API returns data.

### Hours scraping (separate from revenue, by `aviv_live.py`)

- 16:00 IL daily (`scrape_hours_midday`):
  - Scrapes `שעות עובדים במשמרת` (current shift)
  - `hours_this_month = hours_baseline + shift_hours`
- 23:30 IL daily (`scrape_hours_end_of_day`):
  - Scrapes `שעות עובדים מתחילת החודש` (authoritative total)
  - `hours_this_month = authoritative_total`
  - `hours_baseline = authoritative_total` (saved for tomorrow's 16:00 estimate)

---

## Scheduler Jobs (`scheduler.py`) — consolidated daily/monthly rhythm

APScheduler (`BlockingScheduler`, tz=Asia/Jerusalem; `misfire_grace_time=120`,
`coalesce`, `max_instances=1`). On startup: `cleanup_orphaned_runs()` (marks
`running`>1h as `error`/"orphaned on startup") + one immediate aviv_live pass.
`get_active_branches()` = `active=1 AND agents_enabled=1` (excludes demo 9999).

| Job id (IL time)                     | When                       | What                                  | Writes? |
| ------------------------------------ | -------------------------- | ------------------------------------- | ------- |
| `nightly_sync`                       | 02:00                      | BilBoy goods + Gmail Z, all branches  | ✍️ |
| `cleanup_user_events_daily`          | 03:00                      | prune `user_events` >90 days          | ✍️ del |
| `recompute_analytics_cache_daily`    | 03:30                      | rebuild `analytics_cache` aggregates  | ✍️ |
| `iec_sync`                           | 06:00                      | IEC electricity via Israeli VPS (SSH) | ✍️ |
| `aviv_early`                         | 06:30–06:55 /5min          | Aviv live revenue (early window)      | ✍️ |
| `aviv_live`                          | 07:00–22:55 /5min          | Aviv live revenue (chain)             | ✍️ |
| `hourly_sales_alerts`                | 07:00–22:00 /30min         | hourly_sales data-health alerts       | 🔍 read-only |
| `hours_midday`                       | 16:00                      | Hours estimate (baseline + shift)     | ✍️ |
| `aviv_report_weekday_afternoon`      | Sun–Thu 16:00              | Employer-report (current month)       | ✍️ |
| `aviv_report_friday`                 | Fri 20:00                  | Employer-report (current month)       | ✍️ |
| `aviv_report_monthly_recon`          | **10th only, 23:00**       | Prev-month reconciliation + alert     | ✍️ |
| `hours_end_of_day`                   | 23:30                      | Hours authoritative total             | ✍️ |
| `aviv_report_weekday_night`          | Sun–Thu 23:30              | Employer-report (current + prev)      | ✍️ |
| `aviv_report_saturday`               | Sat 23:30                  | Employer-report (current + prev)      | ✍️ |
| **z_report_902** (**host crontab**)  | 23:00 + 02:00–09:00 /30min | Report 902 Z + dept + daily_sales mirror | ✍️ |

The **z_report_902** rows run from the host crontab
(`run_z_report_prod.sh` full @ 23:00; `--missing-only` backfill every 30 min
02:00–09:00), NOT from APScheduler. Deprecated `aviv_employees` jobs (15:00,
23:45) are commented out and do not run.

Heaviest moments: **02:00** (bilboy+gmail ×~18) and **23:00–23:30** (z_report +
recon-on-10th + hours-eod + employer-report night all overlap).

---

## Data-Flow Map (source → agent → table → page)

| Data | source → agent → table → page |
| ---- | ----------------------------- |
| Revenue (live) | Aviv `/raw/status/plain` → `aviv_live` (chain) → `live_sales`+`hourly_sales` → `/` live tile, סל ממוצע, sales-by-hour |
| Revenue (daily Z) | (a) Gmail PDF → `gmail_agent` → `daily_sales` (z_report); (b) Aviv 902 → `aviv_z_report` → `daily_sales` mirror + `z_report_902` → `/sales`, `/`, P&L |
| Departments | Aviv 902 XLS → `aviv_z_report` → `z_department_sales` → `/sales` dept tiles, home KPI tiles |
| Goods | BilBoy API → `bilboy` (chain) → `goods_documents` → `/goods`, P&L |
| Hours (monthly) | Aviv `/raw/status` → `aviv_live` hours jobs → `branches.hours_*` → `/ops`, home |
| Hours per-employee + salary | Aviv report 301 → `aviv_employees_report` (chain) → `employee_hours` → `_calculate_salary_cost` → `/`, `/employees`, `/api/history`, `/ops` |
| Shifts (drill-down + open-shift banner) | Aviv report 301 → `aviv_employees_report` → `employee_shifts` → `/employees` card expand + red banner on `/employees`+`/` |
| Electricity | IEC API (VPS) → `iec_agent` → `electricity_invoices` → `/`, `/fixed-expenses`, `/api/electricity/history` |
| Match review | report 301 / CSV → `employee_match_pending` → `/employees` approve/reject UI |

---

## Salary Calculation — Single Source of Truth

`_calculate_salary_cost(branch_id, current_month, db)` in `app.py` is **the**
function. Used by:

- `/api/summary` (home KPI tile)
- `/employees` page
- `/api/history`
- `/ops` branch tiles (since commit `8fc7fe3`)

Every consumer shows the same number. **Never recompute salary inline.**

Logic: `Salary = SUM(employee_hours.total_hours × employees.hourly_rate)` for
the month. No estimation. API is source of truth, CSV is verification.

`employee_hours.source`:
- `aviv_api`    — legacy `aviv_employees` agent (REMOVED from schedule; only old rows)
- `aviv_report` — daily from `aviv_employees_report` agent (the live path)
- `csv`         — end-of-month Gmail CSV (verification when API data exists,
                  fallback when none does)

CSV verification: when CSV arrives and API data exists, hours are compared and
discrepancies > 0.5h are flagged (`employee_hours_discrepancies` table). The
manager resolves via UI: accept API, accept CSV, or ignore.

---

## Employee Name Matching (CSV/API → `employees`)

- CSV/API names often include an employee ID prefix: `441 עידן בקון` →
  strip leading numbers
- Names often include a branch suffix: `עידן בקון איינשטיין` → strip known
  suffixes
- Same employee can appear with different name variants in one CSV (manual
  entry vs clock entry); parser tracks by employee ID and keeps the longest
  name variant
- Matching: exact → strip suffix → first name → fuzzy overlap
- Low-confidence matches → `employee_match_pending` table for manager review
  on `/employees`
- Adding a new employee from a pending match promotes their hours immediately
- Shared matching logic in `agents/_employee_matching.py`

---

## Fixed Expenses

- 3 types: `חודשי` (monthly) / `חד פעמי` (one-time) / `% מהכנסות` (percent of income)
- `% מהכנסות` rows store `amount=0` and `pct_value > 0`; the actual amount is
  computed live from income via `_get_fixed_total(branch_id, month, income, db)`.
  Never store a stale calculated amount.
- Monthly (`חודשי`) rows auto-carry-forward to the next month via
  `_ensure_monthly_expenses()`.
- Electricity is auto-added as a fixed-expense row sourced from IEC (or manual
  entry); fixed-expenses summary uses **monthly-prorated electricity** so
  totals match the home page (commit `f3e75c6`).
- Race fix: `UNIQUE(branch_id, name, month, expense_type)` (migration 004) +
  duplicate-row cleanup. See `scripts/cleanup_fixed_expenses_dupes.py`.
- History start month: only `daily_sales` and `goods_documents` count as
  "real data" — `electricity_invoices` is excluded (commit `c42ccdc`).

---

## Auth & Security

- Login: email + password (bcrypt)
- Email is normalized to lowercase on insert and lookup (case-insensitive,
  commit `d29937f`)
- Session: Flask `SECRET_KEY` from `.env`; "remember me" 30 days
- Rate limit: Nginx 5r/m on `/login`
- **Branch isolation**: all API routes use `session.get('branch_id')` —
  **never** read `branch_id` from URL params
- Password reset: Resend → `noreply@makoletdashboard.com`, 30 min single-use token
- HTTPS via Cloudflare Tunnel (origin IP hidden)

---

## Notifications (brrr)

- URL in `.env` as `BRRR_URL`
- HTTP GET with `User-Agent: MakoletChain/1.0` (Cloudflare bypass)
- Helper: `utils/notify.py` → `notify(title, message)`
- All `notify()` messages are plain English (no Hebrew jargon)
- Aviv Live alerts after 6 consecutive failures; recovery alert on first
  success after a streak. BilBoy alerts: token expired, reconciliation gap,
  general error. Gmail alerts: auth failed, no Z-reports found, pending
  employee matches.
- **Free tier expires Apr 9 2026 — subscribe before then.** May need
  verification/replacement.
- Why brrr (not Slack/Telegram): single shared HTTP push endpoint, Cloudflare
  bypass via UA header works reliably, no per-user tokens to manage.

---

## Email (Resend)

- API key in `.env` as `RESEND_API_KEY`
- From: `noreply@makoletdashboard.com` (region `eu-west-1` Ireland)
- Used for: password reset emails only

---

## `.env` Variables (`/opt/makolet-chain/.env`)

```
SECRET_KEY
GMAIL_ADDRESS=makoletdashboard@gmail.com
GMAIL_APP_PASSWORD
RESEND_API_KEY
BRRR_URL
ADMIN_PASSWORD            # initial admin seed password (TODO: rotate from 12345)
BILBOY_CHAIN_TOKEN        # chain JWT (userId=136 Yaniv, exp 2027-05-27)

# Chain-account flags — ALL =1 on prod (one Aviv/BilBoy login, many branches):
BILBOY_USE_CHAIN=1        # chain token + bilboy_branch_id (else per-store bilboy_pass)
AVIV_LIVE_USE_CHAIN=1     # aviv_live one multi-branch pull (scheduler USE_CHAIN=True)
AVIV_EMP_USE_CHAIN=1      # employer-report 301 chain login
AVIV_Z_USE_CHAIN=1        # report 902 chain login
AVIV_Z_TO_DAILY_SALES=1   # mirror 902 Z totals into daily_sales (INSERT OR IGNORE)
# Aviv chain creds: AVIV_CHAIN_USER / AVIV_CHAIN_PASS
```

---

## Users (prod)

- `makoletdashboard@gmail.com` — admin — Roei  (was `admin@makolet.com` until 2026-05-09)
- `shimonmakolet@gmail.com`    — manager — Shimon (dad, branch 126)
- `galdar0144@gmail.com`       — manager — Gal (branch 127)

### Branch 127 — המכולת תיכון status

- Aviv credentials: `Tichon123/Tichon123` ✅
- BilBoy: chain via `bilboy_branch_id=170` (גל ודרור), no per-store token needed ✅
- Gmail label: `התיכון` ✅
- IEC contract: ⏳ pending
- Employees: ⏳ pending (Gal hasn't sent list yet)
- Fixed expenses: ⏳ pending

---

## Onboarding Checklist (per new branch)

1. Confirm the store is in Yaniv's chain BilBoy (already true for all 18) and
   note its `bilboy_branch_id` — see the chain mapping table under the bilboy
   agent. No per-store BilBoy token needed.
2. Aviv credentials: `user_id` + `password` (usually same value)
3. Gmail label: a unique word in Z-report email subject from `avivpost@avivpos.co.il`
4. Franchise supplier name to exclude from BilBoy
5. Branch ID = franchise number
6. Create branch + first manager via `/admin/branches`
7. Use `/admin/users` to add additional managers / assign extra branches
8. Onboard IEC via the wizard on `/admin/branches`, or set the branch to manual
   electricity entry

---

## Recent Prod Features (May 2026)

- `a9ddba1` — Multi-branch manager role + branch switcher
- `0ca7d66` — סל ממוצע (avg basket) live tile
- `522d4c2` — Cache-busting via `static_v()`
- `9e66ec0` — Fixed-expenses race fix + UNIQUE constraint (migration 004)
- `4954363` — Manual electricity entry as alternative to IEC
- `96e2e65` — Aviv employer-report agent (parser + scheduler + tests)
- `bbb6968` / `432069b` / `4f5fa97` — employer-report polish
  (strip Aviv ID prefix, surface in pending UI, require hourly rate, promote
  hours on add, card hours-check, /ops surfacing, response-envelope unwrap)
- `8fc7fe3` — `/ops` branch tile salary reconciled via `_calculate_salary_cost`
- `d29937f` — `/admin/users` page + case-insensitive email handling
- `dc3e6d9` — `deploy-prod.sh` restarts scheduler too + migration 006
  (formalizes `employee_match_pending` schema)
- `8c702e0` — BilBoy: drop status=9 superseded, keep status=7 replacements
- `f3e75c6` — Fixed-expenses uses monthly-prorated electricity to match home page

---

## Open Issues / Known Tech Debt

- `gmail_agent.py` still has runtime `ALTER TABLE` blocks. Migration 006 makes
  them no-ops, but the stylistic debt remains.
- `aviv_employees` cutover done (2026-05-10); jobs commented out. The dead
  `agents/aviv_employees.py` file can be deleted.
- `gmail_agent.py`: branches with no `gmail_label` leave an `agent_runs` row
  stuck `running` (early-return after the insert) → swept to "orphaned on
  startup" nightly for the ~16 chain stores. Cosmetic; deferred.
- Branch 9011 (ויצמן): zero `goods_documents` while all other stores have data
  — BilBoy goods sync broken for this branch; needs investigation.
- Historical backfill Oct 2025 – Feb 2026 shows ₪0 (never imported).
- `ADMIN_PASSWORD` still `12345` on prod — rotate this.
- brrr free tier expires 2026-04-09; verify or replace before then.

---

## Lessons Learned / Key Bugs Fixed

- `franchise_supplier` must use a straight double quote `"` — never the Hebrew
  typographic `״`
- BilBoy suppliers must be batched in chunks of 30 (URL length limit)
- Aviv Live Playwright: use `domcontentloaded + wait_for_timeout(3000)` —
  **never `networkidle`**
- `% מהכנסות` fixed expenses: store `amount=0`, calculate live — never store
  a stale calculated amount
- Monthly fixed expenses: carry forward from last month with data
- History start month: first month with `daily_sales` or `goods_documents` —
  not employee hours, not electricity
- Aviv BI REST API (`bi1.aviv-pos.co.il`) is totally separate from Firebase
  (`bi-aviv.web.app`)
- IEC API is geo-blocked outside Israel — must run from Israeli VPS
- Gunicorn `-w 1` is a hard requirement until wizard sessions move out of process

---

## Critical Rules (don't break these)

- **NEVER** include `franchise_supplier` docs in BilBoy sync
- **NEVER** read `branch_id` from a URL — always from session
- **NEVER** rsync/scp — always deploy via git
- **NEVER** run ad-hoc SQL on the server — write a migration
- **NEVER** write complex Python as inline SSH — write the `.py`, push, run via SSH
- Aviv outside store hours = silent skip; no DB write, no `agent_runs` entry
- Attendance CSV arriving 1st–5th of month = previous month report
- Salary = the **one** function `_calculate_salary_cost()`, used everywhere
- **NEVER** sum `employee_shifts` for salary — monthly `employee_hours.total_hours`
  is the sole input; shifts are display-only drill-down
- All times stored as UTC, displayed in Israel time (`Asia/Jerusalem`)

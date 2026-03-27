# MakoletChain — Project Memory

## What It Is
Multi-tenant financial BI dashboard for a grocery store chain.
Same UI as MakoletDashboard but supports 18+ branches.
Each branch has its own data. Managers see their branch(es). CEO sees all.

## Architecture
- Every DB table has a branch_id column
- All queries filter branch_id as parameter
- Scheduler loops over active branches

## Server
- VPS: Hetzner CPX32, Helsinki
- IP: 204.168.201.244
- SSH alias: ssh makolet-chain
- Path: /opt/makolet-chain
- Services: makolet-chain (Flask/Gunicorn port 8080), makolet-chain-scheduler (APScheduler)
- Nginx → Gunicorn → Flask:8080

## Deploy Command (ALWAYS restart both services)
ssh makolet-chain "cd /opt/makolet-chain && git pull origin main && systemctl restart makolet-chain && systemctl restart makolet-chain-scheduler"

## GitHub
https://github.com/RoeiAmsalem/MakoletChain

## Stack
Python + Flask + SQLite + HTML/Chart.js
RTL Hebrew UI, dark theme #0d1526

## Branch 126 — מכולת אינשטיין (first branch, Shimon Amsalem)
- aviv_user_id: S33834
- gmail_label: filter by store name in subject
- bilboy: TBD (same as MakoletDashboard)

## Key Rules
- NEVER use pytz — always use zoneinfo (stdlib)
- Deploy always restarts BOTH services
- deep_test.py must pass after every change
- BilBoy: never include supplier "זיכיונות המכולת בע"מ" in goods docs
- Live scraper hours: 06:30–23:30 Israel time

## Team
- Claude (claude.ai) = Architect
- Claude Code = Contractor
- Roei = Project Manager

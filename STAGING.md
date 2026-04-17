# MakoletChain — Staging Environment

**URL:** https://staging.makoletdashboard.com
**Basic auth:** `roei` / `makoletstaging2026`
**App login:** same as prod (DB is a snapshot copy)

## Local folders
- `~/Desktop/MakoletChain`      → main branch → prod
- `~/Desktop/MakoletChain-dev`  → dev branch → staging

## Deploy to staging
```
cd ~/Desktop/MakoletChain-dev
git add -A && git commit -m "..."
git push origin dev
ssh makolet-chain "/opt/makolet-chain-staging/deploy.sh"
```

## Promote staging → prod
```
cd ~/Desktop/MakoletChain
git pull origin main
git merge dev
git push origin main
ssh makolet-chain "cd /opt/makolet-chain && git pull origin main && systemctl restart makolet-chain"
```

## Refresh staging DB from prod
```
ssh makolet-chain "/opt/makolet-chain-staging/refresh-staging-db.sh"
```

## Run one agent manually on staging
```
ssh makolet-chain "/opt/makolet-chain-staging/run-agent.sh bilboy"
```

## Staging flags (in /opt/makolet-chain-staging/.env)
- `ENABLE_AGENTS=false` → scheduler skips all jobs
- `DRY_RUN_GMAIL=true` → Gmail agent fetches but doesn't mark read
- `BRRR_SILENT=true` → brrr pings suppressed
- `PORT=8081`

## Service management
```
sudo systemctl status makolet-chain-staging
sudo systemctl restart makolet-chain-staging
sudo journalctl -u makolet-chain-staging -f
```

#!/bin/bash
# Aviv BI 902 Z-validation agent — PROD.
# Wired into the host crontab pointing at the prod install (/opt/makolet-chain).
# Prod has no APScheduler Z job, so cron is the sole driver of the Z agent.
# This is a deliberate prod-pathed twin of run_z_report.sh (which is staging-only)
# — kept separate so neither can silently target the wrong install.
#
# Logs append to logs/aviv_z_report.log. The agent itself does try/except
# per branch so one branch's failure won't abort the loop.

set -u

APP_DIR="/opt/makolet-chain"
LOG_DIR="$APP_DIR/logs"
LOG_FILE="$LOG_DIR/aviv_z_report.log"

cd "$APP_DIR" || exit 1
mkdir -p "$LOG_DIR"

# Activate venv + explicit .env load (cron has no shell env).
# shellcheck disable=SC1091
source "$APP_DIR/venv/bin/activate"
set -a
# shellcheck disable=SC1091
source "$APP_DIR/.env"
set +a

{
  echo ""
  echo "=== $(date -Iseconds) aviv_z_report run args=[$*] ==="
  python3 -m agents.aviv_z_report "$@" 2>&1
  echo "=== exit=$? ==="
} >> "$LOG_FILE"

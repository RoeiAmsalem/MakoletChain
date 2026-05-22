#!/bin/bash
# Aviv BI 902 Z-validation agent — STAGING ONLY.
# Wired into staging's system crontab at 02:00 IL; never runs on prod.
#
# Logs append to logs/aviv_z_report.log. The agent itself does try/except
# per branch so one branch's failure won't abort the loop.

set -u

APP_DIR="/opt/makolet-chain-staging"
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
  echo "=== $(date -Iseconds) aviv_z_report run ==="
  python3 -m agents.aviv_z_report 2>&1
  echo "=== exit=$? ==="
} >> "$LOG_FILE"

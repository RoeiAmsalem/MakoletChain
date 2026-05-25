#!/bin/bash
# Aviv employer-report (report 301) — STAGING.
# Times in cron are UTC: Israel = UTC+3 in summer IDT (Mar–Oct), UTC+2 in winter.
# Wrapper mirrors main's twice-daily schedule:
#   Sun–Thu 16:00 IL = 13:00 UTC — current month
#   Sun–Thu 23:30 IL = 20:30 UTC — current + previous month
#   Fri      20:00 IL = 17:00 UTC — current month
#   Sat      23:30 IL = 20:30 UTC — current + previous month
# Logs append to logs/aviv_emp.log.

set -u

APP_DIR="/opt/makolet-chain-staging"
LOG_DIR="$APP_DIR/logs"
LOG_FILE="$LOG_DIR/aviv_emp.log"

cd "$APP_DIR" || exit 1
mkdir -p "$LOG_DIR"

# shellcheck disable=SC1091
source "$APP_DIR/venv/bin/activate"
set -a
# shellcheck disable=SC1091
source "$APP_DIR/.env"
set +a

{
  echo ""
  echo "=== $(date -Iseconds) aviv_emp args=[$*] ==="
  python3 -m agents.aviv_employees_report "$@" 2>&1
  echo "=== exit=$? ==="
} >> "$LOG_FILE"

#!/bin/bash
# Billing motor layer B — scheduled SUMIT sweep (STAGING wrapper).
# Wired into the system crontab every 2h; the script itself enforces the
# 07:00-23:00 IL window and the BILLING_SYNC_ENABLED flag.
#
# Crontab entry (UTC box; 04-20 UTC ≈ 07-23 IL in summer, the in-script IL
# gate absorbs the winter DST shift):
#   10 4,6,8,10,12,14,16,18,20 * * * /opt/makolet-chain-staging/run_billing_sweep.sh

set -u

APP_DIR="/opt/makolet-chain-staging"
LOG_DIR="$APP_DIR/logs"
LOG_FILE="$LOG_DIR/billing_sweep.log"

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
  echo "=== $(date -Iseconds) billing_sweep run ==="
  python3 scripts/billing_sweep.py 2>&1
  echo "=== exit=$? ==="
} >> "$LOG_FILE"

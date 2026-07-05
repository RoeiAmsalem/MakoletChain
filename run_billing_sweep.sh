#!/bin/bash
# Billing motor layer B — scheduled SUMIT sweep (cron wrapper).
# ONCE-DAILY safety net at 09:10 IL — SUMIT meters API calls, so the sweep's
# jobs are: catch payments no event layer saw, pick up SUMIT's automatic
# monthly recurring charges, and run the transition alerts. The script
# enforces the IL hour (BILLING_SWEEP_HOUR, default 09) and the
# BILLING_SYNC_ENABLED flag.
#
# Crontab entries (UTC box; BOTH fire, the in-script IL-hour gate lets exactly
# one through year-round across the DST shift — 06:10 UTC = 09:10 IDT summer,
# 07:10 UTC = 09:10 IST winter):
#   10 6,7 * * * /opt/makolet-chain/run_billing_sweep.sh
#
# APP_DIR is derived from the script's own location, so the same file serves
# both trees (/opt/makolet-chain on prod, /opt/makolet-chain-staging on staging).

set -u

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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

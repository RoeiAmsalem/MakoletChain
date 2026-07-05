#!/bin/bash
# Billing motor layer B — scheduled SUMIT sweep (STAGING wrapper).
# ONCE-DAILY safety net at 09:10 IL (the SUMIT webhook is the primary,
# event-driven sync — SUMIT meters API calls, so the sweep only catches missed
# webhooks, SUMIT's automatic monthly recurring charges, and runs the
# transition alerts). The script enforces the IL hour (BILLING_SWEEP_HOUR,
# default 09) and the BILLING_SYNC_ENABLED flag.
#
# Crontab entries (UTC box; BOTH fire, the in-script IL-hour gate lets exactly
# one through year-round across the DST shift — 06:10 UTC = 09:10 IDT summer,
# 07:10 UTC = 09:10 IST winter):
#   10 6,7 * * * /opt/makolet-chain-staging/run_billing_sweep.sh

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

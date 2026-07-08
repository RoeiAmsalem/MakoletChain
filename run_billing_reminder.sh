#!/bin/bash
# Billing motor layer D — daily payment-reminder email (STAGING wrapper).
# Emails warning-state unpaid managers ONCE per month at 08:30 IL. The script
# enforces the IL hour (BILLING_REMINDER_HOUR, default 08), the
# BILLING_REMINDER_ENABLED kill switch, and dry-run (real sends only when
# BILLING_GMAIL_USER + BILLING_GMAIL_APP_PASSWORD are set AND
# BILLING_REMINDER_DRY_RUN=false).
#
# Crontab entries (UTC box; BOTH fire, the in-script IL-hour gate lets exactly
# one through year-round across the DST shift — 05:30 UTC = 08:30 IDT summer,
# 06:30 UTC = 08:30 IST winter):
#   30 5,6 * * * /opt/makolet-chain-staging/run_billing_reminder.sh

set -u

APP_DIR="/opt/makolet-chain-staging"
LOG_DIR="$APP_DIR/logs"
LOG_FILE="$LOG_DIR/billing_reminder.log"

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
  echo "=== $(date -Iseconds) billing_reminder run ==="
  python3 scripts/billing_reminder.py 2>&1
  echo "=== exit=$? ==="
} >> "$LOG_FILE"

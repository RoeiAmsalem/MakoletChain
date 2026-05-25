#!/bin/bash
# Aviv live multi-branch (chain account) — STAGING ONLY.
# One login + one POST returning all branches at once. The agent itself
# silently skips outside store hours, so this is safe to run round the clock.
# Logs append to logs/aviv_live_chain.log.

set -u

APP_DIR="/opt/makolet-chain-staging"
LOG_DIR="$APP_DIR/logs"
LOG_FILE="$LOG_DIR/aviv_live_chain.log"

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
  echo "=== $(date -Iseconds) aviv_live --chain args=[$*] ==="
  python3 -m agents.aviv_live --chain "$@" 2>&1
  echo "=== exit=$? ==="
} >> "$LOG_FILE"

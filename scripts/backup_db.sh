#!/bin/bash
# Daily off-host backup of the prod SQLite DB -> Roei's Google Drive (via rclone).
#
# Why this exists: until now the only backups were manual same-disk snapshots, so a
# disk/host failure = total data loss. This ships a tiny (~5.5MB) daily copy off-host.
#
#   1. SQLite ONLINE backup (`.backup`) — a consistent snapshot of a live DB.
#      NEVER `cp` a live SQLite file (can capture a torn write mid-transaction).
#   2. Integrity-check the snapshot before trusting it.
#   3. rclone copy -> gdrive:makolet-db-backups  (drive.file scope: rclone only ever
#      sees the folder it creates, never the rest of Roei's Drive).
#   4. Confirm the file actually landed in Drive.
#   5. Retention: prune backups older than RETAIN_DAYS (keeps ~14 dailies).
#   6. brrr alert (critical) on ANY failure — a SILENT backup miss is the worst case.
#
# Read-only on makolet_chain.db. Idempotent (same-day re-run just re-copies).
# Cron: 01:30 UTC = 04:30 IL (host crontab) — after nightly_sync + cleanups,
# before the 05:00 IL Z backfill / morning rush.

set -uo pipefail

APP_DIR="/opt/makolet-chain"
DB="$APP_DIR/db/makolet_chain.db"
LOG_DIR="$APP_DIR/logs"
LOG_FILE="$LOG_DIR/backup_db.log"
REMOTE="gdrive:makolet-db-backups"
RETAIN_DAYS=14
DATE="$(TZ='Asia/Jerusalem' date +%F)"        # IL calendar day
TMP="/tmp/makolet_chain_${DATE}.db"

cd "$APP_DIR" || exit 1
mkdir -p "$LOG_DIR"

# venv + .env: cron has no shell env. Needed for BRRR_URL/BRRR_SILENT and the
# utils.notify import used by the failure alert.
# shellcheck disable=SC1091
source "$APP_DIR/venv/bin/activate"
set -a
# shellcheck disable=SC1091
source "$APP_DIR/.env"
set +a

log() { echo "$(TZ='Asia/Jerusalem' date -Iseconds) $*" >> "$LOG_FILE"; }

fail() {
  local msg="$1"
  log "FAILED: $msg"
  # Critical brrr -> pages immediately, bypasses batching; dedup_key collapses
  # retry storms to one page. UA (MakoletChain/1.0) + BRRR_SILENT handled inside
  # utils.notify. Message kept plain English per project convention.
  BACKUP_ERR="$msg" python3 -c "import os; from utils.notify import notify; notify('DB backup FAILED', os.environ['BACKUP_ERR'], critical=True, dedup_key='db_backup_fail')" >> "$LOG_FILE" 2>&1 \
    || log "WARN: brrr send itself errored"
  rm -f "$TMP"
  exit 1
}

log "=== backup start ($DATE) ==="

# 1. SQLite safe online backup (read-only on the live DB).
[ -f "$DB" ] || fail "source DB not found: $DB"
sqlite3 "$DB" ".backup '$TMP'" || fail "sqlite .backup failed"

# 2. Integrity-check the snapshot before trusting/uploading it.
INTEG="$(sqlite3 "$TMP" 'PRAGMA integrity_check;' 2>&1 | head -1)"
[ "$INTEG" = "ok" ] || fail "integrity_check on snapshot != ok: $INTEG"
SIZE=$(wc -c < "$TMP")
[ "$SIZE" -gt 100000 ] || fail "snapshot suspiciously small: ${SIZE} bytes"
log "snapshot ok — ${SIZE} bytes, integrity=ok"

# 3. Upload to Drive (rclone auto-creates makolet-db-backups on first run).
rclone copy "$TMP" "$REMOTE/" --log-file="$LOG_FILE" --log-level INFO \
  || fail "rclone copy to $REMOTE failed"

# 4. Confirm it actually landed (don't trust a 0-exit alone).
rclone lsf "$REMOTE/" 2>>"$LOG_FILE" | grep -qx "makolet_chain_${DATE}.db" \
  || fail "uploaded file not found in Drive after copy"
log "uploaded + verified in Drive: makolet_chain_${DATE}.db"

# 5. Retention — delete backups older than RETAIN_DAYS (non-fatal if it fails;
#    a missed prune never costs us data, so don't page over it).
rclone delete --min-age "${RETAIN_DAYS}d" "$REMOTE/" --log-file="$LOG_FILE" --log-level INFO \
  || log "WARN: retention prune failed (non-fatal)"
REMAIN=$(rclone lsf "$REMOTE/" 2>/dev/null | grep -c '\.db$' || true)
log "retention done — ${REMAIN} backup(s) remain in Drive"

# 6. Cleanup local snapshot.
rm -f "$TMP"
log "=== backup OK ($DATE) — ${SIZE} bytes, ${REMAIN} in Drive ==="
exit 0

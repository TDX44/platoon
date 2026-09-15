#!/usr/bin/env bash
# Nightly backup of the platoon Postgres database.
#
# Runs on the host (prodsrv02) from platoon-backup.timer, NOT inside the app
# container. It shells into the running "db" compose service to take the dump,
# so the compose stack must be up (it uses the app's own docker-compose.yml).
#
# Dumps use pg_dump's custom format (-Fc), which is already compressed and
# lets pg_restore do a selective/parallel restore later. Every dump is
# verified two ways before it is kept: pg_restore --list must be able to parse
# the archive (catches a truncated or corrupt file, the same guarantee
# PRAGMA integrity_check gave for SQLite), and the live personnel table must
# have at least one row (catches a dump taken against an empty/wrong
# database). --list is run *inside* the db container, not with the host's own
# pg_restore, so the version that reads the archive always matches the
# version that wrote it.
#
# Restore (app stopped, run from the repo root, e.g. /opt/homelab/platoon):
#   # The role must exist first: pg_dump captures GRANTs but not roles, and a
#   # restore into a cluster without platoon_app drops every grant while
#   # still exiting 0 — the app then cannot read its own tables.
#   docker compose -f docker-compose.yml exec -T db psql -U platoon_owner \
#     -d platoon -v app_password="$DB_APP_PASSWORD" -f - < scripts/pg-roles.sql
#   docker compose -f docker-compose.yml exec -T db \
#     pg_restore -U platoon_owner --no-owner --clean --if-exists \
#     -d platoon < backups/<file>.dump
set -euo pipefail

APP_DIR="${PLATOON_APP_DIR:-/opt/homelab/platoon}"
COMPOSE_FILE="$APP_DIR/docker-compose.yml"
DB_USER="platoon_owner"
DB_NAME="platoon"
DEST="$APP_DIR/backups"
KEEP_DAYS="${PLATOON_BACKUP_KEEP_DAYS:-30}"
# Off-box copy. Empty disables it; losing the disk then loses the backups too.
OFFBOX="${PLATOON_BACKUP_OFFBOX:-tdx44@100.101.125.14:backups/platoon}"

compose() { docker compose -f "$COMPOSE_FILE" exec -T db "$@"; }

STAMP="$(date -u +%Y%m%d-%H%M%S)"
DUMP="$DEST/platoon-$STAMP.dump"
STATUS="$DEST/LAST_BACKUP"

fail() {
  echo "FAILED $(date -u +%FT%TZ): $*" | tee "$STATUS" >&2
  rm -f "$DUMP"
  exit 1
}

mkdir -p "$DEST" 2>/dev/null || { echo "cannot create $DEST" >&2; exit 1; }
compose pg_isready -U "$DB_USER" -d "$DB_NAME" >/dev/null 2>&1 \
  || fail "database service not reachable"

# Serialise with any still-running previous backup.
exec 9>"$DEST/.lock"
flock -n 9 || fail "another backup is still running"

compose pg_dump -U "$DB_USER" --format=custom "$DB_NAME" > "$DUMP" \
  || fail "pg_dump failed"

# A backup that cannot be restored is worse than none: it buys false confidence.
compose pg_restore --list < "$DUMP" > /dev/null || fail "pg_restore --list failed on $DUMP"

ROWS="$(compose psql -U "$DB_USER" -d "$DB_NAME" -tAc 'SELECT COUNT(*) FROM personnel')" \
  || fail "personnel count query failed"
[ "$ROWS" -gt 0 ] || fail "database contains 0 personnel — refusing to keep this backup"

find "$DEST" -maxdepth 1 -name 'platoon-*.dump' -mtime "+$KEEP_DAYS" -delete

OFFBOX_NOTE="off-box disabled"
if [ -n "$OFFBOX" ]; then
  if rsync -q -e 'ssh -o BatchMode=yes -o ConnectTimeout=10' \
        "$DUMP" "$OFFBOX/" 2>/dev/null; then
    OFFBOX_NOTE="copied to $OFFBOX"
    remote_host="${OFFBOX%%:*}"; remote_path="${OFFBOX#*:}"
    ssh -o BatchMode=yes -o ConnectTimeout=10 "$remote_host" \
      "find '$remote_path' -maxdepth 1 -name 'platoon-*.dump' -mtime +$KEEP_DAYS -delete" \
      2>/dev/null || true
  else
    # Local dump is good; say so loudly but do not throw it away.
    OFFBOX_NOTE="OFF-BOX COPY FAILED to $OFFBOX"
  fi
fi

echo "OK $(date -u +%FT%TZ)  $ROWS personnel  $(du -h "$DUMP" | cut -f1)  $OFFBOX_NOTE" | tee "$STATUS"
case "$OFFBOX_NOTE" in "OFF-BOX COPY FAILED"*) exit 2 ;; esac

#!/usr/bin/env bash
# Nightly snapshot of the accountability database.
#
# Runs on the host (prodsrv02) from platoon-backup.timer, NOT inside the app
# container, so it still works when the app is down. docker-compose bind-mounts
# ./data, so the live DB is an ordinary host file.
#
# Snapshots use SQLite's online backup API rather than cp: cp of a live DB can
# capture a torn write and produce a file that only fails when you try to
# restore it. Every snapshot is integrity-checked before it is kept.
#
# Restore:  gunzip -c backups/<file>.db.gz > data/accountability.db   (app stopped)
set -euo pipefail

APP_DIR="${PLATOON_APP_DIR:-/opt/homelab/platoon}"
DB="$APP_DIR/data/accountability.db"
DEST="$APP_DIR/backups"
KEEP_DAYS="${PLATOON_BACKUP_KEEP_DAYS:-30}"
# Off-box copy. Empty disables it; losing the disk then loses the backups too.
OFFBOX="${PLATOON_BACKUP_OFFBOX:-tdx44@100.101.125.14:backups/platoon}"

STAMP="$(date -u +%Y%m%d-%H%M%S)"
SNAP="$DEST/accountability-$STAMP.db"
STATUS="$DEST/LAST_BACKUP"

fail() {
  echo "FAILED $(date -u +%FT%TZ): $*" | tee "$STATUS" >&2
  rm -f "$SNAP" "$SNAP.gz"
  exit 1
}

mkdir -p "$DEST" 2>/dev/null || { echo "cannot create $DEST" >&2; exit 1; }
[ -f "$DB" ] || fail "no database at $DB"

# Serialise with any still-running previous snapshot.
exec 9>"$DEST/.lock"
flock -n 9 || fail "another backup is still running"

# Consistent snapshot of a live database.
python3 - "$DB" "$SNAP" <<'PY' || fail "sqlite backup failed"
import sqlite3, sys
src, dst = sys.argv[1], sys.argv[2]
source = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
target = sqlite3.connect(dst)
with target:
    source.backup(target)
target.close()
source.close()
PY

# A backup that cannot be restored is worse than none: it buys false confidence.
CHECK="$(python3 -c "
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
print(c.execute('PRAGMA integrity_check').fetchone()[0])
" "$SNAP")" || fail "integrity check did not run"
[ "$CHECK" = "ok" ] || fail "integrity check said: $CHECK"

ROWS="$(python3 -c "
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
print(c.execute('SELECT COUNT(*) FROM personnel').fetchone()[0])
" "$SNAP")" || fail "snapshot has no personnel table"
[ "$ROWS" -gt 0 ] || fail "snapshot contains 0 personnel — refusing to keep it"

gzip -f "$SNAP" || fail "gzip failed"
find "$DEST" -maxdepth 1 -name 'accountability-*.db.gz' -mtime "+$KEEP_DAYS" -delete

OFFBOX_NOTE="off-box disabled"
if [ -n "$OFFBOX" ]; then
  if rsync -q -e 'ssh -o BatchMode=yes -o ConnectTimeout=10' \
        "$SNAP.gz" "$OFFBOX/" 2>/dev/null; then
    OFFBOX_NOTE="copied to $OFFBOX"
    remote_host="${OFFBOX%%:*}"; remote_path="${OFFBOX#*:}"
    ssh -o BatchMode=yes -o ConnectTimeout=10 "$remote_host" \
      "find '$remote_path' -maxdepth 1 -name 'accountability-*.db.gz' -mtime +$KEEP_DAYS -delete" \
      2>/dev/null || true
  else
    # Local snapshot is good; say so loudly but do not throw it away.
    OFFBOX_NOTE="OFF-BOX COPY FAILED to $OFFBOX"
  fi
fi

echo "OK $(date -u +%FT%TZ)  $ROWS personnel  $(du -h "$SNAP.gz" | cut -f1)  $OFFBOX_NOTE" | tee "$STATUS"
case "$OFFBOX_NOTE" in "OFF-BOX COPY FAILED"*) exit 2 ;; esac

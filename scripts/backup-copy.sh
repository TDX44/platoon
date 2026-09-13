#!/usr/bin/env bash
# Copy the newest production snapshot into personal Google Drive.
#
# Runs on the Windows workstation under WSL from the Scheduled Task
# "Platoon DB backup to Drive". It does NOT run on prodsrv02, because Drive is
# only authenticated here, through Google Drive for Desktop:
#   E: -> /mnt/e   jcarr2006@gmail.com   <- this one
#   G:             jonathon@resyrv.com   <- business account, do not use
# rclone on the server would be the always-on answer, but its fixed OAuth
# redirect port 53682 falls inside this machine's reserved range 53613-53712,
# so that handshake cannot complete here.
#
# Consequence worth knowing: this copy refreshes only while this PC is on.
# prodsrv02 (30 days, 03:10 nightly) + prodsrv04 remain authoritative.
#
# Drive FS will not accept folders at the drive root, so the path lives under
# "My Drive".
set -euo pipefail

PROD="${PLATOON_PROD_HOST:-tdx44@10.10.50.200}"
REMOTE_DIR="${PLATOON_PROD_BACKUPS:-/opt/homelab/platoon/backups}"
DEST="${PLATOON_BACKUP_DEST:-/mnt/e/My Drive/platoon db backup}"
KEEP_DAYS="${PLATOON_BACKUP_KEEP_DAYS:-30}"
LOG_DIR="${PLATOON_BACKUP_LOG_DIR:-$HOME/.local/state/platoon}"
LOG="$LOG_DIR/backup-copy.log"

mkdir -p "$LOG_DIR"
say() { echo "$(date -u +%FT%TZ) $*" | tee -a "$LOG"; }
die() { say "FAILED: $*"; exit 1; }

say "--- starting"
# Drive for Desktop may not have mounted yet after a reboot.
[ -d "$(dirname "$DEST")" ] || die "Drive is not mounted at $(dirname "$DEST")"
mkdir -p "$DEST" || die "cannot create $DEST"

NAME="$(ssh -o BatchMode=yes -o ConnectTimeout=15 "$PROD" \
        "basename \$(ls -t $REMOTE_DIR/*.db.gz | head -1)")" || die "cannot reach $PROD"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
ssh -o BatchMode=yes -o ConnectTimeout=15 "$PROD" "cat $REMOTE_DIR/$NAME" > "$TMP/$NAME" \
  || die "could not fetch $NAME"

# Verify before it goes anywhere near Drive: a backup that cannot be restored is
# worse than none, because it buys false confidence.
gunzip -c "$TMP/$NAME" > "$TMP/verify.db" || die "$NAME is not valid gzip"
ROWS="$(python3 -c "
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
assert c.execute('PRAGMA integrity_check').fetchone()[0] == 'ok', 'integrity check failed'
print(c.execute('SELECT COUNT(*) FROM personnel').fetchone()[0])
" "$TMP/verify.db")" || die "$NAME did not verify"
[ "$ROWS" -gt 0 ] || die "$NAME contains 0 personnel"

cp "$TMP/$NAME" "$DEST/$NAME" || die "copy to Drive failed"

# Confirm what actually landed rather than trusting the copy.
[ "$(stat -c%s "$TMP/$NAME")" = "$(stat -c%s "$DEST/$NAME")" ] \
  || die "size mismatch after copying to Drive"

find "$DEST" -maxdepth 1 -name '*.db.gz' -mtime "+$KEEP_DAYS" -delete
say "OK $NAME  $ROWS personnel  $(du -h "$DEST/$NAME" | cut -f1) -> $DEST"

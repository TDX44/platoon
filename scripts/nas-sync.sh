#!/usr/bin/env bash
# Copy the newest production snapshot onto the NAS.
#
# Runs on the Windows workstation under WSL from a Scheduled Task, NOT on
# prodsrv02 — even though the NAS (10.10.50.2) sits on the same LAN as the
# server. The server cannot reach it: SSH is closed, SMB guest is disabled, and
# the NFS exports are IP-restricted to hosts that do not include 10.10.50.200.
# This PC already has the share mounted (H: = \\10.10.50.2\home -> /mnt/h), so
# the copy goes through here.
#
# Consequence worth knowing: this copy refreshes only while this PC is on.
# prodsrv02 (30 days) + prodsrv04 remain authoritative. To make it always-on,
# add 10.10.50.200 to the NAS NFS export and mount it on the server directly;
# then this script is no longer needed.
set -euo pipefail

PROD="${PLATOON_PROD_HOST:-tdx44@10.10.50.200}"
REMOTE_DIR="${PLATOON_PROD_BACKUPS:-/opt/homelab/platoon/backups}"
DEST="${PLATOON_BACKUP_DEST:-/mnt/h/platoon db backup}"
KEEP_DAYS="${PLATOON_BACKUP_KEEP_DAYS:-30}"
LOG_DIR="${PLATOON_BACKUP_LOG_DIR:-$HOME/.local/state/platoon}"
LOG="$LOG_DIR/nas-sync.log"

mkdir -p "$LOG_DIR"
say() { echo "$(date -u +%FT%TZ) $*" | tee -a "$LOG"; }
die() { say "FAILED: $*"; exit 1; }

say "--- starting"
# Drive for Desktop may not have mounted yet after a reboot.
[ -d "$(dirname "$DEST")" ] || die "NAS is not mounted at $(dirname "$DEST")"
mkdir -p "$DEST" || die "cannot create $DEST"

NAME="$(ssh -o BatchMode=yes -o ConnectTimeout=15 "$PROD" \
        "basename \$(ls -t $REMOTE_DIR/*.db.gz | head -1)")" || die "cannot reach $PROD"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
ssh -o BatchMode=yes -o ConnectTimeout=15 "$PROD" "cat $REMOTE_DIR/$NAME" > "$TMP/$NAME" \
  || die "could not fetch $NAME"

# Verify before it goes anywhere near the NAS: a backup that cannot be restored is
# worse than none, because it buys false confidence.
gunzip -c "$TMP/$NAME" > "$TMP/verify.db" || die "$NAME is not valid gzip"
ROWS="$(python3 -c "
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
assert c.execute('PRAGMA integrity_check').fetchone()[0] == 'ok', 'integrity check failed'
print(c.execute('SELECT COUNT(*) FROM personnel').fetchone()[0])
" "$TMP/verify.db")" || die "$NAME did not verify"
[ "$ROWS" -gt 0 ] || die "$NAME contains 0 personnel"

cp "$TMP/$NAME" "$DEST/$NAME" || die "copy to the NAS failed"

# Confirm what actually landed rather than trusting the copy.
[ "$(stat -c%s "$TMP/$NAME")" = "$(stat -c%s "$DEST/$NAME")" ] \
  || die "size mismatch after copying to the NAS"

find "$DEST" -maxdepth 1 -name '*.db.gz' -mtime "+$KEEP_DAYS" -delete
say "OK $NAME  $ROWS personnel  $(du -h "$DEST/$NAME" | cut -f1) -> $DEST"

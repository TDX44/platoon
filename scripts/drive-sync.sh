#!/usr/bin/env bash
# Copy the newest production snapshot into the personal Google Drive.
#
# Runs on the Windows workstation under WSL, driven by a Scheduled Task. It does
# NOT run on prodsrv02: the Drive is only authenticated here, via Google Drive
# for Desktop (E: = jcarr2006@gmail.com). rclone on the server would be the
# always-on answer, but its OAuth redirect port (53682) falls inside this
# machine's reserved range 53613-53712, so that handshake cannot complete here.
#
# Consequence worth knowing: this is a third copy that refreshes whenever this PC
# is on. The authoritative backups remain prodsrv02 (30 days) + prodsrv04.
#
# Every file is verified before it is copied and after it lands, because a
# backup that cannot be restored is worse than none.
set -euo pipefail

PROD="${PLATOON_PROD_HOST:-tdx44@10.10.50.200}"
REMOTE_DIR="${PLATOON_PROD_BACKUPS:-/opt/homelab/platoon/backups}"
DRIVE_DIR='E:\My Drive\Platoon Accountability Backups'
STAGE="/mnt/c/Temp/platoon-backup"
KEEP_DAYS="${PLATOON_DRIVE_KEEP_DAYS:-30}"
PS='/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe'
LOG="$STAGE/drive-sync.log"

say() { echo "$(date -u +%FT%TZ) $*" | tee -a "$LOG"; }

mkdir -p "$STAGE"
say "--- starting"

NAME="$(ssh -o BatchMode=yes -o ConnectTimeout=15 "$PROD" \
        "basename \$(ls -t $REMOTE_DIR/*.db.gz | head -1)")" \
  || { say "FAILED: cannot reach $PROD"; exit 1; }

ssh -o BatchMode=yes -o ConnectTimeout=15 "$PROD" "cat $REMOTE_DIR/$NAME" > "$STAGE/$NAME" \
  || { say "FAILED: could not fetch $NAME"; rm -f "$STAGE/$NAME"; exit 1; }

# Verify before it goes anywhere near Drive.
ROWS="$(gunzip -c "$STAGE/$NAME" > "$STAGE/.verify.db" && python3 -c "
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
assert c.execute('PRAGMA integrity_check').fetchone()[0] == 'ok', 'integrity check failed'
print(c.execute('SELECT COUNT(*) FROM personnel').fetchone()[0])
" "$STAGE/.verify.db")" || { say "FAILED: $NAME did not verify"; rm -f "$STAGE/$NAME" "$STAGE/.verify.db"; exit 1; }
rm -f "$STAGE/.verify.db"
[ "$ROWS" -gt 0 ] || { say "FAILED: $NAME has 0 personnel"; rm -f "$STAGE/$NAME"; exit 1; }

# WSL cannot write E: (it is not mounted here and mounting needs root), so the
# copy is handed to Windows, which already has the Drive authenticated.
"$PS" -NoProfile -NonInteractive -Command "
  \$dest = '$DRIVE_DIR'
  if (-not (Test-Path \$dest)) { New-Item -ItemType Directory -Path \$dest | Out-Null }
  Copy-Item 'C:\Temp\platoon-backup\\$NAME' -Destination \$dest -Force
  Get-ChildItem \$dest -Filter '*.db.gz' |
    Where-Object { \$_.LastWriteTime -lt (Get-Date).AddDays(-$KEEP_DAYS) } |
    Remove-Item -Force
" >/dev/null 2>&1 || { say "FAILED: copy to Drive failed"; exit 1; }

# Confirm what actually landed, rather than trusting the copy.
SIZE_LOCAL=$(stat -c%s "$STAGE/$NAME")
SIZE_DRIVE=$("$PS" -NoProfile -NonInteractive -Command \
  "(Get-Item '$DRIVE_DIR\\$NAME').Length" 2>/dev/null | tr -d '\r\n ')
[ "$SIZE_LOCAL" = "$SIZE_DRIVE" ] \
  || { say "FAILED: size mismatch on Drive (local $SIZE_LOCAL, drive $SIZE_DRIVE)"; exit 1; }

rm -f "$STAGE"/*.db.gz
say "OK $NAME  $ROWS personnel  $SIZE_DRIVE bytes -> Drive"

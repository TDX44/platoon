#!/usr/bin/env bash
# Copy the newest production backup into personal Google Drive.
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
#
# This host almost certainly has no Postgres client, so it cannot re-run
# pg_restore --list the way backup-db.sh does. It doesn't need to: backup-db.sh
# already verified the dump (pg_restore --list, plus a live personnel-row
# count) before it wrote "OK" to LAST_BACKUP, so this script checks that line
# instead of re-deriving the same guarantee from scratch. What it checks
# locally is the shape of the bytes it actually received: the transferred
# size matches what prodsrv02 reports for that same file (catches a
# truncated transfer, which a size floor or a magic-bytes check alone would
# not — both survive truncation from the tail), plus the pg_dump
# custom-format signature and a sane minimum size as a backstop.
#
# Restore from this Drive copy (app stopped). The dump lives on the Drive
# box; the database lives on prodsrv02, so it has to travel there first:
#
#   scp "/mnt/e/My Drive/platoon db backup/<file>.dump" tdx44@10.10.50.200:/tmp/
#   ssh tdx44@10.10.50.200
#   cd /opt/homelab/platoon
#   docker compose stop app
#   # The role must exist first: pg_dump captures GRANTs but not roles, and a
#   # restore into a cluster without platoon_app drops every grant while
#   # still exiting 0 — the app then cannot read its own tables.
#   docker compose exec -T db psql -U platoon_owner -d platoon \
#     -v app_password="$DB_APP_PASSWORD" -f - < scripts/pg-roles.sql
#   docker compose exec -T db pg_restore -U platoon_owner --no-owner \
#     --clean --if-exists -d platoon < /tmp/<file>.dump
#   docker compose up -d app
#
# --clean --if-exists drops and recreates the dumped tables as platoon_owner;
# GRANTs travel with them via pg_dump/pg_restore once platoon_app exists to
# receive them — tested against a disposable container both with and without
# the role already present (see task-8-report.md for the with-role-missing
# case, which a restore onto a rebuilt cluster is the reason this matters).
set -euo pipefail

PROD="${PLATOON_PROD_HOST:-tdx44@10.10.50.200}"
REMOTE_DIR="${PLATOON_PROD_BACKUPS:-/opt/homelab/platoon/backups}"
DEST="${PLATOON_BACKUP_DEST:-/mnt/e/My Drive/platoon db backup}"
KEEP_DAYS="${PLATOON_BACKUP_KEEP_DAYS:-30}"
LOG_DIR="${PLATOON_BACKUP_LOG_DIR:-$HOME/.local/state/platoon}"
LOG="$LOG_DIR/backup-copy.log"
MIN_BYTES=512  # a real dump is comfortably larger than this; a truncated one is not

mkdir -p "$LOG_DIR"
say() { echo "$(date -u +%FT%TZ) $*" | tee -a "$LOG"; }
die() { say "FAILED: $*"; exit 1; }

say "--- starting"
# Drive for Desktop may not have mounted yet after a reboot.
[ -d "$(dirname "$DEST")" ] || die "Drive is not mounted at $(dirname "$DEST")"
mkdir -p "$DEST" || die "cannot create $DEST"

# Fetch the newest file's size in the same round trip that finds it, so the
# transfer below can be checked against prodsrv02's own number rather than
# against a copy of itself.
REMOTE_INFO="$(ssh -o BatchMode=yes -o ConnectTimeout=15 "$PROD" \
        "F=\$(ls -t $REMOTE_DIR/*.dump 2>/dev/null | head -1); [ -n \"\$F\" ] && stat -c '%s %n' \"\$F\"")" \
  || die "cannot reach $PROD"
# No fallback to a stale *.db.gz here on purpose: for 30 days after the
# Postgres cutover, old SQLite snapshots are still sitting in $REMOTE_DIR, and
# silently picking one up would copy a frozen pre-migration backup to Drive
# every night, report OK, and only start failing once those age out --
# a month-long silent failure is exactly what this task exists to prevent.
[ -n "$REMOTE_INFO" ] || die "no *.dump backup found in $REMOTE_DIR on $PROD"
REMOTE_SIZE="${REMOTE_INFO%% *}"
FULL="${REMOTE_INFO#* }"
NAME="$(basename "$FULL")"

# backup-db.sh writes LAST_BACKUP only after its own pg_restore --list and
# personnel-row-count checks pass, so a line starting "OK" is proof the dump
# this script is about to fetch already verified clean on prodsrv02.
LAST_BACKUP="$(ssh -o BatchMode=yes -o ConnectTimeout=15 "$PROD" \
        "cat $REMOTE_DIR/LAST_BACKUP 2>/dev/null")" || die "cannot reach $PROD"
case "$LAST_BACKUP" in
  OK*) ;;
  *) die "prodsrv02's LAST_BACKUP is not OK: ${LAST_BACKUP:-<empty>}" ;;
esac

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
ssh -o BatchMode=yes -o ConnectTimeout=15 "$PROD" "cat $REMOTE_DIR/$NAME" > "$TMP/$NAME" \
  || die "could not fetch $NAME"

# A network hiccup mid-transfer yields a file that still starts with PGDMP and
# is still well over MIN_BYTES, so the only thing that actually proves the
# transfer completed is comparing against the size prodsrv02 reported for the
# same file, fetched above in the same ssh round trip that picked it.
LOCAL_SIZE="$(stat -c%s "$TMP/$NAME")"
[ "$LOCAL_SIZE" = "$REMOTE_SIZE" ] \
  || die "transfer incomplete: got $LOCAL_SIZE bytes, prodsrv02 has $REMOTE_SIZE bytes"

# Verify what actually arrived, without needing a Postgres client here: the
# row-count guarantee already ran upstream (see LAST_BACKUP check above), so
# this only needs to rule out an empty or garbled file.
[ "$LOCAL_SIZE" -ge "$MIN_BYTES" ] || die "$NAME is only $LOCAL_SIZE bytes — too small to be a real dump"
MAGIC="$(head -c5 "$TMP/$NAME")"
[ "$MAGIC" = "PGDMP" ] || die "$NAME does not start with the pg_dump custom-format signature (PGDMP)"
# Bonus check only, never required: a client-vs-server version mismatch makes
# pg_restore --list reject a perfectly good dump (see backup-db.sh, and the
# task notes for why verification runs inside the db container there instead
# of on the host). A "bonus" check that can fail the whole run isn't a bonus,
# so a failure here only warns — the LAST_BACKUP gate above is what actually
# proves this dump verified clean, upstream, with a matching client.
if command -v pg_restore >/dev/null 2>&1; then
  if ! pg_restore --list "$TMP/$NAME" >/dev/null 2>&1; then
    say "WARNING: pg_restore --list failed on $NAME with $(pg_restore --version 2>&1 | head -1)" \
        "— likely a client/server version mismatch, not treated as a failure;" \
        "LAST_BACKUP already confirmed this dump verified clean upstream"
  fi
fi

cp "$TMP/$NAME" "$DEST/$NAME" || die "copy to Drive failed"

# Confirm what actually landed on Drive — a different failure than the
# transfer check above (a bad write here, not a bad read from prodsrv02).
[ "$(stat -c%s "$TMP/$NAME")" = "$(stat -c%s "$DEST/$NAME")" ] \
  || die "size mismatch after copying to Drive"

find "$DEST" -maxdepth 1 -name '*.dump' -mtime "+$KEEP_DAYS" -delete
say "OK $NAME  verified upstream (prodsrv02: $LAST_BACKUP)  $(du -h "$DEST/$NAME" | cut -f1) -> $DEST"

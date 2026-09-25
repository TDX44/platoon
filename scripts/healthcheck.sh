#!/usr/bin/env bash
# Five-minute health check for Platoon Manager, run on prodsrv02 by
# platoon-health.timer. Emails the operator when something is wrong, and once
# more when it recovers. Before this, a 500 at 0630 reached nobody.
#
# Checks: the public app and marketing site answer (so the tunnel is covered,
# not just the container), the app logged no tracebacks or worker timeouts in
# the last window, and the nightly backup is fresh and OK.
#
# ponytail: a dead prodsrv02 cannot report itself. That gap is covered from
# outside by the uptime Worker (scripts/uptime-worker.js).
#
# Mail goes through Resend with the app's own RESEND_API_KEY / NOTIFY_FROM
# (.env, via the unit's EnvironmentFile) to ALERT_EMAIL, defaulting to the first
# PLATFORM_ADMIN_EMAILS address. The same failure is re-sent at most hourly.
set -uo pipefail

APP_DIR="${PLATOON_APP_DIR:-/opt/homelab/platoon}"
STATE="$APP_DIR/backups/.health-state"
WINDOW="${HEALTH_WINDOW:-6m}"
REPEAT_SECONDS=3600
TO="${ALERT_EMAIL:-${PLATFORM_ADMIN_EMAILS%%,*}}"

problems=()

for url in https://app.platoonmanager.com/api/auth/config https://platoonmanager.com/; do
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 "$url")
  [ "$code" = 200 ] || problems+=("$url answered $code")
done

errors=$(docker compose -f "$APP_DIR/docker-compose.yml" logs --no-color --since "$WINDOW" app 2>&1 \
  | grep -cE 'Traceback|WORKER TIMEOUT|\[CRITICAL\]')
[ "$errors" -eq 0 ] || problems+=("app logged $errors error(s) in the last $WINDOW")

last="$APP_DIR/backups/LAST_BACKUP"
if [ ! -f "$last" ]; then
  problems+=("no backups/LAST_BACKUP")
elif [ $(( $(date +%s) - $(stat -c %Y "$last") )) -gt $(( 26 * 3600 )) ]; then
  problems+=("last backup is over 26h old: $(cat "$last")")
elif ! grep -q '^OK' "$last"; then
  problems+=("last backup: $(cat "$last")")
fi

send() {  # subject, body
  [ -n "${RESEND_API_KEY:-}" ] && [ -n "${NOTIFY_FROM:-}" ] && [ -n "$TO" ] || {
    echo "cannot send alert (RESEND_API_KEY / NOTIFY_FROM / ALERT_EMAIL unset): $1" >&2; return 1; }
  python3 -c 'import json,sys; print(json.dumps({"from": sys.argv[1], "to": [sys.argv[2]],
    "subject": sys.argv[3], "text": sys.argv[4]}))' "$NOTIFY_FROM" "$TO" "$1" "$2" \
  | curl -s --fail --max-time 20 https://api.resend.com/emails \
      -H "Authorization: Bearer $RESEND_API_KEY" -H 'Content-Type: application/json' -d @- >/dev/null
}

now=$(date +%s)
prev_sig=''; prev_at=0
[ -f "$STATE" ] && read -r prev_at prev_sig < "$STATE"

if [ ${#problems[@]} -eq 0 ]; then
  if [ -n "$prev_sig" ]; then
    send "Platoon Manager recovered" "All checks pass again on $(hostname) at $(date)." && echo "0 " > "$STATE"
  fi
  exit 0
fi

body=$(printf '%s\n' "${problems[@]}")
sig=$(printf '%s' "$body" | sed 's/[0-9]\+ error(s)/N errors/' | md5sum | cut -c1-12)
echo "$body" >&2
if [ "$sig" != "$prev_sig" ] || [ $(( now - prev_at )) -ge $REPEAT_SECONDS ]; then
  send "Platoon Manager: ${problems[0]}" "$body

Host: $(hostname), $(date)
Logs: ssh prodsrv02 'cd $APP_DIR && docker compose logs --since 30m app'" && echo "$now $sig" > "$STATE"
fi
exit 1

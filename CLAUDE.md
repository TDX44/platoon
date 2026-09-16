# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Platoon Accountability — a personnel accountability tracker for company formations.
Flask backend + a single-file vanilla-JS SPA. No build step and no linter.
Tests are standalone assert-based scripts under `tests/`, run directly and in CI.

## Commands

```bash
# Run locally — needs a reachable Postgres first (DATABASE_URL / MIGRATION_DATABASE_URL)
docker run -d --name platoon-pg -p 5432:5432 \
  -e POSTGRES_DB=platoon -e POSTGRES_USER=platoon_owner -e POSTGRES_PASSWORD=platoon postgres:17
pip install -r requirements.txt
python server.py

# Run production-like (gunicorn + Cloudflare tunnel sidecar + db, all three services)
docker compose up -d --build      # needs a .env file (see .env.example)
```

There is no lint config and no test framework (no pytest) — every check is a
standalone assert-based script, run directly. Tests need a Postgres to talk to:
`TEST_DATABASE_URL` (default `postgresql://platoon_owner:platoon@127.0.0.1:5432/platoon`)
points at an admin connection tests use to create and drop a throwaway schema
per test (`tests/dbharness.py`) — the same `platoon-pg` container above works.

```bash
python tests/test_invites.py          # invite expiry / single-use / platoon validation
python tests/test_auth_resilience.py  # JWKS fallback + auth status codes
python tests/test_schedule_edit.py    # absence edit + state re-derivation
python tests/test_smoke.py            # every route answers; every /api/ route is guarded
python tests/test_duty_roster.py      # duty roster / absence conflict detection
python tests/test_report_history.py   # report history persistence
python tests/test_availability.py     # who is free on date X (date-window rules)
python tests/test_formation_order.py  # formation queue rule (runs the JS under node)
python tests/test_timezone.py          # the duty day follows the unit, not the server
python tests/test_mobile_layout.py    # layout geometry: overflow, duplicated row
                                      # metadata, modal control fit, tap targets
```

CI runs every `tests/test_*.py` (`for f in tests/test_*.py; do python "$f"; done`).

`tests/test_mobile_layout.py` needs Playwright + a chromium browser, which are
dev-only and NOT in `requirements.txt` (that file is the production install
list). Install once with:

```bash
pip install playwright && playwright install chromium
```

Without that, the test prints `ok (skipped: playwright not installed)` and
exits 0 — it never fails a developer's box that hasn't installed browsers.

`gunicorn` is not in `requirements.txt`; it is installed only inside the Docker image.

## Backups

`scripts/backup-db.sh` runs nightly on prodsrv02 (`platoon-backup.timer`, 03:10)
and takes a `pg_dump --format=custom` dump of the `db` compose service to
`backups/`, verified with `pg_restore --list` before it is kept, keeping 30 days
and rsyncing each one to prodsrv04 over Tailscale. `backups/LAST_BACKUP` records
the outcome.

`scripts/backup-copy.sh` is a *third* copy into personal Google Drive
(`/mnt/e/My Drive/platoon db backup`), run on the Windows workstation under WSL
by the Scheduled Task "Platoon DB backup to Drive". `E:` is jcarr2006@gmail.com;
`G:` is the business account and must not be used. It does NOT run on prodsrv02,
because Drive is only authenticated on that PC and rclone's fixed OAuth redirect
port 53682 falls inside that machine's reserved range 53613-53712. So this copy
refreshes only while that PC is on; prodsrv02 + prodsrv04 remain authoritative.
The destination is one variable (`PLATOON_BACKUP_DEST`) if it ever moves.

## Architecture

Two files hold essentially the entire app:

- **`server.py`** (~1100 lines) — the whole Flask backend: routes, auth, and
  SQLite access.
- **`index.html`** (~5000 lines) — the entire frontend: markup, CSS, and a
  vanilla-JS SPA inline in one `<script>`. No framework, no bundler, no CDN libs
  beyond Clerk's script. Client routing uses the History API; `render()` and the
  `render*()` family redraw views from in-memory state.

`server.py` serves `index.html` at `/`. The `spa_fallback` route returns
`index.html` for any non-`api/`, non-static path so client-side routes survive a
hard reload (this is the "SPA reload 404" fix). It serves a real file only when
the path is on the `STATIC_DIRS` / `STATIC_FILES` allowlist — it used to serve
anything that existed on disk, which handed out `server.py` to anyone who asked
for it. **A new asset directory has to be added to `STATIC_DIRS` or it 404s into
the SPA.**

`public/` is the one part of the frontend that is not `index.html`: the
signed-out pages `/welcome`, `/privacy` and `/terms`, plus `public/site.css`.
They are plain HTML with no JS and no Clerk, because Google's OAuth consent
screen links straight at `/privacy` and `/terms` and they have to render for a
stranger with no session. Their `--cp-*` tokens are a copy of the ones in
`index.html`; keep the two in step. Google will not leave Testing without a
reachable privacy-policy URL, so if those routes break, Google sign-in
eventually breaks with them.

Full-page views live at `/<platoon>/<section>` (`accountability`, `directory`,
`availability`, `soldier/<id>`, `schools`, `locations`, `audit`, `settings`). Each is a hidden container in
`.dash-main` revealed by a `body.<name>-active` class, with matching
`open*()` / `close*()` / `render*()` functions — copy the directory page when
adding another. The sidebar highlight is derived from those body classes by
`syncNavActive()`; each `open*()` calls it, and `render()` covers the rest. Every new section needs a branch in `routeAfterLogin()` and the
`popstate` handler, plus a class-clearing line wherever the other pages clear
theirs.

**Formation mode** is the exception to the full-page pattern: a fixed
full-screen overlay (`#formationOverlay`, `body.formation-active`), not a
route, because it is a transient task and there must be nothing behind it to
hit by accident at 0630. `formationQueue(people, todayStr)` is a pure function
and the whole rule about who gets asked — unaccounted only, sorted by
`rankSort()`; anyone already away on a current absence
is skipped but handed back as `known` so the finish screen can show them and
let a wrong one be corrected. It writes only through the existing APIs (`PUT
/api/personnel/<id>` with `status: 'present'`, `POST
/api/personnel/<id>/schedule` for an absence, today→today) and so inherits the
absence lifecycle rather than duplicating it. Tests:
`tests/test_formation_order.py`, which lifts the function out of `index.html`
and runs it under node.

Sortable tables (directory, audit log) share `sortHeaders()` / `toggleSort()` /
`sortRows()`; reuse those rather than writing per-table sort code.

### Data layer

PostgreSQL at `DATABASE_URL`. The app connects as `platoon_app`, a non-owner
role that can read/write rows but not alter schema; `MIGRATION_DATABASE_URL`
(defaults to `DATABASE_URL` if unset) connects as `platoon_owner`, which owns
the tables and is the only role `init_db()` runs DDL as. `get_db()` returns one
pooled connection per request (`psycopg_pool.ConnectionPool`, opened per
gunicorn worker) via `row_factory=dict_row`, not a fresh connection per call.
`init_db()` runs at **module import** (so it also runs under gunicorn, and
every worker process runs it independently) and creates tables idempotently
with `CREATE TABLE IF NOT EXISTS`; because `CREATE TABLE IF NOT EXISTS` is not
safe against concurrent DDL from multiple workers importing the module at
once, the whole body is guarded by a Postgres session-level advisory lock
(`pg_advisory_lock`/`pg_advisory_unlock`) so only one worker runs it at a time.
There is no migration framework — schema changes are made by editing the
`CREATE TABLE` statements and adding ad-hoc `ALTER`/backfill logic in
`init_db()`.

Tables: `personnel`, `personnel_profile`, `settings`, `users`, `audit_log`,
`duty_roster`, `scheduled_events`, `invites`. (Legacy `training_*` tables from the
removed 350-1 tracker feature may still exist in older database files; they are
unused.)

`settings` is a key/value bag, all keys platoon-suffixed: `unit_name_<platoon>`
and the TDY picklists `tdy_schools_<platoon>` / `tdy_locations_<platoon>` (JSON
arrays, seeded once from `DEFAULT_TDY_*` by `init_db()`, then owned by the
Schools and Locations pages). Both are read and written through `/api/settings`.
**Array order is significant** — it is exactly the order the TDY modal's
dropdowns render, so nothing on either side may re-sort these lists.

### Absence lifecycle (single source of truth)

All TDY/leave/pass/other/FTR/late/excused absences live in `scheduled_events`
with a `state` column: `scheduled → active → completed`. Rows are never deleted on activation;
completed rows are the soldier's absence history (shown on the soldier page via
`GET /api/personnel/<id>/absences`).

`personnel.status/from_date/to_date/notes` is a **display cache** of the one
absence that is current, and **`_sync_person_status(conn, person_id, today)` is
its only owner**. Nothing else may write those four columns for an absence
reason. It re-derives every live row of that person from its dates alone
(`_derive_state`) — in *both* directions, so an edit that pushes a window into
the future demotes an `active` row back to `scheduled` — picks the newest current
window as the single active absence (any other current row is filed as history),
and writes or clears the cache to match. `_reconcile_absences(conn, today)` is
just that function looped over everyone with a live row; it keeps its
`{'activated': n, 'completed': n}` shape and the `ABSENCE_ACTIVATE` /
`ABSENCE_COMPLETE` audit rows, and is called from **every `GET /api/personnel`**,
so activation and auto-return-to-duty need no background job. Schedule
create/edit/delete each call `_sync_person_status` directly.

Marking a soldier present is the one thing that ends an absence from outside:
`PUT /api/personnel/<id>` with `status='present'` over an absence status calls
`_end_running_absence()`, which closes the active row at yesterday (or deletes it
if it had not started). Without that the roster said "present" while the absence
kept running underneath. The check is on the *transition* — `apiUpdate()` resends
the current status on every save, so marking a TDY soldier present-for-today
still PUTs `status='tdy'` and must stay a no-op. `POST .../schedule` is
idempotent on (person, status, from_date, to_date) so a double-tapped Save
cannot book the same absence twice.

`late` and `excused` are same-day states in practice but ordinary absences
underneath: a reason in `notes`, a today-to-today window, a row in the soldier's
history, and they complete themselves overnight like everything else. That is
why they are in `ABSENCE_STATUSES` rather than a separate flag. The roster row
and the report both drop the date range for them (`isSameDayState`) — a
today-to-today window is noise. `REASON_FIELDS` in `index.html` is the one place
that decides which statuses get the free-text reason box and how it is worded.

Two rules it deliberately keeps: `completed` is terminal (history is never
resurrected), and the cache is only overwritten when it already holds an absence
or when an absence has just activated — which is what keeps a hand-set
"present" from being reconciled away.
Completed absences reject edits. Tests: `tests/test_schedule_edit.py`.

**Planning off the same table**: `GET /api/availability?platoon=&date=[&to=]`
answers "who is free on date X" (the Availability page, `openAvailability()` /
`renderAvailability()`). It reads `scheduled_events` directly and never
`personnel.status`, which is only today's cache. A row covers a day when
`from_date <= D` and (`to_date = '' or to_date >= D`) — the same open-ended
bounds `_derive_state()` uses — and the dates decide regardless of `state`,
because `completed` is a claim about today, not about the day being asked
about. Range mode means "unavailable on any day of the range" and each person carries the
`days` they are out. Tests: `tests/test_availability.py`.

### Time

The duty day is the **organisation's**, not the server's or the viewer's.
prodsrv02 runs UTC, so `date.today()` rolled the roster over at 1900 Central and
activated absences for courses starting the next morning. `app_today()`,
`app_now()` and `app_stamp()` are the only ways the backend asks what time it
is, and `tests/test_timezone.py` fails the build if a raw `date.today()`
reappears.

The zone is a **setting**, not config: `settings` key `org_timezone`, changed by
an admin from Settings → Organisation, validated as a real IANA zone, and
audited as `ORG_TIMEZONE`. It is deliberately **unsuffixed** while
`unit_name_<platoon>` and the TDY lists are per-platoon — every platoon shares
one duty day. When a second organisation arrives it becomes
`org_timezone_<org>`, and only `load_app_timezone()` / `set_app_timezone()` need
to change. `PLATOON_TZ` is just the fallback before that row exists.

The live zone is cached in a module global and `set_app_timezone()` is its only
writer, so `app_now()` never needs a query of its own — including when it runs
inside a transaction already in progress. A stored value that is not a valid
zone logs a warning and falls back rather than stopping the app from booting.
**Every** stored timestamp — audit log, invites, `scheduled_events.created_at`,
`report_history.created_at` — is written from `app_stamp()` and passed
explicitly, never left to a column DEFAULT. The `to_char(now(), ...)` DEFAULTs
still on those columns run in the **db** container, whose `timezone` GUC was
baked as UTC at initdb; a report saved 2130 Sunday would be filed under Monday.
`docker-compose.yml` pins that GUC (`-c timezone=`) so the unreachable backstop
is at least not wrong, and pins the app container's `TZ` so its logs read
against the same duty day — but the `TZ` env var affects nothing that is
stored.

The frontend has its own `APP_TZ` with the same default and adopts the server's
value from **both** `/api/auth/config` and every `GET /api/settings`, so a phone
in Germany reports the same duty day as the roster back home and picks up an
admin's change on the next load. Keep the two defaults in step.

### Multi-platoon model

Three fixed platoons defined in the `PLATOONS` dict: `1st`, `2nd`, `hq`. Most
data rows carry a `platoon` column; most routes are platoon-scoped and gated by
`has_platoon_access(user, platoon)`.

### Auth

Authentication is Clerk-based (JWT verified against Clerk's JWKS via `PyJWKClient`).
`CLERK_ENABLED` is true only when `CLERK_PUBLISHABLE_KEY` and a frontend API URL
are configured. Three decorators guard routes — `clerk_auth_required` (verifies
the session token), `login_required`, and `admin_required`. `sync_clerk_user()`
mirrors a Clerk identity into the local `users` table; emails in
`CLERK_ADMIN_EMAILS` are auto-granted admin. `ProxyFix` is applied because the app
runs behind the Cloudflare tunnel.

Clerk's JWKS is fetched over the network, so a DNS blip on the host used to 401
every request and sign everyone out with a raw `urlopen error` in the UI.
`_signing_key_for()` therefore keeps the last successfully fetched key set and
reuses it (matching by exact `kid`) when Clerk is unreachable, and an unreachable
Clerk maps to **503**, never 401 — a 401 makes the client sign the user out over
a transient blip. The client retries `/api/auth/sync` once on a 503.

Sign-up is **invite-only**: a Clerk account that has never synced here is rejected
by `sync_clerk_user()` with a 403 unless it presents a live `invite_token`, matches
a pre-existing local row, or qualifies for the admin bootstrap (`CLERK_ADMIN_EMAILS`,
or the very first user when that list is unset). Admins mint single-use
`/invite/<token>` links from Manage Access; each carries the platoons/admin grant
and expires after `INVITE_EXPIRY_DAYS`. The frontend stashes the token in
`sessionStorage` so it survives Clerk's email-verification and OAuth redirects.
Invites are deliberately left out of backup/restore — they are short-lived
credentials, not data.

### Background reset (important gotcha)

`_midnight_reset_worker` clears daily `present` status and reconciles absences.
**The thread is started only inside `if __name__ == '__main__'`**, so it runs
under `python server.py` but **NOT under gunicorn in production**. That's fine:
absence reconciliation happens on every roster read (see Absence lifecycle);
only the midnight clearing of `present_date` depends on a trigger, and the
frontend's day handling plus `/api/reset` cover it.

### Backup

`/api/backup` exports a `version: 2` JSON snapshot (v2 = absence `state` on
`scheduled_events`); `/api/backup/restore` accepts versions 1 and 2 (v1 events
default to `state='scheduled'` and reconcile on the next read). If you change
the schema, update both, and keep the `version` check working.

### Design system

All styling flows from a single `--cp-*` token set defined at `:root`
(light) with a `body.dark-mode` override block, following `design.md`
conventions: brand blue `#146bc5`, neutral surfaces, 4–8px radii, borders over
shadows, tabular numerals. Legacy `--theme_*`/`--dash-*` variable names are
aliases onto `--cp-*` — the aliases live on `body` (NOT `:root`) so dark-mode
overrides resolve correctly; keep new aliases there.

**Size type in `em`, never `rem`.** The desktop scale comes from
`body { font-size: 21px }` above 900px, so `rem` values stay pinned to the 16px
root and render visibly smaller than the rest of the UI.

## Deployment

No CI/CD. Deploys are manual. Production runs on `prodsrv02` (`10.10.50.200`),
user `tdx44`, at `/opt/homelab/platoon`:

```bash
ssh tdx44@10.10.50.200 'cd /opt/homelab/platoon && git pull && docker compose up -d --build'
```

`docker-compose.yml` runs two services: `app` (gunicorn `-w 2` on :5000) and
`cloudflared` (the public ingress tunnel; `TUNNEL_TOKEN` from `.env`).

## Conventions

- The frontend is intentionally one file — add views as `render*()` functions and
  wire them into `render()` / the History-API router, not as separate modules.
  The exception is `public/`: pages that must render signed-out, with no JS.
- New API routes go under `/api/`, return JSON, and use the existing auth
  decorators and `log_action()` for the audit trail.
- `.env` holds all secrets (`SECRET_KEY`, Clerk keys, `TUNNEL_TOKEN`) and is
  gitignored; see `.env.example`.

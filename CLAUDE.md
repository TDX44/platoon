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
standalone assert-based script, run directly. Tests need a Postgres to talk
to, and **now require two connection strings, not one**:
`TEST_DATABASE_URL` (default `postgresql://platoon_owner:platoon@127.0.0.1:5432/platoon`)
is the admin/`platoon_owner` connection tests use to create and drop a
throwaway schema per test and to seed fixtures that must bypass RLS
(`tests/dbharness.py`'s `owner_conn()`); **`TEST_APP_DATABASE_URL`** (a
`platoon_app` role, e.g. `postgresql://platoon_app:<pw>@127.0.0.1:5432/platoon`)
is required so the app itself is exercised as the non-owner role RLS
actually binds — running tests only as the owner would silently skip every
policy. Run `scripts/pg-roles.sql` once against the `platoon-pg` container
above to create `platoon_app` before running tests locally.

```bash
python tests/test_invites.py          # invite expiry / single-use / unit validation
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
python tests/test_tenancy.py          # RLS default-deny, cross-tenant 404s, boot guard
python tests/test_units.py            # unit CRUD, slug uniqueness, owner-only gates
python tests/test_auth_flow.py        # signup states: needs_unit, invite, legacy claim
python tests/test_units_migration.py  # platoons-to-units.py against a fixture shaped like prod
python tests/test_unit_tree_js.py     # frontend tree helpers (unitById/unitBySlug/...), under node
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

The zone is a **setting**, not config: `settings` key `org_timezone`, scoped
`(root_id, NULL, 'org_timezone')` — one duty day per root, read per request
rather than cached, so a request against one tenant never serves another
tenant's zone. Changed by an owner from Settings → Organisation, validated as
a real IANA zone, and audited as `ORG_TIMEZONE`. `PLATOON_TZ` is just the
fallback before that row exists.

A module global cannot hold "the" zone once there is more than one tenant per
process, so there is no such global: the resolved zone lives on `g.tz` for the
duration of the request, loaded from that root's `org_timezone` setting the
same place the tenant GUC gets declared. A stored value that is not a valid
zone logs a warning and falls back to `PLATOON_TZ` rather than stopping the
request. **Every** stored timestamp — audit log, invites,
`scheduled_events.created_at`, `report_history.created_at` — is written from
`app_stamp()` and passed explicitly, never left to a column DEFAULT. The
`to_char(now(), ...)` DEFAULTs still on those columns run in the **db**
container, whose `timezone` GUC was baked as UTC at initdb; a report saved
2130 Sunday would be filed under Monday. `docker-compose.yml` pins that GUC
(`-c timezone=`) so the unreachable backstop is at least not wrong, and pins
the app container's `TZ` so its logs read against the same duty day — but the
`TZ` env var affects nothing that is stored.

The frontend has its own `APP_TZ` with the same default and adopts the
server's value from `/api/me` and every `GET /api/settings` — never from
`/api/auth/config`, which is fetched before sign-in and so before any tenant
is known and cannot carry a zone. A phone in Germany reports the same duty day
as the roster back home and picks up an admin's change on the next load. Keep
the two defaults in step.

### Tenancy

Organisations nest as a `units` adjacency list (`parent_id` self-reference,
`kind` one of company/platoon/squad/team/section/detachment/flight/crew,
`UNIQUE(root_id, slug)`). Every tenant table — `units`, `personnel`,
`personnel_profile`, `scheduled_events`, `duty_roster`, `report_history`,
`audit_log`, `settings`, `users`, `invites` — carries `root_id`, which is
"which tree" for that row; a root unit's own `root_id` equals its own `id`.

**RLS is the tenant blast door**, not an optional extra. `sql/rls.sql` puts a
`USING`/`WITH CHECK` policy on every tenant table:
`root_id = NULLIF(current_setting('app.root_id', true), '')::int`. The
`NULLIF` matters: once any custom GUC has been set on a session, a rolled-back
transaction leaves it as `''` rather than unset, and `''::int` raises —
without `NULLIF` that would turn default-deny into a 500 on the next pooled
request instead of "no rows". `_resolved_user()` runs
`SELECT set_config('app.root_id', <root_id>, true)` (via `set_tenant()`)
before any other statement on the request's transaction. An unattached user
(`unit_id IS NULL`) declares **no** tenant at all — `set_tenant(conn, None)`
sets the GUC to `''`, not `0`, because `0` is a value `auth_create_root_unit`
transiently writes and would otherwise be one shared writable tenant.
`log_action()` writes nothing when no tenant is declared, so a stranger's
first sign-in is not audited and `UNIT_CREATE` is the first audit row of a
new tenant; `/api/auth/sync` declares the tenant only after a successful
sync, so an attached user's `LOGIN` is still audited.

Because RLS binds every statement the app role runs, and does nothing for the
handful of operations that legitimately need to run *before* a tenant is
known (finding a user by Clerk id, redeeming an invite, creating a first
root), those operations are the **six `auth_*` functions in
`sql/auth_functions.sql`** — `auth_user_by_clerk_id`,
`auth_user_by_identity`, `auth_invite`, `auth_create_user`,
`auth_claim_legacy_user`, `auth_create_root_unit`. They are `SECURITY
DEFINER`, owned by `platoon_owner`, `SET search_path FROM CURRENT` (the
standard guard against search-path hijacking of definer functions), and
`EXECUTE` is revoked from `PUBLIC` and granted only to `platoon_app`. This
list is deliberately small and enumerable: if a cross-tenant read or write is
not one of these six functions, it does not happen.

Table owners and `BYPASSRLS` roles skip policies silently, which looks
exactly like a working app while leaking every tenant. `server.py` refuses to
boot (`_assert_rls_safe_role()`) if `DATABASE_URL`'s role is `rolsuper`,
`rolbypassrls`, or owns any table in the schema — including ownership held
through a granted role, tested with `pg_has_role(current_user, tableowner,
'USAGE')` rather than a name comparison. `FORCE ROW LEVEL SECURITY` is
deliberately not used: it would also bind `platoon_owner`, which runs
`init_db()` and the migration across every root by design.

RLS stops cross-tenant access; it says nothing about *subtree* visibility
inside one tenant. `current_subtree()` computes the set of unit ids at or
below the signed-in user's `unit_id` with one recursive CTE, cached on `g`
for the request; `can_access(unit_id)` tests membership. A row outside the
caller's subtree, or in another tenant entirely, answers **404**, never
403 — the row does not exist from the caller's side.

**Roles** are `owner` and `leader`. Both see and edit their whole subtree
(roster, absences, duty, reports, availability, audit log, backup export,
creating/renaming/deleting empty units, inviting leaders, editing users)
in the subtree. **Owner-only:** grant the `owner` role, rename or delete the
root, change `org_timezone`, remove a user, full backup restore, delete
another user's report.

**Self-serve signup:** a Clerk account that has never synced here gets a
local row with `unit_id NULL` — "signed in, attached nowhere" — and
`/api/me` reports `needs_unit: true`. Every `/api/` route except `/api/me`,
`/api/auth/sync` and `POST /api/units` 403s such a user. `POST /api/units`
with no `parent_id` creates a new root (`auth_create_root_unit`), attaches
the caller as `owner`, and seeds that root's defaults — no admin, no invite,
no bootstrap list required. An invite instead attaches the user at
`invite.unit_id` with `invite.role`.

**Running the tests now requires `TEST_APP_DATABASE_URL`** (a
`platoon_app` connection string) in addition to the admin
`TEST_DATABASE_URL` — tests exercise the app as `platoon_app` so RLS is
actually in the loop, not bypassed by an owner connection. Fixtures come
from `tests/dbharness.py`: `owner_conn()` (a `platoon_owner` connection for
setup that must bypass RLS, e.g. seeding two tenants), `make_tree()` (root +
one child unit), `make_user(unit_id, role, username)`, and `as_user(user)`
(monkeypatches `get_current_user`).

### Auth

Authentication is Clerk-based (JWT verified against Clerk's JWKS via `PyJWKClient`).
`CLERK_ENABLED` is true only when `CLERK_PUBLISHABLE_KEY` and a frontend API URL
are configured. Three decorators guard routes — `login_required` (signed in;
unattached users pass), `attached_required` (also requires `unit_id`), and
`owner_required`. `sync_clerk_user()` mirrors a Clerk identity into the local
`users` table. `ProxyFix` is applied because the app runs behind the
Cloudflare tunnel.

Clerk's JWKS is fetched over the network, so a DNS blip on the host used to 401
every request and sign everyone out with a raw `urlopen error` in the UI.
`_signing_key_for()` therefore keeps the last successfully fetched key set and
reuses it (matching by exact `kid`) when Clerk is unreachable, and an unreachable
Clerk maps to **503**, never 401 — a 401 makes the client sign the user out over
a transient blip. The client retries `/api/auth/sync` once on a 503.

A Clerk account that has never synced here either redeems a live
`invite_token` (attaches at the invite's unit and role) or matches a
pre-existing local row by email/username with an empty `clerk_user_id`
(`auth_claim_legacy_user` — how the migrated organisation's five accounts,
and the dev rehearsal's copy of them, land under a new Clerk instance) —
otherwise it gets a fresh `unit_id NULL` row and the self-serve signup screen
described under Tenancy above. There is no admin-bootstrap allowlist; whoever
creates a root becomes its `owner`.
Owners and leaders mint single-use `/invite/<token>` links from Manage
Access (`owner` role offered only to an owner, only for the root); each
invite carries the target unit and role and expires after
`INVITE_EXPIRY_DAYS`. The
frontend stashes the token in `sessionStorage` so it survives Clerk's
email-verification and OAuth redirects. Invites are deliberately left out of
backup/restore — they are short-lived credentials, not data.

### Background reset (important gotcha)

`_midnight_reset_worker` clears daily `present` status and reconciles absences.
**The thread is started only inside `if __name__ == '__main__'`**, so it runs
under `python server.py` but **NOT under gunicorn in production**. That's fine:
absence reconciliation happens on every roster read (see Absence lifecycle);
only the midnight clearing of `present_date` depends on a trigger, and the
frontend's day handling plus `/api/reset` cover it.

### Backup

`/api/backup` exports a `version: 3` JSON snapshot scoped to the caller's
**subtree**: `units`, `personnel`, `personnel_profile`, `scheduled_events`,
`duty_roster`, `report_history`, `settings` for those units, plus `users` and
`invites` for an owner only. Rows reference units by `slug`, not id. Restore
(`/api/backup/restore`) is **owner-only** and replaces the caller's whole
tree (units matched by slug, created if absent); sequences are resynced
after. `version: 1` and `version: 2` files are refused with a clear message —
those predate per-tenant scoping, and anyone holding one restores it before
the A1 migration, not after. If you change the schema, update both export and
restore, and keep the `version` check working.

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

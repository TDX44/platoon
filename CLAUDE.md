# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Platoon Manager (formerly Platoon Accountability) — a personnel accountability tracker for company formations.
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
python tests/test_mobile_layout.py    # layout geometry on home (org chart, incl. a
                                      # wide org), the top bar and its menus, roster,
                                      # directory, availability, units, every settings
                                      # section, the Billing page, soldier, formation
                                      # and the create-unit screen: overflow, duplicated
                                      # row metadata, modal control fit, tap targets
python tests/test_tenancy.py          # RLS default-deny, cross-tenant 404s, boot guard
python tests/test_units.py            # unit CRUD, slug uniqueness, owner-only gates
python tests/test_auth_flow.py        # signup states: needs_unit, invite, legacy claim
python tests/test_units_migration.py  # platoons-to-units.py against a fixture shaped like prod
python tests/test_unit_tree_js.py     # frontend tree helpers (unitById/unitBySlug/...), under node
python tests/test_platform_admin.py   # /admin: the Clerk-verified email gate, the
                                      # cross-tenant counts, what the payload may not carry
python tests/test_platform_admin_js.py # the /admin page's markup, under node
python tests/test_billing_state.py    # billing_rules.billing_state(): every state and boundary, no DB
python tests/test_billing.py          # the subscriptions row, the 402 gate sweep, extend/checkout/portal
                                      # with Stripe stubbed, the signed webhook, deletion, backup, /admin comp
python tests/test_billing_js.py       # banner, pricing screen, Billing page and modal rule, under node
python tests/test_settings_nav_js.py  # the settings nav, top-bar search and settings routes, under node
python tests/test_import.py           # alpha-roster import: rank aliases, unit matching, duplicates, one transaction
python tests/test_import_js.py        # the CSV/TSV parser and column mapping, under node
python tests/test_recurring_absences.py # repeating absences: expansion limits, series cancel, backup
python tests/test_duty_rotation.py    # duty_rotation.propose() fairness + the rotation/bulk routes
python tests/test_notifications.py    # email alerts: prefs, the cron gate, per-tenant timer, at-most-once
```

CI runs every `tests/test_*.py` (`for f in tests/test_*.py; do python "$f"; done`).

`tests/test_mobile_layout.py` needs Playwright + a chromium browser, which are
dev-only and NOT in `requirements.txt` (that file is the production install
list). Install once with:

```bash
pip install playwright && playwright install chromium
```

Without that, the test prints
`SKIPPED (not ok): playwright not installed — layout checks did NOT run` and
exits 0 — it never fails a developer's box that hasn't installed browsers. The
line deliberately does not say "ok": this test skipped its way through the
whole unit-tree rewrite while it was broken. CI installs chromium, so CI always
runs it for real.

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
signed-out **marketing site** (`public/home.html`) plus the legal pages
`privacy.html` and `terms.html`, `public/site.css`, `public/fonts/` and the
screenshots in `images/site/`. They are plain HTML with no JS at all and no
Clerk, because Google's OAuth consent screen links straight at `/privacy` and
`/terms` and they have to render for a stranger with no session. **Nothing in
`public/` may grow an executable `<script>` tag** — `tests/test_smoke.py` fails
the build if one appears. The one exception is `<script
type="application/ld+json">` structured data (each guide's `Article`, the front
page's `Organization` + `SoftwareApplication`), which the browser never runs.
Every public page carries a canonical on `https://platoonmanager.com`, the
sitemap lists `/privacy` and `/terms` rather than the `/legal/*` aliases, and a
trailing slash on a public URL (`/blog/`, `/privacy/`, `/blog/<slug>/`) is a
301 to the bare one rather than the app shell. HSTS is added by an
`after_request` hook only when the request is https (ProxyFix supplies the
scheme behind the tunnel). The mobile menu and the FAQ are `<details>` elements for
exactly that reason, and the display face is self-hosted in `public/fonts/`
rather than linked from a CDN. `site.css` carries two token sets: the `--cp-*`
block is a copy of the one in `index.html` and must stay in step with it, and
the `--pm-*` block is the marketing site's own light-blue palette, which the
app never sees. Google will not leave Testing without a reachable
privacy-policy URL, so if those routes break, Google sign-in eventually breaks
with them.

**Which site answers `/` is decided by the Host header.** `MARKETING_HOSTS`
(default `platoonmanager.com,www.platoonmanager.com`) serves `home.html` at
`/`; **every other host — the app subdomain, the LAN address, localhost —
serves the app shell there, exactly as before.** The env var deliberately names
the *marketing* side rather than the app side: unset or misspell it and a
visitor misses the brochure, which is recoverable, instead of a leader losing
the product at 0630, which is not. `/home` serves the marketing page on any
host, which is how you preview it in dev. Every "Sign in" and "Start free
trial" points at **`/app`**, which redirects to `APP_URL`
(`https://app.platoonmanager.com`) on a marketing host and to `/` anywhere
else, so clicking it in dev does not bounce you into production. `/welcome` is
kept as an alias of the marketing page because that was its URL before the
site moved to the root. Both branches are pinned by
`check_marketing_host_split` in `tests/test_smoke.py`.

**`LEGACY_HOSTS`** (default `platoon.carr7.com`) is the hostname the app
answered on before the product had its own domain. `redirect_legacy_host()`, the
app's only `before_request`, **301s** it onto the new domain rather than
switching it off, because people have it bookmarked. Only `/` goes to the
marketing site; **every other path keeps itself and its query string on
`APP_URL`**, since a bookmark on that host is `/<unit>/accountability`, not a
front page — sending them all to the front page would turn every bookmark in the
company into a brochure. Two exemptions, both deliberate: **`/api/` is never
redirected**, because a cross-origin 301 does not move anybody, it breaks an open
tab's in-flight request, and at 0630 that is somebody's accountability entry; and
**non-GET is never redirected**, because a 301 turns a POST into a GET and drops
the body. The next navigation moves them, which is what actually retires the
host. Pinned by `check_legacy_host_redirect`.

Full-page views live at `/<platoon>/<section>` (`accountability`, `directory`,
`availability`, `soldier/<id>`, `schools`, `locations`, `audit`, `units`,
`settings[/<section>]`). Each is a hidden container in
`.dash-main` revealed by a `body.<name>-active` class, with matching
`open*()` / `close*()` / `render*()` functions — copy the directory page when
adding another. The sidebar highlight is derived from those body classes by
`syncNavActive()`; each `open*()` calls it, and `render()` covers the rest. Every new section needs a branch in `routeAfterLogin()` and the
`popstate` handler, plus a class-clearing line wherever the other pages clear
theirs.

**The top bar** (`#appTopbar`) is one element — search, the light/dark
button, and the account menu — that `placeTopbar()` moves between
`#homeTopbarSlot` and `#dashTopbarSlot` whenever `showAppScreen()` shows the
home screen or the dashboard, so there is one set of ids and handlers. (It
replaced the home screen's hamburger menu.) The account menu splits
**Personal** (Profile & account, Preferences, Billing) from **Organization**
(Access, Organization settings), plus Platform admin for the
operator. The search box offers people on the loaded roster, the units you
lead and every page and setting (`SEARCH_PAGES`); `globalSearchResults()` is
pure and a result runs by index, so a typed name never reaches an onclick.
`/` or Ctrl/Cmd-K focuses it. Anything that needs a unit and is opened from
the home screen (`goToSettings()`, `goToPage()`) enters the user's own unit
first.

**Settings** come in two kinds. `/<unit>/settings` is General (logo,
time zone); `/<unit>/settings/<section>` is `profile`, `preferences`,
`billing`, `people` (shown as "Access") or `data` (`SETTINGS_SECTIONS`, all
rendered by `renderSettings()`), and `/<unit>/billing` is an alias for the
Billing section. The **personal** ones (`PERSONAL_SECTIONS`: profile,
preferences, billing) are reached only from the account menu, open under the
main nav, and light their menu item rather than any nav entry. The
**organization** ones have a sidebar of their own. Units, Schools, Locations
and the Audit log stay full pages but count as organization settings: while
any of those is open, `syncNavActive()`
sets `body.settings-mode`, which swaps the sidebar's main nav for the settings
nav (`SETTINGS_NAV`, rendered by `settingsNavHtml()`) and, below 900px, shows
the same entries as a row of tabs (`#settingsTabs`). A new settings page is a
`SETTINGS_NAV` entry plus, if it is a section rather than a page,
a `SETTINGS_SECTIONS` entry and a branch in `renderSettings()`.

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

The **home screen** draws the tree as an order-of-battle chart, and all of that
markup comes out of one pure function, `orgChartHtml(top, childrenOf,
headcountOf)` — the top unit above a row of nested `<ul>`s, one column per
direct child, everything deeper stacked inside its own column. Below 700px a
media query folds the very same markup back into an indented tree, so there is
no second rendering path to keep in step. Tests: `tests/test_org_chart_js.py`,
which runs it under node.

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
`duty_roster`, `scheduled_events`, `invites`, `subscriptions`, `report_history`,
`units`, `notification_prefs`, `notification_sends` (and `stripe_events`, not
tenant data). (Legacy `training_*` tables from the
removed 350-1 tracker feature may still exist in older database files; they are
unused.)

`settings` is a key/value bag, all keys platoon-suffixed: `unit_name_<platoon>`
and the TDY picklists `tdy_schools_<platoon>` / `tdy_locations_<platoon>` (JSON
arrays, seeded once from `DEFAULT_TDY_*` by `init_db()`, then owned by the
Schools and Locations pages). Both are read and written through `/api/settings`.
**Array order is significant** — it is exactly the order the TDY modal's
dropdowns render, so nothing on either side may re-sort these lists.

**Key `logo`** is a per-unit `settings` row holding a base64 PNG, at most
512 x 512 pixels and 400 KB decoded. `_validate_logo_png()` is the only way
bytes get in, and it runs on **both** `PUT /api/units/<id>/logo` **and
`/api/backup/restore`** — a backup file is user input, and a rotten value there
drops its own row into `skipped_rows` rather than failing the restore or
sitting in the database waiting to be served. **Any future settings key that
carries user bytes must be validated on both paths the same way.** A unit with
no logo of its own resolves to the nearest ancestor that has one
(`_resolved_logos()`, two queries for the whole tenant, walked upward in
Python — never one query per unit), and the resolved owner's `unit_id`, short
`v` hash and `name` ride along on every unit in `GET /api/units`. The `name` is
there because the owner of an inherited logo is an ancestor and `/api/units`
returns only the caller's own subtree, so the client cannot look it up.
`GET /api/units/<id>/logo` is **deliberately not `can_access`-gated** — any
attached member of the tenant may read any unit's resolved logo, because a
team leader's sidebar shows the company's mark and a logo is branding, not
data; writes are gated, and another tenant's id is a 404 on all three verbs.
The response is `immutable` for a year and the client cache-busts with `?v=`,
so the bytes at a given URL genuinely never change.

The **built-in mark** a unit with no logo falls back to is
`images/app-logo.png` — 512x512, the one constant `BUILTIN_LOGO`, shown on
the login and create-unit screens (no tenant is known there), the home screen,
the sidebar and the Settings preview. It doubles as the manifest's 512 `any`
icon. Every icon in `images/` is **generated**, not hand-made:
`scripts/make-icons.py` derives the favicon, the apple-touch tile and the
maskable icon from `app-logo.png`, which is itself the committed master.
Pillow is dev-only and deliberately absent from `requirements.txt`; run the
script from a scratch venv. Re-running it is a fixed point, so a changed icon
in a diff means somebody meant it. `tests/test_app_icons.py` opens every path
the manifest, the `<link>` tags and `BUILTIN_LOGO` name and checks the PNG
header says the size they claim. A PWA manifest icon cannot vary per tenant
without a per-tenant manifest, so the installed app icon is always the
built-in one however many units upload their own.

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
just that function looped over the people it would actually change — one SQL
prefilter, using `_derive_state`'s bounds, picks those with a live row whose
state is due to move or with more than one current row, so a settled roster
costs one query; it keeps its
`{'activated': n, 'completed': n}` shape and the `ABSENCE_ACTIVATE` /
`ABSENCE_COMPLETE` audit rows, and is called from **every `GET /api/personnel`**,
so activation and auto-return-to-duty need no background job. Schedule
create/edit/delete each call `_sync_person_status` directly.

Marking a soldier present is the one thing that ends an absence from outside:
`PUT /api/personnel/<id>` with `status='present'` over an absence status calls
`_end_running_absence()`, which closes the active row at yesterday — at today
for one that began today (every late and excused), so it stays as history — or
deletes it if it had not started. Without that the roster said "present" while the absence
kept running underneath. The check is on the *transition* — `apiUpdate()` resends
the current status on every save, so marking a TDY soldier present-for-today
still PUTs `status='tdy'` and must stay a no-op. That route accepts **only**
`present` or the soldier's current status (anything else is a 400 pointing at
`POST .../schedule`) and never writes `from_date`/`to_date`/`notes` from the
body while an absence is cached. `POST .../schedule` is
idempotent on (person, status, from_date, to_date) so a double-tapped Save
cannot book the same absence twice — enforced by the partial unique index
`scheduled_events_live_dedupe` (`WHERE state != 'completed'`) and `ON
CONFLICT DO NOTHING`, so two racing requests cannot both insert. Only **live**
rows count: a completed one is history, so late, marked present, then late
again the same day books a second absence. Both schedule routes take canonical `YYYY-MM-DD` dates
only, with the end on or after the start; a blank end on `late`/`excused`
means the start day.

`late` and `excused` are same-day states in practice but ordinary absences
underneath: a reason in `notes`, a today-to-today window, a row in the soldier's
history, and they complete themselves overnight like everything else. That is
why they are in `ABSENCE_STATUSES` rather than a separate flag. The roster row
and the report both drop the date range for them (`isSameDayState`) — a
today-to-today window is noise. `REASON_FIELDS` in `index.html` is the one place
that decides which statuses get the free-text reason box and how it is worded.

**Repeating absences** are not a new kind of absence. A `recurrence` on
`POST .../schedule` (`{type: 'weekly', weekdays: [0..6, Monday = 0], until}` or
`{type: 'interval', every: N, until}`) is expanded by `expand_recurrence()` into
ordinary rows at booking time — each the first window's length, at most
`RECURRENCE_MAX_OCCURRENCES` (60), `until` at most `RECURRENCE_MAX_DAYS` (183)
ahead, a real end date required, and no two occurrences overlapping (only one
absence can be current). They share a `scheduled_events.series_id` (32 hex
chars, added idempotently by `init_db()`), which exists only so `DELETE
/api/schedules/series/<id>` can cancel the occurrences that have not started;
a running one and history stay. The live dedupe index applies row by row, so a
double-tapped recurring Save books nothing more. One `SCHEDULE_SERIES` /
`DELETE_SERIES` audit row each. Tests: `tests/test_recurring_absences.py`.

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

### Alpha-roster import

The Import modal still takes the old one-person-per-line paste (`RANK Last,
First`, posted one soldier at a time). Text — pasted or read from a chosen
file — whose header row names the soldier (`Last` + `First`, or `Name`) is a
**spreadsheet** instead: `parseDelimited()` (tab if the first line has one,
else comma, RFC 4180 quoting), `guessImportColumn()` and `mapImportRows()` in
`index.html` split and map it (the user can change any column's target), and
everything a row *means* is decided by **`POST /api/personnel/import`**:
`normalize_rank()` (case, dots, hyphens and `RANK_ALIASES`; an ambiguous "LT"
stays an error), the same `_name_errors()` as a single add,
`validation.validate_profile()` on the `IMPORT_PROFILE_FIELDS` columns, the
unit matched by name or slug, case-insensitive, **only inside the caller's
subtree** (unmatched or ambiguous falls back to the unit the import was
started from, flagged), and a likely duplicate — same last + first, rank
ignored, in the subtree or earlier in the file — skipped unless
`include_duplicates`. `dry_run: true` is the preview, so the preview cannot
promise what the import refuses. A real run refuses the whole batch if any row
has an error (the client leaves those rows out), caps at `IMPORT_MAX_ROWS`
(500), inserts in the request's one transaction and writes one
`IMPORT_PERSONNEL` audit row. The default `unit_id` goes through
`_unit_gate()`: another tenant's unit is 404, one outside the subtree 403.
Tests: `tests/test_import.py`, `tests/test_import_js.py`.

### Duty rotation

"Generate rotation…" on the duty roster. **`duty_rotation.py`** is the rule,
pure like `billing_rules.py`: `propose(days, pool, history, absences,
holidays, separate_weekends, taken)` gives each day to the free soldier with
the fewest earlier turns of that duty, then not the same soldier two days
running, then the longest since their last turn, then pool order. Weekends
(and any holiday dates given) keep a separate tally when `separate_weekends`.
Away means the availability rule — `from_date <= D and (to_date = '' or to_date
>= D)`, any state — and a day that already has the duty is left alone.
`POST /api/duty/rotation` feeds it (history = every `duty_roster` row of that
duty for the pool before the range, wherever filed) and **writes nothing**; it
returns the proposal, who is away on each day and the pool. The leader can
change any pick, then **`POST /api/duty/bulk`** saves up to `DUTY_BULK_MAX`
entries in one transaction: every soldier must be in the unit's subtree (the
`add_duty` rule), dates canonical, an entry that already exists (same date,
duty, soldier) is skipped so a double tap books nothing, conflicts warn as for
a single entry, one `ADD_DUTY_ROTATION` audit row. Both take `unit_id` through
`_unit_gate()` (404 / 403). Tests: `tests/test_duty_rotation.py`.

### Email notifications

Per account, opted into under **Preferences** (`GET`/`PUT
/api/me/notifications`, table `notification_prefs`: `accountability_enabled`
+ `accountability_time`, `digest_enabled` + `digest_time`, `HH:MM` on the
tenant's clock). (a) **Accountability not complete** mails the account if
anyone in its unit's subtree is still unaccounted for (`present` and not
marked today — the frontend's `isUnaccounted()`) at or after its time; a
complete roster sends nothing and records nothing, so a soldier added later
still triggers it. (b) The **morning digest**: due back today (an absence
whose `to_date` was yesterday), starting an absence today, and overdue — which
cannot be an active row past its end, since reconciliation completes those,
so it is the roster cache still holding an absence past its `to_date`, or
FTR. `late`/`excused` are left out of the first two as same-day noise. An
empty digest is recorded as `nothing to send` and not re-evaluated that day.

**Inert unless `RESEND_API_KEY` and `NOTIFY_FROM` are both set** (the timer
route then answers `{"email": "disabled"}` and the Preferences card says so).
Mail goes through Resend's HTTP API with `urllib` (`_send_email()`, which tests
replace), plain text plus HTML with every value `html.escape`d, and a
User-Agent set explicitly (Cloudflare-fronted APIs refuse urllib's default).

There is still no background worker. **`POST /api/cron/notify`** is driven by
`scripts/platoon-notify.timer` (every 5 minutes, `curl` to the host's :5000
with `X-Cron-Secret: $CRON_SECRET` from `.env`) and authenticated **only** by
that header: `cron_secret_required` answers 404 while `CRON_SECRET` is unset
and 403 on a wrong value (`hmac.compare_digest`). It has no session and no
tenant, so the one cross-tenant thing it may do is `auth_notify_roots()` (root
ids with anybody opted in, nothing else); then for each root it declares the
tenant, loads that root's zone into `g.tz`, reconciles absences as a roster read
would, and evaluates every opted-in account under RLS. Each (account, rule,
duty day) is **claimed** in `notification_sends` (tenant table, RLS) and
**committed before the send**, so a second timer run or a crash can never mail
it twice — at-most-once, a failed send is recorded `failed` and not retried
(a missed alert beats a flood). The route commits per root, re-declaring the
tenant after each commit because `set_config(..., true)` dies with the
transaction; one tenant raising rolls back only itself. Tests:
`tests/test_notifications.py`.

### Time

The duty day is the **organization's**, not the server's or the viewer's.
prodsrv02 runs UTC, so `date.today()` rolled the roster over at 1900 Central and
activated absences for courses starting the next morning. `app_today()`,
`app_now()` and `app_stamp()` are the only ways the backend asks what time it
is, and `tests/test_timezone.py` fails the build if a raw `date.today()`
reappears.

The zone is a **setting**, not config: `settings` key `org_timezone`, scoped
`(root_id, NULL, 'org_timezone')` — one duty day per root, read per request
rather than cached, so a request against one tenant never serves another
tenant's zone. Changed by an owner from Settings → General, validated as
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

Organizations nest as a `units` adjacency list (`parent_id` self-reference,
`kind` one of company/platoon/squad/team/section/detachment/flight/crew,
`UNIQUE(root_id, slug)`). Every tenant table — `units`, `personnel`,
`personnel_profile`, `scheduled_events`, `duty_roster`, `report_history`,
`audit_log`, `settings`, `users`, `invites`, `subscriptions`,
`notification_prefs`, `notification_sends` — carries `root_id`, which is
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
root, and the notification timer finding which roots to visit), those
operations are the **eight `auth_*` functions in
`sql/auth_functions.sql`** — `auth_user_by_clerk_id`,
`auth_user_by_identity`, `auth_invite`, `auth_create_user`,
`auth_claim_legacy_user`, `auth_attach_invited_user`, `auth_create_root_unit`,
`auth_notify_roots`. They are `SECURITY
DEFINER`, owned by `platoon_owner`, `SET search_path FROM CURRENT` (the
standard guard against search-path hijacking of definer functions), and
`EXECUTE` is revoked from `PUBLIC` and granted only to `platoon_app`. This
list is deliberately small and enumerable: if a cross-tenant read or write is
not one of these eight functions, the four `billing_*` functions in
`sql/billing_functions.sql` (the webhook has no session and declares no
tenant, so it cannot go through RLS either) or `admin_billing_rows()` in
`sql/admin_functions.sql`, it does not happen.

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
for the request; `can_access(unit_id)` tests membership. The two failures are
deliberately different codes: a row in the caller's **own** tenant but outside
their subtree answers **403** — it exists, they may not have it — while a row
in **another** tenant answers **404**, because RLS never handed the row over
and the route genuinely cannot tell it from one that was never there. The two
`/api/users/<id>` routes are the exception and answer 404 either way: a user
row is an account, and 403 would confirm which ids are real accounts.

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
(`auth_claim_legacy_user` — how the migrated organization's five accounts,
and the dev rehearsal's copy of them, land under a new Clerk instance) —
otherwise it gets a fresh `unit_id NULL` row and the self-serve signup screen
described under Tenancy above. An account that already exists **unattached**
and later presents a live invite is attached by it
(`auth_attach_invited_user`, which only moves a `unit_id NULL` row and only for
an invite already accepted by that same Clerk id); an attached account is never
moved by one. Accepting is a single conditional `UPDATE ... WHERE accepted_at =
''` (`_claim_invite()`), so of two sign-ins racing one link exactly one gets
it. **`username` is written only when the account is made** — a later sync
updates `email` and `full_name` but leaves `username` alone, because an owner
may rename it and a report's `created_by` is matched against it. There is no admin-bootstrap allowlist; whoever
creates a root becomes its `owner`.
Owners and leaders mint single-use `/invite/<token>` links from Manage
Access (`owner` role offered only to an owner, only for the root); each
invite carries the target unit and role and expires after
`INVITE_EXPIRY_DAYS`. The
frontend stashes the token in `sessionStorage` so it survives Clerk's
email-verification and OAuth redirects. Invites are deliberately left out of
backup/restore — they are short-lived credentials, not data.

### Platform admin

One person runs this instance, and `/admin` is theirs: a **read-only,
cross-tenant** dashboard (totals, one row per organization, the newest users)
served by `GET /api/admin/overview`. It is not an app role — `owner` and
`leader` are still the only roles inside a tenant — it is the operator of the
server.

**Who.** `PLATFORM_ADMIN_EMAILS` (comma-separated, case-insensitive, default
`jonathon.carr5@gmail.com`; empty turns the dashboard off). The address is
compared against **Clerk's** copy, never ours: `users.email` is written from
the `/api/auth/sync` request body, so it is the caller's own claim about
themselves, and a stranger can put any address in their row. The session JWT
carries `sub` and no email (this instance uses Clerk's default token), so
`_clerk_verified_email()` asks Clerk's Backend API — `GET
https://api.clerk.com/v1/users/<sub>` with `CLERK_SECRET_KEY` — for the
`primary_email_address_id` entry and requires `verification.status ==
'verified'`. api.clerk.com is behind Cloudflare, which rejects urllib's
default User-Agent with error 1010, so one is sent explicitly. The verdict is
cached per Clerk id for five minutes, **positive and negative** (caching only
the yes would let any signed-in stranger make us call Clerk at will).

**A failure is cached too**, as `PLATFORM_ADMIN_UNKNOWN` under its own short
TTL (`PLATFORM_ADMIN_FAIL_TTL_SECONDS`, 30 s). It has to be: the lookup blocks
and gunicorn runs two **sync** workers, so with Clerk's API unreachable but
sessions still valid on the stale-key fallback, one browser polling `/api/me`
would park a worker per request — and anyone can aim that by putting the
operator's address in their own `users.email`. It is never stored as a real
refusal (that would outlive the outage) and never as a grant. **An
unreachable Clerk fails closed** (503 on the dashboard, no menu on
`/api/me`), never open. `platform_admin_required` declares **no tenant**, works whether or
not the admin is attached to a unit, and answers a signed-in non-admin with
**404** — the surface is not advertised. `/api/me` and `/api/auth/sync` carry
`platform_admin`, and that flag costs a Clerk call only when the stored email
already matches the list, so an ordinary sign-in never touches Clerk.

**What it may show.** Every number comes from the three `admin_*` SECURITY
DEFINER functions in **`sql/admin_functions.sql`**, installed at boot beside
`auth_functions.sql`. That file's header is the rule: counts, sizes,
timestamps, organization names/slugs, and app users' email/name/role — and
nothing else. No soldier names, no profile rows, no audit `details`, no invite
tokens, no logo bytes, no Clerk ids. **The database cannot tell an admin
request from any other** — `platoon_app` holds EXECUTE on all three — so every
Python caller must sit behind `platform_admin_required`, and
`tests/test_platform_admin.py` greps `server.py` to prove it. The file drops
all three functions before recreating them: `CREATE OR REPLACE` cannot change
a `RETURNS TABLE` column list, and a statement that raises in there aborts
`init_db()`'s transaction, which is the app failing to boot. Postgres's DDL is
transactional, so there is no window where a concurrent request finds the
function missing.

Because `log_action()` needs a tenant and this read belongs to none, the
dashboard logs to `app.logger` instead: one info line per read, a warning per
refusal and per unverifiable request, identified by the token's `sub` and
never by anything out of the request.

**The slug `admin` is reserved** (`RESERVED_SLUGS` in `server.py`, applied in
`slugify()`, which the root path, the child path **and `/api/backup/restore`**
all go through): a unit named "Admin" gets `admin-unit`, because the client
resolves `/admin` before it looks a slug up and a unit holding that slug would
otherwise be unreachable. A restore takes the slug from an uploaded file, so
it normalises what it **stores** while still keying its unit map on the
**file's** slug — every other row in the file names its unit by that string,
and an ordinary backup's slugs must round-trip unchanged or every bookmarked
`/<slug>/<section>` breaks. The page is its own screen (`#adminScreen`,
`showAppScreen('admin')`), not a page inside the dashboard shell, because it
is not scoped to a unit — the operator opens it from the home screen with no
`currentUnit`. Tests: `tests/test_platform_admin.py`,
`tests/test_platform_admin_js.py`.

### Billing

Per-account Stripe billing, spec `docs/superpowers/specs/2026-09-17-stripe-billing-design.md`
(read section 10 first — the rulings that fit the spec to this code).
**`billing_rules.py`** is the whole rule: `billing_state(row, now, default_on,
platform_admin, enabled)` → `COMPED | ACTIVE | PAST_DUE | TRIAL | GRACE | LOCKED`,
pure, UTC, clock-injected, no Flask. `_resolved_user()` creates the
`subscriptions` row at an attached account's first sign-in (the trial starts
at the first sign-in that finds the account billed, not at creation) and
computes `g.billing`. The three tenant decorators (`login_required`,
`attached_required`, `owner_required`) each call `_billing_block()` **before**
their own role check, so a locked account gets the same **402**
`{'error': 'subscription_required', 'billing': {...}}` whatever its role —
`attached_required`'s unit-membership 403 and `owner_required`'s owner-only
403 never run for it. The 402 fires on every `/api/` route except `/api/me`
and the `/api/auth/`, `/api/billing/`, `/api/admin/` prefixes. `GET
/api/units` is deliberately not exempt. `platform_admin_required` declares no
tenant and has no `g.billing`.

Env: `STRIPE_MODE=test|live` picks which of `STRIPE_TEST_*` / `STRIPE_LIVE_*`
(secret key + webhook signing secret) the process reads; dev is `test`,
production `live`. **No key for the active mode = billing off**, every
account `COMPED`, one warning at boot. The two are required **together**: a
secret key with an empty webhook secret is a `SystemExit` at import, because
cards would be charged while every delivery is answered 503 and the app never
learns what it sold. `BILLING_DEFAULT=on|off` is the
default for accounts whose `billing_mode` is `default`; `/admin` comps or
bills any account (`PUT /api/admin/users/<id>/billing_mode`). The operator's
own account is always comped.

The webhook (`POST /api/billing/webhook`, undecorated, signature is the
auth, listed in the smoke test's `PUBLIC_API`) reads the body with a bounded
`request.stream.read(WEBHOOK_MAX_BYTES + 1)` — a `Content-Length` check alone
does not cap a chunked body — verifies it with `stripe.Webhook.construct_event`,
then parses those same verified bytes with `json.loads` (construct_event's
`StripeObject` return is not dict-like in stripe-python >= 12). It is the
only writer of the Stripe columns, through the four SECURITY DEFINER
`billing_*` functions in `sql/billing_functions.sql`; `tests/test_billing.py`
greps `server.py` to keep it that way. Replays are no-ops (`stripe_events`); a
handler that raises is a 500 and the event record rolls back with it, so
Stripe's retry is handled rather than skipped. `invoice.payment_failed`
must name the subscription it is for (a plain `subscription` id, or
`parent.subscription_details.subscription` since API 2025-03); one that names
none is logged and **dropped**, because a NULL subscription satisfies
`billing_apply_stripe`'s guard by construction and `past_due` is an open
state. For the ones that do name it, `billing_apply_stripe` refuses to move a
**locked** status (`canceled`, `unpaid`, `incomplete_expired`) to `past_due` —
Stripe does not order deliveries, so a late failed invoice cannot re-open a
cancelled account. `billing_checkout` 409s when the account is already
subscribed (the portal changes a live plan), and when a
**`customer.subscription.created`** adopts an id over a different stored one,
`_apply_stripe` cancels the superseded subscription at Stripe best-effort so
one card cannot carry two. Only `created`: `billing_apply_stripe` adopts any
id on an `active`/`trialing` status (A14) and Stripe does not order
deliveries, so a retried `customer.subscription.updated` for an *older*
subscription, arriving after the new one's `created`, adopts the old id — and
cancelling on that would kill the subscription just paid for. A second
Checkout completing is the case the cancel exists for, and it always arrives
as `created`. When that SQL guard suppresses the update,
`_apply_stripe` writes **no audit row**: a row for a write that did not
happen is a lie the support desk would act on. Cancellation is at period end
through the Billing Portal and never flips local state — the webhook does.
Stripe is called through the `_stripe_*` seams (prices, customer, checkout,
portal, cancel, invoices, card); tests replace those. `stripe_customer_id` is
stored `<mode>:<id>`.

**The Billing page** (Settings → Billing, `/<unit>/settings/billing`) follows
Resyrv's `/billing`: a current-plan card beside a payment-method card (card
on file, the portal's entry points, usage), the plan picker while there is no
subscription, then the invoice history. Its data is `GET
/api/billing/details` — the verdict, the plan from the stored lookup key, and
the card and invoices fetched from Stripe per view, **never stored**, reduced
to display fields (brand, last four, expiry; dates, amounts, status, Stripe's
own `https` links) and cached per worker for 30 s keyed on the row's
`updated_at`, so a webhook write is always a miss. Stripe failing is
`stripe_error: true`, not a 500. `POST /api/billing/portal` takes an optional
`intent` — `payment_method`, `plan` or `cancel` — and deep-links the portal
into that flow using the **stored** subscription id (never one from the
request); a flow the portal is not configured for falls back to the portal's
front page. Checkout and the portal return to `/<unit>/settings/billing`.

Prices are cached per worker — 1 h on success, 60 s on a failure or a
partial answer (fewer than both lookup keys returned) — and are `[]` in
both failure cases, never a stale or half-complete amount.

The plan buttons carry the price lookup key in a `data-key` attribute, read
back via `this.dataset.key`; no server string is ever interpolated into
inline JS. The pricing screen (`#billingScreen`, `showBillingScreen()`) is
only ever a **locked** account's; everyone else uses the Billing page, which
is an ordinary settings route. The pricing screen pushes no history, so the
`popstate` handler returns through `routeAfterLogin()` while
`body.billing-active` — the same path that sends a still-`LOCKED` account
straight back to it.

### Day reset — there is no background worker

There used to be a `_midnight_reset_worker` thread, started only inside
`if __name__ == '__main__'`, so it never ran under gunicorn in production. A1
deleted it: it opened a connection with **no tenant set**, so RLS matched
nothing, and it printed `Day reset … absences reconciled: {...}` having changed
zero rows — a log line that said it worked.

Nothing replaced it, because nothing needed to. Absence activation and
auto-return-to-duty happen on **every `GET /api/personnel`**
(`_reconcile_absences`, see Absence lifecycle), so a roster that is being
looked at is always current. Clearing yesterday's `present` marks is
`/api/reset` — per unit, or owner-wide across the root — plus the frontend's
own day handling. The one scheduled job is outside the app: the email timer
(`scripts/platoon-notify.timer`) calling `POST /api/cron/notify`, which sets the
tenant per root itself — see Email notifications.

### Backup

`/api/backup` exports a `version: 3` JSON snapshot scoped to the caller's
**subtree**: `units`, `personnel`, `personnel_profile`, `scheduled_events`,
`duty_roster`, `report_history`, `settings` for those units, plus `users` for
an owner only. Invites are deliberately **not** exported — they are
short-lived credentials, not data. Rows reference units by `slug`, not id. Restore
(`/api/backup/restore`) is **owner-only** and replaces the caller's whole
tree (units matched by slug, created if absent); sequences are resynced
after. `version: 1` and `version: 2` files are refused with a clear message —
those predate per-tenant scoping, and anyone holding one restores it before
the A1 migration, not after. If you change the schema, update both export and
restore, and keep the `version` check working. `scheduled_events.series_id`
rides along (`SELECT *`); a restored one that is not 32 hex characters becomes
NULL, keeping the absence. An owner's `users` rows carry the four
`notify_*` preference keys (`NOTIFY_BACKUP_KEYS`, only for an account that has
a `notification_prefs` row); restore checks them with the same
`_notify_prefs_from()` a PUT uses and counts a bad set in `skipped_rows`.
`notification_sends` is a send log and is not exported. **Row ids are global primary
keys**, so a restore keeps the file's id where it is free (an ordinary
round trip keeps every soldier's URL) and gives the row a fresh one where it
is not — restoring one organization's export into another while the first
still exists — and every dependent row (`personnel_profile`,
`scheduled_events`, `duty_roster`) is written through the old->new personnel
map, never with the file's number. The file is user input, so a `users` row
whose role is not in `ROLES` (or is `owner` below the root) is skipped, an
`org_timezone` that is not a real IANA zone and a personnel `status` that is
not `present` or an absence drop their own row into `skipped_rows`, like the
logo. An owner's `users` rows
also carry `billing_mode`, `trial_started_at`, `trial_ends_at` and
`extended_at` (optional keys; restore creates a `subscriptions` row from
them **only for an account that has none** — an existing row is the live
record and is left exactly as it is, since overwriting it could re-open an
ended trial, clear an extension or flip a `billed` account to `default` — and
never writes a Stripe column). The file is attacker-supplied, so
restore takes neither the comp switch nor an unbounded date off it: an
incoming `'comped'` becomes `'default'` (only `'default'` and `'billed'` are
accepted — comping is `billing_set_mode` behind `platform_admin_required`),
and both trial stamps are clamped to a ceiling of `now + TRIAL_DAYS`, or
`now + TRIAL_DAYS + EXTENSION_DAYS` when the incoming row carries
`extended_at` — otherwise a round trip would clip a trial the account had
already extended. A missing or unparseable `trial_ends_at` lands **on** that
ceiling rather than NULL: `_billing_row`'s backfill only fires on a NULL
`trial_started_at`, so a NULL end is never repaired and `billing_state` reads
it as a trial with `TRIAL_DAYS` left, for ever. `extended_at` comes back as it
stands; it only ever removes an entitlement.

### Input validation

**`validation.py`** is the boundary for everything a leader types into a
soldier: pure, no Flask, no DB, no clock, like `billing_rules.py`. `PUT
/api/personnel/<id>/profile` normalises through it (`validate_profile()`
rewrites the dict in place) and then refuses the **whole** write rather than
storing part of it, answering `{'error', 'field'}` so the client can put the
message under the field it is about. A soldier's own name goes through
`_name_errors()` on **both** `POST /api/personnel` and `PUT
/api/personnel/<id>`, because both take it off the same modal.

**Every rule exists twice** — here and inline in `index.html` as
`validateField()` / `normalizeField()` — because there is no build step to
share a module with. `tests/test_validation.py` lifts the JS out, runs both
over one table of cases, and fails the moment they disagree; it also compares
the four vocabularies (`CLEARANCES`, `WEAPONS`, `US_STATES`,
`PHONE_COUNTRIES`) list-for-list. Change a rule in one and that test names the
other.

Two fields outgrew their column and are **JSON in the TEXT column they already
had** rather than new tables: `weapons_qual` (a list of `{weapon, date}`) and
nothing else. `address` instead got five new columns beside it
(`address_street`, `address_street2`, `address_city`, `address_state`,
`address_zip`) — the old free-text column is **kept**, shown back on the
soldier page to be re-entered, and only written when the leader presses
Discard. `parse_weapons()` returning `None` means "this is the old shape",
which is never an error and never thrown away.

**Phones carry a country.** The stored string is the whole number;
`phone_split()` reads a leading `+<dial>` against `DIAL_CODES` (longest first,
so `+35` never shadows `+351`) and a value with no `+` is the NANP, which is
what every row written before the picker existed is. `+1` is stored with **no
prefix** — it is the default and implied — so those rows keep meaning what
they meant. `+1` is exactly 10 digits and the field refuses an eleventh;
anything else is capped at E.164's 15 less the dial code. A `+` whose country
code is not on the list resolves to dial `''` and is **refused**, never read
as `+1` — otherwise "+999 123 4567" would have found ten digits and built a
`tel:` link that calls a stranger. A value containing a letter is a DSN or an
extension and is stored exactly as typed. `PHONE_COUNTRIES` is curated, not
exhaustive, and only ever extended by hand: a wrong dial code is worse than a
missing one.

Every single date — profile dates, the duty-roster date, a weapons qual —
opens the same two-month range picker the absence modal uses, in its
single-date mode (`rpOpen(event, id, null, 'single')`): one month, one click,
and month/year `<select>`s in place of the title. Those selects carry
**`data-sel`, never `data-act`** — `rpOnClick()` matches `[data-act]` on day
buttons, and a select carrying it was read as a click on a day, wrote
`undefined` to the field and closed the picker. `tests/test_date_picker_js.py`
and `tests/test_phone_control_js.py` drive Chromium, because both of those are
click and keystroke behaviour a DOM-string test cannot see.

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

No CI/CD. Deploys are manual. Production runs on `prodsrv02` (`10.10.60.2`),
user `tdx44`, at `/opt/homelab/platoon`:

```bash
ssh prodsrv02 'cd /opt/homelab/platoon && git pull && docker compose up -d --build'
```

**The A1 (unit-tree) release is the one exception: deploy it ONLY via
`docs/superpowers/plans/2026-09-16-unit-tree-cutover-runbook.md`.** The
ordinary one-liner above runs the new image before the data is migrated,
which is not recoverable by redeploying.

`docker-compose.yml` runs two services: `app` (gunicorn `-w 2` on :5000) and
`cloudflared` (the public ingress tunnel; `TUNNEL_TOKEN` from `.env`).

Email notifications need, in `.env`, `RESEND_API_KEY`, `NOTIFY_FROM` (a sender
on a Resend-verified domain) and `CRON_SECRET`, then the timer installed on
prodsrv02 the same way as the backup one:
`sudo cp scripts/platoon-notify.{service,timer} /etc/systemd/system/ &&
sudo systemctl daemon-reload && sudo systemctl enable --now platoon-notify.timer`.

## Conventions

- The frontend is intentionally one file — add views as `render*()` functions and
  wire them into `render()` / the History-API router, not as separate modules.
  The exception is `public/`: pages that must render signed-out, with no JS.
- New API routes go under `/api/`, return JSON, and use the existing auth
  decorators and `log_action()` for the audit trail.
- `.env` holds all secrets (`SECRET_KEY`, Clerk keys, `TUNNEL_TOKEN`, Stripe keys,
  `RESEND_API_KEY`, `CRON_SECRET`) and is
  gitignored; see `.env.example`.

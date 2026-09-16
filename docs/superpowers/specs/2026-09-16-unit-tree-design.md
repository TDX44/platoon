# A1 — Unit tree, tenancy, self-serve signup

**Date:** 2026-09-16
**Status:** design agreed in conversation; awaiting spec review
**Depends on:** A0 (Postgres, non-owner app role) — shipped 2026-09-16
**Roadmap:** `2026-09-14-multi-tenant-roadmap.md` (locked decisions apply)

## Goal

Anyone can sign in and create their own organisation; nobody can ever see
another organisation's rows, even through an application bug. Inside an
organisation, units nest (company → platoon → squad → team …) and a user
sees and edits the subtree beneath the one unit they are attached to.

Today's single organisation (three hardcoded platoons, 59 soldiers, five
users) migrates into one company root with three platoon children, keeping
its URLs.

## Non-goals (explicitly later)

- **Delegation grants** (lending access for a dated window) — A1.5, the
  immediate follow-up. Additive; nothing here blocks it.
- **Grafting** one root under another. The schema allows it (`parent_id`
  mutable); no flow exists.
- **Positions / chain of command** (B), **billing** (C).
- Any restore of `version: 1` or `2` backups after the migration.

## Row-level security — the guarantee everything else sits on

RLS is the tenant blast door. It is not optional, not "enabled later", and
self-serve signup does not ship in a build where the tenancy test below is
not green.

**Mechanism**

- Every table that holds tenant data carries `root_id INTEGER NOT NULL`:
  `units`, `personnel`, `personnel_profile`, `scheduled_events`,
  `duty_roster`, `report_history`, `audit_log`, `settings`, `users`,
  `invites`.
- On each of them:
  ```sql
  ALTER TABLE t ENABLE ROW LEVEL SECURITY;
  CREATE POLICY tenant ON t
    USING      (root_id = current_setting('app.root_id', true)::int)
    WITH CHECK (root_id = current_setting('app.root_id', true)::int);
  ```
  `current_setting(..., true)` returns NULL when the variable is unset, and
  `root_id = NULL` is never true, so a connection that has not declared a
  tenant sees **zero rows and can write none**. Default deny.
- The request lifecycle from A0 (one transaction per request, commit only
  on status < 400) runs `SELECT set_config('app.root_id', %s, true)` immediately after
  the user is resolved and before any other statement. The `true` makes it
  transaction-local (the same as `SET LOCAL`, which cannot take a bound
  parameter), so it dies with the transaction, so a pooled connection cannot carry one tenant's id into
  the next request. An unattached user (no unit yet) sets `0`, which
  matches nothing.
- The app connects as `platoon_app`, which A0 made a non-owner with no
  `BYPASSRLS`. Table owners bypass policies silently, so **`server.py`
  refuses to boot** if `current_user` owns any protected table or has
  `rolbypassrls` — a `SELECT` against `pg_tables`/`pg_roles` at import,
  next to `init_db()`. `FORCE ROW LEVEL SECURITY` is deliberately not used:
  it would also bind `platoon_owner`, which runs `init_db()` and the
  migration across every root by design.
- `init_db()` (owner) creates the policies idempotently
  (`DROP POLICY IF EXISTS … ; CREATE POLICY …`) so a fresh database and an
  upgraded one end identical. `scripts/pg-roles.sql` is unchanged.

**What it does and does not do**

- Does: make cross-tenant reads and writes impossible for the application
  role regardless of any `WHERE` clause, ORM-free string SQL included.
- Does not: enforce *subtree* visibility inside a tenant. That is
  `can_access()` in the application (below), because a per-row recursive
  policy is the wrong cost for the wrong guarantee.

**Proof (tests/test_tenancy.py)**

Connect as `platoon_app` (the `TEST_APP_DATABASE_URL` harness from A0),
create two roots each with a soldier, then:
1. with `app.root_id` = root A, `SELECT * FROM personnel` returns only A's
   soldier; an `UPDATE personnel SET status='present'` with **no WHERE**
   touches exactly one row; an `INSERT` with `root_id` = B raises.
2. with the variable unset, every protected table returns zero rows.
3. through the Flask app as a user attached at A, `GET /api/personnel/<B's
   id>` is 404, not 403 — the row does not exist from A's side.
4. the boot guard: the test runs `import server` in a subprocess with
   `DATABASE_URL` pointed at `platoon_owner` and asserts it exits non-zero
   naming the guard (a subprocess, because Python caches the module).
Mutation check: drop the policy on `personnel` and (1) and (3) must fail.

## Data model

```
units            id, parent_id NULL→units, root_id, kind, name, slug, created_at
                 UNIQUE(root_id, slug); root rows have root_id = id
users            id, username, email, full_name, clerk_user_id,
                 unit_id NULL→units, role ('owner'|'leader'), root_id
invites          token, label, unit_id→units, role, root_id, created_by,
                 created_at, expires_at, accepted_at, accepted_by
settings         root_id, unit_id NULL→units, key, value
                 UNIQUE INDEX ON (root_id, COALESCE(unit_id, 0), key)
personnel        … unit_id→units, root_id            (platoon TEXT dropped)
scheduled_events … unit_id, root_id                  (platoon dropped)
duty_roster      … unit_id, root_id                  (platoon dropped)
report_history   … unit_id, root_id                  (platoon dropped)
audit_log        … unit_id NULL, root_id             (platoon dropped)
personnel_profile … root_id                          (scoped via personnel)
```

- `kind` is a label from `UNIT_KINDS = company, platoon, squad, team,
  section, detachment, flight, crew`. Nothing enforces nesting order.
- `slug` is derived from `name` (lower, `[a-z0-9]+`, hyphens collapsed),
  suffixed `-2`, `-3` … until unique within the root. Slugs are stable once
  created; renaming a unit does not change its slug.
- `root_id` is a cache of "which tree". The tree is the truth. The only
  operation that could desynchronise it is moving a subtree, which A1 does
  not offer.
- `users.unit_id IS NULL` means "signed in, attached nowhere yet". Such a
  user gets `403` from every `/api/` route except `/api/me`,
  `/api/auth/sync` and `POST /api/units` (root creation).
- `is_admin`, `users.platoons`, `invites.platoons`, `PLATOONS`,
  `unit_name_*` settings, `CLERK_ADMIN_EMAILS` and the first-user admin
  bootstrap are **deleted**.

## Access inside a tenant

- `can_access(user, unit_id)`: `unit_id` is in the subtree rooted at
  `user.unit_id`. The subtree id set is computed once per request with one
  recursive CTE and cached on `g`; routes test membership. Every route that
  used `has_platoon_access` uses this; `has_platoon_access` is deleted.
- **Roles.** `leader` and `owner`. Both see and edit their whole subtree:
  roster, absences, duty, reports, availability, audit log (filtered to the
  subtree), backup export (subtree), creating child units, renaming or
  deleting empty units in the subtree, inviting `leader`s into the subtree,
  editing users attached in the subtree.
  **Owner-only:** grant the `owner` role, rename or delete the root, change
  `org_timezone`, remove a user, full backup restore, delete another
  user's report. `owner` is meaningful only at the root; the migration and
  root creation are the only things that assign it, plus an owner promoting
  someone attached at the root.
- Where a route takes a unit today as `?platoon=` / body `platoon`, it takes
  `unit=<id>` / body `unit_id`. Where it derives the platoon from the row
  being edited, it derives `unit_id` from the row. Same 30 routes, same
  shapes.

## Signup and invites

- **No invite, first sign-in:** `sync_clerk_user()` creates the user with
  `unit_id NULL`. `/api/me` returns `needs_unit: true`. The client shows one
  screen — unit name, kind — and calls `POST /api/units {name, kind}` with
  no `parent_id`, which creates the root (`root_id = id`), attaches the
  caller as `owner`, seeds that root's default TDY lists and timezone, and
  audits `UNIT_CREATE`. Then the normal app loads.
- **With an invite:** attach at `invite.unit_id` with `invite.role`, as
  today. Single use, `INVITE_EXPIRY_DAYS`, `sessionStorage` carry across
  Clerk redirects — all unchanged.
- **Invite form / Manage Access:** label, unit (dropdown of the inviter's
  subtree, default current unit), role. `owner` is offered only to owners
  and only for the root. The three hardcoded platoon checkboxes go.
- Legacy-row matching by email/username in `sync_clerk_user()` stays; it is
  also what lets the owner into the dev stack's copy of production with a
  dev-instance Clerk id.

## Frontend

- `currentPlatoon` → `currentUnit` `{id, slug, name, kind, parent_id}`.
  `PLATOONS`, `PLATOON_PATHS`, `PATH_TO_PLATOON` are deleted. `GET
  /api/units` returns the user's subtree as a flat list `[{id, parent_id,
  kind, name, slug, count}]`; the client resolves `/<slug>/<section>` in
  it. Seeded slugs `1stplatoon`, `2ndplatoon`, `hq` keep existing bookmarks
  working; the root is `company` until renamed (rename does not change the
  slug).
- **Home** renders the tree: the attachment unit at the top, children
  indented, each a card (kind, name, head count). Clicking a card opens the
  familiar per-unit page. A unit's roster is its **subtree's** personnel,
  so a company view is the whole company and a platoon view is that
  platoon and its squads. Formation mode queues the subtree the same way.
- **Add soldier** at a unit with children shows a unit dropdown (subtree,
  default current). **Edit soldier** can move them within the subtree.
- **Units page** under Settings: list the subtree; create child (name,
  kind); rename; delete when it has no personnel, no child units and no
  users. Root rename/delete is owner-only. No move.
- Settings → Organisation (timezone) is owner-only, as today for admins.
  TDY lists are per unit, edited on the unit's Schools/Locations pages.
- The 15 fetches that pass platoon pass `unit`. `REASON_FIELDS`, the
  absence lifecycle, sortable tables, the confirm modal and formation mode
  are untouched.

## Backup, audit

- **Export** = the caller's subtree: `units`, `personnel`,
  `personnel_profile`, `scheduled_events`, `duty_roster`, `report_history`,
  `settings` for those units; `users` and `invites` only for an owner.
  `version: 3`; rows reference units by `slug`, not id.
- **Restore** is owner-only and replaces the caller's whole tree (units
  matched by slug, created if absent). Sequences are resynced after, as
  A0's restore test requires. `version` 1 and 2 files are refused with a
  clear message; anyone holding one restores it *before* the A1 migration.
- **Audit rows** carry `unit_id` (NULL for org-level actions) and
  `root_id`; the page filters to the subtree. New actions: `UNIT_CREATE`,
  `UNIT_RENAME`, `UNIT_DELETE`, `PERSON_MOVE`, `ROLE_CHANGE`.

## Migration of the existing organisation

`scripts/platoons-to-units.py`, run once, inside one transaction, as
`platoon_owner`, after a pg_dump that is the only rollback (A1 is **not**
reversible by redeploying the old image, unlike A0).

1. Insert root `Company` (kind company, slug `company`), then children
   `1st`, `2nd`, `hq` with names from `unit_name_<p>` and slugs from the
   old path map (`1stplatoon`, `2ndplatoon`, `hq`).
2. Add `unit_id`, `root_id` to the scoped tables; backfill from `platoon`;
   set `NOT NULL`; drop `platoon`.
3. Users: `platoons='*'` or `is_admin=1` → root, `owner`. A single platoon
   → that child, `leader`. Several platoons → root, `leader`. Empty →
   `unit_id NULL`. Invites map the same way; drop `is_admin`, `platoons`.
4. Settings: `org_timezone` → `(root, NULL)`; `tdy_*_<p>` → `(root, child)`;
   `unit_name*` rows deleted.
5. Enable RLS and create policies (same code path as `init_db()`).
6. Verify before commit: per-table row counts unchanged; every scoped row
   has a `root_id` equal to the root; zero rows with `unit_id` NULL where
   NOT NULL applies; `personnel` id 1 and id 3 still Carr and Bennett; the
   five users land where step 3 says. Any miss → rollback, exit 1.

Rehearsed on dev against a fresh copy of production, twice clean, before
the cutover, with the A0 runbook as the template. Production's `.env` and
`docker-compose.yml` need no change.

## Tests

- Existing route tests replace their hand-built user dicts with a helper in
  `tests/dbharness.py`: `make_tree()` → root, one child; `as_user(unit,
  role)` monkeypatches `get_current_user`. Assertions that read `?platoon=`
  become `?unit=`.
- New: `tests/test_tenancy.py` (above), `tests/test_units.py` (create root,
  create child, slug uniqueness, rename keeps slug, delete refuses
  non-empty, unattached user 403s everywhere but the three allowed routes,
  leader cannot grant owner, owner-only gates), `tests/test_units_migration.py`
  (a fixture shaped like production through the script; counts, user
  placement, slugs, RLS enabled on every table).
- `tests/test_smoke.py` keeps asserting every `/api/` route is guarded.
- Browser checklist for the rehearsal, confirmed from `audit_log`: new
  Clerk account creates a root; invite a leader into a child; that leader
  sees only the child; owner sees all; bookmark `/2ndplatoon/directory`
  resolves; export then restore.

## Risks

- **Owner-role bypass** — the boot guard and `test_tenancy` step 4 make it
  impossible to run the app as the owner without noticing.
- **Forgetting `SET LOCAL` on a new code path** — every path goes through
  the one request hook; default-deny means the symptom is "nothing loads",
  not "everything leaks".
- **`root_id` drift** — no subtree moves in A1; the migration verifies it.
- **Mechanical churn** (208 backend, 186 frontend mentions of platoon) —
  the A0 method applies: enumerate by grep, convert, prove by tests that
  can fail, and grep for stragglers (`platoon`, `is_admin`, `has_platoon_access`
  must all reach zero outside historical docs).

# Unit-tree (A1) cutover runbook — production

Not reversible by redeploying: the migration rewrites five tables. The
rollback is the pg_dump taken in Step 1 restored with the A0 image.

## Step 1 — backup, and record the rollback commit

```bash
cd /opt/homelab/platoon
./scripts/backup-db.sh && cat backups/LAST_BACKUP        # the dump named here is the rollback
git rev-parse --short HEAD                               # WRITE THIS DOWN
```

## Step 2 — pull and build while the old app serves

**Do NOT run `docker compose up` / the ordinary deploy until Step 4.** The new
image's `init_db()` enables RLS on data that has not been backfilled yet:
every row still has `root_id NULL`, so every policy matches nothing, every
user lands on the create-unit (`needs_unit`) screen, and the first one who
takes that screen at face value and creates a root makes the migration script
refuse to run for good (`units already has rows`) — leaving the pg_dump from
Step 1 as the only way back. Build only; start nothing.

```bash
git pull
docker compose build app
docker compose run --rm -T --no-deps app python -c "import sys; print('new image ok')"
```

## Step 3 — stop, migrate, verify

Pick `<ROOT NAME>` before you start: it must not be blank, and it must not
slugify to `hq`, `1stplatoon` or `2ndplatoon`. The script slugifies the root
name itself (lowercase, non-alphanumerics to `-`) and `units` is
`UNIQUE (root_id, slug)`, so a root whose slug collides with one of the three
children's fixed slugs aborts on a raw `UniqueViolation` traceback instead of
the script's own `die()` message. "HQ" and "1st Platoon" are the names to
avoid; a company name such as "Alpha Company" is safe.

```bash
docker compose stop app
docker compose run --rm -T app python -c "import server; print('schema ok')"    # init_db: additive columns, RLS, functions
docker compose run --rm -T app python scripts/platoons-to-units.py "<ROOT NAME>"  # MIGRATION_DATABASE_URL is in the image env
docker compose exec -T db psql -U platoon_owner -d platoon <<'SQL'
SELECT id, parent_id, kind, name, slug FROM units ORDER BY id;                    -- 1 company + 3 platoons
SELECT id, rank, last, unit_id FROM personnel WHERE id IN (1,3) ORDER BY id;      -- Carr, Bennett
SELECT username, unit_id, role FROM users ORDER BY id;                            -- two owners at the root, 2nd-only leaders at 2nd
SELECT relname, relrowsecurity FROM pg_class WHERE relname IN ('personnel','users','units','settings') ORDER BY 1;  -- all t
SELECT (SELECT count(*) FROM personnel), (SELECT count(*) FROM scheduled_events), (SELECT count(*) FROM audit_log);
SELECT count(*) FROM settings WHERE root_id IS NULL;                             -- 0
SELECT column_name FROM information_schema.columns
  WHERE table_name = 'personnel' AND column_name = 'platoon';                    -- no rows
SQL
```

The script's last line on success is `migration verified`. Anything else means
it did not commit.

**If the script exits non-zero it already rolled back and named the offending
row** (`migration ABORTED: … — rolled back, nothing changed`). The database is
unchanged, but RLS is already enabled from `init_db()` in the step above, so
**do not start the app**: every request would see an un-backfilled tenant.
Fix or remove the row it named — for `invites with no unit: <tokens>`, revoke
those invites — and re-run the script. If it cannot be fixed, go to Step 5
(rollback).

## Step 4 — start and verify

```bash
docker compose up -d --build
docker compose logs --since 2m app | grep -Ei 'error|traceback|refusing to start'; echo "rc=$? (1 = clean)"
curl -s -o /dev/null -w '%{http_code}\n' https://platoon.carr7.com/
```

Browser: sign in (owner) → home shows the company and three platoons →
`/2ndplatoon/directory` bookmark resolves → Units page → Manage Access shows
units and roles → export a backup (version 3).

Then: `./scripts/backup-db.sh` (first Postgres dump of the new shape)

## Step 5 — rollback, only if needed

```bash
docker compose stop app
git checkout <commit from Step 1>
docker compose build app
docker compose exec -T db psql -U platoon_owner -d platoon -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
docker compose exec -T db psql -U platoon_owner -d platoon -v ON_ERROR_STOP=1 -v app_password="$(grep '^DB_APP_PASSWORD=' .env | cut -d= -f2-)" -f - < scripts/pg-roles.sql
docker compose exec -T db pg_restore -U platoon_owner --no-owner -d platoon < backups/<dump from Step 1>
docker compose up -d --build
```

## Dev rehearsal

Read this whole section before starting — do not reconstruct the commands
from memory. Run every step below on the **dev** stack
(`/opt/homelab/platoon-dev`), never on prod, except the one read-only
`pg_dump` against prod's `db` container.

**Hard rule, from the A0 rehearsals:** after ANY `docker compose down` on
dev, immediately `docker compose up -d` for **all** services, not just `db`.
`cloudflared` does not restart itself, and a dev stack with `db` up but
`cloudflared` gone answers every request with a Cloudflare 530 — which then
looks exactly like a migration failure until someone checks `docker compose
ps` and notices `cloudflared` isn't there.

**Two consecutive clean rehearsals are required before the production
cutover is put to the user.** Record each run in
`.superpowers/sdd/2026-09-16-unit-tree/progress.md`, the way A0 did, quoting
the `audit_log` rows that prove each browser step actually happened —
narration ("looks fine") does not count as a clean run.

### Refresh dev's copy of production

```bash
ssh tdx44@10.10.50.200
cd /opt/homelab/platoon
docker compose exec -T db pg_dump -U platoon_owner -Fc platoon > /tmp/prod-copy.dump
exit
scp tdx44@10.10.50.200:/tmp/prod-copy.dump /opt/homelab/platoon-dev/prod-copy.dump
ssh tdx44@10.10.50.200 'rm /tmp/prod-copy.dump'          # read-only on prod; do not leave a copy lying around
```

`pg_dump` against the running `db` container is read-only and takes no lock
that blocks the live app — this step never touches prod's serving path.

### Restore into dev

```bash
cd /opt/homelab/platoon-dev
docker compose down
docker volume rm platoon-dev_pgdata
docker compose up -d db
until docker compose exec -T db pg_isready -U platoon_owner -d platoon -q; do sleep 1; done
DB_APP_PASSWORD="$(grep '^DB_APP_PASSWORD=' .env | cut -d= -f2-)"
docker compose exec -T db psql -U platoon_owner -d platoon -q -v ON_ERROR_STOP=1 \
  -v app_password="$DB_APP_PASSWORD" -f - < scripts/pg-roles.sql
docker compose exec -T db pg_restore -U platoon_owner --no-owner -d platoon < prod-copy.dump
docker compose up -d          # ALL services — cloudflared included, see the hard rule above
```

### Run Steps 2-4 on dev

Same commands as production above, run from `/opt/homelab/platoon-dev`
against the copy just restored, with `<ROOT NAME>` the same value planned
for production — including the "do not start the app before Step 4" rule,
which matters here for exactly the same reason.

Stop after Step 4's `curl` on dev. The **browser** half of Step 4 cannot pass
yet: the restored rows carry production's Clerk ids, so no dev account can
sign into them. Do the reclaim below first, then work the browser checklist
that follows it (which covers everything Step 4's browser half asked for).

### Reclaim the migrated accounts under the dev Clerk instance

**Rehearsal-only. Production keeps its Clerk ids and none of this runs there.**

Dev runs its own Clerk instance (`fluent-kite-43.clerk.accounts.dev`), whose
account ids do not match production's — restoring prod's data into dev
otherwise locks every migrated user out, as
`[[clerk-instance-switch-locks-users-out]]` describes for A0.

Blanking `clerk_user_id` no longer works. Under R18 an **attached** legacy row
(`clerk_user_id = ''` with a `root_id`) can only be claimed by a sign-in that
presents a live invite for that same root; a bare email match is deliberately
not enough, because it was a cross-tenant account takeover. So the dev id is
mapped onto the migrated row by hand instead. Do this **once per account you
actually need on dev** — usually just the owner:

1. Sign in on the dev Clerk instance with that account. Expect the create-unit
   (`needs_unit`) screen. **Do not create a unit.** The sign-in's only purpose
   is to make Clerk mint the dev-side id and have `/api/auth/sync` write a
   stranger row for it.

   If the sign-in instead fails with *"That username is already in use
   locally"*, no stranger row was written: `users.username` is UNIQUE and the
   migrated row already holds the one this account would take. Park it, sign
   in again, and put it back in step 3 —

   ```bash
   docker compose exec -T db psql -U platoon_owner -d platoon \
     -c "UPDATE users SET username = username || '.migrated' WHERE username = '<the migrated owner>';"
   ```

   then step 3's `UPDATE` matches `'<the migrated owner>.migrated'` and sets
   `username` back alongside `clerk_user_id`.

2. Find that stranger row and delete it — the partial unique index
   `idx_users_clerk_user_id` would otherwise collide when the same id is
   written onto the migrated row in step 3:

   ```bash
   docker compose exec -T db psql -U platoon_owner -d platoon \
     -c "SELECT id, username, clerk_user_id FROM users WHERE clerk_user_id <> '' AND unit_id IS NULL;"
   ```

   Note the `clerk_user_id`, then (substituting the id it printed):

   ```bash
   docker compose exec -T db psql -U platoon_owner -d platoon \
     -c "DELETE FROM users WHERE id = <that id>;"
   ```

3. Put that dev Clerk id on the migrated row:

   ```bash
   docker compose exec -T db psql -U platoon_owner -d platoon \
     -c "UPDATE users SET clerk_user_id = '<that dev id>' WHERE username = '<the migrated owner>';"
   ```

4. Reload the app in the browser. It now shows the migrated company instead of
   the create-unit screen.

If a step needs SQL with `$$` or anything else the remote shell would expand,
write it to a file and `scp` it over, then `psql -f`, the way the A0 runbook
did — do not paste it through `ssh … -c`.

### Browser checklist, each step confirmed from `audit_log`

1. Sign in as the migrated owner on the dev Clerk instance — after the
   id-mapping above, so this is a reload rather than a first sign-in → home
   shows the company and three platoons, not the `needs_unit` screen. Confirm:
   `SELECT * FROM audit_log WHERE action = 'LOGIN' ORDER BY id DESC LIMIT 1`
   shows that username, with `root_id` pointing at the migrated company.
2. `/2ndplatoon/directory` bookmark resolves to the migrated 2nd platoon.
3. Units page → Manage Access shows the migrated units and roles.
4. Export a backup (version 3), confirm the file names units by slug.
5. **Sign up as a brand-new dev Clerk account** (an email never seen before
   in this database). It gets the `needs_unit` screen, not the migrated
   company — create a new root here. Confirm from `audit_log`:
   `UNIT_CREATE` is that account's first-ever row (no `LOGIN` precedes it —
   pre-tenant sign-in is not audited), and its `root_id` differs from the
   migrated company's.
6. As that new account, confirm the home page, Units page and any subtree
   list show **nothing** from the migrated company — no platoons, no
   soldiers, no users. This is the tenancy guarantee, proven with a second,
   independent tenant in the same database rather than taken on faith from
   `tests/test_tenancy.py` alone.

Two clean runs of everything above, back to back, before the production
cutover is put to the user.

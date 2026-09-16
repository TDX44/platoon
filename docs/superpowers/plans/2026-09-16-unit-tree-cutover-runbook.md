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

```bash
git pull
docker compose build app
docker compose run --rm -T --no-deps app python -c "import sys; print('new image ok')"
```

## Step 3 — stop, migrate, verify

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
SQL
```

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
for production.

### Reclaim the migrated accounts under the dev Clerk instance

Dev runs its own Clerk instance
(`fluent-kite-43.clerk.accounts.dev`), whose account ids do not match
production's — restoring prod's data into dev otherwise locks every
migrated user out, as
`[[clerk-instance-switch-locks-users-out]]` describes for A0. Clear the
copied Clerk ids so the next sign-in on each account re-adopts its row by
email:

```bash
docker compose exec -T db psql -U platoon_owner -d platoon -c "UPDATE users SET clerk_user_id = ''"
```

### Browser checklist, each step confirmed from `audit_log`

1. Sign in as the migrated owner on the dev Clerk instance → the account
   claims its pre-existing row by email (not a fresh `needs_unit` screen) →
   home shows the company and three platoons. Confirm: `SELECT * FROM
   audit_log WHERE action = 'LOGIN' ORDER BY id DESC LIMIT 1` shows that
   username, with `root_id` pointing at the migrated company.
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

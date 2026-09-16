# Postgres cutover runbook — production

Read from this file at the keyboard. Do not reconstruct the commands.
Every command below was run in that order on the dev stack in rehearsal
runs 1 and 2 (2026-09-15) against a copy of production's database.

**Rule:** if anything prints an error, stop at that step. Nothing before
Step 7's final command has changed the SQLite file, and Step 9 rolls back.

## Order matters in two places

1. **Roles before `import server`.** Importing `server` runs `init_db()` as
   `platoon_owner` and then `load_app_timezone()` as `platoon_app`. If the
   roles script has not run yet the second connection fails on a missing role.
   The plan's original Step 7 had these the other way round; the rehearsals
   ran roles first and that is what is written here.
2. **`import server` before the migration.** `init_db()` creates the tables
   the migration copies into. The migration then deletes the three
   `WO1 Smith, John` placeholders and six seeded `tdy_*` settings that
   `init_db()` put there, so production's rows land on their real ids.

## Step 5 — merge and take a fresh backup (workstation, then prodsrv02)

```bash
cd /home/tdx44/github/platoon
git checkout main && git pull
git rev-parse --short HEAD          # WRITE THIS DOWN: it is the rollback commit
git merge --no-ff postgres-port
git push origin main
```

```bash
ssh tdx44@10.10.50.200
cd /opt/homelab/platoon
./scripts/backup-db.sh && cat backups/LAST_BACKUP     # still the SQLite script: prod has not pulled yet
ls -la backups/ | tail -3
```

Expected: `OK ... 61 personnel ...` (or whatever today's head count is).

## Step 6 — stop the app, keep the SQLite file, add the passwords

```bash
cd /opt/homelab/platoon
docker compose stop app
ls -la data/accountability.db        # stays exactly here; it is the rollback
ls data/accountability.db-wal data/accountability.db-shm 2>/dev/null   # expect: no such file (journal_mode=delete)
```

```bash
printf 'DB_OWNER_PASSWORD=%s\nDB_APP_PASSWORD=%s\n' "$(openssl rand -hex 24)" "$(openssl rand -hex 24)" >> .env
grep -c '^DB_' .env                  # expect 2
```

`.env` is read by compose, not by your shell. Every later command that needs
the app password pulls it out of the file explicitly.

## Step 7 — migrate

**Executed on production 2026-09-16 14:33Z in this order, 28 s of downtime.**
Pull, build, start Postgres and run the roles script while the OLD app is
still serving — none of that touches what it reads — and stop the app only
for the schema + migrate + verify. The explicit `build app` matters: the
old image is still tagged `platoon-app`, and `docker compose run` does not
rebuild, so without it the old SQLite server imports "successfully".

```bash
cd /opt/homelab/platoon
git pull                             # brings docker-compose.yml with the db service
docker compose build app
docker compose run --rm -T --no-deps app python -c "import psycopg; print('new image: psycopg', psycopg.__version__)"
docker compose up -d db
until docker compose exec -T db pg_isready -U platoon_owner -d platoon -q; do sleep 1; done
```

```bash
DB_APP_PASSWORD="$(grep '^DB_APP_PASSWORD=' .env | cut -d= -f2-)"
docker compose exec -T db psql -U platoon_owner -d platoon -q -v ON_ERROR_STOP=1 \
  -v app_password="$DB_APP_PASSWORD" -f - < scripts/pg-roles.sql
echo "roles rc=$?"                   # expect 0; the script is idempotent, re-run it if in doubt
```

Now Step 6's `docker compose stop app`, then:

```bash
docker compose run --rm app python -c "import server; print('schema ok')"
docker compose run --rm app python scripts/sqlite-to-pg.py /data/accountability.db
```

Expected: `cleared 3 placeholder personnel row(s) seeded by init_db()`,
`cleared 6 settings row(s) seeded by init_db()`, one count line per table,
then **`migration verified`**.
If it prints `MISMATCH` or `migration ABORTED`: nothing was committed. Go to Step 9.

Verify deeper than counts before starting the app (counts once passed while
losing three real soldiers):

```bash
docker compose exec -T db psql -U platoon_owner -d platoon <<'SQL'
SELECT id, rank, last, first FROM personnel WHERE id IN (1,3) ORDER BY id;   -- 1 Carr, 3 Bennett
SELECT count(*) AS placeholders FROM personnel WHERE rank='WO1' AND last='Smith' AND first='John';  -- 0
SELECT key, left(value,60) FROM settings WHERE key LIKE 'tdy_schools_%';    -- production's lists, not the seed
SELECT 'personnel' AS seq, last_value, is_called FROM personnel_id_seq                -- is_called = t on every
UNION ALL SELECT 'users', last_value, is_called FROM users_id_seq                       -- row that copied data;
UNION ALL SELECT 'audit_log', last_value, is_called FROM audit_log_id_seq               -- duty_roster is empty in
UNION ALL SELECT 'duty_roster', last_value, is_called FROM duty_roster_id_seq           -- prod so f there is fine
UNION ALL SELECT 'scheduled_events', last_value, is_called FROM scheduled_events_id_seq
UNION ALL SELECT 'report_history', last_value, is_called FROM report_history_id_seq;
SELECT (SELECT count(*) FROM scheduled_events WHERE state='active') AS active_events,
       (SELECT count(*) FROM personnel WHERE status<>'present') AS non_present;          -- equal
SELECT count(*) AS open_ended FROM scheduled_events WHERE to_date='';
SQL
```

## Step 8 — start and verify

```bash
docker compose up -d --build
sleep 5
docker compose ps
docker compose logs --since 2m app | grep -Ei 'error|traceback|InFailedSql' ; echo "log grep rc=$? (1 = clean)"
curl -s -o /dev/null -w '%{http_code}\n' https://platoon.carr7.com/
```

In a browser at https://platoon.carr7.com — there must be **no amber DEV bar**:

1. Roster loads, head count matches.
2. Edit one absence; the audit log shows `EDIT_SCHEDULE` with a Central timestamp.
3. Settings → Manage access loads.
4. Sign out and sign back in.

Then prove the nightly backup works on Postgres before the timer finds out:

```bash
./scripts/backup-db.sh && cat backups/LAST_BACKUP     # now the pg_dump script
ls -la backups/ | tail -2                              # a platoon-*.dump alongside the old .db.gz files
```

Confirm from the audit log rather than from memory:

```bash
docker compose exec -T db psql -U platoon_owner -d platoon -c \
  "SELECT id, timestamp, username, action FROM audit_log ORDER BY id DESC LIMIT 6"
```

## Step 9 — rollback, only if needed

```bash
cd /opt/homelab/platoon
docker compose stop app
git checkout <the commit written down in Step 5>
docker compose up -d --build app
curl -s -o /dev/null -w '%{http_code}\n' https://platoon.carr7.com/
```

The old image reads `data/accountability.db`, which nothing above modified.
The `db` container is left running and unused; `git checkout main` later
re-attaches it. To retry, `docker compose down db && docker volume rm
platoon_pgdata` gives a clean Postgres for the next attempt.

## Step 10 — after seven clean days

```bash
mv data/accountability.db data/accountability.db.pre-postgres
```

Keep it. Do not delete it.

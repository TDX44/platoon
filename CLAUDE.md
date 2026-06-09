# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Platoon Accountability — a personnel accountability tracker for A Co. 15th MI BN (AE).
Flask backend + a single-file vanilla-JS SPA. No build step, no test suite, no linter.

## Commands

```bash
# Run locally (Flask dev server, port 5000, debug=True, auto-reset thread active)
pip install -r requirements.txt
python server.py

# Run production-like (gunicorn + Cloudflare tunnel sidecar)
docker compose up -d --build      # needs a .env file (see .env.example)
```

There are no tests and no lint config — do not invent commands for them.

`gunicorn` is not in `requirements.txt`; it is installed only inside the Docker image.

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
hard reload (this is the "SPA reload 404" fix).

### Data layer

SQLite at `DB_PATH` = `${DATA_DIR}/accountability.db` (`DATA_DIR` defaults to the
repo dir locally, `/data` in Docker — a mounted volume). `get_db()` opens a fresh
connection per call with `Row` factory. `init_db()` runs at **module import** (so
it also runs under gunicorn) and creates tables idempotently with
`CREATE TABLE IF NOT EXISTS`. There is no migration framework — schema changes are
made by editing the `CREATE TABLE` statements and adding ad-hoc `ALTER`/backfill
logic in `init_db()`.

Tables: `personnel`, `settings`, `users`, `audit_log`, `duty_roster`,
`scheduled_events`. (Legacy `training_*` tables from the removed 350-1
tracker feature may still exist in older database files; they are unused.)

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

### Background reset (important gotcha)

`_midnight_reset_worker` clears daily `present` status and promotes
`scheduled_events` whose start date has arrived. **The thread is started only
inside `if __name__ == '__main__'`**, so it runs under `python server.py` but
**NOT under gunicorn in production**. In production, scheduled-event activation
relies on the `/api/activate-scheduled` endpoint instead.

### Backup

`/api/backup` exports a `version: 1` JSON snapshot; `/api/backup/restore` imports
one. If you change the schema, update both, and keep the `version` check working.

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
- New API routes go under `/api/`, return JSON, and use the existing auth
  decorators and `log_action()` for the audit trail.
- `.env` holds all secrets (`SECRET_KEY`, Clerk keys, `TUNNEL_TOKEN`) and is
  gitignored; see `.env.example`.

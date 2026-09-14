-- Ownership split. platoon_owner owns the schema and runs init_db();
-- platoon_app is what the running application connects as and can only read
-- and write rows. A1 adds RLS policies, which a table owner would bypass
-- silently -- which is the reason the application is not the owner.

-- Idempotent: a repair re-run (e.g. after schema public was dropped and
-- recreated, which also destroys the ALTER DEFAULT PRIVILEGES below) must not
-- fail on "role already exists" -- that leaves psql exiting non-zero under
-- set -e, or nothing applied at all under ON_ERROR_STOP=1. Re-running also
-- rotates the password.
--
-- A PL/pgSQL DO $$ ... $$ block will NOT work here: psql does not interpolate
-- :'app_password' inside a dollar-quoted string, so the literal text ":'..'"
-- reaches the server and fails with a syntax error. \gset + \if keeps the
-- CREATE/ALTER as plain SQL, where psql's substitution still applies.
SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'platoon_app') AS role_exists \gset

\if :role_exists
  ALTER ROLE platoon_app WITH LOGIN PASSWORD :'app_password';
\else
  CREATE ROLE platoon_app LOGIN PASSWORD :'app_password';
\endif

GRANT CONNECT ON DATABASE platoon TO platoon_app;
GRANT USAGE ON SCHEMA public TO platoon_app;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO platoon_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO platoon_app;

-- Tables created later by init_db() inherit the same grants.
ALTER DEFAULT PRIVILEGES FOR ROLE platoon_owner IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO platoon_app;
ALTER DEFAULT PRIVILEGES FOR ROLE platoon_owner IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO platoon_app;

"""Give each test its own isolated Postgres schema, as the roles production uses.

setup() creates a throwaway schema as the owner, grants it to platoon_app the
same way scripts/pg-roles.sql grants public, points MIGRATION_DATABASE_URL
(owner: init_db, migrations) and DATABASE_URL (app role: everything the app
does) at it, and returns the schema name. Call it before importing server;
teardown() at the end of main().

Fixture rows are written through owner_conn(): the owner bypasses RLS, which
is what a fixture wants. App behaviour goes through server.app.test_client()
as platoon_app, which is what RLS binds — so a test that reads a row the app
should not see gets exactly what production would get: nothing.
"""
import os
import secrets

import psycopg
from psycopg.rows import dict_row

DEFAULT_URL = 'postgresql://platoon_owner:platoon@127.0.0.1:5432/platoon'


def admin_url():
    return os.environ.get('TEST_DATABASE_URL', DEFAULT_URL)


def app_url():
    url = os.environ.get('TEST_APP_DATABASE_URL')
    assert url, ('TEST_APP_DATABASE_URL is required: the app is exercised as platoon_app '
                 'so that row-level security binds. Run scripts/pg-roles.sql and export it.')
    return url


def _with_schema(url, schema):
    sep = '&' if '?' in url else '?'
    return f'{url}{sep}options=-csearch_path%3D{schema}'


def setup():
    """Create a throwaway schema; point the owner and app URLs at it."""
    schema = 'test_' + secrets.token_hex(6)
    assert schema.replace('_', '').isalnum(), schema
    with psycopg.connect(admin_url(), autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{schema}"')
        conn.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO platoon_app')
        conn.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" '
                     'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO platoon_app')
        conn.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" '
                     'GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO platoon_app')
    os.environ['MIGRATION_DATABASE_URL'] = _with_schema(admin_url(), schema)
    os.environ['DATABASE_URL'] = _with_schema(app_url(), schema)
    return schema


def teardown(schema):
    """Drop the test schema, even when the test died mid-transaction.

    A failing assertion between owner_conn() and close() leaves a connection
    holding locks on these tables, and DROP SCHEMA then waits on it forever —
    the suite hangs instead of reporting the real failure. So: bound the wait,
    and if it expires, terminate the other backends of this database (only our
    own two roles, never anything else that happens to share the server) and
    try once more.
    """
    with psycopg.connect(admin_url(), autocommit=True) as conn:
        conn.execute("SET lock_timeout = '5s'")
        try:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            return
        except psycopg.errors.LockNotAvailable:
            pass
        conn.execute(
            'SELECT pg_terminate_backend(pid) FROM pg_stat_activity '
            'WHERE datname = current_database() AND pid <> pg_backend_pid() '
            "AND usename IN ('platoon_owner', 'platoon_app')")
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def owner_conn():
    """A connection that bypasses RLS, for writing fixtures. Caller closes it."""
    return psycopg.connect(os.environ['MIGRATION_DATABASE_URL'], row_factory=dict_row)


def make_tree(name='Test Co'):
    """One root (company) with one platoon child. Returns ids and the child slug."""
    conn = owner_conn()
    try:
        slug = name.lower().replace(' ', '-')
        root = conn.execute(
            'INSERT INTO units (parent_id, root_id, kind, name, slug) VALUES (NULL, 0, %s, %s, %s) RETURNING id',
            ('company', name, slug)).fetchone()['id']
        conn.execute('UPDATE units SET root_id = %s WHERE id = %s', (root, root))
        child = conn.execute(
            'INSERT INTO units (parent_id, root_id, kind, name, slug) VALUES (%s, %s, %s, %s, %s) RETURNING id',
            (root, root, 'platoon', '2nd Platoon', '2ndplatoon')).fetchone()['id']
        conn.execute("INSERT INTO settings (root_id, unit_id, key, value) VALUES (%s, NULL, 'org_timezone', 'America/Chicago')", (root,))
        for uid in (root, child):
            for kind in ('schools', 'locations'):
                conn.execute('INSERT INTO settings (root_id, unit_id, key, value) VALUES (%s, %s, %s, %s)',
                             (root, uid, f'tdy_{kind}', '[]'))
        conn.commit()
    finally:
        conn.close()
    return {'root': root, 'child': child, 'child_slug': '2ndplatoon'}


def make_user(unit_id, role='leader', username=None):
    """A Clerk-synced user attached at unit_id (None = signed in, attached nowhere)."""
    conn = owner_conn()
    try:
        root_id = None
        if unit_id is not None:
            root_id = conn.execute('SELECT root_id FROM units WHERE id = %s', (unit_id,)).fetchone()['root_id']
        username = username or f'user-{secrets.token_hex(3)}'
        row = conn.execute(
            'INSERT INTO users (username, password_hash, clerk_user_id, email, unit_id, role, root_id) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING *',
            (username, 'x', f'clerk_{username}', f'{username}@example.com', unit_id, role, root_id)
        ).fetchone()
        conn.commit()
        return dict(row)
    finally:
        conn.close()


def as_user(user):
    """Make the app treat `user` as the signed-in user. The decorators still
    call set_tenant() themselves from user['root_id'], so RLS is exercised."""
    import server
    server.get_current_user = lambda: user

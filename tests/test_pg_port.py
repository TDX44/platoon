"""Things the SQLite-to-Postgres conversion can silently get wrong.

Run with: python tests/test_pg_port.py

Module-level setup() runs once, before anything imports `server` — Python
caches modules, so `server.DATABASE_URL` binds at first import. A per-test
setup()/import server would leave every test after the first silently
asserting against the first test's schema. Later tasks add test functions
here that import server; they must not call dbharness.setup() themselves.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)

import psycopg  # noqa: E402

import dbharness  # noqa: E402

_SCHEMA = dbharness.setup()

import server  # noqa: E402


def test_harness_isolates():
    """Each test gets a private schema that really is private.

    Exercises the harness itself with raw psycopg (no `server` import), so it
    creates and drops its own two extra schemas rather than reusing _SCHEMA.
    """
    module_url = os.environ['DATABASE_URL']

    a = dbharness.setup()
    url_a = os.environ['DATABASE_URL']
    with psycopg.connect(url_a) as conn:
        conn.execute('CREATE TABLE only_in_a (id int)')
        conn.commit()

    b = dbharness.setup()
    url_b = os.environ['DATABASE_URL']
    with psycopg.connect(url_b) as conn:
        rows = conn.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = current_schema() AND table_name = 'only_in_a'"
        ).fetchall()
        assert rows == [], 'schema b must not see schema a tables'

    dbharness.teardown(a)
    dbharness.teardown(b)

    with psycopg.connect(dbharness.admin_url()) as conn:
        left = conn.execute(
            'SELECT 1 FROM information_schema.schemata WHERE schema_name = %s', (a,)
        ).fetchall()
        assert left == [], 'teardown must drop the schema'

    # Restore the env to the module-level schema — setup() above rebound it
    # twice (to a, then b), and leaving it on a dropped schema would break
    # anything that runs after this test in the same process.
    os.environ['DATABASE_URL'] = module_url
    os.environ['MIGRATION_DATABASE_URL'] = module_url


def test_init_db_is_idempotent():
    """init_db() runs at import under gunicorn, so it must survive re-running."""
    server.init_db()          # second call; the import already ran it once
    conn = server.get_db()
    names = {r['table_name'] for r in conn.execute(
        'SELECT table_name FROM information_schema.tables '
        'WHERE table_schema = current_schema()').fetchall()}
    conn.close()

    for expected in ('personnel', 'personnel_profile', 'settings', 'users',
                      'audit_log', 'duty_roster', 'scheduled_events',
                      'invites', 'report_history'):
        assert expected in names, f'{expected} table missing after init_db()'


def main():
    try:
        test_harness_isolates()
        test_init_db_is_idempotent()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

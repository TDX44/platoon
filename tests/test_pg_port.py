"""Things the move to Postgres can silently get wrong.

Covered here: schema isolation in the harness, an idempotent init_db(), a
real error surviving init_db()'s failure path, and the transaction semantics
the SQLite build never had to think about -- an audit row committing with the
change it describes, a failed or 4xx request rolling nothing back in, a failed
audit write never passing for a 200 -- plus what the unprivileged app role may
and may not do to the schema and to a sequence.

The one-shot SQLite import this file also used to drive was retired with
scripts/sqlite-to-pg.py: an A0 SQLite file can only be lifted by an A0 image,
because the A1 schema has no `platoon` column and requires unit_id/root_id
that no SQLite file ever carried. The A0 runbook at its own commit still
documents that path.

Run with: python tests/test_pg_port.py

Module-level setup() runs once, before anything imports `server` — Python
caches modules, so `server.DATABASE_URL` binds at first import. A per-test
setup()/import server would leave every test after the first silently
asserting against the first test's schema. Later tasks add test functions
here that import server; they must not call dbharness.setup() themselves.
"""
import os
import sys
import threading

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)

import psycopg  # noqa: E402

import dbharness  # noqa: E402

_SCHEMA = dbharness.setup()

import server  # noqa: E402

# Flask locks route registration after the app has handled its first request
# ("the setup method 'route' can no longer be called..."), so these probes for
# the commit/rollback tests below must be registered once, up front, before
# any test_client() request runs — not inside the test functions that use
# them.


@server.app.route('/__test_probe_500')
def _probe_500():
    conn = server.get_db()
    server.set_tenant(conn, 0)
    conn.execute("INSERT INTO settings (root_id, unit_id, key, value) VALUES (0, NULL, 'probe_500', 'WROTE') "
                 "ON CONFLICT (root_id, COALESCE(unit_id, 0), key) DO UPDATE SET value = EXCLUDED.value")
    raise RuntimeError('deliberate failure for the probe')


@server.app.route('/__test_probe_400')
def _probe_400():
    conn = server.get_db()
    server.set_tenant(conn, 0)
    conn.execute("INSERT INTO settings (root_id, unit_id, key, value) VALUES (0, NULL, 'probe_400', 'WROTE') "
                 "ON CONFLICT (root_id, COALESCE(unit_id, 0), key) DO UPDATE SET value = EXCLUDED.value")
    return server.jsonify({'error': 'deliberate'}), 400


_audit_probe_returned_200 = False


@server.app.route('/__test_probe_audit_fails')
def _probe_audit_fails():
    global _audit_probe_returned_200
    conn = server.get_db()
    server.set_tenant(conn, 0)
    conn.execute("INSERT INTO settings (root_id, unit_id, key, value) VALUES (0, NULL, 'probe_audit', 'WROTE') "
                 "ON CONFLICT (root_id, COALESCE(unit_id, 0), key) DO UPDATE SET value = EXCLUDED.value")
    # Break the audit insert from inside this very transaction, so the
    # rollback undoes the damage too. This is the shape of the real failure:
    # log_action's statement errors, Postgres marks the whole transaction
    # aborted, and log_action swallows the exception. (A1: the app role owns
    # no tables, so the break can no longer be a DDL rename — an aborted
    # transaction is the same condition and is what actually bites.)
    try:
        conn.execute('SELECT 1 / 0')
    except psycopg.errors.DivisionByZero:
        pass
    server.log_action('PROBE_AUDIT', 'this insert cannot land')
    _audit_probe_returned_200 = True   # log_action must not have raised
    return server.jsonify({'ok': True}), 200


@server.app.route('/__test_probe_409')
def _probe_409():
    """A route that deliberately answers 4xx on an aborted transaction.

    update_user() and sync_clerk_user() both do exactly this: catch a
    UniqueViolation and answer 409 / "username already in use". Those answers
    are already honest about nothing having been saved, so the
    aborted-transaction guard must leave their status code alone.
    """
    conn = server.get_db()
    try:
        conn.execute('SELECT 1 / 0')
    except Exception:
        pass
    return server.jsonify({'error': 'Username already exists'}), 409


@server.app.route('/__test_probe_200')
def _probe_200():
    conn = server.get_db()
    server.set_tenant(conn, 0)
    conn.execute("INSERT INTO settings (root_id, unit_id, key, value) VALUES (0, NULL, 'probe_200', 'WROTE') "
                 "ON CONFLICT (root_id, COALESCE(unit_id, 0), key) DO UPDATE SET value = EXCLUDED.value")
    return server.jsonify({'ok': True}), 200


def _throwaway_schema():
    """A private schema for one test, plus the callback that puts the env back.

    dbharness.setup() rebinds DATABASE_URL and MIGRATION_DATABASE_URL, and
    teardown() drops the schema without unbinding them — so a test that forgets
    to restore leaves every later owner_conn() in this file pointed at a schema
    that no longer exists. `server` itself is immune (it cached both URLs at
    import), which is exactly why this went unnoticed until fixtures started
    reading the environment.
    """
    saved = (os.environ.get('DATABASE_URL'), os.environ.get('MIGRATION_DATABASE_URL'))

    def restore():
        os.environ['DATABASE_URL'], os.environ['MIGRATION_DATABASE_URL'] = saved

    return dbharness.setup(), restore


def test_harness_isolates():
    """Each test gets a private schema that really is private.

    Exercises the harness itself with raw psycopg (no `server` import), so it
    creates and drops its own two extra schemas rather than reusing _SCHEMA.
    DDL runs on the owner URL: platoon_app owns nothing and cannot create
    tables, which is the whole point of the split (test_app_role_cannot_change_schema).
    """
    module_url = os.environ['DATABASE_URL']
    module_migration_url = os.environ['MIGRATION_DATABASE_URL']

    a = dbharness.setup()
    url_a = os.environ['MIGRATION_DATABASE_URL']
    with psycopg.connect(url_a) as conn:
        conn.execute('CREATE TABLE only_in_a (id int)')
        conn.commit()

    b = dbharness.setup()
    url_b = os.environ['MIGRATION_DATABASE_URL']
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
    os.environ['MIGRATION_DATABASE_URL'] = module_migration_url


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


def test_settings_scope_key_survives_a_restart():
    """`settings` is keyed on (root_id, unit_id, key), and stays keyed on it.

    A1 dropped the old PRIMARY KEY (key) — one organization's org_timezone must
    not collide with another's — and replaced it with the settings_scope_key
    unique index. init_db() runs on every start, so this checks both halves:
    a second run neither disturbs existing rows nor drops the new key, and two
    roots can hold the same key while one root cannot hold it twice.

    Runs on the module-level schema: server cached MIGRATION_DATABASE_URL at
    import, so init_db() would build its tables there whatever the environment
    says (the reason the old seeding test's private schema was decorative).
    """
    conn = dbharness.owner_conn()
    try:
        for root in (1, 2):
            conn.execute('INSERT INTO settings (root_id, unit_id, key, value) '
                         'VALUES (%s, NULL, %s, %s)', (root, 'scope_probe', f'zone-{root}'))
        conn.commit()

        server.init_db()   # the restart

        rows = conn.execute("SELECT root_id, value FROM settings WHERE key = 'scope_probe' "
                            'ORDER BY root_id').fetchall()
        assert [(r['root_id'], r['value']) for r in rows] == [(1, 'zone-1'), (2, 'zone-2')], rows
        try:
            conn.execute("INSERT INTO settings (root_id, unit_id, key, value) "
                         "VALUES (1, NULL, 'scope_probe', 'again')")
            assert False, 'settings_scope_key did not survive the restart'
        except psycopg.errors.UniqueViolation:
            conn.rollback()
    finally:
        conn.execute("DELETE FROM settings WHERE key = 'scope_probe'")
        conn.commit()
        conn.close()


def test_init_db_failure_surfaces_the_real_error():
    """A failed migration must not be masked by the advisory-unlock cleanup.

    init_db()'s finally unlocks the session-level advisory lock before
    closing the connection. If the body raised, that connection's
    transaction is already aborted, so the unlock itself can raise
    InFailedSqlTransaction -- and if that escaped from finally it would
    replace the real cause. Task 9 runs init_db() against production data
    for the first time; an operator debugging a failed migration needs the
    real exception, not "current transaction is aborted".
    """
    schema, restore_env = _throwaway_schema()
    real_columns = server._columns

    def _broken_columns(cur, table):
        if table == 'users':
            cur.execute('SELECT * FROM this_table_does_not_exist')
        return real_columns(cur, table)

    server._columns = _broken_columns
    try:
        try:
            server.init_db()
            assert False, 'init_db() should have raised'
        except psycopg.errors.UndefinedTable:
            pass  # the real cause -- not InFailedSqlTransaction
    finally:
        server._columns = real_columns

    # The failed call must still have released the advisory lock: a fresh
    # connection calling init_db() again must not block waiting for it. Runs
    # on a thread with a timeout so a regression fails loudly instead of
    # hanging the whole test suite.
    done = threading.Event()

    def _second_call():
        server.init_db()
        done.set()

    t = threading.Thread(target=_second_call, daemon=True)
    t.start()
    t.join(timeout=5)
    assert done.is_set(), 'init_db() blocked -- the advisory lock was not released after the failure'

    dbharness.teardown(schema)
    restore_env()


def test_audit_row_commits_with_its_change():
    """log_action must join the caller's transaction, not open its own.

    Under SQLite it opened a second connection because SQLite has one writer.
    In Postgres that would mean an audit row committing for a change that then
    failed — an audit trail that lies.
    """
    schema, restore_env = _throwaway_schema()
    import server

    with server.app.test_request_context('/'):
        conn = server.get_db()
        assert conn is server.get_db(), 'get_db must return one connection per request'

        server.set_tenant(conn, 0)
        conn.execute("INSERT INTO settings (root_id, unit_id, key, value) VALUES (0, NULL, 'probe', 'v1') "
                     "ON CONFLICT (root_id, COALESCE(unit_id, 0), key) DO UPDATE SET value = EXCLUDED.value")
        server.log_action('PROBE', 'details')
        conn.rollback()

        rows = conn.execute("SELECT 1 FROM audit_log WHERE action = 'PROBE'").fetchall()
        assert rows == [], 'a rolled-back change must roll back its audit row too'

    dbharness.teardown(schema)
    restore_env()


def test_audit_row_survives_a_successful_change():
    """The positive half of the pair above.

    log_action() swallows every exception (`except Exception: pass`), so a
    log_action that is silently dead — writes nothing, raises internally and
    is swallowed — produces the exact same observable result as the negative
    test above: no matching row in audit_log. That test alone cannot tell
    "correctly rolled back" apart from "never wrote anything in the first
    place". This one proves the row really lands when the surrounding
    transaction actually commits.

    Uses the module-level schema (_SCHEMA) rather than a fresh one — no
    dbharness.setup() call here, per the file-level rule above.

    test_request_context() pushes a request context directly — it never runs
    the real dispatch machinery (full_dispatch_request / after_request), only
    do_teardown_request on pop. _close_db's commit is now gated on the
    after_request-set g.db_commit flag (a real request sets it; this bare
    context never does), so this commits explicitly rather than relying on
    teardown to do it — otherwise teardown would see no flag and roll back,
    same as it correctly does for an unhandled exception or an early error
    return in a real request.
    """
    with server.app.test_request_context('/'):
        conn = server.get_db()
        server.set_tenant(conn, 0)
        conn.execute("INSERT INTO settings (root_id, unit_id, key, value) VALUES (0, NULL, 'probe2', 'v1') "
                     "ON CONFLICT (root_id, COALESCE(unit_id, 0), key) DO UPDATE SET value = EXCLUDED.value")
        server.log_action('PROBE_POSITIVE', 'details-positive')
        conn.commit()

    conn = dbharness.owner_conn()
    row = conn.execute(
        "SELECT action, details, timestamp FROM audit_log WHERE action = 'PROBE_POSITIVE'"
    ).fetchone()
    conn.close()

    assert row is not None, 'a successful change must leave its audit row behind'
    assert row['action'] == 'PROBE_POSITIVE'
    assert row['details'] == 'details-positive'
    assert row['timestamp'], 'audit row must carry a non-empty timestamp'


def test_failed_request_rolls_back_partial_writes():
    """A raised exception must not commit whatever ran before it.

    @app.errorhandler(Exception) turns every raised exception into a normal
    500 response rather than letting it propagate, so teardown_request always
    sees exc=None — even for a request that failed. Under the old
    `if exc is None: commit()` that meant a failing request committed
    everything it had written before it blew up (concretely: delete_person()
    deletes scheduled_events and personnel_profile before personnel, so a
    failure on the last delete used to leave the first two committed behind a
    500 that claimed nothing happened). The fix is that teardown now only
    commits when after_request has marked the response a success
    (status < 400); a raised exception never reaches after_request with such a
    response, so the flag stays unset and teardown rolls back.
    """
    client = server.app.test_client()
    resp = client.get('/__test_probe_500')
    assert resp.status_code == 500, f'expected 500, got {resp.status_code}'

    conn = dbharness.owner_conn()
    row = conn.execute("SELECT value FROM settings WHERE root_id = 0 AND key = 'probe_500'").fetchone()
    conn.close()
    assert row is None, 'a write made before a raised exception must roll back'


def test_4xx_response_rolls_back_partial_writes():
    """An early `return ..., 400` after a partial write must not commit it.

    Same root cause as the 500 case, different trigger: a route that writes,
    then later decides the request is invalid and returns a 4xx, must not
    leave the earlier write committed. (update_settings() writes the
    timezone and unit_name rows before a bad TDY list can 400 out; that write
    must not survive.)
    """
    client = server.app.test_client()
    resp = client.get('/__test_probe_400')
    assert resp.status_code == 400, f'expected 400, got {resp.status_code}'

    conn = dbharness.owner_conn()
    row = conn.execute("SELECT value FROM settings WHERE root_id = 0 AND key = 'probe_400'").fetchone()
    conn.close()
    assert row is None, 'a write behind a 400 response must roll back'


def test_200_response_commits():
    """The guard test: a normal success must still commit.

    Without this, the two tests above would also pass for a teardown that
    never commits at all — rollback-always looks identical to
    rollback-on-failure from the outside unless something also proves the
    success path really persists.
    """
    client = server.app.test_client()
    resp = client.get('/__test_probe_200')
    assert resp.status_code == 200, f'expected 200, got {resp.status_code}'

    conn = dbharness.owner_conn()
    row = conn.execute("SELECT value FROM settings WHERE root_id = 0 AND key = 'probe_200'").fetchone()
    conn.close()
    assert row is not None and row['value'] == 'WROTE', 'a successful request must commit'


def test_failed_audit_write_is_never_a_silent_200():
    """A failed log_action must not cost the user their change in silence.

    Under SQLite log_action had its own connection, so `except Exception: pass`
    isolated the damage. On the shared request connection it does not: the
    failed audit INSERT aborts the whole transaction, the exception is
    swallowed, the route returns 200, after_request sets g.db_commit, and
    _close_db issues COMMIT — which Postgres turns into a silent ROLLBACK on
    an aborted transaction, raising nothing. The soldier is marked present,
    the user is told it worked, and nothing was saved.

    So: the handler still completes (an audit failure must not break a
    request), but the response is a 500, not a 200 — and the write really is
    gone, which is the honest outcome, now reported as such.
    """
    client = server.app.test_client()
    resp = client.get('/__test_probe_audit_fails')

    assert _audit_probe_returned_200, 'log_action must not raise out of the route'
    assert resp.status_code == 500, (
        f'a request whose transaction was aborted must not answer success; got '
        f'{resp.status_code}')

    conn = dbharness.owner_conn()
    row = conn.execute("SELECT value FROM settings WHERE root_id = 0 AND key = 'probe_audit'").fetchone()
    audit = conn.execute(
        "SELECT 1 FROM audit_log WHERE action = 'PROBE_AUDIT'").fetchall()
    conn.close()
    assert row is None, 'the aborted transaction really did discard the write'
    assert audit == [], 'the audit row must not have landed either'

    # ...and the guard must not swallow a deliberate 4xx on the same condition.
    resp = client.get('/__test_probe_409')
    assert resp.status_code == 409, (
        'a route that catches a DB error and answers 409 (update_user, '
        f'sync_clerk_user) must keep that status; got {resp.status_code}')
    assert resp.get_json() == {'error': 'Username already exists'}, resp.get_json()


def test_app_role_cannot_change_schema():
    """The application connects as a non-owner.

    In A1 the RLS policies live on these tables, and a table owner bypasses its
    own policies without error. Proving the app is not the owner now means that
    failure mode cannot appear later.
    """
    app_url = os.environ.get('TEST_APP_DATABASE_URL')
    if not app_url:
        print('  (skipped: TEST_APP_DATABASE_URL not set)')
        return
    with psycopg.connect(app_url, autocommit=True) as conn:
        try:
            conn.execute('CREATE TABLE should_not_exist (id int)')
            assert False, 'the application role must not be able to create tables'
        except psycopg.errors.InsufficientPrivilege:
            pass


def test_app_role_can_resync_a_sequence():
    """The app role must be able to setval(), not just nextval().

    USAGE on a sequence permits nextval() and currval() but NOT setval().
    Every earlier check exercised sequences through an ordinary INSERT, which
    only needs nextval() -- so a grant missing UPDATE passed all of them and
    failed in the browser with "permission denied for sequence
    personnel_id_seq" the first time a backup was restored. The restore route
    and the migration script both call setval() to resync after copying rows
    with explicit ids, so this is the disaster-recovery path.

    The grant is read out of scripts/pg-roles.sql rather than written here, so
    narrowing that script breaks this test instead of production.
    """
    app_url = os.environ.get('TEST_APP_DATABASE_URL')
    if not app_url:
        print('  (skipped: TEST_APP_DATABASE_URL not set)')
        return

    grants = [
        line.strip()
        for line in open(
            os.path.join(os.path.dirname(__file__), '..', 'scripts', 'pg-roles.sql')
        )
        if 'ON ALL SEQUENCES IN SCHEMA public' in line
    ]
    assert grants, 'pg-roles.sql has no ALL SEQUENCES grant to read'

    schema = 'seqgrant_probe'
    owner_url = os.environ['TEST_DATABASE_URL']
    with psycopg.connect(owner_url, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
        conn.execute(f'CREATE SCHEMA {schema}')
        conn.execute(
            f'CREATE TABLE {schema}.probe '
            '(id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v TEXT)'
        )
        conn.execute(f'GRANT USAGE ON SCHEMA {schema} TO platoon_app')
        conn.execute(
            f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} '
            'TO platoon_app'
        )
        for line in grants:
            conn.execute(line.replace('IN SCHEMA public', f'IN SCHEMA {schema}'))

    try:
        with psycopg.connect(app_url, autocommit=True) as conn:
            # nextval, which USAGE alone already allowed -- the check that hid the bug.
            conn.execute(f"INSERT INTO {schema}.probe (v) VALUES ('a')")
            # setval, which needs UPDATE. This is the line that failed in the browser.
            conn.execute(
                f"SELECT setval(pg_get_serial_sequence('{schema}.probe', 'id'), 500)"
            )
            nxt = conn.execute(
                f"INSERT INTO {schema}.probe (v) VALUES ('b') RETURNING id"
            ).fetchone()[0]
        assert nxt == 501, f'sequence did not resync: next id was {nxt}, expected 501'
    finally:
        with psycopg.connect(owner_url, autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')


def main():
    try:
        test_harness_isolates()
        test_init_db_is_idempotent()
        test_settings_scope_key_survives_a_restart()
        test_init_db_failure_surfaces_the_real_error()
        test_audit_row_commits_with_its_change()
        test_audit_row_survives_a_successful_change()
        test_failed_request_rolls_back_partial_writes()
        test_4xx_response_rolls_back_partial_writes()
        test_200_response_commits()
        test_failed_audit_write_is_never_a_silent_200()
        test_app_role_cannot_change_schema()
        test_app_role_can_resync_a_sequence()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

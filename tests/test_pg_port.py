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
    conn.execute("INSERT INTO settings (key, value) VALUES ('probe_500', 'WROTE') "
                 "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    raise RuntimeError('deliberate failure for the probe')


@server.app.route('/__test_probe_400')
def _probe_400():
    conn = server.get_db()
    conn.execute("INSERT INTO settings (key, value) VALUES ('probe_400', 'WROTE') "
                 "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    return server.jsonify({'error': 'deliberate'}), 400


@server.app.route('/__test_probe_200')
def _probe_200():
    conn = server.get_db()
    conn.execute("INSERT INTO settings (key, value) VALUES ('probe_200', 'WROTE') "
                 "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    return server.jsonify({'ok': True}), 200


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


def test_seeding_is_idempotent():
    """init_db() seeds the TDY picklists on every start.

    Under SQLite that was INSERT OR IGNORE. If the ON CONFLICT target is wrong,
    a second start quietly duplicates every picklist row instead of raising.
    """
    schema = dbharness.setup()
    server.init_db()

    conn = server.get_db()
    before = conn.execute('SELECT COUNT(*) AS n FROM settings').fetchone()['n']
    conn.close()

    server.init_db()

    conn = server.get_db()
    after = conn.execute('SELECT COUNT(*) AS n FROM settings').fetchone()['n']
    dupes = conn.execute(
        'SELECT key FROM settings GROUP BY key HAVING COUNT(*) > 1').fetchall()
    conn.close()

    assert after == before, f're-seeding changed the row count: {before} -> {after}'
    assert dupes == [], f'duplicate settings keys after re-seed: {dupes}'

    dbharness.teardown(schema)


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
    schema = dbharness.setup()
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


def test_audit_row_commits_with_its_change():
    """log_action must join the caller's transaction, not open its own.

    Under SQLite it opened a second connection because SQLite has one writer.
    In Postgres that would mean an audit row committing for a change that then
    failed — an audit trail that lies.
    """
    schema = dbharness.setup()
    import server

    with server.app.test_request_context('/'):
        conn = server.get_db()
        assert conn is server.get_db(), 'get_db must return one connection per request'

        conn.execute("INSERT INTO settings (key, value) VALUES ('probe', 'v1') "
                     "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
        server.log_action('PROBE', 'details')
        conn.rollback()

        rows = conn.execute("SELECT 1 FROM audit_log WHERE action = 'PROBE'").fetchall()
        assert rows == [], 'a rolled-back change must roll back its audit row too'

    dbharness.teardown(schema)


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
        conn.execute("INSERT INTO settings (key, value) VALUES ('probe2', 'v1') "
                     "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
        server.log_action('PROBE_POSITIVE', 'details-positive')
        conn.commit()

    conn = server.get_db()
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

    conn = server.get_db()
    row = conn.execute("SELECT value FROM settings WHERE key = 'probe_500'").fetchone()
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

    conn = server.get_db()
    row = conn.execute("SELECT value FROM settings WHERE key = 'probe_400'").fetchone()
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

    conn = server.get_db()
    row = conn.execute("SELECT value FROM settings WHERE key = 'probe_200'").fetchone()
    conn.close()
    assert row is not None and row['value'] == 'WROTE', 'a successful request must commit'


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


def main():
    try:
        test_harness_isolates()
        test_init_db_is_idempotent()
        test_seeding_is_idempotent()
        test_init_db_failure_surfaces_the_real_error()
        test_audit_row_commits_with_its_change()
        test_audit_row_survives_a_successful_change()
        test_failed_request_rolls_back_partial_writes()
        test_4xx_response_rolls_back_partial_writes()
        test_200_response_commits()
        test_app_role_cannot_change_schema()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

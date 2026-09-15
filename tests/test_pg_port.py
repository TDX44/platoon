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


def _migration_env():
    """Env for the migration subprocess, pinned to the module-level schema.

    Several tests above call dbharness.setup() for their own throwaway schema
    and leave os.environ['DATABASE_URL'] pointing at it (already dropped) once
    they finish — harmless for them because `server` cached its own
    DATABASE_URL/MIGRATION_DATABASE_URL at import time and never re-reads the
    environment. A subprocess has no such cache: it reads os.environ fresh, so
    it must be pointed explicitly at the values `server` is actually using.
    """
    env = dict(os.environ)
    env['DATABASE_URL'] = server.DATABASE_URL
    env['MIGRATION_DATABASE_URL'] = server.MIGRATION_DATABASE_URL
    return env


def test_migration_moves_rows_and_resets_sequences():
    """A row count match is the obvious check; the next INSERT is the real one.

    Copying rows with explicit ids leaves Postgres' identity sequences at 1, so
    the first insert after a migration collides with a copied id. That is the
    bug this test exists for.

    Builds a source database meant to look like production's, not a toy one:
    all nine tables, rows in several, a personnel/personnel_profile/
    scheduled_events foreign-key chain that must survive, non-contiguous ids,
    an empty table (duty_roster), a column the Postgres schema has that this
    source table lacks (scheduled_events.location — the kind of thing an
    `ALTER TABLE ... ADD COLUMN` added after this row was written), and a
    legacy table (training_completions) the TABLES allowlist must ignore.

    Runs on the shared module-level schema (per the controller ruling — no
    private dbharness.setup() here), which by this point in the file already
    carries init_db()'s own seed data (a placeholder soldier per platoon, a
    bootstrap admin user) plus committed rows earlier tests in this file left
    behind. So: read the baseline first, pick source ids guaranteed to sit
    above every table's current max (still deliberately non-contiguous
    relative to each other), and assert by looking up the exact rows this
    test inserted rather than comparing whole-table counts against a moving,
    shared baseline.

    Also asserts the migration script cleared init_db()'s placeholder
    personnel row (the fix for the real bug this test caught first: a fresh
    schema seeds a soldier on id 1, which collides with production's own
    lowest real ids and would otherwise silently swallow that soldier).
    """
    import sqlite3
    import subprocess
    import tempfile

    probe = server.get_db()
    placeholder_before = probe.execute(
        "SELECT COUNT(*) AS n FROM personnel WHERE rank = 'WO1' AND last = 'Smith' "
        "AND first = 'John' AND status = 'present'").fetchone()['n']
    duty_roster_before = probe.execute('SELECT COUNT(*) AS n FROM duty_roster').fetchone()['n']
    id_tables = ('personnel', 'users', 'audit_log', 'scheduled_events', 'report_history')
    baseline_max = max(
        probe.execute(f'SELECT COALESCE(MAX(id), 0) AS m FROM {t}').fetchone()['m']
        for t in id_tables)
    probe.close()
    assert placeholder_before > 0, \
        'expected init_db() to have seeded at least one placeholder personnel row by now'
    offset = baseline_max + 1000  # clears every table's current max with room to spare

    src_path = os.path.join(tempfile.mkdtemp(), 'old.db')
    old = sqlite3.connect(src_path)
    old.executescript('''
        CREATE TABLE personnel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rank TEXT, last TEXT, first TEXT, status TEXT, notes TEXT,
            from_date TEXT, to_date TEXT, present_date TEXT, platoon TEXT
        );
        CREATE TABLE personnel_profile (
            person_id INTEGER PRIMARY KEY, phone TEXT, email TEXT
        );
        CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT, password_hash TEXT, is_admin INTEGER, platoons TEXT
        );
        CREATE TABLE audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, user_id INTEGER, username TEXT, action TEXT,
            details TEXT, platoon TEXT
        );
        CREATE TABLE duty_roster (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, platoon TEXT, duty_type TEXT
        );
        CREATE TABLE scheduled_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id INTEGER, platoon TEXT, status TEXT, from_date TEXT,
            to_date TEXT, notes TEXT, created_at TEXT, state TEXT
        );
        CREATE TABLE invites (
            token TEXT PRIMARY KEY, label TEXT, platoons TEXT, is_admin INTEGER
        );
        CREATE TABLE report_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platoon TEXT, unit_name TEXT, text TEXT, created_at TEXT
        );
        CREATE TABLE training_completions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, person_id INTEGER, course TEXT
        );
    ''')

    person_id = offset + 1
    for pid in (person_id, offset + 2, offset + 7, offset + 50):
        old.execute(
            "INSERT INTO personnel (id, rank, last, first, status, notes, "
            "from_date, to_date, present_date, platoon) VALUES "
            "(?, 'SGT', 'Doe', 'John', 'present', '', '', '', '', '2nd')", (pid,))
    old.execute(
        "INSERT INTO personnel_profile (person_id, phone, email) VALUES (?, '555-0100', 'a@b.com')",
        (person_id,))
    old.execute("INSERT INTO settings (key, value) VALUES (?, 'Alpha')", (f'unit_name_test_{offset}',))
    old.execute(
        "INSERT INTO users (id, username, password_hash, is_admin, platoons) "
        "VALUES (?, ?, 'hash', 1, '2nd')", (offset + 1, f'migrated_user_{offset}'))
    old.execute(
        "INSERT INTO audit_log (id, timestamp, user_id, username, action, details, platoon) "
        "VALUES (?, '2026-01-01 00:00:00', 1, 'admin', 'TEST', '', '2nd')", (offset + 1,))
    # No 'location' column here -- the schema-drift case. Non-contiguous ids,
    # both tied to the same personnel row.
    old.execute(
        "INSERT INTO scheduled_events (id, person_id, platoon, status, from_date, "
        "to_date, notes, created_at, state) VALUES "
        "(?, ?, '2nd', 'tdy', '2026-01-01', '2026-01-05', '', '2026-01-01 00:00:00', 'completed')",
        (offset + 3, person_id))
    old.execute(
        "INSERT INTO scheduled_events (id, person_id, platoon, status, from_date, "
        "to_date, notes, created_at, state) VALUES "
        "(?, ?, '2nd', 'leave', '2026-02-01', '2026-02-03', '', '2026-01-01 00:00:00', 'scheduled')",
        (offset + 9, person_id))
    old.execute(
        "INSERT INTO invites (token, label, platoons, is_admin) VALUES (?, 'invite', '2nd', 0)",
        (f'tok_{offset}',))
    old.execute(
        "INSERT INTO report_history (id, platoon, unit_name, text, created_at) VALUES "
        "(?, '2nd', 'Alpha', 'report text', '2026-01-01 00:00:00')", (offset + 4,))
    old.execute("INSERT INTO training_completions (person_id, course) VALUES (?, 'legacy')",
                (person_id,))
    # duty_roster stays empty on purpose.
    old.commit()
    old.close()

    repo = os.path.dirname(_HERE)
    subprocess.run(
        [sys.executable, os.path.join(repo, 'scripts', 'sqlite-to-pg.py'), src_path],
        check=True, cwd=repo, env=_migration_env())

    # Everything from here on must run inside try/finally: closing conn on
    # any exit path -- including an assertion failure -- rolls back its open
    # transaction. Without that, a failing assertion here (e.g. the mutation
    # check below with setval removed) leaves this connection idle-in-
    # transaction, which then hangs main()'s teardown() forever waiting to
    # DROP SCHEMA against a lock this connection is still holding.
    conn = server.get_db()
    try:
        _assert_migration_landed(conn, offset, person_id, duty_roster_before)
    finally:
        conn.close()


def _assert_migration_landed(conn, offset, person_id, duty_roster_before):
    personnel_ids = conn.execute(
        'SELECT id FROM personnel WHERE id IN (%s, %s, %s, %s)',
        (person_id, offset + 2, offset + 7, offset + 50)).fetchall()
    assert {r['id'] for r in personnel_ids} == {person_id, offset + 2, offset + 7, offset + 50}, \
        'expected all 4 migrated personnel rows, non-contiguous ids included'

    placeholder_after = conn.execute(
        "SELECT COUNT(*) AS n FROM personnel WHERE rank = 'WO1' AND last = 'Smith' "
        "AND first = 'John' AND status = 'present'").fetchone()['n']
    assert placeholder_after == 0, \
        'the migration must clear init_db()\'s placeholder personnel row before copying real data'

    settings_row = conn.execute(
        'SELECT value FROM settings WHERE key = %s', (f'unit_name_test_{offset}',)).fetchone()
    assert settings_row is not None and settings_row['value'] == 'Alpha'

    user_row = conn.execute(
        'SELECT username FROM users WHERE id = %s', (offset + 1,)).fetchone()
    assert user_row is not None and user_row['username'] == f'migrated_user_{offset}'

    audit_row = conn.execute(
        'SELECT action FROM audit_log WHERE id = %s', (offset + 1,)).fetchone()
    assert audit_row is not None and audit_row['action'] == 'TEST'

    invite_row = conn.execute(
        'SELECT label FROM invites WHERE token = %s', (f'tok_{offset}',)).fetchone()
    assert invite_row is not None

    report_row = conn.execute(
        'SELECT text FROM report_history WHERE id = %s', (offset + 4,)).fetchone()
    assert report_row is not None and report_row['text'] == 'report text'

    duty_roster_after = conn.execute('SELECT COUNT(*) AS n FROM duty_roster').fetchone()['n']
    assert duty_roster_after == duty_roster_before, \
        'an empty source table must not change the destination row count'

    leftover = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() "
        "AND table_name = 'training_completions'").fetchall()
    assert leftover == [], 'the allowlist must not create or copy a legacy table'

    profile = conn.execute(
        'SELECT person_id FROM personnel_profile WHERE person_id = %s', (person_id,)).fetchone()
    assert profile is not None, 'personnel_profile row for the migrated person is missing'

    events = conn.execute(
        'SELECT id, person_id, location FROM scheduled_events WHERE id IN (%s, %s) ORDER BY id',
        (offset + 3, offset + 9)).fetchall()
    assert [e['id'] for e in events] == [offset + 3, offset + 9]
    assert [e['person_id'] for e in events] == [person_id, person_id], \
        'scheduled_events must still point at the right person'
    assert events[0]['location'] == '', \
        'a column missing from the source row must fall back to the destination default'

    new_person = conn.execute(
        "INSERT INTO personnel (rank, last, first, status, notes, from_date, to_date, "
        "present_date, platoon) VALUES ('PFC','New','Guy','present','','','','','2nd') "
        "RETURNING id").fetchone()
    assert new_person['id'] > offset + 50, \
        f'personnel sequence not reset: next id was {new_person["id"]}'

    new_user = conn.execute(
        "INSERT INTO users (username, password_hash) VALUES (%s, 'h') RETURNING id",
        (f'post_migration_user_{offset}',)).fetchone()
    assert new_user['id'] > offset + 1, f'users sequence not reset: next id was {new_user["id"]}'

    new_audit = conn.execute('INSERT INTO audit_log DEFAULT VALUES RETURNING id').fetchone()
    assert new_audit['id'] > offset + 1, \
        f'audit_log sequence not reset: next id was {new_audit["id"]}'

    new_event = conn.execute(
        "INSERT INTO scheduled_events (person_id, platoon, status) VALUES (%s, '2nd', 'present') "
        "RETURNING id", (person_id,)).fetchone()
    assert new_event['id'] > offset + 9, \
        f'scheduled_events sequence not reset: next id was {new_event["id"]}'

    new_report = conn.execute(
        "INSERT INTO report_history (platoon, text) VALUES ('2nd', 'x') RETURNING id").fetchone()
    assert new_report['id'] > offset + 4, \
        f'report_history sequence not reset: next id was {new_report["id"]}'

    conn.commit()


def test_migration_fails_loudly_on_unknown_source_column():
    """A source column the destination has never heard of must abort, not vanish.

    Production's SQLite file has accumulated ad-hoc columns over months
    (CLAUDE.md). Silently omitting one Postgres doesn't recognize would drop
    that data with no trace; the script must instead name the table and column
    and exit non-zero without writing anything.
    """
    import sqlite3
    import subprocess
    import tempfile

    before = server.get_db()
    before_count = before.execute('SELECT COUNT(*) AS n FROM personnel').fetchone()['n']
    before.close()

    src_path = os.path.join(tempfile.mkdtemp(), 'drift.db')
    old = sqlite3.connect(src_path)
    old.execute(
        "CREATE TABLE personnel (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "rank TEXT, bogus_legacy_column TEXT)")
    old.execute(
        "INSERT INTO personnel (id, rank, bogus_legacy_column) VALUES (999999, 'SGT', 'x')")
    old.commit()
    old.close()

    repo = os.path.dirname(_HERE)
    result = subprocess.run(
        [sys.executable, os.path.join(repo, 'scripts', 'sqlite-to-pg.py'), src_path],
        cwd=repo, env=_migration_env(), capture_output=True, text=True)

    assert result.returncode != 0, 'must exit non-zero on an unrecognized source column'
    combined = result.stdout + result.stderr
    assert 'bogus_legacy_column' in combined, 'the error must name the offending column'

    conn = server.get_db()
    after_count = conn.execute('SELECT COUNT(*) AS n FROM personnel').fetchone()['n']
    conn.close()
    assert after_count == before_count, 'the aborted table must not have partially written rows'


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
        test_migration_moves_rows_and_resets_sequences()
        test_migration_fails_loudly_on_unknown_source_column()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

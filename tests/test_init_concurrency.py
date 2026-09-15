"""Concurrent gunicorn workers must not race init_db()'s DDL.

Postgres's CREATE TABLE IF NOT EXISTS is not safe against concurrent DDL: two
sessions can both check, both find nothing, and one dies with a
UniqueViolation on the system catalog while creating what the other just
created (`gunicorn -w 2` imports server.py, and so calls init_db(), once per
worker). SQLite never showed this — one file, one writer, serialized by the
file lock. This test only proves anything if the workers actually race into
the same empty schema at the same time; a sequential `init_db(); init_db()`
call proves nothing new (test_pg_port.py already covers that).

Run with: python tests/test_init_concurrency.py
"""
import os
import subprocess
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)

import psycopg  # noqa: E402

import dbharness  # noqa: E402

_SCHEMA = dbharness.setup()

N_WORKERS = 8

_WORKER_SRC = f'import sys; sys.path.insert(0, {_ROOT!r}); import server'


def _spawn(env):
    return subprocess.Popen(
        [sys.executable, '-c', _WORKER_SRC],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


def test_concurrent_imports_do_not_race_init_db():
    """Several fresh processes import server (running init_db()) at once."""
    # Same schema for every child -> they really are racing each other, not
    # each working in their own sandbox. Passed through explicitly rather
    # than relying on subprocess's default inherit, since that default is
    # what silently breaks if this test is ever ported to multiprocessing
    # with a spawn start method.
    env = dict(os.environ)

    procs = [_spawn(env) for _ in range(N_WORKERS)]
    results = [(p,) + p.communicate() for p in procs]

    for i, (p, out, err) in enumerate(results):
        assert p.returncode == 0, (
            f'worker {i} exited {p.returncode} (expected 0 -- a race in '
            f'init_db() should be impossible now)\n'
            f'--- stdout ---\n{out.decode(errors="replace")}\n'
            f'--- stderr ---\n{err.decode(errors="replace")}'
        )


def test_schema_is_correct_after_the_race():
    """Every table exists exactly once and no ALTER ran twice."""
    conn = psycopg.connect(os.environ['DATABASE_URL'], row_factory=psycopg.rows.dict_row)

    tables = {r['table_name'] for r in conn.execute(
        'SELECT table_name FROM information_schema.tables '
        'WHERE table_schema = current_schema()').fetchall()}
    for expected in ('personnel', 'personnel_profile', 'settings', 'users',
                      'audit_log', 'duty_roster', 'scheduled_events',
                      'invites', 'report_history'):
        assert expected in tables, f'{expected} table missing after concurrent init_db()'

    dupes = conn.execute(
        'SELECT table_name, column_name, COUNT(*) AS n FROM information_schema.columns '
        'WHERE table_schema = current_schema() GROUP BY table_name, column_name '
        'HAVING COUNT(*) > 1').fetchall()
    conn.close()
    assert dupes == [], f'duplicate columns after concurrent init_db(): {dupes}'


def main():
    try:
        test_concurrent_imports_do_not_race_init_db()
        test_schema_is_correct_after_the_race()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

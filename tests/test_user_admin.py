"""User management (Manage Access) — run with: python tests/test_user_admin.py

Covers the paths a Postgres-port defect hid in for this whole task: no test
reached get_users() or _should_auto_grant_admin() before this file, both of
which query `users.clerk_user_id != ""` — a SQLite compatibility quirk that
Postgres parses as an empty *identifier*, not an empty string, and fails to
parse at all. See task-4-report.md Finding 1.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dbharness  # noqa: E402
_schema = dbharness.setup()

import server  # noqa: E402  (must follow dbharness.setup())

ADMIN = {'is_admin': 1, 'id': 1, 'username': 'boss', 'platoons': '*'}
NON_ADMIN = {'is_admin': 0, 'id': 2, 'username': 'sarge', 'platoons': '2nd'}


def as_user(user):
    server.get_current_user = lambda: user


def insert_user(username, clerk_user_id, email='', is_admin=0):
    conn = server.get_db()
    conn.execute(
        'INSERT INTO users (username, password_hash, is_admin, platoons, clerk_user_id, email) '
        "VALUES (%s, 'x', %s, '2nd', %s, %s)",
        (username, is_admin, clerk_user_id, email)
    )
    conn.commit()
    conn.close()


def check_get_users():
    # A locally-created account (no Clerk identity, clerk_user_id = '') must
    # never show up in Manage Access — it is what _should_auto_grant_admin and
    # get_users both filter on, and the "" vs '' bug broke this filter outright
    # under Postgres (every call 500'd; nothing ever reached the assertions
    # below).
    insert_user('local.only', clerk_user_id='', email='local@example.com')
    insert_user('synced.user', clerk_user_id='clerk_abc123', email='synced@example.com')

    c = server.app.test_client()

    as_user(NON_ADMIN)
    assert c.get('/api/users').status_code == 403, 'only an admin may list users'

    as_user(ADMIN)
    r = c.get('/api/users')
    assert r.status_code == 200, r.get_json()
    rows = r.get_json()
    usernames = {row['username'] for row in rows}
    assert 'synced.user' in usernames, f'a synced Clerk account must be listed: {rows}'
    assert 'local.only' not in usernames, f'a local-only account must not be listed: {rows}'

    synced = next(row for row in rows if row['username'] == 'synced.user')
    assert synced['email'] == 'synced@example.com', synced
    assert 'password_hash' not in synced, 'get_users must not leak password_hash'


def check_should_auto_grant_admin():
    conn = server.get_db()
    try:
        # CLERK_ADMIN_EMAILS set: an exact (case-insensitive) match is
        # granted, everyone else is refused, regardless of what's in `users`.
        server.CLERK_ADMIN_EMAILS = {'boss@example.com'}
        assert server._should_auto_grant_admin(conn, 'Boss@Example.com') is True, \
            'a configured admin email must match case-insensitively'
        assert server._should_auto_grant_admin(conn, 'nobody@example.com') is False, \
            'an unlisted email must never be auto-granted admin'

        # CLERK_ADMIN_EMAILS unset: the first Clerk-synced user ever becomes
        # admin automatically; once one exists, nobody else does.
        server.CLERK_ADMIN_EMAILS = set()
        assert server._should_auto_grant_admin(conn, 'first@example.com') is True, \
            'with no synced users yet and no CLERK_ADMIN_EMAILS, the first sign-in must become admin'

        conn.execute(
            "INSERT INTO users (username, password_hash, is_admin, platoons, clerk_user_id) "
            "VALUES ('already.synced', 'x', 0, '2nd', 'clerk_xyz')"
        )
        conn.commit()
        assert server._should_auto_grant_admin(conn, 'second@example.com') is False, \
            'once any Clerk user has synced, a later one must not be auto-granted admin'
    finally:
        conn.close()


def main():
    # Order matters: this checks the "no synced user exists yet" branch,
    # which check_get_users() below would otherwise have already falsified by
    # inserting a synced user of its own.
    check_should_auto_grant_admin()
    check_get_users()
    print('ok')
    dbharness.teardown(_schema)


if __name__ == '__main__':
    main()

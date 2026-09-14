"""Invite-only sign-up gate — run with: python tests/test_invites.py

Covers the two branches that are easy to break: a token is only usable while
it is unaccepted AND unexpired, and _clean_platoons refuses junk input.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dbharness  # noqa: E402
_schema = dbharness.setup()

import server  # noqa: E402  (must follow the DATA_DIR override)


def insert(conn, token, days, accepted=''):
    conn.execute(
        'INSERT INTO invites (token, platoons, expires_at, accepted_at) '
        "VALUES (%s, '2nd', to_char(now() + %s::interval, 'YYYY-MM-DD HH24:MI:SS'), %s)",
        (token, f'{days:+d} days', accepted)
    )
    conn.commit()


def main():
    conn = server.get_db()
    insert(conn, 'live', 7)
    insert(conn, 'expired', -1)
    insert(conn, 'used', 7, accepted='2026-01-01 00:00:00')

    assert server._valid_invite(conn, 'live') is not None, 'a fresh invite must be usable'
    assert server._valid_invite(conn, 'expired') is None, 'expired invites must be rejected'
    assert server._valid_invite(conn, 'used') is None, 'invites are single use'
    assert server._valid_invite(conn, 'nope') is None, 'unknown tokens must be rejected'
    assert server._valid_invite(conn, '') is None, 'an empty token must never match'
    conn.close()

    assert server._clean_platoons('2nd,1st', False) == '2nd,1st', 'order is preserved'
    assert server._clean_platoons('2nd,2nd', False) == '2nd', 'duplicates collapse'
    assert server._clean_platoons('3rd,evil', False) == '', 'unknown platoons are dropped'
    assert server._clean_platoons('2nd', True) == '*', 'admin invites get every platoon'

    print('ok')
    dbharness.teardown(_schema)


if __name__ == '__main__':
    main()

"""Self-serve signup and invite attachment through sync_clerk_user().

Run with: python tests/test_auth_flow.py

Drives sync_clerk_user() inside a request context with fake Clerk claims, the
way /api/auth/sync does, so the front-door SQL functions are exercised as the
app role with no tenant set — which is the whole point of them.
"""
import os
import sys
from datetime import timedelta

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)

import dbharness  # noqa: E402

_SCHEMA = dbharness.setup()

import server  # noqa: E402
from flask import g  # noqa: E402


def sync(clerk_id, email, invite_token=''):
    with server.app.test_request_context('/api/auth/sync', method='POST'):
        g.auth_claims = {'sub': clerk_id}
        user, err = server.sync_clerk_user({'username': email, 'email': email, 'full_name': 'T',
                                            'invite_token': invite_token})
        if err is None:
            g.db_commit = True
        server._close_db(None)
    return user, err


def stamp_in(days):
    return (server.app_now() + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')


def make_invite(tree, token, role='leader', days=1, unit=None):
    conn = dbharness.owner_conn()
    conn.execute(
        'INSERT INTO invites (token, label, unit_id, role, root_id, created_by, expires_at, created_at) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
        (token, 'PSG', unit or tree['child'], role, tree['root'], 'boss', stamp_in(days), server.app_stamp()))
    conn.commit(); conn.close()


def test_stranger_gets_an_unattached_account():
    user, err = sync('clerk_new_1', 'new1@example.com')
    assert err is None, err
    assert user['unit_id'] is None and user['root_id'] is None, user
    again, err = sync('clerk_new_1', 'new1@example.com')
    assert err is None and again['id'] == user['id'], 'second sign-in finds the same row'


def test_invite_attaches_at_its_unit_with_its_role():
    t = dbharness.make_tree('Hotel Co')
    make_invite(t, 'tok-1')
    user, err = sync('clerk_psg', 'psg@example.com', 'tok-1')
    assert err is None, err
    assert (user['unit_id'], user['role'], user['root_id']) == (t['child'], 'leader', t['root']), user
    conn = dbharness.owner_conn()
    acc = conn.execute("SELECT accepted_by FROM invites WHERE token = 'tok-1'").fetchone()['accepted_by']
    conn.close()
    assert acc == 'clerk_psg'
    user2, err = sync('clerk_late', 'late@example.com', 'tok-1')
    assert err is None and user2['unit_id'] is None, 'a used invite is just a stranger sign-in'


def test_expired_invite_is_ignored():
    t = dbharness.make_tree('Juliet Co')
    make_invite(t, 'tok-old', days=-1)
    user, err = sync('clerk_slow', 'slow@example.com', 'tok-old')
    assert err is None and user['unit_id'] is None


def test_legacy_row_is_claimed_by_email():
    t = dbharness.make_tree('India Co')
    conn = dbharness.owner_conn()
    conn.execute(
        "INSERT INTO users (username, password_hash, clerk_user_id, email, unit_id, role, root_id) "
        "VALUES ('old.hand', 'x', '', 'old@example.com', %s, 'leader', %s)", (t['child'], t['root']))
    conn.commit(); conn.close()
    user, err = sync('clerk_old', 'old@example.com')
    assert err is None and user['unit_id'] == t['child'] and user['clerk_user_id'] == 'clerk_old', user


def main():
    try:
        test_stranger_gets_an_unattached_account()
        test_invite_attaches_at_its_unit_with_its_role()
        test_expired_invite_is_ignored()
        test_legacy_row_is_claimed_by_email()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

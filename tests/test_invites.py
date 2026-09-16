"""Invites on the unit tree — run with: python tests/test_invites.py

An invite names a unit and a role, and it may only name a unit the person
minting it can already reach. The preview is the one unauthenticated door in
the API: the token itself is the secret, and it opens only while the invite is
unaccepted and unexpired.
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

T = dbharness.make_tree()
OWNER = dbharness.make_user(T['root'], 'owner', 'boss')
LEADER = dbharness.make_user(T['child'], 'leader', 'sarge')
OTHER = dbharness.make_tree('Other Co')


def make_invite(tree, token, role='leader', days=1, unit=None, accepted=''):
    conn = dbharness.owner_conn()
    conn.execute(
        'INSERT INTO invites (token, label, unit_id, role, root_id, created_by, expires_at, '
        'created_at, accepted_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
        (token, 'PSG', unit or tree['child'], role, tree['root'], 'boss',
         (server.app_now() + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S'),
         server.app_stamp(), accepted))
    conn.commit()
    conn.close()


def invite_row(token):
    conn = dbharness.owner_conn()
    row = conn.execute('SELECT * FROM invites WHERE token = %s', (token,)).fetchone()
    conn.close()
    return row


def test_a_leader_invites_only_into_their_own_subtree():
    c = server.app.test_client()
    dbharness.as_user(LEADER)
    r = c.post('/api/invites', json={'label': 'New PSG', 'unit_id': T['child'], 'role': 'leader'})
    assert r.status_code == 200, f'a leader may invite into their own unit: {r.get_json()}'
    row = invite_row(r.get_json()['token'])
    assert (row['unit_id'], row['role'], row['root_id']) == (T['child'], 'leader', T['root']), \
        f"the row carries the unit's root: {row}"
    assert c.post('/api/invites', json={'unit_id': T['root']}).status_code == 403, \
        'a unit outside the caller\'s subtree is forbidden'
    assert c.post('/api/invites', json={'unit_id': T['child'], 'role': 'owner'}).status_code == 403, \
        'only an owner may mint an owner invite'
    assert c.post('/api/invites', json={'unit_id': T['child'], 'role': 'general'}).status_code == 400, \
        'an unknown role is rejected outright'
    assert c.post('/api/invites', json={'label': 'Nobody'}).status_code == 403, \
        'an invite with no unit is an invite to nothing'


def test_listing_is_scoped_to_the_root():
    make_invite(T, 'mine')
    make_invite(OTHER, 'theirs')
    c = server.app.test_client()
    dbharness.as_user(OWNER)
    tokens = {i['token'] for i in c.get('/api/invites').get_json()}
    assert 'mine' in tokens, f"the root's own invites are listed: {tokens}"
    assert 'theirs' not in tokens, f"a second tree's invite is absent: {tokens}"


def test_revoking_another_roots_token_is_a_404():
    make_invite(OTHER, 'theirs-2')
    c = server.app.test_client()
    dbharness.as_user(OWNER)
    r = c.delete('/api/invites/theirs-2')
    assert r.status_code == 404, f"another root's token does not exist from here: {r.get_json()}"
    assert invite_row('theirs-2') is not None, 'and it is still there afterwards'


def test_preview_is_public_and_only_for_a_live_token():
    make_invite(T, 'tok-live')
    make_invite(T, 'tok-used', accepted='2026-01-01 00:00:00')
    make_invite(T, 'tok-old', days=-1)
    server.get_current_user = lambda: None
    c = server.app.test_client()
    body = c.get('/api/invites/tok-live/preview').get_json()
    assert body.get('unit_name') == '2nd Platoon' and body.get('role') == 'leader', \
        f'a signed-out invitee is told the unit and role they were invited to: {body}'
    assert c.get('/api/invites/tok-used/preview').status_code == 404, 'an accepted invite is spent'
    assert c.get('/api/invites/tok-old/preview').status_code == 404, 'an expired invite is gone'


def main():
    try:
        test_a_leader_invites_only_into_their_own_subtree()
        test_listing_is_scoped_to_the_root()
        test_revoking_another_roots_token_is_a_404()
        test_preview_is_public_and_only_for_a_live_token()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

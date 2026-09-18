"""Manage Access on the unit tree — run with: python tests/test_user_admin.py

Who a leader may see and change is their own subtree; who an owner may see is
the whole root. A user in a second tree is not a 403 but a 404: row-level
security hides the row, so the route cannot tell it apart from one that was
never there.
"""
import os
import sys

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
OTHER_USER = dbharness.make_user(OTHER['root'], 'owner', 'stranger')


def local_user(unit_id, root_id, username):
    """A pre-Clerk account: a row that exists but has never signed in."""
    conn = dbharness.owner_conn()
    row = conn.execute(
        "INSERT INTO users (username, password_hash, clerk_user_id, email, unit_id, role, root_id) "
        "VALUES (%s, 'x', '', %s, %s, 'leader', %s) RETURNING *",
        (username, f'{username}@example.com', unit_id, root_id)).fetchone()
    conn.commit()
    conn.close()
    return dict(row)


LOCAL = local_user(T['child'], T['root'], 'local.only')


def usernames(client):
    return {u['username'] for u in client.get('/api/users').get_json()}


def test_listing_is_the_callers_subtree():
    c = server.app.test_client()
    dbharness.as_user(LEADER)
    names = usernames(c)
    assert names == {'sarge'}, f'a leader lists only the users inside their own subtree: {names}'
    dbharness.as_user(OWNER)
    names = usernames(c)
    assert names == {'boss', 'sarge'}, f'an owner lists everyone attached in the root: {names}'
    assert 'stranger' not in names, f'a user in a second tree never appears: {names}'


def test_a_local_only_account_is_never_listed():
    c = server.app.test_client()
    dbharness.as_user(OWNER)
    assert 'local.only' not in usernames(c), \
        'an account that has never synced with Clerk must stay out of Manage Access'


def test_moving_and_promoting_a_user():
    top = dbharness.make_user(T['root'], 'leader', 'topkick')
    c = server.app.test_client()

    dbharness.as_user(LEADER)
    r = c.put(f"/api/users/{LEADER['id']}", json={'unit_id': T['root']})
    assert r.status_code == 403, f'a leader cannot move a user to a unit outside their subtree: {r.get_json()}'
    r = c.put(f"/api/users/{LEADER['id']}", json={'role': 'owner'})
    assert r.status_code == 403, f'only an owner may grant owner: {r.get_json()}'

    dbharness.as_user(OWNER)
    r = c.put(f"/api/users/{top['id']}", json={'role': 'owner'})
    assert r.status_code == 200 and r.get_json()['role'] == 'owner', \
        f'an owner may promote a user who is attached at the root: {r.get_json()}'
    r = c.put(f"/api/users/{LEADER['id']}", json={'role': 'owner'})
    assert r.status_code == 400, \
        f'owner is a root role — a user attached below the root cannot hold it: {r.get_json()}'
    r = c.put(f"/api/users/{OTHER_USER['id']}", json={'username': 'x'})
    assert r.status_code == 404, f'a user in a second tree does not exist from here: {r.get_json()}'


def test_an_owners_row_is_owner_only_and_the_root_keeps_one():
    """Two halves of the same brick. Granting owner is owner-only, so anything
    that strips the last owner is a one-way door out of ever administering the
    organization again: a leader must not be able to demote or rename an owner,
    and an owner must not be able to demote or move away the only one left.

    Its own tree, deliberately — the test above promotes a second owner into
    T['root'], and "the last owner" has to mean exactly one.
    """
    tree = dbharness.make_tree('Keepalive Co')
    boss = dbharness.make_user(tree['root'], 'owner', 'keep.boss')
    topkick = dbharness.make_user(tree['root'], 'leader', 'keep.topkick')
    c = server.app.test_client()

    # Rename first: it is the assertion only the role gate can carry, since a
    # rename leaves the owner count untouched and the last-owner rule never
    # fires. Demotion follows, which both rules would refuse.
    dbharness.as_user(topkick)
    r = c.put(f"/api/users/{boss['id']}", json={'username': 'keep.pwned'})
    assert r.status_code == 403, f'a leader may not rename an owner: {r.get_json()}'
    r = c.put(f"/api/users/{boss['id']}", json={'role': 'leader'})
    assert r.status_code == 403, f"a leader may not change an owner's role: {r.get_json()}"

    dbharness.as_user(boss)
    r = c.put(f"/api/users/{boss['id']}", json={'role': 'leader'})
    assert r.status_code == 400, f'the last owner may not demote themselves: {r.get_json()}'
    r = c.put(f"/api/users/{boss['id']}", json={'unit_id': tree['child']})
    assert r.status_code == 400, f'the last owner may not be moved off the root: {r.get_json()}'

    r = c.put(f"/api/users/{topkick['id']}", json={'role': 'owner'})
    assert r.status_code == 200, f'an owner may grant owner at the root: {r.get_json()}'
    r = c.put(f"/api/users/{boss['id']}", json={'role': 'leader'})
    assert r.status_code == 200 and r.get_json()['role'] == 'leader', \
        f'with a second owner in place, an owner may be demoted: {r.get_json()}'


def test_deleting_a_user_is_owner_only():
    victim = dbharness.make_user(T['child'], 'leader', 'victim')
    c = server.app.test_client()
    dbharness.as_user(LEADER)
    assert c.delete(f"/api/users/{victim['id']}").status_code == 403, 'a leader may not delete a user'
    dbharness.as_user(OWNER)
    assert c.delete(f"/api/users/{OWNER['id']}").status_code == 400, 'nobody may delete their own account'
    assert c.delete(f"/api/users/{victim['id']}").status_code == 200, 'an owner may delete a user in the root'
    assert 'victim' not in usernames(c), 'a deleted user is gone from the listing'
    assert c.delete(f"/api/users/{OTHER_USER['id']}").status_code == 404, \
        'deleting nothing is a 404 — a 200 would confirm the id belongs to another tree'


def main():
    try:
        test_listing_is_the_callers_subtree()
        test_a_local_only_account_is_never_listed()
        test_moving_and_promoting_a_user()
        test_an_owners_row_is_owner_only_and_the_root_keeps_one()
        test_deleting_a_user_is_owner_only()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

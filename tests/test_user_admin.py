"""Manage Access on the unit tree — run with: python tests/test_user_admin.py

Every attached account may read the whole organization's account list; what
a leader may change is their own subtree, and an owner's row is owner-only. A user in a second tree is not a 403 but a 404: row-level
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


def test_listing_is_the_whole_org_with_editable_flags():
    """Accounts are readable org-wide; only rows the caller could actually
    change carry editable, and a second tree never appears at all."""
    c = server.app.test_client()
    dbharness.as_user(LEADER)
    rows = c.get('/api/users').get_json()
    names = [u['username'] for u in rows]
    assert set(names) == {'boss', 'sarge'}, f'a leader lists every account in the organization: {names}'
    assert names == ['boss', 'sarge'], f'rows come in unit-tree order, root first: {names}'
    editable = {u['username']: u['editable'] for u in rows}
    assert editable == {'boss': False, 'sarge': True}, \
        f'a leader may edit only inside their subtree, never an owner: {editable}'
    assert 'phone' not in rows[0], 'the listing grew PII it did not carry before'
    # Read-only really is read-only: the server still refuses the write.
    r = c.put(f"/api/users/{OWNER['id']}", json={'username': 'x'})
    assert r.status_code == 404, f'a row outside the subtree is still not editable: {r.get_json()}'
    assert c.delete(f"/api/users/{OWNER['id']}").status_code == 403, 'a leader still may not delete'

    dbharness.as_user(OWNER)
    rows = c.get('/api/users').get_json()
    assert {u['username'] for u in rows} == {'boss', 'sarge'}, rows
    assert all(u['editable'] for u in rows), f'an owner may edit everyone in the root: {rows}'
    assert 'stranger' not in {u['username'] for u in rows}, 'a user in a second tree never appears'


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

    # ...but not moved below the root still holding owner: that is a root
    # role, and a unit_id alone used to carry it down unchanged.
    third = dbharness.make_user(tree['root'], 'owner', 'keep.third')
    r = c.put(f"/api/users/{third['id']}", json={'unit_id': tree['child']})
    assert r.status_code == 400, f'an owner moved below the root must become a leader: {r.get_json()}'
    r = c.put(f"/api/users/{third['id']}", json={'unit_id': tree['child'], 'role': 'leader'})
    assert r.status_code == 200 and (r.get_json()['unit_id'], r.get_json()['role']) == \
        (tree['child'], 'leader'), r.get_json()


def test_a_username_cannot_be_blank():
    c = server.app.test_client()
    dbharness.as_user(OWNER)
    for bad in ('', '   ', None, 5):
        r = c.put(f"/api/users/{LEADER['id']}", json={'username': bad})
        assert r.status_code == 400, (bad, r.status_code)
    assert 'sarge' in usernames(c)


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
        test_listing_is_the_whole_org_with_editable_flags()
        test_a_local_only_account_is_never_listed()
        test_moving_and_promoting_a_user()
        test_an_owners_row_is_owner_only_and_the_root_keeps_one()
        test_deleting_a_user_is_owner_only()
        test_a_username_cannot_be_blank()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

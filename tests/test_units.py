"""Units API: roots, children, slugs, roles, and what an unattached user may do.

Run with: python tests/test_units.py
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)

import dbharness  # noqa: E402

_SCHEMA = dbharness.setup()

import server  # noqa: E402


def _audit(root_id):
    conn = dbharness.owner_conn()
    rows = conn.execute('SELECT action, details, unit_id FROM audit_log WHERE root_id = %s', (root_id,)).fetchall()
    conn.close()
    return rows


def test_unattached_user_can_only_create_a_root():
    u = dbharness.make_user(None)
    dbharness.as_user(u)
    c = server.app.test_client()
    me = c.get('/api/me').get_json()
    assert me['needs_unit'] is True and me['unit_id'] is None, me
    assert c.get('/api/units').get_json() == []
    for path in ('/api/personnel?unit=1', '/api/settings?unit=1', '/api/users', '/api/invites'):
        assert c.get(path).status_code == 403, path
    assert c.post('/api/units', json={'name': 'X', 'kind': 'company', 'parent_id': 1}).status_code == 403
    r = c.post('/api/units', json={'name': 'Charlie Company', 'kind': 'company'})
    assert r.status_code == 201, r.get_json()
    unit = r.get_json()
    assert unit['slug'] == 'charlie-company' and unit['parent_id'] is None
    conn = dbharness.owner_conn()
    row = conn.execute('SELECT unit_id, role, root_id FROM users WHERE id = %s', (u['id'],)).fetchone()
    tz = conn.execute("SELECT value FROM settings WHERE root_id = %s AND unit_id IS NULL AND key = 'org_timezone'",
                      (unit['id'],)).fetchone()
    conn.close()
    assert row == {'unit_id': unit['id'], 'role': 'owner', 'root_id': unit['id']}, row
    assert tz, 'a new root gets its clock'
    dbharness.as_user(dict(u, **row))
    me = c.get('/api/me').get_json()
    assert me['needs_unit'] is False and me['role'] == 'owner'
    assert 'UNIT_CREATE' in {r['action'] for r in _audit(unit['id'])}
    assert c.post('/api/units', json={'name': 'Second', 'kind': 'company'}).status_code == 400, 'one tree per user'


def test_children_slugs_rename_delete():
    t = dbharness.make_tree('Delta Co')
    dbharness.as_user(dbharness.make_user(t['root'], 'owner'))
    c = server.app.test_client()
    r = c.post('/api/units', json={'name': '2nd Platoon', 'kind': 'platoon', 'parent_id': t['root']})
    assert r.status_code == 201, r.get_json()
    assert r.get_json()['slug'] == '2nd-platoon'
    r2 = c.post('/api/units', json={'name': '2nd Platoon', 'kind': 'platoon', 'parent_id': t['root']})
    assert r2.get_json()['slug'] == '2nd-platoon-2', 'slugs are unique per root'
    assert c.post('/api/units', json={'name': 'X', 'kind': 'brigade', 'parent_id': t['root']}).status_code == 400
    assert c.post('/api/units', json={'name': '  ', 'kind': 'squad', 'parent_id': t['root']}).status_code == 400
    plt = r.get_json()['id']
    squad = c.post('/api/units', json={'name': 'Alpha', 'kind': 'squad', 'parent_id': plt}).get_json()
    listing = c.get('/api/units').get_json()
    assert {u['id'] for u in listing} >= {t['root'], t['child'], plt, squad['id']}
    assert c.put(f"/api/units/{squad['id']}", json={'name': 'Alpha Squad'}).status_code == 200
    assert next(u for u in c.get('/api/units').get_json() if u['id'] == squad['id'])['slug'] == 'alpha', 'rename keeps the slug'
    assert c.delete(f'/api/units/{plt}').status_code == 409, 'has a child'
    assert c.delete(f"/api/units/{squad['id']}").status_code == 200
    assert c.delete(f'/api/units/{plt}').status_code == 200
    assert {'UNIT_RENAME', 'UNIT_DELETE'} <= {r['action'] for r in _audit(t['root'])}


def test_leader_is_confined_to_subtree_and_cannot_touch_root():
    t = dbharness.make_tree('Echo Co')
    dbharness.as_user(dbharness.make_user(t['child'], 'leader'))
    c = server.app.test_client()
    ids = {u['id'] for u in c.get('/api/units').get_json()}
    assert ids == {t['child']}, ids
    assert c.post('/api/units', json={'name': 'Sq', 'kind': 'squad', 'parent_id': t['root']}).status_code == 403
    assert c.put(f"/api/units/{t['root']}", json={'name': 'Nope'}).status_code == 403
    assert c.post('/api/units', json={'name': 'Sq', 'kind': 'squad', 'parent_id': t['child']}).status_code == 201
    other = dbharness.make_tree('Foxtrot Co')
    assert c.put(f"/api/units/{other['root']}", json={'name': 'x'}).status_code == 404, 'another root does not exist from here'


def test_root_rename_and_delete_are_owner_only():
    t = dbharness.make_tree('Golf Co')
    dbharness.as_user(dbharness.make_user(t['root'], 'leader'))
    c = server.app.test_client()
    assert c.put(f"/api/units/{t['root']}", json={'name': 'Renamed'}).status_code == 403
    dbharness.as_user(dbharness.make_user(t['root'], 'owner'))
    assert c.put(f"/api/units/{t['root']}", json={'name': 'Renamed'}).status_code == 200
    assert c.delete(f"/api/units/{t['root']}").status_code == 409, 'root with a child cannot go'


def main():
    try:
        test_unattached_user_can_only_create_a_root()
        test_children_slugs_rename_delete()
        test_leader_is_confined_to_subtree_and_cannot_touch_root()
        test_root_rename_and_delete_are_owner_only()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

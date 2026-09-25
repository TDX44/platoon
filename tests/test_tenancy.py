"""Row-level security is the tenant blast door — prove it holds.

Run with: python tests/test_tenancy.py

Connects as platoon_app (the role production uses), creates two roots each
with a soldier, and asserts that with the tenant set to A nothing of B is
visible or writable — even to an UPDATE with no WHERE clause — and that with
no tenant set every protected table is empty. Also proves the boot guard:
server refuses to import when DATABASE_URL is a role that could bypass RLS.
"""
import os
import subprocess
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

import dbharness  # noqa: E402

_SCHEMA = dbharness.setup()

import server  # noqa: E402

PROTECTED = ['units', 'personnel', 'personnel_profile', 'scheduled_events', 'duty_roster',
             'report_history', 'audit_log', 'settings', 'users', 'invites']


def app_conn():
    return psycopg.connect(os.environ['DATABASE_URL'], row_factory=dict_row)


def seed_two_roots():
    a = dbharness.make_tree('Alpha Co')
    b = dbharness.make_tree('Bravo Co')
    conn = dbharness.owner_conn()
    try:
        for tree, last in ((a, 'Alpha'), (b, 'Bravo')):
            conn.execute(
                "INSERT INTO personnel (rank, last, first, unit_id, root_id) VALUES ('SGT', %s, 'X', %s, %s)",
                (last, tree['child'], tree['root']))
        conn.commit()
    finally:
        conn.close()
    return a, b


def test_policies_exist_on_every_protected_table():
    conn = dbharness.owner_conn()
    try:
        for t in PROTECTED:
            row = conn.execute(
                'SELECT c.relrowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace '
                'WHERE c.relname = %s AND n.nspname = current_schema()', (t,)).fetchone()
            assert row and row['relrowsecurity'], f'{t}: RLS not enabled'
            pol = conn.execute(
                "SELECT 1 FROM pg_policies WHERE tablename = %s AND policyname = 'tenant' AND schemaname = current_schema()",
                (t,)).fetchone()
            assert pol, f'{t}: no tenant policy'
    finally:
        conn.close()


def test_tenant_a_cannot_see_or_touch_b():
    a, b = seed_two_roots()
    with app_conn() as conn:
        server.set_tenant(conn, a['root'])
        rows = conn.execute('SELECT last FROM personnel').fetchall()
        assert [r['last'] for r in rows] == ['Alpha'], rows
        touched = conn.execute("UPDATE personnel SET status = 'present'").rowcount
        assert touched == 1, f'an unscoped UPDATE reached {touched} rows'
        try:
            conn.execute(
                "INSERT INTO personnel (rank, last, first, unit_id, root_id) VALUES ('PVT', 'Leak', 'Y', %s, %s)",
                (b['child'], b['root']))
            assert False, 'insert into another root must be rejected by WITH CHECK'
        except psycopg.errors.InsufficientPrivilege:
            pass
        conn.rollback()


def test_no_tenant_means_no_rows():
    seed_two_roots()
    with app_conn() as conn:
        for t in PROTECTED:
            n = conn.execute(f'SELECT count(*) AS n FROM {t}').fetchone()['n']
            assert n == 0, f'{t}: {n} rows visible with no tenant set'


def test_through_the_app_the_other_root_is_a_404():
    a, b = seed_two_roots()
    dbharness.as_user(dbharness.make_user(a['root'], 'owner'))
    c = server.app.test_client()
    conn = dbharness.owner_conn()
    other = conn.execute("SELECT id FROM personnel WHERE last = 'Bravo'").fetchone()['id']
    conn.close()
    assert c.put(f'/api/personnel/{other}', json={'status': 'present'}).status_code == 404


def test_an_unattached_user_declares_no_tenant():
    """`0` is a tenant, not the absence of one.

    A user who has signed in but not yet created or joined a unit has no
    root_id. Parking them on tenant 0 would give every unattached user of every
    organization one shared, writable tenant — and 0 is a value rows really
    hold: auth_create_root_unit inserts root_id = 0 before it knows the new id.
    So the seeded half-built unit below is exactly what such a user must not
    see.
    """
    conn = dbharness.owner_conn()
    conn.execute("INSERT INTO units (parent_id, root_id, kind, name, slug) "
                 "VALUES (NULL, 0, 'company', 'Half-built Co', 'half-built')")
    conn.commit()
    conn.close()

    dbharness.as_user(dbharness.make_user(None))
    with server.app.test_request_context('/'):
        assert server._resolved_user(), 'the unattached user must still be signed in'
        db = server.get_db()
        guc = db.execute("SELECT current_setting('app.root_id', true) AS v").fetchone()['v']
        assert guc in ('', None), f'an unattached user declared tenant {guc!r}'
        n = db.execute('SELECT count(*) AS n FROM units').fetchone()['n']
        assert n == 0, f'an unattached user can see {n} unit(s)'


FRONT_DOOR = [
    'auth_user_by_clerk_id(text)',
    'auth_user_by_identity(text, text)',
    'auth_invite(text, text)',
    'auth_create_user(text, text, text, text, int, text, int)',
    'auth_claim_legacy_user(int, text, text, text, text, int, text, int)',
    'auth_attach_invited_user(int, text, text)',
    'auth_create_root_unit(text, text, text, int)',
    'auth_notify_roots()',
]


def test_front_door_reads_without_a_tenant():
    """The one door through the blast door really opens, and only for its key.

    get_current_user() runs before any tenant is known, so it cannot read
    `users` directly — RLS hides every row. auth_user_by_clerk_id is SECURITY
    DEFINER and therefore can; if that ever silently became SECURITY INVOKER,
    nobody would be able to sign in and no other test would notice.
    """
    tree = dbharness.make_tree('Doorway Co')
    user = dbharness.make_user(tree['root'], 'owner')
    with app_conn() as conn:
        n = conn.execute('SELECT count(*) AS n FROM users').fetchone()['n']
        assert n == 0, f'users must be invisible with no tenant set, saw {n}'
        row = conn.execute('SELECT * FROM auth_user_by_clerk_id(%s)',
                           (user['clerk_user_id'],)).fetchone()
        assert row is not None, \
            'the front door returned nothing — is auth_user_by_clerk_id still SECURITY DEFINER?'
        assert row['id'] == user['id'], row
        assert conn.execute("SELECT * FROM auth_user_by_clerk_id('')").fetchone() is None, \
            'an empty clerk id must never match a row'


def test_front_door_grants():
    """Only platoon_app may knock, and PUBLIC may not.

    A SECURITY DEFINER function is executable by PUBLIC by default, so the
    REVOKE in sql/auth_functions.sql is load-bearing: without it every role in
    the cluster could read any tenant's users and mint accounts. A NULL proacl
    means "defaults still apply", which for a function INCLUDES PUBLIC — hence
    acldefault() rather than trusting an empty ACL.
    """
    conn = dbharness.owner_conn()
    try:
        schema = conn.execute('SELECT current_schema() AS s').fetchone()['s']
        for sig in FRONT_DOOR:
            qualified = f'{schema}.{sig}'
            granted = conn.execute(
                "SELECT has_function_privilege('platoon_app', %s, 'EXECUTE') AS ok",
                (qualified,)).fetchone()['ok']
            assert granted, f'{sig}: platoon_app cannot execute the front door'
            public = conn.execute(
                'SELECT EXISTS (SELECT 1 FROM pg_proc p, '
                "aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) a "
                'WHERE p.oid = %s::regprocedure AND a.grantee = 0) AS public_exec',
                (qualified,)).fetchone()['public_exec']
            assert not public, f'{sig}: still executable by PUBLIC'
    finally:
        conn.close()


def test_boot_guard_refuses_owner_role():
    env = dict(os.environ, DATABASE_URL=os.environ['MIGRATION_DATABASE_URL'])
    proc = subprocess.run([sys.executable, '-c', 'import server'], env=env,
                          capture_output=True, text=True, cwd=os.path.dirname(_HERE))
    assert proc.returncode != 0, 'server imported as a role that owns the tables'
    assert 'bypass row-level security' in proc.stderr, proc.stderr[-600:]


def main():
    try:
        test_policies_exist_on_every_protected_table()
        test_tenant_a_cannot_see_or_touch_b()
        test_no_tenant_means_no_rows()
        test_through_the_app_the_other_root_is_a_404()
        test_an_unattached_user_declares_no_tenant()
        test_front_door_reads_without_a_tenant()
        test_front_door_grants()
        test_boot_guard_refuses_owner_role()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

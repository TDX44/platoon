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
        test_boot_guard_refuses_owner_role()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

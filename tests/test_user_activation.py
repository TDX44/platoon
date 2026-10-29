"""Deactivate/reactivate accounts without deleting assignments or billing.

Real Postgres/RLS, real request auth lookup with only Clerk verification
stubbed. Includes competing owner mutations and migration/backup coverage.
"""
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from types import SimpleNamespace
import psycopg

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
import dbharness

SCHEMA = dbharness.setup()
import server
from flask import request

REAL_VERIFY = server._verify_clerk_session_token
server._verify_clerk_session_token = lambda: ({'sub': request.headers.get('X-Fixture-Identity', '')}, None)


def sql(query, args=()):
    with dbharness.owner_conn() as conn:
        cur = conn.execute(query, args)
        return cur.fetchall() if cur.description else None


def call(user, method, path, data=None):
    return server.app.test_client().open(path, method=method, json=data,
        headers={'X-Fixture-Identity': user['clerk_user_id']})


def change(actor, target, active):
    return call(actor, 'PUT', f"/api/users/{target['id']}", {'is_active': active})


def check_lifecycle_and_sessions():
    tree = dbharness.make_tree('Activation Co')
    owner = dbharness.make_user(tree['root'], 'owner', 'active-owner')
    leader = dbharness.make_user(tree['child'], 'leader', 'active-leader')
    other = dbharness.make_tree('Activation Other Co')
    outsider = dbharness.make_user(other['root'], 'owner', 'outside-owner')
    assert leader['is_active'] is True
    assert change(leader, leader, False).status_code == 403
    assert change(outsider, leader, False).status_code == 404
    assert change(owner, owner, False).status_code == 400
    for bad in ('false', 0, None, []):
        assert change(owner, leader, bad).status_code == 400
    rows = call(owner, 'GET', '/api/users').get_json()
    own = next(r for r in rows if r['id'] == owner['id'])
    target = next(r for r in rows if r['id'] == leader['id'])
    assert own['active_owner_count'] == 1 and not own['can_toggle_active']
    assert target['is_active'] and target['can_toggle_active']

    # Create a real subscription record; activation must leave every column.
    assert call(leader, 'GET', '/api/me').status_code == 200
    before = sql('SELECT * FROM subscriptions WHERE user_id = %s', (leader['id'],))
    response = change(owner, leader, False)
    assert response.status_code == 200 and response.get_json()['is_active'] is False, response.get_json()
    assert sql('SELECT * FROM subscriptions WHERE user_id = %s', (leader['id'],)) == before
    row = sql('SELECT unit_id, role, is_active FROM users WHERE id = %s', (leader['id'],))[0]
    assert row == {'unit_id': tree['child'], 'role': 'leader', 'is_active': False}

    # The same valid Clerk identity is rejected on its very next org request.
    for method, path, body in [
        ('GET', '/api/users', None), ('GET', '/api/units', None),
        ('GET', f"/api/personnel?unit={tree['child']}", None),
        ('GET', '/api/invites', None), ('GET', '/api/me/notifications', None),
        ('POST', '/api/units', {'name': 'Bypass', 'kind': 'company'}),
        ('PUT', f"/api/users/{leader['id']}", {'is_active': True}),
        ('POST', '/api/billing/checkout', {}), ('POST', '/api/billing/extend', {}),
        ('POST', '/api/auth/sync', {'username': 'bypass', 'is_active': True, 'invite_token': 'still-live'}),
    ]:
        response = call(leader, method, path, body)
        assert response.status_code == 403, (method, path, response.status_code, response.get_json())
        assert response.get_json()['code'] == 'account_inactive', response.get_json()
    assert sql('SELECT username, is_active FROM users WHERE id = %s', (leader['id'],))[0] == {
        'username': 'active-leader', 'is_active': False}

    # Own billing remains accessible, including a portal cancellation link.
    response = call(leader, 'GET', '/api/me')
    assert response.status_code == 200 and response.get_json()['is_active'] is False
    assert response.get_json()['platform_admin'] is False
    assert call(leader, 'GET', '/api/billing/details').status_code == 200
    originals = (server.STRIPE_ENABLED, server._stripe_customer_for_mode, server._stripe_portal)
    try:
        server.STRIPE_ENABLED = True
        server._stripe_customer_for_mode = lambda row: 'cus_fixture'
        server._stripe_portal = lambda params: SimpleNamespace(url='https://billing.example.invalid/cancel')
        response = call(leader, 'POST', '/api/billing/portal', {'intent': 'cancel'})
        assert response.status_code == 200, response.get_json()
        assert response.get_json()['url'].endswith('/cancel')
    finally:
        server.STRIPE_ENABLED, server._stripe_customer_for_mode, server._stripe_portal = originals

    # Cached platform-admin approval never grants a deactivated local account.
    verdict = server._platform_admin_verdict
    try:
        server._platform_admin_verdict = lambda sub: True
        response = call(leader, 'GET', '/api/admin/overview')
        assert response.status_code == 403 and response.get_json()['code'] == 'account_inactive'
    finally:
        server._platform_admin_verdict = verdict
    before_reactivate = sql('SELECT * FROM subscriptions WHERE user_id = %s', (leader['id'],))
    assert change(owner, leader, True).status_code == 200
    assert call(leader, 'GET', '/api/users').status_code == 200
    assert sql('SELECT * FROM subscriptions WHERE user_id = %s', (leader['id'],)) == before_reactivate
    audit = sql("SELECT action FROM audit_log WHERE action IN ('ACCOUNT_DEACTIVATE','ACCOUNT_REACTIVATE')")
    assert {a['action'] for a in audit} == {'ACCOUNT_DEACTIVATE', 'ACCOUNT_REACTIVATE'}
    return tree, owner, leader


def check_backup_invites_notifications(tree, owner, leader):
    assert change(owner, leader, False).status_code == 200
    backup = call(owner, 'GET', '/api/backup').get_json()
    saved = next(u for u in backup['users'] if u['username'] == leader['username'])
    assert saved['is_active'] is False
    assert call(owner, 'POST', '/api/backup/restore', backup).status_code == 200
    assert sql('SELECT is_active FROM users WHERE id = %s', (leader['id'],))[0]['is_active'] is False
    # Old v3 files omit activation: existing inactive accounts stay inactive.
    saved.pop('is_active')
    assert call(owner, 'POST', '/api/backup/restore', backup).status_code == 200
    assert sql('SELECT is_active FROM users WHERE id = %s', (leader['id'],))[0]['is_active'] is False

    # A still-live invite cannot reactivate an existing Clerk identity.
    invite = call(owner, 'POST', '/api/invites', {'unit_id': tree['child'], 'label': 'Existing account'})
    token = invite.get_json()['token']
    response = call(leader, 'POST', '/api/auth/sync', {'username': leader['username'], 'invite_token': token})
    assert response.status_code == 403 and response.get_json()['code'] == 'account_inactive'
    assert sql('SELECT accepted_at FROM invites WHERE token = %s', (token,))[0]['accepted_at'] == ''
    sql("INSERT INTO users (username, password_hash, clerk_user_id, unit_id, root_id, is_active) "
        "VALUES ('inactive-legacy', 'retained', '', %s, %s, false)", (tree['child'], tree['root']))
    response = call({'clerk_user_id': 'clerk_legacy_new'}, 'POST', '/api/auth/sync', {
        'username': 'inactive-legacy', 'email': 'legacy@example.invalid', 'invite_token': token})
    assert response.status_code == 403 and response.get_json()['code'] == 'account_inactive'
    assert sql("SELECT clerk_user_id FROM users WHERE username = 'inactive-legacy'")[0]['clerk_user_id'] == ''
    assert sql('SELECT accepted_at FROM invites WHERE token = %s', (token,))[0]['accepted_at'] == ''

    # No organization email is delivered after deactivation; prefs survive.
    sql("INSERT INTO personnel (rank, last, first, unit_id, root_id) VALUES ('SGT', 'Missing', 'Fixture', %s, %s)",
        (tree['child'], tree['root']))
    sql('INSERT INTO notification_prefs (user_id, root_id, accountability_enabled, accountability_time) '
        "VALUES (%s, %s, true, '00:00') ON CONFLICT (user_id) DO UPDATE SET accountability_enabled = true",
        (leader['id'], tree['root']))
    sent = []
    old = (server.email_enabled, server._send_email, server.CRON_SECRET)
    try:
        server.email_enabled = lambda: True
        server._send_email = lambda *args: sent.append(args) or True
        server.CRON_SECRET = 'fixture-activation-cron'
        response = server.app.test_client().post('/api/cron/notify',
            headers={'X-Cron-Secret': 'fixture-activation-cron'})
        assert response.status_code == 200, response.get_json()
        assert sent == []
        assert sql('SELECT accountability_enabled FROM notification_prefs WHERE user_id = %s',
                   (leader['id'],))[0]['accountability_enabled'] is True
    finally:
        server.email_enabled, server._send_email, server.CRON_SECRET = old


def check_owner_races():
    tree = dbharness.make_tree('Owner Race Co')
    a = dbharness.make_user(tree['root'], 'owner', 'race-a')
    b = dbharness.make_user(tree['root'], 'owner', 'race-b')
    # Both arrive with valid owner sessions and attempt to demote each other.
    # One succeeds; the waiter must re-read its own newly demoted role.
    barrier = threading.Barrier(2)
    def demote(actor, target):
        barrier.wait(timeout=5)
        return call(actor, 'PUT', f"/api/users/{target['id']}", {'role': 'leader'})
    with ThreadPoolExecutor(max_workers=2) as executor:
        one = executor.submit(demote, a, b)
        two = executor.submit(demote, b, a)
        results = [one.result(timeout=15), two.result(timeout=15)]
    assert sorted(r.status_code for r in results) == [200, 403], [r.get_json() for r in results]
    active = sql("SELECT * FROM users WHERE root_id = %s AND role = 'owner' AND is_active", (tree['root'],))
    assert len(active) == 1, active
    assert call(dict(active[0]), 'PUT', f"/api/users/{active[0]['id']}", {'role': 'leader'}).status_code == 400
    # Inactive owners do not satisfy the last-active-owner invariant.
    loser = b if active[0]['id'] == a['id'] else a
    sql("UPDATE users SET role = 'owner', is_active = false WHERE id = %s", (loser['id'],))
    assert call(dict(active[0]), 'PUT', f"/api/users/{active[0]['id']}", {'role': 'leader'}).status_code == 400

    # Competing owner deactivations also re-check the waiting actor's status.
    sql("UPDATE users SET role = 'owner', is_active = true WHERE root_id = %s", (tree['root'],))
    barrier = threading.Barrier(2)
    def deactivate(actor, target):
        barrier.wait(timeout=5)
        return change(actor, target, False)
    with ThreadPoolExecutor(max_workers=2) as executor:
        one = executor.submit(deactivate, a, b)
        two = executor.submit(deactivate, b, a)
        results = [one.result(timeout=15), two.result(timeout=15)]
    assert sorted(r.status_code for r in results) == [200, 403], [r.get_json() for r in results]
    assert sql("SELECT count(*) AS n FROM users WHERE root_id = %s AND role = 'owner' AND is_active",
               (tree['root'],))[0]['n'] == 1


def check_upgrade_migration():
    # An actual pre-column table, in a separate disposable schema, exercises
    # upgrade rather than only booting fresh databases with the new column.
    old_schema = SCHEMA + '_upgrade'
    old_url = server.MIGRATION_DATABASE_URL
    try:
        with psycopg.connect(dbharness.admin_url()) as conn:
            conn.execute(f'CREATE SCHEMA "{old_schema}"')
            conn.execute(f'CREATE TABLE "{old_schema}".users (LIKE "{SCHEMA}".users INCLUDING ALL)')
            conn.execute(f'ALTER TABLE "{old_schema}".users DROP COLUMN is_active')
            conn.execute(f'INSERT INTO "{old_schema}".users (username, password_hash, clerk_user_id) '
                         "VALUES ('upgrade-fixture', 'retained', 'clerk_upgrade_fixture')")
        server.MIGRATION_DATABASE_URL = dbharness._with_schema(dbharness.admin_url(), old_schema)
        server.init_db()
        with psycopg.connect(server.MIGRATION_DATABASE_URL, row_factory=psycopg.rows.dict_row) as conn:
            row = conn.execute("SELECT username, password_hash, is_active FROM users WHERE username = 'upgrade-fixture'").fetchone()
            assert row == {'username': 'upgrade-fixture', 'password_hash': 'retained', 'is_active': True}, row
            col = conn.execute("SELECT is_nullable, column_default FROM information_schema.columns "
                               "WHERE table_schema = %s AND table_name = 'users' AND column_name = 'is_active'",
                               (old_schema,)).fetchone()
            assert col['is_nullable'] == 'NO' and col['column_default'] == 'true', col
    finally:
        server.MIGRATION_DATABASE_URL = old_url
        with psycopg.connect(dbharness.admin_url()) as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS "{old_schema}" CASCADE')


def main():
    tree, owner, leader = check_lifecycle_and_sessions()
    check_backup_invites_notifications(tree, owner, leader)
    check_owner_races()
    check_upgrade_migration()
    print('ok')


if __name__ == '__main__':
    try:
        main()
    finally:
        server._verify_clerk_session_token = REAL_VERIFY
        dbharness.teardown(SCHEMA)

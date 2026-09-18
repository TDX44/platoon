"""Stripe billing: the row, the gate, the routes, the webhook.

Run with: python tests/test_billing.py

The app is exercised as platoon_app (RLS binds). Stripe itself is never
called: the five _stripe_* seams in server.py are replaced per test, and the
webhook is fed payloads signed with the real HMAC scheme.
"""
import hashlib
import hmac
import json
import os
import re
import sys
import time
from datetime import timedelta
from types import SimpleNamespace

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)

import psycopg  # noqa: E402

import dbharness  # noqa: E402

_SCHEMA = dbharness.setup()

os.environ['CLERK_PUBLISHABLE_KEY'] = 'pk_test_' + 'a' * 48
os.environ['CLERK_FRONTEND_API_URL'] = 'https://billing-test.clerk.accounts.dev'
os.environ['PLATFORM_ADMIN_EMAILS'] = 'jonathon.carr5@gmail.com'
os.environ['STRIPE_MODE'] = 'test'
os.environ['STRIPE_TEST_SECRET_KEY'] = 'sk_test_' + 'b' * 24
os.environ['STRIPE_TEST_WEBHOOK_SECRET'] = 'whsec_test_' + 'c' * 16
os.environ['BILLING_DEFAULT'] = 'on'

import server  # noqa: E402  (must follow the env overrides above)
import billing_rules  # noqa: E402
from billing_rules import utcnow  # noqa: E402

DAY = timedelta(days=1)
WEBHOOK_SECRET = os.environ['STRIPE_TEST_WEBHOOK_SECRET']

BILLING_FUNCTIONS = [
    'billing_find_by_customer(text)',
    'billing_apply_stripe(text, text, text, text, timestamptz, boolean)',
    'billing_record_event(text)',
    'billing_set_mode(int, text, text)',
]


def test_the_tables_exist_and_subscriptions_is_a_tenant_table():
    conn = dbharness.owner_conn()
    try:
        cols = {r['column_name']: r['data_type'] for r in conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = 'subscriptions'").fetchall()}
        for c in ('user_id', 'root_id', 'billing_mode', 'trial_started_at', 'trial_ends_at', 'extended_at',
                  'stripe_customer_id', 'stripe_subscription_id', 'stripe_status', 'stripe_price_lookup_key',
                  'current_period_end', 'cancel_at_period_end', 'comped_by', 'updated_at'):
            assert c in cols, f'subscriptions.{c} is missing'
        for c in ('trial_started_at', 'trial_ends_at', 'extended_at', 'current_period_end', 'updated_at'):
            assert cols[c] == 'timestamp with time zone', f'{c} must be timestamptz, is {cols[c]}'
        rls = conn.execute("SELECT relrowsecurity FROM pg_class WHERE relname = 'subscriptions' "
                           "AND relnamespace = current_schema()::regnamespace").fetchone()['relrowsecurity']
        assert rls, 'subscriptions has no row-level security'
        policy = conn.execute("SELECT count(*) AS n FROM pg_policies WHERE tablename = 'subscriptions' "
                              "AND policyname = 'tenant' AND schemaname = current_schema()").fetchone()['n']
        assert policy == 1, 'the tenant policy is not on subscriptions'
        assert conn.execute("SELECT to_regclass('stripe_events') IS NOT NULL AS ok").fetchone()['ok']
        # The mode is a CHECK constraint, not a Python-only rule.
        try:
            conn.execute("INSERT INTO subscriptions (user_id, root_id, billing_mode, updated_at) VALUES (999999, 1, 'free', now())")
        except psycopg.errors.CheckViolation:
            conn.rollback()
        except psycopg.errors.ForeignKeyViolation:
            raise AssertionError('billing_mode has no CHECK constraint (the FK fired first)')
        else:
            raise AssertionError('billing_mode accepted a value outside default/comped/billed')
    finally:
        conn.close()


def test_public_cannot_execute_the_billing_functions():
    conn = dbharness.owner_conn()
    try:
        schema = conn.execute('SELECT current_schema() AS s').fetchone()['s']
        for sig in BILLING_FUNCTIONS + ['admin_billing_rows()']:
            qualified = f'{schema}.{sig}'
            assert conn.execute("SELECT has_function_privilege('platoon_app', %s, 'EXECUTE') AS ok",
                                (qualified,)).fetchone()['ok'], f'{sig}: the app cannot call it'
            public = conn.execute(
                'SELECT EXISTS (SELECT 1 FROM pg_proc p, '
                "aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) a "
                'WHERE p.oid = %s::regprocedure AND a.grantee = 0) AS public_exec',
                (qualified,)).fetchone()['public_exec']
            assert not public, f'{sig}: still executable by PUBLIC'
            definer = conn.execute('SELECT prosecdef FROM pg_proc WHERE oid = %s::regprocedure',
                                   (qualified,)).fetchone()['prosecdef']
            assert definer, f'{sig}: not SECURITY DEFINER'
            config = conn.execute('SELECT proconfig FROM pg_proc WHERE oid = %s::regprocedure',
                                  (qualified,)).fetchone()['proconfig'] or []
            assert any(c.startswith('search_path=') for c in config), f'{sig}: no pinned search_path'
    finally:
        conn.close()


def test_stripe_config_is_read_from_the_active_mode():
    assert server.STRIPE_MODE == 'test'
    assert server.STRIPE_ENABLED is True
    assert server.STRIPE_SECRET_KEY == os.environ['STRIPE_TEST_SECRET_KEY']
    assert server.STRIPE_WEBHOOK_SECRET == WEBHOOK_SECRET
    assert server.BILLING_DEFAULT_ON is True
    assert server.PRICE_LOOKUP_KEYS == ('platoon_leader_monthly', 'platoon_leader_annual')
    import stripe
    assert stripe.api_key == server.STRIPE_SECRET_KEY
    assert stripe.max_network_retries == 0, 'two sync gunicorn workers cannot afford retries'


def test_the_pin():
    req = open(os.path.join(_ROOT, 'requirements.txt'), encoding='utf-8').read()
    assert re.search(r'^stripe==\d+\.\d+\.\d+$', req, re.M), 'stripe must be pinned in requirements.txt'
    env = open(os.path.join(_ROOT, '.env.example'), encoding='utf-8').read()
    for name in ('STRIPE_MODE', 'STRIPE_TEST_SECRET_KEY', 'STRIPE_LIVE_SECRET_KEY',
                 'STRIPE_TEST_WEBHOOK_SECRET', 'STRIPE_LIVE_WEBHOOK_SECRET', 'BILLING_DEFAULT'):
        assert f'{name}=' in env, f'{name} is not documented in .env.example'


def seed():
    """Alpha: an owner and a leader. Bravo: one owner. Plus one stray who
    joined nothing."""
    a = dbharness.make_tree('Alpha Co')
    b = dbharness.make_tree('Bravo Co')
    return {'a': a, 'b': b,
            'a_owner': dbharness.make_user(a['root'], 'owner', 'alpha-owner'),
            'a_leader': dbharness.make_user(a['child'], 'leader', 'alpha-leader'),
            'b_owner': dbharness.make_user(b['root'], 'owner', 'bravo-owner'),
            'stray': dbharness.make_user(None, 'leader', 'stray')}


def sub_row(user_id):
    conn = dbharness.owner_conn()
    try:
        r = conn.execute('SELECT * FROM subscriptions WHERE user_id = %s', (user_id,)).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def set_sub(user_id, **cols):
    """Owner-side edit of an account's billing row (a fixture, so it bypasses RLS)."""
    conn = dbharness.owner_conn()
    try:
        sets = ', '.join(f'{k} = %s' for k in cols)
        conn.execute(f'UPDATE subscriptions SET {sets}, updated_at = now() WHERE user_id = %s',
                     (*cols.values(), user_id))
        conn.commit()
    finally:
        conn.close()


def client_as(user):
    dbharness.as_user(user)
    return server.app.test_client()


def test_no_row_before_an_attached_sign_in(fx):
    assert sub_row(fx['a_leader']['id']) is None, 'a row appeared before the account ever signed in'
    c = client_as(fx['stray'])
    me = c.get('/api/me')
    assert me.status_code == 200 and me.get_json()['billing'] is None, me.get_json()
    assert sub_row(fx['stray']['id']) is None, 'an unattached user has no tenant and must get no row'


def test_first_attached_sign_in_starts_the_trial(fx):
    before = utcnow()
    c = client_as(fx['a_leader'])
    me = c.get('/api/me').get_json()
    row = sub_row(fx['a_leader']['id'])
    assert row and row['root_id'] == fx['a']['root'], row
    assert row['trial_started_at'] is not None and before <= row['trial_started_at'] <= utcnow(), row
    assert row['trial_ends_at'] == row['trial_started_at'] + billing_rules.TRIAL_DAYS * DAY, row
    b = me['billing']
    assert b['state'] == 'TRIAL' and b['days_left'] == 14 and b['extension_available'] is True, b
    assert b['subscribed'] is False and b['portal_available'] is False and b['prices'] == [], b
    # A second request does not restart it.
    c.get('/api/me')
    assert sub_row(fx['a_leader']['id'])['trial_started_at'] == row['trial_started_at']


def test_the_trial_does_not_start_while_the_default_is_off(fx):
    server.BILLING_DEFAULT_ON = False
    try:
        c = client_as(fx['b_owner'])
        b = c.get('/api/me').get_json()['billing']
        assert b['state'] == 'COMPED', b
        row = sub_row(fx['b_owner']['id'])
        assert row is not None and row['trial_started_at'] is None, row
    finally:
        server.BILLING_DEFAULT_ON = True
    # Flipped on: the trial starts at the NEXT sign-in, from then, not retroactively.
    b = c.get('/api/me').get_json()['billing']
    assert b['state'] == 'TRIAL' and b['days_left'] == 14, b
    assert sub_row(fx['b_owner']['id'])['trial_started_at'] is not None


def test_sync_carries_billing_too(fx):
    """/api/auth/sync is @clerk_auth_required and declares its own tenant; it
    must create the row and answer with the same payload as /api/me."""
    real = server._verify_clerk_session_token
    server._verify_clerk_session_token = lambda: ({'sub': fx['a_owner']['clerk_user_id']}, None)
    try:
        r = server.app.test_client().post('/api/auth/sync', json={'username': 'alpha-owner', 'email': 'alpha-owner@example.com'})
        assert r.status_code == 200, r.get_json()
        assert r.get_json()['billing']['state'] == 'TRIAL', r.get_json()
        assert sub_row(fx['a_owner']['id']) is not None
    finally:
        server._verify_clerk_session_token = real


def api_rules():
    for rule in server.app.url_map.iter_rules():
        if not rule.rule.startswith('/api/'):
            continue
        methods = [m for m in ('GET', 'POST', 'PUT', 'DELETE') if m in rule.methods]
        path = re.sub(r'<int:[^>]+>', '1', rule.rule)
        path = re.sub(r'<[^>]+>', 'x', path)
        yield rule.rule, path, methods[0]


PUBLIC_API = {'/api/auth/config', '/api/logout', '/api/invites/<token>/preview', '/api/billing/webhook'}


def is_exempt(rule):
    return rule in PUBLIC_API or rule == '/api/me' or rule.startswith(server.BILLING_EXEMPT_PREFIXES)


def test_every_api_route_is_locked_for_a_locked_account(fx):
    leader = fx['a_leader']
    set_sub(leader['id'], trial_ends_at=utcnow() - 10 * DAY)
    c = client_as(leader)
    gated, exempt = [], []
    for rule, path, method in api_rules():
        r = c.open(path, method=method, json={})
        if is_exempt(rule):
            exempt.append(rule)
            assert r.status_code != 402, f'{rule} is on the exempt list but answered 402'
        else:
            gated.append(rule)
            body = r.get_json() or {}
            assert r.status_code == 402 and body.get('error') == 'subscription_required', (rule, r.status_code, body)
            assert body['billing']['state'] == 'LOCKED' and body['billing']['reason'] == 'trial_expired', body
    assert '/api/units' in gated, 'GET /api/units must not be a read carve-out'
    # /api/billing/* is exempt by prefix; the routes themselves arrive in Task 4,
    # so the rule is what is asserted here, not their presence in the sweep.
    assert '/api/me' in exempt and is_exempt('/api/billing/extend'), exempt
    assert len(gated) > 20, gated
    set_sub(leader['id'], trial_ends_at=utcnow() + 10 * DAY)


def test_open_states_are_not_gated(fx):
    leader = fx['a_leader']
    c = client_as(leader)
    for label, cols in (('trial', dict(trial_ends_at=utcnow() + 5 * DAY)),
                        ('grace', dict(trial_ends_at=utcnow() - 1 * DAY)),
                        ('active', dict(stripe_status='active')),
                        ('past_due', dict(stripe_status='past_due')),
                        ('comped', dict(billing_mode='comped', stripe_status='canceled'))):
        set_sub(leader['id'], **cols)
        r = c.get('/api/units')
        assert r.status_code == 200, (label, r.status_code, r.get_json())
        b = c.get('/api/me').get_json()['billing']
        assert b['state'] == {'trial': 'TRIAL', 'grace': 'GRACE', 'active': 'ACTIVE',
                              'past_due': 'PAST_DUE', 'comped': 'COMPED'}[label], (label, b)
    set_sub(leader['id'], billing_mode='default', stripe_status=None, trial_ends_at=utcnow() + 5 * DAY)


def test_rls_hides_another_tenants_row(fx):
    # As Bravo, the app role (RLS binds) can see only Bravo's rows.
    from psycopg.rows import dict_row
    conn = psycopg.connect(os.environ['DATABASE_URL'], row_factory=dict_row)
    try:
        server.set_tenant(conn, fx['b']['root'])
        ids = {r['user_id'] for r in conn.execute('SELECT user_id FROM subscriptions').fetchall()}
        conn.rollback()
    finally:
        conn.close()
    assert fx['a_leader']['id'] not in ids and fx['b_owner']['id'] in ids, ids
    # And with no tenant declared at all, nothing.
    conn = psycopg.connect(os.environ['DATABASE_URL'], row_factory=dict_row)
    try:
        assert conn.execute('SELECT count(*) AS n FROM subscriptions').fetchone()['n'] == 0
    finally:
        conn.close()


def main():
    try:
        test_the_tables_exist_and_subscriptions_is_a_tenant_table()
        test_public_cannot_execute_the_billing_functions()
        test_stripe_config_is_read_from_the_active_mode()
        test_the_pin()
        fx = seed()
        test_no_row_before_an_attached_sign_in(fx)
        test_first_attached_sign_in_starts_the_trial(fx)
        test_the_trial_does_not_start_while_the_default_is_off(fx)
        test_sync_carries_billing_too(fx)
        test_every_api_route_is_locked_for_a_locked_account(fx)
        test_open_states_are_not_gated(fx)
        test_rls_hides_another_tenants_row(fx)
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

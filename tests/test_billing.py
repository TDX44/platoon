"""Stripe billing: the row, the gate, the routes, the webhook.

Run with: python tests/test_billing.py

The app is exercised as platoon_app (RLS binds). Stripe itself is never
called: the _stripe_* seams in server.py are replaced per test, and the
webhook is fed payloads signed with the real HMAC scheme.
"""
import hashlib
import hmac
import io
import logging
import json
import os
import re
import subprocess
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

# Hard guard: nothing may reach the real Stripe API from this file. Every
# test installs its own seams via Stripe().install() before it does anything
# billing-related; anything that slips through before that raises loudly
# instead of silently succeeding (or silently failing) against api.stripe.com.
def _unstubbed(*_a, **_k):
    raise AssertionError('a test reached Stripe without installing the stub')


server._stripe_prices = _unstubbed
server._stripe_customer_create = _unstubbed
server._stripe_checkout = _unstubbed
server._stripe_portal = _unstubbed
server._stripe_cancel = _unstubbed
server._stripe_invoices = _unstubbed
server._stripe_card = _unstubbed

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
    # A key with no webhook secret is a live payment path the app can never
    # reconcile: cards charged, every delivery 503. Both or neither.
    assert not server.STRIPE_ENABLED or server.STRIPE_WEBHOOK_SECRET, \
        'STRIPE_ENABLED with an empty webhook secret'
    env = dict(os.environ, STRIPE_MODE='test', STRIPE_TEST_SECRET_KEY='sk_test_' + 'd' * 24,
               STRIPE_TEST_WEBHOOK_SECRET='')
    out = subprocess.run([sys.executable, '-c', 'import server'], cwd=_ROOT, env=env,
                         capture_output=True, text=True)
    assert out.returncode != 0 and 'STRIPE_TEST_WEBHOOK_SECRET' in out.stderr, \
        f'a secret key with no webhook secret booted: {out.returncode} {out.stderr[-400:]}'


def test_the_pin():
    req = open(os.path.join(_ROOT, 'requirements.txt'), encoding='utf-8').read()
    assert re.search(r'^stripe==\d+\.\d+\.\d+$', req, re.M), 'stripe must be pinned in requirements.txt'
    env = open(os.path.join(_ROOT, '.env.example'), encoding='utf-8').read()
    for name in ('STRIPE_MODE', 'STRIPE_TEST_SECRET_KEY', 'STRIPE_LIVE_SECRET_KEY',
                 'STRIPE_TEST_WEBHOOK_SECRET', 'STRIPE_LIVE_WEBHOOK_SECRET', 'BILLING_DEFAULT'):
        assert f'{name}=' in env, f'{name} is not documented in .env.example'


def price(key, amount, interval, pid=None):
    return SimpleNamespace(id=pid or f'price_{key}', lookup_key=key, unit_amount=amount, currency='usd',
                           recurring=SimpleNamespace(interval=interval))


BOTH_PRICES = [price('platoon_leader_annual', 1999, 'year'), price('platoon_leader_monthly', 299, 'month')]


class Stripe:
    """The seams, recorded. Every _stripe_* call lands in .calls."""

    def __init__(self, prices=None, fail_prices=False, invoices=None, card=None, fail_details=False,
                 refuse_flows=False):
        self.prices, self.fail_prices, self.calls = prices if prices is not None else BOTH_PRICES, fail_prices, []
        self.invoices, self.card, self.fail_details = invoices or [], card, fail_details
        self.refuse_flows = refuse_flows

    def install(self):
        server._PRICES_CACHE = (0.0, [])
        server._BILLING_DETAILS_CACHE.clear()

        def prices():
            self.calls.append(('prices',))
            if self.fail_prices:
                raise OSError('stripe is down')
            return list(self.prices)

        def customer(params, idempotency_key):
            self.calls.append(('customer', params, idempotency_key))
            return SimpleNamespace(id='cus_NEW')

        def checkout(params):
            self.calls.append(('checkout', params))
            return SimpleNamespace(url='https://checkout.stripe.com/c/pay/cs_test_1')

        def portal(params):
            self.calls.append(('portal', params))
            if self.refuse_flows and 'flow_data' in params:
                raise RuntimeError('This flow is not enabled in the portal configuration')
            return SimpleNamespace(url='https://billing.stripe.com/p/session/1')

        def cancel(sub_id):
            self.calls.append(('cancel', sub_id))

        def invoices(customer_id):
            self.calls.append(('invoices', customer_id))
            if self.fail_details:
                raise OSError('stripe is down')
            return list(self.invoices)

        def card(customer_id):
            self.calls.append(('card', customer_id))
            if self.fail_details:
                raise OSError('stripe is down')
            return self.card

        server._stripe_prices, server._stripe_customer_create = prices, customer
        server._stripe_checkout, server._stripe_portal, server._stripe_cancel = checkout, portal, cancel
        server._stripe_invoices, server._stripe_card = invoices, card
        return self


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


def drop_sub(user_id):
    """No billing row at all: what a restore sees for an account it creates."""
    conn = dbharness.owner_conn()
    try:
        conn.execute('DELETE FROM subscriptions WHERE user_id = %s', (user_id,))
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
    assert b['subscribed'] is False and b['portal_available'] is False, b
    # Stripe().install() (installed globally in main(), before seed()) is
    # live from the first request on, so a fresh TRIAL account already sees
    # both real plans -- this is the intended upsell, not an empty stub.
    assert [p['lookup_key'] for p in b['prices']] == ['platoon_leader_monthly', 'platoon_leader_annual'], b['prices']
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
    # extended_at is set too: Task 4 makes /api/billing/extend a real route, and
    # with extension_available it would actually succeed as this sweep walks
    # over it (an exempt route can execute, not just skip the 402), unlocking
    # the account mid-sweep and writing a stray BILLING_EXTEND audit row.
    # Marking the extension already used keeps it a no-op 409 like the other
    # untouched exempt routes.
    set_sub(leader['id'], trial_ends_at=utcnow() - 10 * DAY, extended_at=utcnow() - 20 * DAY)
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


def test_prices_are_cached_and_ordered(fx):
    st = Stripe().install()
    c = client_as(fx['a_leader'])
    p = c.get('/api/me').get_json()['billing']['prices']
    assert [x['lookup_key'] for x in p] == ['platoon_leader_monthly', 'platoon_leader_annual'], p
    assert p[0] == {'lookup_key': 'platoon_leader_monthly', 'amount': 299, 'interval': 'month', 'currency': 'usd'}, p
    assert 'id' not in p[0], 'the Stripe price id stays server-side'
    c.get('/api/me')
    assert st.calls.count(('prices',)) == 1, 'a second /api/me must hit the cache, not Stripe'


def test_prices_empty_when_stripe_is_down_or_incomplete(fx):
    st = Stripe(fail_prices=True).install()
    c = client_as(fx['a_leader'])
    assert c.get('/api/me').get_json()['billing']['prices'] == []
    c.get('/api/me')
    assert st.calls.count(('prices',)) == 1, 'a failure is cached too (two sync workers)'
    # Only one of the two prices found: never show a lone wrong price.
    Stripe(prices=[price('platoon_leader_monthly', 299, 'month')]).install()
    assert c.get('/api/me').get_json()['billing']['prices'] == []
    # Failure TTL is the short one.
    exp, _ = server._PRICES_CACHE
    assert exp - time.monotonic() <= server.PRICES_FAIL_TTL + 1


def test_extend_once_in_trial_and_once_in_grace(fx):
    Stripe().install()
    leader = fx['a_leader']
    ends = utcnow() + 5 * DAY
    set_sub(leader['id'], trial_ends_at=ends, extended_at=None)
    c = client_as(leader)
    r = c.post('/api/billing/extend')
    assert r.status_code == 200, r.get_json()
    b = r.get_json()['billing']
    assert b['state'] == 'TRIAL' and b['days_left'] == 12 and b['extension_available'] is False, b
    row = sub_row(leader['id'])
    assert row['extended_at'] is not None
    assert abs((row['trial_ends_at'] - (ends + 7 * DAY)).total_seconds()) < 2, 'in trial, the end moves by 7 days'
    r = c.post('/api/billing/extend')
    assert r.status_code == 409, 'the extension is once'
    # In grace: the extension runs from now, and the account is back in TRIAL.
    set_sub(leader['id'], trial_ends_at=utcnow() - 1 * DAY, extended_at=None)
    assert c.get('/api/me').get_json()['billing']['state'] == 'GRACE'
    r = c.post('/api/billing/extend')
    assert r.status_code == 200, r.get_json()
    b = r.get_json()['billing']
    assert b['state'] == 'TRIAL' and b['days_left'] == 7, b
    conn = dbharness.owner_conn()
    try:
        n = conn.execute("SELECT count(*) AS n FROM audit_log WHERE action = 'BILLING_EXTEND' AND root_id = %s",
                         (fx['a']['root'],)).fetchone()['n']
    finally:
        conn.close()
    assert n == 2, 'each extension is audited in the tenant'
    # Locked after an extension already used: 409, not a second extension.
    set_sub(leader['id'], trial_ends_at=utcnow() - 10 * DAY)
    assert c.post('/api/billing/extend').status_code == 409
    set_sub(leader['id'], trial_ends_at=utcnow() + 5 * DAY, extended_at=None)


def test_extend_guard_holds_when_the_verdict_is_stale(fx):
    """test_extend_once_in_trial_and_once_in_grace never proves the DB-level
    `AND extended_at IS NULL` guard on its own: every 409 it sees is already
    answered by the view's own extension_available check, computed from a
    fresh read of the row (see task-4-report.md, mutation 2). This test forces
    the gap open by handing the route a verdict that still claims the
    extension is available -- standing in for a request that read the row a
    moment before a concurrent extend committed -- and checks that the UPDATE
    itself, not the view, is what refuses the second extension."""
    leader = fx['a_leader']
    stamp = utcnow() - DAY
    set_sub(leader['id'], trial_ends_at=utcnow() + 5 * DAY, extended_at=stamp)
    before = sub_row(leader['id'])
    real_verdict = server._billing_verdict

    def stale_verdict(row, user):
        return real_verdict({**row, 'extended_at': None}, user)

    server._billing_verdict = stale_verdict
    try:
        c = client_as(leader)
        r = c.post('/api/billing/extend')
    finally:
        server._billing_verdict = real_verdict
    assert r.status_code == 409, r.get_json()
    assert 'already been used' in r.get_json()['error'], r.get_json()
    after = sub_row(leader['id'])
    assert after['extended_at'] == before['extended_at'] == stamp, 'the guard must not touch a row it refuses'
    assert after['trial_ends_at'] == before['trial_ends_at'], 'nor move the trial end it refuses to extend'
    set_sub(leader['id'], trial_ends_at=utcnow() + 5 * DAY, extended_at=None)


def test_checkout_creates_the_customer_once_and_opens_a_session(fx):
    st = Stripe().install()
    leader = fx['a_leader']
    set_sub(leader['id'], stripe_customer_id=None)
    c = client_as(leader)
    r = c.post('/api/billing/checkout', json={'lookup_key': 'platoon_leader_annual'},
               base_url='https://platoondev.carr7.com')
    assert r.status_code == 200 and r.get_json() == {'url': 'https://checkout.stripe.com/c/pay/cs_test_1'}, r.get_json()
    kinds = [x[0] for x in st.calls]
    assert kinds.count('customer') == 1 and kinds.count('checkout') == 1, kinds
    _, cparams, key = next(x for x in st.calls if x[0] == 'customer')
    assert key == f'user:{leader["id"]}:test', key
    assert cparams['email'] == leader['email'] and cparams['metadata']['user_id'] == str(leader['id']), cparams
    assert sub_row(leader['id'])['stripe_customer_id'] == 'test:cus_NEW'
    _, params = next(x for x in st.calls if x[0] == 'checkout')
    assert params['mode'] == 'subscription' and params['customer'] == 'cus_NEW', params
    assert params['line_items'] == [{'price': 'price_platoon_leader_annual', 'quantity': 1}], params
    assert params['client_reference_id'] == str(leader['id']), params
    assert params['metadata'] == {'user_id': str(leader['id']), 'root_id': str(fx['a']['root']), 'mode': 'test'}, params
    assert params['success_url'] == 'https://platoondev.carr7.com/2ndplatoon/settings/billing?billing=success', params
    assert params['cancel_url'] == 'https://platoondev.carr7.com/2ndplatoon/settings/billing', params
    # Second checkout reuses the customer.
    c.post('/api/billing/checkout', json={'lookup_key': 'platoon_leader_monthly'})
    assert [x[0] for x in st.calls].count('customer') == 1, 'the customer is created once per mode'
    # A customer from the other mode is ignored and a new one made.
    set_sub(leader['id'], stripe_customer_id='live:cus_OLD')
    c.post('/api/billing/checkout', json={'lookup_key': 'platoon_leader_monthly'})
    assert [x[0] for x in st.calls].count('customer') == 2
    assert sub_row(leader['id'])['stripe_customer_id'] == 'test:cus_NEW'
    # Unknown plan.
    assert c.post('/api/billing/checkout', json={'lookup_key': 'gold'}).status_code == 400
    conn = dbharness.owner_conn()
    try:
        n = conn.execute("SELECT count(*) AS n FROM audit_log WHERE action = 'BILLING_CHECKOUT_STARTED'").fetchone()['n']
    finally:
        conn.close()
    assert n == 3, n
    # Already subscribed: a stale tab cannot open a second subscription on the
    # same card — the portal is where a live plan changes.
    set_sub(leader['id'], stripe_status='active')
    r = c.post('/api/billing/checkout', json={'lookup_key': 'platoon_leader_monthly'})
    assert r.status_code == 409 and 'portal' in r.get_json()['error'], r.get_json()
    assert [x[0] for x in st.calls].count('checkout') == 3, 'a second Checkout was opened on one card'
    set_sub(leader['id'], stripe_status=None)


def test_portal_needs_a_customer(fx):
    st = Stripe().install()
    leader = fx['a_leader']
    set_sub(leader['id'], stripe_customer_id=None)
    c = client_as(leader)
    assert c.post('/api/billing/portal').status_code == 409
    set_sub(leader['id'], stripe_customer_id='test:cus_NEW')
    r = c.post('/api/billing/portal', base_url='https://platoondev.carr7.com')
    assert r.status_code == 200 and r.get_json()['url'].startswith('https://billing.stripe.com/'), r.get_json()
    _, params = next(x for x in st.calls if x[0] == 'portal')
    assert params == {'customer': 'cus_NEW', 'return_url': 'https://platoondev.carr7.com/2ndplatoon/settings/billing'}, params
    assert c.get('/api/me').get_json()['billing']['portal_available'] is True


def test_billing_routes_work_while_locked(fx):
    Stripe().install()
    leader = fx['a_leader']
    set_sub(leader['id'], trial_ends_at=utcnow() - 10 * DAY, extended_at=utcnow() - 12 * DAY)
    c = client_as(leader)
    assert c.get('/api/units').status_code == 402
    assert c.post('/api/billing/checkout', json={'lookup_key': 'platoon_leader_monthly'}).status_code == 200
    assert c.post('/api/billing/portal').status_code == 200
    assert c.get('/api/billing/details').status_code == 200, 'the Billing page is how a locked account sees its state'
    set_sub(leader['id'], trial_ends_at=utcnow() + 5 * DAY, extended_at=None)


def test_portal_intents_deep_link_and_fall_back(fx):
    """The Billing page's buttons each open one portal flow. The subscription
    id in the flow is always the stored one; a request cannot name another,
    and a flow the portal is not configured for opens the front page."""
    leader = fx['a_leader']
    set_sub(leader['id'], stripe_customer_id='test:cus_NEW', stripe_subscription_id='sub_MINE',
            stripe_status='active', cancel_at_period_end=False)
    c = client_as(leader)

    def flow_for(body, **stripe_kw):
        st = Stripe(**stripe_kw).install()
        r = c.post('/api/billing/portal', json=body)
        assert r.status_code == 200, r.get_json()
        return [p.get('flow_data') for kind, p in (x[:2] for x in st.calls) if kind == 'portal']

    assert flow_for({'intent': 'payment_method'}) == [{'type': 'payment_method_update'}]
    assert flow_for({'intent': 'cancel', 'subscription': 'sub_SOMEONE_ELSE'}) == [
        {'type': 'subscription_cancel', 'subscription_cancel': {'subscription': 'sub_MINE'}}]
    assert flow_for({'intent': 'plan'}) == [
        {'type': 'subscription_update', 'subscription_update': {'subscription': 'sub_MINE'}}]
    assert flow_for({'intent': 'nonsense'}) == [None], 'an unknown intent is the front page'
    assert flow_for({}) == [None]
    # Refused by the portal's configuration: one flow attempt, then the front page.
    assert flow_for({'intent': 'cancel'}, refuse_flows=True) == [
        {'type': 'subscription_cancel', 'subscription_cancel': {'subscription': 'sub_MINE'}}, None]
    # Already cancelling: there is nothing to cancel, so no cancel flow.
    set_sub(leader['id'], cancel_at_period_end=True)
    assert flow_for({'intent': 'cancel'}) == [None]
    # Not subscribed: no subscription flows at all, but a card can still be updated.
    set_sub(leader['id'], stripe_status='canceled', cancel_at_period_end=False)
    assert flow_for({'intent': 'plan'}) == [None]
    set_sub(leader['id'], stripe_status=None, stripe_subscription_id=None)
    assert flow_for({'intent': 'payment_method'}) == [{'type': 'payment_method_update'}]


HOSTILE = '"><img src=x onerror=alert(1)>'


def test_billing_details_reduce_stripe_to_display_fields(fx):
    leader = fx['a_leader']
    c = client_as(leader)
    # No customer yet: the plan and verdict, and nothing asked of Stripe.
    set_sub(leader['id'], stripe_customer_id=None, stripe_status=None, stripe_price_lookup_key=None)
    st = Stripe().install()
    d = c.get('/api/billing/details').get_json()
    assert d['card'] is None and d['invoices'] == [] and d['stripe_error'] is False and d['plan'] is None, d
    assert d['billing']['state'] == 'TRIAL', d['billing']
    assert not [x for x in st.calls if x[0] in ('invoices', 'card')], st.calls

    paid = {'id': 'in_1', 'number': 'A-0001', 'created': 1789000000, 'status': 'paid', 'total': 1999,
            'currency': 'usd', 'hosted_invoice_url': 'https://invoice.stripe.com/i/acct/1',
            'invoice_pdf': 'https://pay.stripe.com/invoice/acct/1/pdf',
            'lines': {'data': [{'period': {'start': 1789000000, 'end': 1820536000}}]},
            'customer_email': 'someone@example.invalid', 'account_name': 'secret'}
    # Attribute access, the way stripe-python >= 12 hands objects over.
    hostile = SimpleNamespace(id=HOSTILE, number=None, created='yesterday', status=HOSTILE, total='1999',
                              currency=HOSTILE, hosted_invoice_url='javascript:alert(1)',
                              invoice_pdf='http://pay.stripe.com/x', lines=None)
    draft = {'id': 'in_d', 'status': 'draft', 'total': 5, 'currency': 'usd', 'created': 1789000000}
    card = SimpleNamespace(brand='visa', last4='4242', exp_month=4, exp_year=2028, fingerprint='fp_secret')
    set_sub(leader['id'], stripe_customer_id='test:cus_NEW', stripe_subscription_id='sub_MINE',
            stripe_status='active', stripe_price_lookup_key='platoon_leader_annual')
    st = Stripe(invoices=[paid, hostile, draft], card=card).install()
    d = c.get('/api/billing/details').get_json()
    assert d['plan'] == {'lookup_key': 'platoon_leader_annual', 'amount': 1999, 'interval': 'year',
                         'currency': 'usd'}, d['plan']
    assert d['card'] == {'brand': 'visa', 'last4': '4242', 'exp_month': 4, 'exp_year': 2028}, d['card']
    assert len(d['invoices']) == 2, 'drafts are not history'
    good, bad = d['invoices']
    assert good == {'id': 'in_1', 'number': 'A-0001', 'created': '2026-09-10T00:26:40+00:00',
                    'period_start': '2026-09-10T00:26:40+00:00', 'period_end': '2027-09-10T00:26:40+00:00',
                    'status': 'paid', 'amount': 1999, 'currency': 'usd',
                    'url': 'https://invoice.stripe.com/i/acct/1',
                    'pdf': 'https://pay.stripe.com/invoice/acct/1/pdf'}, good
    assert bad == {'id': None, 'number': None, 'created': None, 'period_start': None, 'period_end': None,
                   'status': None, 'amount': None, 'currency': None, 'url': None, 'pdf': None}, bad
    assert 'secret' not in json.dumps(d) and 'someone@' not in json.dumps(d), 'a Stripe field leaked through'
    assert ('invoices', 'cus_NEW') in st.calls and ('card', 'cus_NEW') in st.calls

    # Cached per customer: a reload does not ask Stripe again...
    n = len(st.calls)
    c.get('/api/billing/details')
    assert len([x for x in st.calls[n:] if x[0] in ('invoices', 'card')]) == 0, st.calls[n:]
    # ...until the row changes (a webhook landed), which must be seen at once.
    set_sub(leader['id'], cancel_at_period_end=True)
    c.get('/api/billing/details')
    assert len([x for x in st.calls[n:] if x[0] in ('invoices', 'card')]) == 2, st.calls[n:]

    # An unknown brand or a malformed last4 never reaches the page as sent.
    Stripe(card=SimpleNamespace(brand=HOSTILE, last4='42<b>', exp_month='4', exp_year=None)).install()
    assert c.get('/api/billing/details').get_json()['card'] == {
        'brand': 'card', 'last4': None, 'exp_month': None, 'exp_year': None}

    # Stripe down: the page gets an answer that says so, not a 500.
    Stripe(fail_details=True).install()
    r = c.get('/api/billing/details')
    assert r.status_code == 200, r.status_code
    d = r.get_json()
    assert d['stripe_error'] is True and d['card'] is None and d['invoices'] == [], d
    set_sub(leader['id'], stripe_customer_id='test:cus_NEW', stripe_subscription_id=None, stripe_status=None,
            stripe_price_lookup_key=None, cancel_at_period_end=False)
    Stripe().install()


def test_unattached_and_signed_out_cannot_use_billing_routes(fx):
    c = client_as(fx['stray'])
    for path in ('/api/billing/extend', '/api/billing/checkout', '/api/billing/portal'):
        assert c.post(path, json={}).status_code == 403, path
    assert c.get('/api/billing/details').status_code == 403
    dbharness.as_user(None)
    for path in ('/api/billing/extend', '/api/billing/checkout', '/api/billing/portal'):
        assert server.app.test_client().post(path, json={}).status_code == 401, path
    assert server.app.test_client().get('/api/billing/details').status_code == 401


def signed(payload, secret=WEBHOOK_SECRET, ts=None):
    """A body plus the Stripe-Signature header the SDK will accept."""
    body = json.dumps(payload).encode()
    ts = ts or int(time.time())
    mac = hmac.new(secret.encode(), f'{ts}.'.encode() + body, hashlib.sha256).hexdigest()
    return body, {'Stripe-Signature': f't={ts},v1={mac}', 'Content-Type': 'application/json'}


_EVENT_N = [0]


def event(type_, obj, livemode=False, event_id=None):
    _EVENT_N[0] += 1
    return {'id': event_id or f'evt_{_EVENT_N[0]}', 'type': type_, 'livemode': livemode,
            'data': {'object': obj}}


def subscription_obj(customer, sub_id='sub_1', status='active', lookup_key='platoon_leader_monthly',
                     period_end=1_800_000_000, cancel=False):
    return {'id': sub_id, 'object': 'subscription', 'customer': customer, 'status': status,
            'cancel_at_period_end': cancel,
            'items': {'data': [{'price': {'id': 'price_x', 'lookup_key': lookup_key},
                                'current_period_end': period_end}]}}


class quiet:
    """The app logger silenced for a block, level saved and restored whatever
    happens. Some tests below provoke warnings and one deliberate traceback on
    purpose; a passing run should print `ok` and nothing else."""

    def __enter__(self):
        self._level = server.app.logger.level
        server.app.logger.setLevel(logging.CRITICAL)
        return self

    def __exit__(self, *exc):
        server.app.logger.setLevel(self._level)
        return False


def post_webhook(payload, **kw):
    dbharness.as_user(None)
    body, headers = signed(payload, **kw)
    return server.app.test_client().post('/api/billing/webhook', data=body, headers=headers)


def audit_count(action, root_id):
    conn = dbharness.owner_conn()
    try:
        return conn.execute('SELECT count(*) AS n FROM audit_log WHERE action = %s AND root_id = %s',
                            (action, root_id)).fetchone()['n']
    finally:
        conn.close()


def test_webhook_rejects_a_bad_signature_and_a_huge_body(fx):
    body, headers = signed(event('customer.subscription.created', subscription_obj('cus_A')))
    with quiet():  # every rejection below logs a warning on purpose
        r = server.app.test_client().post('/api/billing/webhook', data=body,
                                          headers={**headers, 'Stripe-Signature': 't=1,v1=deadbeef'})
        assert r.status_code == 400, r.get_json()
        r = server.app.test_client().post('/api/billing/webhook', data=body, headers={'Content-Type': 'application/json'})
        assert r.status_code == 400, 'no signature header must be 400, not 500'
        body2, headers2 = signed(event('x', {}), secret='whsec_wrong')
        assert server.app.test_client().post('/api/billing/webhook', data=body2, headers=headers2).status_code == 400
        # Stale timestamp (outside the SDK's 300 s tolerance).
        body3, headers3 = signed(event('x', {}), ts=int(time.time()) - 3600)
        assert server.app.test_client().post('/api/billing/webhook', data=body3, headers=headers3).status_code == 400
    # Too big is refused before the body is read.
    big = b'{' + b' ' * (server.WEBHOOK_MAX_BYTES + 1) + b'}'
    r = server.app.test_client().post('/api/billing/webhook', data=big, headers=headers)
    assert r.status_code == 413, r.status_code
    # ...and with no Content-Length to trust either (chunked), the read itself
    # is bounded.
    r = server.app.test_client().post(
        '/api/billing/webhook', input_stream=io.BytesIO(big),
        headers={'Content-Type': 'application/json', 'Transfer-Encoding': 'chunked'},
        environ_overrides={'wsgi.input_terminated': True, 'CONTENT_LENGTH': ''})
    assert r.status_code == 413, f'a chunked body is not size-capped: {r.status_code}'
    conn = dbharness.owner_conn()
    try:
        assert conn.execute('SELECT count(*) AS n FROM stripe_events').fetchone()['n'] == 0, 'a refused event was recorded'
    finally:
        conn.close()


def test_subscription_events_land_on_the_right_row_and_only_touch_stripe_columns(fx):
    leader, bravo = fx['a_leader'], fx['b_owner']
    set_sub(leader['id'], stripe_customer_id='test:cus_A', trial_ends_at=utcnow() - 10 * DAY, billing_mode='default')
    set_sub(bravo['id'], stripe_customer_id='test:cus_B', trial_ends_at=utcnow() + 5 * DAY)
    before = sub_row(leader['id'])
    r = post_webhook(event('customer.subscription.created', subscription_obj('cus_A', 'sub_A1', 'active')))
    assert r.status_code == 200, r.get_json()
    row = sub_row(leader['id'])
    assert row['stripe_subscription_id'] == 'sub_A1' and row['stripe_status'] == 'active', row
    assert row['stripe_price_lookup_key'] == 'platoon_leader_monthly', row
    assert row['current_period_end'].timestamp() == 1_800_000_000 and row['cancel_at_period_end'] is False, row
    for col in ('billing_mode', 'trial_started_at', 'trial_ends_at', 'extended_at', 'stripe_customer_id'):
        assert row[col] == before[col], f'{col} changed on a Stripe event'
    assert sub_row(bravo['id'])['stripe_status'] is None, "Bravo's row was touched by Alpha's event"
    assert audit_count('BILLING_ACTIVE', fx['a']['root']) == 1
    assert audit_count('BILLING_ACTIVE', fx['b']['root']) == 0
    # The locked account is now open.
    assert client_as(leader).get('/api/units').status_code == 200
    # Cancel at period end: still active, flag mirrored.
    post_webhook(event('customer.subscription.updated', subscription_obj('cus_A', 'sub_A1', 'active', cancel=True)))
    row = sub_row(leader['id'])
    assert row['stripe_status'] == 'active' and row['cancel_at_period_end'] is True, row
    assert client_as(leader).get('/api/me').get_json()['billing']['state'] == 'ACTIVE'
    # A failed invoice that names no subscription is dropped, not applied: a
    # NULL subscription satisfies billing_apply_stripe's guard by
    # construction and past_due is an OPEN state, so a one-off invoice would
    # otherwise hold an account open.
    with quiet():  # the refusal is logged on purpose
        post_webhook(event('invoice.payment_failed', {'id': 'in_0', 'object': 'invoice', 'customer': 'cus_A'}))
    assert sub_row(leader['id'])['stripe_status'] == 'active', \
        'a failed invoice naming no subscription was applied to a live row'
    assert audit_count('BILLING_PAST_DUE', fx['a']['root']) == 0
    # Naming the live subscription, it counts: past_due, open with a warning;
    # other columns kept.
    post_webhook(invoice_failed('cus_A', 'sub_A1', 'in_1'))
    row = sub_row(leader['id'])
    assert row['stripe_status'] == 'past_due' and row['stripe_subscription_id'] == 'sub_A1', row
    assert audit_count('BILLING_PAST_DUE', fx['a']['root']) == 1
    assert client_as(leader).get('/api/units').status_code == 200
    # invoice.paid is audit only.
    post_webhook(event('invoice.paid', {'id': 'in_2', 'object': 'invoice', 'customer': 'cus_A'}))
    assert audit_count('BILLING_PAID', fx['a']['root']) == 1
    assert sub_row(leader['id'])['stripe_status'] == 'past_due'
    # deleted → canceled → locked (trial long over).
    post_webhook(event('customer.subscription.deleted', subscription_obj('cus_A', 'sub_A1', 'canceled')))
    assert sub_row(leader['id'])['stripe_status'] == 'canceled'
    assert audit_count('BILLING_CANCELLED', fx['a']['root']) == 1
    r = client_as(leader).get('/api/units')
    assert r.status_code == 402 and r.get_json()['billing']['reason'] == 'payment_required', r.get_json()


def test_a_late_delete_for_a_superseded_subscription_cannot_lock(fx):
    leader = fx['a_leader']
    post_webhook(event('customer.subscription.created', subscription_obj('cus_A', 'sub_A2', 'active')))
    assert sub_row(leader['id'])['stripe_subscription_id'] == 'sub_A2'
    cancelled = audit_count('BILLING_CANCELLED', fx['a']['root'])
    with quiet():  # the refusal is logged on purpose
        post_webhook(event('customer.subscription.deleted', subscription_obj('cus_A', 'sub_A1', 'canceled')))
    row = sub_row(leader['id'])
    assert row['stripe_status'] == 'active' and row['stripe_subscription_id'] == 'sub_A2', row
    assert audit_count('BILLING_CANCELLED', fx['a']['root']) == cancelled, \
        'the suppressed late delete was audited as a cancellation that never happened'


def invoice_failed(customer, subscription=None, invoice_id='in_f'):
    """An invoice.payment_failed object. Stripe puts the subscription id under
    parent.subscription_details from API 2025-03; omitting it entirely is a
    one-off invoice."""
    obj = {'id': invoice_id, 'object': 'invoice', 'customer': customer}
    if subscription:
        obj['parent'] = {'type': 'subscription_details',
                         'subscription_details': {'subscription': subscription}}
    return event('invoice.payment_failed', obj)


def test_a_failed_invoice_cannot_reopen_a_cancelled_account(fx):
    leader = fx['a_leader']
    post_webhook(event('customer.subscription.deleted', subscription_obj('cus_A', 'sub_A2', 'canceled')))
    assert sub_row(leader['id'])['stripe_status'] == 'canceled'
    locked_audits = audit_count('BILLING_PAST_DUE', fx['a']['root'])
    with quiet():  # both refusals are logged on purpose
        # A late failed invoice for the subscription that was cancelled...
        assert post_webhook(invoice_failed('cus_A', 'sub_A2')).status_code == 200
        assert sub_row(leader['id'])['stripe_status'] == 'canceled', \
            'a failed invoice re-opened a cancelled account'
        # ...and a one-off invoice that names no subscription at all.
        assert post_webhook(invoice_failed('cus_A')).status_code == 200
        assert sub_row(leader['id'])['stripe_status'] == 'canceled', \
            'a subscriptionless failed invoice re-opened a cancelled account'
    assert audit_count('BILLING_PAST_DUE', fx['a']['root']) == locked_audits
    r = client_as(leader).get('/api/units')
    assert r.status_code == 402, r.status_code
    # A new subscription re-opens it, and a failed payment on THAT one counts:
    # it is live, so past_due is the truth and the account stays open.
    post_webhook(event('customer.subscription.created', subscription_obj('cus_A', 'sub_A3', 'active')))
    assert sub_row(leader['id'])['stripe_status'] == 'active'
    # A late failed invoice for the subscription sub_A3 replaced must not
    # touch the live one: naming the subscription is what tells them apart.
    with quiet():
        post_webhook(invoice_failed('cus_A', 'sub_A2', 'in_late'))
    assert sub_row(leader['id'])['stripe_status'] == 'active', \
        'a failed invoice for a superseded subscription hit the live one'
    post_webhook(invoice_failed('cus_A', 'sub_A3'))
    assert sub_row(leader['id'])['stripe_status'] == 'past_due'
    assert client_as(leader).get('/api/units').status_code == 200
    # Hand the next test the row it expects: sub_A2, active.
    post_webhook(event('customer.subscription.created', subscription_obj('cus_A', 'sub_A2', 'active')))


def test_a_new_subscription_cancels_the_one_it_supersedes(fx):
    """One account, one live subscription. Adopting a newer id without
    cancelling the older one leaves the same card carrying two."""
    leader, root = fx['a_leader'], fx['a']['root']
    st = Stripe().install()
    assert sub_row(leader['id'])['stripe_subscription_id'] == 'sub_A2'
    before = audit_count('BILLING_CANCELLED', root)
    post_webhook(event('customer.subscription.created', subscription_obj('cus_A', 'sub_A4', 'active')))
    row = sub_row(leader['id'])
    assert row['stripe_subscription_id'] == 'sub_A4' and row['stripe_status'] == 'active', row
    assert [x for x in st.calls if x[0] == 'cancel'] == [('cancel', 'sub_A2')], st.calls
    assert audit_count('BILLING_CANCELLED', root) == before + 1, 'the cancellation was not audited'
    # Only `created` cancels. billing_apply_stripe adopts any id on an
    # active status, so a retried `updated` for an OLDER subscription,
    # delivered after the new one's `created`, adopts the old id — cancelling
    # on that would kill the subscription just paid for.
    st = Stripe().install()
    post_webhook(event('customer.subscription.updated', subscription_obj('cus_A', 'sub_A2', 'active')))
    assert sub_row(leader['id'])['stripe_subscription_id'] == 'sub_A2'
    assert not any(x[0] == 'cancel' for x in st.calls), \
        'a retried update cancelled the live subscription it superseded'
    st = Stripe().install()
    post_webhook(event('customer.subscription.created', subscription_obj('cus_A', 'sub_A4', 'active')))
    # Stripe refusing the cancel does not undo the adoption.
    def cancel_boom(sub_id):
        raise OSError('stripe down')
    server._stripe_cancel = cancel_boom
    with quiet():  # the failure is logged on purpose
        post_webhook(event('customer.subscription.created', subscription_obj('cus_A', 'sub_A5', 'active')))
    assert sub_row(leader['id'])['stripe_subscription_id'] == 'sub_A5'
    # The same subscription again cancels nothing.
    st = Stripe().install()
    post_webhook(event('customer.subscription.created', subscription_obj('cus_A', 'sub_A5', 'active', cancel=False)))
    assert not any(x[0] == 'cancel' for x in st.calls), st.calls
    # Hand the next test the row it expects: sub_A2, active.
    set_sub(leader['id'], stripe_subscription_id='sub_A2')


def test_replay_other_mode_unknown_customer_and_unknown_type(fx):
    leader = fx['a_leader']
    ev = event('customer.subscription.updated', subscription_obj('cus_A', 'sub_A2', 'past_due'), event_id='evt_replay')
    assert post_webhook(ev).status_code == 200
    assert sub_row(leader['id'])['stripe_status'] == 'past_due'
    set_sub(leader['id'], stripe_status='active')
    r = post_webhook(ev)
    assert r.status_code == 200 and r.get_json().get('replay') is True, r.get_json()
    assert sub_row(leader['id'])['stripe_status'] == 'active', 'a replayed event was applied again'
    # An event from the other mode is acknowledged and ignored.
    r = post_webhook(event('customer.subscription.updated', subscription_obj('cus_A', 'sub_A2', 'canceled'), livemode=True))
    assert r.status_code == 200 and sub_row(leader['id'])['stripe_status'] == 'active'
    # Unknown customer: 200, nothing changes anywhere.
    assert post_webhook(event('customer.subscription.updated', subscription_obj('cus_NOBODY', 'sub_Z', 'canceled'))).status_code == 200
    # checkout.session.completed and anything else: acknowledged, ignored.
    assert post_webhook(event('checkout.session.completed', {'id': 'cs_1', 'customer': 'cus_A'})).status_code == 200
    assert post_webhook(event('charge.refunded', {'id': 'ch_1'})).status_code == 200
    assert sub_row(leader['id'])['stripe_status'] == 'active'


def test_a_failing_handler_is_500_and_the_event_is_not_recorded(fx):
    real = server._handle_stripe_event

    def boom(conn, ev):
        raise RuntimeError('handler died')
    server._handle_stripe_event = boom
    try:
        # The 500 below logs the traceback on purpose; it is not a failure.
        with quiet():
            r = post_webhook(event('customer.subscription.updated', subscription_obj('cus_A'), event_id='evt_boom'))
        assert r.status_code == 500, r.status_code
    finally:
        server._handle_stripe_event = real
    conn = dbharness.owner_conn()
    try:
        assert conn.execute("SELECT count(*) AS n FROM stripe_events WHERE event_id = 'evt_boom'").fetchone()['n'] == 0, \
            'an event whose handler failed was recorded, so the Stripe retry will be treated as a replay'
    finally:
        conn.close()
    # The retry succeeds.
    assert post_webhook(event('customer.subscription.updated', subscription_obj('cus_A'), event_id='evt_boom')).status_code == 200


def test_deleting_a_user_cancels_their_subscription_best_effort(fx):
    st = Stripe().install()
    victim = dbharness.make_user(fx['a']['child'], 'leader', 'alpha-doomed')
    client_as(victim).get('/api/me')   # creates the row
    set_sub(victim['id'], stripe_customer_id='test:cus_D', stripe_subscription_id='sub_D', stripe_status='active')
    owner = client_as(fx['a_owner'])
    r = owner.delete(f'/api/users/{victim["id"]}')
    assert r.status_code == 200, r.get_json()
    assert ('cancel', 'sub_D') in st.calls, st.calls
    assert sub_row(victim['id']) is None, 'the subscriptions row did not cascade'
    # Stripe failing does not stop the delete.
    victim2 = dbharness.make_user(fx['a']['child'], 'leader', 'alpha-doomed-2')
    client_as(victim2).get('/api/me')
    set_sub(victim2['id'], stripe_customer_id='test:cus_E', stripe_subscription_id='sub_E', stripe_status='active')

    def cancel_boom(sub_id):
        raise OSError('stripe down')
    server._stripe_cancel = cancel_boom
    assert client_as(fx['a_owner']).delete(f'/api/users/{victim2["id"]}').status_code == 200
    assert sub_row(victim2['id']) is None
    # No subscription: nothing is called.
    st = Stripe().install()
    victim3 = dbharness.make_user(fx['a']['child'], 'leader', 'alpha-doomed-3')
    assert client_as(fx['a_owner']).delete(f'/api/users/{victim3["id"]}').status_code == 200
    assert not any(x[0] == 'cancel' for x in st.calls)


def test_backup_carries_the_trial_and_never_the_stripe_ids(fx):
    leader = fx['a_leader']
    set_sub(leader['id'], billing_mode='billed', extended_at=utcnow() - DAY, stripe_customer_id='test:cus_A',
            stripe_subscription_id='sub_A2', stripe_status='active')
    row = sub_row(leader['id'])
    dump = client_as(fx['a_owner']).get('/api/backup').get_json()
    u = next(x for x in dump['users'] if x['username'] == 'alpha-leader')
    assert u['billing_mode'] == 'billed' and u['trial_ends_at'] == row['trial_ends_at'].isoformat(), u
    assert u['trial_started_at'] == row['trial_started_at'].isoformat() and u['extended_at'] == row['extended_at'].isoformat(), u
    assert 'stripe' not in json.dumps(dump).lower(), 'a Stripe id or status reached the backup'
    # A leader's export has no users list at all (unchanged rule).
    assert 'users' not in client_as(leader).get('/api/backup').get_json()
    # Restore into the same tree: an account that already has a billing row
    # keeps it exactly. Overwriting it let a hand-edited file re-open a trial
    # that had ended, clear an extension so it could be bought again, or flip
    # a 'billed' account back to 'default'.
    set_sub(leader['id'], trial_ends_at=utcnow() - 2 * DAY)
    before = sub_row(leader['id'])
    edited = json.loads(json.dumps(dump))
    eu = next(x for x in edited['users'] if x['username'] == 'alpha-leader')
    eu.update(billing_mode='default', extended_at=None, trial_ends_at=(utcnow() + 10 * DAY).isoformat())
    r = client_as(fx['a_owner']).post('/api/backup/restore', json=edited)
    assert r.status_code == 200, r.get_json()
    after = sub_row(leader['id'])
    keep = ('billing_mode', 'trial_started_at', 'trial_ends_at', 'extended_at',
            'stripe_subscription_id', 'stripe_status')
    assert {k: after[k] for k in keep} == {k: before[k] for k in keep}, (before, after)
    # A restore that creates the account's row takes the stamps off the file.
    drop_sub(leader['id'])
    assert client_as(fx['a_owner']).post('/api/backup/restore', json=dump).status_code == 200
    after = sub_row(leader['id'])
    assert after['billing_mode'] == 'billed' and after['extended_at'] == row['extended_at'], after
    assert after['stripe_subscription_id'] is None, 'restore must never write a Stripe column'
    # A file without the keys restores as before (no row is invented).
    conn = dbharness.owner_conn()
    try:
        conn.execute('DELETE FROM subscriptions WHERE user_id = %s', (leader['id'],))
        conn.commit()
    finally:
        conn.close()
    for x in dump['users']:
        for k in ('billing_mode', 'trial_started_at', 'trial_ends_at', 'extended_at'):
            x.pop(k, None)
    assert client_as(fx['a_owner']).post('/api/backup/restore', json=dump).status_code == 200
    assert sub_row(leader['id']) is None
    client_as(leader).get('/api/me')   # fresh trial for the rest of the file
    set_sub(leader['id'], billing_mode='default', stripe_customer_id='test:cus_A', stripe_subscription_id='sub_A2',
            stripe_status=None, trial_ends_at=utcnow() + 5 * DAY, extended_at=None)


def test_a_restored_backup_cannot_comp_an_account(fx):
    """/api/backup/restore is only @owner_required, so the file is
    attacker-supplied: an owner must not be able to hand-edit an export into a
    free forever tenant, or into a trial ending in 2030."""
    leader = fx['a_leader']
    dump = client_as(fx['a_owner']).get('/api/backup').get_json()
    u = next(x for x in dump['users'] if x['username'] == 'alpha-leader')
    u['billing_mode'] = 'comped'
    u['trial_started_at'] = (utcnow() + 1999 * DAY).isoformat()
    u['trial_ends_at'] = (utcnow() + 2000 * DAY).isoformat()
    drop_sub(leader['id'])
    assert client_as(fx['a_owner']).post('/api/backup/restore', json=dump).status_code == 200
    row = sub_row(leader['id'])
    assert row['billing_mode'] == 'default', 'a hand-edited backup comped the account'
    ceiling = utcnow() + billing_rules.TRIAL_DAYS * DAY + timedelta(minutes=1)
    assert row['trial_ends_at'] <= ceiling, f'restored trial ends {row["trial_ends_at"]}'
    assert row['trial_started_at'] <= ceiling, f'restored trial starts {row["trial_started_at"]}'
    # End to end: with the dates pushed into the past the account locks. A
    # comp would have kept it open whatever the clock said.
    set_sub(leader['id'], trial_ends_at=utcnow() - 30 * DAY, extended_at=utcnow() - 30 * DAY)
    b = client_as(leader).get('/api/me').get_json()['billing']
    assert b['state'] == 'LOCKED' and b['reason'] == 'trial_expired', b
    assert client_as(leader).get('/api/units').status_code == 402
    set_sub(leader['id'], billing_mode='default', trial_ends_at=utcnow() + 5 * DAY, extended_at=None)


def test_a_restored_row_always_has_a_trial_end(fx):
    """A NULL trial_ends_at is permanent free access: _billing_row's backfill
    only fires on a NULL trial_started_at, so nothing repairs it, and
    billing_state reads a missing end as a trial with TRIAL_DAYS left. A
    missing or unparseable date must therefore land on the ceiling, never on
    NULL."""
    leader = fx['a_leader']
    dump = client_as(fx['a_owner']).get('/api/backup').get_json()
    for bad in ('not-a-date', 'MISSING'):
        u = next(x for x in dump['users'] if x['username'] == 'alpha-leader')
        u['billing_mode'] = 'billed'
        u['trial_started_at'] = (utcnow() - 3 * DAY).isoformat()
        u.pop('extended_at', None)
        if bad == 'MISSING':
            u.pop('trial_ends_at', None)
        else:
            u['trial_ends_at'] = bad
        drop_sub(leader['id'])
        assert client_as(fx['a_owner']).post('/api/backup/restore', json=dump).status_code == 200, bad
        row = sub_row(leader['id'])
        assert row is not None and row['trial_ends_at'] is not None, \
            f'{bad}: the restored row has no trial end, so the account is on a trial that never ends'
        assert row['trial_ends_at'] <= utcnow() + billing_rules.TRIAL_DAYS * DAY + timedelta(minutes=1), row
        # ...and it really does end: dates in the past, account shut.
        set_sub(leader['id'], trial_ends_at=utcnow() - 30 * DAY, extended_at=utcnow() - 30 * DAY)
        assert client_as(leader).get('/api/units').status_code == 402, \
            f'{bad}: the restored account is still open with its trial long over'
    # An extended trial is not shortened by the round trip: its ceiling carries
    # the extension days it already bought.
    u = next(x for x in dump['users'] if x['username'] == 'alpha-leader')
    u['extended_at'] = (utcnow() - DAY).isoformat()
    u['trial_ends_at'] = (utcnow() + billing_rules.TRIAL_DAYS * DAY
                          + billing_rules.EXTENSION_DAYS * DAY - DAY).isoformat()
    drop_sub(leader['id'])
    assert client_as(fx['a_owner']).post('/api/backup/restore', json=dump).status_code == 200
    row = sub_row(leader['id'])
    assert row['trial_ends_at'] > utcnow() + billing_rules.TRIAL_DAYS * DAY, \
        f'an extended trial was clipped back to an unextended one: {row["trial_ends_at"]}'
    assert row['trial_ends_at'] <= (utcnow() + billing_rules.TRIAL_DAYS * DAY
                                    + billing_rules.EXTENSION_DAYS * DAY + timedelta(minutes=1)), row
    set_sub(leader['id'], billing_mode='default', trial_ends_at=utcnow() + 5 * DAY, extended_at=None)


ADMIN_EMAIL = 'jonathon.carr5@gmail.com'
_real_verify = server._verify_clerk_session_token


def admin_client(sub='clerk_boss', email=ADMIN_EMAIL):
    server._clerk_verified_email = lambda cid: email if cid == sub else ''
    server._PLATFORM_ADMIN_CACHE.clear()
    server._verify_clerk_session_token = lambda: ({'sub': sub}, None)
    return server.app.test_client()


def test_admin_overview_counts_and_labels_billing(fx):
    leader, a_owner, b_owner = fx['a_leader'], fx['a_owner'], fx['b_owner']
    set_sub(leader['id'], billing_mode='default', stripe_status=None, trial_ends_at=utcnow() + 5 * DAY)
    set_sub(a_owner['id'], billing_mode='comped')
    set_sub(b_owner['id'], billing_mode='default', stripe_status='active')
    try:
        body = admin_client().get('/api/admin/overview').get_json()
    finally:
        server._verify_clerk_session_token = _real_verify
    t = body['totals']
    assert (t['billing_trial'], t['billing_comped'], t['billing_active']) == (1, 1, 1), t
    assert t['billing_grace'] == 0 and t['billing_locked'] == 0, t
    by_email = {u['email']: u for u in body['recent_users']}
    assert by_email['alpha-leader@example.com']['billing_state'] == 'TRIAL', by_email['alpha-leader@example.com']
    assert by_email['alpha-leader@example.com']['billing_mode'] == 'default'
    assert by_email['alpha-owner@example.com']['billing_state'] == 'COMPED'
    assert by_email['bravo-owner@example.com']['billing_state'] == 'ACTIVE'
    assert by_email['stray@example.com']['billing_state'] is None, 'an unattached user has no billing'
    assert 'stripe' not in json.dumps(body).lower(), 'a Stripe id reached the admin payload'


def test_admin_overview_reports_revenue_from_the_live_prices(fx):
    """MRR is counted from the prices Stripe returns now, never a stored
    amount: an annual subscriber is a twelfth of the annual price per month.
    Only a genuinely subscribed account counts -- a comped account carrying a
    stale lookup key from a cancelled run must not be billed for twice."""
    leader, a_owner, b_owner = fx['a_leader'], fx['a_owner'], fx['b_owner']
    set_sub(leader['id'], billing_mode='default', stripe_status='active',
            stripe_price_lookup_key='platoon_leader_monthly')
    set_sub(b_owner['id'], billing_mode='default', stripe_status='active',
            stripe_price_lookup_key='platoon_leader_annual')
    set_sub(a_owner['id'], billing_mode='comped', stripe_status='canceled',
            stripe_price_lookup_key='platoon_leader_monthly')
    Stripe().install()          # BOTH_PRICES: 299/month and 1999/year
    try:
        body = admin_client().get('/api/admin/overview').get_json()
    finally:
        server._verify_clerk_session_token = _real_verify
        server._PRICES_CACHE = (0.0, [])
    rev = body['revenue']
    assert rev['prices_available'] is True, rev
    plans = {p['lookup_key']: p for p in rev['plans']}
    assert plans['platoon_leader_monthly']['subscribers'] == 1, plans
    assert plans['platoon_leader_annual']['subscribers'] == 1, \
        f'the comped account is being counted as a subscriber: {plans}'
    # 299 + 1999/12 = 465.58... -> 466
    assert rev['mrr_cents'] == round(299 + 1999 / 12), rev
    # ARR is derived from the UNROUNDED monthly figure, so it is what the two
    # accounts actually pay in a year: 299*12 + 1999. Deriving it from the
    # rounded MRR instead would give 5592 -- a rounding artifact, not money.
    assert rev['arr_cents'] == 299 * 12 + 1999, rev
    assert rev['currency'] == 'usd', rev


def test_admin_overview_rolls_billing_up_per_organization(fx):
    leader, a_owner, b_owner = fx['a_leader'], fx['a_owner'], fx['b_owner']
    set_sub(leader['id'], billing_mode='default', stripe_status=None, trial_ends_at=utcnow() + 5 * DAY)
    set_sub(a_owner['id'], billing_mode='comped')
    set_sub(b_owner['id'], billing_mode='default', stripe_status='active')
    try:
        body = admin_client().get('/api/admin/overview').get_json()
    finally:
        server._verify_clerk_session_token = _real_verify
    orgs = {o['org_name']: o['billing'] for o in body['organizations']}
    assert orgs['Alpha Co']['billing_trial'] == 1, orgs
    assert orgs['Alpha Co']['billing_comped'] == 1, orgs
    assert orgs['Alpha Co']['billing_active'] == 0, orgs
    assert orgs['Bravo Co']['billing_active'] == 1, orgs
    assert orgs['Bravo Co']['billing_trial'] == 0, orgs
    assert sum(orgs['Alpha Co'].values()) == 2 and sum(orgs['Bravo Co'].values()) == 1, orgs


def test_admin_overview_watchlist_picks_up_the_four_states(fx):
    leader, a_owner, b_owner = fx['a_leader'], fx['a_owner'], fx['b_owner']
    set_sub(leader['id'], billing_mode='default', stripe_status='past_due')
    set_sub(a_owner['id'], billing_mode='default', stripe_status='active', cancel_at_period_end=True)
    # Trial with two days left: inside the three-day window.
    set_sub(b_owner['id'], billing_mode='default', stripe_status=None, trial_ends_at=utcnow() + 2 * DAY)
    try:
        body = admin_client().get('/api/admin/overview').get_json()
    finally:
        server._verify_clerk_session_token = _real_verify
    w = body['watchlist']
    assert 'alpha-leader@example.com' in w['past_due'], w
    assert 'alpha-owner@example.com' in w['cancelling'], w
    assert 'bravo-owner@example.com' in w['trial_ending'], w
    assert w['locked'] == [], w


def test_admin_comp_toggle(fx):
    leader = fx['a_leader']
    try:
        c = admin_client()
        r = c.put(f'/api/admin/users/{leader["id"]}/billing_mode', json={'mode': 'comped'})
        assert r.status_code == 200 and r.get_json() == {'user_id': leader['id'], 'billing_mode': 'comped'}, r.get_json()
        row = sub_row(leader['id'])
        assert row['billing_mode'] == 'comped' and row['comped_by'] == 'clerk_boss', row
        assert audit_count('BILLING_COMP', fx['a']['root']) == 1
        assert c.put(f'/api/admin/users/{leader["id"]}/billing_mode', json={'mode': 'free'}).status_code == 400
        assert c.put(f'/api/admin/users/{fx["stray"]["id"]}/billing_mode', json={'mode': 'comped'}).status_code == 404
        assert c.put('/api/admin/users/999999/billing_mode', json={'mode': 'comped'}).status_code == 404
        # Back to default.
        assert c.put(f'/api/admin/users/{leader["id"]}/billing_mode', json={'mode': 'default'}).status_code == 200
        assert sub_row(leader['id'])['billing_mode'] == 'default'
        # A non-admin is 404, like every /api/admin/ route.
        other = admin_client(sub='clerk_nobody', email='nobody@example.com')
        assert other.put(f'/api/admin/users/{leader["id"]}/billing_mode', json={'mode': 'comped'}).status_code == 404
        assert sub_row(leader['id'])['billing_mode'] == 'default'
    finally:
        server._verify_clerk_session_token = _real_verify
    # Comped overrides a cancelled Stripe status end to end.
    set_sub(leader['id'], billing_mode='comped', stripe_status='canceled')
    assert client_as(leader).get('/api/units').status_code == 200
    set_sub(leader['id'], billing_mode='default', stripe_status=None)


def test_webhook_functions_are_only_called_from_the_webhook():
    src = open(os.path.join(_ROOT, 'server.py'), encoding='utf-8').read()
    code = re.sub(r'#[^\n]*|"""[\s\S]*?"""', '', src)
    blocks = re.split(r'\n(?=@app\.route|\ndef )', code)
    allowed = {'billing_webhook', '_handle_stripe_event', '_apply_stripe', '_audit_for_customer'}
    for block in blocks:
        if re.search(r'\bbilling_(find_by_customer|apply_stripe|record_event)\s*\(', block):
            m = re.search(r'def (\w+)\(', block)
            assert m and m.group(1) in allowed, f'a webhook-only billing_ function is called outside the webhook:\n{block[:300]}'
        if re.search(r'\bbilling_set_mode\s*\(', block):
            assert '@platform_admin_required' in block, f'billing_set_mode called outside the admin gate:\n{block[:300]}'
    assert any(re.search(r'\bbilling_set_mode\s*\(', b) for b in blocks), 'nothing calls billing_set_mode — renamed?'


def main():
    try:
        test_the_tables_exist_and_subscriptions_is_a_tenant_table()
        test_public_cannot_execute_the_billing_functions()
        test_stripe_config_is_read_from_the_active_mode()
        test_the_pin()
        # Installed before seed(): every subsequent request that touches billing
        # (starting with the first attached sign-in) goes through the stub, never
        # api.stripe.com.
        Stripe().install()
        fx = seed()
        test_no_row_before_an_attached_sign_in(fx)
        test_first_attached_sign_in_starts_the_trial(fx)
        test_the_trial_does_not_start_while_the_default_is_off(fx)
        test_sync_carries_billing_too(fx)
        test_every_api_route_is_locked_for_a_locked_account(fx)
        test_open_states_are_not_gated(fx)
        test_rls_hides_another_tenants_row(fx)
        test_prices_are_cached_and_ordered(fx)
        test_prices_empty_when_stripe_is_down_or_incomplete(fx)
        test_extend_once_in_trial_and_once_in_grace(fx)
        test_extend_guard_holds_when_the_verdict_is_stale(fx)
        test_checkout_creates_the_customer_once_and_opens_a_session(fx)
        test_portal_needs_a_customer(fx)
        test_billing_routes_work_while_locked(fx)
        test_portal_intents_deep_link_and_fall_back(fx)
        test_billing_details_reduce_stripe_to_display_fields(fx)
        test_unattached_and_signed_out_cannot_use_billing_routes(fx)
        test_webhook_rejects_a_bad_signature_and_a_huge_body(fx)
        test_subscription_events_land_on_the_right_row_and_only_touch_stripe_columns(fx)
        test_a_late_delete_for_a_superseded_subscription_cannot_lock(fx)
        test_a_failed_invoice_cannot_reopen_a_cancelled_account(fx)
        test_a_new_subscription_cancels_the_one_it_supersedes(fx)
        test_replay_other_mode_unknown_customer_and_unknown_type(fx)
        test_a_failing_handler_is_500_and_the_event_is_not_recorded(fx)
        test_deleting_a_user_cancels_their_subscription_best_effort(fx)
        test_backup_carries_the_trial_and_never_the_stripe_ids(fx)
        test_a_restored_backup_cannot_comp_an_account(fx)
        test_a_restored_row_always_has_a_trial_end(fx)
        test_admin_overview_counts_and_labels_billing(fx)
        test_admin_overview_reports_revenue_from_the_live_prices(fx)
        test_admin_overview_rolls_billing_up_per_organization(fx)
        test_admin_overview_watchlist_picks_up_the_four_states(fx)
        test_admin_comp_toggle(fx)
        test_webhook_functions_are_only_called_from_the_webhook()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

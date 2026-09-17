"""The platform-operator dashboard: who may open it, and what it may say.

Run with: python tests/test_platform_admin.py

/api/admin/* is the one surface that reads across every tenant, so the gate on
it is the whole feature. The gate is a Clerk-VERIFIED primary email, looked up
by the JWT `sub` and nothing else: the stored `users.email` is client-supplied
(it arrives in the /api/auth/sync body) and so can only ever be a hint. Every
test below is written to fail if that ordering is ever reversed.

The data itself comes from SECURITY DEFINER `admin_*` functions, which the
database will happily run for any signed-in request — so the last two checks
are structural: PUBLIC cannot execute them, and every Python caller sits behind
the decorator.
"""
import json
import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

import dbharness  # noqa: E402

_SCHEMA = dbharness.setup()

# Production always runs with Clerk configured; the token verification itself
# is stubbed per test, so no network is ever touched.
os.environ['CLERK_PUBLISHABLE_KEY'] = 'pk_test_' + 'a' * 48
os.environ['CLERK_FRONTEND_API_URL'] = 'https://admin-test.clerk.accounts.dev'
os.environ['PLATFORM_ADMIN_EMAILS'] = 'jonathon.carr5@gmail.com'

import server  # noqa: E402  (must follow the env overrides above)

ADMIN_EMAIL = 'jonathon.carr5@gmail.com'
OVERVIEW = '/api/admin/overview'

_real_verify = server._verify_clerk_session_token
# install() replaces the lookup with a stub; this is the real one, which the
# test of Clerk's own answer calls directly.
_real_lookup = server._clerk_verified_email


class Clerk:
    """The one seam. Records every lookup so "did this cost a Clerk call?" is
    an assertion rather than a guess."""

    def __init__(self, answers=None, raises=False):
        self.answers = answers or {}
        self.raises = raises
        self.calls = []

    def __call__(self, clerk_user_id):
        self.calls.append(clerk_user_id)
        if self.raises:
            raise OSError('clerk is down')
        return self.answers.get(clerk_user_id, '')


def install(clerk, session_sub=None):
    """Point the app at a stubbed Clerk, and optionally at a valid session."""
    server._clerk_verified_email = clerk
    server._PLATFORM_ADMIN_CACHE.clear()
    if session_sub is None:
        server._verify_clerk_session_token = _real_verify
    else:
        server._verify_clerk_session_token = lambda: ({'sub': session_sub}, None)
    return server.app.test_client()


# ── Fixture: two tenants, with exactly countable contents ──

SECRET_INVITE_TOKEN = 'pending-token-do-not-leak'
SECRET_AUDIT_DETAIL = 'audit detail that must not leak'


def seed_two_tenants():
    """Alpha: 2 units, 3 soldiers, 2 attached users (1 owner), a logo, and one
    invite of each kind. Bravo: 2 units, 1 soldier, 1 user, nothing else.
    Plus two people who have signed in and joined nothing."""
    a = dbharness.make_tree('Alpha Co')
    b = dbharness.make_tree('Bravo Co')
    a_owner = dbharness.make_user(a['root'], 'owner', 'alpha-owner')
    dbharness.make_user(a['child'], 'leader', 'alpha-leader')
    dbharness.make_user(b['root'], 'owner', 'bravo-owner')
    stray = [dbharness.make_user(None, 'leader', 'stray-1'),
             dbharness.make_user(None, 'leader', 'stray-2')]
    conn = dbharness.owner_conn()
    try:
        for last in ('Alpha', 'Anderson', 'Ashby'):
            conn.execute("INSERT INTO personnel (rank, last, first, unit_id, root_id) "
                         "VALUES ('SGT', %s, 'X', %s, %s)", (last, a['child'], a['root']))
        conn.execute("INSERT INTO personnel (rank, last, first, unit_id, root_id) "
                     "VALUES ('SGT', 'Bravo', 'Y', %s, %s)", (b['child'], b['root']))
        conn.execute("INSERT INTO settings (root_id, unit_id, key, value) "
                     "VALUES (%s, %s, 'logo', %s)", (a['root'], a['root'], 'data:image/png;base64,AAAA'))
        for token, expires, accepted in (
                (SECRET_INVITE_TOKEN, '2999-01-01 00:00:00', ''),
                ('expired-token-zzz', '2001-01-01 00:00:00', ''),
                ('accepted-token-zz', '2999-01-01 00:00:00', '2020-01-01 00:00:00')):
            conn.execute(
                'INSERT INTO invites (token, label, unit_id, role, root_id, created_by, '
                'expires_at, accepted_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                (token, 'PSG', a['child'], 'leader', a['root'], 'boss', expires, accepted))
        recent = server.app_stamp()
        for stamp, details in ((recent, SECRET_AUDIT_DETAIL), ('2001-02-03 04:05:06', 'ancient')):
            conn.execute('INSERT INTO audit_log (timestamp, user_id, username, action, details, '
                         'unit_id, root_id) VALUES (%s, %s, %s, %s, %s, %s, %s)',
                         (stamp, a_owner['id'], 'boss', 'LOGIN', details, a['root'], a['root']))
        conn.commit()
    finally:
        conn.close()
    return {'a': a, 'b': b, 'stray': stray, 'a_owner': a_owner, 'last_activity': recent}


# ── The gate ──

def test_unauthenticated_is_401():
    """404 hides the surface from a signed-in stranger; a caller with no
    session at all gets the same 401 as every other route, because pretending
    a route does not exist to someone who has not even tried to sign in tells
    them nothing and breaks the client's retry."""
    client = install(Clerk({}))
    r = client.get(OVERVIEW)
    assert r.status_code == 401, (r.status_code, r.get_data(as_text=True)[:200])
    assert r.is_json, 'the 401 must be JSON like every other API route'


def test_a_verified_admin_gets_every_tenant(fx):
    clerk = Clerk({'clerk_boss': ADMIN_EMAIL})
    client = install(clerk, session_sub='clerk_boss')
    r = client.get(OVERVIEW)
    assert r.status_code == 200, (r.status_code, r.get_data(as_text=True)[:300])
    body = r.get_json()
    names = sorted(o['org_name'] for o in body['organisations'])
    assert names == ['Alpha Co', 'Bravo Co'], names
    assert clerk.calls == ['clerk_boss'], clerk.calls
    assert body['generated_at'], 'the dashboard must say when it was generated'


def test_an_unverified_admin_email_is_404():
    """Same address, not yet proven. Clerk reports it, the app must not take
    it: an unverified address is one anybody can type into a sign-up form."""
    client = install(Clerk({'clerk_impostor': ''}), session_sub='clerk_impostor')
    assert client.get(OVERVIEW).status_code == 404


def test_a_different_email_is_404():
    client = install(Clerk({'clerk_rando': 'someone@example.com'}), session_sub='clerk_rando')
    r = client.get(OVERVIEW)
    assert r.status_code == 404, r.status_code
    assert 'admin' not in r.get_data(as_text=True).lower(), \
        'the 404 body advertises the surface it is meant to hide'


def test_a_client_supplied_email_is_not_a_grant():
    """The hostile case this project has already been bitten by once: an email
    that arrives in the request. Header, query string, body — none of them is
    a credential, and none may reach the lookup."""
    clerk = Clerk({'clerk_rando': 'someone@example.com'})
    client = install(clerk, session_sub='clerk_rando')
    r = client.get(f'{OVERVIEW}?email={ADMIN_EMAIL}',
                   headers={'X-Email': ADMIN_EMAIL, 'X-Platform-Admin': 'true'},
                   json={'email': ADMIN_EMAIL})
    assert r.status_code == 404, r.status_code
    assert clerk.calls == ['clerk_rando'], \
        f'the lookup key came from the request, not the token: {clerk.calls}'


def test_a_stored_admin_email_is_only_a_hint():
    """users.email is written from the /api/auth/sync body, so a stranger can
    put the admin's address in their own row. Clerk's answer is the grant."""
    user = dbharness.make_user(None, 'leader', 'liar')
    conn = dbharness.owner_conn()
    conn.execute('UPDATE users SET email = %s WHERE id = %s', (ADMIN_EMAIL, user['id']))
    conn.commit(); conn.close()

    clerk = Clerk({user['clerk_user_id']: 'liar@example.com'})
    client = install(clerk, session_sub=user['clerk_user_id'])
    assert client.get(OVERVIEW).status_code == 404, 'a stored email granted access'
    assert clerk.calls == [user['clerk_user_id']], clerk.calls


def test_clerk_unreachable_fails_closed():
    client = install(Clerk(raises=True), session_sub='clerk_boss')
    r = client.get(OVERVIEW)
    assert r.status_code in (403, 404, 503), r.status_code
    assert r.status_code != 200, 'an unreachable Clerk opened the door'
    assert not server._PLATFORM_ADMIN_CACHE, 'a failed lookup must not be cached as a verdict'


def test_the_verdict_is_cached_both_ways():
    clerk = Clerk({'clerk_boss': ADMIN_EMAIL, 'clerk_rando': 'no@example.com'})
    client = install(clerk, session_sub='clerk_boss')
    assert client.get(OVERVIEW).status_code == 200
    assert client.get(OVERVIEW).status_code == 200
    assert clerk.calls == ['clerk_boss'], f'the dashboard re-asked Clerk: {clerk.calls}'

    server._verify_clerk_session_token = lambda: ({'sub': 'clerk_rando'}, None)
    assert client.get(OVERVIEW).status_code == 404
    assert client.get(OVERVIEW).status_code == 404
    assert clerk.calls == ['clerk_boss', 'clerk_rando'], \
        f'a refusal is not cached, so anyone can make us call Clerk at will: {clerk.calls}'


def test_the_cache_expires():
    clerk = Clerk({'clerk_boss': ADMIN_EMAIL})
    client = install(clerk, session_sub='clerk_boss')
    assert client.get(OVERVIEW).status_code == 200
    server._PLATFORM_ADMIN_CACHE['clerk_boss'] = (0, True)   # long expired
    assert client.get(OVERVIEW).status_code == 200
    assert clerk.calls == ['clerk_boss', 'clerk_boss'], \
        f'an expired verdict was reused forever: {clerk.calls}'


def clerk_user_payload(primary_verified=True, primary='addr-1'):
    """What GET https://api.clerk.com/v1/users/<id> answers with, trimmed to
    the fields the lookup reads."""
    return {
        'id': 'clerk_boss',
        'primary_email_address_id': primary,
        'email_addresses': [
            {'id': 'addr-1', 'email_address': 'Jonathon.Carr5@GMAIL.com',
             'verification': {'status': 'verified' if primary_verified else 'unverified'}},
            # A second, fully verified address that is NOT the primary one.
            # Anyone can add an address to their own Clerk account, so taking
            # "any verified address" instead of the primary would let someone
            # else's account answer to this one.
            {'id': 'addr-2', 'email_address': 'someone.else@example.com',
             'verification': {'status': 'verified'}},
        ],
    }


def fake_clerk_http(payload, error=None):
    """Stand in for urlopen, one layer below the seam the rest of the file
    stubs — this is what actually parses Clerk's answer."""
    import io
    import contextlib
    sent = {}

    def urlopen(req, timeout=None):
        sent['url'] = req.full_url
        sent['headers'] = dict(req.headers)
        sent['timeout'] = timeout
        if error:
            raise error
        return contextlib.closing(io.BytesIO(json.dumps(payload).encode()))

    return urlopen, sent


def test_the_clerk_lookup_reads_the_verified_primary_only():
    real_urlopen, real_key = server.urlopen, server.CLERK_SECRET_KEY
    try:
        server.CLERK_SECRET_KEY = 'sk_test_fake'

        server.urlopen, sent = fake_clerk_http(clerk_user_payload())
        assert _real_lookup('clerk_boss') == ADMIN_EMAIL, \
            'a verified primary address is not being returned (lower-cased)'
        assert sent['url'].endswith('/users/clerk_boss'), sent['url']
        assert sent['headers'].get('Authorization') == 'Bearer sk_test_fake', sent['headers']
        # Cloudflare answers urllib's default User-Agent with error 1010.
        assert 'python-urllib' not in sent['headers'].get('User-agent', '').lower(), sent['headers']
        assert sent['headers'].get('User-agent'), 'no User-Agent was sent'
        assert sent['timeout'] and sent['timeout'] <= 5, f'no bounded timeout: {sent["timeout"]}'

        server.urlopen, _ = fake_clerk_http(clerk_user_payload(primary_verified=False))
        assert _real_lookup('clerk_boss') == '', \
            'an UNVERIFIED primary address was accepted'

        server.urlopen, _ = fake_clerk_http(clerk_user_payload(primary='addr-2'))
        assert _real_lookup('clerk_boss') == 'someone.else@example.com', \
            'the lookup is not following primary_email_address_id'

        server.urlopen, _ = fake_clerk_http(None, error=OSError('connection refused'))
        try:
            _real_lookup('clerk_boss')
            assert False, 'an unreachable Clerk returned an answer instead of raising'
        except OSError:
            pass

        server.CLERK_SECRET_KEY = ''
        try:
            _real_lookup('clerk_boss')
            assert False, 'no secret key must raise, not quietly answer'
        except RuntimeError:
            pass
    finally:
        server.urlopen, server.CLERK_SECRET_KEY = real_urlopen, real_key


# ── The flag on /api/me ──

def test_me_reports_the_flag():
    boss = dbharness.make_user(None, 'leader', 'the-boss')
    conn = dbharness.owner_conn()
    conn.execute('UPDATE users SET email = %s WHERE id = %s', (ADMIN_EMAIL, boss['id']))
    conn.commit(); conn.close()
    boss['email'] = ADMIN_EMAIL

    clerk = Clerk({boss['clerk_user_id']: ADMIN_EMAIL})
    client = install(clerk)
    dbharness.as_user(boss)
    body = client.get('/api/me').get_json()
    assert body.get('platform_admin') is True, body
    assert clerk.calls == [boss['clerk_user_id']], clerk.calls


def test_an_ordinary_user_never_asks_clerk():
    """The flag rides on every /api/me. If an ordinary sign-in cost a call to
    api.clerk.com, every page load would carry Clerk's latency and Clerk's
    outages — so the stored email decides when it is worth asking at all."""
    clerk = Clerk({})
    client = install(clerk)
    dbharness.as_user(dbharness.make_user(None, 'leader', 'ordinary'))
    body = client.get('/api/me').get_json()
    assert body.get('platform_admin') is False, body
    assert clerk.calls == [], f'an ordinary /api/me called Clerk {len(clerk.calls)}x'


# ── The numbers ──

def test_the_overview_counts_two_tenants(fx):
    client = install(Clerk({'clerk_boss': ADMIN_EMAIL}), session_sub='clerk_boss')
    body = client.get(OVERVIEW).get_json()
    t = body['totals']
    assert t['organisations'] == 2, t
    assert t['unit_count'] == 4, t
    assert t['personnel_count'] == 4, t
    assert t['user_count'] == 3, f'attached users only: {t}'
    assert t['unattached_users'] == 2, t
    assert t['pending_invites'] == 1, f'pending is unaccepted AND unexpired: {t}'
    assert t['database_bytes'] > 0, t

    orgs = {o['org_name']: o for o in body['organisations']}
    alpha, bravo = orgs['Alpha Co'], orgs['Bravo Co']
    assert (alpha['unit_count'], alpha['personnel_count'], alpha['user_count']) == (2, 3, 2), alpha
    assert (bravo['unit_count'], bravo['personnel_count'], bravo['user_count']) == (2, 1, 1), bravo
    assert alpha['pending_invites'] == 1 and bravo['pending_invites'] == 0, (alpha, bravo)
    assert alpha['has_logo'] is True and bravo['has_logo'] is False, (alpha, bravo)
    assert 'alpha-owner@example.com' in (alpha['owner_emails'] or ''), alpha
    assert 'alpha-leader@example.com' not in (alpha['owner_emails'] or ''), \
        'a leader is being listed as an owner'
    assert alpha['last_activity'] == fx['last_activity'], alpha
    assert alpha['audit_7d'] == 1, f'the ancient row is being counted as this week: {alpha}'
    assert bravo['audit_7d'] == 0, bravo
    assert alpha['org_slug'] == 'alpha-co' and alpha['org_kind'] == 'company', alpha
    assert alpha['created_stamp'], 'units.created_at exists; show it'

    emails = [u['email'] for u in body['recent_users']]
    assert 'stray-2@example.com' in emails, emails
    stray = [u for u in body['recent_users'] if u['email'] == 'stray-2@example.com'][0]
    assert stray['org_name'] is None, f'an unattached user belongs to no organisation: {stray}'
    attached = [u for u in body['recent_users'] if u['email'] == 'alpha-owner@example.com'][0]
    assert attached['org_name'] == 'Alpha Co', attached
    assert attached['role'] == 'owner' and attached['signed_in'] is True, attached


def test_the_payload_carries_no_secrets(fx):
    """Read-only is not the same as harmless. The dashboard shows shape and
    size, never contents: no soldier is named in it, no invite token is in it,
    no audit detail and no Clerk id."""
    client = install(Clerk({'clerk_boss': ADMIN_EMAIL}), session_sub='clerk_boss')
    raw = json.dumps(client.get(OVERVIEW).get_json())
    for secret in (SECRET_INVITE_TOKEN, SECRET_AUDIT_DETAIL, 'Anderson', 'Ashby',
                   'clerk_alpha-owner', 'data:image/png'):
        assert secret not in raw, f'{secret!r} is in the dashboard payload'


# ── Structure: the database cannot tell an admin apart, so the code must ──

def test_rls_is_untouched_by_the_admin_functions(fx):
    """The functions are SECURITY DEFINER; the app role is not. A plain
    cross-tenant SELECT as platoon_app must still see nothing."""
    with psycopg.connect(os.environ['DATABASE_URL'], row_factory=dict_row) as conn:
        server.set_tenant(conn, fx['a']['root'])
        rows = conn.execute('SELECT name FROM units').fetchall()
        assert all(r['name'] != 'Bravo Co' for r in rows), rows
        assert conn.execute('SELECT count(*) AS n FROM admin_totals(%s)',
                            ('2999-01-01 00:00:00',)).fetchone()['n'] == 1, \
            'the function did not run for the app role, so nothing above was tested'


ADMIN_FUNCTIONS = [
    'admin_totals(text)',
    'admin_organisations(text)',
    'admin_recent_users(int)',
]


def test_public_cannot_execute():
    conn = dbharness.owner_conn()
    try:
        schema = conn.execute('SELECT current_schema() AS s').fetchone()['s']
        for sig in ADMIN_FUNCTIONS:
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
            assert definer, f'{sig}: not SECURITY DEFINER, so it cannot read across tenants'
    finally:
        conn.close()


def test_booting_again_over_a_live_database_is_clean():
    """This ships onto a database that is already running, by `git pull &&
    docker compose up -d --build`, and every gunicorn worker runs init_db() on
    the way up — twice over, on a redeploy. Re-applying admin_functions.sql
    over functions that already exist must be a no-op, and must not quietly
    hand EXECUTE back to PUBLIC on the way through."""
    server.init_db()
    server.init_db()
    test_public_cannot_execute()
    client = install(Clerk({'clerk_boss': ADMIN_EMAIL}), session_sub='clerk_boss')
    assert client.get(OVERVIEW).status_code == 200, 'the dashboard broke on the second boot'


def test_every_admin_call_is_behind_the_decorator():
    """The gate is in Python, so it is Python that has to be checked. Every
    route that mentions an admin_ function must carry the decorator, and every
    mention must be inside a route."""
    src = open(os.path.join(_ROOT, 'server.py'), encoding='utf-8').read()
    code = re.sub(r'#[^\n]*|"""[\s\S]*?"""', '', src)
    blocks = re.split(r'\n(?=@app\.route|\ndef )', code)
    callers = [b for b in blocks if re.search(r'\badmin_(totals|organisations|recent_users)\s*\(', b)]
    assert callers, 'no code calls the admin_ functions at all — did they get renamed?'
    for block in callers:
        assert '@platform_admin_required' in block, (
            'an admin_ function is called from code that is not gated by '
            f'@platform_admin_required:\n{block[:400]}')


def test_admin_is_a_reserved_slug():
    """A unit named "Admin" must not take the /admin URL away from the
    dashboard — and must still be creatable, under a slug of its own."""
    assert server.slugify('Admin') != 'admin', server.slugify('Admin')
    assert server.slugify('  ADMIN  ') != 'admin', server.slugify('  ADMIN  ')
    assert server.slugify('Administration') == 'administration', 'only the exact slug is reserved'

    t = dbharness.make_tree('Reserved Co')
    dbharness.as_user(dbharness.make_user(t['root'], 'owner', 'reserve-boss'))
    client = server.app.test_client()
    r = client.post('/api/units', json={'name': 'Admin', 'kind': 'squad', 'parent_id': t['root']})
    assert r.status_code == 201, (r.status_code, r.get_json())
    assert r.get_json()['slug'] != 'admin', r.get_json()

    unit_id = r.get_json()['id']
    before = r.get_json()['slug']
    r = client.put(f'/api/units/{unit_id}', json={'name': 'Admin'})
    assert r.status_code == 200, (r.status_code, r.get_json())
    assert r.get_json()['slug'] == before, 'a rename changed the slug; /admin can now be stolen'


def main():
    try:
        fx = seed_two_tenants()
        test_unauthenticated_is_401()
        test_a_verified_admin_gets_every_tenant(fx)
        # The counts are exactly the fixture's, so they run before anything
        # that adds a user or a unit of its own.
        test_the_overview_counts_two_tenants(fx)
        test_the_payload_carries_no_secrets(fx)
        test_an_unverified_admin_email_is_404()
        test_a_different_email_is_404()
        test_a_client_supplied_email_is_not_a_grant()
        test_a_stored_admin_email_is_only_a_hint()
        test_the_clerk_lookup_reads_the_verified_primary_only()
        test_clerk_unreachable_fails_closed()
        test_the_verdict_is_cached_both_ways()
        test_the_cache_expires()
        test_me_reports_the_flag()
        test_an_ordinary_user_never_asks_clerk()
        test_rls_is_untouched_by_the_admin_functions(fx)
        test_public_cannot_execute()
        test_booting_again_over_a_live_database_is_clean()
        test_every_admin_call_is_behind_the_decorator()
        test_admin_is_a_reserved_slug()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()

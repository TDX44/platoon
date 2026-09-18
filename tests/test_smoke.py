"""Whole-app smoke test — catches a typo'd route or import error before it
reaches production, since there is no CI today. Run with:
    python tests/test_smoke.py
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dbharness  # noqa: E402
_schema = dbharness.setup()
# Production always runs with Clerk configured. Enable it here too (with a
# fake key/domain) so login_required/attached_required hit their real 401 path
# instead of the "Clerk is not configured" 500 — every check below is
# unauthenticated and never sends a token, so the JWKS client is never
# actually contacted.
os.environ['CLERK_PUBLISHABLE_KEY'] = 'pk_test_' + 'a' * 48
os.environ['CLERK_FRONTEND_API_URL'] = 'https://smoke-test.clerk.accounts.dev'

import server  # noqa: E402  (must follow the env overrides above)

EXPECTED_TABLES = [
    'personnel', 'personnel_profile', 'settings', 'users', 'audit_log',
    'duty_roster', 'scheduled_events',
]

# '/' and the catch-all are checked directly below, not via the generic sweep.
# '/app' joins them because it is the one public route whose right answer is a
# redirect, which the sweep treats as a bad deploy; check_marketing_host_split
# pins both of its branches instead.
SKIP_RULES = {'/', '/app', '/<path:path>'}



# Source-level gate: the fixed three platoons are gone, and `platoon` survives
# only as one of the unit *kinds* plus a few legacy proper names (the env var,
# the default secret, the backup filename, the two database roles). A column, a
# scope argument, a per-platoon settings key or an `is_admin` flag coming back
# means the tree is no longer the only scope -- and that is exactly the kind of
# thing that reappears quietly in a merge.
BANNED_IN_SERVER = (
    'has_platoon_access', 'PLATOONS', 'is_admin', 'DEFAULT_TDY_SCHOOLS',
    'DEFAULT_TDY_LOCATIONS', 'platoon = %s', 'platoons',
    # Task 4 renamed this audit action; the spec allows no behaviour change
    # outside its scope, so the audit log keeps saying DELETE_PERSON.
    'REMOVE_PERSON',
    # Settings keys are scoped by (root_id, unit_id) columns now, never by a
    # suffix on the key.
    'unit_name_', 'tdy_schools_', 'tdy_locations_',
)

# The only bare `platoon` tokens server.py is still allowed to contain.
ALLOWED_PLATOON_TOKENS = (
    "'platoon'",                            # a unit kind in UNIT_KINDS
    'PLATOON_TZ',                           # the fallback-timezone env var
    'platoon-tracker-change-in-production',  # the dev-only default secret
    'platoon-backup-',                      # the backup download filename
    'platoon_owner', 'platoon_app',          # the two database roles
    'platoon-accountability/',               # the User-Agent sent to api.clerk.com
    'platoon_leader_',                       # the Stripe price lookup keys (PRICE_LOOKUP_KEYS)
    'platoonmanager.com',                    # the product's own domain (MARKETING_HOSTS, APP_URL)
    'platoon.carr7.com',                     # the retired hostname (LEGACY_HOSTS)
)


def check_no_fixed_platoons_remain():
    src = open(os.path.join(ROOT, 'server.py'), encoding='utf-8').read()
    code = re.sub(r'#[^\n]*|"""[\s\S]*?"""', '', src)
    for word in BANNED_IN_SERVER:
        assert word not in code, f'{word!r} is still in server.py'
    rest = code
    for token in ALLOWED_PLATOON_TOKENS:
        rest = rest.replace(token, '')
    hits = sorted(set(re.findall(r'\w*platoon\w*', rest, re.I)))
    assert not hits, f'`platoon` still appears in server.py code: {hits}'


def check_tables():
    conn = server.get_db()
    names = {r['table_name'] for r in conn.execute(
        'SELECT table_name FROM information_schema.tables '
        'WHERE table_schema = current_schema()').fetchall()}
    conn.close()
    for table in EXPECTED_TABLES:
        assert table in names, f'init_db did not create the {table!r} table'


def check_index(client):
    r = client.get('/')
    assert r.status_code == 200, f'GET / should return 200, got {r.status_code}'
    body = r.get_data(as_text=True).lower()
    assert '<html' in body and 'platoon manager' in body, 'GET / does not look like the SPA shell'


def check_spa_fallback(client):
    r = client.get('/some/unknown/page')
    assert r.status_code == 200, f'unknown non-api path should fall back to index.html, got {r.status_code}'
    assert '<html' in r.get_data(as_text=True).lower(), 'spa_fallback did not serve index.html'

    r = client.get('/api/totally-not-a-route')
    assert r.status_code == 404, f'unknown /api/ path should 404, got {r.status_code}'
    assert r.is_json, 'unknown /api/ path must return JSON, not HTML'


def check_public_pages(client):
    """The signed-out pages must render standalone, not as the SPA shell: Google's
    OAuth consent screen links straight at /privacy and /terms."""
    for path, marker in (('/home', 'Start free trial'),
                         ('/welcome', 'Platoon Manager'),
                         ('/privacy', 'Privacy Policy'),
                         ('/terms', 'Terms of Service')):
        r = client.get(path)
        assert r.status_code == 200, f'{path} should render, got {r.status_code}'
        body = r.get_data(as_text=True)
        assert marker in body, f'{path} did not render its own page'
        assert '<script' not in body.lower(), f'{path} must render with no JS at all'

    # /legal/* is the URL shape people actually reach for. Before these aliases
    # existed it fell through to spa_fallback and served the app shell with a
    # 200, which looks like the page is simply missing.
    index = client.get('/').get_data()
    for path in ('/legal/privacy', '/legal/terms'):
        body = client.get(path).get_data()
        assert body != index, f'{path} served the SPA shell instead of the legal page'
        assert b'Platoon Manager' in body, f'{path} did not render a legal page'

    r = client.get('/public/site.css')
    assert r.status_code == 200 and 'cp-accent' in r.get_data(as_text=True), \
        'the public stylesheet must be served'


def check_marketing_host_split(client):
    """'/' serves the marketing site on the marketing domain and the app shell on
    every other host. This split is the only thing between a leader opening the
    app at 0630 and being handed a brochure, so pin both sides of it -- including
    that an unrecognised host (localhost, the LAN address, app.*) gets the app.
    """
    app_shell = client.get('/').get_data()
    assert b'<html' in app_shell.lower(), '/ did not serve the app shell on an ordinary host'

    host = server.MARKETING_HOSTS[0]
    body = client.get('/', headers={'Host': host}).get_data()
    assert body != app_shell, f'{host}/ served the app shell instead of the marketing site'
    assert b'Start free trial' in body, f'{host}/ did not serve the marketing site'
    assert b'<script' not in body.lower(), 'the marketing site must render with no JS at all'

    # 'Sign in' leaves for the app's own subdomain from the marketing host, and
    # stays local anywhere else -- clicking it in dev must not land in production.
    r = client.get('/app', headers={'Host': host})
    assert r.status_code == 302 and r.headers['Location'] == server.APP_URL, \
        ('marketing host /app', r.status_code, r.headers.get('Location'))
    r = client.get('/app')
    assert r.status_code == 302 and r.headers['Location'] == '/', \
        ('ordinary host /app', r.status_code, r.headers.get('Location'))


def check_marketing_assets_exist(client):
    """Every image and font the public pages name is really on disk and really
    served. images/site/ is a new directory under an existing STATIC_DIRS prefix;
    a path typo here is a broken hero on the front page and nothing else fails.
    """
    for page in ('home.html', 'privacy.html', 'terms.html'):
        html = open(os.path.join(ROOT, 'public', page)).read()
        for path in sorted(set(re.findall(r'"(/(?:images|public)/[^"]+)"', html))):
            r = client.get(path)
            assert r.status_code == 200, f'{page} references {path}, which serves {r.status_code}'


def check_legacy_host_redirect(client):
    """The old hostname is retired by 301, not switched off -- people have it
    bookmarked. '/' goes to the new front page; every other path keeps itself and
    its query string on the app subdomain, because a bookmark there is a roster,
    not a brochure.
    """
    h = {'Host': server.LEGACY_HOSTS[0]}

    r = client.get('/', headers=h)
    assert r.status_code == 301 and r.headers['Location'] == server.MARKETING_URL, \
        ('legacy /', r.status_code, r.headers.get('Location'))

    r = client.get('/alpha/accountability?tab=roster', headers=h)
    assert r.status_code == 301, ('legacy deep link', r.status_code)
    assert r.headers['Location'] == server.APP_URL + '/alpha/accountability?tab=roster', \
        f"a bookmarked roster lost its path or query: {r.headers.get('Location')}"

    # A cross-origin 301 does not move an open tab, it breaks its request -- and
    # a 301 on a POST drops the body. Neither may happen.
    assert client.get('/api/me', headers=h).status_code != 301, \
        '/api/ on the legacy host was redirected; an open tab loses its in-flight write'
    assert client.post('/api/reset', headers=h).status_code != 301, \
        'a non-GET was redirected; the 301 would turn the POST into a GET'

    # And none of it fires on any other host.
    assert client.get('/').status_code == 200, 'the legacy redirect fired on an ordinary host'


def check_no_unit_identifier(client):
    """The app is a generic company-formation accountability tool. Nothing served
    to a browser may name the unit that happens to run this instance -- the unit
    name belongs in the unit tree's own rows, not in shipped markup."""
    banned = ('15th', 'MI BN', 'A Co')
    for path in ('/', '/welcome', '/privacy', '/terms', '/manifest.json'):
        body = client.get(path).get_data(as_text=True)
        hits = [b for b in banned if b in body]
        assert not hits, f'{path} names the unit: {hits}'


def check_source_is_not_served(client):
    """spa_fallback serves assets only. It used to serve any file that existed on
    disk, which handed out server.py to anyone who asked."""
    index = client.get('/').get_data()
    for path in ('/server.py', '/requirements.txt', '/Dockerfile', '/CLAUDE.md'):
        body = client.get(path).get_data()
        assert body == index, f'{path} is being served from disk; it must fall back to index.html'

    for path in ('/images/icon-32.png', '/manifest.json', '/public/site.css'):
        assert client.get(path).get_data() != index, f'{path} should be served as an asset'


def check_auth_config(client):
    r = client.get('/api/auth/config')
    assert r.status_code == 200, f'/api/auth/config must be public, got {r.status_code}'
    assert r.is_json, '/api/auth/config must return JSON'


def argless_get_routes():
    for rule in server.app.url_map.iter_rules():
        if 'GET' not in rule.methods or rule.arguments or rule.rule in SKIP_RULES:
            continue
        yield rule.rule


def check_unauthenticated_routes(client):
    for rule in argless_get_routes():
        if rule == '/api/auth/config':
            continue
        r = client.get(rule)
        # 200/401/403 are the only legitimate outcomes for a logged-out request.
        # Anything else (500 from an unhandled exception, a stray redirect...)
        # means a bad deploy would have gone out unnoticed.
        assert r.status_code in (200, 401, 403), (
            f'GET {rule} unauthenticated returned {r.status_code}, expected 200/401/403'
        )
        if r.status_code in (401, 403):
            assert r.is_json, f'GET {rule} returned {r.status_code} but the body is not JSON'


# Routes that are public on purpose. Anything else under /api/ must sit behind
# clerk_auth_required / login_required / attached_required / owner_required, all
# of which use functools.wraps and so leave a __wrapped__ on the view function.
# Forgetting a decorator on a new route is silent and serious, and the GET sweep
# above cannot see it on a POST/PUT/DELETE route.
PUBLIC_API = {
    '/api/auth/config',
    '/api/logout',
    '/api/invites/<token>/preview',
    '/api/billing/webhook',
}


def check_every_api_route_is_guarded():
    unguarded = []
    for rule in server.app.url_map.iter_rules():
        if not rule.rule.startswith('/api/') or rule.rule in PUBLIC_API:
            continue
        view = server.app.view_functions[rule.endpoint]
        if not hasattr(view, '__wrapped__'):
            methods = sorted(m for m in rule.methods if m in {'GET', 'POST', 'PUT', 'DELETE'})
            unguarded.append(f'{rule.rule} {methods}')
    assert not unguarded, (
        'these /api/ routes have no auth decorator (add one, or add the route to '
        f'PUBLIC_API if it is deliberately public): {unguarded}'
    )


def check_units_is_open_to_the_unattached(client):
    """/api/units GET is the one route deliberately reachable by a signed-in
    user who belongs to no unit yet: they have no tenant, so RLS hands them
    nothing, and the empty list is what tells the frontend to offer "create a
    unit" rather than an error. Runs last — it monkeypatches the current user."""
    dbharness.as_user(dbharness.make_user(None))
    r = client.get('/api/units')
    assert r.status_code == 200, (r.status_code, r.get_json())
    assert r.get_json() == [], r.get_json()


def main():
    check_no_fixed_platoons_remain()
    check_tables()
    client = server.app.test_client()
    check_index(client)
    check_spa_fallback(client)
    check_public_pages(client)
    check_marketing_host_split(client)
    check_marketing_assets_exist(client)
    check_legacy_host_redirect(client)
    check_no_unit_identifier(client)
    check_source_is_not_served(client)
    check_auth_config(client)
    check_unauthenticated_routes(client)
    check_every_api_route_is_guarded()
    check_units_is_open_to_the_unattached(client)
    print('ok')
    dbharness.teardown(_schema)


if __name__ == '__main__':
    main()

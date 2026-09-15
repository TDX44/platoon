"""Whole-app smoke test — catches a typo'd route or import error before it
reaches production, since there is no CI today. Run with:
    python tests/test_smoke.py
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['DATA_DIR'] = tempfile.mkdtemp()
# Production always runs with Clerk configured. Enable it here too (with a
# fake key/domain) so login_required/admin_required hit their real 401 path
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
SKIP_RULES = {'/', '/<path:path>'}


def check_tables():
    conn = server.get_db()
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    conn.close()
    for table in EXPECTED_TABLES:
        assert table in names, f'init_db did not create the {table!r} table'


def check_index(client):
    r = client.get('/')
    assert r.status_code == 200, f'GET / should return 200, got {r.status_code}'
    body = r.get_data(as_text=True).lower()
    assert '<html' in body and 'platoon accountability' in body, 'GET / does not look like the SPA shell'


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
    for path, marker in (('/welcome', 'Platoon Accountability'),
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
        assert b'Platoon Accountability' in body, f'{path} did not render a legal page'

    r = client.get('/public/site.css')
    assert r.status_code == 200 and 'cp-accent' in r.get_data(as_text=True), \
        'the public stylesheet must be served'


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
# clerk_auth_required / login_required / admin_required, all of which use
# functools.wraps and so leave a __wrapped__ on the view function. Forgetting a
# decorator on a new route is silent and serious, and the GET sweep above cannot
# see it on a POST/PUT/DELETE route.
PUBLIC_API = {
    '/api/auth/config',
    '/api/logout',
    '/api/invites/<token>/preview',
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


def main():
    check_tables()
    client = server.app.test_client()
    check_index(client)
    check_spa_fallback(client)
    check_public_pages(client)
    check_source_is_not_served(client)
    check_auth_config(client)
    check_unauthenticated_routes(client)
    check_every_api_route_is_guarded()
    print('ok')


if __name__ == '__main__':
    main()

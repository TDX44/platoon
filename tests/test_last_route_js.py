"""A cold start lands where the user left off.

The iOS app always launches at '/', and so does a tab reopened on the bare
address; restoreLastRoute() puts back the page saved by saveLastRoute() when
the page was last hidden. Drives the real index.html in Chromium through
test_mobile_layout's harness (stubbed API, no Clerk).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_mobile_layout as t  # noqa: E402  (skips cleanly without playwright)
from playwright.sync_api import sync_playwright  # noqa: E402

FIXTURE = {'units': t.UNITS_FIXTURE, 'roster': t.ROSTER_FIXTURE, 'directory': t.DIRECTORY_FIXTURE,
           'availability': t.AVAILABILITY_FIXTURE, 'users': t.USERS_FIXTURE, 'invites': t.INVITES_FIXTURE,
           'settings': t.SETTINGS_FIXTURE, 'user': t.USER_FIXTURE, 'profile': t.PROFILE_FIXTURE,
           'admin': t.ADMIN_FIXTURE}

# Leave the page the way backgrounding the app does, then start again at '/'
# as a fresh launch would, and route.
HIDE_JS = """() => {
  document.body.classList.add('app-authenticated');
  window.dispatchEvent(new Event('pagehide'));
}"""
COLD_START_JS = """async (path) => {
  history.replaceState({}, '', path);
  lastRouteRestored = false;
  routeAfterLogin();
  await new Promise(r => setTimeout(r, 50));
  return { path: location.pathname, unit: currentUnit ? currentUnit.slug : null };
}"""


def main():
    httpd, thread, port = t.start_server()
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            page = browser.new_page()
            errors = []
            page.on('pageerror', lambda e: errors.append(str(e)))
            page.goto(f'http://127.0.0.1:{port}/', wait_until='load')
            page.evaluate(t.INIT_JS, FIXTURE)

            # In 2nd Platoon, app backgrounded, relaunched at '/': back in 2nd Platoon.
            page.evaluate(t.ENTER_UNIT_JS, 3)
            page.evaluate(HIDE_JS)
            got = page.evaluate(COLD_START_JS, '/')
            assert got == {'path': '/2nd-platoon/accountability', 'unit': '2nd-platoon'}, got
            print('ok - a cold start at / returns to the unit the user left')

            # A deeper page comes back as itself.
            page.evaluate("() => openDirectory()")
            page.evaluate(HIDE_JS)
            got = page.evaluate(COLD_START_JS, '/')
            assert got['path'] == '/2nd-platoon/directory', got
            print('ok - the page inside the unit is restored too')

            # Only once per load: going Home afterwards stays Home.
            page.evaluate("() => history.replaceState({}, '', '/')")
            page.evaluate("() => routeAfterLogin()")
            assert page.evaluate("location.pathname") == '/'
            print('ok - restored once per page load, not on every route')

            # Left at Home: Home is what comes back.
            page.evaluate(HIDE_JS)
            got = page.evaluate(COLD_START_JS, '/')
            assert got['path'] == '/', got
            print('ok - leaving from Home reopens Home')

            # Another account's saved place is never used.
            page.evaluate("() => localStorage.setItem('lastRoute', JSON.stringify({u: 999, p: '/2nd-platoon/accountability'}))")
            got = page.evaluate(COLD_START_JS, '/')
            assert got['path'] == '/', got
            print("ok - another account's last place is ignored")

            # A unit that no longer exists (or is out of reach) stays Home.
            page.evaluate("() => localStorage.setItem('lastRoute', JSON.stringify({u: 1, p: '/gone-platoon/accountability'}))")
            got = page.evaluate(COLD_START_JS, '/')
            assert got['path'] == '/', got
            page.evaluate("() => localStorage.setItem('lastRoute', JSON.stringify({u: 1, p: '/admin'}))")
            got = page.evaluate(COLD_START_JS, '/')
            assert got['path'] == '/', got
            print('ok - an unknown unit (or /admin) is not restored')

            # An address the user asked for wins over the saved one.
            page.evaluate("() => localStorage.setItem('lastRoute', JSON.stringify({u: 1, p: '/2nd-platoon/accountability'}))")
            got = page.evaluate(COLD_START_JS, '/1st-platoon/accountability')
            assert got['path'] == '/1st-platoon/accountability', got
            got = page.evaluate(COLD_START_JS, '/?billing=success')
            assert got['path'] == '/', got
            print('ok - an explicit address or query is never overridden')

            # Signed out (login screen at '/'): nothing is saved.
            page.evaluate("() => { localStorage.removeItem('lastRoute'); document.body.classList.remove('app-authenticated'); window.dispatchEvent(new Event('pagehide')); }")
            assert page.evaluate("localStorage.getItem('lastRoute')") is None
            print('ok - nothing is saved before sign-in')

            assert not errors, errors
            browser.close()
    finally:
        t.stop_server(httpd, thread)
        t.dbharness.teardown(t._schema)


if __name__ == '__main__':
    main()

"""Real SPA checks for leader lifecycle controls and inactive-account recovery.

Runs without a database or Clerk. Every API response is a local synthetic
fixture; raw fetch calls in recovery checks are explicitly intercepted.
Run: python3 tests/test_access_activation_browser.py (Playwright + Chromium).
Set PLATOON_CAPTURE_AUDIT_SCREENSHOTS=1 to refresh the synthetic audit screenshot.
"""
import copy
import functools
from http.server import ThreadingHTTPServer
import os
import threading

from test_mobile_usability import Handler, ROOT, fixtures


def account_fixture():
    env, data = fixtures()
    data['user'].update(is_active=True, platform_admin=False)
    data['user']['billing'].update(state='COMPED', days_left=None, extension_available=False)
    data['users'] = [
        dict(id=1, username='ada.owner', full_name='Ada Owner', email='ada@example.invalid',
             role='owner', unit_id=1, unit_name='Headhunter Company', editable=True,
             is_active=True, can_toggle_active=False),
        dict(id=2, username='ray.leader', full_name='Ray Fixtureton', email='ray@example.invalid',
             role='leader', unit_id=2, unit_name='1st Platoon', editable=True,
             is_active=True, can_toggle_active=True),
        dict(id=3, username='nia.leader', full_name='Nia Placeholder', email='nia@example.invalid',
             role='leader', unit_id=3, unit_name='2nd Platoon', editable=True,
             is_active=False, can_toggle_active=True),
        dict(id=4, username='outside.leader', full_name='Outside Editable Scope',
             email='a.long.synthetic.address.for.layout@example.invalid', role='leader',
             unit_id=5, unit_name='Team Bravo (Weapons)', editable=False,
             is_active=True, can_toggle_active=False),
    ]
    return env, data


ACCESS_STUB = """() => {
  window.__fixtureApi = api;
  window.__writes = [];
  window.__mutationMode = 'success';
  window.__holdMutation = false;
  api = async (method, path, body) => {
    if (method !== 'GET' && /^\\/users\\/\\d+$/.test(path)) {
      window.__writes.push({ method, path, body });
      if (window.__holdMutation) await new Promise(resolve => { window.__releaseMutation = resolve; });
      if (window.__mutationMode === 'null') return null;
      const id = Number(path.split('/').pop());
      const row = __fixture.users.find(u => u.id === id);
      if (method === 'PUT') { Object.assign(row, body); return { ...row }; }
      if (method === 'DELETE') { __fixture.users = __fixture.users.filter(u => u.id !== id); return { ok: true }; }
      throw new Error('Unexpected fixture write');
    }
    return window.__fixtureApi(method, path, body);
  };
}"""


def prepare(page, base, env, data):
    page.route('**/*', lambda route: route.continue_()
               if route.request.url.startswith(base + '/') else route.abort())
    page.route('**/api/auth/config', lambda route: route.fulfill(json={'enabled': False}))
    page.goto(base)
    page.evaluate(env['INIT_JS'], copy.deepcopy(data))
    page.evaluate(env['ENTER_UNIT_JS'], 1)
    page.evaluate(ACCESS_STUB)
    page.evaluate("async () => { openSettings(true, 'people'); await loadAccessPage(); }")
    page.wait_for_selector('#accessUserList .user-row')


def assert_fits(page, width, label):
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1'), (
        f'{label} overflows {width}px')


def confirm(page, button='confirmOkBtn'):
    page.locator('#confirmModal').wait_for(state='visible')
    page.locator('#' + button).click()


def check_layout_and_permissions(page, width):
    assert page.locator('#accessSummary').inner_text() == '3 of 4 leader accounts'
    assert page.locator('#accessFilters [data-filter=active]').inner_text() == 'Active (3)'
    assert page.locator('#accessFilters [data-filter=inactive]').inner_text() == 'Inactive (1)'
    assert page.locator('.access-edit-btn[data-uid="1"]').count() == 1
    assert page.locator('.access-toggle-btn[data-uid="1"]').count() == 0
    assert page.locator('.user-del-btn[data-uid="1"]').count() == 0
    assert page.locator('.access-toggle-btn[data-uid="4"]').count() == 0
    assert page.locator('.user-del-btn[data-uid="4"]').count() == 0
    assert page.locator('.access-edit-btn[data-uid="4"]').count() == 0
    assert page.locator('.access-toggle-btn[data-uid="2"]').inner_text() == 'Deactivate'
    page.locator('.access-account-more').first.locator('summary').click()
    assert page.locator('.user-del-btn[data-uid="2"]').inner_text() == 'Remove access'
    page.locator('.access-account-more').first.locator('summary').click()
    page.locator('#accessFilters [data-filter=inactive]').click()
    assert page.locator('#accessSummary').inner_text() == '1 of 4 leader accounts'
    assert page.locator('.access-toggle-btn[data-uid="3"]').inner_text() == 'Reactivate'
    page.locator('#accessFilters [data-filter=all]').click()
    assert page.locator('#accessSummary').inner_text() == '4 of 4 leader accounts'
    page.locator('#accessSearch').fill('nia')
    assert page.locator('#accessSummary').inner_text() == '1 of 4 leader accounts'
    assert page.locator('#accessUserList .user-row').count() == 1
    page.locator('#accessSearch').fill('')
    assert_fits(page, width, 'leader accounts')
    if width <= 390:
        for el in page.locator('#accessFilters button, .access-edit-btn, .access-toggle-btn, .access-account-more summary').all():
            assert el.bounding_box()['height'] >= 44, 'account action is smaller than 44px'
    page.evaluate("currentUser.role = 'leader'; renderSettings(); accessFilter = 'all'; renderAccessUsers()")
    assert page.locator('.access-toggle-btn, .user-del-btn').count() == 0
    assert page.locator('.access-edit-btn[data-uid="2"]').count() == 1
    assert page.evaluate("async () => { await setUserActive(2, false); await deleteUser(2, 'Ray'); return __writes.length; }") == 0
    page.evaluate("currentUser.role = 'owner'; renderSettings(); renderAccessUsers()")


def check_lifecycle(page):
    page.locator('#accessFilters [data-filter=all]').click()
    page.locator('.access-toggle-btn[data-uid="2"]').click()
    assert page.locator('.access-toggle-btn[data-uid="2"]').is_disabled()
    confirm(page, 'confirmCancelBtn')
    page.wait_for_function('accessPending.size === 0')
    assert page.evaluate('__writes.length') == 0
    page.evaluate("__mutationMode = 'null'")
    page.locator('.access-toggle-btn[data-uid="2"]').click()
    confirm(page)
    page.wait_for_function('accessPending.size === 0')
    assert page.locator('#apiToast').inner_text() == 'Account access was not changed. Try again.'
    assert 'app-toast-success' not in page.locator('#apiToast').get_attribute('class')
    assert page.locator('.access-toggle-btn[data-uid="2"]').inner_text() == 'Deactivate'
    assert page.evaluate('__writes') == [dict(method='PUT', path='/users/2', body={'is_active': False})]

    page.evaluate("__writes = []; __mutationMode = 'success'; __holdMutation = true")
    page.locator('.access-toggle-btn[data-uid="2"]').click()
    confirm(page)
    page.wait_for_function('__writes.length === 1 && typeof __releaseMutation === "function"')
    assert page.locator('.access-edit-btn[data-uid="2"]').is_disabled()
    page.evaluate('setUserActive(2, false)')
    assert page.evaluate('__writes.length') == 1, 'duplicate pending deactivation was sent'
    page.evaluate('__holdMutation = false; __releaseMutation()')
    page.wait_for_function('accessPending.size === 0')
    assert page.locator('.access-toggle-btn[data-uid="2"]').inner_text() == 'Reactivate'
    assert page.locator('#accessFilters [data-filter=active]').inner_text() == 'Active (2)'
    assert page.locator('#apiToast').inner_text() == 'Leader deactivated.'
    page.locator('.access-toggle-btn[data-uid="2"]').click()
    confirm(page)
    page.wait_for_function('accessPending.size === 0')
    assert page.locator('.access-toggle-btn[data-uid="2"]').inner_text() == 'Deactivate'
    assert page.evaluate('__writes.slice(-1)[0]') == dict(method='PUT', path='/users/2', body={'is_active': True})
    assert page.locator('#apiToast').inner_text() == 'Leader reactivated.'
    assert page.evaluate('async () => { const before = __writes.length; await setUserActive(1, false); await deleteUser(1, "Self"); return __writes.length === before; }')
    page.evaluate("__mutationMode = 'null'")
    page.locator('.access-account-more').first.locator('summary').click()
    page.locator('.user-del-btn[data-uid="2"]').click()
    confirm(page)
    page.wait_for_function('accessPending.size === 0')
    assert page.locator('#apiToast').inner_text() == 'Access was not removed. Try again.'
    assert 'app-toast-success' not in page.locator('#apiToast').get_attribute('class')
    assert page.locator('.access-edit-btn[data-uid="2"]').count() == 1
    page.locator('.access-edit-btn[data-uid="2"]').click()
    page.locator('#userEditModal').wait_for(state='visible')
    page.locator('#userEditSave').click()
    page.wait_for_function('!userEditSaving')
    assert page.locator('#userEditModal').is_visible(), 'failed edit closed the draft'
    assert page.locator('#userEditError').is_visible()
    page.locator('#userEditModal .btn-cancel').click()


RAW_AUTH_STUB = """() => {
  api = __realApi;
  showLoginScreen = __realShowLoginScreen;
  ensureClerk = async () => ({ user: {
    id: 'user_fixture', username: 'ada.owner', fullName: 'Ada Owner',
    primaryEmailAddress: { emailAddress: 'ada@example.invalid' }
  } });
  window.__fetches = [];
  window.__reactivated = false;
  window.__denyPath = '/users';
  window.__denyMe = false;
  window.__deferUnits = false;
  window.fetch = async (url, opts = {}) => {
    const path = String(url);
    __fetches.push({ path, method: opts.method || 'GET', body: opts.body ? JSON.parse(opts.body) : null });
    const response = (value, status = 200) => new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } });
    const inactive = () => response({ error: 'Account deactivated', code: 'account_inactive' }, 403);
    if (path === '/api/auth/sync') return __reactivated ? response({ ...__fixture.user, is_active: true }) : inactive();
    if (path === '/api/billing/portal') return response({ url: location.href.split('#')[0] + '#billing-test' });
    if (path === '/api/billing/details') return response({ billing: __fixture.user.billing, plan: null, invoices: [], card: null });
    if (path === '/api/billing/refresh') return response({ found: false, billing: __fixture.user.billing });
    if (path === '/api/me') return response({ ...__fixture.user, is_active: !__denyMe });
    if (path === '/api/logout') return response({ ok: true });
    if (path === '/api/units' && __deferUnits) return new Promise(resolve => {
      window.__releaseUnits = () => resolve(response(__fixture.units));
    });
    if (path === '/api' + __denyPath) return inactive();
    if (path === '/api/units') return response(__fixture.units);
    if (path.startsWith('/api/personnel')) return response(__fixture.roster);
    if (path.startsWith('/api/settings')) return response(__fixture.settings);
    if (path === '/api/users') return response(__fixture.users);
    if (path === '/api/invites') return response([]);
    throw new Error('Unexpected raw fetch in isolated recovery fixture: ' + path);
  };
}"""


def assert_inactive(page):
    assert page.locator('#inactiveRetry').is_visible()
    assert page.locator('#inactiveBilling').is_visible()
    assert page.locator('#inactiveSignout').is_visible()
    assert not page.locator('#platoonScreen').is_visible()
    assert not page.locator('#homeScreen').is_visible()
    assert page.evaluate('accountInactive && currentUser === null && currentUnit === null && personnel.length === 0 && units.length === 0 && directoryRows.length === 0 && accessUsers === null')
    assert page.locator('#settingsView').inner_text() == ''
    assert page.locator('#unitCards').inner_text() == ''
    assert page.locator('#personnelBody').inner_text() == ''


def check_inactive_recovery(page):
    page.evaluate(RAW_AUTH_STUB)
    page.evaluate("() => { __deferUnits = true; window.__lateUnits = api('GET', '/units'); }")
    page.wait_for_function('typeof __releaseUnits === "function"')
    assert page.evaluate("async () => await api('GET', '/users')") is None
    assert_inactive(page)
    page.evaluate('__releaseUnits()')
    assert page.evaluate('async () => await __lateUnits') is None, 'late success exposed org data after deactivation'
    assert_inactive(page)
    for width in (320, 390, 1280):
        page.set_viewport_size({'width': width, 'height': 844})
        assert_fits(page, width, 'inactive account screen')
    page.locator('#inactiveRetry').click()
    page.wait_for_function('!authBusy')
    assert_inactive(page)
    assert any(r['path'] == '/api/auth/sync' for r in page.evaluate('__fetches'))
    assert page.evaluate("async () => !!(await api('GET', '/billing/details'))")
    assert page.evaluate("async () => !!(await api('POST', '/billing/refresh'))")
    assert_inactive(page)
    page.evaluate("__reactivated = true; __deferUnits = false; __denyPath = ''; history.replaceState({}, '', '/')")
    page.locator('#inactiveRetry').click()
    page.wait_for_function('!accountInactive && !!currentUser && units.length > 0')
    page.locator('#homeScreen').wait_for(state='visible')
    assert not page.locator('#loginScreen').is_visible()
    assert page.evaluate('currentUser.is_active === true')
    # /me can be readable while inactive; a false flag must still enter the
    # same dedicated denial screen rather than restoring normal navigation.
    page.evaluate('__denyMe = true')
    assert page.evaluate("async () => await api('GET', '/me')") is None
    assert_inactive(page)
    # Verify the real portal button redirects, using a same-document fixture
    # destination so no Stripe service or external network is ever contacted.
    # This is last: real portal navigation replaces the application's document.
    page.locator('#inactiveBilling').click()
    page.wait_for_url('**#billing-test')
    assert page.evaluate('__fetches.some(r => r.path === "/api/billing/portal" && r.method === "POST" && r.body.intent === "cancel")')


def main():
    try:
        from playwright.sync_api import sync_playwright, Error
    except ImportError:
        print('SKIPPED (not ok): Playwright is required for leader activation browser checks')
        return
    env, data = account_fixture()
    httpd = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(Handler, directory=str(ROOT)))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    base = f'http://127.0.0.1:{httpd.server_port}'
    try:
        with sync_playwright() as pw:
            try:
                browser = pw.chromium.launch()
            except Error:
                print('SKIPPED (not ok): Chromium is required for leader activation browser checks')
                return
            try:
                for width in (320, 390, 1280):
                    page = browser.new_page(viewport={'width': width, 'height': 844})
                    try:
                        prepare(page, base, env, data)
                        check_layout_and_permissions(page, width)
                        if width == 390:
                            if os.environ.get('PLATOON_CAPTURE_AUDIT_SCREENSHOTS') == '1':
                                output = ROOT / 'docs/audits/2026-10-28-screenshots/leader-accounts-after.png'
                                output.parent.mkdir(parents=True, exist_ok=True)
                                page.evaluate('refreshInviteList()')
                                page.screenshot(path=str(output), full_page=True,
                                                style='#moreSheet:not(.open), .dash-bottomnav { display: none !important; }')
                            check_lifecycle(page)
                            check_inactive_recovery(page)
                    finally:
                        page.close()
            finally:
                browser.close()
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)
    print('ok: leader activation UI, permissions, failures, inactive data clearing, retry and billing')


if __name__ == '__main__':
    main()

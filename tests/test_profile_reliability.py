"""Browser regressions for profile saves, drafts, and modal isolation.

Runs the real SPA with local synthetic fixtures; needs no database or Clerk.
Run with python3 tests/test_profile_reliability.py (Playwright + Chromium).
"""
import functools
import json
from http.server import ThreadingHTTPServer
import threading

from test_mobile_usability import ROOT, Handler, fixtures


def open_profile(page):
    page.evaluate('openSoldierPage(2)')
    page.wait_for_function('profileBaseline.length > 0')


def edit_profile(page, email='draft@example.invalid'):
    page.locator('#pf_email').fill(email)
    assert page.evaluate('profileDrafts.has(2)'), 'editing must retain a draft'


def pending_save(page):
    page.evaluate("""() => {
      window.__putCalls = [];
      window.api = (method, path, body) => {
        if (method === 'PUT' && path.endsWith('/profile')) {
          window.__putCalls.push({path, body});
          return new Promise(resolve => { window.__resolveSave = resolve; });
        }
        return window.__fixtureApi(method, path, body);
      };
      window.__savePromise = saveProfile();
    }""")
    page.wait_for_function('window.__putCalls.length === 1')


def finish_save(page):
    page.evaluate('window.__resolveSave(window.__putCalls[0].body)')
    return page.evaluate('window.__savePromise')


def test_null_save(page):
    open_profile(page)
    edit_profile(page)
    page.evaluate("""() => {
      window.api = (method, path, body) => method === 'PUT' && path.endsWith('/profile')
        ? Promise.resolve(null) : window.__fixtureApi(method, path, body);
    }""")
    assert page.evaluate('saveProfile()') is False, 'null response is not a saved profile'
    assert 'not saved' in page.locator('#apiToast').inner_text().lower()
    assert 'app-toast-success' not in page.locator('#apiToast').get_attribute('class')
    assert page.locator('#pf_email').input_value() == 'draft@example.invalid'
    assert page.evaluate('profileDrafts.has(2) && !profileSaving')
    assert not page.locator('#soldierView .soldier-hero-actions .dash-btn-primary').is_disabled()


def test_double_save(page):
    open_profile(page)
    edit_profile(page)
    pending_save(page)
    assert page.locator('#soldierView .soldier-hero-actions .dash-btn-primary').is_disabled()
    assert page.evaluate('saveProfile()') is False
    assert page.evaluate('window.__putCalls.length') == 1
    assert finish_save(page) is True
    assert not page.evaluate('profileDrafts.has(2)')


def test_edits_during_save_remain_dirty(page):
    open_profile(page)
    edit_profile(page, 'submitted@example.invalid')
    pending_save(page)
    edit_profile(page, 'newer@example.invalid')
    assert finish_save(page) is True
    assert page.locator('#pf_email').input_value() == 'newer@example.invalid'
    assert page.evaluate('profileDrafts.has(2)'), 'later edits were incorrectly marked saved'
    assert 'Unsaved changes' in page.locator('#profileSaveStatus').inner_text()
    assert page.evaluate('window.__putCalls[0].body.email') == 'submitted@example.invalid'


def ask_to_leave(page):
    page.locator('#soldierView .soldier-back').click()
    page.wait_for_selector('#profileDraftModal.active')


def test_keep_editing(page):
    open_profile(page)
    edit_profile(page)
    ask_to_leave(page)
    page.get_by_role('button', name='Keep editing', exact=True).click()
    assert page.evaluate("document.body.classList.contains('soldier-active')")
    assert not page.locator('#profileDraftModal').is_visible()
    assert page.locator('#pf_email').input_value() == 'draft@example.invalid'
    assert page.evaluate('profileDrafts.has(2)')


def test_discard(page):
    open_profile(page)
    original = page.locator('#pf_email').input_value()
    edit_profile(page)
    ask_to_leave(page)
    page.get_by_role('button', name='Discard', exact=True).click()
    assert not page.evaluate("document.body.classList.contains('soldier-active')")
    assert not page.evaluate('profileDrafts.has(2)')
    open_profile(page)
    assert page.locator('#pf_email').input_value() == original


def test_save_and_continue(page):
    open_profile(page)
    edit_profile(page)
    page.evaluate("""() => {
      window.__putCalls = [];
      window.api = (method, path, body) => {
        if (method === 'PUT' && path.endsWith('/profile')) {
          window.__putCalls.push({path, body});
          return Promise.resolve(body);
        }
        return window.__fixtureApi(method, path, body);
      };
    }""")
    ask_to_leave(page)
    page.get_by_role('button', name='Save and continue', exact=True).click()
    page.wait_for_function("!document.body.classList.contains('soldier-active')")
    assert page.evaluate('window.__putCalls.length') == 1
    assert page.evaluate('window.__putCalls[0].body.email') == 'draft@example.invalid'
    assert not page.evaluate('profileDrafts.has(2)')


def test_failed_save_and_continue_stays(page):
    open_profile(page)
    edit_profile(page)
    page.evaluate("""() => {
      window.api = (method, path, body) => method === 'PUT' && path.endsWith('/profile')
        ? Promise.resolve(null) : window.__fixtureApi(method, path, body);
    }""")
    ask_to_leave(page)
    page.get_by_role('button', name='Save and continue', exact=True).click()
    page.wait_for_function('!profileSaving')
    assert page.evaluate("document.body.classList.contains('soldier-active')")
    assert page.locator('#profileDraftModal').is_visible()
    assert page.evaluate('profileDrafts.has(2)')


def test_draft_reopens(page):
    open_profile(page)
    edit_profile(page)
    page.evaluate('closeSoldierPage()')
    open_profile(page)
    assert page.locator('#pf_email').input_value() == 'draft@example.invalid'
    assert page.evaluate('profileDrafts.has(2)')
    assert 'Unsaved changes' in page.locator('#profileSaveStatus').inner_text()


def test_discarded_legacy_address_draft_reopens(page):
    page.evaluate("""() => {
      window.api = async (method, path, body) => {
        const result = await window.__fixtureApi(method, path, body);
        return method === 'GET' && path.endsWith('/profile')
          ? {...result, address: 'Previously recorded address'} : result;
      };
    }""")
    open_profile(page)
    assert page.locator('#addressLegacy').is_visible()
    page.locator('#addressLegacy button').click()
    assert not page.locator('#addressLegacy').is_visible()
    page.evaluate('closeSoldierPage()')
    open_profile(page)
    assert page.evaluate('addressLegacyDiscarded'), 'the draft lost the discard choice'
    assert not page.locator('#addressLegacy').is_visible(), 'reopening restored a discarded address warning'


def test_modal_tab_trap(page):
    page.evaluate('openAddModal()')
    page.wait_for_function('isolatedModal && isolatedModal.id === "addModal"')
    page.locator('#addModal button').last.focus()
    page.keyboard.press('Tab')
    assert page.evaluate('!!document.activeElement.closest("#addModal")'), 'Tab escaped Add Person'
    assert page.evaluate('document.activeElement === document.querySelector("#addModal input, #addModal select, #addModal button")')
    page.keyboard.press('Shift+Tab')
    assert page.evaluate('document.activeElement === [...document.querySelectorAll("#addModal button")].at(-1)')
    page.keyboard.press('Escape')
    assert not page.locator('#addModal').is_visible()
    assert not page.evaluate('document.getElementById("platoonScreen").inert')


def test_more_escape_and_inert(page):
    more = page.locator('.dash-bottomnav button').last
    more.click()
    page.wait_for_function('isolatedModal && isolatedModal.id === "moreSheet"')
    assert page.evaluate('document.getElementById("moreSheet").contains(document.activeElement)')
    assert page.evaluate('document.querySelector(".dash-header").inert'), 'roster controls remain active behind More'
    page.keyboard.press('Escape')
    page.wait_for_function('!document.getElementById("moreSheet").classList.contains("open")')
    assert page.evaluate('document.getElementById("moreSheet").inert')
    assert not page.evaluate('document.querySelector(".dash-header").inert')
    assert more.evaluate('(el) => el === document.activeElement'), 'More did not return focus'


def test_guarded_present_keeps_new_absence(page):
    result = page.evaluate("""async () => {
      const person = personnel.find(p => p.id === 2);
      let sent;
      window.api = async (method, path, body) => {
        sent = {method, path, body};
        return {id: person.id, status: 'leave', from_date: getTodayStr(),
          to_date: getTodayStr(), present_date: '', notes: 'New absence', skipped: true};
      };
      const res = await guardedPresent(person);
      return {sent, skipped: res.skipped, status: person.status, present: person.present_date};
    }""")
    assert result['sent']['body']['only_if_unaccounted'] is True
    assert result['sent']['method'] == 'PUT'
    assert result['skipped'] and result['status'] == 'leave' and result['present'] == ''


def test_focus_refresh_preserves_open_profile(page):
    open_profile(page)
    edit_profile(page)
    result = page.evaluate("""async () => {
      let calls = 0;
      window.api = (...args) => { calls++; return window.__fixtureApi(...args); };
      await refreshRosterOnReturn();
      return calls;
    }""")
    assert result == 0, 'returning to the app refreshed an open profile'
    assert page.locator('#pf_email').input_value() == 'draft@example.invalid'
    assert page.evaluate('profileDrafts.has(2)')


def test_slow_initial_profile_read(page):
    page.evaluate("""() => {
      window.api = (method, path, body) => method === 'GET' && path.endsWith('/profile')
        ? new Promise(resolve => { window.__resolveProfile = resolve; })
        : window.__fixtureApi(method, path, body);
      openSoldierPage(2);
    }""")
    assert page.locator('#pf_email').is_disabled(), 'typing during a pending read can be overwritten'
    assert page.locator('.profile-savebar [data-profile-save]').is_disabled()
    assert page.evaluate('saveProfile()') is False
    page.evaluate('window.__resolveProfile(window.__fixture.profile)')
    page.wait_for_function('profileBaseline.length > 0')
    assert not page.locator('#pf_email').is_disabled()
    edit_profile(page)


def test_failed_initial_read_cannot_save(page):
    page.evaluate("""() => {
      window.__profilePuts = 0;
      window.api = (method, path, body) => {
        if (path.endsWith('/profile')) {
          if (method === 'PUT') window.__profilePuts++;
          return Promise.resolve(null);
        }
        return window.__fixtureApi(method, path, body);
      };
      openSoldierPage(2);
    }""")
    page.wait_for_function('document.querySelector("[data-profile-state]").textContent.includes("Could not load")')
    assert page.locator('#pf_email').is_disabled()
    assert page.evaluate('saveProfile()') is False
    assert page.evaluate('window.__profilePuts') == 0, 'empty unloaded form was submitted'


def test_failed_reopen_keeps_draft_recoverable(page):
    open_profile(page)
    edit_profile(page)
    page.evaluate("""() => {
      closeSoldierPage();
      window.api = (method, path, body) => method === 'GET' && path.endsWith('/profile')
        ? Promise.resolve(null) : window.__fixtureApi(method, path, body);
      openSoldierPage(2);
    }""")
    page.wait_for_function('document.querySelector("[data-profile-state]").textContent.includes("Could not load")')
    assert page.evaluate('profileDrafts.has(2)'), 'failed read erased the previous draft'
    page.evaluate('closeSoldierPage(); window.api = window.__fixtureApi')
    open_profile(page)
    assert page.locator('#pf_email').input_value() == 'draft@example.invalid'


def test_logout_clears_drafts(page):
    open_profile(page)
    edit_profile(page)
    page.route('**/api/logout', lambda route: route.fulfill(json={'ok': True}))
    page.evaluate('showLoginScreen = window.__realShowLoginScreen')
    page.evaluate('doLogout()')
    assert page.evaluate('profileDrafts.size') == 0
    assert page.evaluate('profileBaseline') == ''
    assert page.locator('#loginScreen').is_visible()


def test_phone_save_footer_stays_reachable(page):
    open_profile(page)
    edit_profile(page)
    page.evaluate('window.scrollTo(0, document.documentElement.scrollHeight)')
    footer = page.locator('.profile-savebar')
    assert footer.is_visible()
    bounds = footer.bounding_box()
    nav = page.locator('.dash-bottomnav').bounding_box()
    assert 0 <= bounds['y'] < 844 and bounds['y'] + bounds['height'] <= nav['y'] + 1
    assert 'Unsaved changes' in footer.inner_text()
    assert footer.locator('button').bounding_box()['height'] >= 44
    pending_save(page)
    assert footer.locator('button').is_disabled()
    assert finish_save(page) is True
    assert not footer.locator('button').is_disabled()
    assert 'All changes saved' in footer.inner_text()


def test_nested_date_picker_keeps_dialog_boundary(page):
    page.evaluate('openTdyLeave(personnel.findIndex(p => p.id === 2))')
    parent = page.locator('#tdyLeaveModal')
    page.wait_for_function('isolatedModal && isolatedModal.id === "tdyLeaveModal"')
    page.locator('#awayFromInput').dispatch_event('mousedown')
    page.wait_for_selector('#tdyLeaveModal [role="dialog"] .rp-pop')
    assert page.evaluate('rp && !rp.pop.closest("[inert]")'), 'nested date picker is inert'
    day = page.locator('#tdyLeaveModal .rp-pop [data-d]').first
    selected = day.get_attribute('data-d')
    day.click()
    assert page.locator('#awayFromInput').input_value() == selected, 'date selection did not reach From'
    assert parent.is_visible()
    assert page.locator('#tdyLeaveModal .rp-pop').is_visible()
    page.keyboard.press('Escape')
    assert page.evaluate('rp === null'), 'Escape did not close the picker'
    assert parent.is_visible(), 'Escape closed the parent dialog as well as the picker'
    page.locator('#tdyLeaveModal .modal-buttons button').last.focus()
    page.keyboard.press('Tab')
    assert page.evaluate('!!document.activeElement.closest("#tdyLeaveModal")'), 'Tab escaped the parent dialog'


def main():
    try:
        from playwright.sync_api import sync_playwright, Error
    except ImportError:
        print('SKIPPED (not ok): Playwright is required for profile reliability checks')
        return
    env, data = fixtures()
    httpd = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(Handler, directory=str(ROOT)))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    base = f'http://127.0.0.1:{httpd.server_port}'
    failures = []
    cases = [value for name, value in globals().items() if name.startswith('test_') and callable(value)]
    try:
        with sync_playwright() as pw:
            try:
                browser = pw.chromium.launch()
            except Error:
                print('SKIPPED (not ok): Chromium is required for profile reliability checks')
                return
            try:
                for test in cases:
                    page = browser.new_page(viewport={'width': 390, 'height': 844})
                    page.set_default_timeout(5000)
                    page.route('**/*', lambda route: route.continue_()
                               if route.request.url.startswith(base + '/') else route.abort())
                    page.route('**/api/auth/config', lambda route: route.fulfill(
                        content_type='application/json', body=json.dumps({'enabled': False})))
                    try:
                        page.goto(base)
                        page.evaluate(env['INIT_JS'], data)
                        page.evaluate(env['ENTER_UNIT_JS'], 1)
                        page.evaluate('window.__fixtureApi = window.api')
                        test(page)
                        print('PASS', test.__name__)
                    except Exception as exc:
                        failures.append((test.__name__, str(exc)))
                        print('FAIL', test.__name__, str(exc))
                    finally:
                        page.close()
            finally:
                browser.close()
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)
    assert not failures, f'{len(failures)} profile reliability checks failed: {failures}'
    print(f'ok: {len(cases)} profile reliability and modal checks')


if __name__ == '__main__':
    main()

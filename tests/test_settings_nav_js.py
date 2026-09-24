"""The settings nav, the top bar's search and the settings routes, lifted out
of index.html and run under node.

Run with: python tests/test_settings_nav_js.py

Settings used to be two different menus — a hamburger on the home screen and
a page inside a unit — and are now one area with one nav. These are the pure
parts of that: which entries the nav offers (the operator's only when the flag
says so), which one is lit, what the search box finds, and that a soldier's
name typed by a leader never becomes markup or lands inside an onclick.
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')

HOSTILE = '"><img src=x onerror=alert(1)>'

PEOPLE = [
    {'id': 1, 'rank': 'SGT', 'first': 'Ray', 'last': 'Fixtureton', 'status': 'present'},
    {'id': 2, 'rank': 'SPC', 'first': 'Nia', 'last': 'Placeholder', 'status': 'tdy'},
    {'id': 3, 'rank': 'PFC', 'first': HOSTILE, 'last': HOSTILE, 'status': HOSTILE},
] + [{'id': 10 + i, 'rank': 'PVT', 'first': f'Many{i}', 'last': 'Sameton', 'status': 'leave'} for i in range(9)]
UNITS = [
    {'id': 1, 'parent_id': None, 'kind': 'company', 'name': 'Headhunter Company', 'slug': 'headhunter-company'},
    {'id': 2, 'parent_id': 1, 'kind': 'platoon', 'name': '1st Platoon', 'slug': '1st-platoon'},
    {'id': 3, 'parent_id': 1, 'kind': 'platoon', 'name': HOSTILE, 'slug': 'x'},
]

DRIVER = r'''
const PEOPLE = ''' + json.dumps(PEOPLE) + r''';
const UNITS = ''' + json.dumps(UNITS) + r''';
const HOSTILE = ''' + json.dumps(HOSTILE) + r''';
const out = {};
out.navUser = settingsNavHtml('billing', { admin: false });
out.navAdmin = settingsNavHtml('general', { admin: true });
out.tabsUser = settingsTabsHtml('units', { admin: false });
out.tabsAdmin = settingsTabsHtml('schools', { admin: true });
const pages = SEARCH_PAGES.map(p => ({ label: p.label, hint: p.hint, words: p.words }));
const strip = rs => rs.map(r => [r.group, r.kind, r.id, r.label]);
out.searchEmpty = globalSearchResults('   ', PEOPLE, UNITS, pages);
out.searchRay = strip(globalSearchResults('ray', PEOPLE, UNITS, pages));
out.searchByRank = strip(globalSearchResults('spc plac', PEOPLE, UNITS, pages));
out.searchPlatoon = strip(globalSearchResults('platoon', PEOPLE, UNITS, pages));
out.searchDark = strip(globalSearchResults('dark', PEOPLE, UNITS, pages));
out.searchInvoice = strip(globalSearchResults('INVOICE', PEOPLE, UNITS, pages));
out.searchMany = globalSearchResults('sameton', PEOPLE, UNITS, pages).length;
out.searchHomeScreen = strip(globalSearchResults('fixtureton', [], UNITS, pages));
const hostile = globalSearchResults('img', PEOPLE, UNITS, pages);
out.hostileHtml = globalSearchHtml(hostile, 0);
out.hostileCount = hostile.length;
out.noneHtml = globalSearchHtml([], 0);
out.initials = [userInitials({ full_name: 'SFC Ada Fixtureton' }), userInitials({ username: 'ray.leader' }),
                userInitials({ email: 'x@example.invalid' }), userInitials({}), userInitials(null)];
// parseAppRoute against a stubbed address bar and tree.
const routes = {};
for (const path of ['/headhunter-company/settings', '/headhunter-company/settings/billing',
                    '/headhunter-company/settings/profile', '/headhunter-company/billing',
                    '/headhunter-company/units', '/admin', '/nowhere/settings']) {
  window.location.pathname = path;
  const r = parseAppRoute();
  routes[path] = [r.unit ? r.unit.slug : null, r.section, r.sub === undefined ? '-' : r.sub];
}
out.routes = routes;
console.log(JSON.stringify(out));
'''

PRELUDE = r'''
const window = { location: { pathname: '/' } };
let units = ''' + json.dumps(UNITS) + r''';
'''


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html — was it renamed or removed?'
    return m.group(0)


def render():
    node = shutil.which('node')
    assert node, 'node is required (it ships with the CI image)'
    src = open(INDEX, encoding='utf-8').read()
    js = '\n'.join([
        PRELUDE,
        extract(src, r'function escapeHtml\(str\) \{.*?\n\}', 'escapeHtml()'),
        extract(src, r'function unitBySlug\(slug\) \{.*?\n\}', 'unitBySlug()'),
        extract(src, r'function kindLabel\(kind\) \{.*?\n\}', 'kindLabel()'),
        extract(src, r'const SETTINGS_NAV = \[.*?\n\];', 'SETTINGS_NAV'),
        extract(src, r'function settingsNavItems\(opts\) \{.*?\n\}', 'settingsNavItems()'),
        extract(src, r'function settingsNavHtml\(active, opts\) \{.*?\n\}', 'settingsNavHtml()'),
        extract(src, r'function settingsTabsHtml\(active, opts\) \{.*?\n\}', 'settingsTabsHtml()'),
        extract(src, r'const SEARCH_PAGES = \[.*?\n\];', 'SEARCH_PAGES'),
        extract(src, r'const SEARCH_LIMIT = \{.*?\};', 'SEARCH_LIMIT'),
        extract(src, r'function globalSearchResults\(.*?\n\}', 'globalSearchResults()'),
        extract(src, r'const STATUS_LABELS_SEARCH = \{.*?\};', 'STATUS_LABELS_SEARCH'),
        extract(src, r'function globalSearchHtml\(.*?\n\}', 'globalSearchHtml()'),
        extract(src, r'function userInitials\(u\) \{.*?\n\}', 'userInitials()'),
        extract(src, r'function parseAppRoute\(\) \{.*?\n\}', 'parseAppRoute()'),
        DRIVER,
    ])
    path = os.path.join(tempfile.mkdtemp(), 'settings_nav.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(js)
    proc = subprocess.run([node, path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def keys(html):
    return re.findall(r'data-settings="(\w+)"', html)


def test_the_nav_has_both_halves_and_one_lit_entry(out):
    nav = out['navUser']
    assert 'Personal' in nav and 'Organization' in nav, 'the nav is not split into its two halves'
    assert keys(nav) == ['profile', 'preferences', 'billing', 'general', 'people', 'units', 'schools',
                         'locations', 'data', 'audit'], keys(nav)
    assert nav.count(' active"') == 1 and 'data-settings="billing" aria-current="page"' in nav, nav
    assert 'dashGoAccountability()' in nav, 'there is no way back out of settings'
    assert "onclick=\"openUnits()\"" in nav and "onclick=\"openAuditLog()\"" in nav, \
        'full pages keep their own open*()'
    assert "onclick=\"openBillingPage()\"" in nav


def test_the_operator_entry_needs_the_flag(out):
    assert 'admin' not in keys(out['navUser']) and 'Platform' not in out['navUser']
    assert 'admin' not in keys(out['tabsUser'])
    assert keys(out['navAdmin'])[-1] == 'admin' and 'Platform' in out['navAdmin']
    assert 'openAdmin()' in out['navAdmin'] and 'admin' in keys(out['tabsAdmin'])


def test_the_tabs_are_the_same_nav(out):
    assert keys(out['tabsUser']) == keys(out['navUser']), 'the phone tabs and the sidebar disagree'
    assert 'aria-selected="true" data-settings="units"' in out['tabsUser'], out['tabsUser']
    assert out['tabsUser'].count('aria-selected="true"') == 1


def test_search_finds_people_units_and_pages(out):
    assert out['searchEmpty'] == []
    assert out['searchRay'] == [['People', 'person', 1, 'SGT Fixtureton, Ray']], out['searchRay']
    assert ['People', 'person', 2, 'SPC Placeholder, Nia'] in out['searchByRank'], out['searchByRank']
    assert [r[:3] for r in out['searchPlatoon'] if r[0] == 'Units'] == [['Units', 'unit', 2]], out['searchPlatoon']
    assert any(r[3] == 'Units' for r in out['searchPlatoon']), 'the words on a page count too ("platoon" → Units)'
    assert [r[3] for r in out['searchDark']] == ['Preferences'], out['searchDark']
    assert [r[3] for r in out['searchInvoice']] == ['Billing'], 'search is case-insensitive'
    assert out['searchMany'] == 6, 'people are capped so pages and units still show'
    assert out['searchHomeScreen'] == [], 'no roster on the home screen means no people offered'


def test_a_hostile_name_never_becomes_markup(out):
    h = out['hostileHtml']
    assert out['hostileCount'] >= 2, 'the fixture should match both a person and a unit'
    assert '<img' not in h and '&lt;img' in h, 'a name reached the page as markup'
    assert re.findall(r'onclick="([^"]*)"', h) == [f'runSearchResult({i})' for i in range(out['hostileCount'])], \
        'a result runs by index; nothing typed by a leader lands inside an onclick'
    assert 'No matches' in out['noneHtml']


def test_initials(out):
    assert out['initials'] == ['SF', 'RL', 'X', '?', '?'], out['initials']


def test_settings_routes(out):
    r = out['routes']
    assert r['/headhunter-company/settings'] == ['headhunter-company', 'settings', None]
    assert r['/headhunter-company/settings/billing'] == ['headhunter-company', 'settings', 'billing']
    assert r['/headhunter-company/settings/profile'] == ['headhunter-company', 'settings', 'profile']
    assert r['/headhunter-company/billing'] == ['headhunter-company', 'settings', 'billing'], \
        'the short /billing path is where Stripe used to send people back to'
    assert r['/headhunter-company/units'] == ['headhunter-company', 'units', '-']
    assert r['/admin'] == [None, 'admin', '-'] and r['/nowhere/settings'] == [None, 'home', '-']


def test_the_wiring(src):
    """The parts that are not pure: the routes open the section they name,
    and every screen change puts the one top bar where it belongs."""
    for name, pattern in (('routeAfterLogin()', r'function routeAfterLogin\(\) \{.*?\n\}'),
                          ('the popstate handler', r"window\.addEventListener\('popstate'.*?\n\}\);")):
        body = extract(src, pattern, name)
        assert 'openSettings(false, route.sub)' in body, f'{name} ignores the settings section in the URL'
    show = extract(src, r'function showAppScreen\(screen\) \{.*?\n\}', 'showAppScreen()')
    assert 'placeTopbar(screen)' in show, 'showAppScreen() does not move the top bar'
    assert "classList.remove('settings-mode')" in show, 'leaving the dashboard leaves the settings nav up'
    assert src.count('id="appTopbar"') == 1, 'there must be exactly one top bar'
    assert 'hamburger' not in re.sub(r'<style>.*?</style>', '', src, flags=re.S), \
        'the home-screen hamburger menu is back'
    open_settings = extract(src, r'function openSettings\(push = true, section = null\) \{.*?\n\}', 'openSettings()')
    assert 'loadBillingDetails()' in open_settings, 'the Billing section never asks for its details'
    for fn in ('goToSettings', 'goToPage'):
        body = extract(src, rf'(async )?function {fn}\(\w+\) \{{.*?\n\}}', f'{fn}()')
        assert 'unitById(currentUser.unit_id)' in body, f'{fn}() cannot open a unit page from the home screen'


def main():
    out = render()
    src = open(INDEX, encoding='utf-8').read()
    test_the_nav_has_both_halves_and_one_lit_entry(out)
    test_the_operator_entry_needs_the_flag(out)
    test_the_tabs_are_the_same_nav(out)
    test_search_finds_people_units_and_pages(out)
    test_a_hostile_name_never_becomes_markup(out)
    test_initials(out)
    test_settings_routes(out)
    test_the_wiring(src)
    print('ok')


if __name__ == '__main__':
    main()

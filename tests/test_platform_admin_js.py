"""The /admin page's markup, lifted out of index.html and run under node.

Run with: python tests/test_platform_admin_js.py

adminOverviewHtml() is pure, so the one thing that matters about it — that a
string which came off the wire never becomes markup — is testable without a
browser. The payload crosses tenants, so the organisation names and user
emails in it come from people this operator has never met.

Also checks the two entry points are gated on currentUser.platform_admin, and
that the numbers go through the page's formatter rather than being pasted in
raw.
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')

HOSTILE_NAME = '<img src=x onerror=alert(1)> "Ghost" Co'
HOSTILE_EMAIL = '"><script>alert(2)</script>@example.com'

PAYLOAD = {
    # The stamp is server-made today, but it lands on the page the same way
    # every other server string does, so it is fixtured the same way too.
    'generated_at': '2026-09-17 06:30:00 <img src=x onerror=alert(3)>',
    'totals': {'organisations': 2, 'unit_count': 1234, 'personnel_count': 9876,
               'user_count': 12, 'unattached_users': 3, 'pending_invites': 1,
               'database_bytes': 86423219},
    'organisations': [
        {'org_id': 1, 'org_name': 'Alpha Co', 'org_slug': 'alpha-co', 'org_kind': 'company',
         'created_stamp': '2026-01-02 03:04:05', 'unit_count': 4, 'personnel_count': 12345,
         'user_count': 5, 'owner_emails': 'boss@example.com', 'pending_invites': 2,
         'has_logo': True, 'last_activity': '2026-09-17 06:00:00', 'audit_7d': 17},
        {'org_id': 2, 'org_name': HOSTILE_NAME, 'org_slug': 'ghost', 'org_kind': 'platoon',
         'created_stamp': '', 'unit_count': 1, 'personnel_count': 2,
         'user_count': 1, 'owner_emails': None, 'pending_invites': 0,
         'has_logo': False, 'last_activity': None, 'audit_7d': 0},
    ],
    'recent_users': [
        {'user_id': 9, 'email': HOSTILE_EMAIL, 'full_name': HOSTILE_NAME, 'role': 'leader',
         'org_name': None, 'signed_in': True},
        {'user_id': 8, 'email': 'boss@example.com', 'full_name': 'Ada B', 'role': 'owner',
         'org_name': 'Alpha Co', 'signed_in': False},
    ],
}

EMPTY = {'generated_at': '2026-09-17 06:30:00', 'totals': {},
         'organisations': [], 'recent_users': []}

DRIVER = r'''
const PAYLOAD = ''' + json.dumps(PAYLOAD) + r''';
const EMPTY = ''' + json.dumps(EMPTY) + r''';
const orgSort = { key: 'personnel_count', dir: -1 };
const userSort = { key: 'user_id', dir: -1 };
console.log(JSON.stringify({
  page: adminOverviewHtml(PAYLOAD, orgSort, userSort),
  empty: adminOverviewHtml(EMPTY, orgSort, userSort),
  loading: adminOverviewHtml(null, orgSort, userSort),
  byName: adminOverviewHtml(PAYLOAD, { key: 'org_name', dir: 1 }, userSort),
  bytes: [adminBytes(0), adminBytes(999), adminBytes(86423219), adminBytes(5 * 1024 ** 3)],
  nums: [adminNum(0), adminNum(1234), adminNum(9876543)],
  cleared: (() => {
    adminData = { organisations: [{ org_name: 'Alpha Co', owner_emails: 'boss@example.com' }] };
    adminScreenEl.innerHTML = '<td>Alpha Co</td><td>boss@example.com</td>';
    clearPlatformAdmin();
    return { data: adminData, html: adminScreenEl.innerHTML, removed: removedClasses };
  })(),
}));
'''

# The three things clearPlatformAdmin() touches, stubbed so it can run headless.
FAKE_DOM = r'''
let adminData = null;
const adminScreenEl = { innerHTML: '' };
const removedClasses = [];
const document = {
  getElementById: (id) => (id === 'adminScreen' ? adminScreenEl : null),
  body: { classList: { remove: (c) => removedClasses.push(c) } },
};
'''


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html — was it renamed or removed?'
    return m.group(0)


def render(src, node):
    js = '\n'.join([
        FAKE_DOM,
        extract(src, r'function escapeHtml\(str\) \{.*?\n\}', 'escapeHtml()'),
        extract(src, r'function sortHeaders\(.*?\n\}', 'sortHeaders()'),
        extract(src, r'function sortRows\(.*?\n\}', 'sortRows()'),
        extract(src, r'const ADMIN_ORG_COLUMNS = \[.*?\n\];', 'ADMIN_ORG_COLUMNS'),
        extract(src, r'const ADMIN_USER_COLUMNS = \[.*?\n\];', 'ADMIN_USER_COLUMNS'),
        extract(src, r'function adminNum\(.*?\n\}', 'adminNum()'),
        extract(src, r'function adminBytes\(.*?\n\}', 'adminBytes()'),
        extract(src, r'function adminCell\(.*?\n\}', 'adminCell()'),
        extract(src, r'function adminSortValue\(.*?\n\}', 'adminSortValue()'),
        extract(src, r'function adminOverviewHtml\(.*?\n\}', 'adminOverviewHtml()'),
        extract(src, r'function clearPlatformAdmin\(\) \{.*?\n\}', 'clearPlatformAdmin()'),
        DRIVER,
    ])
    path = os.path.join(tempfile.mkdtemp(), 'admin.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(js)
    proc = subprocess.run([node, path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def test_a_hostile_organisation_name_is_never_markup(out):
    html = out['page']
    assert '<img src=x' not in html, 'an organisation name reached the page as markup'
    assert '<script>' not in html, 'a user email reached the page as markup'
    assert '&lt;img src=x onerror=alert(1)&gt; &quot;Ghost&quot; Co' in html, \
        'the organisation name is not escaped the way escapeHtml() escapes it'
    assert '&lt;script&gt;alert(2)&lt;/script&gt;' in html, 'the email is not escaped'
    # The only tags on this page are the ones the function writes itself.
    tags = set(re.findall(r'<(/?[a-zA-Z][\w-]*)', html))
    assert tags <= {'div', '/div', 'span', '/span', 'section', '/section', 'h1', '/h1',
                    'h2', '/h2', 'button', '/button', 'table', '/table', 'thead', '/thead',
                    'tbody', '/tbody', 'tr', '/tr', 'th', '/th', 'td', '/td'}, \
        f'a server string opened a tag of its own: {sorted(tags)}'


def test_no_handler_carries_anything_but_a_literal(out):
    for key in ('page', 'empty', 'loading'):
        for call in re.findall(r'onclick="([^"]*)"', out[key]):
            assert re.fullmatch(r"(closeAdmin|refreshAdmin)\(\)|sortAdmin(Orgs|Users)\('\w+'\)", call), \
                f'{key}: an inline handler carries more than a literal: {call!r}'


def test_numbers_go_through_the_page_formatter(out):
    html = out['page']
    assert '>1,234<' in html, 'the unit total is not grouped by the page formatter'
    assert '>9,876<' in html, 'the personnel total is not grouped'
    # Not just the totals strip: a number inside a TABLE CELL goes through the
    # same formatter, which is a separate code path (adminCell).
    assert '>12,345<' in html, 'a table cell prints its number ungrouped'
    assert '>12345<' not in html, 'a table cell prints its number ungrouped'
    assert '86423219' not in html, 'the database size is printed as raw bytes'
    assert '82.4 MB' in html, f'the database size is not human-readable: {html[:400]}'
    assert out['nums'] == ['0', '1,234', '9,876,543'], out['nums']
    assert out['bytes'] == ['0 B', '999 B', '82.4 MB', '5.0 GB'], out['bytes']


def test_missing_values_read_as_missing_not_as_null(out):
    html = out['page']
    assert 'null' not in html and 'undefined' not in html, \
        f'a missing value printed itself: {html}'
    assert html.count('—') >= 2, 'empty cells are not marked as empty'
    assert '>yes<' in html and '>no<' in html, 'has_logo / signed_in are not rendered'


def test_both_tables_sort_through_the_shared_helpers(out):
    def org_names(html):
        return re.findall(r'<td class="admin-strong">([^<]*)', html)
    by_size = org_names(out['page'])
    by_name = org_names(out['byName'])
    assert by_size[0] == 'Alpha Co', by_size
    assert by_size != by_name, 'changing the sort column changed nothing'
    assert by_name[0].startswith('&lt;img'), f'name-ascending puts the escaped name first: {by_name}'
    # Sorted headers are marked by the shared helper, not by hand.
    assert 'class="sortable sorted"' in out['page'], 'the sorted column is not marked'
    assert out['page'].count('sort-caret') == 14, 'every column must carry a caret'


def test_the_empty_and_loading_states_say_so(out):
    assert 'No organisations yet.' in out['empty'], out['empty'][:300]
    assert 'No users yet.' in out['empty']
    assert 'Loading' in out['loading'] and '<table' not in out['loading'], out['loading']


def test_the_menu_entries_are_gated_on_the_flag(src):
    """Both entry points must test currentUser.platform_admin, and the
    hamburger item must start hidden — a default-visible item shows itself to
    everyone for the moment before loadHome() runs."""
    item = extract(src, r'<button id="adminMenuItem".*?</button>', 'the Admin menu item')
    assert 'style="display:none"' in item, f'the Admin menu item starts visible: {item}'
    home = extract(src, r'async function loadHome\(\) \{.*?\n\}', 'loadHome()')
    assert re.search(r"adminMenuItem'\)\.style\.display\s*=\s*\n?\s*currentUser\.platform_admin", home), \
        'loadHome() does not gate the Admin menu item on platform_admin'

    settings = extract(src, r'function renderSettings\(\) \{.*?\n\}', 'renderSettings()')
    assert 'openAdmin()' in settings, 'the Settings Account card has no Admin row'
    assert re.search(r'currentUser\.platform_admin\s*\n?\s*\?\s*settingsNavRow\(\'Admin\'', settings), \
        'the Settings Admin row is not gated on platform_admin'

    for name, body in (
            ('routeAfterLogin()', extract(src, r'function routeAfterLogin\(\) \{.*?\n\}',
                                          'routeAfterLogin()')),
            ('the popstate handler', extract(src, r"window\.addEventListener\('popstate'.*?\n\}\);",
                                             'the popstate handler'))):
        assert "route.section === 'admin'" in body, f'{name} does not handle /admin'
        assert body.index('currentUser.platform_admin') < body.index('openAdmin('), \
            f'{name} opens the dashboard before it checks the flag'
        # A non-admin is sent home; the address bar has to agree, or the next
        # reload tries /admin again and Back walks straight into it.
        assert 'replaceState' in body, f'{name} leaves /admin in the address bar'

    parse = extract(src, r'function parseAppRoute\(\) \{.*?\n\}', 'parseAppRoute()')
    admin_at = parse.index("parts[0] === 'admin'")
    assert admin_at < parse.index('unitBySlug('), \
        '/admin is resolved after the slug lookup, so a unit slugged "admin" shadows it'


def test_signing_out_takes_every_tenants_data_with_it(out, src):
    """The payload names every organisation on the instance and its owners'
    email addresses. Hiding the screen leaves all of it in the DOM and in a
    global for whoever signs in next on a shared machine, so the sign-out path
    has to empty both."""
    cleared = out['cleared']
    assert cleared['data'] is None, f'the payload survived sign-out in a global: {cleared["data"]}'
    assert cleared['html'] == '', f'the rendered page survived sign-out: {cleared["html"]!r}'
    assert 'admin-active' in cleared['removed'], cleared['removed']

    # showLoginScreen() is the funnel: the menu item, the session timeout and
    # api()'s 401 handler all end there.
    login = extract(src, r'function showLoginScreen\(view\) \{.*?\n\}', 'showLoginScreen()')
    assert 'clearPlatformAdmin()' in login, \
        'showLoginScreen() does not clear the platform dashboard'
    for caller in ('doLogout', 'api'):
        body = extract(src, r'(?:async )?function ' + caller + r'\(.*?\n\}', caller)
        assert 'showLoginScreen(' in body, \
            f'{caller}() no longer goes through showLoginScreen() — find the new funnel'


def test_the_page_never_reads_the_current_unit(src):
    """It is not unit-scoped: the operator opens it from the home screen, where
    currentUnit is null."""
    for name in ('adminOverviewHtml', 'renderAdmin', 'refreshAdmin', 'openAdmin'):
        body = extract(src, r'(?:async )?function ' + name + r'\(.*?\n\}', name)
        assert 'currentUnit' not in body, f'{name}() reads currentUnit'


def main():
    src = open(INDEX, encoding='utf-8').read()
    node = shutil.which('node')
    assert node, 'node is required to run the frontend rules (it ships with the CI image)'
    out = render(src, node)
    test_a_hostile_organisation_name_is_never_markup(out)
    test_no_handler_carries_anything_but_a_literal(out)
    test_numbers_go_through_the_page_formatter(out)
    test_missing_values_read_as_missing_not_as_null(out)
    test_both_tables_sort_through_the_shared_helpers(out)
    test_the_empty_and_loading_states_say_so(out)
    test_the_menu_entries_are_gated_on_the_flag(src)
    test_signing_out_takes_every_tenants_data_with_it(out, src)
    test_the_page_never_reads_the_current_unit(src)
    print('ok')


if __name__ == '__main__':
    main()

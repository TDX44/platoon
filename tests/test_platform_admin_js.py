"""The /admin page's markup, lifted out of index.html and run under node.

Run with: python tests/test_platform_admin_js.py

adminOverviewHtml() is pure, so the one thing that matters about it — that a
string which came off the wire never becomes markup — is testable without a
browser. The payload crosses tenants, so the organization names and user
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
    'totals': {'organizations': 2, 'unit_count': 1234, 'personnel_count': 9876,
               'user_count': 12, 'unattached_users': 3, 'pending_invites': 1,
               'database_bytes': 86423219},
    'organizations': [
        {'org_id': 1, 'org_name': 'Alpha Co', 'org_slug': 'alpha-co', 'org_kind': 'company',
         'created_stamp': '2026-01-02 03:04:05', 'unit_count': 4, 'personnel_count': 12345,
         'user_count': 5, 'owner_emails': 'boss@example.com', 'pending_invites': 2,
         'has_logo': True, 'last_activity': '2026-09-17 06:00:00', 'audit_7d': 17,
         'org_timezone': 'America/Chicago',
         'billing': {'billing_trial': 2, 'billing_grace': 0, 'billing_locked': 0,
                     'billing_active': 2, 'billing_comped': 1}},
        {'org_id': 2, 'org_name': HOSTILE_NAME, 'org_slug': 'ghost', 'org_kind': 'platoon',
         'created_stamp': '', 'unit_count': 1, 'personnel_count': 2,
         'user_count': 1, 'owner_emails': None, 'pending_invites': 0,
         'has_logo': False, 'last_activity': None, 'audit_7d': 0,
         'org_timezone': HOSTILE_NAME,
         'billing': {'billing_trial': 0, 'billing_grace': 0, 'billing_locked': 1,
                     'billing_active': 0, 'billing_comped': 0}},
    ],
    'revenue': {'mrr_cents': 46658, 'arr_cents': 559900, 'currency': 'usd',
                'prices_available': True,
                'plans': [{'lookup_key': HOSTILE_NAME, 'subscribers': 3,
                           'amount': 299, 'interval': 'month'},
                          {'lookup_key': 'platoon_leader_annual', 'subscribers': 1,
                           'amount': 1999, 'interval': 'year'}]},
    'watchlist': {'past_due': [HOSTILE_EMAIL], 'cancelling': [], 'locked': ['x@example.com'],
                  'trial_ending': []},
    'recent_users': [
        {'user_id': 9, 'email': HOSTILE_EMAIL, 'full_name': HOSTILE_NAME, 'role': 'leader',
         'org_name': None, 'signed_in': True},
        {'user_id': 8, 'email': 'boss@example.com', 'full_name': 'Ada B', 'role': 'owner',
         'org_name': 'Alpha Co', 'signed_in': False},
    ],
}

EMPTY = {'generated_at': '2026-09-17 06:30:00', 'totals': {},
         'organizations': [], 'recent_users': []}

DETAIL = {
    'org_id': 2, 'org_name': HOSTILE_NAME,
    'units': [
        {'unit_id': 1, 'unit_name': 'Root', 'unit_slug': 'root', 'unit_kind': 'company',
         'parent_id': None, 'depth': 0, 'personnel_count': 0, 'user_count': 1},
        {'unit_id': 2, 'unit_name': HOSTILE_NAME, 'unit_slug': 'ghost', 'unit_kind': 'platoon',
         'parent_id': 1, 'depth': 1, 'personnel_count': 12, 'user_count': 2},
    ],
    'accounts': [
        {'user_id': 7, 'email': HOSTILE_EMAIL, 'billing_mode': 'default',
         'billing_state': 'ACTIVE', 'days_left': None, 'subscribed': True,
         'plan': HOSTILE_NAME, 'status': 'active', 'cancel_at_period_end': True,
         'trial_ends_at': None, 'current_period_end': '2026-10-01 00:00:00'},
    ],
}

DRIVER = r'''
const PAYLOAD = ''' + json.dumps(PAYLOAD) + r''';
const EMPTY = ''' + json.dumps(EMPTY) + r''';
const DETAIL = ''' + json.dumps(DETAIL) + r''';
const orgSort = { key: 'personnel_count', dir: -1 };
const userSort = { key: 'user_id', dir: -1 };
console.log(JSON.stringify({
  page: adminOverviewHtml(PAYLOAD, orgSort, userSort),
  empty: adminOverviewHtml(EMPTY, orgSort, userSort),
  loading: adminOverviewHtml(null, orgSort, userSort),
  byName: adminOverviewHtml(PAYLOAD, { key: 'org_name', dir: 1 }, userSort),
  byPaying: adminOverviewHtml(PAYLOAD, { key: 'billing_active', dir: -1 }, userSort),
  detail: adminOrgDetailHtml(DETAIL),
  detailLoading: adminOrgDetailHtml(null),
  filtered: (() => {
    adminUserFilter = 'boss';
    const html = adminOverviewHtml(PAYLOAD, orgSort, userSort);
    adminUserFilter = '';
    return html;
  })(),
  money: [adminMoney(0, 'usd'), adminMoney(46658, 'usd'), adminMoney(199, 'NOTACURRENCY')],
  bytes: [adminBytes(0), adminBytes(999), adminBytes(86423219), adminBytes(5 * 1024 ** 3)],
  nums: [adminNum(0), adminNum(1234), adminNum(9876543)],
  cleared: (() => {
    adminData = { organizations: [{ org_name: 'Alpha Co', owner_emails: 'boss@example.com' }] };
    adminScreenEl.innerHTML = '<td>Alpha Co</td><td>boss@example.com</td>';
    adminOrgDetail = { org_name: 'Alpha Co' };
    adminUserFilter = 'boss';
    clearPlatformAdmin();
    return { data: adminData, html: adminScreenEl.innerHTML, removed: removedClasses,
             detail: adminOrgDetail, filter: adminUserFilter };
  })(),
}));
'''

# The three things clearPlatformAdmin() touches, stubbed so it can run headless.
FAKE_DOM = r'''
let adminData = null;
let adminOrgDetail = null;
let adminUserFilter = '';
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
        extract(src, r'const ADMIN_UNIT_COLUMNS = \[.*?\n\];', 'ADMIN_UNIT_COLUMNS'),
        extract(src, r'const ADMIN_ACCOUNT_COLUMNS = \[.*?\n\];', 'ADMIN_ACCOUNT_COLUMNS'),
        extract(src, r'const ADMIN_WATCH = \[.*?\n\];', 'ADMIN_WATCH'),
        extract(src, r'function adminNum\(.*?\n\}', 'adminNum()'),
        extract(src, r'function adminBytes\(.*?\n\}', 'adminBytes()'),
        extract(src, r'function adminCell\(.*?\n\}', 'adminCell()'),
        extract(src, r'function adminSortValue\(.*?\n\}', 'adminSortValue()'),
        extract(src, r'function adminCompToggle\(.*?\n\}', 'adminCompToggle()'),
        extract(src, r'function adminMoney\(.*?\n\}', 'adminMoney()'),
        extract(src, r'function adminRevenueHtml\(.*?\n\}', 'adminRevenueHtml()'),
        extract(src, r'function adminWatchlistHtml\(.*?\n\}', 'adminWatchlistHtml()'),
        extract(src, r'function adminOrgLink\(.*?\n\}', 'adminOrgLink()'),
        extract(src, r'function adminOrgDetailHtml\(.*?\n\}', 'adminOrgDetailHtml()'),
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


def test_a_hostile_organization_name_is_never_markup(out):
    html = out['page']
    assert '<img src=x' not in html, 'an organization name reached the page as markup'
    assert '<script>' not in html, 'a user email reached the page as markup'
    assert '&lt;img src=x onerror=alert(1)&gt; &quot;Ghost&quot; Co' in html, \
        'the organization name is not escaped the way escapeHtml() escapes it'
    assert '&lt;script&gt;alert(2)&lt;/script&gt;' in html, 'the email is not escaped'
    # The only tags on this page are the ones the function writes itself.
    tags = set(re.findall(r'<(/?[a-zA-Z][\w-]*)', html))
    # ul/li are the watchlist cards and `input` is the user filter; both were
    # added with the revenue/watchlist/drill-down work. Widen this set only
    # when the FUNCTION grows a tag, never to make a failure go away -- an
    # unexpected tag here means a string off the wire became markup.
    assert tags <= {'div', '/div', 'span', '/span', 'section', '/section', 'h1', '/h1',
                    'h2', '/h2', 'button', '/button', 'table', '/table', 'thead', '/thead',
                    'tbody', '/tbody', 'tr', '/tr', 'th', '/th', 'td', '/td',
                    'ul', '/ul', 'li', '/li', 'input'}, \
        f'a server string opened a tag of its own: {sorted(tags)}'


def test_no_handler_carries_anything_but_a_literal(out):
    for key in ('page', 'empty', 'loading', 'detail', 'filtered', 'byPaying'):
        for call in re.findall(r'onclick="([^"]*)"', out[key]):
            assert re.fullmatch(
                r"(closeAdmin|refreshAdmin|closeAdminOrg)\(\)"
                r"|sortAdmin(Orgs|Users)\('\w+'\)"
                r"|openAdminOrg\(\d+\)"                 # an integer, checked by adminOrgLink()
                r"|setBillingMode\(\d+, '(comped|default)'\)", call), \
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


def column_keys(src, const):
    block = extract(src, rf'const {const} = \[.*?\n\];', const)
    return re.findall(r"key: '(\w+)'", block)


def test_both_tables_sort_through_the_shared_helpers(out, src):
    def org_names(html):
        # The name is inside the drill-down button now, so target that rather
        # than the cell -- the users table uses admin-strong for emails too.
        return re.findall(r'<button type="button" class="admin-link"[^>]*>([^<]*)', html)
    by_size = org_names(out['page'])
    by_name = org_names(out['byName'])
    assert by_size[0] == 'Alpha Co', by_size
    assert by_size != by_name, 'changing the sort column changed nothing'
    assert by_name[0].startswith('&lt;img'), f'name-ascending puts the escaped name first: {by_name}'
    # Sorted headers are marked by the shared helper, not by hand.
    assert 'class="sortable sorted"' in out['page'], 'the sorted column is not marked'
    expected = len(column_keys(src, 'ADMIN_ORG_COLUMNS')) + len(column_keys(src, 'ADMIN_USER_COLUMNS'))
    assert out['page'].count('sort-caret') == expected, \
        f'every column must carry a caret: expected {expected}'


def test_revenue_is_money_not_a_raw_number(out):
    html = out['page']
    assert '$466.58' in html, f'MRR is not formatted as money: {html[:400]}'
    assert '$5,599.00' in html, 'ARR is not formatted as money'
    assert '46658' not in html, 'the raw cent count reached the page'
    # An unknown currency must degrade, not throw.
    assert out['money'][0] == '$0.00' and '1.99' in out['money'][2], out['money']


def test_a_hostile_plan_name_and_watchlist_email_are_escaped(out):
    html = out['page']
    assert '&lt;img src=x' in html, 'the hostile plan/timezone string is not escaped'
    assert '<img src=x' not in html, 'a plan name or watch email reached the page as markup'
    assert 'Payment failed' in html and 'Locked out' in html, 'the watchlist is not drawn'
    assert 'Cancelling at period end' not in html, 'an empty watch bucket drew a card anyway'


def test_the_org_table_shows_the_billing_rollup(out):
    html = out['page']
    assert '>Paying<' in html and '>Comped<' in html and '>Timezone<' in html, 'rollup columns missing'
    assert 'America/Chicago' in html, 'the org timezone is not shown'
    # Sorting by a rollup key only works because the billing dict is spread
    # onto the row; without that the column sorts as all-undefined and the
    # header is never marked.
    assert 'Paying' in out['byPaying'], 'the Paying column is missing'
    assert out['byPaying'].count('class="sortable sorted"') == 1, \
        'exactly one column should be marked sorted'


def test_the_user_filter_narrows_the_table(out):
    # Scoped to the users section: the watchlist above it legitimately still
    # names the hostile account, because a filter on the directory is not a
    # filter on "who needs attention".
    users_section = out['filtered'].split('<h2>Users')[-1]
    assert 'boss@example.com' in users_section, users_section[:300]
    assert '&lt;script&gt;alert(2)&lt;/script&gt;' not in users_section, \
        'the filter did not exclude the non-matching user'
    assert 'value="boss"' in users_section, 'the filter box does not keep what was typed'
    assert '&lt;script&gt;alert(2)&lt;/script&gt;' in out['page'].split('<h2>Users')[-1], \
        'the unfiltered table should still show everyone'


def test_the_drill_down_escapes_and_indents(out):
    html = out['detail']
    assert '<img src=x' not in html, 'a unit or plan name reached the drill-down as markup'
    assert '&lt;img src=x' in html, 'the hostile unit name is not escaped'
    assert 'padding-left:1.2em' in html, 'the unit tree is not indented by depth'
    assert '>Units<' in html and '>Accounts<' in html, 'the drill-down sections are missing'
    assert 'All organizations' in html, 'there is no way back out of the drill-down'
    assert 'Loading' in out['detailLoading'] and '<table' not in out['detailLoading']


def test_the_empty_and_loading_states_say_so(out):
    assert 'No organizations yet.' in out['empty'], out['empty'][:300]
    assert 'No users yet.' in out['empty']
    assert 'Loading' in out['loading'] and '<table' not in out['loading'], out['loading']


def test_the_menu_entries_are_gated_on_the_flag(src):
    """Both entry points must test currentUser.platform_admin, and the
    account-menu item must start hidden — a default-visible item shows itself
    to everyone for the moment before the menu is first synced."""
    item = extract(src, r'<button id="adminMenuItem".*?</button>', 'the Admin menu item')
    assert 'style="display:none"' in item, f'the Admin menu item starts visible: {item}'
    sync = extract(src, r'function syncUserMenu\(\) \{.*?\n\}', 'syncUserMenu()')
    assert re.search(r"adminMenuItem'\)\.style\.display\s*=\s*\n?\s*currentUser && currentUser\.platform_admin", sync), \
        'syncUserMenu() does not gate the Admin menu item on platform_admin'
    place = extract(src, r'function placeTopbar\(screen\) \{.*?\n\}', 'placeTopbar()')
    assert 'syncUserMenu()' in place, 'the menu is not re-synced when a screen is shown'

    # The settings nav: the entry is marked admin-only, and the one filter
    # every rendering goes through drops it unless the flag is set.
    nav = extract(src, r'const SETTINGS_NAV = \[.*?\n\];', 'SETTINGS_NAV')
    assert re.search(r"key: 'admin'[^}]*open: 'openAdmin\(\)'[^}]*admin: true", nav), \
        'the settings nav has no admin-only Platform admin entry'
    items = extract(src, r'function settingsNavItems\(opts\) \{.*?\n\}', 'settingsNavItems()')
    assert '!it.admin || admin' in items, 'settingsNavItems() does not drop admin-only entries'
    render_nav = extract(src, r'function renderSettingsNav\(\) \{.*?\n\}', 'renderSettingsNav()')
    assert 'admin: !!(currentUser && currentUser.platform_admin)' in render_nav, \
        'the settings nav is not gated on platform_admin'

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
    """The payload names every organization on the instance and its owners'
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
    test_a_hostile_organization_name_is_never_markup(out)
    test_no_handler_carries_anything_but_a_literal(out)
    test_numbers_go_through_the_page_formatter(out)
    test_missing_values_read_as_missing_not_as_null(out)
    test_both_tables_sort_through_the_shared_helpers(out, src)
    test_revenue_is_money_not_a_raw_number(out)
    test_a_hostile_plan_name_and_watchlist_email_are_escaped(out)
    test_the_org_table_shows_the_billing_rollup(out)
    test_the_user_filter_narrows_the_table(out)
    test_the_drill_down_escapes_and_indents(out)
    test_the_empty_and_loading_states_say_so(out)
    test_the_menu_entries_are_gated_on_the_flag(src)
    test_signing_out_takes_every_tenants_data_with_it(out, src)
    test_the_page_never_reads_the_current_unit(src)
    print('ok')


if __name__ == '__main__':
    main()

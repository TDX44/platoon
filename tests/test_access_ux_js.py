"""Access lives on the tree, so the pure helpers behind that view are lifted
out of index.html and run under node.

"Who runs 2nd Platoon?" used to be answerable only from a flat list behind
three stacked modals. The Units page now answers it on the row itself, which
needs four small pure functions: who leads each unit, what a grant would
actually reach, how long an invite has left, and what a role means in words.
Everything else on that page is DOM work; these are the parts with rules in
them, so these are the parts with a test.

Run with: python tests/test_access_ux_js.py
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')

# HHC ─ 1st Platoon ─ 1st Section ─ Alpha Team
#     └ 2nd Platoon
UNITS = [
    {'id': 1, 'parent_id': None, 'kind': 'company', 'name': 'HHC', 'slug': 'hhc', 'count': 1},
    {'id': 2, 'parent_id': 1, 'kind': 'platoon', 'name': '1st Platoon', 'slug': '1stplatoon', 'count': 2},
    {'id': 3, 'parent_id': 2, 'kind': 'section', 'name': '1st Section', 'slug': '1stsection', 'count': 3},
    {'id': 4, 'parent_id': 3, 'kind': 'team', 'name': 'Alpha Team', 'slug': 'alpha', 'count': 1},
    {'id': 5, 'parent_id': 1, 'kind': 'platoon', 'name': '2nd Platoon', 'slug': '2ndplatoon', 'count': 20},
    {'id': 6, 'parent_id': 2, 'kind': 'section', 'name': '2nd Section', 'slug': '2ndsection', 'count': 0},
    {'id': 7, 'parent_id': 2, 'kind': 'section', 'name': '3rd Section', 'slug': '3rdsection', 'count': 0},
]

USERS = [
    {'id': 10, 'username': 'zulu', 'full_name': 'Zulu Zeta', 'unit_id': 1, 'role': 'leader'},
    {'id': 11, 'username': 'boss', 'full_name': 'Alice Owner', 'unit_id': 1, 'role': 'owner'},
    {'id': 12, 'username': 'abel', 'full_name': '', 'unit_id': 1, 'role': 'leader'},
    {'id': 13, 'username': 'psg', 'full_name': 'Bravo Jones', 'unit_id': 2, 'role': 'leader'},
    {'id': 14, 'username': 'nowhere', 'full_name': 'No Unit', 'unit_id': None, 'role': 'leader'},
]

DRIVER = r'''
const out = {};
const by = leadersByUnit(USERS);
// Keyed by unit id, owner first, then by the name actually shown.
out.unit1 = (by[1] || []).map(u => u.username);
out.unit2 = (by[2] || []).map(u => u.username);
out.unit5 = by[5] || null;
out.unattached = Object.keys(by).filter(k => k === 'null' || k === 'undefined');
out.empty = leadersByUnit([]);
out.nullUsers = leadersByUnit(null);

out.reachLeaf = accessReach(4);
out.reachMid = accessReach(2);
out.reachRoot = accessReach(1);
out.reachMissing = accessReach(999);

const now = '2026-09-17 08:00:00';
out.exp5 = inviteExpiryLabel('2026-09-22 23:59:59', now);
out.exp1 = inviteExpiryLabel('2026-09-18 00:00:01', now);
out.exp0 = inviteExpiryLabel('2026-09-17 00:00:01', now);
out.expPast = inviteExpiryLabel('2026-09-16 23:59:59', now);
out.expNone = inviteExpiryLabel('', now);
out.expJunk = inviteExpiryLabel('not-a-date', now);

out.roleLeader = roleHelp('leader');
out.roleOwner = roleHelp('owner');
out.roleJunk = roleHelp('nonsense');

// A fetch that has not landed, and one that failed, are two things — and
// neither of them is "nobody leads this unit".
out.statePending = accessLoadState(null);
out.stateUndef = accessLoadState(undefined);
out.stateFailed = accessLoadState(ACCESS_FAILED);
out.stateLoaded = accessLoadState([]);
out.stateFull = accessLoadState(USERS);
console.log(JSON.stringify(out));
'''


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html — was it renamed or removed?'
    return m.group(0)


def script_text(source):
    """The one big inline <script> that is the whole SPA."""
    blocks = re.findall(r'<script>(.*?)</script>', source, re.S)
    assert blocks, 'no inline script in index.html'
    return max(blocks, key=len)


def test_helpers_under_node(src, node):
    js = '\n'.join([
        'let units = ' + json.dumps(UNITS) + ';',
        'const USERS = ' + json.dumps(USERS) + ';',
        extract(src, r'function unitById\(id\) \{.*?\n\}', 'unitById()'),
        extract(src, r'function unitChildren\(id\) \{.*?\n\}', 'unitChildren()'),
        extract(src, r'function unitSubtree\(id\) \{.*?\n\}', 'unitSubtree()'),
        extract(src, r'function unitHeadcount\(id\) \{.*?\n\}', 'unitHeadcount()'),
        extract(src, r'function unitLabel\(u\) \{.*?\n\}', 'unitLabel()'),
        extract(src, r'function leadersByUnit\(.*?\n\}', 'leadersByUnit()'),
        extract(src, r'const ACCESS_REACH_NAMES = \d+;', 'ACCESS_REACH_NAMES'),
        extract(src, r'function accessReach\(.*?\n\}', 'accessReach()'),
        extract(src, r'function inviteExpiryLabel\(.*?\n\}', 'inviteExpiryLabel()'),
        extract(src, r'const ROLE_HELP = \{.*?\n\};', 'ROLE_HELP'),
        extract(src, r'function roleHelp\(.*?\n\}', 'roleHelp()'),
        extract(src, r"const ACCESS_FAILED = '[^']+';", 'ACCESS_FAILED'),
        extract(src, r'function accessLoadState\(.*?\n\}', 'accessLoadState()'),
        DRIVER,
    ])
    path = os.path.join(tempfile.mkdtemp(), 'access.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(js)
    proc = subprocess.run([node, path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)

    # ── leadersByUnit ──
    assert out['unit1'] == ['boss', 'abel', 'zulu'], \
        f"the owner leads the list, then the name each row shows: {out['unit1']}"
    assert out['unit2'] == ['psg'], out['unit2']
    assert out['unit5'] is None, 'a unit with nobody must not invent an empty entry'
    assert not out['unattached'], \
        f'a user attached to no unit must not become a bucket: {out["unattached"]}'
    assert out['empty'] == {} and out['nullUsers'] == {}, \
        'a failed /api/users must still render rows, not throw'

    # ── accessReach ──
    assert out['reachLeaf'] == {'names': ['Alpha Team'], 'more': 0, 'soldiers': 1, 'units': 1}, \
        out['reachLeaf']
    assert out['reachMid'] == {
        'names': ['1st Platoon', '1st Section', '2nd Section', '3rd Section'],
        'more': 1, 'soldiers': 6, 'units': 5,
    }, f'the names come in the order the Units page lists them: {out["reachMid"]}'
    assert out['reachRoot']['soldiers'] == 27 and out['reachRoot']['units'] == 7, out['reachRoot']
    assert len(out['reachRoot']['names']) == 4 and out['reachRoot']['more'] == 3, \
        f'the name list is cut at four: {out["reachRoot"]}'
    assert out['reachMissing'] == {'names': [], 'more': 0, 'soldiers': 0, 'units': 0}, \
        'a unit outside the loaded tree reaches nothing, it does not throw'

    # ── inviteExpiryLabel ── dates, never times: an invite dying at 00:00:01
    # tomorrow has "tomorrow" left on it, not an hour.
    assert out['exp5'] == 'expires in 5 days', out['exp5']
    assert out['exp1'] == 'expires tomorrow', out['exp1']
    assert out['exp0'] == 'expires today', \
        f'a same-day expiry is today even when its time has passed: {out["exp0"]}'
    assert out['expPast'] == 'expired', out['expPast']
    assert out['expNone'] == '' and out['expJunk'] == '', \
        'no usable date means no claim about one'

    # ── roleHelp ──
    assert out['roleLeader'] == 'Sees and edits this unit and everything beneath it.', out['roleLeader']
    assert out['roleOwner'] == \
        'Also manages the organisation: owners, time zone, removing people, backups.', out['roleOwner']
    assert out['roleJunk'] == '', 'an unknown role explains nothing rather than lying'

    # ── accessLoadState ──
    assert out['statePending'] == 'pending' and out['stateUndef'] == 'pending', out
    assert out['stateFailed'] == 'failed', \
        'a failed fetch must be distinguishable from an empty one'
    assert out['stateLoaded'] == 'loaded' and out['stateFull'] == 'loaded', out


def test_the_page_says_what_it_does(src):
    """The rename, and the one-step invite the Units page now carries."""
    assert 'Manage Access' not in src, \
        'the old title is still on screen somewhere — it is "People & invites" now'
    for name in ('openUserMgmt', 'refreshUserList', 'userMgmtModal'):
        assert name in src, f'{name} was renamed; the ids and function names stay put'
    assert src.count('People &amp; invites') + src.count('People & invites') >= 3, \
        'the hamburger item, the Settings row and the modal heading all carry the new name'
    assert "Also on each unit's row under Units." in src, \
        'the Settings row does not point at the new home for this'

    units = extract(src, r'function renderUnits\(\) \{.*?\n\}', 'renderUnits()')
    assert 'Invite leader' in units, 'the unit row has no one-step invite'
    assert 'unitLeaderChips(' in units and 'unitInviteChips(' in units, \
        'the unit row shows neither its leaders nor its open invites'
    assert 'leadersByUnit(' in units, 'renderUnits() does not group the loaded people by unit'
    assert 'No leader yet' in extract(src, r'function unitLeaderChips\(.*?\n\}', 'unitLeaderChips()'), \
        'a unit with nobody must say so'
    # One fetch for the page, not one per row.
    page = extract(src, r'async function refreshUnitsPage\(\) \{.*?\n\}', 'refreshUnitsPage()')
    assert "'/users'" in page and "'/invites'" in page, \
        'the people and invites behind the rows are not loaded with the page'
    assert "'/users'" not in units and "'/invites'" not in units, \
        'renderUnits() fetches while rendering — it must draw from what the page loaded'

    # Sharing is offered only where it exists.
    assert 'navigator.share' in src, 'the created link cannot be shared'
    assert re.search(r'navigator\.share\s*(?:&&|\))', src) or 'typeof navigator.share' in src, \
        'navigator.share is called without being feature-detected'
    assert 'AbortError' in src, 'a dismissed share sheet must not surface as an error'

    # Role help follows the select everywhere a role is chosen.
    sync = extract(src, r'function syncRoleSelect\(.*?\n\}', 'syncRoleSelect()')
    assert 'syncRoleHelp(' in sync, \
        'the shared role select does not explain the role it has selected'
    assert 'roleHelp(' in extract(src, r'function syncRoleHelp\(.*?\n\}', 'syncRoleHelp()'), \
        'syncRoleHelp() does not use the wording the rest of the app agreed on'
    for help_id in ('userRoleHelp', 'invRoleHelp'):
        assert help_id in src, f'{help_id} is missing, so that select explains nothing'
    assert 'accessReach(' in extract(src, r'function renderAccessReach\(.*?\n\}', 'renderAccessReach()'), \
        'the edit-access modal never says what the grant would reach'


def test_the_page_never_claims_what_it_could_not_load(src):
    """api() returns null on failure. Drawn as [], that reads as "nobody"."""
    page = extract(src, r'async function refreshUnitsPage\(\) \{.*?\n\}', 'refreshUnitsPage()')
    assert 'ACCESS_FAILED' in page, \
        'a failed /api/users or /api/invites is stored as an empty list, which is a lie'
    assert not re.search(r'unit(?:Users|Invites) = \w+ \|\| \[\]', page), \
        'the || [] fallback is back: a failed fetch would render as "No leader yet"'

    leaders = extract(src, r'function unitLeaderChips\(.*?\n\}', 'unitLeaderChips()')
    assert "Couldn't load access" in leaders or 'Couldn’t load access' in leaders, \
        'a failed people load says nothing at all about what went wrong'
    assert 'accessLoadState(' in leaders or re.search(r'state ===', leaders), \
        'unitLeaderChips() does not branch on whether the load actually landed'
    # "No leader yet" is a claim, so it may only be reachable once loaded.
    tail = leaders[leaders.index('No leader yet'):]
    head = leaders[:leaders.index('No leader yet')]
    assert 'pending' in head and 'failed' in head, \
        '"No leader yet" is reachable before the fetch lands or after it fails'
    assert tail  # the string is there at all

    invites = extract(src, r'function unitInviteChips\(.*?\n\}', 'unitInviteChips()')
    assert 'failed' in invites and 'pending' in invites, \
        'a failed invite load silently reads as "no pending invites"'

    # A previous unit's chips must not flash on the next visit.
    opener = extract(src, r'function openUnits\(push = true\) \{.*?\n\}', 'openUnits()')
    assert 'unitUsers = null' in opener and 'unitInvites = null' in opener, \
        'openUnits() leaves the last visit\'s people and invites on screen'


def test_every_access_write_refreshes_the_page(src):
    shared = extract(src, r'async function refreshAccessViews\(.*?\n\}', 'refreshAccessViews()')
    assert "units-active" in shared and 'refreshUnitsPage()' in shared, \
        'the shared refresh does not actually refresh the Units page'
    for fn, pattern in (
        ('saveUserEdit()', r'async function saveUserEdit\(\) \{.*?\n\}'),
        ('deleteUser()', r'async function deleteUser\(.*?\n\}'),
        ('revokeInvite()', r'async function revokeInvite\(.*?\n\}'),
        ('createInvite()', r'async function createInvite\(\) \{.*?\n\}'),
    ):
        assert 'refreshAccessViews()' in extract(src, pattern, fn), \
            f'{fn} changes access but leaves the Units page showing the old chips'

    # A declined confirm is not a revoke, so it must not cost a reload.
    revoke = extract(src, r'async function revokeInvite\(.*?\n\}', 'revokeInvite()')
    body = revoke[revoke.index('confirmDialog'):]
    assert body.index('return') < body.index('refreshAccessViews()'), \
        'declining the confirm still refreshes the page'
    # The chip handler must not refresh a second time on top of that.
    wire = extract(src, r'function wireUnitRowActions\(.*?\n\}', 'wireUnitRowActions()')
    assert 'refreshUnitsPage()' not in wire, \
        'the chip handler refreshes even when the revoke was declined'


def test_an_invite_says_what_it_grants(src):
    invites = extract(src, r'function unitInviteChips\(.*?\n\}', 'unitInviteChips()')
    assert re.search(r"role === 'owner'", invites), \
        'a pending OWNER invite chip looks exactly like a leader invite'
    form = extract(src, r'function unitInviteForm\(.*?\n\}', 'unitInviteForm()')
    assert form.count('unitInviteRoleHelp') == 1 and 'canOwn ?' not in form.split('unitInviteRoleHelp')[0][-40:], \
        'the role help only appears for an owner; a leader inviting a leader is the common case'
    assert "roleHelp('leader')" in form, \
        'with no select to follow, the help line has nothing to say'


def test_user_text_never_becomes_code(src):
    """Names, labels and tokens are server strings; they go in as data."""
    units = extract(src, r'function renderUnits\(\) \{.*?\n\}', 'renderUnits()')
    invite = extract(src, r'function unitInviteChips\(.*?\n\}', 'unitInviteChips()')
    leaders = extract(src, r'function unitLeaderChips\(.*?\n\}', 'unitLeaderChips()')
    for name, block in (('renderUnits', units), ('unitInviteChips', invite),
                        ('unitLeaderChips', leaders)):
        for attr in re.findall(r'onclick="[^"]*"', block):
            # Unit ids are numbers the page put there; anything else must not
            # be pasted into an attribute that the browser will execute.
            assert re.fullmatch(r'onclick="[A-Za-z0-9_ .=;()${}\[\]\'/+-]*"', attr), attr
            for interp in re.findall(r'\$\{([^}]*)\}', attr):
                assert re.fullmatch(r'[A-Za-z0-9_.]*\.id', interp.strip()), \
                    f'{name} interpolates {interp!r} into an onclick attribute'
    for block, what in ((invite, 'invite'), (leaders, 'leader')):
        assert 'data-' in block, f'the {what} chips carry no data- attributes to read from'

    # Escaping by coverage, not by presence: every interpolation that carries a
    # server string has to be wrapped, not just one of them somewhere nearby.
    form = extract(src, r'function unitInviteForm\(.*?\n\}', 'unitInviteForm()')
    server_strings = ('label', 'created_by', 'token', 'full_name', 'username',
                      'url', 'unit_name', 'name', 'bits')
    for name, block in (('unitLeaderChips', leaders), ('unitInviteChips', invite),
                        ('unitInviteForm', form)):
        # The innermost ${...} are the leaves that actually reach the markup.
        for expr in re.findall(r'\$\{([^{}]*)\}', block):
            if not any(re.search(rf'\b{w}\b', expr) for w in server_strings):
                continue
            assert 'escapeHtml(' in expr, \
                f'{name} interpolates the server string {expr.strip()!r} unescaped'

    # Chips and the inline form are listeners bound after the fact.
    wire = extract(src, r'function wireUnitRowActions\(.*?\n\}', 'wireUnitRowActions()')
    assert 'addEventListener(' in wire, 'the unit-row chips have no handlers'


def test_the_inline_script_still_parses(src, node):
    path = os.path.join(tempfile.mkdtemp(), 'spa.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(script_text(src))
    proc = subprocess.run([node, '--check', path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr


def main():
    src = open(INDEX, encoding='utf-8').read()
    node = shutil.which('node')
    assert node, 'node is required to run the frontend rules (it ships with the CI image)'
    test_helpers_under_node(src, node)
    test_the_page_says_what_it_does(src)
    test_the_page_never_claims_what_it_could_not_load(src)
    test_every_access_write_refreshes_the_page(src)
    test_an_invite_says_what_it_grants(src)
    test_user_text_never_becomes_code(src)
    test_the_inline_script_still_parses(src, node)
    print('ok')


if __name__ == '__main__':
    main()

"""The home screen's org chart, lifted out of index.html and run under node.

The home screen draws the unit tree as an order-of-battle diagram: the top unit
centred, one column per direct child, and everything deeper stacked under its
column. All of the markup comes out of one pure function, `orgChartHtml()`, so
the shape of the chart — and the escaping of every server string in it — is
testable without a browser.

Run with: python tests/test_org_chart_js.py
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')

# HHC ─ 1st Platoon ─ 1st Squad ─ Alpha Team
#     │             └ 2nd Squad
#     ├ 2nd Platoon
#     └ 3rd Platoon
THREE_LEVEL = [
    {'id': 1, 'parent_id': None, 'kind': 'company', 'name': 'HHC', 'count': 1},
    {'id': 2, 'parent_id': 1, 'kind': 'platoon', 'name': '1st Platoon', 'count': 2},
    {'id': 3, 'parent_id': 2, 'kind': 'squad', 'name': '1st Squad', 'count': 3},
    {'id': 4, 'parent_id': 3, 'kind': 'team', 'name': 'Alpha Team', 'count': 4},
    {'id': 5, 'parent_id': 2, 'kind': 'squad', 'name': '2nd Squad', 'count': 5},
    {'id': 6, 'parent_id': 1, 'kind': 'platoon', 'name': '2nd Platoon', 'count': 6},
    {'id': 7, 'parent_id': 1, 'kind': 'platoon', 'name': '3rd Platoon', 'count': 7},
]

HOSTILE = [
    {'id': 1, 'parent_id': None, 'kind': 'company', 'name': 'HHC', 'count': 0},
    {'id': 2, 'parent_id': 1, 'kind': 'platoon',
     'name': '<img src=x onerror=alert(1)> "Ghost"', 'count': 0},
]

ONE_CHILD = [
    {'id': 1, 'parent_id': None, 'kind': 'company', 'name': 'HHC', 'count': 0},
    {'id': 2, 'parent_id': 1, 'kind': 'platoon', 'name': 'Only', 'count': 3},
]

NO_CHILD = [{'id': 1, 'parent_id': None, 'kind': 'company', 'name': 'HHC', 'count': 9}]

# Five cards deep: company → platoon → section → squad → team.
DEEP = [
    {'id': 1, 'parent_id': None, 'kind': 'company', 'name': 'L0', 'count': 1},
    {'id': 2, 'parent_id': 1, 'kind': 'platoon', 'name': 'L1', 'count': 1},
    {'id': 3, 'parent_id': 2, 'kind': 'section', 'name': 'L2', 'count': 1},
    {'id': 4, 'parent_id': 3, 'kind': 'squad', 'name': 'L3', 'count': 1},
    {'id': 5, 'parent_id': 4, 'kind': 'team', 'name': 'L4', 'count': 1},
]

DRIVER = r'''
function chart(tree) {
  units = tree;
  return orgChartHtml(unitById(tree[0].id), unitChildren, unitHeadcount);
}
console.log(JSON.stringify({
  three: chart(THREE_LEVEL),
  hostile: chart(HOSTILE),
  oneChild: chart(ONE_CHILD),
  noChild: chart(NO_CHILD),
  deep: chart(DEEP),
  noTop: orgChartHtml(null, unitChildren, unitHeadcount),
}));
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


def render(src, node):
    js = '\n'.join([
        'let units = [];',
        'const THREE_LEVEL = ' + json.dumps(THREE_LEVEL) + ';',
        'const HOSTILE = ' + json.dumps(HOSTILE) + ';',
        'const ONE_CHILD = ' + json.dumps(ONE_CHILD) + ';',
        'const NO_CHILD = ' + json.dumps(NO_CHILD) + ';',
        'const DEEP = ' + json.dumps(DEEP) + ';',
        extract(src, r'function escapeHtml\(str\) \{.*?\n\}', 'escapeHtml()'),
        extract(src, r'function unitById\(id\) \{.*?\n\}', 'unitById()'),
        extract(src, r'function unitChildren\(id\) \{.*?\n\}', 'unitChildren()'),
        extract(src, r'function unitSubtree\(id\) \{.*?\n\}', 'unitSubtree()'),
        extract(src, r'function unitHeadcount\(id\) \{.*?\n\}', 'unitHeadcount()'),
        extract(src, r'function kindLabel\(kind\) \{.*?\n\}', 'kindLabel()'),
        extract(src, r'function orgChartHtml\(.*?\n\}', 'orgChartHtml()'),
        DRIVER,
    ])
    path = os.path.join(tempfile.mkdtemp(), 'orgchart.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(js)
    proc = subprocess.run([node, path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def cards(html):
    """Every card, in document order, as (id, name)."""
    return re.findall(r'selectUnitById\((\d+)\)[^>]*>\s*'
                      r'<div class="platoon-card-label">[^<]*</div>\s*'
                      r'<div class="platoon-card-name">(.*?)</div>', html, re.S)


def test_a_three_level_tree_becomes_a_chart(out):
    html = out['three']
    assert 'class="org-chart" data-cols="3"' in html, \
        f'three direct children means three columns: {html[:200]}'

    # The top unit is the one card above the columns.
    top = re.search(r'<li class="org-node org-top">(.*?)<ul class="org-branches">', html, re.S)
    assert top, 'the top card is not sitting above a branch row'
    assert 'HHC' in top.group(1) and top.group(1).count('selectUnitById(') == 1, \
        f'exactly one card belongs above the branch row: {top.group(1)}'
    assert 'unit-card-top' in top.group(1), 'the top card is not marked as the top card'

    # One column per direct child, in unitChildren() order (name-sorted).
    # Splitting on the column marker is enough: whatever falls between two
    # markers belongs to the column that opened first.
    cols = html.split('<li class="org-node org-col">')[1:]
    assert len(cols) == 3, f'expected 3 columns, got {len(cols)}'
    heads = [cards(c)[0][1] for c in cols]
    assert heads == ['1st Platoon', '2nd Platoon', '3rd Platoon'], heads

    # Grandchildren live inside their own column, not loose in the row.
    assert [n for _, n in cards(cols[0])] == \
        ['1st Platoon', '1st Squad', 'Alpha Team', '2nd Squad'], cards(cols[0])
    assert len(cards(cols[1])) == 1 and len(cards(cols[2])) == 1, \
        'a childless column must not pick up someone else\u2019s children'

    # Headcounts roll up the subtree, same as the old card list did.
    assert '<div class="platoon-card-count">28 personnel</div>' in top.group(1), \
        'the top card does not count the whole tree'
    assert '14 personnel' in cols[0], f'1st Platoon should count 2+3+4+5: {cols[0]}'

    # Nested lists, not flat rows — the phone stylesheet needs the nesting.
    assert html.count('<ul class="org-stack">') == 2, \
        'level 2+ must be nested <ul>s, one per parent that has children'


def test_a_unit_name_is_never_code(out):
    html = out['hostile']
    assert '<img src=x' not in html, 'a unit name reached the page as markup'
    # The only tags on the page are the ones this function writes: a name that
    # opened one of its own would show up here.
    tags = set(re.findall(r'<(/?[a-zA-Z][\w-]*)', html))
    assert tags <= {'div', '/div', 'button', '/button', 'ul', '/ul', 'li', '/li'}, \
        f'a unit name opened a tag of its own: {sorted(tags)}'
    assert '&lt;img src=x onerror=alert(1)&gt; &quot;Ghost&quot;' in html, \
        f'the name is not escaped the way escapeHtml() escapes it: {html}'


def test_onclick_carries_a_number_and_nothing_else(out):
    for key in ('three', 'hostile', 'oneChild', 'noChild', 'deep'):
        html = out[key]
        calls = re.findall(r'onclick="([^"]*)"', html)
        assert calls, f'{key}: no card is clickable'
        for call in calls:
            assert re.fullmatch(r'selectUnitById\(\d+\)', call), \
                f'{key}: onclick carries more than a number: {call!r}'


def test_one_child_and_no_child_draw_no_bar(out):
    one = out['oneChild']
    assert 'data-cols="1"' in one, f'a single child is one column: {one[:200]}'
    assert one.count('<ul class="org-branches">') == 1, one
    assert '<ul class="org-stack">' not in one, 'a childless column needs no stack'

    none = out['noChild']
    assert 'data-cols="0"' in none, f'no children is no columns: {none[:200]}'
    assert '<ul class="org-branches">' not in none, \
        'an empty branch row still draws a connector — it must not be emitted at all'
    assert len(cards(none)) == 1 and cards(none)[0][1] == 'HHC', cards(none)

    assert out['noTop'] == '', 'no unit at all is no chart'


def test_five_deep_stays_five_deep(out):
    html = out['deep']
    assert 'data-cols="1"' in html, html[:200]
    assert [n for _, n in cards(html)] == ['L0', 'L1', 'L2', 'L3', 'L4'], cards(html)
    # L0 is the top card, L1 the column head, L2..L4 each a further <ul>.
    assert html.count('<ul class="org-stack">') == 3, \
        f'the deeper levels are flattened instead of nested: {html}'
    # …and each one is inside the previous, not a sibling.
    depth, seen = 0, 0
    for tok in re.findall(r'<ul class="org-stack">|</ul>', html):
        depth += 1 if tok.startswith('<ul') else -1
        seen = max(seen, depth)
    assert seen == 3, f'the stacks are siblings, not nested: depth {seen}'


def test_the_home_screen_uses_the_pure_function(src):
    home = extract(src, r'function renderHome\(\) \{.*?\n\}', 'renderHome()')
    assert 'orgChartHtml(' in home, 'renderHome() still builds its own markup'
    assert 'No unit yet.' in home, 'the empty state went missing'
    assert 'renderHomeLead(' in home, 'the lead line went missing'
    assert 'data-depth' not in src, 'the old depth-indented card list is still here'
    # Wide organisations scroll inside the chart, never the page.
    assert re.search(r'\.org-chart\s*\{[^}]*overflow-x:\s*auto', src), \
        'the chart does not scroll horizontally inside itself'
    assert re.search(r'@media \(max-width: 700px\)', src), \
        'there is no phone fallback to the indented tree'


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
    out = render(src, node)
    test_a_three_level_tree_becomes_a_chart(out)
    test_a_unit_name_is_never_code(out)
    test_onclick_carries_a_number_and_nothing_else(out)
    test_one_child_and_no_child_draw_no_bar(out)
    test_five_deep_stays_five_deep(out)
    test_the_home_screen_uses_the_pure_function(src)
    test_the_inline_script_still_parses(src, node)
    print('ok')


if __name__ == '__main__':
    main()

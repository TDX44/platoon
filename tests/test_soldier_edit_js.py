"""Name, rank and unit are changed in one place: the edit modal.

Clicking a roster name opens the soldier page, and from there renaming someone
used to mean finding three inputs in an Overview card whose Save button also
saved fourteen unrelated profile fields — while the real edit modal, the one
with the unit select and Delete, was reachable only from the roster row's ⋯
menu. Two ways to do the same thing, neither of them where the user was.

The page now opens that same modal from its hero. This pins the parts of that
which can silently regress: the button exists and opens the modal, the old
inline card is gone for good, "Save changes" no longer writes identity fields,
and the page survives an edit that deletes or moves the person it is about.

Run with: python tests/test_soldier_edit_js.py
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')

PERSONNEL = [
    {'id': 7, 'rank': 'SSG', 'last': 'Jones', 'first': 'Ray'},
    {'id': 3, 'rank': 'PFC', 'last': 'Ash', 'first': 'Kim'},
]

DRIVER = r'''
const out = {};
out.first = soldierIndexById(7);
out.second = soldierIndexById(3);
// The hero interpolates the id into the attribute as a number, but a route or
// a data- attribute hands it back as a string; both must find the same person.
out.asString = soldierIndexById('3');
out.missing = soldierIndexById(99);
out.nullId = soldierIndexById(null);
out.undef = soldierIndexById(undefined);
console.log(JSON.stringify(out));
'''


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html — was it renamed or removed?'
    return m.group(0)


def script_text(source):
    blocks = re.findall(r'<script>(.*?)</script>', source, re.S)
    assert blocks, 'no inline script in index.html'
    return max(blocks, key=len)


def test_finding_the_person_the_page_is_about(src, node):
    js = '\n'.join([
        'let personnel = ' + json.dumps(PERSONNEL) + ';',
        extract(src, r'function soldierIndexById\(.*?\n\}', 'soldierIndexById()'),
        DRIVER,
    ])
    path = os.path.join(tempfile.mkdtemp(), 'soldier.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(js)
    proc = subprocess.run([node, path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)
    assert out['first'] == 0 and out['second'] == 1, out
    assert out['asString'] == 1, f'the id arrives as a string too: {out["asString"]}'
    for key in ('missing', 'nullId', 'undef'):
        assert out[key] == -1, f'{key} must be -1, not a stray index: {out[key]}'


def test_the_hero_opens_the_real_edit_modal(src):
    page = extract(src, r'function renderSoldierPage\(\) \{.*?\n  `;\n', 'renderSoldierPage()')
    hero = extract(page, r'<div class="soldier-hero-actions">.*?</div>', 'the hero action buttons')
    assert 'openEditModal(' in hero, \
        'the soldier page has no button onto the edit modal'
    assert 'soldierIndexById(' in hero, \
        'the hero passes something other than this person\'s index to the modal'
    assert re.search(r'<button[^>]*>(?:(?!</button>).)*Edit', hero, re.S), \
        'whatever opens the modal is not a real <button> with an Edit label'
    assert 'saveProfile()' in hero, 'the Save changes button is gone'

    # openEditModal() is now reachable with an index that can be -1 (the person
    # left the roster between render and click), so it has to survive that.
    modal = extract(src, r'function openEditModal\(i\) \{.*?\n\}', 'openEditModal()')
    assert re.search(r'if \(!p\)\s*return', modal), \
        'openEditModal() reads personnel[i] without checking there is a person there'


def test_there_is_only_one_way_to_change_a_name(src):
    assert 'pf_identity_' not in src, \
        'the inline Name & rank card is still there — that is the second way'
    assert 'saveIdentity' not in src, \
        'the code behind the inline card is still there'
    assert 'Name &amp; rank' not in src and 'Name & rank' not in src, \
        'the Overview tab still offers a Name & rank card'

    save = extract(src, r'async function saveProfile\(\) \{.*?\n\}', 'saveProfile()')
    for field in ('rank', 'last', 'first'):
        assert not re.search(rf'\b{field}\b', save), \
            f'"Save changes" still touches {field}; identity belongs to the modal now'


def test_an_edit_does_not_strand_the_page(src):
    sync = extract(src, r'function syncSoldierPageAfterEdit\(.*?\n\}', 'syncSoldierPageAfterEdit()')
    assert 'closeSoldierPage()' in sync, \
        'a deleted or moved-away person leaves the page rendering nothing'
    assert 'renderSoldierPage()' in sync, 'the page never picks up the new name'
    assert 'currentSoldierPerson()' in sync, \
        'nothing checks whether the person is still on this roster'
    # The re-render rebuilds every pf_ input empty, so anything typed into the
    # profile and not yet saved has to be carried across it.
    assert 'pf_' in sync, 'the re-render silently discards unsaved profile fields'

    # Both paths that can change or remove the person run it.
    for fn, pattern in (('savePerson()', r'async function savePerson\(\) \{.*?\n\}'),
                        ('removePerson()', r'async function removePerson\(i\) \{.*?\n\}')):
        assert 'syncSoldierPageAfterEdit()' in extract(src, pattern, fn), \
            f'{fn} leaves an open soldier page showing the old person'
    # The tab the user was on is what renderSoldierPage() already restores.
    page = extract(src, r'function renderSoldierPage\(\) \{.*?\n\}', 'renderSoldierPage()')
    assert 'setSoldierTab(soldierTab)' in page, \
        'a refresh throws the user back to the Overview tab'


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
    test_finding_the_person_the_page_is_about(src, node)
    test_the_hero_opens_the_real_edit_modal(src)
    test_there_is_only_one_way_to_change_a_name(src)
    test_an_edit_does_not_strand_the_page(src)
    test_the_inline_script_still_parses(src, node)
    print('ok')


if __name__ == '__main__':
    main()

"""The frontend's unit-tree helpers, lifted out of index.html and run under node."""
import json, os, re, shutil, subprocess, sys, tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')
UNITS = [
    {'id': 1, 'parent_id': None, 'kind': 'company', 'name': 'HHC', 'slug': 'hhc', 'count': 1},
    {'id': 2, 'parent_id': 1, 'kind': 'platoon', 'name': '2nd Platoon', 'slug': '2ndplatoon', 'count': 20},
    {'id': 3, 'parent_id': 2, 'kind': 'squad', 'name': 'Alpha', 'slug': 'alpha', 'count': 4},
    {'id': 4, 'parent_id': 1, 'kind': 'platoon', 'name': '1st Platoon', 'slug': '1stplatoon', 'count': 30},
]


def extract(src, pattern, what):
    m = re.search(pattern, src, re.S)
    assert m, f'could not find {what} in index.html'
    return m.group(0)


def main():
    src = open(INDEX, encoding='utf-8').read()
    for word in ('currentPlatoon', 'PLATOON_PATHS', 'PATH_TO_PLATOON', 'userHasAccess', 'is_admin', 'chk1st', 'invChk1st'):
        assert word not in src, f'{word} is still in index.html'
    assert src.count('unit=${currentUnit.id}') + src.count('unit_id: currentUnit.id') >= 15, 'not every fetch passes the unit'
    js = '\n'.join([
        'let units = ' + json.dumps(UNITS) + ';',
        extract(src, r'function unitById\(id\) \{.*?\n\}', 'unitById'),
        extract(src, r'function unitBySlug\(slug\) \{.*?\n\}', 'unitBySlug'),
        extract(src, r'function unitChildren\(id\) \{.*?\n\}', 'unitChildren'),
        extract(src, r'function unitSubtree\(id\) \{.*?\n\}', 'unitSubtree'),
        extract(src, r'function unitHeadcount\(id\) \{.*?\n\}', 'unitHeadcount'),
        'console.log(JSON.stringify({sub: unitSubtree(2), head: unitHeadcount(1), bySlug: unitBySlug("1stplatoon").id, '
        'kids: unitChildren(1).map(u => u.id), missing: unitBySlug("nope") || null}));',
    ])
    path = os.path.join(tempfile.mkdtemp(), 'units.js')
    open(path, 'w').write(js)
    proc = subprocess.run([shutil.which('node'), path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)
    assert sorted(out['sub']) == [2, 3], out
    assert out['head'] == 55, out
    # kids comes back name-sorted, not id-sorted: "1st Platoon" (4) precedes
    # "2nd Platoon" (2), which is the order the home tree has to render.
    assert out['bySlug'] == 4 and out['kids'] == [4, 2] and out['missing'] is None, out
    print('ok')


if __name__ == '__main__':
    main()

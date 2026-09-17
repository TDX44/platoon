"""The two pure functions behind the unit logo, lifted out of index.html.

Run with: python tests/test_unit_logo_js.py

Everything else about the logo is DOM work — a file input, a canvas, an <img>
src. The parts with rules in them are: how an image is shrunk to fit the
server's 512 x 512 cap without ever being blown up, and which URL an <img>
should point at for a unit that may own, inherit or have no logo at all. Those
two get a test; the rest gets read.
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')

UNITS = [
    {'id': 1, 'parent_id': None, 'kind': 'company', 'name': 'HHC', 'slug': 'hhc', 'count': 1,
     'logo': {'unit_id': 1, 'v': 'a1b2c3d4e5f6'}},
    {'id': 2, 'parent_id': 1, 'kind': 'platoon', 'name': '1st Platoon', 'slug': '1stplatoon',
     'count': 2, 'logo': {'unit_id': 1, 'v': 'a1b2c3d4e5f6'}},
    {'id': 3, 'parent_id': 1, 'kind': 'platoon', 'name': '2nd Platoon', 'slug': '2ndplatoon',
     'count': 0, 'logo': None},
]

DRIVER = r'''
const out = {};
out.wide   = fitWithin(1000, 500, 512);
out.tall   = fitWithin(500, 1000, 512);
out.small  = fitWithin(300, 200, 512);
out.exact  = fitWithin(512, 512, 512);
out.sliver = fitWithin(513, 1, 512);
out.zero   = fitWithin(0, 100, 512);
out.nan    = fitWithin(NaN, NaN, 512);
out.junk   = fitWithin('wide', 'tall', 512);
out.neg    = fitWithin(-10, 20, 512);
out.undef  = fitWithin(undefined, undefined, 512);
out.frac   = fitWithin(1000.7, 500.2, 512);
out.retry  = fitWithin(1000, 500, 256);

out.own       = unitLogoUrl(units[0]);
out.inherited = unitLogoUrl(units[1]);
out.none      = unitLogoUrl(units[2]);
out.missing   = unitLogoUrl(null);
out.sidebar   = unitLogoUrl(units[2], BUILTIN_SIDEBAR_LOGO);
out.builtin   = BUILTIN_LOGO;
out.builtinSide = BUILTIN_SIDEBAR_LOGO;
console.log(JSON.stringify(out));
'''


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html — was it renamed or removed?'
    return m.group(0)


def test_helpers_under_node(src, node):
    js = '\n'.join([
        'const units = ' + json.dumps(UNITS) + ';',
        extract(src, r"const BUILTIN_LOGO = '[^']+';", 'BUILTIN_LOGO'),
        extract(src, r"const BUILTIN_SIDEBAR_LOGO = '[^']+';", 'BUILTIN_SIDEBAR_LOGO'),
        extract(src, r'function fitWithin\(.*?\n\}', 'fitWithin()'),
        extract(src, r'function unitLogoUrl\(.*?\n\}', 'unitLogoUrl()'),
        DRIVER,
    ])
    path = os.path.join(tempfile.mkdtemp(), 'logo.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(js)
    proc = subprocess.run([node, path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)

    # ── fitWithin ── the long edge lands exactly on the cap; nothing grows.
    assert out['wide'] == {'w': 512, 'h': 256}, out['wide']
    assert out['tall'] == {'w': 256, 'h': 512}, out['tall']
    assert out['small'] == {'w': 300, 'h': 200}, \
        f'a small image was scaled UP to the cap: {out["small"]}'
    assert out['exact'] == {'w': 512, 'h': 512}, out['exact']
    assert out['sliver'] == {'w': 512, 'h': 1}, \
        f'a 513x1 strip must keep at least one pixel of height: {out["sliver"]}'
    assert out['retry'] == {'w': 256, 'h': 128}, \
        f'the smaller retry pass uses the same maths: {out["retry"]}'
    for key in ('zero', 'nan', 'junk', 'neg', 'undef'):
        assert out[key] == {'w': 0, 'h': 0}, \
            f'{key}: a degenerate size must come back as nothing, not NaN: {out[key]}'
    assert out['frac'] == {'w': 512, 'h': 256}, \
        f'a fractional intrinsic size still yields whole pixels: {out["frac"]}'
    for key, val in out.items():
        if isinstance(val, dict) and 'w' in val:
            assert float(val['w']).is_integer() and float(val['h']).is_integer(), (key, val)
            assert val['w'] <= 512 and val['h'] <= 512, (key, val)

    # ── unitLogoUrl ── a unit that owns one, one that inherits, one with none.
    assert out['own'] == '/api/units/1/logo?v=a1b2c3d4e5f6', out['own']
    assert out['inherited'] == '/api/units/1/logo?v=a1b2c3d4e5f6', \
        f'a unit with no logo of its own must fetch the one it inherits: {out["inherited"]}'
    assert out['none'] == out['builtin'] == '/images/logo-duck.png', out
    assert out['missing'] == out['builtin'], 'a missing unit must not build a /api/units/undefined URL'
    assert out['sidebar'] == out['builtinSide'] == '/images/sidebar-ducks.png', out

    # The version is pasted into a query string, so it has to be url-safe.
    for url in (out['own'], out['inherited']):
        assert re.fullmatch(r'/api/units/\d+/logo\?v=[A-Za-z0-9._~%-]*', url), url


def test_the_page_uses_one_helper_in_exactly_the_two_places(src):
    apply_fn = extract(src, r'function applyUnitLogo\(\) \{.*?\n\}', 'applyUnitLogo()')
    assert 'homeLogo' in apply_fn and 'sidebarLogo' in apply_fn, \
        'applyUnitLogo() does not touch both the home screen and the sidebar brand'
    assert apply_fn.count('unitLogoUrl(') == 2, \
        f'the two images must both go through the one helper: {apply_fn}'
    assert 'currentUnit' in apply_fn and 'currentUser.unit_id' in apply_fn, \
        'the home logo follows the top unit and the sidebar follows the open one'
    assert 'onerror' in apply_fn, 'a 404 from the logo route would show a broken image'

    home = extract(src, r'async function loadHome\(\) \{.*?\n\}', 'loadHome()')
    assert 'applyUnitLogo()' in home, 'loadHome() never applies the unit logo'
    assert home.index('loadUnits()') < home.index('applyUnitLogo()'), \
        'applyUnitLogo() runs before the units (and their logos) are loaded'
    select = extract(src, r'function selectUnit\(unit, push = true.*?\n\}', 'selectUnit()')
    assert 'applyUnitLogo()' in select, \
        'changing unit leaves the previous unit\'s logo in the sidebar'

    # The parallel rewrite of the home screen owns renderHome() and #unitCards.
    render_home = extract(src, r'function renderHome\(\) \{.*?\n\}', 'renderHome()')
    assert 'applyUnitLogo' not in render_home and 'unitLogoUrl' not in render_home, \
        'the logo work leaked into renderHome(), which another change owns'

    # Signed-out screens know no tenant, so they keep the built-in mark.
    for screen in ('loginScreen', 'createUnitScreen'):
        block = extract(src, rf'<div id="{screen}".*?</div>\s*</div>', screen)
        assert 'logo-duck.png' in block and 'api/units' not in block, \
            f'{screen} tries to show a tenant logo with no tenant known'


def test_the_box_does_not_move_when_the_logo_does(src):
    """Any aspect ratio up to 512px goes in the same box, or the nav jumps."""
    brand = extract(src, r'\.dash-brand img \{[^}]*\}', '.dash-brand img rule')
    assert 'object-fit: contain' in brand, brand
    assert 'aspect-ratio' in brand, \
        'with height:auto the sidebar box resizes to whatever image lands in it'
    home_img = extract(src, r'<img[^>]*id="homeLogo"[^>]*>', '#homeLogo')
    assert 'object-fit:contain' in home_img.replace(' ', ''), home_img
    assert re.search(r'width:\s*140px', home_img) and re.search(r'height:\s*140px', home_img), \
        'the home logo box must stay 140x140 whatever shape the upload is'


def test_the_upload_never_sends_the_original_file(src):
    up = extract(src, r'async function uploadUnitLogo\(.*?\n\}', 'uploadUnitLogo()')
    assert 'fitWithin(' in src, 'nothing uses the resize maths'
    assert 'toBlob' in src, 'the upload does not re-encode through a canvas'
    assert "'image/png'" in src, 'the canvas is not asked for a PNG'
    assert 'svg' not in up.lower(), 'SVG is a script vector; it must never be accepted'
    assert re.search(r"api\('PUT', `?/units/\$\{[^}]+\}/logo", up), \
        'the upload does not PUT to the unit logo route'

    # Both writes land through the same refresh, or one of them leaves the
    # page showing the logo that was just replaced.
    refresh = extract(src, r'async function refreshUnitLogos\(.*?\n\}', 'refreshUnitLogos()')
    for name, fn in (('uploadUnitLogo()', up),
                     ('removeUnitLogo()',
                      extract(src, r'async function removeUnitLogo\(.*?\n\}', 'removeUnitLogo()'))):
        assert 'refreshUnitLogos()' in fn, f'{name} leaves the page showing the old logo'
    for call in ('loadUnits()', 'applyUnitLogo()', 'renderSettings()'):
        assert call in refresh, f'refreshUnitLogos() never calls {call}'
    assert 'unitById(currentUnit.id)' in refresh, \
        'currentUnit still points at the pre-reload copy, so its logo is stale'

    row = extract(src, r'function logoSettingsRow\(.*?\n\}', 'logoSettingsRow()')
    assert 'accept="image/png,image/jpeg,image/webp"' in row, \
        'the file picker offers formats the flow cannot handle (or refuses ones it can)'
    assert 'type="file"' in row and 'hidden' in row, 'the file input is not the hidden kind'
    assert '512' in row, 'the row never says what the size limit actually is'
    assert 'Remove' in row and 'unit.logo' in row, \
        'Remove is offered for a logo this unit does not own'
    assert 'escapeHtml(' in row, 'a unit name is pasted into the hint unescaped'

    settings = extract(src, r'function renderSettings\(\) \{.*?\n\}', 'renderSettings()')
    assert 'logoSettingsRow()' in settings, 'the Settings page never shows the row'


def test_the_inline_script_still_parses(src, node):
    blocks = re.findall(r'<script>(.*?)</script>', src, re.S)
    assert blocks, 'no inline script in index.html'
    path = os.path.join(tempfile.mkdtemp(), 'spa.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(max(blocks, key=len))
    proc = subprocess.run([node, '--check', path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr


def main():
    src = open(INDEX, encoding='utf-8').read()
    node = shutil.which('node')
    assert node, 'node is required to run the frontend rules (it ships with the CI image)'
    test_helpers_under_node(src, node)
    test_the_page_uses_one_helper_in_exactly_the_two_places(src)
    test_the_box_does_not_move_when_the_logo_does(src)
    test_the_upload_never_sends_the_original_file(src)
    test_the_inline_script_still_parses(src, node)
    print('ok')


if __name__ == '__main__':
    main()

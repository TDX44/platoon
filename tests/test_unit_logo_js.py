"""The unit logo's client side, lifted out of index.html and run under node.

Run with: python tests/test_unit_logo_js.py

Three things are actually tested here rather than grepped for, because grepping
for them proved hollow under review:

  * `fitWithin()` — the resize maths, pure.
  * `logoSettingsRow()` — RENDERED, so "is the unit name escaped" is answered by
    looking at the markup rather than by finding the word escapeHtml nearby.
    This is also where the inherited-owner bug lives: the owner of an inherited
    logo is an ANCESTOR, and /api/units only ever returns the caller's own
    subtree, so looking the owner up in `units` misses every time. The name
    has to arrive on the logo object itself.
  * `uploadUnitLogo()` — DRIVEN, with stubs for the half-dozen browser things
    it touches, so the canvas really is asked for 512x256, the input really is
    cleared, and the error sentence really does match what went wrong.
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')

# HHC owns a logo; 1st Platoon inherits it; 2nd Platoon is in a tenant with
# none at all. Note what is NOT here: unit 1 is absent from PLATOON_UNITS,
# exactly as the server would leave it out of a platoon leader's /api/units.
V = 'a1b2c3d4e5f6'
COMPANY = {'id': 1, 'parent_id': None, 'kind': 'company', 'name': 'HHC', 'count': 1,
           'logo': {'unit_id': 1, 'v': V, 'name': 'HHC'}}
PLATOON_INHERITS = {'id': 2, 'parent_id': 1, 'kind': 'platoon', 'name': '1st Platoon',
                    'count': 2, 'logo': {'unit_id': 1, 'v': V, 'name': 'HHC'}}
PLATOON_OWNS = {'id': 3, 'parent_id': 1, 'kind': 'platoon', 'name': '2nd Platoon',
                'count': 0, 'logo': {'unit_id': 3, 'v': 'ffffffffffff', 'name': '2nd Platoon'}}
PLATOON_NONE = {'id': 4, 'parent_id': 1, 'kind': 'platoon', 'name': '3rd Platoon',
                'count': 0, 'logo': None}

HOSTILE_NAME = '<img src=x onerror=alert(1)>"'
PLATOON_HOSTILE = {'id': 5, 'parent_id': 1, 'kind': 'platoon',
                   'name': 'Quiet ' + HOSTILE_NAME, 'count': 0,
                   'logo': {'unit_id': 1, 'v': V, 'name': 'Loud ' + HOSTILE_NAME}}
# An id and a version that are trying to get out of the src attribute.
PLATOON_NASTY_ID = {'id': 6, 'parent_id': 1, 'kind': 'platoon', 'name': 'Six', 'count': 0,
                    'logo': {'unit_id': '1" onload="alert(1)', 'v': '" onerror="alert(2)',
                             'name': 'Owner'}}

DRIVER = r'''
const out = {};

// ── fitWithin ──
out.wide   = fitWithin(1000, 500, 512);
out.tall   = fitWithin(500, 1000, 512);
out.small  = fitWithin(300, 200, 512);
out.exact  = fitWithin(512, 512, 512);
out.sliver = fitWithin(513, 1, 512);
out.floor  = fitWithin(2000, 1, 512);
out.floor2 = fitWithin(1, 4000, 512);
out.zero   = fitWithin(0, 100, 512);
out.nan    = fitWithin(NaN, NaN, 512);
out.junk   = fitWithin('wide', 'tall', 512);
out.neg    = fitWithin(-10, 20, 512);
out.undef  = fitWithin(undefined, undefined, 512);
out.frac   = fitWithin(1000.7, 500.2, 512);
out.retry  = fitWithin(1000, 500, 256);

// ── unitLogoUrl ──
out.urlOwn       = unitLogoUrl(COMPANY);
out.urlInherited = unitLogoUrl(PLATOON_INHERITS);
out.urlNone      = unitLogoUrl(PLATOON_NONE);
out.urlMissing   = unitLogoUrl(null);
out.urlNasty     = unitLogoUrl(PLATOON_NASTY_ID);
out.builtin      = BUILTIN_LOGO;

// ── logoSettingsRow, rendered ──
// `units` deliberately never contains the company: an inherited logo's owner
// is an ancestor, and the server does not hand those to this caller.
function row(unit, tree) {
  units = tree;
  currentUnit = unit;
  return logoSettingsRow();
}
const SUBTREE = [PLATOON_INHERITS, PLATOON_OWNS, PLATOON_NONE, PLATOON_HOSTILE, PLATOON_NASTY_ID];
out.rowOwn       = row(PLATOON_OWNS, SUBTREE);
out.rowInherited = row(PLATOON_INHERITS, SUBTREE);
out.rowNone      = row(PLATOON_NONE, SUBTREE);
out.rowHostile   = row(PLATOON_HOSTILE, SUBTREE);
out.rowNastyId   = row(PLATOON_NASTY_ID, SUBTREE);
out.rowTopOwner  = row(COMPANY, [COMPANY]);

console.log(JSON.stringify(out));
'''

# uploadUnitLogo() drives a canvas, a file input and the API. All six of those
# are stubbed here, so what is asserted is what the function DID, not what its
# source happens to contain.
UPLOAD_DRIVER = r'''
const out = {};
let alerts, calls, canvases, refreshed, blobFor;

globalThis.showToast = (m) => alerts.push(m);
globalThis.api = async (method, path, body) => { calls.push([method, path, body]); return { success: true }; };
globalThis.refreshUnitLogos = async () => { refreshed += 1; };
globalThis.createImageBitmap = async () => ({ width: 1000, height: 500, close() { closed = true; } });
globalThis.document = {
  createElement: () => {
    const c = {
      width: 0, height: 0, drew: null,
      getContext: () => ({ drawImage: (_b, _x, _y, w, h) => { c.drew = [w, h]; } }),
      toBlob: (cb) => cb(blobFor(c)),
    };
    canvases.push(c);
    return c;
  },
};
let closed = false;
currentUnit = { id: 7, name: 'Seventh' };

function fileInput(type) {
  return { value: 'C:\\fake\\path\\logo.png', files: type ? [{ type }] : [] };
}
function reset(maker) {
  alerts = []; calls = []; canvases = []; refreshed = 0; closed = false;
  blobFor = maker;
}
const okBlob = () => ({ size: 100, arrayBuffer: async () => new Uint8Array([1, 2, 3]).buffer });
const bigBlob = () => ({ size: 500 * 1024, arrayBuffer: async () => new Uint8Array([1]).buffer });

(async () => {
  // 1. The happy path: 1000x500 shrinks to 512x256 on the canvas and is PUT.
  reset(okBlob);
  let input = fileInput('image/png');
  await uploadUnitLogo(input);
  out.happy = { alerts, calls, refreshed, closed, cleared: input.value === '',
                canvas: canvases.map(c => [c.width, c.height]),
                drew: canvases.map(c => c.drew) };

  // 2. Every encoding is over the cap: step down twice, then say so.
  reset(bigBlob);
  input = fileInput('image/png');
  await uploadUnitLogo(input);
  out.tooBig = { alerts, calls: calls.length, canvas: canvases.map(c => [c.width, c.height]) };

  // 3. The encoder returned nothing at all. That is not a size problem and
  //    must not be reported as one.
  reset(() => null);
  input = fileInput('image/png');
  await uploadUnitLogo(input);
  out.noEncode = { alerts, calls: calls.length };

  // 4. A type the flow cannot handle. SVG is the one that matters.
  reset(okBlob);
  input = fileInput('image/svg+xml');
  await uploadUnitLogo(input);
  out.svg = { alerts, calls: calls.length, canvas: canvases.length,
              cleared: input.value === '' };

  // 5. No file at all (the picker was dismissed).
  reset(okBlob);
  input = fileInput(null);
  await uploadUnitLogo(input);
  out.empty = { alerts, calls: calls.length, cleared: input.value === '' };

  console.log(JSON.stringify(out));
})();
'''


# applyUnitLogo() is the other half that only looks like DOM work: which URL
# each of the two images gets, which box shape they are put in, and what
# happens when one 404s are all decisions, so they are driven rather than
# grepped for.
APPLY_DRIVER = r'''
const out = {};
let home, side;

function fakeImg() {
  const cls = new Set();
  return {
    src: '', onerror: null,
    classList: {
      toggle: (c, on) => { if (on) cls.add(c); else cls.delete(c); },
      remove: (c) => cls.delete(c),
    },
    classes: () => [...cls],
  };
}
function snap() {
  return { homeSrc: home.src, homeClasses: home.classes(),
           sideSrc: side.src, sideClasses: side.classes() };
}
function apply(user, unit, tree, missing) {
  units = tree; currentUser = user; currentUnit = unit;
  home = fakeImg(); side = fakeImg();
  globalThis.document = {
    getElementById: (id) => {
      if (missing) return null;
      return id === 'homeLogo' ? home : id === 'sidebarLogo' ? side : null;
    },
  };
  applyUnitLogo();
  return snap();
}

const TREE = [PLATOON_INHERITS, PLATOON_OWNS, PLATOON_NONE];
const AT_PLT = { unit_id: 2 };

// The top unit inherits the company logo; the open unit owns its own.
out.custom = apply(AT_PLT, PLATOON_OWNS, TREE, false);
// Nothing anywhere: both images fall back to the one built-in mark.
out.builtin = apply({ unit_id: 4 }, PLATOON_NONE, TREE, false);
// The sidebar 404s (a logo removed in another tab).
apply(AT_PLT, PLATOON_OWNS, TREE, false);
side.onerror();
out.afterError = { src: side.src, classes: side.classes(), handlerCleared: side.onerror === null };
// Neither element on the page yet (a signed-out or half-built screen).
out.missing = apply(AT_PLT, PLATOON_OWNS, TREE, true);
// No signed-in user at all.
out.noUser = apply(null, null, TREE, false);

console.log(JSON.stringify(out));
'''


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html — was it renamed or removed?'
    return m.group(0)


def lift(src, names):
    return [extract(src, pattern, what) for pattern, what in names]


def run_node(node, js, name):
    path = os.path.join(tempfile.mkdtemp(), name)
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(js)
    proc = subprocess.run([node, path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def render(src, node):
    js = '\n'.join([
        'let units = [];',
        'let currentUnit = null;',
        'const COMPANY = ' + json.dumps(COMPANY) + ';',
        'const PLATOON_INHERITS = ' + json.dumps(PLATOON_INHERITS) + ';',
        'const PLATOON_OWNS = ' + json.dumps(PLATOON_OWNS) + ';',
        'const PLATOON_NONE = ' + json.dumps(PLATOON_NONE) + ';',
        'const PLATOON_HOSTILE = ' + json.dumps(PLATOON_HOSTILE) + ';',
        'const PLATOON_NASTY_ID = ' + json.dumps(PLATOON_NASTY_ID) + ';',
    ] + lift(src, [
        (r'function escapeHtml\(str\) \{.*?\n\}', 'escapeHtml()'),
        (r'function unitById\(id\) \{.*?\n\}', 'unitById()'),
        (r'function unitLabel\(u\) \{.*?\n\}', 'unitLabel()'),
        (r"const BUILTIN_LOGO = '[^']+';", 'BUILTIN_LOGO'),
        (r'function unitLogoUrl\(.*?\n\}', 'unitLogoUrl()'),
        (r'function fitWithin\(.*?\n\}', 'fitWithin()'),
        (r'function logoSettingsRow\(.*?\n\}', 'logoSettingsRow()'),
    ]) + [DRIVER])
    return run_node(node, js, 'logo.js')


def drive_apply(src, node):
    js = '\n'.join([
        'let units = [];',
        'let currentUnit = null;',
        'let currentUser = null;',
        'const PLATOON_INHERITS = ' + json.dumps(PLATOON_INHERITS) + ';',
        'const PLATOON_OWNS = ' + json.dumps(PLATOON_OWNS) + ';',
        'const PLATOON_NONE = ' + json.dumps(PLATOON_NONE) + ';',
    ] + lift(src, [
        (r'function unitById\(id\) \{.*?\n\}', 'unitById()'),
        (r"const BUILTIN_LOGO = '[^']+';", 'BUILTIN_LOGO'),
        (r'function unitLogoUrl\(.*?\n\}', 'unitLogoUrl()'),
        (r'function applyUnitLogo\(\) \{.*?\n\}', 'applyUnitLogo()'),
    ]) + [APPLY_DRIVER])
    return run_node(node, js, 'apply.js')


def drive_upload(src, node):
    js = '\n'.join([
        'let currentUnit = null;',
    ] + lift(src, [
        (r'const LOGO_MAX_PX = \d+;', 'LOGO_MAX_PX'),
        (r'const LOGO_MAX_BYTES = [^;]+;', 'LOGO_MAX_BYTES'),
        (r'function fitWithin\(.*?\n\}', 'fitWithin()'),
        (r'function logoCanvasPng\(.*?\n\}', 'logoCanvasPng()'),
        (r'function bytesToBase64\(.*?\n\}', 'bytesToBase64()'),
        (r'async function uploadUnitLogo\(.*?\n\}', 'uploadUnitLogo()'),
    ]) + [UPLOAD_DRIVER])
    return run_node(node, js, 'upload.js')


# ── fitWithin ──

def test_the_resize_maths(out):
    assert out['wide'] == {'w': 512, 'h': 256}, out['wide']
    assert out['tall'] == {'w': 256, 'h': 512}, out['tall']
    assert out['small'] == {'w': 300, 'h': 200}, \
        f'a small image was scaled UP to the cap: {out["small"]}'
    assert out['exact'] == {'w': 512, 'h': 512}, out['exact']
    assert out['sliver'] == {'w': 512, 'h': 1}, out['sliver']
    # 513x1 rounds to 1 on its own, so it never reaches the floor. A 2000x1
    # strip scales its short edge to 0.256 and does.
    assert out['floor'] == {'w': 512, 'h': 1}, \
        f'the short edge collapsed to zero instead of keeping one pixel: {out["floor"]}'
    assert out['floor2'] == {'w': 1, 'h': 512}, out['floor2']
    assert out['retry'] == {'w': 256, 'h': 128}, \
        f'the smaller retry pass uses the same maths: {out["retry"]}'
    for key in ('zero', 'nan', 'junk', 'neg', 'undef'):
        assert out[key] == {'w': 0, 'h': 0}, \
            f'{key}: a degenerate size must come back as nothing, not NaN: {out[key]}'
    assert out['frac'] == {'w': 512, 'h': 256}, out['frac']
    for key, val in out.items():
        if isinstance(val, dict) and 'w' in val:
            assert float(val['w']).is_integer() and float(val['h']).is_integer(), (key, val)
            assert 0 <= val['w'] <= 512 and 0 <= val['h'] <= 512, (key, val)


def test_the_url(out):
    assert out['urlOwn'] == f'/api/units/1/logo?v={V}', out['urlOwn']
    assert out['urlInherited'] == f'/api/units/1/logo?v={V}', \
        f'a unit with no logo of its own must fetch the one it inherits: {out["urlInherited"]}'
    assert out['urlNone'] == out['builtin'] == '/images/app-logo.png', out
    assert out['urlMissing'] == out['builtin'], \
        'a missing unit must not build a /api/units/undefined URL'
    # The id and version go into a query string and then into an attribute, so
    # nothing that can close one may survive. encodeURIComponent leaves the
    # sub-delims !'()* alone, which are legal in a URL and inert in markup.
    for ch in '"\'<>& ':
        assert ch not in out['urlNasty'], \
            f'{ch!r} survived into the URL, which lands in a src attribute: {out["urlNasty"]}'
    assert re.fullmatch(r"/api/units/[A-Za-z0-9._~%!()*-]+/logo\?v=[A-Za-z0-9._~%!()*-]*",
                        out['urlNasty']), out['urlNasty']


# ── logoSettingsRow, rendered ──

HANDLER_ATTRS = re.compile(r'\son(?:click|change|error|load)="([^"]*)"')


def tags(html):
    return re.findall(r'<(/?[a-zA-Z][\w-]*)', html)


def test_a_unit_that_owns_its_logo(out):
    html = out['rowOwn']
    assert 'Shown for 2nd Platoon' in html, html
    assert 'Inherited from' not in html and 'No logo yet' not in html, html
    assert '>Remove<' in html, 'the owning unit cannot remove its own logo'
    assert 'src="/api/units/3/logo?v=ffffffffffff"' in html, html
    assert '512 &times; 512' in html, 'the row never says what the size limit is'


def test_an_inherited_logo_names_the_unit_it_came_from(out):
    """The bug this replaced: unitById(logo.unit_id) is an ANCESTOR lookup in a
    subtree-only list, so it always missed, and the row printed "No logo yet"
    next to a preview of the company logo."""
    html = out['rowInherited']
    assert 'No logo yet' not in html, \
        'the row denies a logo it is showing a preview of — the owner lookup missed'
    assert 'Inherited from HHC.' in html, \
        f'the owning unit is not named, although the server sent its name: {html}'
    assert 'Inherited from .' not in html and 'Inherited from undefined' not in html, html
    assert '>Remove<' not in html, \
        "Remove is offered for a logo this unit does not own — it would 403 or "\
        "delete somebody else's"
    assert 'src="/api/units/1/logo?v=' in html, 'the preview is not the inherited logo'
    # And it still offers to replace it.
    assert '>Upload<' in html and 'give 1st Platoon its own' in html, html


def test_a_unit_with_no_logo_anywhere(out):
    html = out['rowNone']
    assert 'No logo yet' in html, html
    assert 'Inherited from' not in html and '>Remove<' not in html, html
    assert 'src="/images/app-logo.png"' in html, 'the preview is not the built-in mark'


def test_the_top_unit_reads_as_its_own(out):
    html = out['rowTopOwner']
    assert 'Shown for HHC' in html and '>Remove<' in html, html


def test_no_name_from_the_server_ever_becomes_markup(out):
    """Both names on this row come from the server: the unit's own, and the
    owning unit's, which arrives on the logo object. Dropping escapeHtml from
    either one has to be visible here."""
    html = out['rowHostile']
    assert HOSTILE_NAME not in html, f'a unit name reached the page as markup: {html}'
    # (Searching for the string "onerror=alert(1)" would be wrong here: the
    # correctly ESCAPED name legitimately contains it as text. What matters is
    # that it is not a tag and not an attribute, which is what follows.)
    # One <img> (the preview) and one <input> (the file picker), and no more:
    # an injected name would open a second <img>.
    assert tags(html).count('img') == 1, f'a second tag was opened: {tags(html)}'
    assert tags(html).count('input') == 1, tags(html)
    assert set(tags(html)) <= {'div', '/div', 'img', 'input', 'button', '/button'}, \
        f'an unexpected tag is in the row: {sorted(set(tags(html)))}'
    # Both names, escaped exactly the way escapeHtml escapes them.
    escaped = '&lt;img src=x onerror=alert(1)&gt;&quot;'
    assert f'Inherited from Loud {escaped}.' in html, \
        f'the OWNING unit name is not escaped: {html}'
    assert f'give Quiet {escaped} its own' in html, \
        f'the unit\u2019s own name is not escaped: {html}'


def test_no_id_is_ever_pasted_into_a_handler(out):
    """Every handler on this row is a fixed string. Nothing interpolated, so
    nothing to escape — and if that ever stops being true, this fails."""
    allowed = {
        'uploadUnitLogo(this)',
        "document.getElementById('unitLogoInput').click()",
        'removeUnitLogo()',
        "this.onerror=null;this.src='/images/app-logo.png'",
    }
    for key in ('rowOwn', 'rowInherited', 'rowNone', 'rowHostile', 'rowNastyId', 'rowTopOwner'):
        found = HANDLER_ATTRS.findall(out[key])
        assert found, f'{key}: the row has no handlers at all'
        for handler in found:
            assert handler in allowed, f'{key}: a handler carries interpolated data: {handler!r}'

    # The one place a server value DOES reach an attribute is the preview src.
    html = out['rowNastyId']
    assert 'onload="alert(1)' not in html and 'onerror="alert(2)' not in html, \
        f'the logo id or version broke out of the src attribute: {html}'
    assert tags(html).count('img') == 1, tags(html)


# ── uploadUnitLogo, driven ──

def test_the_upload_shrinks_and_sends_a_png(up):
    happy = up['happy']
    assert happy['alerts'] == [], happy['alerts']
    assert happy['canvas'] == [[512, 256]], \
        f'the canvas was not sized by fitWithin (1000x500 -> 512x256): {happy["canvas"]}'
    assert happy['drew'] == [[512, 256]], \
        f'the bitmap was not drawn at the shrunk size: {happy["drew"]}'
    assert len(happy['calls']) == 1, happy['calls']
    method, path, body = happy['calls'][0]
    assert method == 'PUT' and path == '/units/7/logo', (method, path)
    assert list(body) == ['png_base64'] and body['png_base64'] == 'AQID', body
    assert happy['refreshed'] == 1, 'the page was not refreshed after a successful upload'
    assert happy['closed'], 'the decoded bitmap is never released'
    assert happy['cleared'], \
        'the file input keeps its value, so picking the same file again does nothing'


def test_an_image_that_will_not_fit_steps_down_then_says_so(up):
    big = up['tooBig']
    assert big['canvas'] == [[512, 256], [384, 192], [256, 128]], \
        f'the retry ladder is not 512 -> 384 -> 256: {big["canvas"]}'
    assert big['calls'] == 0, 'an over-cap PNG was sent anyway'
    assert len(big['alerts']) == 1 and '400 KB' in big['alerts'][0], big['alerts']


def test_a_failed_encode_is_not_reported_as_a_size_problem(up):
    """Truthfulness: toBlob returning null is the encoder giving up, which has
    nothing to do with 400 KB. Telling the user to 'try a simpler image' sends
    them off to solve a problem they do not have."""
    no = up['noEncode']
    assert no['calls'] == 0, no
    assert len(no['alerts']) == 1, no['alerts']
    assert '400 KB' not in no['alerts'][0] and 'detailed' not in no['alerts'][0], \
        f'a failed encode was blamed on the image being too big: {no["alerts"][0]!r}'
    assert 'PNG' in no['alerts'][0], f'the message says nothing useful: {no["alerts"][0]!r}'


def test_an_svg_never_reaches_a_canvas_or_the_server(up):
    svg = up['svg']
    assert svg['calls'] == 0 and svg['canvas'] == 0, \
        'an SVG was decoded and uploaded — it is a script vector'
    assert len(svg['alerts']) == 1 and 'PNG' in svg['alerts'][0], svg['alerts']
    assert svg['cleared'], 'the file input was not cleared on the rejected path'


def test_a_dismissed_picker_does_nothing_but_still_clears(up):
    empty = up['empty']
    assert empty['calls'] == 0 and empty['alerts'] == [], empty
    assert empty['cleared'], 'the input keeps a stale value after an empty pick'


# ── applyUnitLogo, driven ──

def test_each_image_gets_the_logo_of_its_own_unit(ap):
    """Two images, two different units: the home screen shows the logo of the
    user's TOP unit, the sidebar shows the logo of the unit that is open."""
    c = ap['custom']
    assert c['homeSrc'] == f'/api/units/1/logo?v={V}', \
        f"the home screen is not showing the top unit's resolved logo: {c['homeSrc']}"
    assert c['sideSrc'] == '/api/units/3/logo?v=ffffffffffff', \
        f'the sidebar is not showing the open unit’s own logo: {c["sideSrc"]}'


def test_an_upload_and_the_built_in_mark_go_in_the_same_box(ap):
    """Both marks are square now — the built-in one by construction, an upload
    because the validator caps it at a 512 square — so the sidebar has one box
    and applyUnitLogo() swaps nothing but the src. A class appearing on either
    image means a second box shape has come back."""
    assert ap['custom']['sideSrc'] == '/api/units/3/logo?v=ffffffffffff', ap['custom']
    assert ap['builtin']['sideSrc'] == '/images/app-logo.png', ap['builtin']
    assert ap['builtin']['homeSrc'] == '/images/app-logo.png', ap['builtin']
    for key in ('custom', 'builtin'):
        assert ap[key]['sideClasses'] == [] and ap[key]['homeClasses'] == [], \
            f'{key}: applyUnitLogo() is putting a class on the image again: {ap[key]}'


def test_a_logo_that_404s_falls_back_to_the_built_in_mark(ap):
    err = ap['afterError']
    assert err['src'] == '/images/app-logo.png', \
        f'a 404 leaves a broken image in the sidebar: {err["src"]}'
    assert err['classes'] == [], \
        f'the fallback left a class behind: {err["classes"]}'
    assert err['handlerCleared'] is True, \
        'the handler is not cleared, so a failing fallback loops forever'


def test_it_survives_a_page_that_has_neither_image_yet(ap):
    assert ap['missing'] == {'homeSrc': '', 'homeClasses': [],
                             'sideSrc': '', 'sideClasses': []}, ap['missing']
    assert ap['noUser']['homeSrc'] == '/images/app-logo.png', \
        'with nobody signed in the home screen must still show the built-in mark'


# ── Where the two images live ──

def test_the_page_uses_one_helper_in_exactly_the_two_places(src):
    apply_fn = extract(src, r'function applyUnitLogo\(\) \{.*?\n\}', 'applyUnitLogo()')
    assert 'homeLogo' in apply_fn and 'sidebarLogo' in apply_fn, \
        'applyUnitLogo() does not touch both the home screen and the sidebar brand'
    assert apply_fn.count('unitLogoUrl(') == 2, \
        f'the two images must both go through the one helper: {apply_fn}'
    assert 'currentUnit' in apply_fn and 'currentUser.unit_id' in apply_fn, \
        'the home logo follows the top unit and the sidebar follows the open one'
    home = extract(src, r'async function loadHome\(\) \{.*?\n\}', 'loadHome()')
    assert 'applyUnitLogo()' in home, 'loadHome() never applies the unit logo'
    assert home.index('loadUnits()') < home.index('applyUnitLogo()'), \
        'applyUnitLogo() runs before the units (and their logos) are loaded'
    select = extract(src, r'function selectUnit\(unit, push = true.*?\n\}', 'selectUnit()')
    assert 'applyUnitLogo()' in select, \
        "changing unit leaves the previous unit's logo in the sidebar"

    render_home = extract(src, r'function renderHome\(\) \{.*?\n\}', 'renderHome()')
    assert 'applyUnitLogo' not in render_home and 'unitLogoUrl' not in render_home, \
        'the logo work leaked into renderHome(), which another change owns'

    for screen in ('loginScreen', 'createUnitScreen'):
        block = extract(src, rf'<div id="{screen}".*?</div>\s*</div>', screen)
        assert 'app-logo.png' in block and 'api/units' not in block, \
            f'{screen} tries to show a tenant logo with no tenant known'


def test_the_box_does_not_move_when_the_logo_does(src):
    """Any aspect ratio up to 512px goes in a box of fixed shape, or the nav
    jumps as the image lands. One box now: both the built-in mark and every
    upload the validator accepts fit a square."""
    brand = extract(src, r'\.dash-brand img \{[^}]*\}', '.dash-brand img rule')
    assert 'object-fit: contain' in brand, brand
    assert 'aspect-ratio: 1 / 1' in brand, \
        'with height:auto the sidebar box resizes to whatever image lands in it'
    assert 'brand-custom' not in src, \
        'the second box shape is back; there is only one mark shape now'

    home_img = extract(src, r'<img[^>]*id="homeLogo"[^>]*>', '#homeLogo')
    assert 'object-fit:contain' in home_img.replace(' ', ''), home_img
    assert re.search(r'width:\s*140px', home_img) and re.search(r'height:\s*140px', home_img), \
        'the home logo box must stay 140x140 whatever shape the upload is'


def test_both_writes_go_through_the_same_reload(src):
    up = extract(src, r'async function uploadUnitLogo\(.*?\n\}', 'uploadUnitLogo()')
    rm = extract(src, r'async function removeUnitLogo\(.*?\n\}', 'removeUnitLogo()')
    refresh = extract(src, r'async function refreshUnitLogos\(.*?\n\}', 'refreshUnitLogos()')
    for name, fn in (('uploadUnitLogo()', up), ('removeUnitLogo()', rm)):
        assert 'refreshUnitLogos()' in fn, f'{name} leaves the page showing the old logo'
    for call in ('reloadUnits()', 'applyUnitLogo()', 'renderSettings()'):
        assert call in refresh, f'refreshUnitLogos() never calls {call}'

    # One helper re-pulls the tree and re-points currentUnit at the fresh copy.
    reload_fn = extract(src, r'async function reloadUnits\(\) \{.*?\n\}', 'reloadUnits()')
    assert 'loadUnits()' in reload_fn and 'unitById(currentUnit.id)' in reload_fn, reload_fn
    for name, pattern in (('saveHeaderRename()', r'async function saveHeaderRename\(\) \{.*?\n\}'),
                          ('refreshUnitsPage()', r'async function refreshUnitsPage\(\) \{.*?\n\}')):
        fn = extract(src, pattern, name)
        assert 'reloadUnits()' in fn, f'{name} re-pulls the tree its own way'
        assert 'unitById(currentUnit.id)' not in fn, \
            f'{name} still carries its own copy of the re-point'

    row = extract(src, r'function logoSettingsRow\(.*?\n\}', 'logoSettingsRow()')
    assert 'accept="image/png,image/jpeg,image/webp"' in row, \
        'the file picker offers formats the flow cannot handle (or refuses ones it can)'
    assert 'type="file"' in row and 'hidden' in row, 'the file input is not the hidden kind'
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
    out = render(src, node)
    test_the_resize_maths(out)
    test_the_url(out)
    test_a_unit_that_owns_its_logo(out)
    test_an_inherited_logo_names_the_unit_it_came_from(out)
    test_a_unit_with_no_logo_anywhere(out)
    test_the_top_unit_reads_as_its_own(out)
    test_no_name_from_the_server_ever_becomes_markup(out)
    test_no_id_is_ever_pasted_into_a_handler(out)
    up = drive_upload(src, node)
    test_the_upload_shrinks_and_sends_a_png(up)
    test_an_image_that_will_not_fit_steps_down_then_says_so(up)
    test_a_failed_encode_is_not_reported_as_a_size_problem(up)
    test_an_svg_never_reaches_a_canvas_or_the_server(up)
    test_a_dismissed_picker_does_nothing_but_still_clears(up)
    ap = drive_apply(src, node)
    test_each_image_gets_the_logo_of_its_own_unit(ap)
    test_an_upload_and_the_built_in_mark_go_in_the_same_box(ap)
    test_a_logo_that_404s_falls_back_to_the_built_in_mark(ap)
    test_it_survives_a_page_that_has_neither_image_yet(ap)
    test_the_page_uses_one_helper_in_exactly_the_two_places(src)
    test_the_box_does_not_move_when_the_logo_does(src)
    test_both_writes_go_through_the_same_reload(src)
    test_the_inline_script_still_parses(src, node)
    print('ok')


if __name__ == '__main__':
    main()

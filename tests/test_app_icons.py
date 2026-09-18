"""The built-in logo and app icons exist, and are the size everything claims.

A favicon or a manifest icon fails silently: the browser asks, gets a 404 into
the SPA fallback, and shows its own blank square. Nobody notices until the app
is on a home screen looking like a broken page. So every path that any of the
five places below hands a browser is opened here and its real pixel size read
out of the PNG header.

Stdlib only, on purpose — `struct` on the IHDR is exactly what
`server._validate_logo_png()` does, and the reason there is no image library
in requirements.txt.

Run with: python tests/test_app_icons.py
"""
import json
import os
import re
import struct
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dbharness  # noqa: E402
_schema = dbharness.setup()

import server  # noqa: E402  (must follow the DATA_DIR override)

PNG_SIGNATURE = b'\x89PNG\r\n\x1a\n'

# A Company's own duck mascot was the built-in art. It is that unit's identity,
# not the app's, and nothing shipped names the unit running the instance — so
# the files are gone and no reference to them may creep back in a merge.
RETIRED_ART = ('logo-duck', 'sidebar-ducks', 'company-logo')


def read(name):
    with open(os.path.join(ROOT, name), encoding='utf-8') as fh:
        return fh.read()


def png_size(path):
    """(width, height) straight out of the IHDR, or a raised AssertionError."""
    with open(path, 'rb') as fh:
        head = fh.read(24)
    assert head.startswith(PNG_SIGNATURE), f'{path} is not a PNG'
    assert head[12:16] == b'IHDR', f'{path} does not open with an IHDR chunk'
    return struct.unpack('>II', head[16:24])


# ── Every claim anything makes about an icon ──
# (the file, the size it was promised as or None, who promised it)

def claims():
    out = []

    manifest = json.loads(read('manifest.json'))
    for icon in manifest['icons']:
        w, h = icon['sizes'].split('x')
        out.append((icon['src'], (int(w), int(h)),
                    f"manifest {icon['src']} purpose={icon['purpose']}"))

    # <link rel="icon"> / <link rel="apple-touch-icon"> on the app and on every
    # signed-out page, which are plain HTML and drift on their own.
    pages = ['index.html'] + [f'public/{n}' for n in sorted(os.listdir(
        os.path.join(ROOT, 'public'))) if n.endswith('.html')]
    for page in pages:
        src = read(page)
        for tag in re.findall(r'<link[^>]*rel="(?:apple-touch-)?icon"[^>]*>', src):
            href = re.search(r'href="([^"]+)"', tag)
            assert href, f'{page}: an icon <link> with no href: {tag}'
            size = re.search(r'sizes="(\d+)x(\d+)"', tag)
            out.append((href.group(1),
                        (int(size.group(1)), int(size.group(2))) if size else None,
                        f'{page} {tag}'))

        # Plain <img src="/images/..."> — the login mark, the home mark, the
        # create-unit mark and the install button all point at a file too.
        for src_attr in re.findall(r'<img[^>]+src="(/images/[^"]+)"', src):
            out.append((src_attr, None, f'{page} <img src="{src_attr}">'))

    # BUILTIN_LOGO is what every one of those images falls back to when a unit
    # has no logo, and what applyUnitLogo() rewrites a 404 to.
    builtin = re.search(r"const BUILTIN_LOGO = '([^']+)';", read('index.html'))
    assert builtin, 'index.html no longer defines BUILTIN_LOGO'
    out.append((builtin.group(1), None, 'index.html BUILTIN_LOGO'))

    # The service worker precaches nothing today (it is a self-destruct stub),
    # but the moment it lists an asset that asset has to exist.
    for path in re.findall(r'["\'](/images/[^"\']+)["\']', read('sw.js')):
        out.append((path, None, f'sw.js precache {path}'))

    return out


def test_every_icon_anything_references_exists_at_the_size_it_claims():
    seen = set()
    for path, size, who in claims():
        # Ask the allowlist itself rather than restating it: spa_fallback()
        # serves a real file only for these, and anything else silently
        # becomes the SPA shell. A literal '/images/' here stayed green even
        # when STATIC_DIRS no longer contained it.
        rel = path.lstrip('/')
        assert rel in server.STATIC_FILES or rel.startswith(server.STATIC_DIRS), \
            f'{who}: {path} is not on the STATIC_DIRS/STATIC_FILES allowlist ' \
            f'({server.STATIC_DIRS}, {server.STATIC_FILES}) — the browser gets the SPA shell'
        on_disk = os.path.join(ROOT, path.lstrip('/'))
        assert os.path.isfile(on_disk), \
            f'{who}: points at {path}, which is not on disk — the browser gets ' \
            'the SPA fallback and shows a blank square'
        if not rel.endswith('.png'):
            # The marketing screenshots under images/site/ are WebP. They still
            # have to exist and still have to be on the allowlist -- both checked
            # above, which is the part that keeps a typo from silently becoming
            # the SPA shell -- but there is no PNG header to read and nothing
            # declares a size for them.
            assert size is None, f'{who}: declares a pixel size but is not a PNG'
            seen.add(path)
            continue
        actual = png_size(on_disk)
        if size:
            assert actual == size, \
                f'{who}: declared {size[0]}x{size[1]} but the file is ' \
                f'{actual[0]}x{actual[1]}'
        seen.add(path)
    assert len(seen) >= 4, f'only {len(seen)} icons referenced; something stopped matching'


def test_the_built_in_logo_would_pass_the_apps_own_upload_validator():
    """The default mark and a unit's upload land in the same <img> boxes, so
    the default has to be the same shape of thing the app accepts: a PNG, at
    most 512 square, at most 400 KB. If the built-in one would be rejected on
    upload, the boxes were never designed for it."""
    import base64
    builtin = re.search(r"const BUILTIN_LOGO = '([^']+)';", read('index.html')).group(1)
    with open(os.path.join(ROOT, builtin.lstrip('/')), 'rb') as fh:
        raw = fh.read()
    _, width, height = server._validate_logo_png(base64.b64encode(raw).decode())
    assert width <= server.LOGO_MAX_PX and height <= server.LOGO_MAX_PX, (width, height)
    assert len(raw) <= server.LOGO_MAX_BYTES, len(raw)


def test_nothing_still_reaches_for_the_retired_duck_art():
    hits = []
    for base, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in ('.git', '__pycache__', 'backups', 'data')]
        for name in files:
            full = os.path.join(base, name)
            rel = os.path.relpath(full, ROOT)
            if rel == os.path.join('tests', os.path.basename(__file__)):
                continue        # this file names them in order to ban them
            for art in RETIRED_ART:
                if art in name:
                    hits.append(f'{rel} (the file itself)')
                    continue
            try:
                with open(full, encoding='utf-8') as fh:
                    body = fh.read()
            except (UnicodeDecodeError, OSError):
                continue        # a PNG is not text; its name was checked above
            for art in RETIRED_ART:
                if art in body:
                    hits.append(f'{rel} still names {art}')
    assert not hits, 'the retired art is still referenced:\n  ' + '\n  '.join(hits)


if __name__ == '__main__':
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn):
            fn()
    print('ok')

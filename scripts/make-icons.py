#!/usr/bin/env python3
"""Regenerate every built-in logo/icon in images/ from one square master.

Pillow is a DEV-ONLY tool here and is deliberately NOT in requirements.txt
(that file is the production install list). Run it from a scratch venv
outside the repo:

    python3 -m venv /tmp/icon-venv && /tmp/icon-venv/bin/pip install pillow
    /tmp/icon-venv/bin/python scripts/make-icons.py            # from the master
    /tmp/icon-venv/bin/python scripts/make-icons.py NEW-ART.png  # from new art

images/app-logo.png IS the master: the 512px square crop, committed, shipped
as the default logo, and the manifest's 512 "any" icon. With no argument the
script re-derives the smaller and full-bleed icons from it and leaves it
alone, which makes regeneration a fixed point — run it twice and nothing
changes. Given a path it re-crops that file to the artwork's alpha bounding
box (exports from an image tool carry a wide transparent margin that is part
of the file, not of the icon) and rewrites the master as well.

Nothing ships above 512px and the app's own upload validator caps a unit's
logo at 512 too, so a 512 master loses nothing. Re-run from the original
artwork if a larger asset is ever needed.

Three shapes come out of the one master, because three consumers want
different things:

  * transparent corners  — the rounded square as drawn, for the web page and
    for manifest `purpose: any`, where the browser puts it on its own surface.
  * full-bleed, safe zone — maskable and apple-touch. Both crop the image to
    a platform shape we do not control, so the field runs to all four edges
    (no black corners on iOS, no white ring under an Android mask) and the
    artwork is scaled down until it clears the crop.
  * 32px favicon — a plain downscale of the whole mark.
"""
import os
import sys

from PIL import Image

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IMAGES = os.path.join(REPO, 'images')
MASTER = os.path.join(IMAGES, 'app-logo.png')
MASTER_PX = 512

# A pixel counts as artwork when it is opaque and markedly brighter than the
# dark-green field: the white shield/figures and the four arc colours all are,
# every shade of the field is not. That is how the script finds what has to
# stay inside a safe zone without anybody hand-typing a bounding box.
INK_MIN_BRIGHTNESS = 120
INK_MIN_ALPHA = 200

# Maskable spec: the guaranteed-visible region is a circle of 80% diameter, so
# the artwork's *diagonal* is what has to fit, not its width.
MASKABLE_SAFE_DIAMETER = 0.80
# iOS clips to a squircle and is far less aggressive than an Android circle
# mask, so the same 56% would look like a stamp on an envelope. 68% keeps the
# artwork clear of the rounded-off corners and still fills the tile.
APPLE_INK_SPAN = 0.68


def square_master(path):
    """Crop to the artwork's alpha box, pad to a square, scale to MASTER_PX."""
    im = Image.open(path).convert('RGBA')
    box = im.getchannel('A').point(lambda v: 255 if v > 8 else 0).getbbox()
    if not box:
        raise SystemExit(f'{path}: every pixel is transparent')
    art = im.crop(box)
    side = max(art.size)
    out = Image.new('RGBA', (side, side), (0, 0, 0, 0))
    out.paste(art, ((side - art.width) // 2, (side - art.height) // 2))
    if side == MASTER_PX:
        return out          # already the master; resizing it would only blur
    return out.resize((MASTER_PX, MASTER_PX), Image.LANCZOS)


def ink_box(master):
    """Bounding box of the artwork inside the field."""
    px = master.load()
    mask = Image.new('L', master.size, 0)
    mp = mask.load()
    for y in range(master.height):
        for x in range(master.width):
            r, g, b, a = px[x, y]
            if a >= INK_MIN_ALPHA and max(r, g, b) > INK_MIN_BRIGHTNESS:
                mp[x, y] = 255
    box = mask.getbbox()
    if not box:
        raise SystemExit('found no artwork inside the field')
    return box


def field_colour(master):
    """The field colour, sampled from the flat area just inside the edge."""
    w, h = master.size
    pts = [(int(w * f), int(h * g)) for f, g in
           ((0.06, 0.5), (0.5, 0.06), (0.94, 0.5), (0.5, 0.94))]
    hits = [master.getpixel(p) for p in pts]
    return tuple(sum(c[i] for c in hits) // len(hits) for i in range(3)) + (255,)


# Flat art, so a 256-colour palette is visually lossless and roughly 16x
# smaller. Re-running the script on its own output is stable: the palette is
# already within 256 colours, so nothing degrades a second time.
PALETTE_COLOURS = 256


def save(im, name):
    path = os.path.join(IMAGES, name)
    im.quantize(colors=PALETTE_COLOURS, method=Image.FASTOCTREE).save(
        path, 'PNG', optimize=True)
    print(f'  {name:<26} {im.width}x{im.height}  {os.path.getsize(path):>7,} B')


def transparent(master, size):
    """The rounded square as drawn, corners still transparent."""
    return master.resize((size, size), Image.LANCZOS)


def full_bleed(master, box, size, ink_span):
    """Field to all four edges, artwork scaled to `ink_span` of the canvas.

    The whole master is scaled and pasted, not just the cropped artwork, so
    there is no seam — everything around the artwork is the same field colour
    that fills the canvas.
    """
    scale = (size * ink_span) / max(box[2] - box[0], box[3] - box[1])
    big = master.resize((max(1, round(master.width * scale)),
                         max(1, round(master.height * scale))), Image.LANCZOS)
    cx = (box[0] + box[2]) / 2 * scale
    cy = (box[1] + box[3]) / 2 * scale
    out = Image.new('RGBA', (size, size), field_colour(master))
    out.alpha_composite(big, (round(size / 2 - cx), round(size / 2 - cy)))
    return out


def maskable_span(box):
    """Ink span that puts the artwork's diagonal inside the safe circle."""
    w, h = box[2] - box[0], box[3] - box[1]
    return MASKABLE_SAFE_DIAMETER * max(w, h) / (w ** 2 + h ** 2) ** 0.5


def main():
    if len(sys.argv) > 1:
        # The default logo: login, create-unit, home and the sidebar all show
        # this one file, and a unit's own upload replaces it at runtime.
        save(square_master(sys.argv[1]), os.path.basename(MASTER))
    # Always read the master back off disk, so the very first run derives from
    # exactly the bytes every later run will — otherwise this run resamples an
    # un-quantized image and the next one resamples the quantized file, and
    # every icon shows up as changed for no visible reason.
    master = Image.open(MASTER).convert('RGBA')
    box = ink_box(master)
    print('field #%02x%02x%02x' % field_colour(master)[:3])
    # No icon-512.png: the manifest's 512 "any" entry is app-logo.png itself.
    # The same square twice under two names is one file to forget to redo.
    save(transparent(master, 32), 'icon-32.png')
    save(transparent(master, 192), 'icon-192.png')
    save(full_bleed(master, box, 180, APPLE_INK_SPAN), 'icon-180.png')
    save(full_bleed(master, box, 512, maskable_span(box)), 'icon-maskable-512.png')


if __name__ == '__main__':
    main()

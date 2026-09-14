"""Mobile-layout geometry checks — catches the class of bug that only ever
showed up on the user's phone, never in hand-driven Chrome emulation:
a date input wider than the viewport, a mobile/desktop CSS pair that were
both visible at once (duplicated row metadata), and content bleeding past
the screen edge.

This is NOT screenshot/baseline regression testing (see CLAUDE.md) — it
asserts geometric facts (bounding boxes vs. viewport, computed visibility)
that are true of a correct layout and false of every bug above, so it does
not rot the way a pixel baseline does.

The app requires Clerk auth, so a plain browser load renders nothing. This
drives the SPA the same way the frontend drives itself: it starts a real
server, loads '/', then pokes the page's own globals (`personnel`,
`currentPlatoon`, `render()`) exactly the way `load()` would after a real
login, and stubs `api()` for the one extra network call the directory view
makes. server.py is not touched or weakened.

Run with: python tests/test_mobile_layout.py
Requires Playwright + a chromium browser (dev-only, not in requirements.txt):
    pip install playwright && playwright install chromium
Without those installed, this prints "ok (skipped: ...)" and exits 0.
"""
import os
import socket
import sys
import tempfile
import threading
import time
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['DATA_DIR'] = tempfile.mkdtemp()

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import sync_playwright
except ImportError:
    print('ok (skipped: playwright not installed)')
    sys.exit(0)

import server  # noqa: E402  (must follow the DATA_DIR override)

# The app answers on the unit's clock (server.app_today()), so the tests
# must ask the same question. Using date.today() here made CI fail on its
# UTC runner every evening between 1900 and midnight Central.
TODAY = date.fromisoformat(server.app_today())


def day(offset):
    return (TODAY + timedelta(days=offset)).isoformat()


LONG_NOTE = 'Attending Advanced Individual Readiness and Combatives Recertification Course, extended stay pending follow-on orders'

# One person per status the roster renders, plus one carrying a future
# scheduled_events entry, plus long-content stress (long notes, long last name).
# Names are obviously fake. Shape matches what index.html's load() builds
# (personnel = data.map(...)), which is what we bypass by writing it directly.
PERSONNEL_FIXTURE = [
    {'id': 1, 'rank': 'SPC', 'last': 'Testperson-Featherstonehaugh', 'first': 'Wanda',
     'status': 'tdy', 'notes': LONG_NOTE, 'from': day(-1), 'to': day(6),
     'present_date': '', 'scheduled_events': []},
    {'id': 2, 'rank': 'SGT', 'last': 'Fixtureton', 'first': 'Ray',
     'status': 'present', 'notes': '', 'from': '', 'to': '',
     'present_date': TODAY.isoformat(), 'scheduled_events': []},
    {'id': 3, 'rank': 'PFC', 'last': 'Placeholder', 'first': 'Nia',
     'status': 'present', 'notes': '', 'from': '', 'to': '',
     'present_date': '', 'scheduled_events': []},
    {'id': 4, 'rank': 'CPL', 'last': 'Sampleford', 'first': 'Kai',
     'status': 'leave', 'notes': 'Block leave', 'from': day(2), 'to': day(10),
     'present_date': '', 'scheduled_events': []},
    {'id': 5, 'rank': 'SPC', 'last': 'Dummyval', 'first': 'Theo',
     'status': 'pass', 'notes': '', 'from': day(0), 'to': day(1),
     'present_date': '', 'scheduled_events': []},
    {'id': 6, 'rank': 'SSG', 'last': 'Exampleson', 'first': 'Priya',
     'status': 'other', 'notes': 'Staff Duty Recovery', 'from': day(-1), 'to': day(0),
     'present_date': '', 'scheduled_events': []},
    {'id': 7, 'rank': 'SGT', 'last': 'Mockridge', 'first': 'Dev',
     'status': 'ftr', 'notes': '', 'from': day(-3), 'to': day(-1),
     'present_date': '', 'scheduled_events': []},
    {'id': 9, 'rank': 'SPC', 'last': 'Placeholderman', 'first': 'Ola',
     'status': 'present', 'notes': '', 'from': '', 'to': '',
     'present_date': TODAY.isoformat(),
     'scheduled_events': [{'id': 99, 'person_id': 9, 'platoon': '2nd', 'status': 'tdy',
                            'from_date': day(14), 'to_date': day(22),
                            'notes': 'IO - Dothan, AL', 'location': '', 'state': 'scheduled'}]},
]


def _next_absence(p):
    if not p['scheduled_events']:
        return None
    e = p['scheduled_events'][0]
    return {'status': e['status'], 'from_date': e['from_date'], 'to_date': e['to_date']}


# What GET /api/directory returns — same fixture, backend field names.
DIRECTORY_FIXTURE = [
    {'id': p['id'], 'rank': p['rank'], 'last': p['last'], 'first': p['first'],
     'status': p['status'], 'from_date': p['from'], 'to_date': p['to'], 'notes': p['notes'],
     'dod_id': '1234567890', 'dob': None, 'mos': '35F', 'section': 'S2', 'phone': '',
     'next_absence': _next_absence(p)}
    for p in PERSONNEL_FIXTURE
]

# What GET /api/availability returns, built from the same people.
AVAILABILITY_FIXTURE = {
    'platoon': '2nd', 'date': day(3), 'to': day(9), 'span': 7,
    'available': [{'id': p['id'], 'rank': p['rank'], 'last': p['last'], 'first': p['first']}
                  for p in PERSONNEL_FIXTURE if p['status'] == 'present'],
    'unavailable': [{'id': p['id'], 'rank': p['rank'], 'last': p['last'], 'first': p['first'],
                     'status': p['status'], 'from_date': p['from'], 'to_date': p['to'],
                     'notes': p['notes'], 'location': '',
                     'days': [day(3), day(4)], 'whole_range': False}
                    for p in PERSONNEL_FIXTURE
                    if p['status'] in ('tdy', 'leave', 'pass', 'other', 'ftr')],
}

WIDTHS = [320, 390, 1280]
# ponytail: honest current floor, not an aspirational one. Measured directly
# against this app at 320/390px: the shortest real button today is the
# "Set Status" / "Mark Present" pair (.dash-btn-sm) at 29px; everything else
# (row-menu triggers, section headers, bottom nav) is 34px+. 28px gives 1px of
# rendering slack. Raise this only once .dash-btn-sm is redesigned taller.
MIN_TAP_TARGET_PX = 28


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start_server():
    """Real HTTP server (a browser needs one — app.test_client() cannot serve
    a page for Playwright to load), started and torn down cleanly."""
    import logging
    from werkzeug.serving import make_server
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    port = free_port()
    httpd = make_server('127.0.0.1', port, server.app)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    for _ in range(100):
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=0.2):
                return httpd, thread, port
        except OSError:
            time.sleep(0.05)
    raise RuntimeError('server did not start')


def stop_server(httpd, thread):
    httpd.shutdown()
    thread.join(timeout=5)


INIT_JS = """
(fixture) => {
  window.api = async (method, path) => {
    if (path.startsWith('/directory')) return fixture.directory;
    if (path.startsWith('/availability')) return fixture.availability;
    return [];
  };
  personnel = fixture.personnel;
  currentPlatoon = '2nd';
  document.getElementById('loginScreen').style.display = 'none';
  document.getElementById('platoonScreen').style.display = 'block';
  render();
}
"""


def overflowing_elements(page):
    """Elements whose right edge extends past the viewport (1px slack).

    Skips anything inside a deliberate horizontal-scroll container
    (overflow-x: auto/scroll, e.g. the desktop directory table) — that
    content is meant to scroll within its own box, not bleed past the
    screen the way the real bugs did.
    """
    return page.evaluate("""
    () => {
      const vw = window.innerWidth;
      const bad = [];
      const inScrollContainer = (el) => {
        for (let a = el.parentElement; a; a = a.parentElement) {
          const ox = getComputedStyle(a).overflowX;
          if (ox === 'auto' || ox === 'scroll') return true;
        }
        return false;
      };
      document.querySelectorAll('body *').forEach(el => {
        const r = el.getBoundingClientRect();
        if (r.width === 0 && r.height === 0) return;
        if (r.right > vw + 1 && !inScrollContainer(el)) {
          const cls = (el.className && typeof el.className === 'string')
            ? '.' + el.className.trim().split(/\\s+/).join('.') : '';
          bad.push((el.id ? '#' + el.id : '') + cls || el.tagName);
        }
      });
      return bad;
    }
    """)


def check_no_horizontal_overflow(page, width, view_label):
    scroll_w, inner_w = page.evaluate('[document.documentElement.scrollWidth, window.innerWidth]')
    assert scroll_w <= inner_w + 1, (
        f'{view_label} @ {width}px: page scrolls horizontally '
        f'(scrollWidth={scroll_w} > innerWidth={inner_w})'
    )
    bad = overflowing_elements(page)
    assert not bad, f'{view_label} @ {width}px: element(s) overflow the viewport: {bad}'


def check_no_duplicate_meta(page, width):
    """The real bug: a base rule after its media query left both the desktop
    .dash-row-meta and the phone lines visible together, so every row showed
    its status twice."""
    results = page.evaluate("""
    () => Array.from(document.querySelectorAll('.dash-row')).map(row => {
      const meta = row.querySelector('.dash-row-meta');
      const mobile = row.querySelector('.dash-row-mobiletop');
      const visible = el => !!el && getComputedStyle(el).display !== 'none';
      return { meta: visible(meta), mobile: visible(mobile) };
    })
    """)
    assert results, f'no .dash-row elements found @ {width}px — fixture or render() is broken'
    for i, r in enumerate(results):
        assert r['meta'] != r['mobile'], (
            f'.dash-row[{i}] @ {width}px: .dash-row-meta visible={r["meta"]} and '
            f'.dash-row-mobiletop visible={r["mobile"]} — exactly one must be visible'
        )


def check_rows_are_one_height(page, width):
    """Every row inside a section must be exactly the same height — that is what
    stopped the list jittering when a long location wrapped.

    Sections differ from each other on purpose and are checked separately:
    Present for Duty carries no location or dates so it has no detail line,
    and Needs Action gets a full-width second line for its two buttons."""
    groups = page.evaluate("""
    () => {
      const g = {};
      document.querySelectorAll('.dash-section').forEach(s => {
        const title = s.querySelector('.dash-section-title').textContent;
        const hs = Array.from(s.querySelectorAll('.dash-row'))
                        .map(r => Math.round(r.getBoundingClientRect().height));
        if (hs.length) g[title] = hs;
      });
      return g;
    }
    """)
    assert groups, f'no sections rendered @ {width}px — fixture or render() is broken'
    for title, heights in groups.items():
        distinct = sorted(set(heights))
        assert len(distinct) == 1, (
            f'"{title}" rows @ {width}px are not a single height: {distinct}'
        )


def check_modal_controls_fit(page, width):
    page.evaluate('openTdyLeave(0)')
    bad = page.evaluate("""
    () => {
      const vw = window.innerWidth;
      const modal = document.querySelector('#tdyLeaveModal.active');
      if (!modal) return ['modal did not open'];
      const bad = [];
      modal.querySelectorAll('input, select, button').forEach(el => {
        const r = el.getBoundingClientRect();
        if (r.width === 0 && r.height === 0) return;
        if (r.right > vw + 1) {
          bad.push((el.id ? '#' + el.id : el.tagName) + ' right=' + Math.round(r.right) + ' vw=' + vw);
        }
      });
      return bad;
    }
    """)
    page.evaluate('closeTdyLeaveModal()')
    assert not bad, f'TDY/Leave modal @ {width}px: control(s) wider than viewport: {bad}'


def check_tap_targets(page, width):
    bad = page.evaluate(f"""
    () => {{
      const bad = [];
      document.querySelectorAll('#personnelBody button, .dash-header-actions button, .dash-bottomnav button').forEach(el => {{
        const r = el.getBoundingClientRect();
        if (r.width === 0 && r.height === 0) return;  // not visible
        if (r.height < {MIN_TAP_TARGET_PX}) {{
          bad.push((el.id ? '#' + el.id : el.className || el.tagName) + ' height=' + r.height.toFixed(1));
        }}
      }});
      return bad;
    }}
    """)
    assert not bad, f'accountability @ {width}px: button(s) under {MIN_TAP_TARGET_PX}px tall: {bad}'


def check_formation_fits(page, width, height, label):
    """Formation mode is used one-handed in front of the platoon: every button
    must be fully on screen without scrolling, including the five stacked
    reason buttons, which are the tallest thing the app draws."""
    bad = page.evaluate("""
    () => {
      const o = document.getElementById('formationOverlay');
      if (!o || !o.innerText.trim()) return ['formation overlay is empty'];
      const bad = [];
      o.querySelectorAll('button').forEach(el => {
        const r = el.getBoundingClientRect();
        if (r.width === 0 && r.height === 0) return;
        if (r.bottom > window.innerHeight + 1 || r.right > window.innerWidth + 1 || r.top < -1) {
          bad.push((el.className || el.tagName) + ' "' + el.innerText.trim().slice(0, 14) + '"');
        }
      });
      if (o.scrollHeight > o.clientHeight + 1) bad.push('overlay scrolls: ' + o.scrollHeight + ' > ' + o.clientHeight);
      return bad;
    }
    """)
    assert not bad, f'formation mode ({label}) @ {width}x{height}: off-screen or clipped: {bad}'


def run_checks(page, base_url):
    fixture = {'personnel': PERSONNEL_FIXTURE, 'directory': DIRECTORY_FIXTURE,
               'availability': AVAILABILITY_FIXTURE}
    for width in WIDTHS:
        page.set_viewport_size({'width': width, 'height': 900})
        page.goto(f'{base_url}/', wait_until='load')
        page.evaluate(INIT_JS, fixture)

        check_no_horizontal_overflow(page, width, 'accountability')
        check_no_duplicate_meta(page, width)
        if width < 900:
            check_rows_are_one_height(page, width)
        check_modal_controls_fit(page, width)
        if width < 900:
            check_tap_targets(page, width)

        page.evaluate('openDirectory()')
        page.wait_for_timeout(50)
        check_no_horizontal_overflow(page, width, 'directory')

        page.evaluate('closeDirectoryPage(); openAvailability()')
        page.wait_for_timeout(100)
        check_no_horizontal_overflow(page, width, 'availability')
        page.evaluate('closeAvailabilityPage()')

        # Formation mode is full-screen, so short viewports are the real test —
        # the five reason buttons must still fit on the smallest phone.
        for height in (568, 900):
            page.set_viewport_size({'width': width, 'height': height})
            page.evaluate('startFormation()')
            check_formation_fits(page, width, height, 'present/away')
            page.evaluate('formationAsk()')
            check_formation_fits(page, width, height, 'reason picker')
            check_no_horizontal_overflow(page, width, 'formation')
            page.evaluate('exitFormation()')
        page.set_viewport_size({'width': width, 'height': 900})


def main():
    httpd, thread, port = start_server()
    try:
        base_url = f'http://127.0.0.1:{port}'
        with sync_playwright() as pw:
            try:
                browser = pw.chromium.launch()
            except PlaywrightError:
                print('ok (skipped: playwright browser binary not installed)')
                return
            try:
                page = browser.new_page()
                run_checks(page, base_url)
            finally:
                browser.close()
    finally:
        stop_server(httpd, thread)
    print('ok')


if __name__ == '__main__':
    main()

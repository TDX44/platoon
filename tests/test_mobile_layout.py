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
drives the SPA the same way a signed-in browser does: it starts a real
server, loads '/', stubs `api()` with a fixture unit tree, then calls the
app's own entry points — `loadHome()`, `selectUnit()`, `openDirectory()`,
`openUnits()`, `openSettings()`, `openSoldierPage()`, `startFormation()`,
`showCreateUnitScreen()` — rather than hand-building the DOM. server.py is
not touched or weakened.

Run with: python tests/test_mobile_layout.py
Requires Playwright + a chromium browser (dev-only, not in requirements.txt):
    pip install playwright && playwright install chromium
Without those installed this prints a loud SKIPPED line and exits 0 — the
skip is deliberately not the word "ok", because this test spent the whole
unit-tree rewrite silently skipping while it was broken.
"""
import os
import socket
import sys
import threading
import time
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dbharness  # noqa: E402
_schema = dbharness.setup()

# A skip is not a pass. Anything that greps this file's output for "ok" must
# not be fooled by a run in which nothing was measured.
SKIP_PREFIX = 'SKIPPED (not ok): '
SKIP_SUFFIX = ' — layout checks did NOT run'

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import sync_playwright
except ImportError:
    print(SKIP_PREFIX + 'playwright not installed' + SKIP_SUFFIX)
    sys.exit(0)

import server  # noqa: E402  (must follow the DATA_DIR override)

# The app answers on the unit's clock (server.app_today()), so the tests
# must ask the same question. Using date.today() here made CI fail on its
# UTC runner every evening between 1900 and midnight Central.
TODAY = date.fromisoformat(server.app_today())
APP_TZ = server.app_timezone()


def day(offset):
    return (TODAY + timedelta(days=offset)).isoformat()


LONG_NOTE = 'Attending Advanced Individual Readiness and Combatives Recertification Course, extended stay pending follow-on orders'

# ─── The unit tree ───
# Company → two platoons → a squad → a team, which is the shape every screen
# below has to survive: four levels of indentation on the Units page, a
# three-row org chart on the home screen, and a Unit column in the directory
# (which only appears when the current unit has children).
UNITS_FIXTURE = [
    {'id': 1, 'parent_id': None, 'kind': 'company', 'name': 'Headhunter Company',
     'slug': 'headhunter-company', 'count': 0, 'logo': None},
    {'id': 2, 'parent_id': 1, 'kind': 'platoon', 'name': '1st Platoon',
     'slug': '1st-platoon', 'count': 2, 'logo': None},
    {'id': 3, 'parent_id': 1, 'kind': 'platoon', 'name': '2nd Platoon',
     'slug': '2nd-platoon', 'count': 3, 'logo': None},
    {'id': 4, 'parent_id': 3, 'kind': 'squad', 'name': 'Alpha Squad',
     'slug': 'alpha-squad', 'count': 2, 'logo': None},
    {'id': 5, 'parent_id': 4, 'kind': 'team', 'name': 'Team Bravo (Weapons)',
     'slug': 'team-bravo', 'count': 1, 'logo': None},
]
ROOT_UNIT_ID = 1

# A company with nine platoons abreast: the org chart's columns are 198px
# minimum, so this is ~1800px of chart inside a 320px screen. The chart is a
# deliberate overflow-x:auto box, so the *page* must still not scroll.
WIDE_UNITS_FIXTURE = [dict(UNITS_FIXTURE[0])] + [
    {'id': 100 + i, 'parent_id': 1, 'kind': 'platoon',
     'name': f'{i}th Platoon (Forward Support)', 'slug': f'p{i}', 'count': 11, 'logo': None}
    for i in range(1, 10)
]

# One person per status the roster renders, plus one carrying a future
# scheduled_events entry, plus long-content stress (long notes, long last name).
# Shape matches what index.html's load() builds (personnel = data.map(...));
# ROSTER_FIXTURE below turns the same people back into what the API returns.
PERSONNEL_FIXTURE = [
    {'id': 1, 'unit_id': 2, 'rank': 'SPC', 'last': 'Testperson-Featherstonehaugh', 'first': 'Wanda',
     'status': 'tdy', 'notes': LONG_NOTE, 'from': day(-1), 'to': day(6),
     'present_date': '', 'scheduled_events': []},
    {'id': 2, 'unit_id': 2, 'rank': 'SGT', 'last': 'Fixtureton', 'first': 'Ray',
     'status': 'present', 'notes': '', 'from': '', 'to': '',
     'present_date': TODAY.isoformat(), 'scheduled_events': []},
    {'id': 3, 'unit_id': 3, 'rank': 'PFC', 'last': 'Placeholder', 'first': 'Nia',
     'status': 'present', 'notes': '', 'from': '', 'to': '',
     'present_date': '', 'scheduled_events': []},
    {'id': 4, 'unit_id': 3, 'rank': 'CPL', 'last': 'Sampleford', 'first': 'Kai',
     'status': 'leave', 'notes': 'Block leave', 'from': day(2), 'to': day(10),
     'present_date': '', 'scheduled_events': []},
    {'id': 5, 'unit_id': 3, 'rank': 'SPC', 'last': 'Dummyval', 'first': 'Theo',
     'status': 'pass', 'notes': '', 'from': day(0), 'to': day(1),
     'present_date': '', 'scheduled_events': []},
    {'id': 6, 'unit_id': 4, 'rank': 'SSG', 'last': 'Exampleson', 'first': 'Priya',
     'status': 'other', 'notes': 'Staff Duty Recovery', 'from': day(-1), 'to': day(0),
     'present_date': '', 'scheduled_events': []},
    {'id': 7, 'unit_id': 4, 'rank': 'SGT', 'last': 'Mockridge', 'first': 'Dev',
     'status': 'ftr', 'notes': '', 'from': day(-3), 'to': day(-1),
     'present_date': '', 'scheduled_events': []},
    {'id': 9, 'unit_id': 5, 'rank': 'SPC', 'last': 'Placeholderman', 'first': 'Ola',
     'status': 'present', 'notes': '', 'from': '', 'to': '',
     'present_date': TODAY.isoformat(),
     'scheduled_events': [{'id': 99, 'person_id': 9, 'unit_id': 5, 'status': 'tdy',
                            'from_date': day(14), 'to_date': day(22),
                            'notes': 'IO - Dothan, AL', 'location': '', 'state': 'scheduled'}]},
]

# What GET /api/personnel returns — the same people, backend field names.
ROSTER_FIXTURE = [
    {'id': p['id'], 'unit_id': p['unit_id'], 'rank': p['rank'], 'last': p['last'], 'first': p['first'],
     'status': p['status'], 'notes': p['notes'], 'from_date': p['from'], 'to_date': p['to'],
     'present_date': p['present_date'], 'scheduled_events': p['scheduled_events']}
    for p in PERSONNEL_FIXTURE
]


def _next_absence(p):
    if not p['scheduled_events']:
        return None
    e = p['scheduled_events'][0]
    return {'status': e['status'], 'from_date': e['from_date'], 'to_date': e['to_date']}


# What GET /api/directory returns — same fixture, backend field names.
DIRECTORY_FIXTURE = [
    {'id': p['id'], 'unit_id': p['unit_id'], 'rank': p['rank'], 'last': p['last'], 'first': p['first'],
     'status': p['status'], 'from_date': p['from'], 'to_date': p['to'], 'notes': p['notes'],
     'dod_id': '1234567890', 'dob': None, 'mos': '35F', 'section': 'S2', 'phone': '',
     'next_absence': _next_absence(p)}
    for p in PERSONNEL_FIXTURE
]

# What GET /api/availability returns, built from the same people.
AVAILABILITY_FIXTURE = {
    'unit': ROOT_UNIT_ID, 'date': day(3), 'to': day(9), 'span': 7,
    'available': [{'id': p['id'], 'rank': p['rank'], 'last': p['last'], 'first': p['first']}
                  for p in PERSONNEL_FIXTURE if p['status'] == 'present'],
    'unavailable': [{'id': p['id'], 'rank': p['rank'], 'last': p['last'], 'first': p['first'],
                     'status': p['status'], 'from_date': p['from'], 'to_date': p['to'],
                     'notes': p['notes'], 'location': '',
                     'days': [day(3), day(4)], 'whole_range': False}
                    for p in PERSONNEL_FIXTURE
                    if p['status'] in ('tdy', 'leave', 'pass', 'other', 'ftr')],
}

# The signed-in user: an owner at the root, so every Units-page control and the
# owner-only Organisation settings row are on screen to be measured.
USER_FIXTURE = {
    'id': 1, 'username': 'ada.fixture', 'email': 'ada@example.invalid',
    'full_name': 'SFC Ada Fixtureton-Placeholder', 'unit_id': ROOT_UNIT_ID,
    'unit_name': 'Headhunter Company', 'unit_slug': 'headhunter-company',
    'role': 'owner', 'root_id': ROOT_UNIT_ID, 'timezone': APP_TZ,
    'needs_unit': False, 'invited_by': '',
    # Also the operator of the instance, so the Admin menu item, the Settings
    # row and the /admin screen itself are all on screen to be measured.
    'platform_admin': True,
}

# What GET /api/admin/overview returns. Deliberately wide content — long
# organisation names, long owner emails, nine columns — because the admin
# tables are the widest thing the app draws on a 320px phone.
ADMIN_FIXTURE = {
    'generated_at': TODAY.isoformat() + ' 06:30:00',
    'totals': {'organisations': 3, 'unit_count': 1284, 'personnel_count': 9876,
               'user_count': 142, 'unattached_users': 37, 'pending_invites': 6,
               'database_bytes': 86423219},
    'organisations': [
        {'org_id': 1, 'org_name': 'Headhunter Company (Forward Support Battalion)',
         'org_slug': 'headhunter-company', 'org_kind': 'company',
         'created_stamp': '2026-01-02 03:04:05', 'unit_count': 5, 'personnel_count': 42,
         'user_count': 7, 'owner_emails': 'ada.fixtureton-placeholder@example.invalid',
         'pending_invites': 2, 'has_logo': True,
         'last_activity': TODAY.isoformat() + ' 06:00:00', 'audit_7d': 173},
        {'org_id': 2, 'org_name': 'Second Placeholder Battalion', 'org_slug': 'second-placeholder',
         'org_kind': 'company', 'created_stamp': '2026-02-02 03:04:05', 'unit_count': 2,
         'personnel_count': 8, 'user_count': 1, 'owner_emails': None, 'pending_invites': 0,
         'has_logo': False, 'last_activity': None, 'audit_7d': 0},
    ],
    'recent_users': [
        {'user_id': 9, 'email': 'brand.new.signup@example.invalid', 'full_name': 'PFC Brand Newsignup',
         'role': 'leader', 'org_name': None, 'signed_in': True},
        {'user_id': 8, 'email': 'ada.fixtureton-placeholder@example.invalid',
         'full_name': 'SFC Ada Fixtureton-Placeholder', 'role': 'owner',
         'org_name': 'Headhunter Company (Forward Support Battalion)', 'signed_in': True},
    ],
}

# Leader chips on the Units page, one of them on a deep unit.
USERS_FIXTURE = [
    USER_FIXTURE,
    {'id': 2, 'username': 'ray.leader', 'email': 'ray@example.invalid',
     'full_name': 'SSG Ray Fixtureton', 'unit_id': 3, 'unit_name': '2nd Platoon',
     'unit_slug': '2nd-platoon', 'role': 'leader', 'root_id': ROOT_UNIT_ID,
     'timezone': APP_TZ, 'needs_unit': False},
    {'id': 3, 'username': 'nia.leader', 'email': 'nia@example.invalid',
     'full_name': 'SGT Nia Placeholder-Sampleford', 'unit_id': 4, 'unit_name': 'Alpha Squad',
     'unit_slug': 'alpha-squad', 'role': 'leader', 'root_id': ROOT_UNIT_ID,
     'timezone': APP_TZ, 'needs_unit': False},
]

# Invite chips: one long label with a Copy/Revoke pair, one owner invite.
INVITES_FIXTURE = [
    {'token': 'a' * 32, 'label': 'Incoming platoon sergeant, reports 15th',
     'unit_id': 2, 'unit_name': '1st Platoon', 'role': 'leader',
     'created_by': 'SFC Ada Fixtureton-Placeholder',
     'expires_at': day(5) + ' 09:00:00', 'status': 'pending'},
    {'token': 'b' * 32, 'label': 'Company XO', 'unit_id': ROOT_UNIT_ID,
     'unit_name': 'Headhunter Company', 'role': 'owner', 'created_by': 'SFC Ada Fixtureton-Placeholder',
     'expires_at': day(1) + ' 09:00:00', 'status': 'pending'},
]

SETTINGS_FIXTURE = {
    'unit_name': 'Headhunter Company', 'kind': 'company',
    'tdy_schools': ['Air Assault', 'Combatives Level 1', 'Ranger'],
    'tdy_locations': ['Fort Example', 'Dothan, AL'],
    'timezone': APP_TZ,
}

PROFILE_FIXTURE = {'dod_id': '1234567890', 'mos': '35F', 'section': 'S2', 'phone': ''}

WIDTHS = [320, 390, 1280]
# The admin dashboard's totals strip reflows continuously, and a tile only
# clips at the width where its longest label stops fitting -- so that one
# screen is swept rather than sampled.
ADMIN_SWEEP_WIDTHS = [320, 360, 420, 560, 768, 900, 1024, 1100, 1280, 1400]
# ponytail: honest current floor, not an aspirational one. Measured directly
# against this app at 320/390px: the shortest real button today is the
# "Set Status" / "Mark Present" pair (.dash-btn-sm) at 29px; everything else
# (row-menu triggers, section headers, bottom nav) is 34px+. 28px gives 1px of
# rendering slack. Raise this only once .dash-btn-sm is redesigned taller.
MIN_TAP_TARGET_PX = 28

ROSTER_BUTTONS = '#personnelBody button, .dash-header-actions button, .dash-bottomnav button'
HOME_CARDS = '#unitCards .unit-card'


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


# Stub the one thing a signed-out browser cannot have — the API — and then let
# the app drive itself. `window.__units` is what GET /units answers, so the
# wide-organisation pass is a change of fixture, not a change of code path.
INIT_JS = """
async (fixture) => {
  window.__fixture = fixture;
  window.__units = fixture.units;
  window.api = async (method, path) => {
    const p = String(path).split('?')[0];
    if (p === '/units') return window.__units;
    if (p === '/personnel') return fixture.roster;
    if (p === '/settings') return fixture.settings;
    if (p === '/directory') return fixture.directory;
    if (p === '/availability') return fixture.availability;
    if (p === '/users') return fixture.users;
    if (p === '/invites') return fixture.invites;
    if (p === '/me') return fixture.user;
    if (p === '/admin/overview') return fixture.admin;
    if (/^\\/personnel\\/\\d+\\/profile$/.test(p)) return fixture.profile;
    return [];
  };
  // initApp() is still racing us towards Clerk, which a test box cannot
  // reach. Whatever it concludes, it must not blank the screen we draw here.
  // Keep the real one reachable: signing out is a behaviour this file
  // checks, even though the stub is what stops initApp() blanking the
  // screen mid-render.
  window.__realShowLoginScreen = showLoginScreen;
  showLoginScreen = () => {};
  currentUser = fixture.user;
  await loadHome();
}
"""

SHOW_HOME_JS = """
async (units) => { window.__units = units; await loadHome(); }
"""

# selectUnit() is the real "open this unit" path (route push, logo, sidebar
# identity, then load()). It does not return load()'s promise, so the second
# await is what makes the render deterministic rather than timing-dependent.
ENTER_UNIT_JS = """
async (unitId) => {
  window.__units = window.__fixture.units;
  await loadUnits();
  selectUnit(unitById(unitId));
  await load();
}
"""


def overflowing_elements(page):
    """Elements whose right edge extends past the viewport (1px slack).

    Skips anything inside a deliberate horizontal-scroll container
    (overflow-x: auto/scroll, e.g. the desktop directory table and the home
    screen's org chart) — that content is meant to scroll within its own box,
    not bleed past the screen the way the real bugs did.
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


def check_fits_width(page, width, selector, view_label):
    """A panel wider than the screen that is also centred overflows to the
    *left*, where neither scrollWidth nor a right-edge test can see it."""
    bad = page.evaluate("""
    (sel) => {
      const vw = window.innerWidth;
      const out = [];
      document.querySelectorAll(sel).forEach(el => {
        const r = el.getBoundingClientRect();
        if (r.width === 0 && r.height === 0) return;
        if (r.width > vw + 1 || r.left < -1 || r.right > vw + 1) {
          out.push(sel + ' left=' + Math.round(r.left) + ' width=' + Math.round(r.width));
        }
      });
      if (!out.length && !document.querySelector(sel)) out.push(sel + ' is not on screen');
      return out;
    }
    """, selector)
    assert not bad, f'{view_label} @ {width}px: panel does not fit the viewport: {bad}'


def check_nothing_clips_inside(page, width, selector, view_label):
    """Content that runs out past the box it is drawn in.

    The totals tiles are the case that bit: "ORGANISATIONS" is one long word,
    and in a narrow tile it ran out under the tile's own border instead of
    wrapping. scrollWidth against clientWidth sees exactly that, whether the
    box hides the overflow or lets it bleed."""
    bad = page.evaluate("""
    (sel) => {
      const out = [];
      document.querySelectorAll(sel).forEach(box => {
        const r = box.getBoundingClientRect();
        if (r.width === 0 && r.height === 0) return;
        if (box.scrollWidth > box.clientWidth + 1) {
          out.push((box.className || box.tagName) + ' content ' + box.scrollWidth
                   + ' > box ' + box.clientWidth + ' "' + box.innerText.trim().slice(0, 20) + '"');
        }
      });
      return out;
    }
    """, selector)
    assert not bad, f'{view_label} @ {width}px: content clipped by its own box: {bad}'


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


def check_tap_targets(page, width, selector, view_label):
    bad = page.evaluate("""
    ([sel, min]) => {
      const bad = [];
      document.querySelectorAll(sel).forEach(el => {
        const r = el.getBoundingClientRect();
        if (r.width === 0 && r.height === 0) return;  // not visible
        if (r.height < min) {
          bad.push((el.id ? '#' + el.id : el.className || el.tagName) + ' height=' + r.height.toFixed(1));
        }
      });
      return bad;
    }
    """, [selector, MIN_TAP_TARGET_PX])
    assert not bad, f'{view_label} @ {width}px: control(s) under {MIN_TAP_TARGET_PX}px tall: {bad}'


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


def check_home(page, width, units, label):
    """The home screen is the whole org chart. The chart scrolls inside itself
    on purpose; the page it sits on must not, at any width, however wide the
    organisation gets."""
    page.evaluate(SHOW_HOME_JS, units)
    cards = page.evaluate("document.querySelectorAll('%s').length" % HOME_CARDS)
    assert cards, f'{label} @ {width}px: the org chart drew no unit cards'
    check_no_horizontal_overflow(page, width, label)
    if width < 900:
        check_tap_targets(page, width, HOME_CARDS, label)


def run_checks(page, base_url):
    fixture = {'units': UNITS_FIXTURE, 'roster': ROSTER_FIXTURE, 'directory': DIRECTORY_FIXTURE,
               'availability': AVAILABILITY_FIXTURE, 'users': USERS_FIXTURE,
               'invites': INVITES_FIXTURE, 'settings': SETTINGS_FIXTURE,
               'user': USER_FIXTURE, 'profile': PROFILE_FIXTURE,
               'admin': ADMIN_FIXTURE}
    for width in WIDTHS:
        page.set_viewport_size({'width': width, 'height': 900})
        page.goto(f'{base_url}/', wait_until='load')
        page.evaluate(INIT_JS, fixture)

        # Home: the org chart, normal tree and a nine-across organisation.
        check_home(page, width, UNITS_FIXTURE, 'home')
        check_home(page, width, WIDE_UNITS_FIXTURE, 'home (wide org)')

        page.evaluate(ENTER_UNIT_JS, ROOT_UNIT_ID)

        check_no_horizontal_overflow(page, width, 'accountability')
        check_no_duplicate_meta(page, width)
        if width < 900:
            check_rows_are_one_height(page, width)
        check_modal_controls_fit(page, width)
        if width < 900:
            check_tap_targets(page, width, ROSTER_BUTTONS, 'accountability')

        page.evaluate('openDirectory()')
        page.wait_for_timeout(50)
        check_no_horizontal_overflow(page, width, 'directory')

        page.evaluate('closeDirectoryPage(); openAvailability()')
        page.wait_for_timeout(100)
        check_no_horizontal_overflow(page, width, 'availability')
        page.evaluate('closeAvailabilityPage()')

        # Units: four levels of indent, leader chips and invite chips.
        page.evaluate('(async () => { openUnits(); await refreshUnitsPage(); })()')
        page.wait_for_timeout(50)
        assert page.evaluate("document.querySelectorAll('.unit-chip-invite').length"), (
            f'units @ {width}px: no invite chips rendered — fixture is stale')
        check_no_horizontal_overflow(page, width, 'units')
        page.evaluate('closeUnits()')

        # Settings, including the Unit card's logo row.
        page.evaluate('openSettings()')
        assert page.evaluate("!!document.getElementById('unitLogoInput')"), (
            f'settings @ {width}px: the Unit logo row did not render')
        check_no_horizontal_overflow(page, width, 'settings')
        page.evaluate('closeSettings()')

        # A soldier page, with the "Edit name & rank" button the hero grew.
        page.evaluate('openSoldierPage(1)')
        page.wait_for_timeout(50)
        assert page.evaluate("!!document.querySelector('.soldier-hero-edit')"), (
            f'soldier @ {width}px: the Edit name & rank button did not render')
        check_no_horizontal_overflow(page, width, 'soldier')
        page.evaluate('closeSoldierPage(); render()')

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

        # The platform dashboard: two nine-column tables on a 320px phone.
        # They scroll inside their own wrappers; the page must not.
        page.evaluate('(async () => { await openAdmin(false); })()')
        page.wait_for_timeout(50)
        assert page.evaluate("document.querySelectorAll('#adminScreen .admin-table tbody tr').length >= 4"), (
            f'admin @ {width}px: the dashboard tables did not render — fixture is stale')
        check_no_horizontal_overflow(page, width, 'admin')
        check_nothing_clips_inside(page, width, '.admin-total', 'admin totals')
        if width < 900:
            check_tap_targets(page, width, '#adminScreen button', 'admin')

        for w in ADMIN_SWEEP_WIDTHS:
            page.set_viewport_size({'width': w, 'height': 900})
            check_nothing_clips_inside(page, w, '.admin-total', 'admin totals')
            check_no_horizontal_overflow(page, w, 'admin')
        page.set_viewport_size({'width': width, 'height': 900})

        # Signing out has to TAKE every other tenant's name and owner email,
        # not merely hide them: this is a shared machine.
        page.evaluate('window.__realShowLoginScreen()')
        left = page.evaluate("[adminData, document.getElementById('adminScreen').innerHTML]")
        assert left == [None, ''], f'admin data survived sign-out: {str(left)[:200]}'
        page.evaluate(INIT_JS, fixture)

        # First run: the signed-in user belongs to no unit yet. Last, because
        # it swaps the platoon screen out from under everything above.
        page.evaluate('showCreateUnitScreen()')
        # The panel is centred in a flex column, so a panel too wide to shrink
        # hangs off *both* edges — half of which scrollWidth cannot see. That
        # is what check_fits_width is for, so it is asked first.
        check_fits_width(page, width, '#createUnitScreen .login-card', 'create unit')
        check_no_horizontal_overflow(page, width, 'create unit')


def main():
    try:
        httpd, thread, port = start_server()
        try:
            base_url = f'http://127.0.0.1:{port}'
            with sync_playwright() as pw:
                try:
                    browser = pw.chromium.launch()
                except PlaywrightError:
                    print(SKIP_PREFIX + 'playwright chromium binary not installed' + SKIP_SUFFIX)
                    return
                try:
                    page = browser.new_page()
                    run_checks(page, base_url)
                finally:
                    browser.close()
        finally:
            stop_server(httpd, thread)
        print('ok')
    finally:
        dbharness.teardown(_schema)


if __name__ == '__main__':
    main()

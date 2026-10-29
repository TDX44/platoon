"""Exercise the real SPA's compact phone directory without PostgreSQL or Clerk.

Uses synthetic fixtures declared by test_mobile_layout.py, without importing
that module or its database setup. Run with python3 tests/test_mobile_usability.py.
Requires Playwright and Chromium; missing dependencies are an explicit skip.
"""
import ast
import functools
import json
from datetime import date, datetime, timedelta
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent


def fixtures():
    source = ROOT / 'tests/test_mobile_layout.py'
    env = {'date': date, 'timedelta': timedelta,
           'TODAY': datetime.now(ZoneInfo('America/Chicago')).date(),
           'APP_TZ': 'America/Chicago', 'ROOT_UNIT_ID': 1}
    names = {
        'day', '_next_absence', 'LONG_NOTE', 'UNITS_FIXTURE',
        'PERSONNEL_FIXTURE', 'ROSTER_FIXTURE', 'DIRECTORY_FIXTURE',
        'AVAILABILITY_FIXTURE', 'USER_FIXTURE', 'SETTINGS_FIXTURE',
        'PROFILE_FIXTURE', 'INIT_JS', 'ENTER_UNIT_JS',
    }
    for node in ast.parse(source.read_text()).body:
        name = ''
        if isinstance(node, ast.FunctionDef):
            name = node.name
        elif isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
        if name in names:
            exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), 'exec'), env)
    data = {key: env[value] for key, value in {
        'units': 'UNITS_FIXTURE', 'roster': 'ROSTER_FIXTURE',
        'directory': 'DIRECTORY_FIXTURE', 'availability': 'AVAILABILITY_FIXTURE',
        'user': 'USER_FIXTURE', 'settings': 'SETTINGS_FIXTURE',
        'profile': 'PROFILE_FIXTURE',
    }.items()}
    data.update(users=[], invites=[], admin={})
    data['directory'][0]['phone'] = '+12025550123'
    return env, data


class Handler(SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


def main():
    try:
        from playwright.sync_api import sync_playwright, Error
    except ImportError:
        print('SKIPPED (not ok): Playwright is required for mobile usability checks')
        return
    env, data = fixtures()
    httpd = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(Handler, directory=str(ROOT)))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    base = f'http://127.0.0.1:{httpd.server_port}'
    try:
        with sync_playwright() as pw:
            try:
                browser = pw.chromium.launch()
            except Error:
                print('SKIPPED (not ok): Chromium is required for mobile usability checks')
                return
            try:
                page = browser.new_page(viewport={'width': 390, 'height': 844})
                page.route('**/*', lambda route: route.continue_()
                           if route.request.url.startswith(base + '/') else route.abort())
                page.route('**/api/auth/config', lambda route: route.fulfill(
                    content_type='application/json', body=json.dumps({'enabled': False})))
                page.goto(base)
                page.evaluate(env['INIT_JS'], data)
                page.evaluate(env['ENTER_UNIT_JS'], 1)
                assert not page.locator('#reportPreviewCard').is_visible(), 'phone repeats Reports preview'
                assert page.locator('#personnelBody .dash-row').first.bounding_box()['y'] < 470, (
                    'phone header crowds the first person below the useful part of the screen')
                for width in (320, 390, 768, 1280):
                    page.set_viewport_size({'width': width, 'height': 844})
                    page.evaluate('openDirectory()')
                    details = page.locator('#directoryView .dir-detail').first
                    assert details.is_visible() == (width > 900), 'administrative detail visibility is wrong'
                    if width <= 900:
                        assert not page.locator('#directoryView .dir-phone.dir-empty').first.is_visible()
                        assert page.locator('#directoryView a[href="tel:+12025550123"]').is_visible()
                        text = page.locator('#directoryView a[href="sms:+12025550123"]')
                        assert text.is_visible() and text.bounding_box()['height'] >= 44
                        assert page.locator('#directoryView .dir-row-menu summary').first.bounding_box()['height'] >= 44
                    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1'), (
                        f'directory overflows at {width}px')
                page.set_viewport_size({'width': 390, 'height': 844})
                assert page.evaluate('document.documentElement.scrollHeight') < 1500, (
                    'eight-person phone directory is no longer compact')
                assert page.locator('#directorySearch').get_attribute('aria-label')
            finally:
                browser.close()
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)
    print('ok: compact directory, phone touch targets, roster density and desktop fields')


if __name__ == '__main__':
    main()

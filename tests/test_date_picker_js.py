"""The single-date picker, driven in a real browser. No server, no DB.

Every date field in the app now opens the same picker the absence range uses,
in a one-month mode with month/year selects. Those selects shipped carrying
data-act, which is the attribute rpOnClick() matches on days — so a click on
the year was read as a click on a day, set the field to undefined and closed
the picker. It is a click-behaviour bug, invisible to a DOM-string test, so
this one drives Chromium.
"""
import os
import pathlib
import sys

SKIP_PREFIX = 'SKIPPED (not ok): '
SKIP_SUFFIX = ' — date-picker checks did NOT run'

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print(SKIP_PREFIX + 'playwright not installed' + SKIP_SUFFIX)
    sys.exit(0)

ROOT = pathlib.Path(__file__).resolve().parent.parent
URL = (ROOT / 'index.html').as_uri()

# A date of birth: the case that made the selects necessary in the first place.
YEAR, MONTH_IDX, DAY = '1994', '1', '1994-02-17'


def open_picker(page):
    page.evaluate("""() => {
      document.querySelectorAll('.pickharness').forEach(n => n.remove());
      const h = document.createElement('div');
      h.className = 'soldier-card pickharness';
      h.style.cssText = 'position:fixed;left:20px;top:20px;width:320px;z-index:99999;background:#fff';
      h.innerHTML = profileField('Date of Birth', 'dob', 'date');
      document.body.appendChild(h);
    }""")
    page.locator('#pf_dob').dispatch_event('mousedown')
    page.wait_for_selector('.rp-pop', timeout=3000)


def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport={'width': 1200, 'height': 900})
        page.goto(URL, wait_until='domcontentloaded')
        page.wait_for_timeout(400)
        open_picker(page)

        assert page.locator('.rp-month').count() == 1, 'single mode must show one month, not two'
        assert page.locator('.rp-sel').count() == 2, 'single mode must offer month and year selects'
        # A range field still gets the two-month picker and no selects.
        assert 'rp-single' in page.locator('.rp-pop').get_attribute('class')

        # The bug: clicking the year must not be read as picking a day.
        page.locator('.rp-sel').nth(1).click()
        page.wait_for_timeout(120)
        assert page.locator('.rp-pop').count() == 1, 'clicking the year closed the picker'
        assert page.evaluate("document.getElementById('pf_dob').value") == '', \
            'clicking the year wrote a value into the field'

        page.select_option('.rp-sel >> nth=1', YEAR)
        page.wait_for_timeout(120)
        page.select_option('.rp-sel >> nth=0', MONTH_IDX)
        page.wait_for_timeout(120)
        assert page.locator('.rp-pop').count() == 1, 'changing month or year closed the picker'
        assert page.locator('.rp-sel').nth(1).input_value() == YEAR, 'the year did not stick'
        assert page.locator('.rp-sel').nth(0).input_value() == MONTH_IDX, 'the month did not stick'
        # 1994 is not a leap year, and the grid is built from real month lengths.
        assert page.locator('.rp-d[data-d="1994-02-28"]').count() == 1
        assert page.locator('.rp-d[data-d="1994-02-29"]').count() == 0

        # One click picks and closes — there is no second half of a range.
        page.locator(f'.rp-d[data-d="{DAY}"]').click()
        page.wait_for_timeout(150)
        assert page.evaluate("document.getElementById('pf_dob').value") == DAY, 'the day did not land'
        assert page.locator('.rp-pop').count() == 0, 'picking a day left the picker open'

        # Clear Date empties the field and closes.
        open_picker(page)
        page.evaluate("document.getElementById('pf_dob').value = '1994-02-17'")
        page.locator('.rp-reset').click()
        page.wait_for_timeout(150)
        assert page.evaluate("document.getElementById('pf_dob').value") == ''
        assert page.locator('.rp-pop').count() == 0

        browser.close()
    print('ok')


if __name__ == '__main__':
    main()

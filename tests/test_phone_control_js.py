"""The phone control — a country beside the number — driven in a real browser.

The rule this exists for: at +1 the field takes ten digits and refuses an
eleventh, however many are typed or pasted at it. That is keystroke behaviour,
so a node test cannot see it. tests/test_phone_js.py covers the pure functions;
this covers the control they are wired into.
"""
import pathlib
import sys

SKIP_PREFIX = 'SKIPPED (not ok): '
SKIP_SUFFIX = ' — phone-control checks did NOT run'

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print(SKIP_PREFIX + 'playwright not installed' + SKIP_SUFFIX)
    sys.exit(0)

ROOT = pathlib.Path(__file__).resolve().parent.parent
URL = (ROOT / 'index.html').as_uri()


def build(page):
    page.evaluate("""() => {
      document.querySelectorAll('.phharness').forEach(n => n.remove());
      const h = document.createElement('div');
      h.className = 'soldier-card phharness';
      h.style.cssText = 'position:fixed;left:0;top:0;right:0;z-index:99999;background:#fff';
      h.innerHTML = profileField('Phone', 'phone', 'tel');
      document.body.appendChild(h);
    }""")


def country_index(page, name):
    return page.evaluate("n => PHONE_COUNTRIES.findIndex(c => c[0] === n)", name)


def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport={'width': 390, 'height': 800})
        page.goto(URL, wait_until='domcontentloaded')
        page.wait_for_timeout(400)
        build(page)

        # Defaults to the United States, and every country is offered.
        assert page.evaluate("PHONE_COUNTRIES[+document.getElementById('cc_phone').value][0]") \
            == 'United States', 'the country did not default to the US'
        assert page.evaluate("document.getElementById('cc_phone').options.length") \
            == page.evaluate("PHONE_COUNTRIES.length")
        first = page.evaluate("document.getElementById('cc_phone').options[0].text")
        assert first.startswith('\U0001F1FA\U0001F1F8'), f'no flag on the option: {first!r}'
        assert '+1' in first and 'United States' in first, first

        # THE HARD STOP: four digits past ten change nothing.
        page.locator('#pf_phone').click()
        page.keyboard.type('21255501439999')
        assert page.evaluate("pf_phone.value") == '(212) 555-0143', \
            page.evaluate("pf_phone.value")
        assert page.evaluate("getPhoneField('phone')") == '(212) 555-0143'
        assert page.evaluate("validateField('phone', getPhoneField('phone'))") == ''
        actions = page.evaluate("document.getElementById('pfa_phone').innerHTML")
        assert 'tel:+12125550143' in actions and 'sms:+12125550143' in actions, actions

        # An eleventh digit typed into the MIDDLE is refused too.
        page.evaluate("pf_phone.setSelectionRange(2, 2)")
        page.keyboard.type('9')
        assert page.evaluate("pf_phone.value.replace(/\\D/g,'').length") == 10, \
            page.evaluate("pf_phone.value")

        # Another country has its own ceiling: E.164's 15 less the dial code.
        build(page)
        page.select_option('#cc_phone', str(country_index(page, 'Germany')))
        page.locator('#pf_phone').click()
        page.keyboard.type('301234567899999999')
        value = page.evaluate("pf_phone.value")
        assert value == '3012345678999', value          # 13 = 15 - len('49')
        assert page.evaluate("getPhoneField('phone')") == '+49 3012345678999'

        # A pasted number that names its own country adopts it.
        build(page)
        page.evaluate("""() => {
          const i = document.getElementById('pf_phone');
          i.value = '+44 20 7946 0958';
          i.setSelectionRange(i.value.length, i.value.length);
          i.dispatchEvent(new Event('input', { bubbles: true }));
        }""")
        assert page.evaluate("PHONE_COUNTRIES[+document.getElementById('cc_phone').value][0]") \
            == 'United Kingdom', 'a pasted +44 number did not adopt the United Kingdom'
        assert page.evaluate("phoneE164(getPhoneField('phone'))") == '+442079460958'

        # What is stored comes back into the same two controls.
        for stored, country in (('+44 20 7946 0958', 'United Kingdom'),
                                ('(212) 555-0143', 'United States'),
                                ('+49 30 12345678', 'Germany')):
            got = page.evaluate("s => { setPhoneField('phone', s); return ["
                                "PHONE_COUNTRIES[+document.getElementById('cc_phone').value][0],"
                                "getPhoneField('phone')]; }", stored)
            assert got == [country, stored], (stored, got)

        for width in (320, 390, 768):
            page.set_viewport_size({'width': width, 'height': 800})
            build(page)
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth"), \
                f'the phone control overflows at {width}px'

        browser.close()
    print('ok')


if __name__ == '__main__':
    main()

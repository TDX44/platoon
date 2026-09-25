"""The billing surfaces' markup, lifted out of index.html and run under node.

Run with: python tests/test_billing_js.py

Every string in these renderers comes off the wire from /api/me. The state
words are looked up, the dates are parsed, the amounts are numbers — and the
one free-text field a hostile server (or a tampered response) could carry, a
price lookup_key, lands inside an onclick attribute, so it goes through
escapeHtml() like everything else.
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')

HOSTILE = '"><img src=x onerror=alert(1)>'
PRICES = [{'lookup_key': 'platoon_leader_monthly', 'amount': 299, 'interval': 'month', 'currency': 'usd'},
          {'lookup_key': 'platoon_leader_annual', 'amount': 1999, 'interval': 'year', 'currency': 'usd'}]


def billing(**kw):
    b = {'state': 'TRIAL', 'reason': None, 'days_left': 9, 'trial_ends_at': '2026-10-01T12:00:00+00:00',
         'grace_ends_at': '2026-10-04T12:00:00+00:00', 'extension_available': True, 'subscribed': False,
         'cancel_at_period_end': False, 'current_period_end': None, 'prices': PRICES, 'portal_available': False}
    b.update(kw)
    return b


CASES = {
    'trial': billing(),
    'trial_last_day': billing(days_left=1),
    'trial_urgent': billing(days_left=3, extension_available=False),
    'grace': billing(state='GRACE', reason='trial_expired', days_left=2),
    'grace_no_ext': billing(state='GRACE', reason='trial_expired', days_left=2, extension_available=False),
    'past_due': billing(state='PAST_DUE', subscribed=True, portal_available=True, extension_available=False),
    'active': billing(state='ACTIVE', subscribed=True, portal_available=True, extension_available=False,
                      current_period_end='2026-10-17T12:00:00+00:00', prices=[]),
    'cancelling': billing(state='ACTIVE', subscribed=True, portal_available=True, extension_available=False,
                          current_period_end='2026-10-17T12:00:00+00:00', cancel_at_period_end=True, prices=[]),
    'comped': billing(state='COMPED', extension_available=False, prices=[]),
    'locked_trial': billing(state='LOCKED', reason='trial_expired', days_left=None, extension_available=False),
    'locked_payment': billing(state='LOCKED', reason='payment_required', days_left=None, extension_available=False,
                              portal_available=True),
    'locked_no_prices': billing(state='LOCKED', reason='trial_expired', days_left=None, extension_available=False, prices=[]),
    'hostile': billing(state='LOCKED', reason='trial_expired', trial_ends_at=HOSTILE, grace_ends_at=HOSTILE,
                       current_period_end=HOSTILE, days_left=HOSTILE,
                       prices=[{'lookup_key': HOSTILE, 'amount': 299, 'interval': 'month', 'currency': 'usd'},
                               {'lookup_key': 'platoon_leader_annual', 'amount': 1999, 'interval': 'year', 'currency': 'usd'}]),
    'none': None,
}

# What /api/billing/details adds: the plan, the card, the invoices.
DETAILS = {
    'plan': {'lookup_key': 'platoon_leader_annual', 'amount': 1999, 'interval': 'year', 'currency': 'usd'},
    'card': {'brand': 'visa', 'last4': '4242', 'exp_month': 4, 'exp_year': 2028},
    'invoices': [
        {'id': 'in_1', 'number': 'A-1', 'created': '2026-09-17T12:00:00+00:00',
         'period_start': '2026-09-17T12:00:00+00:00', 'period_end': '2027-09-17T12:00:00+00:00',
         'status': 'paid', 'amount': 1999, 'currency': 'usd',
         'url': 'https://invoice.stripe.com/i/1', 'pdf': 'https://pay.stripe.com/invoice/1/pdf'},
        {'id': 'in_0', 'number': None, 'created': '2025-09-17T12:00:00+00:00', 'period_start': None,
         'period_end': None, 'status': 'uncollectible', 'amount': 299, 'currency': 'usd', 'url': None, 'pdf': None},
    ],
    'stripe_error': False,
}
HOSTILE_DETAILS = {
    'plan': {'lookup_key': HOSTILE, 'amount': HOSTILE, 'interval': HOSTILE, 'currency': HOSTILE},
    'card': {'brand': HOSTILE, 'last4': HOSTILE, 'exp_month': HOSTILE, 'exp_year': HOSTILE},
    'invoices': [{'id': HOSTILE, 'number': HOSTILE, 'created': HOSTILE, 'period_start': HOSTILE,
                  'period_end': HOSTILE, 'status': HOSTILE, 'amount': HOSTILE, 'currency': HOSTILE,
                  'url': 'javascript:alert(1)//' + HOSTILE, 'pdf': 'https://x.example/' + HOSTILE}],
    'stripe_error': False,
}
USAGE = {'people': 42, 'units': 5}

DRIVER = r'''
const CASES = ''' + json.dumps(CASES) + r''';
const HOSTILE = ''' + json.dumps(HOSTILE) + r''';
const out = {};
const DETAILS = ''' + json.dumps(DETAILS) + r''';
const HOSTILE_DETAILS = ''' + json.dumps(HOSTILE_DETAILS) + r''';
const USAGE = ''' + json.dumps(USAGE) + r''';
for (const [name, b] of Object.entries(CASES)) {
  out[name] = { banner: billingBannerHtml(b), pricing: pricingScreenHtml(b), page: billingPageHtml(b, DETAILS, USAGE) };
}
out.loading = billingPageHtml(CASES.active, null, null);
out.stripeDown = billingPageHtml(CASES.active, { plan: null, card: null, invoices: [], stripe_error: true }, USAGE);
out.noInvoices = billingPageHtml(CASES.trial, { plan: null, card: null, invoices: [], stripe_error: false }, USAGE);
out.hostileDetails = billingPageHtml(CASES.active, HOSTILE_DETAILS, { people: HOSTILE, units: HOSTILE });
out.money = [billingMoney(1999, 'usd'), billingMoney(null, 'usd'), billingMoney(HOSTILE, 'usd'), billingMoney(500, HOSTILE)];
out.modal = {
  due: billingModalDue(CASES.trial_urgent, null, '2026-09-28'),
  seenToday: billingModalDue(CASES.trial_urgent, '2026-09-28', '2026-09-28'),
  seenYesterday: billingModalDue(CASES.trial_urgent, '2026-09-27', '2026-09-28'),
  notUrgent: billingModalDue(CASES.trial, null, '2026-09-28'),
  grace: billingModalDue(CASES.grace, null, '2026-09-28'),
  none: billingModalDue(null, null, '2026-09-28'),
};
out.date = [billingDate('2026-10-01T12:00:00+00:00'), billingDate(''), billingDate(HOSTILE), billingDate(null)];
out.price = [billingPrice({amount: 299}), billingPrice({amount: 1999})];
out.toggle = [adminCompToggle({user_id: 7, billing_mode: 'default'}), adminCompToggle({user_id: 7, billing_mode: 'comped'}),
              adminCompToggle({user_id: 7, billing_mode: null}), adminCompToggle({user_id: HOSTILE, billing_mode: HOSTILE})];
console.log(JSON.stringify(out));
'''


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html — was it renamed or removed?'
    return m.group(0)


def render():
    node = shutil.which('node')
    assert node, 'node is required (it ships with the CI image)'
    src = open(INDEX, encoding='utf-8').read()
    js = '\n'.join([
        extract(src, r'function escapeHtml\(str\) \{.*?\n\}', 'escapeHtml()'),
        extract(src, r'function settingsRow\(.*?\n\}', 'settingsRow()'),
        extract(src, r'function adminNum\(.*?\n\}', 'adminNum()'),
        extract(src, r'function adminCell\(.*?\n\}', 'adminCell()'),
        extract(src, r'function billingDate\(.*?\n\}', 'billingDate()'),
        extract(src, r'function billingPrice\(.*?\n\}', 'billingPrice()'),
        extract(src, r'function billingDays\(.*?\n\}', 'billingDays()'),
        extract(src, r'function billingBannerHtml\(.*?\n\}', 'billingBannerHtml()'),
        extract(src, r'function billingPlanCards\(.*?\n\}', 'billingPlanCards()'),
        extract(src, r'function pricingScreenHtml\(.*?\n\}', 'pricingScreenHtml()'),
        extract(src, r'const BILLING_FEATURES = \[.*?\];', 'BILLING_FEATURES'),
        extract(src, r'const CARD_BRAND_NAMES = \{.*?\};', 'CARD_BRAND_NAMES'),
        extract(src, r'const INVOICE_STATUS = \{.*?\};', 'INVOICE_STATUS'),
        extract(src, r'const BILLING_BADGE = \{.*?\};', 'BILLING_BADGE'),
        extract(src, r'function billingLongDate\(.*?\n\}', 'billingLongDate()'),
        extract(src, r'function billingMoney\(.*?\n\}', 'billingMoney()'),
        extract(src, r'function billingLink\(.*?\n\}', 'billingLink()'),
        extract(src, r'function billingPlanCardHtml\(.*?\n\}', 'billingPlanCardHtml()'),
        extract(src, r'function billingUsageHtml\(.*?\n\}', 'billingUsageHtml()'),
        extract(src, r'function billingPaymentCardHtml\(.*?\n\}', 'billingPaymentCardHtml()'),
        extract(src, r'function billingHistoryHtml\(.*?\n\}', 'billingHistoryHtml()'),
        extract(src, r'function billingPageHtml\(.*?\n\}', 'billingPageHtml()'),
        extract(src, r'function billingModalDue\(.*?\n\}', 'billingModalDue()'),
        extract(src, r'function adminCompToggle\(.*?\n\}', 'adminCompToggle()'),
        DRIVER,
    ])
    path = os.path.join(tempfile.mkdtemp(), 'billing.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(js)
    proc = subprocess.run([node, path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


ALLOWED_TAGS = {'div', '/div', 'span', '/span', 'section', '/section', 'h1', '/h1', 'h3', '/h3', 'h4', '/h4',
                'p', '/p', 'button', '/button', 'img', 'strong', '/strong', 'ul', '/ul', 'li', '/li',
                'svg', '/svg', 'rect', 'path', 'table', '/table', 'thead', '/thead', 'tbody', '/tbody',
                'tr', '/tr', 'th', '/th', 'td', '/td', 'a', '/a'}


def tags(html):
    return set(re.findall(r'<(/?[a-zA-Z][\w-]*)', html))


def test_banner_copy(out):
    assert '9 days left in your trial' in out['trial']['banner'] and 'Add billing details' in out['trial']['banner']
    assert 'Extend my trial 7 days' in out['trial']['banner']
    assert '1 day left' in out['trial_last_day']['banner'] and '1 days' not in out['trial_last_day']['banner']
    assert 'billing-banner-warn' in out['trial_urgent']['banner'], 'the last three days turn the banner to the warning colour'
    assert 'billing-banner-warn' not in out['trial']['banner']
    assert 'Extend my trial' not in out['trial_urgent']['banner'], 'no extension link once it is used'
    g = out['grace']['banner']
    assert 'Your trial ended' in g and '2 days until access is paused' in g and 'Choose a plan' in g and 'billing-banner-danger' in g, g
    assert 'Extend my trial 7 days' in g and 'Extend my trial' not in out['grace_no_ext']['banner']
    p = out['past_due']['banner']
    assert "couldn't process your last payment" in p and 'Update your card' in p and 'openBillingPortal()' in p, p
    for name in ('active', 'cancelling', 'comped', 'locked_trial', 'none'):
        assert out[name]['banner'] == '', f'{name} must show no banner: {out[name]["banner"]!r}'


def test_pricing_screen(out):
    h = out['locked_trial']['pricing']
    assert 'Your trial has ended.' in h, h
    assert 'data-key="platoon_leader_monthly"' in h and 'data-key="platoon_leader_annual"' in h, h
    assert 'startCheckout(this.dataset.key)' in h, h
    assert '$2.99' in h and '$19.99' in h and 'Save 44%' in h, h
    assert 'nothing is deleted' in h and 'doLogout()' in h, h
    assert 'Manage billing' not in h, 'no portal link without a Stripe customer'
    assert 'syncBillingWithStripe()' not in h, 'nothing to sync before there is a Stripe customer'
    h2 = out['locked_payment']['pricing']
    assert 'Your subscription is no longer active.' in h2 and 'Manage billing' in h2, h2
    assert 'syncBillingWithStripe()' in h2, 'a locked account that just paid needs the way back from a lost webhook'
    h3 = out['locked_no_prices']['pricing']
    assert 'startCheckout' not in h3 and 'Prices are loading' in h3 and 'refreshBilling()' in h3, h3
    assert 'id="pricingStatus"' in h, 'the success poll needs somewhere to write'


def test_billing_page(out):
    """Resyrv's shape: current plan beside payment method, then the plan
    picker (unsubscribed only), then the history."""
    t = out['trial']['page']
    assert 'Current plan' in t and 'Free trial' in t and 'Trial ends Oct 1' in t and '9 days left' in t, t
    assert 'Extend 7 days' in t and 'Choose a plan' in t and 'scrollToBillingPlans()' in t
    assert 'id="billingPlans"' in t and 'data-key="platoon_leader_monthly"' in t and '$19.99' in t and '/year' in t
    assert 'id="pricingStatus"' in t, 'checkout from the Billing page needs somewhere to say so'
    assert 'Update payment method' not in t, 'no portal before there is a Stripe customer'
    assert 'syncBillingWithStripe()' not in t, 'no refresh before there is a Stripe customer'
    assert 'Stripe will collect it at checkout' in t or 'Visa ending in 4242' in t
    assert 'Grace until Oct 4' in out['grace']['page'] and 'Your trial ended.' in out['grace']['page']

    a = out['active']['page']
    assert 'Annual' in a and 'billed yearly' in a and '$19.99' in a and '$1.67' in a, a
    assert 'Renews Oct 17' in a, a
    assert "openBillingPortal('plan')" in a and "openBillingPortal('cancel')" in a and 'Cancel subscription' in a
    assert "openBillingPortal('payment_method')" in a and 'View invoices' in a and 'Update billing details' in a
    assert 'syncBillingWithStripe()' in a and 'Sync with Stripe' in a, 'the Billing page offers the refresh'
    assert 'Visa ending in 4242' in a and 'Expires 04/28' in a, a
    assert '42 &middot; Unlimited' in a and '5 &middot; Unlimited' in a, 'the usage meters'
    assert 'startCheckout' not in a and 'id="billingPlans"' not in a, 'a subscribed account is not sold a plan'
    assert 'Billing history' in a and 'Paid' in a and 'Uncollectible' in a and '$19.99' in a
    assert 'href="https://invoice.stripe.com/i/1"' in a and 'href="https://pay.stripe.com/invoice/1/pdf"' in a
    assert 'Sep 17, 2026 – Sep 17, 2027' in a, 'the service period'

    c = out['cancelling']['page']
    assert 'Ends Oct 17' in c and 'not be billed again' in c and 'set to cancel' in c and 'Keep my plan' in c, c
    assert 'Cancel subscription' not in c
    comp = out['comped']['page']
    assert 'Complimentary access' in comp and 'startCheckout' not in comp and 'Payment method' not in comp
    assert 'Usage' in comp and 'openBillingPortal' not in comp
    p = out['past_due']['page']
    assert 'Past due' in p and 'Update card' in p and "openBillingPortal('payment_method')" in p, p
    assert 'Billing does not apply' in out['none']['page']

    assert 'Loading' in out['loading'] and '<table' not in out['loading'], 'details still loading'
    assert 'could not be reached' in out['stripeDown'] and 'could not be loaded' in out['stripeDown']
    assert 'No invoices yet' in out['noInvoices']
    assert out['money'] == ['$19.99', '—', '—', '$5.00'], out['money']


def test_hostile_strings_never_become_markup(out):
    for surface in ('banner', 'pricing', 'page'):
        html = out['hostile'][surface]
        assert '<img src=x' not in html, f'{surface}: a server string reached the page as markup'
        assert tags(html) <= ALLOWED_TAGS, f'{surface}: a server string opened a tag of its own: {sorted(tags(html))}'
    h = out['hostileDetails']
    assert '<img src=x' not in h and tags(h) <= ALLOWED_TAGS, sorted(tags(h))
    assert 'javascript:' not in h, 'only an https link may become an href'
    assert 'href="https://x.example/&quot;&gt;&lt;img' in h, 'an https link is escaped inside its attribute'
    assert 'Card ending' not in h and 'Payment method unavailable' not in h, 'a bad last4 is no card at all'
    assert 'data-key="&quot;&gt;&lt;img src=x onerror=alert(1)&gt;"' in out['hostile']['pricing'], \
        'the hostile lookup_key must be escaped inside the data-key attribute'
    assert 'startCheckout(this.dataset.key)' in out['hostile']['pricing'], \
        'the plan button must read the key from the DOM, not from an inline literal'
    assert "startCheckout('" not in out['hostile']['pricing'], \
        'no key may ever sit inside a JS string literal in onclick'
    assert '<img src=x' not in ''.join(out['toggle']) and 'NaN' not in out['toggle'][3]


def test_modal_rule(out):
    m = out['modal']
    assert m['due'] is True and m['seenYesterday'] is True
    assert m['seenToday'] is False, 'once a day'
    assert m['notUrgent'] is False and m['grace'] is False and m['none'] is False


def test_helpers(out):
    assert out['date'] == ['Oct 1', '', '', '']
    assert out['price'] == ['$2.99', '$19.99']
    assert "setBillingMode(7, 'comped')" in out['toggle'][0] and '>Comp<' in out['toggle'][0]
    assert "setBillingMode(7, 'default')" in out['toggle'][1] and '>Uncomp<' in out['toggle'][1]
    assert out['toggle'][2] == '', 'no billing row (unattached) — no toggle'


def main():
    out = render()
    test_banner_copy(out)
    test_pricing_screen(out)
    test_billing_page(out)
    test_hostile_strings_never_become_markup(out)
    test_modal_rule(out)
    test_helpers(out)
    print('ok')


if __name__ == '__main__':
    main()

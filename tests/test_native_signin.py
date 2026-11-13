"""The iOS app's sign-in handoff — run with: python tests/test_native_signin.py

Google refuses to sign anybody in inside an embedded web view, so the iOS app
(mobile/) opens /?native-signin=<nonce> in an in-app Safari sheet; once the
user is signed in there, POST /api/auth/native-ticket mints a short-lived Clerk
sign-in token and the page hands it back through the app's URL scheme. The app
redeems it only when the nonce is the one it sent.

Server: the route mints for the caller's own Clerk id and nobody else's, only
when signed in, rate-limited, 503 (never a 500) when Clerk is unreachable, and
outside the 402 gate. Frontend (lifted out of index.html and run under node):
the nonce rule, and the stash that carries the nonce across Google's redirect.
Wiring: the scheme, user-agent token and bundle id agree between index.html
and the native project, which nothing else would notice drifting apart.
"""
import json
import os
import plistlib
import re
import shutil
import subprocess
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(_HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, _HERE)
import dbharness  # noqa: E402
_schema = dbharness.setup()

import server  # noqa: E402

T = dbharness.make_tree('Native Co')
LEADER = dbharness.make_user(T['child'], 'leader', 'nativesarge')
OTHER = dbharness.make_user(T['child'], 'leader', 'nativeother')
STRANGER = dbharness.make_user(None, 'leader', 'nativestranger')
MOBILE = os.path.join(ROOT, 'mobile')

minted = []


def fake_token(clerk_user_id):
    minted.append(clerk_user_id)
    return f'ticket-for-{clerk_user_id}'


def post(client):
    return client.post('/api/auth/native-ticket')


def check_server(client):
    server._clerk_sign_in_token = fake_token

    dbharness.as_user(None)
    assert post(client).status_code == 401, 'signed out must not get a ticket'

    dbharness.as_user(LEADER)
    r = post(client)
    assert r.status_code == 200, r.get_json()
    assert r.get_json() == {'ticket': f"ticket-for-{LEADER['clerk_user_id']}"}, r.get_json()
    assert minted == [LEADER['clerk_user_id']], 'minted for someone other than the caller'
    conn = dbharness.owner_conn()
    rows = conn.execute("SELECT details FROM audit_log WHERE action = 'NATIVE_SIGNIN' AND root_id = %s",
                        (T['root'],)).fetchall()
    conn.close()
    assert len(rows) == 1, rows
    assert 'ticket-for' not in rows[0]['details'], 'the ticket itself landed in the audit log'

    r = post(client)
    assert r.status_code == 429 and r.headers.get('Retry-After'), 'a second mint inside the cooldown'
    assert len(minted) == 1, 'the cooldown still called Clerk'

    # Somebody signed in but attached nowhere is signing in to make a unit.
    dbharness.as_user(STRANGER)
    assert post(client).status_code == 200

    def down(_):
        raise OSError('clerk is down')
    server._clerk_sign_in_token = down
    dbharness.as_user(OTHER)
    r = post(client)
    assert r.status_code == 503, f'an unreachable Clerk is {r.status_code}, not 503'

    # The 402 gate exempts /api/auth/: a locked account still signs in, and
    # the app shows it the locked screen rather than a dead sign-in.
    assert any('/api/auth/native-ticket'.startswith(p) for p in server.BILLING_EXEMPT_PREFIXES)


DRIVER = r'''
(async () => {
  const out = {};
  // In the Safari sheet: a good nonce is stashed and the URL cleaned.
  window.location.search = '?native-signin=' + 'a'.repeat(32);
  captureNativeSignin();
  out.stashed = nativeSigninPending();
  out.replaced = replaced.slice();
  store[NATIVE_SIGNIN_KEY] = undefined; delete store[NATIVE_SIGNIN_KEY];
  window.location.search = '?native-signin=<script>';
  captureNativeSignin();
  out.badStashed = nativeSigninPending();

  // In the app.
  const ok = n => `platoonmanager://signin?ticket=T1&nonce=${n}`;
  await nativeSigninReturned(ok('whatever'));            // nothing in flight
  out.noneInFlight = created.length;
  await nativeSignInWithBrowser();
  const nonce = nativeSigninNonce;
  out.opened = opened.slice();
  out.nonceShape = /^[0-9a-f]{32}$/.test(nonce);
  await nativeSigninReturned(ok('f'.repeat(32)));        // someone else's link
  await nativeSigninReturned(`platoonmanager://elsewhere?ticket=T1&nonce=${nonce}`);
  await nativeSigninReturned(`https://evil.example/signin?ticket=T1&nonce=${nonce}`);
  out.wrong = created.length;
  await nativeSigninReturned(ok(nonce));
  out.created = created.slice();
  out.finalized = finalized.slice();
  out.closed = closed;
  await nativeSigninReturned(ok(nonce));                 // replayed
  out.replayed = created.length;
  console.log(JSON.stringify(out));
})();
'''

STUBS = r'''
const store = {};
const sessionStorage = {
  setItem: (k, v) => { store[k] = v; },
  getItem: k => (k in store ? store[k] : null),
  removeItem: k => { delete store[k]; },
};
const replaced = [];
const history = { replaceState: (s, t, u) => replaced.push(u) };
const opened = []; let closed = 0;
const window = {
  location: { origin: 'https://app.example', search: '' },
  Capacitor: {
    nativePromise: async (plugin, method, opts) => {
      if (plugin === 'Browser' && method === 'open') opened.push(opts.url);
      if (plugin === 'Browser' && method === 'close') closed++;
    },
  },
};
const crypto = require('crypto').webcrypto;
const NATIVE_APP = false;   // nativeSigninPending() reads it; the sheet is the web
const created = [], finalized = [];
const clerk = { client: { signIn: { create: async a => { created.push(a); return { status: 'complete', createdSessionId: 'sess_1' }; } } } };
async function ensureClerk() {}
async function finalizeAuth(sid) { finalized.push(sid); }
function showLoginScreen() {}
function setAuthError(m) { throw new Error('unexpected error: ' + m); }
function clerkErrorMessage(e, f) { return f; }
'''


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html — was it renamed or removed?'
    return m.group(0)


def check_frontend(src):
    node = shutil.which('node')
    assert node, 'node is required to run the frontend rules (it ships with the CI image)'
    js = '\n'.join([
        STUBS,
        extract(src, r"const NATIVE_SCHEME = '[^']+';", 'NATIVE_SCHEME'),
        extract(src, r"const NATIVE_SIGNIN_KEY = '[^']+';", 'NATIVE_SIGNIN_KEY'),
        "let nativeSigninNonce = '';",
        extract(src, r'function newNonce\(\) \{.*?\n\}', 'newNonce()'),
        extract(src, r'async function nativeSignInWithBrowser\(\) \{.*?\n\}', 'nativeSignInWithBrowser()'),
        extract(src, r'async function nativeSigninReturned\(link\) \{.*?\n\}', 'nativeSigninReturned()'),
        extract(src, r'function captureNativeSignin\(\) \{.*?\n\}', 'captureNativeSignin()'),
        extract(src, r'function nativeSigninPending\(\) \{.*?\n\}', 'nativeSigninPending()'),
        DRIVER,
    ])
    path = os.path.join(tempfile.mkdtemp(), 'native.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(js)
    proc = subprocess.run([node, path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)

    assert out['stashed'] == 'a' * 32 and out['replaced'] == ['/'], out
    assert out['badStashed'] == '', 'a malformed nonce was stashed'
    assert out['noneInFlight'] == 0, 'a link redeemed with no sign-in in flight'
    assert out['opened'] and out['opened'][0].startswith('https://app.example/?native-signin='), out['opened']
    assert out['nonceShape'], 'the nonce is not 32 hex characters'
    assert out['wrong'] == 0, 'a link with the wrong nonce, host or scheme was redeemed'
    assert out['created'] == [{'strategy': 'ticket', 'ticket': 'T1'}], out['created']
    assert out['finalized'] == ['sess_1'] and out['closed'] == 1, out
    assert out['replayed'] == 1, 'the same link redeemed twice'


def check_wiring(src):
    route = extract(src, r'function routeAfterLogin\(\) \{.*?\n\}', 'routeAfterLogin()')
    first = route.index('nativeSigninPending()')
    assert first < route.index('accountInactive') and first < route.index('showBillingScreen'), \
        'the Safari sheet must hand off before routeAfterLogin() goes anywhere else'
    assert 'captureNativeSignin()' in extract(src, r'async function initApp\(\) \{.*?\n\}', 'initApp()')
    assert 'nativeSignInWithBrowser()' in extract(src, r'async function authOAuth\(.*?\n\}', 'authOAuth()'), \
        'Google inside the app goes to accounts.google.com in the web view, which Google refuses'

    cfg = json.load(open(os.path.join(MOBILE, 'capacitor.config.json')))
    token = re.search(r'const NATIVE_APP = /\\b(\w+)\\//', src)
    assert token and cfg['appendUserAgent'].startswith(token.group(1) + '/'), \
        'the app does not identify itself with the user-agent token index.html looks for'
    scheme = extract(src, r"const NATIVE_SCHEME = '[^']+';", 'NATIVE_SCHEME').split("'")[1].rstrip(':')
    assert f'{scheme}://signin?' in src, 'the handoff link is not on the scheme the app listens for'
    plist = plistlib.load(open(os.path.join(MOBILE, 'ios', 'App', 'App', 'Info.plist'), 'rb'))
    schemes = [s for t in plist.get('CFBundleURLTypes', []) for s in t.get('CFBundleURLSchemes', [])]
    assert schemes == [scheme], f'Info.plist registers {schemes}, index.html hands back on {scheme}'
    pbx = open(os.path.join(MOBILE, 'ios', 'App', 'App.xcodeproj', 'project.pbxproj')).read()
    ids = set(re.findall(r'PRODUCT_BUNDLE_IDENTIFIER = ([^;]+);', pbx))
    assert ids == {cfg['appId']}, f'bundle ids {ids} != capacitor.config.json {cfg["appId"]}'
    assert cfg['server']['url'].startswith('https://'), 'the app must load the site over https'

    # The app sells nothing (Apple wants in-app purchase for that): every way
    # into Stripe is hidden under html.native-app.
    css = extract(src, r'/\* The iOS app \(mobile/.*?\{ display: none !important; \}', 'the native-app CSS')
    for sel in ('#billingBanner', '[data-settings="billing"]', '[data-personal="billing"]', '#inactiveBilling'):
        assert sel in css, f'{sel} is reachable inside the iOS app'
    assert 'NATIVE_APP' in extract(src, r'function showBillingScreen\(\) \{.*?\n\}', 'showBillingScreen()')
    assert 'NATIVE_APP' in extract(src, r'async function maybeShowBillingModal\(\) \{.*?\n\}', 'maybeShowBillingModal()')


def main():
    try:
        client = server.app.test_client()
        check_server(client)
        src = open(os.path.join(ROOT, 'index.html'), encoding='utf-8').read()
        check_frontend(src)
        check_wiring(src)
        print('ok')
    finally:
        dbharness.teardown(_schema)


if __name__ == '__main__':
    main()

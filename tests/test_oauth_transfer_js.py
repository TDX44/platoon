"""A Google account Clerk has never seen must still be able to get in.

Clerk's OAuth callback comes back to the SPA with no `__clerk*` query param
when the attempt has to cross flows: an unknown external account that started
a sign-IN is "transferable" to a sign-up, and a known one that started a
sign-UP is transferable to a sign-in. Without that transfer the rehearsal saw
the sign-in form again, no /api/auth/sync, and `external_account_not_found`.

`pendingOAuthTransfer()` is lifted out of index.html and run under node against
fake Clerk client objects, so this fails if the detection stops recognising
either direction, and the source assertions fail if the sign-up button goes
back to starting a sign-in or the transfer call disappears from initApp().

Rehearsal 3 found the next hole: a Google profile with a one-word name lands
the transferred sign-up at `missing_requirements` with
`missingFields: ['last_name']`, and Clerk then NAVIGATES to `continueSignUpUrl`.
The old code cleared the pending flag before that navigation, so the reload saw
a plain signed-out visit and painted a bare sign-in form — no message, no
account. `oauthGapKind()` is lifted the same way: it decides from
`missingFields` (never `requiredFields`, which lists `password` for an OAuth
sign-up that needs none) whether the gap is only names, which the app can close
with an inline form instead of sending the user away.

Run with: python tests/test_oauth_transfer_js.py
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')

STUB_CLERK = r'''
const calls = [];
const store = {};
const sessionStorage = {
  setItem: (k, v) => { store[k] = v; },
  getItem: k => (k in store ? store[k] : null),
  removeItem: k => { delete store[k]; },
};
const window = { location: { origin: 'https://platoondev.example' } };
const clerk = { client: {
  signIn: { authenticateWithRedirect: a => calls.push(['signIn', a]) },
  signUp: { authenticateWithRedirect: a => calls.push(['signUp', a]) },
} };
'''

DRIVER = r'''
const out = {};
out.nullClient = pendingOAuthTransfer(null);
out.empty = pendingOAuthTransfer({});
out.signin = pendingOAuthTransfer({ signIn: { firstFactorVerification: { status: 'transferable' } } });
out.signup = pendingOAuthTransfer({ signUp: { verifications: { externalAccount: { status: 'transferable' } } } });
// A fresh client carries objects with no verification yet — not a transfer.
out.idle = pendingOAuthTransfer({ signIn: { firstFactorVerification: null }, signUp: { verifications: {} } });
out.failed = pendingOAuthTransfer({ signIn: { firstFactorVerification: { status: 'failed' } } });

out.missingMsg = oauthIncompleteMessage('missing_requirements');
out.otherMsg = oauthIncompleteMessage('needs_identifier');

// The live rehearsal-3 payload: Google gave one word, so only last_name is
// short. requiredFields also lists password, which an OAuth sign-up never has.
out.gapLive = oauthGapKind({
  status: 'missing_requirements',
  missingFields: ['last_name'],
  requiredFields: ['last_name', 'email_address', 'password', 'first_name'],
});
out.gapBothNames = oauthGapKind({ status: 'missing_requirements', missingFields: ['first_name', 'last_name'] });
out.gapPassword = oauthGapKind({ status: 'missing_requirements', missingFields: ['password'] });
out.gapMixed = oauthGapKind({ status: 'missing_requirements', missingFields: ['last_name', 'phone_number'] });
out.gapEmpty = oauthGapKind({ status: 'missing_requirements', missingFields: [] });
out.gapNoFields = oauthGapKind({ status: 'missing_requirements' });
out.gapComplete = oauthGapKind({ status: 'complete', missingFields: ['last_name'] });
out.gapNull = oauthGapKind(null);
out.gapUndef = oauthGapKind(undefined);
out.namedMsg = oauthIncompleteMessage('missing_requirements', ['phone_number']);

(async () => {
  // Nothing is pending until a redirect is actually started.
  out.pendingBefore = getOAuthPending();
  await authOAuth('oauth_google', 'signup');
  out.pendingAfterSignup = getOAuthPending();
  clearOAuthPending();
  out.pendingCleared = getOAuthPending();
  await authOAuth('oauth_google', 'signin');
  out.pendingAfterSignin = getOAuthPending();
  out.flows = calls.map(c => c[0]);
  out.redirects = calls.map(c => [c[1].redirectUrl, c[1].redirectUrlComplete]);
  out.strategy = calls[0][1].strategy;
  console.log(JSON.stringify(out));
})();
'''


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html — was it renamed or removed?'
    return m.group(0)


def script_text(source):
    """The one big inline <script> that is the whole SPA."""
    blocks = re.findall(r'<script>(.*?)</script>', source, re.S)
    assert blocks, 'no inline script in index.html'
    return max(blocks, key=len)


def main():
    src = open(INDEX, encoding='utf-8').read()
    node = shutil.which('node')
    assert node, 'node is required to run the frontend rules (it ships with the CI image)'

    # 1. The sign-up form's Google button starts a sign-UP, not a sign-in.
    assert re.search(r"authOAuth\('oauth_google',\s*'signup'\)", src), \
        'googleSignup does not pass the signup mode to authOAuth()'
    assert re.search(r"authOAuth\('oauth_google',\s*'signin'\)", src), \
        'googleSignin does not pass the signin mode to authOAuth()'

    # 2. initApp() actually performs the transfer when the callback did not.
    init = extract(src, r'async function initApp\(\) \{.*?\n\}', 'initApp()')
    assert 'pendingOAuthTransfer(' in init, 'initApp() never checks for a pending OAuth transfer'
    assert 'completeOAuthTransfer(' in init, 'initApp() never performs the transfer'
    transfer = extract(src, r'async function completeOAuthTransfer\(.*?\n\}', 'completeOAuthTransfer()')
    assert transfer.count('transfer: true') == 2, \
        'both directions must be transferred (signUp.create and signIn.create)'
    assert 'setActive(' in transfer, 'a completed transfer must be made the active session'
    assert 'oauthIncompleteMessage(' in transfer, \
        'a sign-up that needs more fields must say so, not loop'

    # The client outlives the redirect, so the callback/transfer path must be
    # gated on this navigation — a __clerk* param or the flag authOAuth() set.
    assert 'getOAuthPending()' in init, \
        'initApp() re-enters the transfer path on every signed-out visit'
    assert 'clearOAuthPending()' in init, 'the pending flag is never consumed'
    assert 'setOAuthPending(' in extract(src, r'async function authOAuth\(.*?\n\}', 'authOAuth()'), \
        'authOAuth() never marks the redirect as pending'
    # The transfer decision must be re-read AFTER the callback, not before it.
    assert re.search(r'handleRedirectCallback\(.*?pendingOAuthTransfer\(clerk\.client\)', init, re.S), \
        'initApp() reuses a stale transfer value from before handleRedirectCallback()'
    for key in ('signInUrl', 'signUpUrl', 'continueSignUpUrl'):
        assert key in init, f'handleRedirectCallback() does not pin {key} to this app'

    # 2b. The flow survives Clerk's OWN navigation to continueSignUpUrl.
    # Clearing the flag before the callback is exactly the rehearsal-3 bug: the
    # reload came back with nothing left saying an OAuth flow was in progress.
    assert init.index('handleRedirectCallback(') < init.index('clearOAuthPending()'), \
        'initApp() consumes the pending flag before Clerk can navigate away'
    assert "'oauth=continue'" in init or '?oauth=continue' in init, \
        "the onward URLs carry no marker, so Clerk's own reload lands on a bare sign-in form"
    assert re.search(r"params\.get\('oauth'\)", init), \
        'initApp() never reads the resume marker back'
    # Consumed once, and the callback is not re-run on the resume leg: that is
    # what stops a missing_requirements sign-up from bouncing forever.
    assert 'replaceState' in init, 'the resume marker is never consumed'
    assert 'oauthGapKind(' in init, 'initApp() does not classify a stalled sign-up'

    # 2c. A names-only gap is filled in the app, not sent away.
    assert re.search(r'signUp\.update\(\{\s*firstName', src), \
        'nothing calls signUp.update({ firstName, lastName }) to close a name gap'
    render = extract(src, r'function renderAuthForm\(\) \{.*?\n\}', 'renderAuthForm()')
    names_view = extract(render, r"if \(authView === 'oauth-names'\).*?\n  \}",
                         "the 'oauth-names' view")
    assert 'Finish creating your account' in names_view, names_view[:200]
    for field in ('oauthFirstName', 'oauthLastName'):
        assert field in names_view, f'the name-completion form has no {field} input'

    # 3b. Clerk's Smart CAPTCHA needs a mount point wherever a sign-up can be
    # created — including the transfer that starts from the sign-IN form.
    signin_view = extract(render, r"if \(authView === 'signin'\).*?\n  \}", "the 'signin' view")
    for view, label in ((signin_view, 'signin'), (names_view, 'oauth-names')):
        assert 'id="clerk-captcha"' in view, \
            f'the {label} view has no #clerk-captcha element for Clerk to mount into'

    # 4. The inline script still parses.
    body = script_text(src)
    path = os.path.join(tempfile.mkdtemp(), 'spa.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(body)
    proc = subprocess.run([node, '--check', path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr

    # 5. The detection itself, run for real.
    js = '\n'.join([
        STUB_CLERK,
        extract(src, r"const OAUTH_PENDING_KEY = '[^']+';", 'OAUTH_PENDING_KEY'),
        extract(src, r'function setOAuthPending\(.*?\n\}', 'setOAuthPending()'),
        extract(src, r'function getOAuthPending\(.*?\n\}', 'getOAuthPending()'),
        extract(src, r'function clearOAuthPending\(.*?\n\}', 'clearOAuthPending()'),
        extract(src, r'async function authOAuth\(.*?\n\}', 'authOAuth()'),
        extract(src, r'function pendingOAuthTransfer\(.*?\n\}', 'pendingOAuthTransfer()'),
        extract(src, r'const OAUTH_NAME_FIELDS = \[[^\]]*\];', 'OAUTH_NAME_FIELDS'),
        extract(src, r'function oauthGapKind\(.*?\n\}', 'oauthGapKind()'),
        extract(src, r'const OAUTH_FIELD_LABELS = \{.*?\n\};', 'OAUTH_FIELD_LABELS'),
        extract(src, r'function oauthIncompleteMessage\(.*?\n\}', 'oauthIncompleteMessage()'),
        DRIVER,
    ])
    path = os.path.join(tempfile.mkdtemp(), 'oauth.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(js)
    proc = subprocess.run([node, path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)

    assert out['nullClient'] is None, 'no client at all is not a transfer'
    assert out['empty'] is None, 'an empty client is not a transfer'
    assert out['signin'] == 'signin', 'a transferable sign-in must become a sign-up'
    assert out['signup'] == 'signup', 'a transferable sign-up must become a sign-in'
    assert out['idle'] is None, 'an untouched client must not look like a transfer'
    assert out['failed'] is None, 'only "transferable" counts'
    assert out['flows'] == ['signUp', 'signIn'], \
        f"the Google buttons start the wrong flows: {out['flows']}"
    assert out['strategy'] == 'oauth_google', out['strategy']
    # initApp() runs at the origin root, so that is where the callback must land.
    for pair in out['redirects']:
        assert pair == ['https://platoondev.example/', 'https://platoondev.example/'], pair
    assert out['pendingBefore'] == '', 'a visit with no redirect must not look pending'
    assert out['pendingAfterSignup'] == 'signup', out['pendingAfterSignup']
    assert out['pendingCleared'] == '', 'the pending flag must clear'
    assert out['pendingAfterSignin'] == 'signin', out['pendingAfterSignin']
    assert 'email and password' in out['missingMsg'], out['missingMsg']
    assert 'needs_identifier' in out['otherMsg'], out['otherMsg']

    assert out['gapLive'] == 'names', \
        'the rehearsal-3 payload (only last_name short) must be fillable in-app'
    assert out['gapBothNames'] == 'names', out['gapBothNames']
    assert out['gapPassword'] == 'other', 'a missing password is not a name gap'
    assert out['gapMixed'] == 'other', 'one non-name field makes the whole gap "other"'
    assert out['gapEmpty'] == 'other', 'an unexplained stall must not claim to be names'
    assert out['gapNoFields'] == 'other', out['gapNoFields']
    assert out['gapComplete'] is None, 'a finished sign-up has no gap'
    assert out['gapNull'] is None and out['gapUndef'] is None, 'no sign-up, no gap'
    assert 'phone number' in out['namedMsg'], \
        f"a gap we cannot fill must name the field: {out['namedMsg']}"
    print('ok')


if __name__ == '__main__':
    main()

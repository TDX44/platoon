# Native Login Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Clerk's prebuilt sign-in widget in Platoon with a hand-built, native-looking auth flow (email+password, Google OAuth, forgot-password reset, self sign-up) driven by ClerkJS's imperative resources, styled with the existing `.login-card` CSS.

**Architecture:** Frontend-only change to the single-file `index.html` SPA. A module-level `authView` string drives a `renderAuthForm()` function that redraws the contents of `#clerkAuthMount`. Auth actions call `clerk.client.signIn` / `clerk.client.signUp` imperative methods; on success, `clerk.setActive()` → existing `syncClerkUser()` → existing `routeAfterLogin()`. The old `mountClerkSignIn()` is retained as a manual "classic sign-in" escape hatch. No `server.py` changes.

**Tech Stack:** Vanilla JS, `@clerk/clerk-js@5` (Clerk "Core 2"), inline CSS in `index.html`. No build step, no test framework.

---

## Testing note

This project has **no automated test suite and no linter** (`CLAUDE.md`). Verification at each gate is **manual in the browser**. To run locally:

```bash
cd ~/github/platoon
python server.py   # Flask dev server on http://localhost:5000, debug=True
```

Clerk must be configured (`.env` with `CLERK_PUBLISHABLE_KEY` etc.) for any auth path to work. The Clerk Dashboard for that instance must have **Password**, **email verification code (`email_code`)**, **Google SSO**, and **password reset** enabled, or the corresponding calls return a `strategy not allowed` error.

After each task, hard-reload `http://localhost:5000` (Ctrl+Shift+R) to pick up `index.html` changes.

---

## File structure

Single file touched: `index.html`.

- **CSS block** (~line 222–256, the `/* ─── Login screen ─── */` section): add classes for the OAuth button, "or" divider, ghost/toggle links, and code input; relax `#clerkAuthMount` so the form fills width instead of being a centered fixed-height box.
- **Login markup** (~line 2362–2375): unchanged structurally; `#clerkAuthMount` now receives our form. Remove the now-misleading `#loginStatus` default text.
- **Auth JS** (~line 5073–5310, the `// ─── Auth ───` section): add the view state machine, render function, and action handlers. Repoint `showLoginScreen()` and `initApp()` from `mountClerkSignIn()` to `renderAuthForm()`. Keep `mountClerkSignIn()` for the escape hatch.

---

## Task 1: CSS for the native auth form

**Files:**
- Modify: `index.html:255` (the `#clerkAuthMount` rule) and insert new rules after line 256.

- [ ] **Step 1: Relax the mount container and add form CSS**

Replace the `#clerkAuthMount` rule at `index.html:255`:

```css
  #clerkAuthMount { min-height: 320px; width: 100%; display: flex; justify-content: center; }
```

with this block (the original rule, relaxed, plus the new form classes):

```css
  #clerkAuthMount { width: 100%; }
  /* native auth form */
  .auth-form { width: 100%; display: flex; flex-direction: column; }
  .auth-hint { color: #8aa0b6; font-size: 0.85em; margin-bottom: 12px; line-height: 1.4; }
  .btn-oauth {
    width: 100%; padding: 11px; margin-bottom: 4px;
    background: #1a1a2e; color: #e0e0e0;
    border: 1px solid #3a4256; border-radius: 6px;
    font-size: 0.95em; font-weight: 600; cursor: pointer;
    display: flex; align-items: center; justify-content: center; gap: 8px;
    transition: background 0.2s, border-color 0.2s;
  }
  .btn-oauth:hover { background: #21213a; border-color: #4a5a80; }
  .btn-oauth:disabled { opacity: 0.6; cursor: default; }
  .auth-divider { display: flex; align-items: center; gap: 10px; margin: 14px 0; }
  .auth-divider::before, .auth-divider::after { content: ''; flex: 1; height: 1px; background: #3a4256; }
  .auth-divider span { color: #7d88a6; font-size: 0.72em; font-weight: 600; letter-spacing: 0.08em; text-transform: uppercase; }
  .auth-link { background: none; border: none; color: #6aa6ff; font-size: 0.82em; cursor: pointer; padding: 0; }
  .auth-link:hover { text-decoration: underline; }
  .auth-forgot { display: flex; justify-content: flex-end; margin: -8px 0 12px; }
  .auth-switch { text-align: center; color: #8aa0b6; font-size: 0.82em; margin-top: 14px; }
  .auth-back { text-align: center; margin-top: 12px; }
  .auth-code-input { letter-spacing: 0.3em; text-align: center; font-size: 1.1em !important; }
```

- [ ] **Step 2: Add light-mode overrides for the new classes**

Find the existing light-mode login override near `index.html:380`:

```css
  body.light-mode .login-card input { background: #f0f4f8; border-color: #c0c8d8; color: #2d3436; }
```

Immediately after it, add:

```css
  body.light-mode .btn-oauth { background: #f0f4f8; border-color: #c0c8d8; color: #2d3436; }
  body.light-mode .btn-oauth:hover { background: #e4ebf3; }
  body.light-mode .auth-divider::before, body.light-mode .auth-divider::after { background: #c0c8d8; }
```

- [ ] **Step 3: Verify CSS loads (no behavior yet)**

Hard-reload the page. The login screen should still render the Clerk widget (unchanged so far) with no console errors. Confirm no CSS syntax errors in DevTools.

- [ ] **Step 4: Commit**

```bash
git add index.html
git commit -m "feat: add CSS for native auth form"
```

---

## Task 2: Sign-in view (email + password) replacing the widget default

This is the core task: introduce the state machine, render function, helpers, and the email/password sign-in handler, and repoint the entry points away from the widget.

**Files:**
- Modify: `index.html` auth section (functions near lines 5082, 5169, 5294) and the login markup (line 2372).

- [ ] **Step 1: Remove the stale Clerk status default text**

At `index.html:2372`, change:

```html
    <div class="login-status" id="loginStatus">Loading Clerk sign-in…</div>
```

to:

```html
    <div class="login-status" id="loginStatus"></div>
```

- [ ] **Step 2: Add the state machine + helpers + sign-in handler**

Insert the following block immediately **before** the existing `function mountClerkSignIn()` declaration (currently at `index.html:5169`). This defines everything the render function needs.

```javascript
// ─── Native auth flow (replaces the prebuilt Clerk widget) ───
let authView = 'signin';
let authBusy = false;

function clerkErrorMessage(err, fallback) {
  const first = err && err.errors && err.errors[0];
  return (
    (first && (first.longMessage || first.message)) ||
    (err && (err.longMessage || err.message)) ||
    fallback
  );
}

function setAuthError(message) {
  const errEl = document.getElementById('loginError');
  if (!errEl) return;
  if (message) {
    errEl.textContent = message;
    errEl.style.display = 'block';
  } else {
    errEl.textContent = '';
    errEl.style.display = 'none';
  }
}

function setAuthView(view) {
  authView = view;
  setAuthError('');
  renderAuthForm();
}

async function finalizeAuth(createdSessionId) {
  await clerk.setActive({ session: createdSessionId });
  currentUser = await syncClerkUser();
  routeAfterLogin();
}

// Wraps an async handler: disables the form, shows a busy label, restores on finish.
function withBusy(button, busyLabel, fn) {
  return async (event) => {
    if (event) event.preventDefault();
    if (authBusy) return;
    authBusy = true;
    setAuthError('');
    const idleLabel = button ? button.textContent : '';
    if (button) { button.disabled = true; button.textContent = busyLabel; }
    try {
      await fn();
    } catch (err) {
      setAuthError(clerkErrorMessage(err, 'Something went wrong. Please try again.'));
    } finally {
      authBusy = false;
      if (button) { button.disabled = false; button.textContent = idleLabel; }
    }
  };
}

async function authSignIn(email, password) {
  const si = await clerk.client.signIn.create({ identifier: email, password });
  if (si.status === 'complete') {
    await finalizeAuth(si.createdSessionId);
    return;
  }
  setAuthError(`Additional verification required (${si.status}). This sign-in method is not yet supported here.`);
}
```

- [ ] **Step 3: Add the render function**

Insert this `renderAuthForm()` function immediately after the block from Step 2 (still before `mountClerkSignIn()`). For Task 2 it only handles the `signin` view; later tasks extend the `switch`.

```javascript
function renderAuthForm() {
  const mount = document.getElementById('clerkAuthMount');
  if (!mount) return;
  setLoginStatus('');
  const subtitle = document.getElementById('loginSubtitle');

  if (authView === 'signin') {
    if (subtitle) subtitle.textContent = 'Sign in to continue';
    mount.innerHTML = `
      <form class="auth-form" id="signinForm">
        <label for="signinEmail">Email</label>
        <input id="signinEmail" type="email" autocomplete="email" required autofocus placeholder="you@example.com">
        <label for="signinPassword">Password</label>
        <input id="signinPassword" type="password" autocomplete="current-password" required placeholder="Your password">
        <div class="auth-forgot"><button type="button" class="auth-link" id="forgotLink">Forgot password?</button></div>
        <button type="submit" class="btn-login" id="signinSubmit">Sign in</button>
        <div class="auth-switch">New here? <button type="button" class="auth-link" id="toSignup">Create an account</button></div>
        <div class="auth-back"><button type="button" class="auth-link" id="classicLink">Trouble signing in? Use classic sign-in</button></div>
      </form>`;
    const form = document.getElementById('signinForm');
    const submit = document.getElementById('signinSubmit');
    form.addEventListener('submit', withBusy(submit, 'Signing in…', async () => {
      await authSignIn(
        document.getElementById('signinEmail').value.trim(),
        document.getElementById('signinPassword').value
      );
    }));
    document.getElementById('forgotLink').addEventListener('click', () => setAuthView('reset-request'));
    document.getElementById('toSignup').addEventListener('click', () => setAuthView('signup'));
    document.getElementById('classicLink').addEventListener('click', showClassicSignIn);
    return;
  }
}
```

> `showClassicSignIn`, the `signup`/`reset-*`/`verify-email` views, and the Google button are added in later tasks. Until Task 6 adds `showClassicSignIn`, clicking the classic link throws a handled error in the console — that is expected and fixed in Task 6.

- [ ] **Step 4: Repoint `showLoginScreen()` to the native form**

In `showLoginScreen()` (currently `index.html:5091-5096`), change:

```javascript
  ensureClerk()
    .then(() => mountClerkSignIn())
    .catch(err => {
      errEl.textContent = err.message || 'Unable to load Clerk.';
      errEl.style.display = 'block';
    });
```

to:

```javascript
  ensureClerk()
    .then(() => renderAuthForm())
    .catch(err => {
      errEl.textContent = err.message || 'Unable to load Clerk.';
      errEl.style.display = 'block';
    });
```

- [ ] **Step 5: Repoint `initApp()` away from the widget**

In `initApp()` (currently `index.html:5296-5301`), change:

```javascript
    await ensureClerk();
    if (!clerk.user) {
      mountClerkSignIn();
      showLoginScreen();
      return;
    }
```

to:

```javascript
    await ensureClerk();
    if (!clerk.user) {
      showLoginScreen();
      return;
    }
```

- [ ] **Step 6: Verify email/password sign-in in the browser**

Run `python server.py`, open `http://localhost:5000`, hard-reload.
- Expected: the native form renders inside the login card (email, password, "Forgot password?", "Create an account", "classic sign-in" link) — NOT the Clerk widget.
- Sign in with a valid Clerk email/password → app loads, you land in the home view.
- Sign out, enter a wrong password → inline red error appears in `#loginError`, button re-enables, no console crash.

- [ ] **Step 7: Commit**

```bash
git add index.html
git commit -m "feat: native email/password sign-in replacing Clerk widget"
```

---

## Task 3: Google OAuth button

**Files:**
- Modify: `index.html` — add `authOAuth()`, insert the Google button into the `signin` view, and add a redirect-callback guard in `initApp()`.

- [ ] **Step 1: Add the OAuth handler**

Add this function right after `authSignIn()` (from Task 2):

```javascript
async function authOAuth(strategy) {
  const redirectBase = window.location.origin + '/';
  await clerk.client.signIn.authenticateWithRedirect({
    strategy,
    redirectUrl: redirectBase,
    redirectUrlComplete: redirectBase,
  });
}
```

- [ ] **Step 2: Add the Google button + divider to the signin view**

In `renderAuthForm()`'s `signin` branch, replace the form template's opening (the part before `<label for="signinEmail">`) so the OAuth button and divider appear first. Change:

```javascript
    mount.innerHTML = `
      <form class="auth-form" id="signinForm">
        <label for="signinEmail">Email</label>
```

to:

```javascript
    mount.innerHTML = `
      <form class="auth-form" id="signinForm">
        <button type="button" class="btn-oauth" id="googleSignin">Continue with Google</button>
        <div class="auth-divider"><span>or</span></div>
        <label for="signinEmail">Email</label>
```

Then, in the same `signin` branch's wiring section (after the `classicLink` listener), add:

```javascript
    document.getElementById('googleSignin').addEventListener('click',
      withBusy(document.getElementById('googleSignin'), 'Redirecting…', () => authOAuth('oauth_google')));
```

- [ ] **Step 3: Add a redirect-callback guard to `initApp()`**

ClerkJS v5 normally completes the OAuth handshake during `clerk.load()`. As a safety net, handle the callback explicitly when Clerk redirect params are present. In `initApp()`, change:

```javascript
    await ensureClerk();
    if (!clerk.user) {
      showLoginScreen();
      return;
    }
```

to:

```javascript
    await ensureClerk();
    if (!clerk.user) {
      const params = new URLSearchParams(window.location.search);
      const hasClerkRedirect = [...params.keys()].some(k => k.startsWith('__clerk'));
      if (hasClerkRedirect && typeof clerk.handleRedirectCallback === 'function') {
        try {
          await clerk.handleRedirectCallback({});
        } catch (cbErr) {
          console.warn('Clerk redirect callback failed:', cbErr);
        }
      }
      if (!clerk.user) {
        showLoginScreen();
        return;
      }
    }
```

- [ ] **Step 4: Verify Google OAuth round-trip**

Hard-reload. Click "Continue with Google" → redirects to Google → after consent, returns to the app and lands in the home view (signed in).
- If it returns to the login screen instead of signing in, check the DevTools console/network for the callback params and confirm `clerk.handleRedirectCallback` ran. The guard in Step 3 should cover it; if not, note the actual returned URL params and adjust the `startsWith('__clerk')` prefix to match.

- [ ] **Step 5: Commit**

```bash
git add index.html
git commit -m "feat: add Google OAuth to native login"
```

---

## Task 4: Forgot-password reset flow

**Files:**
- Modify: `index.html` — add `authResetRequest()`, `authResetVerify()`, and the `reset-request` / `reset-verify` views.

- [ ] **Step 1: Add the reset handlers**

Add after `authOAuth()`:

```javascript
async function authResetRequest(email) {
  // Anti-enumeration: try to start the reset + send the code, but swallow
  // "identifier not found" errors and ALWAYS advance to the verify screen.
  try {
    await clerk.client.signIn.create({
      strategy: 'reset_password_email_code',
      identifier: email,
    });
  } catch (err) {
    // do not disclose whether the account exists
  }
  setAuthView('reset-verify');
}

async function authResetVerify(code, newPassword) {
  await clerk.client.signIn.attemptFirstFactor({
    strategy: 'reset_password_email_code',
    code,
  });
  const si = await clerk.client.signIn.resetPassword({ password: newPassword });
  if (si.status === 'complete') {
    await finalizeAuth(si.createdSessionId);
    return;
  }
  setAuthError(`Reset incomplete (${si.status}). Please try again.`);
}
```

- [ ] **Step 2: Add the `reset-request` view to `renderAuthForm()`**

Add this branch inside `renderAuthForm()` after the `signin` branch's `return;`:

```javascript
  if (authView === 'reset-request') {
    if (subtitle) subtitle.textContent = 'Reset your password';
    mount.innerHTML = `
      <form class="auth-form" id="resetReqForm">
        <p class="auth-hint">Enter your email and we'll send a code to reset your password.</p>
        <label for="resetEmail">Email</label>
        <input id="resetEmail" type="email" autocomplete="email" required autofocus placeholder="you@example.com">
        <button type="submit" class="btn-login" id="resetReqSubmit">Send reset code</button>
        <div class="auth-back"><button type="button" class="auth-link" id="resetBackToSignin">Back to sign in</button></div>
      </form>`;
    const form = document.getElementById('resetReqForm');
    const submit = document.getElementById('resetReqSubmit');
    form.addEventListener('submit', withBusy(submit, 'Sending…', async () => {
      await authResetRequest(document.getElementById('resetEmail').value.trim());
    }));
    document.getElementById('resetBackToSignin').addEventListener('click', () => setAuthView('signin'));
    return;
  }
```

- [ ] **Step 3: Add the `reset-verify` view**

Add this branch after the `reset-request` branch:

```javascript
  if (authView === 'reset-verify') {
    if (subtitle) subtitle.textContent = 'Enter your reset code';
    mount.innerHTML = `
      <form class="auth-form" id="resetVerifyForm">
        <p class="auth-hint">If an account exists for that email, we've sent a reset code. Enter it below and choose a new password.</p>
        <label for="resetCode">Reset code</label>
        <input id="resetCode" type="text" inputmode="numeric" autocomplete="one-time-code" required autofocus placeholder="123456" class="auth-code-input">
        <label for="resetNewPassword">New password</label>
        <input id="resetNewPassword" type="password" autocomplete="new-password" required minlength="8" placeholder="At least 8 characters">
        <button type="submit" class="btn-login" id="resetVerifySubmit">Reset password &amp; sign in</button>
        <div class="auth-back"><button type="button" class="auth-link" id="resetVerifyBack">Back to sign in</button></div>
      </form>`;
    const form = document.getElementById('resetVerifyForm');
    const submit = document.getElementById('resetVerifySubmit');
    form.addEventListener('submit', withBusy(submit, 'Resetting…', async () => {
      await authResetVerify(
        document.getElementById('resetCode').value.trim(),
        document.getElementById('resetNewPassword').value
      );
    }));
    document.getElementById('resetVerifyBack').addEventListener('click', () => setAuthView('signin'));
    return;
  }
```

- [ ] **Step 4: Verify reset flow**

Hard-reload. From the signin view click "Forgot password?" → enter your email → "Send reset code". Check email, enter the code + a new password → app signs you in.
- Repeat with a non-existent email: it must still advance to the verify screen (no error revealing the account doesn't exist). Entering a bogus code there produces a normal "incorrect code" error.

- [ ] **Step 5: Commit**

```bash
git add index.html
git commit -m "feat: add forgot-password reset to native login"
```

---

## Task 5: Self sign-up flow

**Files:**
- Modify: `index.html` — add `authSignUp()`, `authVerifyEmail()`, and the `signup` / `verify-email` views (with a CAPTCHA mount).

- [ ] **Step 1: Add the sign-up handlers**

Add after `authResetVerify()`:

```javascript
async function authSignUp(email, password) {
  await clerk.client.signUp.create({ emailAddress: email, password });
  await clerk.client.signUp.prepareEmailAddressVerification({ strategy: 'email_code' });
  setAuthView('verify-email');
}

async function authVerifyEmail(code) {
  const su = await clerk.client.signUp.attemptEmailAddressVerification({ code });
  if (su.status === 'complete') {
    await finalizeAuth(su.createdSessionId);
    return;
  }
  setAuthError(`Verification incomplete (${su.status}). Please try again.`);
}
```

- [ ] **Step 2: Add the `signup` view**

Add this branch in `renderAuthForm()` after the `reset-verify` branch. It includes the `#clerk-captcha` mount that Clerk's Smart CAPTCHA needs if bot protection is on.

```javascript
  if (authView === 'signup') {
    if (subtitle) subtitle.textContent = 'Create your account';
    mount.innerHTML = `
      <form class="auth-form" id="signupForm">
        <button type="button" class="btn-oauth" id="googleSignup">Continue with Google</button>
        <div class="auth-divider"><span>or</span></div>
        <label for="signupEmail">Email</label>
        <input id="signupEmail" type="email" autocomplete="email" required autofocus placeholder="you@example.com">
        <label for="signupPassword">Password</label>
        <input id="signupPassword" type="password" autocomplete="new-password" required minlength="8" placeholder="At least 8 characters">
        <div id="clerk-captcha"></div>
        <button type="submit" class="btn-login" id="signupSubmit">Create account</button>
        <div class="auth-switch">Already have an account? <button type="button" class="auth-link" id="toSignin">Sign in</button></div>
      </form>`;
    const form = document.getElementById('signupForm');
    const submit = document.getElementById('signupSubmit');
    form.addEventListener('submit', withBusy(submit, 'Creating account…', async () => {
      await authSignUp(
        document.getElementById('signupEmail').value.trim(),
        document.getElementById('signupPassword').value
      );
    }));
    document.getElementById('googleSignup').addEventListener('click',
      withBusy(document.getElementById('googleSignup'), 'Redirecting…', () => authOAuth('oauth_google')));
    document.getElementById('toSignin').addEventListener('click', () => setAuthView('signin'));
    return;
  }
```

- [ ] **Step 3: Add the `verify-email` view**

Add this branch after the `signup` branch:

```javascript
  if (authView === 'verify-email') {
    if (subtitle) subtitle.textContent = 'Verify your email';
    mount.innerHTML = `
      <form class="auth-form" id="verifyEmailForm">
        <p class="auth-hint">We've sent a verification code to your email. Enter it below to finish creating your account.</p>
        <label for="verifyCode">Verification code</label>
        <input id="verifyCode" type="text" inputmode="numeric" autocomplete="one-time-code" required autofocus placeholder="123456" class="auth-code-input">
        <button type="submit" class="btn-login" id="verifyEmailSubmit">Verify &amp; continue</button>
        <div class="auth-back"><button type="button" class="auth-link" id="verifyBackToSignin">Back to sign in</button></div>
      </form>`;
    const form = document.getElementById('verifyEmailForm');
    const submit = document.getElementById('verifyEmailSubmit');
    form.addEventListener('submit', withBusy(submit, 'Verifying…', async () => {
      await authVerifyEmail(document.getElementById('verifyCode').value.trim());
    }));
    document.getElementById('verifyBackToSignin').addEventListener('click', () => setAuthView('signin'));
    return;
  }
```

- [ ] **Step 4: Verify sign-up flow**

Hard-reload. From signin, click "Create an account" → enter a new email + password → "Create account". If CAPTCHA is enabled it renders in `#clerk-captcha`. Receive the email code → enter it → app signs you in as the new user.
- Per the spec, the first Clerk user becomes admin automatically; a later new user lands with no platoon access (expected — an admin grants access afterward).

- [ ] **Step 5: Commit**

```bash
git add index.html
git commit -m "feat: add self sign-up with email verification to native login"
```

---

## Task 6: Escape hatch (classic widget)

**Files:**
- Modify: `index.html` — add `showClassicSignIn()`; make `mountClerkSignIn()` render a "back" link.

- [ ] **Step 1: Add `showClassicSignIn()`**

Add this function right before `renderAuthForm()`:

```javascript
function showClassicSignIn() {
  const subtitle = document.getElementById('loginSubtitle');
  if (subtitle) subtitle.textContent = 'Classic sign-in';
  setAuthError('');
  const mount = document.getElementById('clerkAuthMount');
  if (mount) {
    mount.innerHTML = '<div id="classicMount"></div><div class="auth-back"><button type="button" class="auth-link" id="backToNative">Back to Platoon sign-in</button></div>';
    document.getElementById('backToNative').addEventListener('click', () => setAuthView('signin'));
    mountClerkSignIn(document.getElementById('classicMount'));
  }
}
```

- [ ] **Step 2: Make `mountClerkSignIn()` accept a target element**

`mountClerkSignIn()` currently looks up `#clerkAuthMount` itself. Update its first lines so it can mount into the `#classicMount` sub-element instead. Change the start of `mountClerkSignIn()`:

```javascript
function mountClerkSignIn() {
  const mount = document.getElementById('clerkAuthMount');
  if (!mount || !clerk) return;
```

to:

```javascript
function mountClerkSignIn(targetEl) {
  const mount = targetEl || document.getElementById('clerkAuthMount');
  if (!mount || !clerk) return;
```

The rest of `mountClerkSignIn()` (the `clerk.mountSignIn(mount, {...})` call) is unchanged.

- [ ] **Step 3: Verify escape hatch**

Hard-reload. On the signin view click "Trouble signing in? Use classic sign-in" → the prebuilt Clerk widget renders, with a "Back to Platoon sign-in" link below it. Sign in via the widget → app loads. Reload, open classic, click "Back" → native form returns.

- [ ] **Step 4: Commit**

```bash
git add index.html
git commit -m "feat: add classic sign-in escape hatch to native login"
```

---

## Task 7: Theme pass and cleanup

**Files:**
- Modify: `index.html` — verify both themes; remove any dead `setLoginStatus` usage that no longer makes sense.

- [ ] **Step 1: Confirm `setLoginStatus` is still coherent**

`renderAuthForm()` calls `setLoginStatus('')` to clear the (now empty) status line; `ensureClerk()` still uses `setLoginStatus(...)` for "Loading Clerk…" messages during script load, which is correct. No change needed unless a stray status string lingers after the form renders — if so, confirm `renderAuthForm()`'s `setLoginStatus('')` runs at the top of each view.

- [ ] **Step 2: Verify dark + light themes**

Hard-reload in dark mode: inspect all five views (signin, signup, verify-email, reset-request, reset-verify) and the classic escape hatch — inputs, buttons, OAuth button, divider, and links should all be legible and on-brand.
Toggle light mode (the app's theme toggle) and re-check the login screen: OAuth button and divider use the light overrides from Task 1; inputs use the existing light input override. Fix any contrast issue by adjusting the Task 1 light-mode block.

- [ ] **Step 3: Full regression pass**

Confirm, end to end: email/password sign-in, wrong-password error, Google OAuth, forgot-password reset (incl. unknown email → still advances), self sign-up + email verification, escape hatch both directions, and reload-while-signed-in staying signed in.

- [ ] **Step 4: Commit**

```bash
git add index.html
git commit -m "chore: theme pass and cleanup for native login"
```

---

## Self-review notes

- **Spec coverage:** email+password (Task 2), Google OAuth (Task 3), forgot-password (Task 4), self sign-up + email-code (Task 5), escape hatch (Task 6), OAuth callback guard (Task 3), anti-enumeration reset (Task 4), unsupported-status messaging (Tasks 2/4/5), dark+light styling (Tasks 1/7), no backend change (none of the tasks touch `server.py`). All covered.
- **API-name risk:** `clerk.client.signIn` / `clerk.client.signUp` methods (`create`, `attemptFirstFactor`, `resetPassword`, `authenticateWithRedirect`, `prepareEmailAddressVerification`, `attemptEmailAddressVerification`) are the `@clerk/clerk-js@5` (Core 2) imperative names. If any 400s, confirm the method/strategy against the loaded `clerk-js@5` build in DevTools and that the strategy is enabled in the Clerk Dashboard.
- **Ordering:** `showClassicSignIn` is referenced in Task 2's signin view but defined in Task 6; the plan notes the console error between tasks is expected. If executing strictly task-by-task with a working build at every step is required, move Task 6 Step 1 before Task 2 Step 6.

# Native Login Design

**Date:** 2026-06-09
**Status:** Approved (design)
**Scope:** Frontend-only change to `index.html`. No `server.py` changes.

## Problem

Platoon's login screen currently renders Clerk's prebuilt widget via
`clerk.mountSignIn()`. We want a login that looks fully native to Platoon —
matching the existing dark/light `.login-card` styling — instead of the
drop-in Clerk component. This mirrors what was done in CoHangar
(`~/github/cohangar`), where the prebuilt `<SignIn>` widget was replaced with
a hand-built `NativeSignInForm` driven by Clerk's headless APIs.

## Key constraint: vanilla JS, not React

CoHangar is a React app using `@clerk/react` v6 Future-API hooks
(`useSignIn()` → `signIn.password()`). **Platoon is a single-file vanilla-JS
SPA** loading `@clerk/clerk-js@5` (Clerk "Core 2") via a `<script>` tag. We
cannot use React hooks. We drive the same Clerk endpoints with the SDK's
imperative resources, `clerk.client.signIn` and `clerk.client.signUp`, which
expose the same methods the hooks wrap (`create`, `prepareFirstFactor`,
`attemptFirstFactor`, `resetPassword`, `authenticateWithRedirect`,
`prepareVerification`, `attemptVerification`) plus `clerk.setActive()`.

The CoHangar files are a behavioral reference (state machine, anti-enumeration
reset, error extraction), not a code source.

## Scope of auth methods

Confirmed with the user; these **must be enabled in the Clerk Dashboard for the
instance the active publishable key points at**, or the calls fail (the
prebuilt widget auto-adapts; a custom flow hard-codes its strategies):

- **Email + Password** sign-in — `signIn.create({ identifier, password })`.
- **Google OAuth** — `signIn.authenticateWithRedirect({ strategy: 'oauth_google', … })`.
- **Forgot-password reset** — `reset_password_email_code` strategy.
- **Self sign-up** — `signUp.create({ emailAddress, password })` +
  `email_code` email verification. If Smart CAPTCHA / bot protection is on,
  sign-up requires a `<div id="clerk-captcha">` mount point.

## Out of scope

- MFA / second factor (`needs_second_factor`) — surface a clear "not yet
  supported" message if Clerk returns that status; do not build the flow.
- `needs_new_password` and other intermediate statuses — same treatment.
- Any backend / `server.py` change. `/api/auth/config`, `/api/auth/sync`, and
  JWKS session verification are strategy-agnostic and already work.
- A server-side feature flag to toggle native vs widget (rejected in favor of
  the lighter "escape hatch" below).

## Architecture

Replace `mountClerkSignIn()` as the default render path. The existing
`.login-card` markup is reused unchanged at the container level; only the
contents of `#clerkAuthMount` change — from a Clerk-managed widget to our own
form, redrawn by a render function from in-memory view state. This follows the
project's mandated "one file, `render*()` functions redraw from state"
convention.

On successful authentication:
`clerk.setActive({ session: <createdSessionId> })` → existing
`syncClerkUser()` (sets `currentUser`) → existing `routeAfterLogin()`. This is
an in-page SPA transition; no full reload (the current widget path relied on
`afterSignInUrl` causing a reload — we no longer need that).

### View state machine

A module-level `authView` string drives `renderAuthForm()`. Views:

| View            | Purpose                                                                 |
|-----------------|-------------------------------------------------------------------------|
| `signin`        | Google button · divider · email+password · "Forgot password?" · "Create account" toggle |
| `signup`        | Google button · divider · email+password · `#clerk-captcha` mount · "Already have an account?" toggle |
| `verify-email`  | After `signUp.create`: 6-digit code → `attemptVerification` → finalize  |
| `reset-request` | Email → send `reset_password_email_code`                                |
| `reset-verify`  | Code + new password → verify + `resetPassword` → finalize               |

`setAuthView(view)` updates state, clears transient error/code/password fields,
and calls `renderAuthForm()`.

### Functions (new, all inline in the `index.html` script)

- `renderAuthForm()` — builds the current view's markup into `#clerkAuthMount`,
  wires submit/click handlers. Reads `authView`.
- `setAuthView(view)` — transition + reset transient fields + re-render.
- `authSignIn(email, password)` — `signIn.create`; on `status === 'complete'`
  → `finalizeAuth`; on `needs_second_factor`/other → "not yet supported" error.
- `authSignUp(email, password)` — `signUp.create` then
  `prepareVerification({ strategy: 'email_code' })` → `setAuthView('verify-email')`.
- `authVerifyEmail(code)` — `signUp.attemptVerification({ strategy:'email_code', code })`;
  on complete → `finalizeAuth`.
- `authResetRequest(email)` — anti-enumeration: try
  `signIn.create({ strategy:'reset_password_email_code', identifier })`;
  swallow "not found" errors; **always** advance to `reset-verify`.
- `authResetVerify(code, newPassword)` —
  `attemptFirstFactor({ strategy:'reset_password_email_code', code })` then
  `resetPassword({ password })`; on complete → `finalizeAuth`.
- `authOAuth(strategy)` — `signIn.authenticateWithRedirect({ strategy,
  redirectUrl, redirectUrlComplete })`.
- `finalizeAuth(createdSessionId)` — `await clerk.setActive({ session })` then
  `currentUser = await syncClerkUser(); routeAfterLogin();`.
- `clerkErrorMessage(err, fallback)` — port of CoHangar's `clerkError`: prefer
  `err.errors?.[0]?.longMessage || .message`, then `err.longMessage || .message`,
  then fallback.

### OAuth callback handling

`authenticateWithRedirect` sends the browser to Google and back to
`redirectUrl` on our origin, with `redirectUrlComplete` as the final landing
URL. ClerkJS processes the OAuth handshake during `clerk.load()` (already
called in `ensureClerk()` from `initApp()`); after it resolves, `clerk.user`
is set and `initApp`'s existing success path runs sync + route.

**Verification required in the browser.** If `clerk.load()` does not
auto-complete the callback, add a guarded `await clerk.handleRedirectCallback()`
in `initApp()` when Clerk redirect params are present on the URL. Use
`redirectUrl = window.location.origin + '/'` and
`redirectUrlComplete = window.location.origin + '/'`.

### Escape hatch (anti-lockout)

`mountClerkSignIn()` is kept in the file. The `signin` view includes a small
"Trouble signing in? Use classic sign-in" link that calls `mountClerkSignIn()`
into `#clerkAuthMount` (replacing the native form) and shows a "Back to Platoon
sign-in" link that returns to `renderAuthForm()`. This preserves a working
login path if a bug is found in the native flow, without server config.

## Error handling

- Every Clerk call in try/catch. Display `clerkErrorMessage(err, <contextual
  fallback>)` in the inline error slot (`#loginError` or a per-view error
  element).
- Submit buttons disable and show a busy label ("Signing in…", "Sending…",
  "Verifying…", "Creating account…") while a request is in flight; re-enable on
  resolve/reject.
- Password reset never discloses account existence (anti-enumeration).
- Unsupported statuses (`needs_second_factor`, etc.) produce a clear message
  rather than a silent failure.

## Styling

Reuse existing `.login-card`, `.login-title`, `.login-subtitle`,
`.login-error`, `.btn-login`, and `.login-card input/label` classes, which
already theme correctly under `body.dark-mode` / `body.light-mode`. New small
elements needed: an "or" divider, a secondary/ghost button for the Google
button and mode-toggle links, and a numeric code input. Keep additions minimal
and consistent with the existing palette (accent `#0984e3` / `#00d4ff`).

## Testing / verification

No automated test suite exists (per `CLAUDE.md`); verification is manual in the
browser, both dark and light themes:

1. Email + password sign-in (valid) → lands in app.
2. Wrong password → inline error, no crash.
3. Google OAuth → round-trips and lands in app.
4. Forgot password → request code, reset, sign in. Unknown email still advances
   to the verify screen (no enumeration).
5. Self sign-up → create, receive email code, verify, lands in app.
6. Escape hatch → "classic sign-in" mounts the widget and can sign in; "back"
   returns to the native form.
7. Reload while signed in → stays signed in (no regression in `initApp`).

## Risks

- **Strategy mismatch with the Clerk Dashboard** is the primary failure mode.
  If a method 400s with "strategy not allowed", the corresponding Dashboard
  option is off for that instance. Documented as a prerequisite above.
- **OAuth callback** behavior under `@clerk/clerk-js@5` must be confirmed in the
  browser; fallback (`handleRedirectCallback`) is specified.

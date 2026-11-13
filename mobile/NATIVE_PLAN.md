# Native iOS app (SwiftUI): plan

The TestFlight app today is the Capacitor shell in `mobile/ios/`: the website
in a web view. This plan replaces it with a real SwiftUI app that talks to the
same server API. It is built and tested on a Mac; nothing here can be compiled
in a Linux session.

## Decisions

- **SwiftUI, iOS 17+.** `@Observable`, `NavigationStack`, `async`/`await`.
  No third-party UI libraries.
- **Same server, same API.** The app calls `https://app.platoonmanager.com/api/...`
  with `Authorization: Bearer <Clerk session token>`. `server.py`'s
  `_get_session_token()` already reads that header before the cookie, so no new
  server auth path is needed. Every rule about who may see or change what stays
  on the server.
- **Clerk's iOS SDK** (`clerk-ios`, Swift Package Manager) for sign-in, email
  and Google both native (Google through `ASWebAuthenticationSession`, which
  Google allows). That retires the Safari-sheet ticket handoff for this app.
- **Same bundle ID** `com.platoonmanager.app`, RESYRV team `687CJND9M7`, so a
  native build simply replaces the Capacitor build on TestFlight. Build
  numbers keep counting up (the Capacitor upload was build 1).
- **Location:** a new Xcode project at `mobile/native/` (`PlatoonManager.xcodeproj`).
  The Capacitor shell in `mobile/ios/` stays until the native app reaches the
  Phase 2 line below, then is deleted in its own PR.
- **Not native, ever (opens the website in `SFSafariViewController`):**
  billing (Apple wants in-app purchase for anything sold inside an app, so the
  app shows no prices and no Stripe links at all), backup/restore, the
  platform admin dashboard, logo upload, alpha-roster import.

## Server and Clerk setup (before Phase 0 can pass)

1. Clerk dashboard (production instance): enable the **Native API**, and add
   the iOS app (team ID `687CJND9M7`, bundle ID `com.platoonmanager.app`).
   Add the app's OAuth redirect if the SDK asks for one.
2. Check the native session token's `azp` claim. `_verify_clerk_session_token()`
   rejects a token whose `azp` is set and not in `CLERK_AUTHORIZED_PARTIES`
   (or the request origin). Native tokens usually carry no `azp`, which passes;
   if they carry one, add it to `CLERK_AUTHORIZED_PARTIES` in prod `.env`.
   Phase 0 proves this one way or the other.
3. After every sign-in the app must call `POST /api/auth/sync` with
   `{username, email, full_name, invite_token?}` (what `syncClerkUser()` in
   `index.html` sends). That is what creates or attaches the local user row.

## API the app uses

Responses are JSON. Errors are `{"error": "...", "field"?: "..."}`.

| Need | Route |
| --- | --- |
| Who am I, timezone, flags | `GET /api/me` (`timezone`, `unit_id`, `role`, `needs_unit`, `billing`, `quick_add`, `platform_admin`) |
| After sign-in | `POST /api/auth/sync` |
| Unit tree + today's counts | `GET /api/units` (each unit: `present`, `unaccounted`) |
| Roster | `GET /api/personnel?unit=<id>` (rows + their live `scheduled_events`) |
| Mark present / undo | `PUT /api/personnel/<id>` (`status: 'present'`, `present_date`) |
| Book / edit / cancel an absence | `POST /api/personnel/<id>/schedule`, `PUT`/`DELETE /api/schedules/<id>`, `DELETE /api/schedules/series/<id>` |
| Absence history | `GET /api/personnel/<id>/absences` |
| Soldier profile | `GET`/`PUT /api/personnel/<id>/profile` |
| TDY lists, settings | `GET /api/settings` |
| Reports | `GET`/`POST /api/reports`, `GET`/`DELETE /api/reports/<id>` |
| Directory, availability | `GET /api/directory`, `GET /api/availability?platoon=&date=[&to=]` |
| Duty roster | `GET`/`POST /api/duty`, `DELETE /api/duty/<id>`, `GET /api/duty/conflicts` |
| Notifications prefs | `GET`/`PUT /api/me/notifications` |
| Quick add | `POST /api/absences/quick` (only when `/api/me` says `quick_add`) |

Status codes the client must handle the way the web app does:
**401** sign out; **402** the account is locked (show a plain "subscription
isn't active" screen with Sign out, never prices); **403** with
`code: account_inactive` the inactive screen; **503** Clerk or the server
is briefly unreachable, retry, never sign out; **429** honour `Retry-After`.

## Rules that live in the web client and must be ported

These are pure functions in `index.html`, each with a test that runs it under
node. Port each to Swift and port its test cases to XCTest so the two cannot
drift; the JS tests are the specification.

| Rule | In `index.html` | Spec (test) |
| --- | --- | --- |
| Who is unaccounted today | `isUnaccounted()` | `tests/test_formation_order.py` |
| Rank order | `RANK_ORDER`, `rankSort()` | `tests/test_formation_order.py`, `tests/test_report_js.py` |
| Formation queue | `formationQueue()`, `formationRemaining()` | `tests/test_formation_order.py` |
| Report text (Detailed / Strength, BY UNIT) | `buildReport()` and helpers | `tests/test_report_js.py` |
| Same-day late/excused | `isSameDayState()` | none (one line; port it with the roster) |
| Reason box wording per status | `REASON_FIELDS` | none (copy as-is) |
| Unit tree helpers, rolled-up counts | `unitById()`, `unitTodayCounts()` and friends | `tests/test_unit_tree_js.py` |
| Field validation | `validateField()`, `normalizeField()` | `tests/test_validation.py` (also mirrors `validation.py`) |

The duty day is the organization's, not the phone's: use `/api/me`'s
`timezone` for "today" everywhere (`getTodayStr()` in the web app).

## Phases

**Phase 0: spike (prove the plumbing, nothing else).** New project, Clerk SDK,
email + Google sign-in, `POST /api/auth/sync`, `GET /api/me`, then list the
roster for the user's unit as plain text. Ship to TestFlight as the next build
number. If `azp` or the redirect fails, stop and fix the setup above.

**Phase 1: the morning.** Native look throughout: large titles, system
font, SF Symbols, haptics, pull-to-refresh, swipe actions.
- Home: the unit tree with today's present/assigned and unaccounted badges.
- Accountability roster: sections Needs Action, Present, Away, FTR, Scheduled.
  Swipe right (or tap the tick) to mark present, with undo.
- Set status sheet: status, native date range, reason (from `REASON_FIELDS`).
- Soldier detail: status, absence history, edit/cancel an absence.
- Locked / inactive / needs-unit screens.

**Phase 2: replaces the Capacitor app.** Formation mode (full screen, one card
at a time, "Everyone else present"), reports (generate, copy, share sheet, save
to history), report history. When Phase 2 is on TestFlight, delete
`mobile/ios/` and the Capacitor files in its own PR.

**Phase 3: the rest of daily use.** Directory (tap to call/text), availability,
duty roster, profile editing, notification preferences, quick add.

Everything else opens the website in `SFSafariViewController`.

## Testing

- XCTest for every ported rule, using the JS tests' cases.
- A UI test that signs in with a test account and marks one soldier present.
- Each phase ends with a TestFlight build installed on a real phone.

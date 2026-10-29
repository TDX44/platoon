# Platoon Manager usability and functionality audit

Date: October 28, 2026. Priority: mobile use and finding settings.

## Scope and evidence

Reviewed the Flask backend, SPA, existing design documents, and frontend tests. Rendered the actual SPA in Chromium with the existing mobile-test fixtures and stubbed API responses at 390 × 844 and 1440 × 1000. No production data or services were used. Screenshots in the adjacent `2026-10-28-screenshots` directory contain synthetic personnel.

All 16 standalone JavaScript-oriented test scripts, including formation ordering, passed. The full database and mobile-layout suites were not run: this environment lacks Flask and configured test database credentials. The browser checks here used a temporary static server and the application's real rendering functions. These findings do not establish production authentication, notification delivery, billing, or backend integration health.

## What already works

- Accountability puts people needing attention first and has direct present/status actions.
- Formation mode provides a focused workflow; bulk actions, recurring absences, availability, duty rotation, and report history already exist.
- Unit trees, organization time zones, field validation, import preview, and role-aware access explanations are implemented.
- Existing tests cover meaningful business rules. Improve these workflows before adding more navigation destinations.

## Prioritized findings

| Priority | Finding and evidence | Recommended change |
|---|---|---|
| P1 | **Settings has no useful front door.** Both Settings and account-menu “All settings” default to `general`, whose title and content are Logo. See `openSettings`, `SETTINGS_SECTIONS`, and the account menu in `index.html`. | Make Settings open a grouped index with plain descriptions and a visible current-unit context. Put logo upload beneath Unit settings. |
| P1 | **Mobile removes the settings hierarchy.** `settingsTabsHtml()` discards group headings and renders ten tabs in one horizontal strip. At 390px, the selected Logo tab is centered while Profile lies offscreen left and Accounts & invites lies offscreen right. The scrollbar is hidden. | Use a vertical settings index grouped into Your account, Current unit, and Organization. Section pages get “Back to settings.” Keep the same labels and hierarchy on desktop. |
| P1 | **The directory is too tall for routine phone use.** Eight fixture personnel produce a 2,550px document. Every card exposes Unit, Status, Return, Next Scheduled, DoD ID, DOB, MOS, and Phone, including empty fields. See `renderDirectory()`. | Default to compact name/rank, unit, status, and call/message actions. Open the profile for administrative details. Make additional detail an explicit choice. Avoid sending sensitive fields in the compact-directory payload if unnecessary. |
| P1 | **The roster spends much of the first phone screen on surrounding controls.** In the trial-state fixture, the banner, global search, title/timestamp, action row, metrics, and filters put the first person near 490px from the top. Only three personnel are visible at 390 × 844. Trial-banner height is state-dependent. | Compress the unit header; make formation the main action; use a short accountability summary and a Search/Filter row. Move report preview to Reports. Collapse secondary trial messaging after acknowledgement while retaining clear access to billing. Preserve readable text and touch targets. |
| P1 | **A failed profile save says “Saved.”** `api()` returns `null` for failures; `saveProfile()` only rejects `res && res.error`, then changes the button to “Saved ✓”. Reproduced in Chromium with `api = async () => null`. | Require a successful response before success feedback. Retain the draft on failure, show a clear retry action, and disable Save while pending. Add a regression test for null responses. |
| P1 | **Accountability can become stale between leaders.** Roster refresh is manual; there is no data refresh on visibility/focus or connection restoration. Existing `loadReq` guards protect navigation races, but writes have no expected-version check. `formationRestPresent()` checks its local snapshot before issuing writes. | Refresh on return to the app and reconnect, show age/disconnection persistently, and add server-side version checks to writes. A bulk-present operation should skip people newly marked away by another leader and report the skips. Avoid overwriting an open form during refresh. |
| P2 | **Settings, account actions, and operational tools are spread across menus.** Desktop sidebar, header More, mobile More, account menu, settings tabs, and report controls overlap. Availability also appears as “Who's Available”; report history becomes “History.” | Choose one canonical home for each task and use consistent labels. Keep shortcuts only when they lead to that same destination. Rename History to Report history and distinguish leader accounts from personnel. |
| P2 | **Dialogs receive focus but do not contain it.** The modal observer handles initial focus, Escape, and focus restoration; no Tab trap or background inertness is implemented. Browser reproduction: Tab from the last Add Person control leaves `#addModal`. The More sheet is outside the modal mechanism. | Contain keyboard focus, make the background inert while open, name dialogs, and restore focus on close. Apply equivalent behavior to the More sheet and formation overlay. |
| P2 | **Profile drafts have no navigation protection.** `closeSoldierPage()` clears the selected person; reopening renders the form and reloads saved values. There is no dirty-state or before-unload protection. | Track changes and offer Save / Discard / Stay when leaving a dirty profile. Put Save in a phone-friendly sticky action area and show Unsaved/Saving/Saved states. |
| P2 | **“Global” people search is scoped to the loaded roster.** `onGlobalSearch()` passes no people on Home and only `personnel` within a unit, despite the placeholder “Search people, units, settings…”. | Either label the scope explicitly or implement server-backed search over the user's authorized units. Keep scope visible in results and avoid showing previous-unit results during navigation. |
| P2 | **Phone interaction coverage is narrower than the product needs.** Geometry tests allow 28px targets generally and enforce 44px only on mark-present and Set Status. Existing tests do not expose the failed-save success message. | Add targeted checks for settings discovery, directory density, keyboard containment, failed saves, dirty drafts, concurrent leaders, and offline/reconnect states. Check portrait, landscape, enlarged text, and real phones. |

## Proposed navigation

Daily work should have stable destinations: **Today**, **Personnel**, **Planning**, **Reports**, **More**. This is a proposed structure to validate with users, not a change already made.

- **Today:** accountability, needs-action filters, formation, quick status changes.
- **Personnel:** compact directory, contact actions, full profiles, add/import personnel.
- **Planning:** availability, scheduled absences, duty roster, rotation proposals.
- **Reports:** current report, formats, copy/export, saved report history.
- **More:** Units, Activity log, Settings. Keep destructive reset/restore actions inside an appropriately labeled section.

Settings opens an index rather than a logo form:

| Group | Destinations |
|---|---|
| Your account | Profile & sign-in; Appearance; Notifications; Billing |
| Current unit · [unit name] | Unit name & logo; Sub-units; Schools & locations |
| Organization · [organization name] | Leader accounts & invites; Duty-day time zone; Backup & restore |

Show permission requirements beside relevant controls. A shortcut from Units can still open the same Sub-units page. Activity log should be reachable from More; changing configuration and inspecting operational history are different tasks. Personal settings should retain an explicit account scope even when opened from a unit URL.

## Suggested delivery order

1. **Trust and navigation:** correct failed-save feedback; build the grouped settings index and section back links; standardize labels and context; fix dialog focus containment.
2. **Mobile daily work:** compact directory cards, simplify the roster header, improve primary action placement, move report preview out of the phone roster, and protect profile drafts.
3. **Coordination:** visible data age and disconnection, refresh-on-return/reconnect, version-aware writes, and guarded bulk actions.
4. **Functionality:** bring existing availability and rotation tools into Planning; add an absence calendar/timeline if planning users need it; add saved filters and actionable returns/conflicts summaries. Verify email setup before promising notifications: the existing known-issues document records it as blocked, but production status was not checked.
5. **Maintainability:** incrementally extract CSS, API/state handling, shared navigation, and page modules from the 13,121-line `index.html`. The 5,209-line backend can be split by domain as needed. A framework migration is not required for the first improvements.

## Acceptance checks for the first releases

- A user can find Notifications, invite a leader, change a unit logo, and reach duty planning from explicit labels without horizontally searching settings tabs.
- At 390 × 844, routine directory cards omit empty administrative fields and expose a direct contact action when a phone number exists.
- Before/after screenshots use identical fixtures and billing state; compare visible personnel count, scrolling distance, and tap counts rather than shrinking text.
- Failed requests never produce success feedback; drafts survive a failure and intentional navigation gets a Save/Discard/Stay choice.
- Keyboard focus stays inside an open dialog and returns to its opener.
- A stale leader cannot silently overwrite a newer absence through a bulk-present action.
- Existing frontend rules stay green; run the database-backed suites and mobile geometry checks in a configured environment before shipping.

## Research still needed

The proposed hierarchy is based on implementation review and the user's stated confusion. Validate it with representative leaders using common tasks: morning accountability, recording an unexpected absence, finding a phone number, inviting another leader, and planning duty. Observe completion, wrong turns, and recovery on actual phones. Production latency and larger-company performance were not measured.

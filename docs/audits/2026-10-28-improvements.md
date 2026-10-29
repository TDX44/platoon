# Mobile usability and settings improvements

Implemented locally on October 28, 2026 following the usability audit.

## Changes

- Settings opens a grouped index with account, current-unit, and organization context. Mobile uses vertical navigation and an All settings back link instead of ten horizontal tabs. Existing section URLs remain valid.
- Notifications is directly discoverable. Leader accounts are distinguished from personnel; schools/locations and report history use clearer labels.
- Mobile directory cards show names, unit/status, and available call/text actions. Administrative columns remain available on desktop and details remain in profiles. Empty phone rows are hidden.
- The mobile roster has a shorter header and summary, with report preview removed from its flow. Formation and Reports remain accessible.
- Profile saves only show success after a successful response. Pending saves are guarded, fields wait for the initial profile read, and the phone Save bar stays above bottom navigation.
- Drafts are kept in memory for the signed-in session, restored when reopening a profile, and cleared on sign-out. App navigation offers Save / Discard / Keep editing; browser unload warns about retained drafts. Drafts are not persisted to local storage.
- Dialogs contain keyboard focus and make the background inert. The mobile More sheet closes with Escape and returns focus. Date pickers opened inside dialogs remain within their focus boundary. Reduced-motion preferences are respected.
- The roster shows offline, failed-refresh, and older-data states. Returning to the app or reconnecting refreshes the roster when no form, dialog, or formation session is active.
- Bulk present actions check current server truth under per-person database locks. They preserve newly booked absences, mirror the response, and report skipped people. Explicit individual present actions retain their intentional override behavior.

## Browser measurements

At 390 × 844 with the same eight-person synthetic roster and trial state:

| Measure | Before | After |
|---|---:|---:|
| Directory document height, without phone actions | 2,550px | 1,084px |
| First roster row, approximate vertical position | 490px | 438px |

Before/after screenshots are in `2026-10-28-screenshots/`. The trial banner remains state-dependent. These measurements do not represent every account or roster size.

## Verification

Validation uses Chromium and disposable PostgreSQL with the real application role and row-level security. The test runs cover mobile geometry, frontend rules, backend routes, and concurrent absence/accountability writes. New browser tests exercise failed saves, duplicate saves, edits during a save, draft navigation and recovery, loading failures, sign-out cleanup, touch targets, and modal keyboard behavior.

The icon test also passes against a clean snapshot of the current source. Running it directly in this checkout encounters retired-art references in pre-existing, ignored `.superpowers` review archives. Those local archives were left untouched.

## Remaining scope

This change does not deploy the app or replace its navigation with the longer-term Today / Personnel / Planning proposal. General optimistic versioning for every profile or record edit remains separate work; the new server guard specifically protects bulk present actions. Email delivery configuration, a planning calendar, and a large-scale source-module extraction remain follow-up items from the audit.

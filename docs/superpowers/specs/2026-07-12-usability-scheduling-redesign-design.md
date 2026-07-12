# Platoon Accountability — Usability, Scheduling Sync & Redesign

**Date:** 2026-07-12
**Status:** Approved

## Problem

1. **Scheduled absences don't sync with daily accountability.** Absences are stored in
   two places — the `scheduled_events` table and four legacy `sched_*` columns on
   `personnel` — and both are written in parallel, so they drift. The midnight
   activation thread never runs in production (it only starts under `python server.py`,
   not gunicorn). The only production activation trigger is a fire-and-forget frontend
   call that runs *after* data is fetched, so activations are always one refresh behind.
   Nothing returns a soldier to duty when their absence end date passes (client-side
   auto-return exists in `render()` but is unreliable and invisible to other clients).
2. **Too many clicks and confusing navigation**, especially on phones (primary usage).
   The TDY/Leave modal packs six status toggles, two text fields, and a custom inline
   calendar into one dialog that doesn't fit small screens. History, Directory, Import,
   and Audit Log are unreachable from the mobile bottom nav.
3. **The soldier page (`/soldier/:id`) lacks absence history** — activation *deletes*
   the scheduled event row, so past absences vanish — and lacks readiness details.
4. **The UI looks AI-generated**: two overlapping half-migrated token systems
   (`--theme_*` and `--dash-*`), textbook Tailwind blue-600/gray palette, generic
   system font stack with a non-loading "Haas" first entry, default radii/shadows.

## Design

### 1. Scheduling: single source of truth with a lifecycle

`scheduled_events` becomes the only storage for absences. Rows are never deleted on
activation; they carry a `state` column:

```
scheduled ──(from_date arrives)──> active ──(to_date passes)──> completed
```

- **Migration (in `init_db()`):** add `state TEXT DEFAULT 'scheduled'`; fold any
  remaining legacy `sched_*` column data into `scheduled_events` (existing backfill
  already does most of this); stop reading/writing `sched_*` everywhere. Columns stay
  physically (SQLite drop is awkward) but are dead.
- **Server-side reconciliation on every read:** `GET /api/personnel` first runs a
  `_reconcile_absences(conn)` step:
  - `scheduled → active`: for events whose `from_date <= today`, set the soldier's
    `status/from_date/to_date/notes` from the event and mark the event `active`.
  - `active → completed`: for events whose `to_date < today`, mark completed and
    return the soldier to duty (`status='present'`, cleared dates/notes,
    `present_date` untouched — they still must be marked present that day).
  - `personnel.status` remains a display cache the server keeps correct; the frontend
    stops calling `/api/activate-scheduled` on load (endpoint kept for the midnight
    thread & manual use, now delegating to the same reconcile function).
- **Auto-return is automatic** (user choice): when `to_date` passes, the soldier
  flips back to unmarked/present-eligible with no manual confirmation.
- **API shape:** `GET /api/personnel` keeps returning `scheduled_events` per person,
  now including `state`; new absences POST to the existing
  `/personnel/:id/schedule` route (renamed semantics: creates a `scheduled` or
  immediately-`active` event depending on `from_date`).
- **Backup:** export becomes `version: 2` (includes `state`); restore accepts
  version 1 (events get `state` inferred: `from_date <= today` → active) and 2.
- **Audit:** activation/completion logged via `log_action`.

### 2. Usability: fewer clicks, coherent navigation, real mobile support

- **Marking present:** unchanged 1-tap `Mark Present`.
- **Status modal rebuilt:** native `<input type="date">` replaces the custom inline
  calendar; vertical single-column flow (status → dates → details → save) that fits a
  375px viewport; the six status toggles become a wrapping chip row.
- **Quick status from the row:** the row "Update ▾" menu gains direct common actions
  so simple cases skip the modal.
- **Navigation:** mobile bottom nav gains a "More" sheet exposing History, Directory,
  Import, Duty Roster, and Audit Log (admin). Desktop header "More" menu and sidebar
  stay, but every destination is reachable on mobile.
- **Mobile fixes:** bulk-bar hardcoded inline colors → tokens; import preview table
  gets an overflow wrapper; dead `max-width:380px` rules for the removed table layout
  deleted; modal small-screen rules added.

### 3. Soldier page additions

- **Absences tab** shows three groups from the lifecycle table: **Current** (active),
  **Upcoming** (scheduled), **Past** (completed, newest first). Falls out of design #1
  with no extra storage.
- **Readiness card** added to Overview alongside the existing Administrative card:
  flags (free text), medical readiness date, dental readiness date, weapons
  qualification date + weapon. Stored in `personnel_profile` (new columns via the
  existing ALTER-based migration pattern). Field list stays lean.

### 4. Visual redesign: command-post direction

- **One token system.** `--theme_*` and `--dash-*` merge into a single `:root` token
  set with a dark-mode override; every hardcoded hex in components moves to tokens.
- **Aesthetic:** purpose-built military ops tool. Dense rows, high contrast,
  slate/graphite surfaces with olive-drab accents; semantic status colors
  (green = present/accounted, amber = scheduled/due back, red = FTR/unaccounted,
  steel-blue = TDY/away-accounted). Condensed display face for headings
  (self-hosted or system-stack condensed), tabular numerals for all counts, small
  radii, flat or hairline elevation instead of soft blurred shadows.
- **Scope:** skin only — layout bones (sidebar dashboard, sectioned roster, soldier
  page tabs) stay. Both light and dark themes intentional.

## Error handling

- Reconciliation runs in the request transaction; failure rolls back and the read
  still returns (reconcile wrapped so a bad row can't 500 the roster).
- Restore validates `version ∈ {1, 2}` and rejects others with a clear message.

## Testing

Repo has no test suite (per CLAUDE.md — none invented). Verification per phase:
- **Phase 1:** scripted `sqlite3`/`curl` smoke checks against the dev server —
  seed a scheduled event dated today → GET roster → soldier active; set `to_date`
  yesterday → GET → soldier returned, event completed, history present. Backup
  round-trip v1→v2.
- **Phases 2–4:** browser-based checks at 375px and 1440px (Browser pane), both
  themes, covering marking flow, modal, bottom-nav More sheet, soldier page tabs.

## Delivery

Four phases, each independently deployable:
1. Scheduling lifecycle fix (backend + minimal frontend)
2. Usability + mobile pass
3. Soldier page (history grouping + readiness fields)
4. Visual redesign token overhaul

# Usability, Scheduling Sync & Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make scheduled TDY/leave/pass absences reliably activate/expire (single source of truth with lifecycle states), streamline the daily marking workflow especially on mobile, add absence history + readiness fields to the soldier page, and restyle the app with a command-post design system.

**Architecture:** Flask backend (`server.py`) + single-file vanilla-JS SPA (`index.html`). Absences move to a lifecycle model in `scheduled_events` (`scheduled → active → completed`); the server reconciles states on every roster read. Legacy `sched_*` columns on `personnel` become dead. Frontend loses its client-side auto-return and fire-and-forget activation.

**Tech Stack:** Python/Flask, SQLite, vanilla JS/CSS. No test framework exists (per CLAUDE.md — do not invent one); each task ends with scripted smoke checks (sqlite3/curl against the dev server) or browser checks.

## Global Constraints

- Two-file app: backend changes in `server.py`, frontend in `index.html`. No new modules, no build step, no CDN libs beyond Clerk.
- Schema changes via `init_db()` idempotent migrations (PRAGMA table_info + ALTER TABLE), never a migration framework.
- New/changed API routes stay under `/api/`, return JSON, use existing auth decorators + `log_action()`.
- Backup export moves to `version: 2`; restore must accept versions 1 and 2.
- Statuses: `present, tdy, leave, pass, other, ftr, loan`. `loan` is never a scheduled event.
- Commit after every task with conventional-commit messages.
- Dev server: `python server.py` (port 5000). When Clerk env vars are absent locally, `CLERK_ENABLED` is false and auth decorators pass through — curl works without tokens.

---

## Phase 1 — Scheduling lifecycle (the sync fix)

### Task 1: `state` column + `_reconcile_absences()`

**Files:**
- Modify: `server.py:192-205` (migrations), `server.py:1153-1189` (`_activate_scheduled`)

**Interfaces:**
- Produces: `_reconcile_absences(conn, today_str) -> dict` with keys `activated`, `completed` (ints). Replaces `_activate_scheduled` everywhere.
- Produces: `scheduled_events.state` column, values `'scheduled' | 'active' | 'completed'`.

- [ ] **Step 1: Add migration** in `init_db()` after the existing `location` migration (server.py:193-194):

```python
    if 'state' not in scols:
        cur.execute("ALTER TABLE scheduled_events ADD COLUMN state TEXT DEFAULT 'scheduled'")
        # Rows that were already "live" per the legacy model: person's current
        # status matches an event whose window covers today.
        cur.execute(
            "UPDATE scheduled_events SET state = 'completed' "
            "WHERE to_date != '' AND to_date < date('now', 'localtime')"
        )
```

Also add `state TEXT DEFAULT 'scheduled'` to the `CREATE TABLE IF NOT EXISTS scheduled_events` statement (server.py:131-142) so fresh databases match.

- [ ] **Step 2: Replace `_activate_scheduled` with `_reconcile_absences`** (same location, server.py:1153):

```python
def _reconcile_absences(conn, today_str):
    """Advance absence lifecycle: scheduled->active when from_date arrives,
    active->completed when to_date passes (returning the soldier to duty)."""
    completed_rows = conn.execute(
        "SELECT * FROM scheduled_events WHERE state = 'active' "
        "AND to_date != '' AND to_date < ?", (today_str,)
    ).fetchall()
    for r in completed_rows:
        conn.execute(
            "UPDATE scheduled_events SET state = 'completed' WHERE id = ?", (r['id'],))
        conn.execute(
            "UPDATE personnel SET status='present', from_date='', to_date='', notes='' "
            "WHERE id = ? AND status = ?", (r['person_id'], r['status'])
        )

    activated_rows = conn.execute(
        "SELECT * FROM scheduled_events WHERE state = 'scheduled' "
        "AND from_date != '' AND from_date <= ? ORDER BY from_date, id", (today_str,)
    ).fetchall()
    for r in activated_rows:
        if r['to_date'] and r['to_date'] < today_str:
            # Window already entirely in the past: never became visible; complete it.
            conn.execute("UPDATE scheduled_events SET state='completed' WHERE id=?", (r['id'],))
            continue
        conn.execute(
            "UPDATE personnel SET status=?, from_date=?, to_date=?, notes=? WHERE id=?",
            (r['status'], r['from_date'], r['to_date'], r['notes'], r['person_id'])
        )
        conn.execute("UPDATE scheduled_events SET state='active' WHERE id=?", (r['id'],))

    return {'activated': len(activated_rows), 'completed': len(completed_rows)}
```

Note the legacy `sched_*` promotion block (server.py:1176-1188) is deleted — the init_db backfill (server.py:196-204) already folded `sched_*` data into `scheduled_events`.

- [ ] **Step 3: Update the three callers.**
  - `activate_scheduled` route (server.py:1116-1124): `result = _reconcile_absences(conn, today_str)` → `return jsonify(result)`.
  - `_midnight_reset_worker` (server.py:1202): `activated = _reconcile_absences(conn, today_str)` and adjust the print.
  - `get_personnel` — wired in Task 2.

- [ ] **Step 4: Smoke-check with a scratch DB** (PowerShell, from the worktree; uses the scratch `DATA_DIR` so the real DB is untouched):

```powershell
$env:DATA_DIR = "$env:TEMP\platoon-smoke"; New-Item -ItemType Directory -Force $env:DATA_DIR | Out-Null
python -c "import server; print('init ok')"
python - <<'PY'
import server, datetime
conn = server.get_db()
today = datetime.date.today().isoformat()
yday  = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
tmrw  = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()
pid = conn.execute("INSERT INTO personnel (rank,last,first,platoon) VALUES ('SGT','Test','A','2nd')").lastrowid
conn.execute("INSERT INTO scheduled_events (person_id,platoon,status,from_date,to_date,state) VALUES (?,?,?,?,?,'scheduled')", (pid,'2nd','tdy',today,tmrw))
print(server._reconcile_absences(conn, today))           # {'activated': 1, 'completed': 0}
print(conn.execute("SELECT status FROM personnel WHERE id=?", (pid,)).fetchone()['status'])  # tdy
conn.execute("UPDATE scheduled_events SET to_date=? WHERE person_id=?", (yday, pid))
print(server._reconcile_absences(conn, today))           # {'activated': 0, 'completed': 1}
print(conn.execute("SELECT status FROM personnel WHERE id=?", (pid,)).fetchone()['status'])  # present
print(conn.execute("SELECT state FROM scheduled_events WHERE person_id=?", (pid,)).fetchone()['state'])  # completed
PY
```

Expected outputs as commented. (Run via `python - <<'PY'` in Git Bash, or save to a temp .py file on Windows.)

- [ ] **Step 5: Commit** — `fix: absence lifecycle states + server-side reconciliation`

### Task 2: Rewire routes off the legacy `sched_*` columns

**Files:**
- Modify: `server.py:620-649` (`get_personnel`), `673-693` (`update_person`), `754-790` (`add_scheduled_event`), `793-823` (`delete_scheduled_event`)

**Interfaces:**
- Consumes: `_reconcile_absences(conn, today_str)`.
- Produces: `GET /api/personnel` runs reconciliation first and returns each person with `scheduled_events` = events with `state != 'completed'` (each event dict includes `state`). It no longer populates `sched_*` from events. `PUT /api/personnel/<id>` rejects (ignores) `sched_*` keys. `POST /api/personnel/<id>/schedule` inserts with `state='scheduled'` (from_date defaulting to today when blank) and reconciles before returning. `DELETE /api/schedules/<id>` returns the person to duty if the deleted event was `active`.

- [ ] **Step 1: `get_personnel`** — after the access check, reconcile + commit, and filter events:

```python
    conn = get_db()
    _reconcile_absences(conn, date.today().isoformat())
    conn.commit()
    rows = conn.execute(
        'SELECT * FROM personnel WHERE platoon = ? ORDER BY rank, last, first', (platoon,)
    ).fetchall()
    scheduled_rows = conn.execute(
        "SELECT * FROM scheduled_events WHERE platoon = ? AND state != 'completed' "
        "ORDER BY from_date, to_date, id", (platoon,)
    ).fetchall()
    conn.close()
    scheduled_by_person = {}
    for r in scheduled_rows:
        scheduled_by_person.setdefault(r['person_id'], []).append(dict(r))
    result = []
    for r in rows:
        item = dict(r)
        item['scheduled_events'] = scheduled_by_person.get(r['id'], [])
        result.append(item)
    return jsonify(result)
```

(The `sched_status/sched_from/sched_to/sched_notes` override block at 643-647 is deleted; the raw dead columns still ride along in `dict(r)` harmlessly.)

- [ ] **Step 2: `update_person`** — remove `'sched_status', 'sched_from', 'sched_to', 'sched_notes'` from the allowed-column tuple (server.py:678-679).

- [ ] **Step 3: `add_scheduled_event`** — default blank `from_date` to today, insert with state, drop the mirror UPDATE (773-785), reconcile so today-dated events activate immediately:

```python
    from_date = (data.get('from_date') or '').strip() or date.today().isoformat()
    cur = conn.execute(
        'INSERT INTO scheduled_events (person_id, platoon, status, from_date, to_date, notes, location, state) '
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'scheduled')",
        (person_id, person['platoon'], status, from_date, data.get('to_date', ''),
         data.get('notes', ''), data.get('location', ''))
    )
    new_id = cur.lastrowid
    _reconcile_absences(conn, date.today().isoformat())
    conn.commit()
```

- [ ] **Step 4: `delete_scheduled_event`** — replace the mirror-maintenance block (807-819) with active-event handling:

```python
    person_id = row['person_id']
    conn.execute('DELETE FROM scheduled_events WHERE id = ?', (event_id,))
    if row['state'] == 'active':
        conn.execute(
            "UPDATE personnel SET status='present', from_date='', to_date='', notes='' "
            "WHERE id = ? AND status = ?", (person_id, row['status'])
        )
    conn.commit()
```

- [ ] **Step 5: Smoke-check over HTTP** — start `python server.py` (scratch `DATA_DIR` still set), then:

```bash
# person + immediate absence (from_date today)
curl -s -X POST localhost:5000/api/personnel -H 'Content-Type: application/json' -d '{"rank":"SPC","last":"Curl","first":"B","platoon":"2nd"}'
curl -s -X POST localhost:5000/api/personnel/<ID>/schedule -H 'Content-Type: application/json' -d '{"status":"leave","from_date":"<TODAY>","to_date":"<TOMORROW>"}'
curl -s "localhost:5000/api/personnel?platoon=2nd"   # person status == "leave", event state == "active"
# cancel it
curl -s -X DELETE localhost:5000/api/schedules/<EVENT_ID>
curl -s "localhost:5000/api/personnel?platoon=2nd"   # person back to "present"
```

- [ ] **Step 6: Commit** — `fix: routes use lifecycle events; retire legacy sched_* columns`

### Task 3: Backup v2

**Files:**
- Modify: `server.py:958-1007` (`export_backup`), `1010-1070` (`import_backup`)

**Interfaces:**
- Produces: export `payload['version'] = 2` (events now naturally include `state` via `SELECT *`). Restore accepts `version in (1, 2)`; v1 events get `state='scheduled'` and the next reconcile fixes them.

- [ ] **Step 1:** In `export_backup`, change `'version': 1` → `'version': 2` (server.py:994).
- [ ] **Step 2:** In `import_backup` (server.py:1015): `if not payload or payload.get('version') not in (1, 2):`. In the events INSERT (1064-1070) add the `state` column: `s.get('state', 'scheduled')`.
- [ ] **Step 3: Smoke-check:** `curl -s localhost:5000/api/backup > b.json` → verify `"version": 2` and events carry `"state"`. Edit `b.json` to `"version": 1`, strip `state` keys, POST to `/api/backup/restore`, GET roster — statuses correct after the built-in reconcile.
- [ ] **Step 4: Commit** — `feat: backup format v2 with absence state (accepts v1 on restore)`

### Task 4: Frontend stops double-bookkeeping

**Files:**
- Modify: `index.html:2800-2851` (`load`/`apiUpdate`), `2895`, `3379-3395` (auto-return in `render`), `3426-3434`, `3607-3620`, `3907-3948` (`saveTdyLeave`), `3971-3977` (`syncFirstSchedule`), `4093-4170` (add-person objects)

**Interfaces:**
- Consumes: new `GET /api/personnel` shape (`scheduled_events` with `state`, no meaningful `sched_*`).
- Produces: `scheduledEventsFor(p)` returns only `state==='scheduled'` events for "upcoming" UI; a new `activeEventFor(p)` is NOT needed (active status already lives on `p.status`).

- [ ] **Step 1:** `load()` — delete line 2818 (`api('POST', '/activate-scheduled');`) and the `sched_*` fields from the mapping (2813-2814). Keep `scheduled_events`, but filter display copies: `scheduled_events: (p.scheduled_events || []).filter(e => e.state !== 'active')` — active events are represented by the person's own status; scheduled section should show upcoming only.
- [ ] **Step 2:** `apiUpdate()` — delete the `sched_*` lines (2848-2849).
- [ ] **Step 3:** `render()` — delete the auto-return block (3383-3395) including `recentlyReturned` usages below it (search `recentlyReturned` and remove its badge rendering; the server now owns returns).
- [ ] **Step 4:** Remove `p.sched_status` fallbacks: line 2895 (`filter === 'scheduled'`), 3426 (`scheduledList`), 3434 (counts), and the legacy single-event fallback at 3613. Each becomes purely `scheduledEventsFor(p)`.
- [ ] **Step 5:** `saveTdyLeave()` — everything except `loan` goes through the events API; the server decides active vs scheduled:

```javascript
  if (awayStatus === 'loan') {
    p.status = 'loan'; p.notes = notesVal; p.from = ''; p.to = '';
    await apiUpdate(p);
  } else {
    const event = await api('POST', `/personnel/${p.id}/schedule`, {
      status: awayStatus, from_date: awayFrom, to_date: awayTo, notes: notesVal
    });
    if (!event || event.error) {
      alert(event && event.error ? event.error : 'Could not save absence.');
      return;
    }
    await load();   // server reconciled; pull truth
  }
  closeTdyLeaveModal();
  render();
```

- [ ] **Step 6:** Delete `syncFirstSchedule()` (3971-3977) and its call sites (3936, 3959, 4016 — replace with nothing; after mutations that used it, call `await load()` if not already reloading). Remove `sched_*` from the two add-person literals (~4093-4098, ~4162-4167).
- [ ] **Step 7: Browser check** (dev server + Browser pane): schedule tomorrow-dated leave → row shows under Scheduled; set from-date today → soldier immediately shows as On Leave without a second refresh; set a seeded active event's to_date to yesterday in sqlite3 → reload → soldier back in Needs Action.
- [ ] **Step 8: Commit** — `fix: frontend reads absence truth from server; drop client auto-return`

---

## Phase 2 — Usability & mobile

### Task 5: Rebuild the status modal (native dates, phone-first layout)

**Files:**
- Modify: `index.html:2588-2651` (modal markup), CSS near `.status-toggle-group`, `3852-3905` (open/close), calendar functions (`toggleCalendar`, `renderCalendar`, `calTarget` — remove)

**Interfaces:**
- Produces: same `saveTdyLeave()` entry point; `awayFrom`/`awayTo` now read from `<input type="date" id="awayFrom">` / `#awayTo` `.value` directly (drop the `fromDisplay`/`toDisplay` + `#calendarContainer` custom calendar entirely).

- [ ] **Step 1:** Replace the two display fields + calendar container (2619-2626) with:

```html
<div class="date-pick-row">
  <label>From <input type="date" id="awayFrom"></label>
  <label>To <input type="date" id="awayTo"></label>
</div>
```

- [ ] **Step 2:** Delete the custom calendar JS (`toggleCalendar`, `renderCalendar`, month-nav handlers, `calTarget`, `awayFrom/awayTo` globals become reads from the inputs at save time). `openTdyLeave` pre-fills the inputs from `p.from`/`p.to`.
- [ ] **Step 3:** Restyle the modal single-column: status chips wrap (`display:flex; flex-wrap:wrap; gap:8px`), fields stacked, sticky Save/Cancel footer. Add `@media (max-width: 480px)` rules: modal full-width bottom sheet (`position:fixed; inset:auto 0 0 0; border-radius:12px 12px 0 0; max-height:90dvh; overflow-y:auto`).
- [ ] **Step 4: Browser check** at 375×812: open modal, set TDY with dates using native pickers, save; verify no horizontal scroll, all controls reachable.
- [ ] **Step 5: Commit** — `feat: phone-first status modal with native date pickers`

### Task 6: Mobile navigation — "More" sheet

**Files:**
- Modify: `index.html:2528-2533` (`.dash-bottomnav`), header More menu (2436-2446), CSS `@media (max-width:900px)` block

**Interfaces:**
- Produces: bottom nav = Dashboard, Roster, Accountability, Reports, **More**. `openMoreSheet()` toggles `#moreSheet`, a bottom sheet listing: Directory, History, Duty Roster, Import List, Export CSV, Reset Day, Audit Log (admin-gated same as sidebar), Settings.

- [ ] **Step 1:** Add the fifth bottom-nav button and `#moreSheet` markup (plain `<div class="more-sheet">` with the same `onclick` handlers the sidebar/header use: `openDirectory()`, `openHistory()`, `openDutyRoster()`, `openImport()`, `exportCSV()`, `resetDay()`, `openAuditLog()`, `toggleDashSettings()`). Reuse existing admin-visibility logic (copy how the sidebar Audit item is shown/hidden).
- [ ] **Step 2:** CSS: sheet slides over content (`position:fixed; bottom:0; left:0; right:0; z-index` above bottom nav), scrim behind it closes on tap.
- [ ] **Step 3: Browser check** at 375px: every destination reachable from bottom nav; admin item hidden for non-admin.
- [ ] **Step 4: Commit** — `feat: mobile More sheet exposes all destinations from bottom nav`

### Task 7: Mobile debt cleanup

**Files:**
- Modify: `index.html:2459-2469` (bulk bar), `2579` (import table), `2305-2324` (dead 380px CSS)

- [ ] **Step 1:** Bulk bar: replace inline `style="background:#1a1a2e;color:#e0e0e0"` with a class using theme tokens.
- [ ] **Step 2:** Wrap `#importPreviewTable` in `<div style="overflow-x:auto">` (or an `.table-scroll` class).
- [ ] **Step 3:** Delete the `max-width:380px` `.table-wrap table` grid-area rules (2318-2324) that target removed markup; keep the `.stats-bar` 2-col rule if `.stats-bar` still exists in live markup (grep first; delete if orphaned).
- [ ] **Step 4: Browser check** both themes at 375px: bulk bar legible in light mode; import preview scrolls instead of overflowing.
- [ ] **Step 5: Commit** — `fix: mobile styling debt (bulk bar tokens, import overflow, dead CSS)`

---

## Phase 3 — Soldier page

### Task 8: Absence history endpoint + grouped Absences tab

**Files:**
- Modify: `server.py` (new route after `add_scheduled_event`), `index.html:3324-3377` (`saveAbsence`/`renderProfileAbsences`)

**Interfaces:**
- Produces: `GET /api/personnel/<id>/absences` → `{"absences": [event dicts incl. state, newest from_date first]}` (auth: same platoon-access pattern as `get_profile`). Soldier page groups by state: Current (`active`), Upcoming (`scheduled`), Past (`completed`).

- [ ] **Step 1:** Add route:

```python
@app.route('/api/personnel/<int:person_id>/absences', methods=['GET'])
@login_required
def get_absences(person_id):
    conn = get_db()
    person = conn.execute('SELECT id, platoon FROM personnel WHERE id = ?', (person_id,)).fetchone()
    if person is None:
        conn.close()
        return jsonify({'error': 'Not found'}), 404
    user = get_current_user()
    if not has_platoon_access(user, person['platoon']):
        conn.close()
        return jsonify({'error': 'Forbidden'}), 403
    _reconcile_absences(conn, date.today().isoformat())
    conn.commit()
    rows = conn.execute(
        'SELECT * FROM scheduled_events WHERE person_id = ? ORDER BY from_date DESC, id DESC',
        (person_id,)
    ).fetchall()
    conn.close()
    return jsonify({'absences': [dict(r) for r in rows]})
```

- [ ] **Step 2:** `renderProfileAbsences()` fetches this endpoint and renders three headed groups (Current/Upcoming/Past); Past rows are read-only (no Remove button), Current/Upcoming keep Remove.
- [ ] **Step 3: Browser check:** soldier with a completed, an active, and a future event shows all three groups correctly.
- [ ] **Step 4: Commit** — `feat: absence history on soldier page (current/upcoming/past)`

### Task 9: Readiness fields

**Files:**
- Modify: `server.py:144-163` (CREATE TABLE), migration block, `PROFILE_FIELDS` (696-700), `index.html` soldier Overview markup (~3205-3289) + `PROFILE_FIELD_IDS`

**Interfaces:**
- Produces: four new `personnel_profile` columns: `flags`, `medical_date`, `dental_date`, `weapons_qual` (all TEXT DEFAULT ''). Added to `PROFILE_FIELDS` so GET/PUT profile handle them automatically.

- [ ] **Step 1:** Add columns to the CREATE TABLE and an idempotent migration loop (`PRAGMA table_info(personnel_profile)` + ALTER for each missing column). Append to `PROFILE_FIELDS`.
- [ ] **Step 2:** Add a "Readiness" card to the Overview tab: Flags (text), Medical readiness (date), Dental readiness (date), Weapons qual (text, e.g. "M4 — 2026-05-01"). Wire ids into `PROFILE_FIELD_IDS`.
- [ ] **Step 3: Browser check:** values save and reload on the soldier page.
- [ ] **Step 4: Commit** — `feat: readiness fields on soldier profile`

---

## Phase 4 — Command-post redesign

### Task 10: Single token system

**Files:**
- Modify: `index.html:487-533` (`--theme_*`), `1911-1990` (`--dash-*`)

**Interfaces:**
- Produces: one `:root` token set (with `[data-theme="dark"]` or existing dark-mode class override) covering: surfaces (3 levels), text (2), border (2), accent, and semantic status tokens `--st-present`, `--st-scheduled`, `--st-away`, `--st-ftr`, `--st-loan` (+ `-bg` tints). `--dash-*` names become aliases pointing at the new tokens during transition, then get find-replaced.

- [ ] **Step 1:** Define the palette. Direction: graphite/slate surfaces, olive-drab accent, semantic colors: green (present/accounted) `#3f6f21`-family, amber (scheduled/due back), red (FTR/unaccounted), steel-blue (TDY/away). Light + dark variants both defined deliberately (no auto-derivation).
- [ ] **Step 2:** Point every `--dash-*` and `--theme_*` variable at the new tokens (aliases), verify nothing visually breaks, then replace usages file-wide and delete the aliases.
- [ ] **Step 3:** Fix the font stack (533): drop the phantom `Haas`; headings get a condensed system stack (`"Arial Narrow", "Helvetica Neue Condensed", system-ui-condensed, sans-serif-condensed, sans-serif`) via a `--font-display` token; enable `font-variant-numeric: tabular-nums` on counts/metrics.
- [ ] **Step 4: Commit** — `refactor: single design-token system`

### Task 11: Restyle components to the command-post direction

**Files:**
- Modify: `index.html` CSS throughout (`.dash-*` components, modals, soldier page, bottom nav, home screen)

- [ ] **Step 1:** Apply the direction: dense rows (reduced vertical padding, hairline separators), sharp radii (2-4px), flat elevation (1px borders instead of blurred shadows), uppercase condensed section headers with letter-spacing, status pills as solid-left-border tags rather than rounded pastel chips, metric cards as stat blocks with big tabular numerals.
- [ ] **Step 2:** Both themes pass: every screen (dashboard, modal, soldier page, directory, history, home) checked in light and dark.
- [ ] **Step 3: Browser check** at 375px and 1440px, both themes, screenshots of: dashboard, status modal, soldier page, More sheet.
- [ ] **Step 4: Commit** — `feat: command-post visual redesign`

### Task 12: Final verification sweep

- [ ] **Step 1:** Full workflow at 375px: log-in screen renders → mark present (1 tap) → set TDY today (activates immediately) → schedule future leave (shows in Scheduled) → sqlite3-expire it → reload → auto-returned. Export backup, restore it, roster intact.
- [ ] **Step 2:** `git log --oneline` sanity; every phase deployable; update `CLAUDE.md` architecture notes (scheduled_events lifecycle, reconcile-on-read, backup v2).
- [ ] **Step 3: Commit** — `docs: update CLAUDE.md for absence lifecycle model`

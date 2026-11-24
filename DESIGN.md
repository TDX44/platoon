# Design System: Platoon Manager

Character: calm and exact, a tool a leader trusts in formation at 0630, one-handed, in any light.

Extracted from the `--cp-*` tokens in `index.html` (`:root` for light, `body.dark-mode` for
dark), which are the source of truth; this file records them and why. Every value below exists
under the same name in code. The marketing site (`TDX44/platoon-site`) carries a copy of the
`--cp-*` block in its `site.css`.

## Color
- `--cp-bg` #f5f7f9 / dark #12161b, `--cp-surface` #ffffff / dark #1a1f26: cool neutral greys;
  the page recedes so status colour is the only thing that pops.
- `--cp-text` #344054 (10.5:1 on surface), `--cp-text-strong` #17212b for headings,
  `--cp-muted` #667085 (5.0:1) for secondary lines. `--cp-faint` #8b95a5 is 3.0:1: icons,
  placeholders and dividers only, never text a leader has to read.
- `--cp-accent` #146bc5 / dark #4a94dd: brand blue, only on the primary action, links, focus
  and the current nav item. White label on it is 5.3:1.
- `--cp-green` #2f8f4e (success), `--cp-amber` #b7791f (warning), `--cp-red` #c73e3e
  (destructive and "unaccounted"; hue 0° vs the accent's 211°). Semantic only, never decoration.
- Danger text on its tint uses `--cp-st-ftr-bg` #b42318 (5.7:1 on `--cp-red-bg`), because
  `--cp-red` on `--cp-red-bg` is 4.4:1 and fails body-text contrast.

## Typography
- Family: Inter, falling back to the platform UI face (`-apple-system`, Segoe UI, Roboto); the
  system stack chosen on purpose, nothing is downloaded, so the app paints at once on a weak
  signal. Numbers use `font-variant-numeric: tabular-nums` so counts do not jitter.
- Scale: set in `em` off `body` (16px on a phone, 21px above 900px), never `rem`: 0.72 / 0.82 /
  0.88 / 1 / 1.15 / 1.4 / 2. Hierarchy comes from weight and size together, not colour.
- Weights: 400 body, 600 labels and buttons, 700 headings and the big counts.

## Spacing
- Base unit 4px; scale 4 / 8 / 12 / 16 / 20 / 24 / 32.
- Rhythm: tight inside a group (4-8px), generous between groups (16-24px). Phone gutter 16px.

## Shape & elevation
- Radii: `--cp-radius-sm` 4px (badges' inner bits, inputs in tables), `--cp-radius` 6px
  (buttons, inputs), `--cp-radius-lg` 8px (cards, sheets, the status card). Pills (badges,
  counts) are fully round.
- Separation: borders (`--cp-border` #d8dee6, `--cp-hairline` #e3e8ee). `--cp-shadow` is a 1px
  lift for cards only; `--cp-shadow-lg` is for overlays (menus, sheets, modals).

## States
- Hover: `--cp-hover` #f1f5f8 behind neutral controls; `--cp-accent-hover` on primary.
- Pressed: `--cp-accent-active` on primary.
- Focus: 2px `--cp-accent` outline, or the `--cp-focus-ring` (accent at 28%) halo on inputs.
- Disabled: 55% opacity, `cursor: not-allowed`, no hover change.
- Touch: every phone control is at least 44px tall (`tests/test_mobile_layout.py` measures it).

## Status and badges
Each status is text on its own tint, light mode (dark mode is 7.0-8.5:1 throughout):
- Present: #1f7a3d on #e8f5ec, 4.8:1
- TDY: #1a5fb4 on #e8f1fc, 5.5:1
- Leave: #8a5300 on #fdf2dc, 5.7:1
- Pass: #0b6e79 on #e2f4f5, 5.3:1
- Late: #b3400d on #fdeee4, 5.1:1
- Excused: #6a45b0 on #f1ecfa, 5.9:1
- Other: #4b5563 on #eef0f3, 6.6:1
- FTR: #ffffff on #b42318, 6.6:1
- Scheduled: #a0266f on #fbe9f3, 6.0:1
- Unaccounted: #b42318 on `--cp-red-bg` #fcecec, 5.7:1
Status is never colour alone: every badge carries its word.

## Motion
- 120ms for hover and press, 200ms for sheets and the progress bar; ease-out. Nothing moves under
  `prefers-reduced-motion`.

## Components
- Button (`.dash-btn`): primary (accent fill), secondary (surface + border), ghost (border
  only), danger text in menus. One primary per screen.
- Card: surface, 1px border, `--cp-radius-lg`, 16px padding on a phone.
- Status card (home): the day, the accounted count, a progress bar, the unaccounted badge and the
  three ways in (Take accountability, Formation mode, Quick add).
- Unit row (home, phone): name and kind on the left, "accounted / total" and the unaccounted badge
  on the right, a 4px progress bar underneath; the whole row is the button.
- Sheet (`.more-sheet`): bottom sheet for the phone's More menu.

## Voice
- Plain and short, the way a squad leader talks: "Take accountability", "37 unaccounted",
  "Everyone accounted for". Sentence case on buttons, verbs first. Dates read like the roster
  (`24NOV`) inside data, and in words ("Tuesday 24 Nov") in headings.

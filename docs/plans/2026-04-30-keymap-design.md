# Keyboard Shortcut System — Design

Date: 2026-04-30
Status: Design (validated via brainstorm)
Inspiration: [Vimium-C](https://github.com/gdh1995/vimium-c)

## Goal

Make Vireo more keyboard-fluent by adding two features on top of the existing per-page shortcut system:

1. **Link hints** — press `f` and tiny letter labels overlay every clickable element; type the label to "click" it. Reaches anything without dedicating a memorized key to it.
2. **Discoverability layer** — a context-aware `?` help modal plus inline badges that show the keystroke on every shortcut-able button.

While we're in there, do a small cleanup sweep so existing shortcuts behave consistently across pages.

## Non-goals

- **Modal command sequences** (`gb`, `gp`, `yy`-style two-key combos). Deferred — single-letter nav already covers this.
- **Vim-style movement everywhere** (`hjkl` / repeat counts). Out of scope; we add `J`/`K` for next/prev list nav (cleanup item) but no full vim motion model.
- **Command palette** (`:` / `Cmd+K` fuzzy action search). Deferred — `?` covers the discoverability gap with much less work; revisit if shortcut count grows past ~50.
- **First-run nudges / "shortcut of the day" toasts.** Cute, easy to make annoying. Deferred.
- **Admin pages** (Stats, Jobs, Logs, Audit, Settings, Lightroom, Workspace). Low ROI — keep mouse-driven.
- **Auto-detected hints on grid items.** Photo grids can have hundreds of thumbnails; auto-hinting would produce a sea of labels. Grid items are opt-in via `F` / `gf` (extended hint mode).

## User-facing features

### Link hints

- `f` — enter hint mode for **chrome** elements (curated `data-hint` set: navbar, primary action buttons, modal controls, filter inputs, lightbox controls).
- `F` (or `gf`) — enter **extended** hint mode that also includes `data-hint-grid` elements (photo cards, miss rows, keyword rows).
- Type a label to trigger; `Backspace` deletes a typed character; `Esc` cancels; typing a non-matching key cancels.
- Label alphabet (Vimium-C default): `sadfjklewcmpgh`. Two characters max. Shortest labels assigned to most prominent elements (largest bounding box, nearest viewport center).
- Visual: small dark badge with light text, bottom-left of the target's bounding box. Reuse Vireo's tooltip color tokens (light/dark theme aware). Typed prefix bolds; non-matching candidates fade to 30% opacity.

### Discoverability — `?` help modal

Replace the existing `?` modal with a context-aware version backed by the keymap registry. Two columns:

- **This page** — page-scoped shortcuts.
- **Always available** — global nav, lightbox shortcuts (when open), etc.

Each row: keystroke • action name • one-line description. Search box at top filters live (substring on name + description). Customizable shortcuts get a tiny pencil icon linking to `shortcuts.html`.

Grouping inside each column comes from a `category` field on each registered shortcut: `Navigation`, `Edit`, `Selection`, `View`, `System`. Flat sections, no nesting.

`?` again or `Esc` closes.

### Discoverability — inline badges

Small faint pill in the corner of any element with a registered keyboard shortcut. Same visual language as link-hint badges but lower contrast — they're "always on" so they need to recede.

- Implementation: single CSS class + tiny boot script that walks `[data-shortcut]` elements and overlays the label from the registry.
- Toggle: `\` (backslash) toggles badge visibility globally. Persisted to `config.json:show_shortcut_badges`.
- Default: ON. Idea is the user learns the keys and turns them off after some weeks.
- Badges only appear for elements with a *named keyboard shortcut*. Elements that are only reachable via link hints get no badge — link hints are ephemeral by design.

## Architecture

### Single source of truth: a JS keymap registry

New file: `vireo/static/js/keymap.js`. API:

```js
Keymap.register(scope, shortcut)
// scope: "global" | "browse" | "cull" | "review" | ... (page id)
// shortcut: { key, name, description, category, action, customizable? }
```

A single dispatcher owns `keydown`. It:

1. Suppresses single-letter / `?` / `f` / `\` keys when focus is in `<input>`, `<textarea>`, or `[contenteditable]`.
2. If hint mode is active, routes everything to the hint engine.
3. Otherwise, looks up the key in the registry (current scope + global) and fires the action.
4. `Esc` is owned by the dispatcher and unwinds a single overlay stack: hint mode → modal → lightbox → bottom panel.

### Why one registry

Today, "what does `J` do" requires grepping templates. With a registry:

- The `?` modal reads from it.
- Inline badges read from it.
- Hint-mode collision avoidance reads from it (though hint mode is modal, so collisions are impossible — but the registry's still the answer to "is `f` taken on this page").
- The `shortcuts.html` editor reads from it (replacing `SHORTCUT_DEFAULTS`).
- Per-shortcut user overrides from `config.json:keyboard_shortcuts` are merged on boot.

### Hintable elements: declarative

We do **not** auto-detect clickables. Anything we want hintable gets `data-hint="action-name"` in the template. Anything in the extended grid set gets `data-hint-grid`. Curated, reviewable in markup, no label sprawl.

### Backwards compatibility

- `config.json:keyboard_shortcuts` format unchanged.
- `shortcuts.html` editor keeps working (now driven off the registry).
- Existing per-page handlers are migrated incrementally. Until migrated, they coexist with the registry — the dispatcher only intercepts keys the registry knows about.

## Per-page hint targets

Curated `data-hint` set for each in-scope surface:

- **Navbar (everywhere)** — workspace switcher, each tab, bottom-panel toggle, theme toggle.
- **Browse** — filter chips/inputs, sort menu, selection toolbar buttons, pagination, view-mode toggle.
- **Cull** — "Apply Culling Decisions", per-group expand/collapse, filter inputs.
- **Review / Pipeline Review** — Accept, Skip, group-review toggle, "remove from group", filter inputs.
- **Lightbox** — close, prev/next, zoom +/-/fit, boxes toggle, zoom mode toggle.
- **Map / Compare / Variants / Duplicates / Misses / Keywords / Pipeline** — primary action buttons, filter inputs, view toggles. (Specific list per page during implementation — small, mechanical.)

Extended grid set (`data-hint-grid`):

- Photo cards in Browse, Cull, Variants, Compare.
- Miss rows in Misses.
- Keyword rows in Keywords.

**Not** hinted: text labels inside cards, every "open" affordance on a thumbnail (use grid mode), admin pages.

## Cleanup sweep (alongside)

- **`J`/`K` for next/previous** in every list/grid: Browse, Cull, Review, Pipeline Review, Misses, Variants, Duplicates, Compare, Keywords. Today only Misses has it. Arrow keys keep working as a synonym.
- **`?` on every page** — already global in `_navbar.html`; verify and document.
- **`Esc` reliability** — currently each overlay has its own handler and they sometimes race. Dispatcher owns `Esc` with a single unwind stack.
- **Input-focus suppression** — uniform across all global single-letter keys (currently inconsistent per page).

## Edge cases

- **Focused inputs**: hint mode does not activate; global single-letter shortcuts suppressed.
- **Inside the lightbox**: hints scope to lightbox controls; thumbnails behind it are not hintable. Honor the existing `_navbar.html:3082` "active overlay" flag.
- **Modals open**: `f` only hints elements inside the modal (visibility-aware DOM scope).
- **Hidden / off-screen elements**: skipped (`getBoundingClientRect` + visibility check).
- **Conflicts with letter shortcuts**: impossible. Hint mode is modal; once `f` is pressed, the dispatcher routes all keys to the hint engine until cancellation.

## Testing

1. **Backend test** — a small test in `vireo/tests/test_app.py` that the endpoint serving merged shortcut defaults + overrides returns correct data.
2. **Manual checklist** (kept in this design doc — see below). For each of the 13 in-scope surfaces: press `?` and verify the modal lists the right shortcuts, press `f` and verify the curated set is hinted, press `F`/`gf` and verify grid items are hinted, press `Esc` and verify cleanup.
3. **Playwright smoke** — one test loads Browse, presses `f`, types the label for a known navbar target, asserts navigation. Per the user-first-testing memory, drive a real browser. One path is enough to catch wiring regressions.

### Manual checklist (per surface)

- [ ] `?` opens modal listing correct shortcuts
- [ ] `f` hints the curated chrome set, no grid sprawl
- [ ] `F`/`gf` extends hints to grid items where applicable
- [ ] `Esc` closes the topmost overlay only
- [ ] Single-letter shortcuts suppressed inside inputs
- [ ] Inline badges appear on shortcut-able buttons
- [ ] `\` toggles badges; persists across reload

## Rollout (PR breakdown)

Each PR is independently mergeable and useful.

1. **PR 1 — Foundation.** `keymap.js` registry + dispatcher + input-focus suppression + `Esc` stack. Migrate global navbar shortcuts only. No user-visible change beyond consolidated behavior.
2. **PR 2 — Discoverability.** New `?` modal + inline badges + `\` toggle. Migrate Browse/Cull/Review/Lightbox shortcuts into the registry (highest-value pages for badges).
3. **PR 3 — Link hints.** Hint engine (`f`/`F`), curated `data-hint` markup on PR 2's pages first.
4. **PR 4 — Coverage.** Migrate remaining user pages (Pipeline, Map, Compare, Variants, Duplicates, Misses, Keywords) into the registry, add their `data-hint` attributes, extend `J`/`K` to grid/list pages that don't have it.

If we stop after PR 2, we've still gained a unified `?` and badges. Admin pages stay untouched throughout.

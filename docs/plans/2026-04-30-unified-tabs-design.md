# Unified navbar tabs

## Problem

Vireo's navbar currently uses a dual model:

- **Linger pages** — 13 always-shown links rendered as static `<a>` tags (Pipeline, Jobs, Pipeline Review, Review, Cull, Misses, Highlights, Browse, Map, Variants, Dashboard, Audit, Compare). Reorderable via `nav_order` JSON column on `workspaces`.
- **Openable tabs** — 7 pages (Settings, Workspace, Lightroom, Shortcuts, Keywords, Duplicates, Logs) that are pinnable from a `+ Tools ▾` dropdown. Stored in `workspaces.open_tabs`. Closeable via `×` on hover.

This dual model has two concrete failures:

1. **Navbar overflows.** Total natural width is ~1730px (13 linger pages + 4 default open tabs + Tools button + 6 right-side icons). At any laptop viewport (1280–1440px) the navbar overflows the viewport and right-side icons get pushed off-screen. There is no overflow handling.
2. **"Bouncing" on hover.** When you hover an openable tab, its `×` button shifts from `display: none` to `display: inline-block`, which expands the tab's intrinsic width. Because the tab strip is a flex item under shrink pressure (since the navbar already overflows), this expansion forces the strip to wrap content onto two lines and dramatically reshuffles tab positions. As you move the mouse, layout shifts under the cursor and hover hops between tabs — visible bouncing. Reproduced via Playwright at 1366px width: hovering `Lightroom` makes its bounding box jump from 90×44 to 249×62.

The split between "always shown" pages and "openable" pages also forces every new page to pick a side, even when the right answer (e.g. for a cross-cutting page like Jobs) is "neither, it's just a page."

## Goal

Replace the dual model with **a single user-curated tab list** plus a command palette as the universal escape hatch. The navbar reflects pages the user actually wants visible right now; everything else is `cmd+K` away.

## Design Decisions

- **One ordered list of pinned tabs per workspace.** Replaces both `nav_order` and `open_tabs`. Every existing page is a first-class entry; no "openable vs linger" distinction.
- **Ephemeral tab slot.** Visiting a page that isn't pinned shows it as an italic tab at the right end of the strip. Replaced when you visit another unpinned page. JS-only state, no DB column.
- **Command palette (`cmd+K`).** Fuzzy-find any page. Pages-only in this PR; workspaces and actions are deliberate follow-ups.
- **Overflow handled by `…` dropdown**, not horizontal scroll. Tabs that don't fit collapse into a "More" menu at the right end of the strip. Active tab and ephemeral tab are always visible.
- **Close button uses absolute positioning.** Showing the `×` no longer changes tab width — eliminates the layout reflow that causes "bouncing" today.
- **Migration is a hard reset.** Every workspace's tabs are reset to the new 9-tab default. No preservation of existing `nav_order` or `open_tabs` (per the solo-user-app convention — no historical migration paths).

## Model

### Page IDs

All 20 existing pages become equal entries:

```
pipeline, jobs, pipeline_review, review, cull, misses, highlights,
browse, map, variants, dashboard, audit, compare,
settings, workspace, lightroom, shortcuts, keywords, duplicates, logs
```

A new constant `ALL_NAV_IDS` in `vireo/db.py` replaces `OPENABLE_NAV_IDS`.

### Default tabs (new workspaces)

```python
DEFAULT_TABS = [
    "browse", "pipeline", "pipeline_review",
    "review", "cull", "jobs",
    "highlights", "misses", "settings",
]
```

Nine pages — chosen to cover daily workflow (look at photos, run AI, watch progress, curate results) plus an obvious settings entry point.

### Globals (always visible, never tabs)

| Element                              | Side  |
| ------------------------------------ | ----- |
| Brand (Vireo logo)                   | left  |
| Workspace switcher                   | left  |
| ⚠ Report, ? Help, ⚙ DevMode, ✶ Theme, ▦ Panel toggle | right |

**Removed from the navbar:**
- `☰` Logs icon (Logs is now a regular tab; pin it if you want it always visible)
- `+ Tools ▾` button (pinning is contextual: pin from the ephemeral tab or via `cmd+K` follow-up)

Layout becomes:

```
[Brand] [Workspace ▾]   [tab1] [tab2] … [ephemeral?] [… overflow]   [⚠][?][⚙][✶][▦]
```

## Tab strip behavior

### Overflow

On render and on resize (via `ResizeObserver`), JS measures the strip's available width vs total tab content width. If tabs don't fit, hide pinned tabs from the right end into a `…` overflow menu until the strip fits. Constraints:

- The active tab is always visible. If keeping the active tab would push out a still-fitting pinned tab, hide the next-rightmost pinned tab instead.
- The ephemeral tab is always visible.
- The `…` menu lists hidden tabs in their pinned order. Clicking one navigates to that page; the visible/hidden split recomputes to keep the new active tab visible.

### Close button

```css
.nav-tab { position: relative; padding-right: 24px; }
.nav-tab-close {
  position: absolute;
  right: 4px;
  top: 50%;
  transform: translateY(-50%);
  /* shown on hover or when active, same as today */
}
```

Reserving padding-right 24px and absolutely positioning the `×` means the tab's box never changes size when the close button appears. This is the structural fix for "bouncing."

### Drag-reorder

Generalized from today's `linger-pages-only` reorder to operate on the entire tab strip. Selector changes from `a[data-nav-id]:not([data-tab])` (`_navbar.html:1814`) to `a[data-nav-id]` over the full strip. Dragging the ephemeral tab into the pinned region promotes it to pinned at the drop position. Drop on the `…` overflow zone appends to the end.

### Close semantics

| Action                              | Result                                                                |
| ----------------------------------- | --------------------------------------------------------------------- |
| Close a non-active pinned tab       | Tab disappears from strip; you stay where you are.                    |
| Close the active pinned tab         | Navigate to the tab on its right (or left if it was last).            |
| Close the active tab when last left | Navigate to `/browse`. Browse appears as ephemeral (since unpinned).  |
| Close the ephemeral tab             | Clear ephemeral slot; same navigation rule as close-active.           |

### Hotkeys

- **Existing per-page single-key shortcuts** (`b`=Browse, `c`=Cull, etc.) — unchanged. Page-bound, customizable via `/shortcuts`.
- **`cmd/ctrl+K`** — open palette.
- **`cmd/ctrl+1` through `cmd/ctrl+9`** — jump to nth pinned tab (browser convention).
- **`cmd/ctrl+W`** — close current tab.

## Command palette (`cmd+K`)

### UI

Center-screen modal, ~480px wide, semi-transparent backdrop. Single text input at top, results list below. `Esc` or click-outside dismisses.

Each result row shows the page label and a small `📌` if currently pinned. The currently-active page row gets a subtle highlight (separate from the keyboard-selection highlight) so you can see "where I am" vs "what I'd navigate to."

### Search

Use the already-vendored Fuse.js. Match against page label (`"Pipeline Review"`) and page ID (`pipeline-review`), with label weighted higher.

Empty input shows all pages: pinned in pinned-order, then unpinned in alphabetical order. The palette is also a discovery surface, not just a filter.

### Keyboard

- Type to filter
- `↑` / `↓` — move keyboard selection (wraps at top/bottom)
- `Enter` — navigate to selected
- `Esc` — close

### Selection behavior

Picking a result navigates to that page. Per the ephemeral-tab rule:
- If the page is pinned → it just becomes the active tab.
- If the page is unpinned → it becomes the ephemeral tab.

No "pin from palette" affordance in V1. The palette is a navigation surface, not a configuration surface.

### Discoverability

Add a small `cmd+K` hint either inside or to the right of the workspace switcher button so the shortcut is visible the first time a user looks at the navbar. Help modal (`F1`) gets a new entry under "Navigation."

## Schema migration

```sql
ALTER TABLE workspaces ADD COLUMN tabs TEXT;
UPDATE workspaces SET tabs = '["browse","pipeline","pipeline_review","review","cull","jobs","highlights","misses","settings"]';
ALTER TABLE workspaces DROP COLUMN nav_order;
ALTER TABLE workspaces DROP COLUMN open_tabs;
```

`DROP COLUMN` requires SQLite ≥ 3.35. If Vireo's pinned SQLite is older, fall back to the standard recreate-table-without-column dance. Confirm at implementation time.

Migration is one-shot — every workspace ends up with the 9-tab default, no preservation. Per the solo-user-app convention, this is acceptable.

## Backend API

| Old                                          | New                                          |
| -------------------------------------------- | -------------------------------------------- |
| `POST /api/workspace/tabs/open`              | `POST /api/workspace/tabs/pin`               |
| `POST /api/workspace/tabs/close`             | `POST /api/workspace/tabs/unpin`             |
| `GET /api/workspace/tabs`                    | unchanged path; response shape simplifies    |
| `PUT /api/workspaces/active/nav-order`       | `POST /api/workspace/tabs/reorder`           |

`GET /api/workspace/tabs` response becomes:

```json
{
  "tabs": ["browse", "pipeline", ...],
  "all_pages": [
    {"id": "browse", "label": "Browse", "href": "/browse"},
    ...
  ]
}
```

No more `open_tabs` vs `openable_pages` split.

### Removed code

- `OPENABLE_NAV_IDS` constant (`db.py:46`)
- `_auto_open_tab(nav_id)` helper (`app.py:992`) and every call to it on page routes — ephemeral tabs are JS-only
- `/api/workspaces/active/nav-order` endpoint and the JS that calls it
- The split rendering in `_navbar.html` (linger pages as static `<a>` tags + `#navOpenTabs` span). All tabs are dynamic now.

### `Database` methods

Replace today's `open_tab` / `close_tab` / `get_open_tabs` with:

- `get_tabs() -> list[str]`
- `set_tabs(tabs: list[str]) -> list[str]` — full-list replace, validates IDs, used by reorder
- `pin_tab(nav_id: str) -> list[str]` — append if absent, no-op if present
- `unpin_tab(nav_id: str) -> list[str]` — remove if present, no-op if absent

All validate against `ALL_NAV_IDS`.

## Files affected

| File                                   | Change                                                                  |
| -------------------------------------- | ----------------------------------------------------------------------- |
| `vireo/db.py`                          | Schema migration; `ALL_NAV_IDS` / `DEFAULT_TABS` constants; new methods |
| `vireo/app.py`                         | Endpoint renames; remove `_auto_open_tab` and callers; remove nav-order endpoint |
| `vireo/templates/_navbar.html`         | Major: HTML restructure, CSS for absolute close button, JS rewrite for unified strip + overflow + drag + palette |
| `vireo/tests/test_tabs_api.py`         | Rewrite around `pin/unpin/reorder`                                      |
| `tests/test_workspaces.py`             | Update fixtures and assertions for unified tabs                         |
| `tests/e2e/test_navigation.py`         | Extend: pin/unpin via DOM, ephemeral tab on URL visit, `cmd+K` flow, overflow `…` menu at narrow viewport, no bouncing on hover |
| (new) one migration test               | Every existing workspace gets default `tabs` after migration            |

## Out of scope (future)

- Palette includes workspaces (jump between workspaces from `cmd+K`)
- Palette includes actions (toggle theme, run scan, etc.)
- Per-pinned-tab custom labels
- Per-workspace-different default tab sets
- Navbar theming

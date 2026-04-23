# Right-click context menus — design

## Problem

Vireo has zero right-click handlers today. The browser's default context menu appears on every surface — photo grid, lightbox, folder tree, keyword rows, collection items, burst group modal. Photographers arriving from Lightroom, Photo Mechanic, or Finder expect right-click to be a primary interaction for rating, flagging, revealing files, and operating on the current selection. The gap slows one-off actions and hurts discoverability for actions that currently only live in the detail panel or batch bar.

This design adds a cross-surface context-menu system as an **additive** layer. No existing keyboard shortcut or button changes.

## Design decisions

1. **Finder-style selection coupling.** If the right-clicked item is already in the current selection, the menu operates on the whole selection. If it's not in the selection, selection is replaced with just that item before the menu opens.
2. **All seven surfaces in the first pass.** One shared component, reused everywhere: photo card (browse + review), lightbox, folder tree, keyword row, collection item, burst group modal photo.
3. **Hybrid menu contents.** Net-new capabilities (Reveal, Copy path, Find Similar, Open in Editor) plus the two highest-frequency existing actions — rating and flagging. Other duplication of detail-panel controls is avoided.
4. **Cross-platform Reveal.** macOS `open -R`, Linux `xdg-open <parent>`, Windows `explorer /select,<path>`. Failures in remote-server setups are acceptable; Copy Path is the universal fallback.
5. **Disabled with hint for single-only items.** Reveal, Open in Editor, Find Similar are greyed with a tooltip when >1 photo is selected. Copy Path works with N paths (newline-joined).
6. **Flat menus with inline chip rows.** Rating, Color, and Flag are single-row chip groups — one pointer-move per action. No submenus except the keyword Change Type menu, which reuses existing floating-dropdown infra.

## Shared component

Lives in `vireo/templates/_navbar.html` alongside the existing `.kw-type-dropdown`. Single API:

```js
openContextMenu(event, items, { anchor, onDismiss })
```

- `items`: array of `{ label, icon, onClick, disabled, disabledHint }`, `{ chips: [...] }` for inline rows, or `{ separator: true }`.
- Menu is appended to `document.body`, `position: absolute`, z-index 1000 (above detail panel and modals).
- Positioned at `event.clientX/Y`, clamped to viewport edges.
- Dismissed on outside-click, Escape, scroll, window blur.
- Arrow-key navigation + Enter to activate.

## Menus by surface

### Photo card — browse grid (`browse.html`)

```
★ ☆ ☆ ☆ ☆ ☆
⬤ ⬤ ⬤ ⬤ ⬤
🏳  ⛔  ◯
─────────────────
Find Similar              (disabled if >1)
Open in Editor            (disabled if >1)
Reveal in Finder/Folder   (disabled if >1)
Copy Path
─────────────────
Add Keyword…              (opens existing modal)
Add to Collection…        (opens existing modal)
─────────────────
Delete
```

### Photo card — review grid (`review.html`)

```
✓ Accept as [species]
✗ Not [species]
▾ Accept as…              (opens existing alternatives popup)
─────────────────
★ ☆ ☆ ☆ ☆ ☆
🏳  ⛔  ◯
─────────────────
Open in Lightbox
Find Similar              (disabled if >1)
Reveal in Finder/Folder   (disabled if >1)
Copy Path
```

Multi-select isn't tracked on the review grid today; menu operates on the single clicked card. Finder-style rule drops in automatically if multi-select lands later.

### Lightbox (shared via `_navbar.html`)

```
★ ☆ ☆ ☆ ☆ ☆
⬤ ⬤ ⬤ ⬤ ⬤
🏳  ⛔  ◯
─────────────────
Find Similar
Open in Editor
Reveal in Finder/Folder
Copy Path
─────────────────
Close Lightbox
```

`contextmenu` on the `<img>` must `preventDefault()` and skip the existing click-lock handler so zoom-lock doesn't fire alongside the menu.

### Folder tree item (sidebar in `browse.html`)

```
Filter by this folder
Expand All Children
Collapse All Children
─────────────────
Reveal in Finder/Folder
Copy Path
─────────────────
Hide from this Workspace      (removes from workspace_folders)
Rescan this Folder            (queues a scoped scan job)
```

No "delete folder" — folders are derived from filesystem scans.

### Keyword row (`keywords.html`)

Finder-style selection applies (page already tracks `selectedIds`).

```
Rename                        (triggers existing inline rename)
Change Type ▸                 (reuses existing .kw-type-dropdown)
─────────────────
Filter Browse by this Keyword
Show Photos with this Keyword (disabled if >1)
─────────────────
Delete
```

Change Type is the one submenu in the design — existing infra makes it cheaper than flattening.

### Collection item (sidebar in `browse.html`)

```
Filter by this Collection
─────────────────
Rename
Duplicate
─────────────────
Delete Collection
```

### Burst group modal photo (`review.html`)

```
⬆  Move to Picks
⬇  Move to Rejects
␣  Move to Candidates
─────────────────
★ ☆ ☆ ☆ ☆ ☆
🏳  ⛔  ◯
─────────────────
Open in Lightbox
Reveal in Finder/Folder
Copy Path
─────────────────
Remove from Group
```

The three move-actions duplicate keybindings but earn their slot because the zone-based modal benefits from explicit right-click-to-move.

## Server endpoints (new)

| Route | Body | Behavior |
|---|---|---|
| `POST /api/files/reveal` | `{photo_id}` | Resolves path from DB, shells out per OS (`subprocess.run` with list argv, short timeout, `shell=False`). Returns `{ok: true}` or `{ok: false, reason}`. |
| `POST /api/folders/rescan` | `{folder_id}` | Queues a `JobRunner` scan job scoped to that folder subtree. |
| `POST /api/collections/<id>/duplicate` | — | DB-only: copies the collection row and its photo memberships into a new row. |

Copy Path is client-side via `navigator.clipboard.writeText` — no endpoint.

## Testing

- **Pure-JS unit tests** for the Finder-style selection-intersection rule.
- **pytest** for the three new endpoints:
  - Reveal: mock `subprocess.run`, parametrize over `darwin`/`linux`/`win32`, assert argv.
  - Rescan: assert job enqueued with correct folder filter.
  - Duplicate: assert new collection row + membership rows copied.
- **Playwright (user-first testing)**: open browse, right-click a photo, click a rating chip, verify rating applied; right-click a folder, Reveal in Finder, verify endpoint called with correct photo.

## Out of scope

- Touch/long-press equivalent — deferred.
- Configurable menus / per-user reordering — deferred.
- Context menus for sidebars in pages not listed above (highlights, keywords graph, etc.).

# Openable navbar tabs — design

## Problem

Vireo's navbar currently has 20 entries. Some of those are pages you live in (Browse, Cull, Review, Audit, Map…), and some are pages you visit briefly to do a thing and leave (Settings, Lightroom import, Shortcuts, Keywords, Duplicates, Logs, Workspace switcher).

Treating both classes the same way clutters the navbar and treats Settings as if it were equal in importance to Browse.

## Goal

Split the navbar into two classes:

- **Linger pages** — always shown, no close button. Where you do real work.
- **Tabs** — openable / closeable from a Tools menu, persisted per workspace, otherwise behave exactly like regular pages (URL works, refresh works, deep links work).

This keeps the daily-driver navbar uncluttered without hiding the occasional-use pages behind a multi-click flow.

## Page classification

**Linger pages** (always shown, no X):

`pipeline · jobs · pipeline-review · review · cull · misses · highlights · browse · map · variants · dashboard · audit · compare`

**Openable tabs** (X to close, opened from Tools menu or by visiting URL):

`settings · workspace · lightroom · shortcuts · keywords · duplicates · logs`

Default tabs for a fresh workspace: `settings, workspace, lightroom` (the three judged frequent enough to want pre-opened).

## UX behavior

### Layout

```
[brand] [linger pages...]  | [open tabs with X...] [+ Tools ▾]  [hamburger]
```

A subtle visual divider separates linger pages from the tab area. Tabs render the same as linger pages but show a small `×` on hover to close. The active tab shows its `×` always (so you can close the page you're on).

### Tools dropdown

A `+ Tools ▾` button at the right end of the tab area opens a dropdown listing all 7 openable pages with a checkmark next to ones already open. The dropdown is a toggle: clicking an unchecked item opens that tab; clicking a checked item closes it. Items appear in a fixed canonical order.

### Open

Click a page in the Tools dropdown → the server appends the nav-id to the workspace's `open_tabs` list, then navigates to that page. The navbar (server-rendered) shows the new tab.

### Close

Click the `×` on a tab → server removes the nav-id from `open_tabs`. If you were on the closed page, navigate to the next tab in the open-tabs list, or `/browse` if there are no open tabs left. If you were not on the closed page, you stay where you are.

### Re-open

Identical to Open. No position memory — tab is appended to the end. (Position memory across many open/close cycles gets confusing fast; YAGNI.)

### Direct URL visit

Visiting `/lightroom` (or any openable page URL) when the tab is closed auto-opens it: the route handler appends the nav-id to `open_tabs` before rendering the page. This matches browser-tab intuition — clicking a link opens a tab.

### Keyboard shortcuts

Existing single-key shortcuts (e.g. `l` for Lightroom) work via the same auto-open path: pressing `l` navigates to `/lightroom`, which appends the tab.

### Workspace scoping

`open_tabs` is per-workspace. Switching workspaces re-renders the navbar from the new workspace's list. Existing workspace-switch flow already triggers a navigation, so no extra work is needed.

## Data model

Add an `open_tabs` TEXT column to the `workspaces` table, storing a JSON array of nav-ids in display order:

```json
["settings", "workspace", "lightroom"]
```

Why a new column rather than `config_overrides`: this is UI state, not config. Keeping them separate avoids confusing the override-merging logic in `db.get_effective_config()` and keeps `config_overrides` semantically clean (threshold, model, keys).

### Migration

On the next `Database.__init__`, add the column if missing and backfill existing workspaces with `["settings", "workspace", "lightroom"]` so nothing disappears for the user. Vireo is single-user, so no historical migration matrix to maintain.

## API

Two new workspace-scoped endpoints:

- `POST /api/workspace/tabs/open` — body `{nav_id}`, appends to end if not already open. Idempotent. Returns the new list.
- `POST /api/workspace/tabs/close` — body `{nav_id}`, removes if present. Idempotent. Returns the new list.

A canonical set of openable nav-ids lives in `vireo/app.py` (or a new `vireo/tabs.py`):

```python
OPENABLE_NAV_IDS = {"settings", "workspace", "lightroom",
                    "shortcuts", "keywords", "duplicates", "logs"}
```

API endpoints reject anything not in this set with 400. Linger-page nav-ids can never become tabs.

No `GET` endpoint is needed — the navbar is server-rendered and the list is loaded with the workspace on every request.

## Implementation surfaces

### `vireo/db.py`
- `Database.__init__` — add `open_tabs` column to `workspaces` schema, backfill defaults during migration.
- New methods: `get_open_tabs()`, `open_tab(nav_id)`, `close_tab(nav_id)`. Each operates on the active workspace and validates against `OPENABLE_NAV_IDS`.

### `vireo/app.py`
- New constant `OPENABLE_NAV_IDS`.
- New routes: `POST /api/workspace/tabs/open`, `POST /api/workspace/tabs/close`.
- Each openable page route (`/settings`, `/workspace`, `/lightroom`, `/shortcuts`, `/keywords`, `/duplicates`, `/logs`) calls `db.open_tab(nav_id)` before rendering.
- Inject `open_tabs` into the template context so `_navbar.html` can render. Use a Flask `context_processor` so every page picks it up automatically.

### `vireo/templates/_navbar.html`
- Split the nav block into "linger" and "tabs" sections with a divider.
- Iterate `open_tabs` to render tab entries with `×` buttons.
- Add the `+ Tools ▾` dropdown listing all 7 openable pages with checkmarks.
- Inline JS:
  - `openTab(navId)` — POST to `/api/workspace/tabs/open`, then `location.href = '/<page>'`.
  - `closeTab(navId)` — POST to `/api/workspace/tabs/close`. If `navId` matches the current page, redirect to the next open tab or `/browse`. Otherwise `location.reload()` (navbar is server-rendered; reload is honest and avoids drift).

### Tests (`vireo/tests/test_tabs.py`, new file)

- Open / close / re-open round-trip persists in DB.
- Closing a tab not in the list is a no-op (200, no error).
- Opening a nav-id not in `OPENABLE_NAV_IDS` returns 400.
- Direct visit to `/lightroom` when closed auto-opens it.
- Two workspaces have independent `open_tabs`.
- Migration backfills defaults on existing workspaces.
- Default tabs created for a new workspace.

## Out of scope (YAGNI)

- Drag-to-reorder tabs.
- Pinning individual tabs (the linger/tab split *is* the "pinned" concept).
- Per-user separation (single-user app).
- Live sync across browser windows on the same workspace.
- Position memory across close/re-open.
- Mobile/narrow-window responsive collapse — desktop-only app.

## Open questions resolved during design

- **Pinned vs default-open for settings/workspace/lightroom**: chose default-open with X — uniform tab model, can re-open from Tools.
- **Where to open from**: Tools menu for v1; other entry points fine to add later.
- **What X does when you close the page you're on**: navigate to next open tab, fall back to `/browse`.
- **Re-open position**: append to end (no memory).
- **Storage location**: server-side, dedicated `workspaces.open_tabs` column.
- **Direct URL visit when closed**: auto-open the tab.
- **Tools dropdown semantics**: toggle (click checked item to close).

# Frontend state ownership

The browse page and shared navbar are being split by responsibility. The first
controllers keep mutable state in factory closures and expose frozen method
objects. Loading a controller script defines its factory without registering
listeners or starting requests. No build tool or framework is required.

| Controller | Owns | Receives from its host |
| --- | --- | --- |
| `VireoBrowseCompare` in `static/vireo-browse-compare.js` | Selection snapshot, pair navigation, zoom and pan, request generations, original-image probes, Escape token and interaction listeners | `open(ids)`, `findPhoto(id)`, `fetch`, `showToast`, and the browser window/Keymap |
| `VireoWorkspaceSwitcher` in `static/vireo-workspace-switcher.js` | Workspace menu visibility, active identity, request generation, dismissal timer, workspace creation dialog | `fetch`, `navigate(path)`, `clearWorkspaceCursors()`, and the browser window |

Browse's page scripts (`static/browse/*.js`) own the live browse selection and
photo cache. Their Compare entry point passes the current selection to `open(ids)`, which copies the array.
Compare never reads or changes the page's selection globals. Closing it invalidates
pending results, detaches listeners and image callbacks, clears dragging, and
balances its Escape token and scroll lock. Reopening installs one set of listeners.

The navbar instantiates `vireoWorkspaceSwitcher`. Buttons, native menu commands,
and the workspace page invoke its methods. Rescan calls `close()` instead of
changing menu markup and flags itself. Closing invalidates menu responses and
cancels both installed and deferred outside-click listeners. Workspace names
still render without waiting for the active-workspace request.

The navbar's `safeFetch` helper is defined in a later script. The switcher receives
a forwarding callback; the initial workspace name loads at `DOMContentLoaded`.
Browse loads its Compare factory before its `browse/*.js` scripts, after the
shared helpers.

`tests/e2e/test_frontend_controllers.py` loads complete modules against real browser
DOMs with controlled asynchronous dependencies. It covers stale responses,
selection snapshots, close/reopen cleanup, and the real template entry points.
Existing Compare and workspace-navigation journeys cover ordinary interactions.

Remaining responsibilities include browse query/pagination, selection and
inspector panels, and the lightbox's viewport/editor state. Further extractions should
move state and lifecycle together, inject dependencies, and replace cross-owner
writes with methods. Avoid introducing a shared mutable state bag or exporting
private variables just to preserve their old names.

## Lightbox navigation and loading

`VireoLightboxSession` in `static/lightbox/session.js` owns requested and displayed
photo identity, navigation generations, initial-load handoff callbacks, source-swap
timers and image listeners, and the adjacent/original preload cache and queue.
Its factory is inert and returns frozen methods. `lightbox/state.js` constructs the
shared instance with callbacks for source URLs, geometry, cached metadata and the
page's navigation list. Rendering, viewport and editing state stay with their
existing owners.

`begin(photoId)` retires the previous photo's pending callbacks and returns an
opaque request token. Metadata and image completions must check `isCurrent(token)`;
`commit(token)` advances the displayed identity only for the current request.
`close()` invalidates tokens, detaches owned handlers, cancels timers and returns
the last displayed photo so Browse reconciles to what the user actually saw.
Closing and reopening the same photo never revalidates an old request.

Preloads already transferring or decoding retain their slot and estimated bytes
until they settle, even after navigation or close removes them from the cache.
Retired bitmaps are then released; only an open session may resume the queue.
`preloadStatus()` returns frozen diagnostic values without exposing Image objects,
callbacks or writable caches. Constructor options allow controlled browser and
preload-limit dependencies in tests.

The navigation list remains page-owned and live: Browse appends/prepends pages to
it and deletion reconciles it with the grid. The session reads it through the
injected `photos()` callback. This extraction does not change that list's identity
or move viewport, edit-save, flag or upload ownership into the session.

`vireo/tests/lightbox_session.cjs` exercises the complete controller with controlled
images and timers. Browser tests cover real navigation, metadata and image races,
source fallback, preload budgets, and page integrations.

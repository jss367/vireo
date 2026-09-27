# Frontend state ownership

The browse page and shared navbar are being split by responsibility. The first
controllers keep mutable state in factory closures and expose frozen method
objects. Loading a controller script defines its factory without registering
listeners or starting requests. No build tool or framework is required.

| Controller | Owns | Receives from its host |
| --- | --- | --- |
| `VireoBrowseCompare` in `static/vireo-browse-compare.js` | Selection snapshot, pair navigation, zoom and pan, request generations, original-image probes, Escape token and interaction listeners | `open(ids)`, `findPhoto(id)`, `fetch`, `showToast`, and the browser window/Keymap |
| `VireoWorkspaceSwitcher` in `static/vireo-workspace-switcher.js` | Workspace menu visibility, active identity, request generation, dismissal timer, workspace creation dialog | `fetch`, `navigate(path)`, `clearWorkspaceCursors()`, and the browser window |

`browse.js` owns the live browse selection and photo cache. Its Compare entry
point passes the current selection to `open(ids)`, which copies the array.
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
Browse loads its Compare factory before `browse.js`, after the shared helpers.

`tests/e2e/test_frontend_controllers.py` loads complete modules against real browser
DOMs with controlled asynchronous dependencies. It covers stale responses,
selection snapshots, close/reopen cleanup, and the real template entry points.
Existing Compare and workspace-navigation journeys cover ordinary interactions.

Remaining responsibilities include browse query/pagination, selection and
inspector panels, and the navbar's lightbox/editor. Further extractions should
move state and lifecycle together, inject dependencies, and replace cross-owner
writes with methods. Avoid introducing a shared mutable state bag or exporting
private variables just to preserve their old names.

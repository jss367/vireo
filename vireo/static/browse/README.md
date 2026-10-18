# Browse page JavaScript

`vireo/templates/browse.html` (Browse, at `/browse`) is a **module page**
(`frontend-module-pages.json`), like Misses: its scripts are ES modules, not
classic scripts sharing page globals. The template loads one entry module
after the shared navbar, the filter bar and the classic helper scripts it
uses:

```html
<script type="module" src="/static/browse/boot.js"></script>
```

and the browser loads every other file through `boot.js`'s imports. Each file
imports what it uses from its siblings and exports what they use. No bundler
is involved.

| File | Responsibility |
| --- | --- |
| `state.js` | Page state: the `state` object (grid window, selection, scope, sidebar and panel load generations, stacks, colour labels, ...), `browseLimits`, `PREDICTION_COLLAPSE_AT`, and the helpers that keep state consistent (`setBrowseTotals`, `searchLaneSeq`, ...) |
| `shared.js` | The boundary with shared code: every navbar, lightbox, filter-bar, sync-panel and classic-helper global the page uses, and `registerPageHooks()` |
| `markup.js` | The `data-browse-on` handler table and `bindBrowseMarkup()` (see Markup below) |
| `boot.js` | The entry module: composing the filter bar's row, binding the markup and the compare overlay, registering the page hooks, bootstrap, the folder-health subscription, infinite scroll, and the photo deep link |
| `config.js` | The `/api/config` read (`_cfgPromise`), shortcut defaults, `applyBrowseConfig` |
| `loading.js` | The loaded window, paging, `loadPhotos`, `resetAndLoad`, filter changes |
| `grid.js` | Grid rendering, appending and prepending pages, card refreshes |
| `cards.js` | Card markup: fields, badges, colour labels, detection boxes |
| `stacks.js` | Burst stacks: membership, badges, trays, cover reconciliation |
| `anchors.js` | Keeping the selected photo or viewport in place across reloads |
| `scroll.js` | Infinite scroll (`observer`, `infiniteScroll`) and keeping the focused card visible |
| `selection.js` | Selection: click, range, select all, clear, `getActiveSelection` |
| `keyboard.js` | Shortcuts and arrow-key navigation |
| `lightbox.js` | Opening the shared lightbox from the grid and reconciling its events |
| `detail.js` | The single-photo detail panel, metadata, the summary panel, undo/redo |
| `edit-actions.js` | Detail-panel edits and the batch inspector |
| `location.js` | Detail-panel location editing (Google Places, keywords, EXIF suggestion) |
| `batch-bar.js` | The floating batch bar (`batchBarTiming`) |
| `batch-actions.js` | Batch edits over the selection |
| `best-batch.js` | Opening the selection in Best Batch or burst review |
| `selection-panel.js` | The multi-selection panel: paste edits, compare, keyword and wildlife state |
| `selection-panel-state.js` | The selection panel's row snapshots (`selectionPanel`) |
| `selection-panel-events.js` | The panel's delegated action listener (`bindSelectionPanelActions`) |
| `prediction-panels.js` | Prediction panels for the detail and selection views, `afterHistoryChange` |
| `panel-requests.js` | Request lanes for the panels (`panelRequests`, below) |
| `panel-loaders.js` | The typed panel loaders (`panelLoaders`, below) |
| `sidebar-folders.js`, `sidebar-keywords.js`, `sidebar-collections.js` | The three sidebar trees and their loaders |
| `collection-editor.js` | The collection rule editor and its live match count |
| `folder-health.js` | Refreshing Browse when folder health changes |
| `calendar.js`, `view-controls.js`, `layout.js` | The calendar heatmap, the view controls, the resizable panes |
| `context-menus.js` | Right-click menus for cards, folders and collections |
| `export.js` | The export modal, offline copies, full-resolution preparation |
| `keyword-autocomplete.js` | Keyword autocomplete for the keyword inputs |

## State

Page state lives in one exported object, `state` in `state.js`. An ES module
cannot reassign another module's variable, so files write fields
(`state.selectedPhotoId = id`) instead of rebinding variables; a value only
its own module writes stays a module variable, exported read-only where
others read it. `state.js` imports nothing, so it is evaluated before any
other page module. Values browser tests tune are fields of small exported
objects (`browseLimits.maxFocusCandidates`, `batchBarTiming.activateQuietMs`,
`infiniteScroll.observerIsNative`, `locationPick.lastPickedAt`) rather than
constants.

## The shared-code boundary

Page files never reach for shared globals directly (ESLint's `no-undef` fails
on a bare `showToast`); they import them from `shared.js`. Its function
forwarders look the global up at call time, as the classic page did, so a
shared function replaced at runtime (tests stub `window.safeFetch` and
`window.showToast`) is still the one called; controller objects and pure
helpers are bound once. Values the navbar owns are read through getters
(`lightboxSession()`, `editNav()`, `nativeMenuPhotoIdsOverride()`, ...). The
three window writes Browse makes are latches shared scripts own, listed as
the page's `sharedWrites` and made only from `shared.js`: the native menu's
photo override, the navbar's keyboard shortcut config, and the Google Maps
callback name the Maps script calls.

The other direction: `boot.js` registers the functions the shared navbar and
lightbox call (the native menu's photo commands, the lightbox's
rating/flag/label/export menu and Delete, undo/redo) with
`registerPageHooks()`, which hands them to `Vireo.registerPageHooks`; see
`vireo/static/vireo-page-hooks.js` and `types/page-hooks.d.ts`. It also
registers the export modal's callbacks (`applyExportPreset`,
`updateExportPreview`, `buildExportPreflightRequest`, ...), which the shared
`vireo-export-presets.js` and `vireo-export-collisions.js` call on Browse and
the Photo Editor alike. These names are the page's `pageHooks` in
`frontend-module-pages.json`.

## Markup

No markup carries an inline `on*=` handler. Each control names its handlers
in `data-browse-on="event:name"` (several space-separated pairs allowed), and
its arguments in data attributes: `data-browse-arg` for the template's
constants (`setFlag` with `rejected`), `data-id`, `data-cover-id`,
`data-rule-path` and the like for rendered rows. `bindBrowseMarkup(root)`
attaches each handler to the element itself, as the inline handler was, so it
runs at the same point in the event's path; an element is bound once, so
code that renders markup into a container binds that container right after
(`bindBrowseMarkup(grid)`). `boot.js` binds the document once.
`vireo/tests/test_browse_modules.py` holds the handler table to the names the
markup uses. The comparison overlay (`_photo_compare.html`, shared with Cull)
uses `data-compare-control`, bound by `VireoBrowseCompare.bindControls`.

## Load order

Module scripts are deferred: `boot.js` runs after the document is parsed,
after the deferred classic scripts before it, and before `DOMContentLoaded`.
The imported modules evaluate first, dependencies before dependents. Their
top-level code only declares, builds controllers (dialogs, the folder
browser, the panel loaders, the compare controller), and registers listeners
that no other Browse module also registers on the same target and event, so
their evaluation order does not change listener order. Import cycles between
them are safe because no module calls an import while it is being evaluated.

## Tests

Browser tests reach the page's modules through `tests/e2e/module_pages.py`:
the `page` fixture publishes every Browse module as `pageModules.<file>`
(camelCased: `pageModules.selectionPanel`) and the state object as
`browseState` after each navigation to Browse. Node tests run a module's
functions in a VM with the context standing in for `state`.

## Panel requests

Panel request ownership is private to `panel-requests.js`, exported as
`panelRequests`:

| Lane | Owner |
| --- | --- |
| `keywords` | Multi-selection keyword suggestions in `selection-panel.js` |
| `predictions` | Multi-selection predictions in `prediction-panels.js` |
| `detailPredictions` | Single-photo predictions in `prediction-panels.js` |
| `predictionPhotos` | The prediction row's Show photos action |
| `wildlife` | Multi-selection wildlife inclusion state |

`lane.begin(key)` returns a request with `isCurrent()` and `fail()`. Repeating
the same key returns `null`, both during a request and after success. A current
failure releases the key so the same selection can retry; a stale failure
cannot clear a newer request's cache. Omit the key when every call should
supersede the previous one.

After each asynchronous read, check `request.isCurrent()` before changing
the panel. In a catch block, `request.fail()` returns true only for the current
request. `lane.invalidate()` drops pending results and cached selections;
call it when leaving a panel or when a mutation changes its data.

The Show photos action also captures `predictions.observe()` so a selection
change invalidates a pending lightbox open. It checks both owners between
500-photo batches. Single-photo predictions are invalidated when loading a
different detail view or entering batch mode. These lanes govern reads and
rendering; they do not cancel writes or server-side work.

Request races are tested against the actual controllers in
`vireo/tests/browse_panel_requests.cjs`, run by pytest's browser state tests.

## Panel loading and Retry

`/static/vireo-panel-loader.js` owns the read lifecycle; `panel-loaders.js` wires
typed instances for selection keywords, selection predictions, and single-photo
predictions, exported as `panelLoaders`. Existing `loadSelectionKeywordSuggestions`,
`loadSelectionPredictions`, and `loadDetailPredictions` entry points forward to
these instances so their callers retain the same API.

A current failure shows a named Retry button inside that panel. The controller
owns a private copy of the failed input and an observation of its request lane
after failure. Retrying requires both that observation and the current selection
or photo to match; changing or invalidating the panel retires its retry. Starting
a retry immediately removes the old action, paints loading, and clears stale
row state. Keyboard focus moves to the container while it loads, without a late
response moving focus back from another control.

Panel ownership uses `getBrowseSelection()`, which reads the actual grid selection.
`getActiveSelection()` additionally honors temporary native-menu action targets;
that override must not cause a still-current panel response to be discarded.

The loader and its Browse wiring use strict type, undefined-name, and unused-name
checks. The selection response fields are typed; nested detail prediction payloads
remain opaque at the legacy renderer boundary. Browser recovery tests in
`tests/e2e/test_browse_panel_retry.py` run in Chromium and Mac WebKit CI.

## Releasing local originals

The photo selection menu and folder context menu offer **Release Local
Originals…**. The folder action includes visible subfolders. Storage offers
the same action for all cached originals across every workspace; these copies
are shared rather than owned by one workspace.

`/static/vireo-release-originals.js` owns the confirmation and job stream on
both pages. The `release-local-originals` background job checks readable source
files before removing managed original, companion, and XMP copies. Source
images must match the cached size and modification time. Missing or changed
source images retain their copies, with reasons in the job details. A newer
source XMP is allowed; a missing source XMP retains its cached snapshot.
Other generated assets, photo metadata, and processing results remain intact.

Release and preparation share the per-photo preparation guard. Release checks
the selected source identity again under a catalog writer transaction before
removal, and pauses or cancels between photos. Results report released, kept,
already released, failed, and unchecked counts plus the removed file bytes.

## Selection-panel state and actions

`selection-panel-state.js` owns the keyword and prediction row snapshots in
`selectionPanel`. Keyword ids and prediction row indexes resolve
through this store; photo-id arrays are copied and frozen. Starting a new
suggestion request, leaving batch mode, or exceeding the selection cap clears
the old rows immediately. Prediction resets also retire the payload used by
Show more, while keeping the user's expanded/collapsed viewing preference.

`selection-panel-events.js` binds one delegated click listener to
`#selectionPanel` through `bindSelectionPanelActions()`, called from `boot.js`
before bootstrap. Rendered buttons carry `data-selection-action` and a keyword id or
prediction row index. Species names and photo-id arrays stay in the store.
The listener ignores disabled buttons, detached elements and missing rows.
Rebinding is safe and does not install a second listener.

`selection-panel.js` renders keyword and wildlife controls and applies their
changes; `prediction-panels.js` renders prediction controls and performs their
actions. Existing grid selection and single-photo detail state still live in
`state.js`; these are separate from the batch panel's row snapshots.

## Dialog focus and keyboard lifecycle

Collection editing, batch keyword entry, and batch collection selection use
`VireoDialog.create(overlay, {initialFocus, onDismiss})` from
`/static/vireo-dialog.js`, a classic script loaded before the page's modules.
The templates provide `role="dialog"`, `aria-modal`, and a title via
`aria-labelledby`. The controller moves focus inside on `open()`, recomputes
the Tab order as controls change, makes the background inert, and restores the
opener on `close()`. It preserves pre-existing inert state and uses Keymap's
shared Escape stack and scroll locks, including when managed dialogs stack.

Each owner's `onDismiss` calls its normal hide/cleanup function, which calls
`close()`. Save, Cancel, Escape, and other close paths must use that same function;
removing the `open` class directly would leave focus/inert/scroll state behind.
`open(returnFocus)` accepts an explicit opener for actions that fetch data before
opening and for pointer clicks (WebKit may not focus the clicked button). Focus is
synchronous, so no delayed callback can steal it after closing.
Existing batch collection choices are native buttons with `aria-pressed` state.

The lifecycle follows the [W3C modal dialog pattern](https://www.w3.org/WAI/ARIA/apg/patterns/dialog-modal/).
Real-DOM and Browse integration regressions live in
`tests/e2e/test_dialog_accessibility.py` and run in the Linux/Windows Chromium and
Mac WebKit PR browser gate.

# Misses page JavaScript

`vireo/templates/misses.html` (Misses, at `/misses`) is the first **module
page** (`frontend-module-pages.json`): its scripts are ES modules, not classic
scripts sharing page globals. The template loads one entry module after the
shared navbar scripts and the shared filter bar:

```html
<script type="module" src="/static/misses/boot.js"></script>
```

and the browser loads every other file through `boot.js`'s imports. Each file
imports what it uses from its siblings (`import { state } from './state.js'`)
and exports what they use. No bundler is involved; the files are served as
they are.

| File | Responsibility |
| --- | --- |
| `state.js` | Page state: the `state` object (`missesData`, the tuning config and preview state, focus, `selection`, lightbox tracking, `appliedMissFilters`, the fail-closed scope error) and the constants `CATEGORY_ORDER`, `CATEGORY_LABELS`, `MISS_TUNING_CONTROL_IDS` |
| `shared.js` | The boundary with shared code: every navbar, lightbox, filter-bar and `vireo-utils.js` global the page uses, and `registerPageHooks()` |
| `helpers.js` | `photoThumbnailUrl`, `parseDetectionBox`, `sortPhotos`, and `getSinceParam` |
| `filters.js` | Reading, cloning and comparing the filter-bar payload, adding it to request bodies, and `applyMissFilters()` |
| `scope.js` | Legacy `?collection_id=` / `?folder_id=` deep links, `noteLegacyScopeError`, and the scope banner |
| `tuning.js` | The threshold sliders and labels, tuning status and disabled state, `loadMissConfig`, and preview, recompute and reset |
| `loading.js` | `applyMissesPayload`, `loadMisses()` with its sequence guard, and `afterHistoryChange` |
| `render.js` | `render()`, each category section and its cards, and section collapse |
| `card-events.js` | The delegated click and double-click listeners on `#missesRoot` for section headers, Reject all buttons, cards and their unflag buttons |
| `photo-open.js` | Opening a miss in the lightbox or Browse, the focused/selected photo lookups, and `pruneSelectionToVisible` |
| `flagging.js` | Bulk reject per category, per-card and selection flag changes, per-card and lightbox unflag, and the Representative badge clearing they share |
| `selection.js` | Clearing and toggling the selection, the selection bar, shift-click ranges, and card click and double-click |
| `context-menu.js` | The right-click menu for miss cards |
| `delete.js` | Deleting the selected photos through the shared delete dialog |
| `keyboard.js` | `setFocused`, `moveFocus`, the keydown handler, and Shift+J/K selection extension |
| `badges.js` | Keeping Representative badges and the applied flag filter current after `lifelist:changed` and `lightbox:flagchanged` |
| `boot.js` | The entry module: registering the page hooks, binding every listener, the slider handlers, `VireoFilter.init()`, the legacy scope resolution, the config load, and the first load |

## State

Page state lives in one exported object, `state` in `state.js`. An ES module
cannot reassign another module's `let`, so files write fields
(`state.previewActive = true`) instead of rebinding variables. Constants are
plain exports. Nothing the page declares is a global: a top-level `var` or
`function` in a module stays in that module.

## The shared-code boundary

The navbar, lightbox, filter bar and `vireo-utils.js` are still classic
scripts that publish globals. Page files never reach for them directly
(ESLint's `no-undef` fails on a bare `showToast`); they import them from
`shared.js`, which is the one list of what the page uses from shared code.
The forwarders there look the global up at call time, as the classic page
did, so a shared function that is missing keeps its `typeof` guard there
(`hasLightbox()`, `hasContextMenu()`, `hasDeleteDialog()`, and `showToast`
that no-ops) and one replaced at runtime is still the one called. The escape
helpers, which `_navbar.html` loads first and nobody replaces, and
`VireoFilter`, which `_filterbar.html` loads before the page, are bound once.
When a shared script becomes a module, change its import in `shared.js`.

The other direction: the shared navbar and lightbox call a few page
functions. `boot.js` hands them to `registerPageHooks()` in `shared.js`, which
registers them with `Vireo.registerPageHooks` (`vireo-page-hooks.js`); shared
code asks for them with `Vireo.pageHook(name)` instead of looking for a global:

- `afterHistoryChange` — the navbar's undo/redo reload
- `loadMisses` — the Missing Originals delete refresh in `navbar-folder-banners.js`
- `getActiveSelection` — the native menu's selected photos
- `openBrowseShortcutPhoto` — the native menu's Open in Lightbox
- `handleMissesLightboxFlagShortcut` — the lightbox's unflag key

These names are the page's `pageHooks` in `frontend-module-pages.json`, which
`vireo/tests/test_module_pages.py` checks against the registration. The page
writes no `window` globals at all: `sharedWrites` is empty, and ESLint rejects
any `window` write in the page's files. (The lightbox's click-to-zoom latch,
which this page used to reset before opening, is now reset by the lightbox
itself when it closes.)

## Load order

Module scripts are deferred: `boot.js` runs after the document is parsed and
before `DOMContentLoaded`, which is where the classic scripts at the end of
the body ran too, so every element the page looks up exists. The browser
evaluates the imported modules first, dependencies before dependents; they
only declare functions and initialize passive state, and import cycles between
them (render ↔ selection ↔ keyboard, ...) are safe because no module calls an
import while it is being evaluated. Boot then:

1. registers the page hooks (the classic page had them as globals from parse
   time, so this goes first),
2. binds the controls that used to carry `onclick=` attributes (the selection
   bar's four buttons, Recompute and Reset preview) and the delegated card
   listeners (`card-events.js`),
3. binds the listeners the original inline script attached at load, in its
   order: `bindMissLightboxTracking()`, `bindMissContextMenu()`,
   `bindMissKeyboard()`, `bindMissLifeListEvents()`,
   `bindMissLightboxFlagEvents()`,
4. runs `installMissSliderHandlers()` and the `VireoFilter.init()` chain.

A new file is loaded by importing it from the file that uses it; a file
nothing imports never runs (`test_module_pages.py` fails on one).

## Rendered markup

`render.js` builds the cards as HTML strings with data attributes
(`data-cat`, `data-idx`, `data-photo-id`), not inline handlers.
`card-events.js` resolves a click to the innermost of unflag button, Reject
all button, card, section header, which is what the inline handlers did: a
button's handler stopped propagation, the card and header handlers ignored
clicks inside a button, and a disabled button ran nothing.

## Tests

Browser tests cannot read `selection` or call `previewMisses()` as globals.
`tests/e2e/module_pages.py`'s `import_page_modules()` imports the page's
modules from the test (`import()` returns the instances the page loaded), and
the Misses tests read `missState` and call `pageModules.<file>.<export>`.

## Guards

Keep these intact when changing this code:

- `loadMisses()` and `previewMisses()` share `state.missLoadRequestId`; a
  response for an older request is dropped. `state.previewRequestId` does the
  same for debounced slider previews, and `invalidateMissPreview()` bumps it.
- `state.appliedMissFilters` is the filter snapshot that produced the cards
  on screen. Recompute and bulk reject send that snapshot, not the live
  filter bar, so they act on the photos the user is looking at.
- `state.legacyScopeError` / `state.legacyScopeRecoveryPending` fail closed:
  an unresolvable deep link renders an empty grid and freezes the tuning
  controls (`MISS_TUNING_CONTROL_IDS`) until a replacement filter has loaded.
  Preview, recompute and reset all return early while either is set.
- `state.previewActive` disables bulk reject, per-card unflag and the
  selection bar's unflag button; the preview endpoint does not persist flags.
- `pruneSelectionToVisible()` runs after every synchronous mutation of
  `state.missesData` so a follow-up bulk flag cannot act on cards no longer
  shown.

# Shared photo lightbox

The lightbox is a set of ES modules in this directory, part of the shared
navbar's module graph: `vireo/static/navbar-boot.js` imports every one of them
(see CLAUDE.md, "`_navbar.html`"). Each file imports what it uses from its
siblings and from the navbar modules, and exports what they use. The session
controller encapsulates navigation identity and asynchronous loading resources.
The viewport controller owns zoom, pan, saved views and interaction lifecycle.

| File | Responsibility |
| --- | --- |
| `shared-state.js` | `lbState`, the lightbox state more than one module writes (imports nothing, so it evaluates first) |
| `session.js` | `VireoLightboxSession`: factory owning photo identity, request tokens, load callbacks, timers and preloads |
| `viewport.js` | `VireoLightboxViewport`: factory owning viewport geometry, saved views, eye alignment, zoom controls and interaction listeners |
| `state.js` | Controller wiring (`vireoLightboxSession`, `vireoLightboxViewport`, `lbViewportHost`), shared render/edit caches, and read-only guards |
| `curation.js` | Species representatives and highlights |
| `edits.js` | Render URLs, RAW/JPEG source selection, edit recipes, adjustment previews and saves |
| `flags.js` | Confirmed, pending, and provisional review flags |
| `overlays.js` | View-menu preferences (`lbOverlays`), detections, eye markers, and masks |
| `crop.js` | Crop-editor interaction and saves |
| `navigation.js` | Opening photos, navigation, metadata loading, and fullscreen |
| `controls.js` | Zoom button adapters, closing, and context menus |
| `delete.js` | Deletion confirmation, job progress, and lightbox reconciliation |
| `inat.js` | iNaturalist upload/export queues and modal lifecycle |
| `keyboard.js` | Lightbox keyboard handling (`bindLightboxKeyboard()`) and the shortcut helpers |
| `source-loading.js` | Image-load status (`lbDetailStatus`), source selection and source-tier presentation |

## State

An ES module cannot assign another module's variable. State that more than
one module writes lives on `lbState` in `shared-state.js`
(`lbState.visualTransitionPending = true`); a module that alone writes a
variable keeps it as a plain module variable and exports it, and importers read
its current value through the live binding. `lbOverlays` (the view-menu
preferences and the mask URL) and `lbDetailStatus` (the detail chip's timing
and state) are objects for the same reason: browser tests set them.

## Load order and listeners

Modules evaluate in dependency order: a module's imports run before it, and
import cycles between lightbox files are safe because no module calls an
import while it is being evaluated (top-level code only declares, reads its own
module, or reads `lbState`). Listeners that share an event with another navbar
module's and may stop it reaching the next (the lightbox's document `keydown`
handlers) are not attached at evaluation: `navbar-boot.js` calls
`bindLightboxKeyboard()` after the command palette's and the shortcuts sheet's
binders, the order the classic scripts attached them in.

Preserve request generation checks when moving behavior: requested photo
identity and the photo currently visible on screen can differ during
navigation. Edit-save and upload ownership checks similarly prevent stale
completions from changing newer state.

`navbar-lightbox-subjects.js` is a separate integration module of the navbar.
Page-specific integrations, such as `browse/lightbox.js`, run after the navbar
and reach the lightbox through `navbar-globals.js` (classic pages) or by
importing it (module pages).

## Tests

Pure-helper and state-regression tests (`vireo/tests/navbar_pure_functions.cjs`,
`browser_state_regressions.cjs`, `lightbox_session.cjs`) lift functions out of
these files and run them in `node:vm`; `tests/frontend/module-source.mjs`
strips `import`/`export` for that. Browser tests reach the modules' internals
through `tests/e2e/navbar_modules.py`, which the e2e `page` fixture runs after
every navigation: it imports the page's own module instances and defines window
accessors for the exports and for the old global names of state that moved
onto `lbState`, `lbOverlays` and `lbDetailStatus`. A test that replaces
`vireoLightboxSession` or `vireoLightboxViewport` goes through
`replaceLightboxControllers()`; one that stubs a viewport host callback
replaces it on `lbViewportHost`.

## Page hooks

The lightbox is on every page, but some of its actions belong to the page
that opened it: a rating or flag goes through the page's own model so its
grid stays current, Delete removes the photo from the page's grid, and a page
may open photos read-only. The lightbox asks for those functions with
`Vireo.pageHook(name)` (`vireo/static/vireo-page-hooks.js`) and falls back to
the shared API when the page registered none. It never looks for a page
global by name. The hooks it uses:

| Hook | Used by |
| --- | --- |
| `setRatingFor` / `setReviewRating` | The context menu's rating chips |
| `setFlagFor` / `setReviewFlag` | `_lbApplyFlag` (flag buttons, keys, context menu); without one it POSTs the shared flag endpoint |
| `handleMissesLightboxFlagShortcut` | The flag keys and buttons on Misses, which keep unflag-as-miss local |
| `setColorLabelFor`, `openPhotoExportModal`, `copyPhotoPaths`, `revealPhoto` / `revealReviewPhoto` | Context-menu items, shown only when registered (reveal falls back to the API) |
| `setWildlifeExcludedFor` | The context menu's wildlife toggle |
| `removeDeletedLightboxPhoto` | Delete: drops the photo from the page's grid before the lightbox moves on |
| `getLightboxOpenOptions` | The read-only state for the next photo after navigation or Delete |
| `getLightboxBrowseDisabledHint` | Edit Photo and Open in Browse, disabled while the page has unsaved work |
| `getDetailPhotoId` | Refreshing the page's detail-panel image after an edit |
| `loadHighlights` / `updateHighlightsLightboxControls` | Reloading Highlights after a curation change |

`types/page-hooks.d.ts` gives each hook's signature, and
`tests/e2e/test_page_hooks.py` lists which page registers which.


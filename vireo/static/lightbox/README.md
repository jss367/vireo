# Shared photo lightbox

`vireo/templates/_navbar.html` loads these classic scripts in order at the
lightbox's original position, after its markup and before page scripts. The
session controller encapsulates navigation identity and asynchronous loading
resources. The viewport controller owns zoom, pan, saved views and interaction
lifecycle. Other responsibilities still share existing functions and state while
page integrations migrate to explicit owners.

| File | Responsibility |
| --- | --- |
| `session.js` | Factory owning photo identity, request tokens, load callbacks, timers and preloads |
| `viewport.js` | Factory owning viewport geometry, saved views, eye alignment, zoom controls and interaction listeners |
| `state.js` | Controller wiring, shared render/edit state, and read-only guards |
| `curation.js` | Species representatives and highlights |
| `edits.js` | Render URLs, RAW/JPEG source selection, edit recipes, adjustment previews and saves |
| `flags.js` | Confirmed, pending, and provisional review flags |
| `overlays.js` | View-menu preferences, detections, eye markers, and masks |
| `crop.js` | Crop-editor interaction and saves |
| `navigation.js` | Opening photos, navigation, metadata loading, and fullscreen |
| `controls.js` | Zoom button adapters, closing, and context menus |
| `delete.js` | Deletion confirmation, job progress, and lightbox reconciliation |
| `inat.js` | iNaturalist upload/export queues and modal lifecycle |
| `keyboard.js` | Lightbox keyboard handling and shared shortcut helpers |
| `source-loading.js` | Image-load status, source selection and source-tier presentation |

Keep load-time dependencies in the same file or an earlier script. Functions
called only after initialization can refer to later scripts. Preserve request
generation checks when moving behavior: requested photo identity and the photo
currently visible on screen can differ during navigation. Edit-save and upload
ownership checks similarly prevent stale completions from changing newer state.

`navbar-lightbox-subjects.js` remains a separate integration loaded later in the
navbar. Page-specific integrations, such as `browse/lightbox.js`, load after the
navbar. Existing pure-helper, state-regression, and browser tests exercise the
scripts through these entry points.

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


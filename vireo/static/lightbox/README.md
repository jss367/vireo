# Shared photo lightbox

`vireo/templates/_navbar.html` loads these classic scripts in order at the
lightbox's original position, after its markup and before page scripts. They
share the existing global functions and variables so inline event handlers and
page integrations keep working. This split organizes the code; it does not yet
encapsulate the shared state.

| File | Responsibility |
| --- | --- |
| `state.js` | Shared photo identity, render caches, request generations, and read-only guards |
| `curation.js` | Species representatives and highlights |
| `edits.js` | Render URLs, RAW/JPEG source selection, edit recipes, adjustment previews and saves |
| `viewport-memory.js` | Per-photo viewport restoration and eye-tracking anchors |
| `flags.js` | Confirmed, pending, and provisional review flags |
| `overlays.js` | View-menu preferences, detections, eye markers, and masks |
| `crop.js` | Crop-editor interaction and saves |
| `navigation.js` | Opening photos, navigation, metadata loading, and fullscreen |
| `controls.js` | Zoom controls, pointer/resize listeners, closing, and context menus |
| `delete.js` | Deletion confirmation, job progress, and lightbox reconciliation |
| `inat.js` | iNaturalist upload/export queues and modal lifecycle |
| `keyboard.js` | Lightbox keyboard handling and shared shortcut helpers |
| `viewport.js` | Display dimensions, transforms, zoom geometry, and pan limits |
| `source-loading.js` | Image-load status, preloading, and source-tier swaps |

Keep load-time dependencies in the same file or an earlier script. Functions
called only after initialization can refer to later scripts. Preserve request
generation checks when moving behavior: requested photo identity and the photo
currently visible on screen can differ during navigation. Edit-save and upload
ownership checks similarly prevent stale completions from changing newer state.

`navbar-lightbox-subjects.js` remains a separate integration loaded later in the
navbar. Page-specific integrations, such as `browse/lightbox.js`, load after the
navbar. Existing pure-helper, state-regression, and browser tests exercise the
scripts through these entry points.

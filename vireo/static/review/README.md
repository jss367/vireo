# Review page JavaScript

`vireo/templates/review.html` (Review Predictions, at `/review`) loads these
classic scripts after the shared navbar scripts, where its inline page script
used to be. Functions remain global for the page's inline event handlers and
the card markup's `onclick` attributes.

This is not Pipeline Review. `pipeline_review.html` (`/pipeline/review`) has
its own scripts in `vireo/static/pipeline-review/`. Both pages have a Group
Review burst modal with similarly named `grm*` functions, but the two are
separate implementations: a fix in one does not reach the other.

| File | Responsibility |
| --- | --- |
| `state.js` | Page state: the loaded predictions and the active view selections |
| `detection-boxes.js` | Show/Hide Boxes, species box colors, and re-syncing boxes after an edit |
| `collections.js` | The collection picker and `switchCollection()` |
| `loading.js` | `loadPredictions()`, its sequence guard, and the reload flag that gates decisions |
| `deep-links.js` | `?model=`, `?labels_fingerprint=` and `?photo_id=` deep links and their filter pills |
| `controls.js` | `renderAll()`, the toolbar (confidence, model, sort, photo size), stats, status tabs, and Accept All |
| `grid.js` | Filtering and sorting the grid, prediction cards, burst-group buttons, and grid clicks |
| `decisions.js` | Accept, reject, accept an alternative, and Accept All |
| `keyboard.js` | The accept/skip shortcuts for the first visible pending card |
| `photo-actions.js` | Rating, flag, reveal, copy path, lightbox, and keeping Representative badges current |
| `context-menu.js` | Right-click menus for review cards and Group Review cards |
| `group-session.js` | `grmState`, opening and closing Group Review, and loading a burst |
| `group-render.js` | Group Review zones and cards, selection, and the loupe's photo and predictions |
| `group-resolution.js` | Group Review view state, thumbnail size, image resolution, and loupe zoom and 1:1 |
| `group-loupe.js` | Loupe lock, eye crosshair, hover zoom, align drags, card transforms, and strip 1:1 |
| `group-cards.js` | Card pan drags, selection clicks, offset resets, and box sharpness |
| `group-changes.js` | Zone moves, the Apply label, applying the burst, and the modal's keyboard |
| `boot.js` | The shortcut config fetch, the bootstrap requests and filter bar, and every listener |

Load `boot.js` last. The other files declare functions and initialize state;
two initializers run at load and each depends only on its own file:
`reviewDetectionBoxesVisible` (`detection-boxes.js`) reads localStorage through
`_reviewStoredBool`, and `GRM_CARD_W`/`GRM_CARD_H` (`group-resolution.js`) come
from `_grmInitialThumbSize()`. Boot runs the page's load-time work in the order
the inline script did: the boxes button's `DOMContentLoaded` handler, the
`/api/config` fetch that sets `_shortcuts`, the bootstrap (view preferences,
sort, photo size, deep-link params, `loadPredictions()`,
`loadCollectionFilter()`, `VireoFilter.init()`), the edit-history listener,
then the `bind*()` functions and the resize listener. The scripts sit before
the Group Review modal's markup, so nothing may look up a `grm*` element at
load. Adding a script requires an explicit template tag before boot. These are
not ES modules and need no bundler.

Page-wide state lives in `state.js`. `predictions` is the collection-filtered
view of `allPredictions` and holds the same objects, so a status written to a
row shows in both, but a burst member outside the collection is only in
`allPredictions`: decision handlers walk both lists. Domain state stays beside
its functions: the load and collection epochs in `loading.js` and
`collections.js`, `_shortcuts` in `keyboard.js`, `grmState` in
`group-session.js`, and the Group Review zoom, pan-offset and drag state
(`_grm*`, `GRM_*`) in `group-resolution.js`, shared by the session, loupe and
card files.

This page records prediction review decisions. Keep these guards intact when
changing this code:

- `loadPredictions()` owns `_loadPredictionsEpoch`. A response for an older
  epoch is dropped, so the unfiltered bootstrap fetch can't overwrite the
  filtered one that `VireoFilter.init()` starts.
- `_predictionsReloading` is true while a load, or a collection switch's
  membership fetch, is in flight. `renderButtons()` shows Accept All as
  "Reloading…", and every decision entry point returns early: the card buttons,
  the alternatives, Accept All, and the A/S keys.
- Each decision captures `_loadPredictionsEpoch` on entry and checks
  `_predictionEpochStale(epoch)` after its request. A stale accept or reject
  leaves local state alone, and Accept All stops its loop.
- `switchCollection()` owns `_switchCollectionEpoch` and drops superseded
  membership responses. The edit-history listener goes through
  `switchCollection()`, not `loadPredictions()`, so the membership snapshot is
  refreshed too.
- Accept and reject mark every row the response names, burst siblings
  included. Accept All uses `Vireo.predictions.groupedDecisionTracker()`
  (`vireo-predictions.js`) so it doesn't re-send a row an earlier accept in the
  same run already decided.
- `grmApply()` sends the statuses the modal displayed as `observed`. When the
  server reports `already_decided`, it reloads instead of patching statuses
  locally.
- The A/S handler (`keyboard.js`) ignores text inputs, browse mode, a missing
  shortcut config, a reload in flight, and any open overlay, Group Review
  included. The Group Review handler (`group-changes.js`) acts only while
  `#grmOverlay` is open and the lightbox is not. Boot registers the A/S handler
  first; keep both guard lists and that order.

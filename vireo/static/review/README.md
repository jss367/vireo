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
| `identity-alternatives.js` | Catalog-backed unresolved identities, evidence, and reversible single-species resolution; independently binds after DOMContentLoaded |
| `queue-state.js` | Browser-local, workspace-and-filter-scoped resume state; unique-photo inspection progress, separate from catalog flags |
| `queue-focus.js` | Upright full-frame/manual center and native 512-pixel crop inspection; source and pixel-scale headers |
| `queue.js` | Sequential per-photo inspection, retained subjects and explicit conflict-checked catalog flag changes |
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

## Unresolved species alternatives

The independent `identity-alternatives.js` panel loads workspace-wide review
groups from `GET /api/identity-alternatives`, including when classification
has no prediction rows. Each group contains `group_id`, `photo_ids`,
`candidates`, `reason`, and `missing_evidence`; the response also names its
`workspace_id`. The form's photo chooser takes
the displayed cards and their burst members within the current collection;
the user explicitly selects the frames before saving.

`POST /api/identity-alternatives` creates a group. Updating an existing group
also sends its observed `group_id`; stale membership returns 409. Resolving
through `POST /api/identity-alternatives/resolve` sends that group ID, a
`photo_ids` subset, and one candidate `species`. Each group's resolution chooser starts with
no frames selected; candidate buttons become available after selecting frames.
The explicit Select all frames to resolve button includes the whole group.
Unselected frames retain their alternatives and evidence in unresolved review.
Both writes carry `observed_workspace_id`; under the writer lock the request
checks both its cached workspace and the persisted latest activation. A switch
in another tab returns 409 without touching tags or review state. The page pins
its initial workspace and disables edits if a refresh detects a switch.
Catalog mutations use the shared decision lock. After commit, the refresh
visits every workspace containing the affected photos (including photo-only
grants), takes only that cache's regroup lock, and rereads global species.
Undo and redo use the same refresh; a failed cache does not prevent attempts
to update the remaining workspaces. The catalog's `identity_alternatives` edit
history action replays the uncertainty and candidate-keyword delta together,
including XMP keyword intents and original association provenance. Other
confirmed species and flags are outside that delta.
Keyword intents are reconciled in every workspace containing the photo, so
syncing a sibling workspace cannot replay an older opposite decision. Each
queue retains an explicit final intent, with captured-write compensation and
immutable-token cleanup handled by the normal pending-change helpers.
An identity history entry superseded by a newer decision or candidate-tag
change is retired after replay rolls back; the request returns 409 and the
user reviews the next history action. Visibility loss and transient failures
retain the entry for retry.

Candidates and evidence are workspace review state in the catalog, like
prediction review decisions; they are not written as confirmed species
keywords. Use the panel's explicit Resolve controls to close the uncertainty.
Its groups stay separate from the prediction status tabs and genuine
multi-species confirmations. The script binds itself after DOMContentLoaded,
refetches after edit-history changes, and drops stale GET responses.

## Photo action feedback

Rating and flag helpers return promises resolving to a confirmed boolean result.
They own their success/error toast, including calls from the native Photo menu
and shared lightbox. Callers must not announce success before the promise settles.
An undo/redo `history_busy` rejection retains the shared transport's warning
without adding a second notification. Only a confirmed reject clears local
Representative badges.

`enqueueReviewPhotoAction` keeps pending actions private. Writes serialize per
photo and field; consecutive identical pending choices share one request and
notification. Other photos and fields remain independent. A failure releases
the queue so the user can retry. Copy Path uses one ordered lane across photos
and confirms only after a nonempty path has actually reached the clipboard.
Lookup failures, missing paths and unavailable/denied clipboard access show errors.

`tests/frontend/review-photo-actions.test.mjs` exercises complete action helpers,
request ordering, duplicate clicks, failed confirmations, badge preservation,
clipboard completion and native-menu adapters.
`tests/e2e/test_review_photo_actions.py` covers real grid, Group Review, lightbox
and native-menu entry points, failed requests and retry recovery in Chromium
and WebKit.

## Resumable photo inspection

The optional side-by-side inspection panel includes every matching photo,
including burst members that the grid collapses. A photo with several matching
predictions is counted once. `rqScopeKey()` includes workspace, collection,
model, status tab, confidence, label fingerprint, deep-linked photo and the
universal filter's effective rules and visual clause. Sort changes retain the
photo anchor. The universal filter already persists its expression per workspace;
`vireo:review-view:<workspace>` remembers the additional Review controls and
the effective filter expression alongside the photo anchor. Explicit filter
deep links take precedence over that snapshot.

`vireo:review-queue:<workspace>:<scope JSON>` stores the selected photo and
per-photo inspection/follow-up, subject and normalized manual center. It never
stores flag or prediction decisions. These notes are explicitly local to this
browser and scope, not a durable library-wide review ledger. Storage failure is
visible. Merely entering or navigating the queue does not count as inspection;
Mark inspected, Needs follow-up, or a successful explicit flag write does.
Decided counts catalog keep/reject flags at the last refresh; follow-up remains
independent and can overlap prior decisions.

Subjects are retained detector boxes, with a local selection that leaves the
saved primary unchanged. The crop uses the chosen box's center or a manual
center in the upright, unedited source frame. It does not run or imply new RAW
analysis. A native crop is not enlarged or fitted; a narrow panel scrolls.
Source, decoded raster dimensions and decoded-to-original scale come from the
existing focus-crop response headers; cached box analysis is described as cached,
without claiming current analysis availability. Missing subjects/crop pixels
are visible and manual centering remains available with a usable full frame.

Flag changes reuse the normal undoable single-photo endpoint with
`observed_flag` and `observed_workspace_id`. The flag baseline is compared under
the existing writer lock; a stale change returns 409 and the user must refresh
before choosing again. Every filter/catalog reload invalidates in-flight
inspection responses. Response epochs also prevent a late crop from replacing
the current photo.


Archive collision reconciliation carries alternative candidates, evidence,
revision markers and identity undo/redo items onto the surviving photo before
removing the duplicate row. Identical batch snapshots collapse to one item.
Conflicting identity revisions, species confirmations or snapshots stop catalog
reconciliation with an explicit error and roll back its photo changes; neither
row's evidence is silently chosen or discarded. Review both rows before retrying.

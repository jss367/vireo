# Review Photo Locations page JavaScript

`vireo/templates/location_review.html` (Review Photo Locations at
`/locations/review`) loads these classic scripts after the shared navbar,
lightbox, and sync-panel scripts. The page used to run as one `'use strict'`
IIFE; each file now starts with its own `'use strict'` directive, and its
functions and the `state` object are page globals. None of these names is
defined by the navbar or lightbox scripts. Check before adding a new top-level
name, because a function declared here would replace a shared global with the
same name.

| File | Responsibility |
| --- | --- |
| `state.js` | `state`: the review source, the options read from the URL, the group queue, candidates, the map, and the assignment |
| `format.js` | Number, date, capture-range, and distance formatting, and `distanceBetween()` |
| `source.js` | Back to Browse, `parseSource()`, the collection picker, changing review options, and pruning a deleted photo from a selection source |
| `queue.js` | The current group, its facts and recomputed metadata, the progress bar, the empty state, `renderCurrentGroup()`, and Previous/Next |
| `assignment.js` | Assignment progress, the navigation lock, deletion reconciliation, place hydration and normalization, and `assignCurrentGroup()` |
| `photos.js` | Thumbnails, the time-review sample with split and review-separately, and the lightbox preview |
| `map.js` | Loading Google Maps or Leaflet, `initMap()`, photo and candidate markers, and the Google place search box |
| `candidates.js` | Place-type tables, candidate merging, grouping, and filtering, `selectChoice()`, `renderCandidates()`, and the suggestion-mode buttons |
| `suggestions.js` | Saved-location suggestions, Google nearby search and reverse geocoding, broader areas, and the time-review saved list |
| `discrepancies.js` | GPS discrepancy review: photo selection and keeping or correcting GPS |
| `initialize.js` | `initialize()`: applies the URL options, loads collections, config, and the map, then requests the preview |
| `boot.js` | Binds every control and the `lightbox:photodeleted` listener, then calls `initialize()` |

Load `boot.js` last. The other files only declare functions and initialize
passive state (`state` reads the URL and `sessionStorage`, and
`candidates.js` builds the place-type tables), so their relative order does
not matter at load time. Boot binds the controls in the order the inline
script used to, then calls `initialize()`. Adding a script requires an
explicit template tag before boot. These are not ES modules and need no
bundler.

Keep these ordering and locking rules intact when changing this code:

- `renderCurrentGroup()` bumps `state.renderToken`. Saved and Google
  suggestion responses (`loadSavedSuggestions`, `loadGoogleSuggestions`,
  `finishSuggestionRender`) for an older token are dropped, so a slow lookup
  can't paint candidates onto the next group. Google suggestions load once
  per group (`state.googleCandidatesLoaded`).
- `state.isAssigning` (a batch is in flight) and
  `hasPartialAssignmentProgress()` (a batch failed after committing some
  chunks) lock everything that would change the group or the choice: source
  and option changes, Skip, Previous/Next, suggestion modes, split and
  review-separately, choosing a different candidate, and opening the
  lightbox. Any new control that changes the group or the choice must check
  both.
- `renderCurrentGroup()` sets `state.assignment` to null. Never call it while
  a partial assignment is pending, or Retry would resubmit chunks that are
  already committed. The `lightbox:photodeleted` handler in `boot.js` updates
  the group quietly in that case.
- `assignCurrentGroup()` snapshots the group's photo ids into
  `state.assignment.photoIds` and tracks sent ids in `processedIds`. Each
  chunk is rebuilt from set membership, and `reconcileAssignmentForDeletion()`
  removes a deleted id from the snapshot so a retry can't 404 the batch.
- Assigning and resolving discrepancies close an open lightbox first, so a
  delete there can't reset the assignment mid-batch.

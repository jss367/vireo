# Browse panel requests

Browse loads classic scripts in the order declared by `browse.html`, with
`boot.js` last. Existing page state is in `state.js`. Panel request ownership
is private to `panel-requests.js`, exposed through `Vireo.browse.panelRequests`:

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

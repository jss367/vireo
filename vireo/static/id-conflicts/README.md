# ID Conflicts page JavaScript

`vireo/templates/id_conflicts.html` (ID Conflicts, at `/id-conflicts`) loads
classic scripts from `vireo/static/id-conflicts/` by responsibility, deferred
behind the navbar's module entry, with `boot.js` last. They share page
globals, as the inline script did; browser tests read and set some of them
(`compareRows`, `compareTotal`, `comparePerPage`, `activeFilter`) and call
`loadComparison`, `reloadRows` and `effectiveSummary` directly.

The comparison is derived on the server (`vireo/id_conflicts.py`); the page
asks for one page of rows plus the counts and renders what it is told.

| File | Responsibility |
| --- | --- |
| `state.js` | Shared state (the page of rows, paging, models, chips, the selection, the request sequences and decision queue) and the saved conflict threshold, read while the script loads |
| `loading.js` | `jsonFetch` (keeps whether the server answered on the error), collections, the models shown, building and fetching a page of rows, `loadComparison` and `reloadRows` |
| `controls.js` | The search box, paging, page size, and the model and sort selects |
| `render.js` | The summary, the taxonomy note, sort, exclude and filter chips, and the pager |
| `table.js` | The comparison table's cells and opening a photo in the lightbox |
| `decisions.js` | Selection and the batch bar; accepting, replacing, rejecting or marking predictions reviewed, one or in bulk, and refreshing only the rows a decision touched |
| `boot.js` | Binds the template's `data-compare-on` controls from its handler table (an unknown name throws), then loads the threshold control and the collections on `DOMContentLoaded` |

Rows rendered by `table.js` and `render.js` still carry inline handlers that
call these globals.

## Guards

- `requestSeq` drops a slower row response; `loadingSeq` drops one queued
  against a collection that is no longer on screen.
- User controls wait for `decisionRefreshQueue`, so a decision's refresh
  lands before the next query reads the comparison.
- A failed decision says "not applied" only when the server answered
  (`err.answered`); a lost response is reported as unconfirmed and the page
  reloads to show what happened.
- The batch's grouped-decision tracker (`vireo-predictions.js`) keeps rows an
  earlier request in the same batch already decided from counting as
  refusals.

# ID Conflicts page JavaScript

`vireo/templates/id_conflicts.html` (ID Conflicts, at `/id-conflicts`) is a
**module page** (`frontend-module-pages.json`; see
`vireo/static/misses/README.md` for the pattern). The template loads one
entry module:

```html
<script type="module" src="/static/id-conflicts/boot.js"></script>
```

and the browser loads every other file through `boot.js`'s imports.

The comparison is derived on the server (`vireo/id_conflicts.py`); the page
asks for one page of rows plus the counts and renders what it is told.

| File | Responsibility |
| --- | --- |
| `state.js` | Page state: the `state` object (the page of rows, paging, models, chips, the selection, the request sequences, the decision queue and the conflict threshold), the preference keys, and `clampConfidence`; it reads the saved sort, page size and threshold while it loads |
| `shared.js` | The boundary with shared code: the escape helpers, `VireoViewPreferences`, `VireoTextSearch`, `showToast`, `openLightbox`, the grouped-decision tracker (`vireo-predictions.js`), the thumbnail URL and the capture-time format |
| `loading.js` | `jsonFetch` (keeps whether the server answered on the error), collections, the models shown, building and fetching a page of rows, `loadComparison` and `reloadRows` |
| `controls.js` | The search box, paging, page size, and the model and sort selects |
| `threshold.js` | The conflict-threshold slider: its label, saving it, and reloading with it |
| `render.js` | The summary, the taxonomy note, sort, exclude and filter chips, and the pager |
| `table.js` | The comparison table's cells and opening a photo in the lightbox |
| `decisions.js` | Selection and the batch bar; accepting, replacing, rejecting or marking predictions reviewed, one or in bulk, and refreshing only the rows a decision touched |
| `events.js` | The delegated document listeners for rendered markup (below) |
| `boot.js` | The entry module: binds the template's `data-compare-on` controls from its handler table (an unknown name throws) and the delegated listeners, then loads the threshold control and the collections on `DOMContentLoaded` |

Page state is the one `state` object in `state.js`; files write its fields,
since a module cannot reassign another module's binding. The page registers
no page hooks and writes no `window` globals.

## Rendered markup

The chips and table rows carry data attributes, not inline handlers, and
`events.js` handles them with one `click` and one `change` listener on the
document: `data-compare-sort`, `data-compare-exclude` and
`data-compare-filter` on the chips, `data-compare-open` on a row's
thumbnail, `data-prediction-id` / `data-prediction-action` on a prediction's
buttons, and `data-compare-select-all` / `data-compare-row` on the
checkboxes. A disabled button dispatches no click, as before.

## Tests

`tests/e2e/test_compare_page.py` overrides the `page` fixture with
`install_page_module_exposure(page, "id-conflicts", "compareState")`, reads
`compareState.compareRows` and calls `pageModules.loading.reloadRows()` and
the like. The race tests used to reassign `jsonFetch` and the refresh
functions; module bindings cannot be reassigned, so they wrap `window.fetch`
instead (`jsonFetch` looks `fetch` up when it runs) and count the compare
queries: a `refresh_photo_id` query is a decision's scoped refresh, any other
one a full or control load.

## Guards

- `state.requestSeq` drops a slower row response; `state.loadingSeq` drops
  one queued against a collection that is no longer on screen.
- User controls wait for `state.decisionRefreshQueue`, so a decision's
  refresh lands before the next query reads the comparison.
- A failed decision says "not applied" only when the server answered
  (`err.answered`); a lost response is reported as unconfirmed and the page
  reloads to show what happened.
- The batch's grouped-decision tracker (`vireo-predictions.js`) keeps rows an
  earlier request in the same batch already decided from counting as
  refusals.

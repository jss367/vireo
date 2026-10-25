# Duplicates page JavaScript

`vireo/templates/duplicates.html` (Duplicate Files, at `/duplicates`) is a
**module page** (`frontend-module-pages.json`; see
`vireo/static/misses/README.md` for the pattern). The template loads one
entry module after the navbar and the filter bar:

```html
<script type="module" src="/static/duplicates/boot.js"></script>
```

and the browser loads every other file through `boot.js`'s imports.

| File | Responsibility |
| --- | --- |
| `state.js` | Page state: the `state` object (the proposals, the scan job and its in-progress flag, the last scan result, the filter match set, epoch and readiness promise, the load epoch, the category and apply state, pending reveals, and the Trash and Keep bookkeeping), `TRASH_BATCH_SIZE`, and `duplicatesEl` |
| `shared.js` | The boundary with shared code: every navbar, filter-bar and `vireo-utils.js` global the page uses (`safeFetch`, `showToast`, `openContextMenu`, the thumbnail URL, the reveal labels, ...), and `registerPageHooks()` |
| `scan.js` | Starting a scan and following its job; the progress line, the empty state, and the restored-scan banners |
| `filter.js` | `refreshFilterMatches` (a matching member reveals its whole group), the post-scan render that waits for a filter refresh in flight, and `visibleBucket` |
| `results.js` | `renderResults` and its sections (Bulk decide, Needs review, resolved cleanup), each group's warnings, buckets, and Clear results |
| `resolved.js` | Pruning resolved hashes from the last scan, rendering unresolved and resolved groups, the resolved section's toggle, and the protected-winner rule |
| `trash-progress.js` | The batched Trash progress line and which skips are actionable |
| `card.js` | Photo cards and their right-click menu |
| `same-shot.js` | Camera identity candidates, evidence, capture times, review visibility and file-manager reveal; never feeds exact-byte resolution or cleanup |
| `same-shot-review.js` | Side-by-side cached thumbnail comparison, saving library-wide review notes and reopening them; pending writes and stale response guards |
| `restore.js` | Restoring the last scan or the catalog's rejected copies |
| `reveal.js` | Reveal in the file manager for a bucket's folders |
| `categories.js` | Filename/folder pattern categories, pure (unit-tested by `tests/frontend/duplicates-categories.test.mjs`) |
| `patterns.js` | The category summary and filter, the group checkboxes and apply bar, and applying rejections |
| `cleanup.js` | Keep-folder bulk resolve, Keep this copy instead, and Trash cleanup |
| `events.js` | The delegated document listeners for rendered markup (below) |
| `boot.js` | The entry module: registers the `startScan` page hook (the native menu's Find Duplicates), binds the template's controls and the delegated listeners, and starts the filter bar and the last-scan restore on `DOMContentLoaded` |

## State and the shared-code boundary

Page state is the one `state` object in `state.js`; files write its fields
(`state.scanInProgress = true`), since a module cannot reassign another
module's binding. Nothing the page declares is a global. Page files reach
shared code only through `shared.js`, whose forwarders look the global up at
call time; the page writes no `window` globals (`sharedWrites` is empty).

## Rendered markup

The cards, buckets and section headers are HTML strings with data attributes,
not inline handlers. `events.js` binds one listener per event on the
document, so it covers fresh scans, restored results, filter re-renders and a
bucket card pinned above the results while its Keep-for-all runs:

- `data-dup-action="keep-copy" | "trash-copy"` on a card's buttons (the card's
  `data-photo-id` names the copy), `"toggle-resolved"` on the resolved
  section's heading, `"trash-all"` on its Move to Trash button;
- `data-keep-folder` (the folder's index in the bucket card's `data-bi`
  bucket), `data-reveal-bucket` and `data-retry-trash` on bucket buttons;
- `data-duplicate-category` on the category summary's buttons and
  `data-show-category` on the "View remaining copies" notice;
- `data-same-shot-more` and `data-same-shot-reveal` in the same-shot list;
- `data-same-shot-key` identifies a catalog snapshot for comparison and review
  controls; `data-same-shot-toggle-reviewed` includes or hides reviewed groups;
- `change` on a `.group-check` updates the apply bar;
- card thumbnails (`img[data-dup-thumb]`) hide their placeholder on `load`
  and say "No thumbnail" on `error`, both listened for in the capture phase
  because those events do not bubble.

A disabled button dispatches no click, as with the old inline handlers.

## Tests

Browser tests reach the page's modules through `tests/e2e/module_pages.py`:
the Duplicates test files override the `page` fixture to install
`install_page_module_exposure(page, "duplicates", "duplicatesState")`, then
read `duplicatesState.lastScanResult` and call
`pageModules.results.renderResults(...)`. `tests/frontend/duplicate-keep-races.test.cjs`
runs selected functions in a `node:vm` context whose `state` stands in for
`state.js`.

## Guards

Keep these intact when changing this code:

- The filter never re-derives groups. It decides which members match, and a
  match reveals the whole group. Every path that renders a bucket or posts its
  hashes (`renderBucket`, `bulkResolveByFolder`) goes through
  `visibleBucket`, so a Keep-for-all never acts on groups the filter hides.
- A failed filter query sets an empty match set, not `null`, so the page fails
  closed instead of widening to the whole scan.
- `state.filterEpoch` drops superseded filter responses. `state.filterReady`
  holds the last-scan restore until the first refresh settles, and
  `renderScanWhenFilterReady` waits for any refresh in flight.
- While a scan runs (`state.scanInProgress`), nothing re-renders a cached
  snapshot.
- Same-shot candidates have no keeper/loser designation or cleanup actions.
  Photo filters reveal the whole matching group; filename categories apply
  only to exact-byte groups. Render candidates 50 groups at a time.
- Same-shot review notes are library-wide database records, separate from photo
  flags and exact-byte resolution. Reviewed groups are hidden by default and can
  be shown and reopened. The server binds each note to camera identity plus the
  sorted members' ids, hashes, paths, sizes, capture dates, visual hashes and edit
  recipes; reload refreshes existing members, while a new scan also discovers
  membership additions. Changed evidence gets a different review key. A save
  validates the client key under a SQLite write transaction. Responses update
  only matching current keys and always re-render through the active filters.
- Comparison uses the duplicate-authorized thumbnail endpoint, including members
  outside the active workspace. It labels cached thumbnails and missing images
  explicitly; it never claims pixel identity or full-resolution verification.
  Review notes never alter files, metadata, ratings, rejection flags or cleanup.
- Bucket indexes stay stable: buckets are rendered from the unfiltered list,
  and one-group buckets are skipped inline.

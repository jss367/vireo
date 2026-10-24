# Duplicates page JavaScript

`vireo/templates/duplicates.html` (Duplicate Files, at `/duplicates`) loads
classic scripts from `vireo/static/duplicates/` by responsibility, deferred
behind the navbar's module entry and the filter bar, with `boot.js` last. They
share page globals, as the inline script and the older `duplicates-*.js`
files did.

| File | Responsibility |
| --- | --- |
| `state.js` | Shared state: the proposals, the scan job and its in-progress flag, the last scan result, the filter match set, epoch and readiness promise, the Trash batch size, and `duplicatesEl` |
| `scan.js` | Starting a scan and following its job; the progress line, the empty state, and the restored-scan banners |
| `filter.js` | `refreshFilterMatches` (a matching member reveals its whole group), the post-scan render that waits for a filter refresh in flight, and `visibleBucket` |
| `results.js` | `renderResults` and its sections (Bulk decide, Needs review, resolved cleanup), each group's warnings, buckets, and Clear results |
| `resolved.js` | Pruning resolved hashes from the last scan, rendering unresolved and resolved groups, and the protected-winner rule |
| `trash-progress.js` | The batched Trash progress line and which skips are actionable |
| `card.js` | Photo cards and their context menu (was `duplicates-card.js`) |
| `same-shot.js` | Review-only camera identity candidates, visual evidence, capture times and file-manager reveal; never feeds exact-byte resolution or cleanup |
| `restore.js` | Restoring the last scan or the catalog's rejected copies (was `duplicates-restore.js`) |
| `reveal.js` | Reveal in the file manager (was `duplicates-reveal.js`) |
| `categories.js` | Filename/folder pattern categories, pure (was `duplicates-categories.js`; unit-tested by `tests/frontend/duplicates-categories.test.mjs`) |
| `patterns.js` | The category summary and filter, and applying rejections (was `duplicates-patterns.js`) |
| `cleanup.js` | Keep-folder bulk resolve and Trash cleanup (was `duplicates-cleanup.js`) |
| `boot.js` | Registers the `startScan` page hook (the native menu's Find Duplicates; was `duplicates-page-hooks.js`), binds the controls that used to carry inline handlers, and starts the filter bar and the last-scan restore on `DOMContentLoaded` |

## Guards

Keep these intact when changing this code:

- The filter never re-derives groups. It decides which members match, and a
  match reveals the whole group. Every path that renders a bucket or posts its
  hashes (`renderBucket`, `bulkResolveByFolder`) goes through
  `visibleBucket`, so a Keep-for-all never acts on groups the filter hides.
- A failed filter query sets an empty match set, not `null`, so the page fails
  closed instead of widening to the whole scan.
- `_filterEpoch` drops superseded filter responses. `_filterReady` holds the
  last-scan restore until the first refresh settles, and
  `renderScanWhenFilterReady` waits for any refresh in flight.
- While a scan runs (`_scanInProgress`), nothing re-renders a cached snapshot.
- Same-shot candidates have no keeper/loser designation or cleanup actions.
  Photo filters reveal the whole matching group; filename categories apply
  only to exact-byte groups. Render candidates 50 groups at a time.
- Bucket indexes stay stable: buckets are rendered from the unfiltered list,
  and one-group buckets are skipped inline.

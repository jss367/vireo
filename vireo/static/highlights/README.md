# Highlights page JavaScript

`vireo/templates/highlights.html` (Highlights, at `/highlights`) is a **module
page** (`frontend-module-pages.json`): its scripts are ES modules, not classic
scripts sharing page globals. The template loads one entry module after the
shared navbar scripts:

```html
<script type="module" src="/static/highlights/boot.js"></script>
```

and the browser loads every other file through `boot.js`'s imports. The page
has no Jinja values to hand over, and its controls name their handlers in
`data-highlights-on="event:name"` attributes (see `boot.js`) instead of inline
`on*=` attributes.

| File | Responsibility |
| --- | --- |
| `state.js` | Page state: the `state` object (the last `/api/highlights` response, expanded rows, the folder name, the debounce timer, collections, the species modal, the lightbox photo, the reject undo and the load and refetch sequences) and `WORKSPACE_SCOPE` |
| `shared.js` | The boundary with shared code: every navbar, lightbox and `vireo-utils.js` global the page uses, and `registerPageHooks()` |
| `scope.js` | Score formatting, the scope and filter request parameters |
| `loading.js` | `loadHighlights()` with its reject-prune guard, bucket sorting, the advanced-mode sort options, `debounceLoad`, and the undo/redo hook |
| `render.js` | `render()`, each species row and the unidentified row, the expand and row action buttons, and `loadMoreBucket` |
| `cards.js` | Photo cards, their thumbnails, and their click and highlight-order buttons |
| `species.js` | Confirming a row, the species modal, representatives, and adding, removing and reordering highlights |
| `ordering.js` | Finding a photo in the loaded rows, and `refetchBucketFromZero` with its per-bucket sequence guard |
| `rejects.js` | Whether a reject needs a reload under a curation filter, and pruning a rejected photo from the loaded rows |
| `lightbox.js` | Advancing the lightbox after a reject, the reject undo bar, and the highlight controls beside the open photo |
| `flag-changes.js` | `onHighlightFlagChanged`: what a lightbox reject, un-reject, pick or unpick does to the grid |
| `save.js` | The Save as Collection modal |
| `boot.js` | The entry module: registering the page hooks, the `data-highlights-on` handler table, the filter control listeners, the lightbox event listeners, restoring view preferences, and the first load |

## State

Page state lives in one exported object, `state` in `state.js`. An ES module
cannot reassign another module's `let`, so files write fields
(`state.currentData = data`) instead of rebinding variables. Nothing the page
declares is a global; browser tests read the state through
`expose_page_modules(page, "highlights", "highlightsState")`
(`tests/e2e/module_pages.py`).

## The shared-code boundary

Page files never reach for navbar or lightbox globals directly (ESLint's
`no-undef` fails on a bare `showToast`); they import them from `shared.js`.
The forwarders there look the global up at call time, so a missing lightbox
keeps its guard (`hasLightbox()`) and a function a test replaces is still the
one called. Live lightbox state (`lightboxRequestedPhotoId()`,
`lightboxPhotoList()`) is read through getters for the same reason.

## Page hooks

The navbar and lightbox reach the page only through `Vireo.pageHook(...)`:
`afterHistoryChange` (undo/redo reloads and drops this session's reject
tracking), `loadHighlights`, and `updateHighlightsLightboxControls`. `boot.js`
registers them, and `frontend-module-pages.json` lists the same names.

## Request ordering

- `loadHighlights` stamps each request with `state.highlightsLoadSeq` and drops
  a response a newer load superseded. After applying one, it prunes only photos
  rejected at or after that request was issued (`state.rejectedAtLoadSeq`), so
  a stale reload cannot bring back a photo the user just rejected, and a photo
  restored elsewhere is not stripped.
- `refetchBucketFromZero` stamps each bucket's refetch in
  `state.bucketRefetchSeq` so a slower Pick refetch cannot overwrite a newer
  Unpick's order; it resolves `false` when superseded so its caller skips the
  render.

# Best Batch page JavaScript

`vireo/templates/best_batch.html` (Best Batch, at `/best-batch`) is a **module
page** (`frontend-module-pages.json`; see `vireo/static/misses/README.md` for
the conventions). The template loads one entry module after the shared navbar:

```html
<script type="module" src="/static/best-batch/boot.js"></script>
```

Browse opens the page with the selected photo ids in `sessionStorage`
(`vireo.bestBatchIds`, `vireo.bestBatchSeedId`) or a `?photo_id=` seed; the
page ranks the batch and lets the user flag the best frame.

| File | Responsibility |
| --- | --- |
| `state.js` | The `state` object (the ranked batch, the inspected frame, the preview image's pending fallback) and `byId` |
| `shared.js` | The boundary with shared code: `showToast`, the preview and thumbnail URL builders, the lightbox, and the escape helpers |
| `helpers.js` | `fetchJson`, image URLs, role and score labels, the eye-crop placement, and reading the batch Browse handed over |
| `render.js` | The status line, the focus-check crops, the ranked list, the inspected-frame preview, and the delegated card clicks |
| `actions.js` | Loading the batch, the lightbox, Review Burst, Flag Best, and Flag Best + Reject Rest |
| `boot.js` | The entry module: binds the toolbar buttons and the preview image that used to carry inline handlers, then loads the batch |

Cards carry `data-photo-id`; one delegated listener each on `#focusGrid` and
`#rankList` inspects the clicked card. The preview image falls back to the
thumbnail once per inspect: the listener is replaced, not stacked, when a new
frame is inspected. The page registers no page hooks and writes no `window`
globals.

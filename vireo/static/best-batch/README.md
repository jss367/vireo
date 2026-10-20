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
| `subjects.js` | Comparison-local subject selection, evidence details, and background analysis progress |
| `boot.js` | The entry module: binds the toolbar buttons and the preview image that used to carry inline handlers, then loads the batch |

Cards carry `data-photo-id`; one delegated listener each on `#focusGrid` and
`#rankList` inspects the clicked card. The preview image falls back to the
thumbnail once per inspect: the listener is replaced, not stacked, when a new
frame is inspected. The page registers no page hooks and writes no `window`
globals.

The species field and **Compare subjects** start a comparison with explicit
subject context. Ambiguous birds remain review items until chosen in the subject
selector. **Analyze selected subjects** computes isolated per-detection mask and
eye evidence in a background job. Full-frame context and the chosen subject's
crop remain side by side; focus tiles use native, generation-pinned crop URLs.
The ordinary Review Burst handoff is disabled during these comparisons because
it cannot preserve this subject context. Editing the species invalidates action
buttons until the comparison is refreshed. Saved primary choices are unchanged.

`focus.js` owns transient focus inspection. Its manual center is read from the
upright full frame loaded with an empty edit recipe, so saved crop and rotation
do not change the coordinate space. Model boxes remain visible suggestions;
weak detections are offered with their confidence for an explicit subject choice
but never become automatic choices solely because they were offered. Manual
inspection uses original-source crop pixels and never changes ranking or saved
subject decisions. The metadata label comes from the actual crop response, and
stale requests are aborted and sequence-guarded before they can replace it.

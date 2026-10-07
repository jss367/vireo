# Photo Editor page JavaScript

`vireo/templates/photo_editor.html` loads these classic scripts after the shared
navbar, folder-browser, export, and batch-edit scripts. Functions remain global
for the page's inline event handlers, and the editor's shared state lives on
`editorState` in `state.js`.

| File | Responsibility |
| --- | --- |
| `state.js` | Shared editor state, the loading freeze, dirty tracking, and status text |
| `recipe.js` | Recipe values, save normalization, dirty state, the summary, and control sync |
| `zoom.js` | Zoom levels, fit/actual sizing, orientation, and the before/after toggle |
| `preview.js` | Histogram feedback, progressive preview queue, bounded latency samples, and histogram/mask scheduling |
| `crop-geometry.js` | Crop clamping and how rotation, flip, and straighten carry a crop |
| `crop.js` | Crop box, crop fields, rotate/flip/straighten, aspect ratios, and committing a crop |
| `crop-ratio.js` | The remembered crop ratio, its revisions, and cross-tab sync through localStorage |
| `crop-drag.js` | Pointer dragging of crop handles and panning the zoomed image |
| `adjustments.js` | Basic, tone curve, color mixer, color grading, and detail (denoise) controls |
| `presets.js` | Edit presets: loading, applying, saving, and deleting |
| `local-adjustments.js` | Subject/background adjustments, the mask snapshot, and the mask overlay |
| `mask-brush.js` | Add/subtract mask painting, inverse geometry, and immutable correction requests |
| `auto-tone.js` | Auto Tone requests (Balanced, Subject and Gentle styles) and their result messages |
| `save.js` | Resetting all edits and saving the recipe |
| `checkpoints.js` | Edit History checkpoints and the saved-edit undo/redo hooks |
| `export.js` | The Export dialog: presets, filename preview, preflight, and starting the job |
| `navigation.js` | Back to Browse, the unsaved-edits guard, and Prev/Next photo navigation |
| `search.js` | The editor's photo search box and its results |
| `context-menu.js` | Copy settings, revert, export, iNaturalist, and the right-click menu |
| `keyboard.js` | Editor keyboard shortcuts |
| `loading.js` | Loading a photo, the empty state, and `initEditor()` |
| `boot.js` | The edit-history busy listener, the export folder browser and controls, and page startup |

Load `boot.js` last. Every other file only declares functions and initializes
passive state, so their relative order does not matter at load time. Boot binds
the edit-history busy listener, constructs the export folder browser, binds the
export controls, and schedules `initEditor()` for `DOMContentLoaded`, in the
order the inline script used to run them. Adding a script requires an explicit
template tag before boot. These are not ES modules and need no bundler.

`photo_editor_color.js` (point curves and sampled color) and
`photo_editor_history.js` (undo/redo of unsaved edits) predate this directory
and still load after `boot.js`. Both bind their own listeners at load, and the
undo/redo capture-phase key handler must stay registered as it is today.

Keep these lifecycle boundaries intact when changing this code:

- `setEditorLoading()` freezes the editing surface while a photo loads, so a
  slider tweak or crop drag cannot land on the photo that is loading. Anything
  that mutates `editorState.recipe` must respect `editorState.loading`.
- `loadPhoto()` owns `editorState.loadSeq`. Responses for an older load,
  preview (`previewSeq`), mask (`localMaskUpdateSeq`, `maskOverlaySeq`) or search
  (`searchSeq`) request must be dropped, not applied.
- `crop-ratio.js` reconciles the server's remembered ratio with other open
  editor tabs by revision. Keep the pending and committed localStorage keys in
  step with `initEditor()`'s reconciliation in `loading.js`.

Preview requests use a 1,024-pixel quick tier during input, throttled to 60 ms,
and the zoom-appropriate tier after 300 ms without input. Both use the normal
server renderer. One request runs at a time; only the latest pending request
is retained. A decoded image is moved into the page only if its input revision
is still current. Photo navigation invalidates timers and pending results.
`editorState.previewTimings` retains at most 100 input-to-display samples in
memory, with size and tier but no photo identifiers or recipes.
Each dispatched image has a 60-second deadline. Timeout detaches the image and
releases the slot for the latest pending edit. A current failure offers Retry
preview without an automatic retry loop; navigation clears the old timer.

Brush coordinates are mapped back through crop, straighten, flips and rotation
into the EXIF-oriented source. The server paints a new content-addressed mask;
prior files and the active AI mask remain unchanged. `local.mask.corrected`
retains a corrected snapshot even with no active region adjustments. It is
removed when rebinding masks for presets or other photos. The original
`source_digest` continues to detect newer AI extractions.

The browser preview regression suite also reports five quick/refined latency
samples and their p50/p95 in JUnit properties, using a synthetic 3,072 × 2,048
JPEG. These are input-to-display times on the current machine, not a camera RAW
benchmark or a before/after speed comparison:

```sh
python -m pytest -o addopts='' -o junit_family=xunit1 -q \
  tests/e2e/test_photo_editor_progressive_preview.py \
  --junitxml=.context/editor-preview-results.xml
```

Cold RAW decoding still uses the full-precision decoder. The quick tier reduces
render/output work; it does not replace that decode with an embedded JPEG.
See [RAW preview benchmarks](../../../docs/raw-preview-benchmarks.md) for the
real-camera corpus runner, memory measurements, regression comparisons, and
the limits of browser cancellation.

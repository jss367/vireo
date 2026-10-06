# Process page JavaScript

`vireo/templates/pipeline.html` (the Process page at `/pipeline`) loads these
classic scripts after the shared navbar scripts and `pipeline-messages.js`.
Functions remain global for the page's inline event handlers.

| File | Responsibility |
| --- | --- |
| `page-init.js` | `/api/pipeline/page-init`, `window._pageInitPending`, and the classify and dashboard-action deep links |
| `sources.js` | Source mode, the collection picker, the folder scope list, and the source summary |
| `processes.js` | Saved processes: loading, applying, editing, and the create/rename/delete dialog |
| `preview.js` | Folder preview: fetching, rendering, thumbnails, and per-file selection |
| `start-gate.js` | The Start button gate and stage toggles |
| `slots.js` | Pipeline slot polling and the queued/running status line |
| `run.js` | Starting and stopping a pipeline run |
| `progress.js` | Stage-to-card maps and live stage progress, ETA, notes, and errors |
| `completion.js` | Terminal results: step outcomes, failure errors, and the completion handoff |
| `models.js` | The classifier model picker and label-file selection |
| `labels-modal.js` | The species-label download modal: place search, filters, and fetch |
| `readiness.js` | Classification readiness and the exiftool install |
| `steps.js` | Standalone Classify and Extract step runs |
| `extract-config.js` | Extract model config, SAM2 variant warnings, readiness, and mask coverage |
| `plan.js` | Card toggles, pipeline state, the run plan, status pills, and card states |
| `boot.js` | `DOMContentLoaded` and mode-change handlers, and starting the slot poller |

Load `boot.js` last. The other files declare functions and initialize state;
three initializers run at load and each depends only on its own file:
`_savedModelConfig` reads the model selects in `extract-config.js`, and
`_cardToStages` (`progress.js`) and `_STAGE_ENABLE_BY_SUFFIX` (`plan.js`) are
built from the tables declared just above them. Boot registers the page-init,
plan, and advanced-option handlers, then saved-process loading, then starts
the slot poller, in the order the inline script used to run them. Adding a
script requires an explicit template tag before boot. These are not ES modules
and need no bundler.

Keep these request-ordering rules intact when changing this code:

- `refreshPipelinePlan()` in `plan.js` owns `_planFetchSeq`. A plan response
  for an older sequence must be dropped, and anything that changes the
  selection (`preview.js` included) bumps the sequence. `_planRefreshPending`
  keeps Start disabled while a refresh is debounced or in flight, so a fast
  click can't start a classify run the plan would block.
- `fetchFolderPreview()` aborts the previous preview request through
  `_previewAbort`.
- `updateReadiness()` (`readinessRequestSequence`) and the label place search
  (`pipelineLabelsSearchRequestId`) drop responses that a newer request
  superseded.
- Tests wait on `window._pageInitPending` before touching stage toggles,
  because the page-init handler overwrites some of them.

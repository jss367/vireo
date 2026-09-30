# Import page JavaScript

`vireo/templates/import.html` loads these classic scripts after the shared
navbar, transfer-help, and folder-browser scripts. Existing inline event
handlers call the page's global functions. State retains its existing `let`
and `const` bindings across scripts.

| File | Responsibility |
| --- | --- |
| `state.js` | Shared source, preview, selection, readiness, and job state |
| `readiness.js` | Metadata-tool readiness, installation, and repair |
| `form.js` | Import mode, file types, tags, validation, and the Start button gate |
| `destinations.js` | Archive targets, folder templates, recent paths, and destination preview |
| `sources.js` | Source folders, streamed scan counts, and stalled-scan progress |
| `folder-browser.js` | Native folder pickers and shared folder-browser integration |
| `after-import.js` | Process defaults, local processing, and archive transfer options |
| `staging.js` | Staging verification, cleanup, and paused-job polling |
| `preview.js` | Preview scheduling, cancellation, signatures, and duplicate checks |
| `preview-grid.js` | Cards, duplicate visibility, capture-day groups, and thumbnails |
| `selection.js` | Individual, range, folder, and select-all file selection |
| `jobs.js` | Import submission, progress, completion, and failed-file retries |
| `results.js` | Result presentation and the card-cleanup handoff |
| `new-images.js` | Captured new-image lists and their import deep links |
| `loading.js` | Initial configuration, form wiring, workspace defaults, and deep links |
| `boot.js` | Folder-browser construction, return/mode listeners, and page startup |

Load `boot.js` last. All other scripts declare functions and initialize passive
state. Boot constructs the folder browser only after the page functions it
calls exist, registers the after-import listeners, then invokes
`initImportPage()`. Adding a script requires an explicit template tag before
boot. Cross-file function calls use the shared page scope; these are not ES
modules and require no bundler.

Keep the lifecycle boundaries intact when changing this code:

- `preview.js` owns request generations and cancellation. Source counts,
  destination previews, duplicate results, and thumbnails must reject stale
  work when the form or source list changes.
- `selection.js` tracks deliberate exclusions separately from duplicate
  eligibility. `updateStartGate()` in `form.js` combines preview, readiness,
  snapshot, and submission state so Start cannot import an unintended list.
- `after-import.js` distinguishes workspace defaults from explicit choices
  and coordinates target refreshes with `destinations.js`. Preserve its
  response sequencing and selection checks.
- `loading.js` binds invalidation and selection handlers before its first
  await. Remote targets and metadata readiness start independently; slow
  readiness must not block the rest of the form or snapshot deep links.

# Jobs page JavaScript

`vireo/templates/jobs.html` (the Jobs page at `/jobs`) is a **module page**
(`frontend-module-pages.json`; see `vireo/static/misses/README.md` for the
pattern). After the shared `import-recovery.js`, the template loads one entry
module:

```html
<script type="module" src="/static/jobs/boot.js"></script>
```

and the browser loads every other file through `boot.js`'s imports.

Both the Jobs and Import pages load `/static/import-recovery.js` to recognize
failures before copying begins. New jobs record `result.import_not_started`;
the helper also recognizes the disk-space error on older history rows. Those
rows offer "Retry import" without inventing a failed-file count, and participate
in the same active-retry and completed-descendant gates as failed-file retries.
"Retry directly to NAS" is available for a local-processing import that failed
before copying. The server checks that neither the original run nor later
attempts left imported photos, preserves the frozen archive destination, and
runs the selected process in the archive instead of staging.

| File | Responsibility |
| --- | --- |
| `state.js` | Page state: the `state` object (`activeJobs`, `historyJobs`, the selection `selectedJobId` / `selectedSource` / `currentView`, the poll timer and per-list refresh ownership `jobsPollState`, the SSE stream, leaf buffers, collapsed steps, optimistic cancels, pending restarts and retries, source-cleanup reviews, workspace names), `LEAF_MAX`, and `jobsElement` |
| `shared.js` | The boundary with shared code: `escapeHtml`, `escapeAttr`, `safeFetch`, `showToast`, the navbar's `formatJobType`, the `VireoTime` formatters, and `import-recovery.js`'s `importFailedBeforeStart`, `canRetryImportDirectToNas` and `directNasImportRetryBody` |
| `format.js` | Escaping, elapsed time and "ago" text, status icons, live-status checks, progress percentages, `jobConfig`, and `plural` |
| `move-route.js` | The move-folder route: From/To paths, and the capture-date folder note built from the plan or the finished result |
| `source-cleanup.js` | Original-folder cleanup after a date-organized move (`state.sourceCleanupReviews`), and the confirm checkbox listener |
| `import-retry.js` | Import Retry, Resume and Restart: in-place restart eligibility and takeover, archive retry gates, `importResumeTakeover` (mirrors `import_resume_takeover` in `services/imports.py`), hints, and the retry request body |
| `job-card.js` | The job card: header controls (pause/resume/cancel), meta, the retry/resume row, pause and interrupted banners, and finished results |
| `steps.js` | A job's step tree: progress bars, throughput and ETA, current file, errors, repair lists, and file leaves; and the failed step's Retry button guard |
| `polling.js` | `fetchJobs()` (`/api/jobs`) and `fetchHistory()` (`/api/jobs/history`) |
| `lineage.js` | Who started a job and what it follows: `groupOperations` (a root job with the follow-ups it started, at any depth), the "Started by Vireo" / "Following your Import" text, the repeat note for a system task's collapsed row, and the origin filter |
| `list.js` | The job list pane: the type, status and origin filters (`filteredHistory`, `applyFilters`), list items grouped by operation, the running badge, and the list click listener |
| `selection.js` | Selecting a job and following a live one over `/api/jobs/<id>/stream` |
| `detail.js` | The detail pane: one job, step toggling, and the Active and History overviews, each operation's follow-ups indented under its root |
| `actions.js` | Detail pane clicks: cleanup buttons, Show that import, Retry/Resume import, step toggles, pause/resume, and cancel |
| `boot.js` | The entry module: binding the list, filter and detail-pane listeners, the first fetches, and the pollers |

Page state is the one `state` object in `state.js`; files write its fields,
since a module cannot reassign another module's binding. The page registers
no page hooks and writes no `window` globals.

The other files only declare functions and initialize passive state. Boot
calls `bindJobListClicks()` (`list.js`), `bindSourceCleanupConfirm()`
(`source-cleanup.js`), `bindStepRetryButtons()` (`steps.js`) and
`bindDetailPaneActions()` (`actions.js`), binds the polling Retry button and
the type, status and origin filters (the first two used to carry `onchange="applyFilters()"`),
then runs `fetchJobs()`, `fetchHistory()`, the 2 s `fetchJobs` poller (kept
in `state.pollTimer`) and the 10 s `fetchHistory` poller. A failed step's
Retry button (`data-step-retry`) used to stop its click with an inline
`onclick`; `bindStepRetryButtons()` stops it at the detail pane in the
capture phase, before the pane's step toggle.

Browser tests reach the modules through `tests/e2e/module_pages.py`: the
Jobs test files override the `page` fixture with
`install_page_module_exposure(page, "jobs", "jobsState")`, then set
`jobsState.historyJobs` and call `pageModules.detail.renderHistoryDetail(...)`.
The node harnesses (`tests/frontend/jobs-polling.test.mjs`,
`vireo/tests/jobs_in_place_restart.cjs`, `jobs_import_resume.cjs`) run the
module source with its imports and `export` keywords removed.

Keep these request-ordering rules intact when changing this code:

- Active jobs and history have separate request generations. Explicit refreshes
  abort and retire older requests; regular polls skip an occupied lane. HTTP,
  JSON and list-shape failures preserve the last successful list. Requests time
  out after 15 seconds so a stalled connection cannot block future refreshes.
- The sidebar warning identifies which list failed and whether saved data is
  being shown. An unsuccessful first load stays unavailable rather than claiming
  there are no jobs. Each lane clears only its own error after a valid response.
  Polls retry automatically; the keyboard-accessible Retry now button retries
  failed idle lanes without duplicating pending work.
- A selected active job that leaves `state.activeJobs` is looked up in
  `state.historyJobs`; if it is not there yet, `fetchHistory(true, true)` records
  that selection and starts a fresh history snapshot once. Further polls
  reuse the pending request, and the next successful response reconciles it. Failure or replacement preserves the intent; a changed selection
  or a job that became active again is left alone. A valid history response
  moves it to its history row, or back to Active if the row is absent.
- Only one SSE stream is open at a time. Changing the selection or the view
  closes `state.sseSource` first, and the stream handlers only redraw the detail
  pane while their job is still selected. The `complete` event refetches both
  lists.
- `state.cancellingJobIds` keeps the optimistic "Cancelling…" button across polls
  until the job leaves the active list; a failed cancel clears it.
- `state.sourceCleanupReviews[id].busy` blocks a second review or cleanup request
  while one is in flight, and a failed or stale review never leaves an armed
  cleanup button.
- Retry/Resume import is gated twice: at render (`hasActiveRetryFor`,
  `importResumeTakeover`) and again on click, because a poll can land between
  render and click. The server enforces the same takeover rule with a 409.
- When inherited processing evidence is outside the 100-row history window,
  polling asks the recovery-options endpoint for authoritative Resume
  eligibility. It also checks when no processing receipt reached the import
  checkpoint, since the pipeline may already record the handoff. Responses
  apply only to the same history-row object, and
  later polls recheck eligibility without caching the HTTP response.
  Failed eligibility requests show an explanation and Check again action;
  they never enable an unverified import retry.
  Pre-copy local imports also check direct-NAS eligibility before showing
  that option and recheck it on click; the original retry remains available
  during this read-only NAS check.
- Saved processing provenance retains groups: every group must have a
  surviving pipeline, while IDs within a legacy inferred group are
  alternatives. Server recovery preserves these groups before preparation
  so a later cancellation cannot change an alternative into an obligation.
- Restart import replays a failed or cancelled in-place import's saved scope
  and settings in its original workspace. It rescans rather than resuming at
  the old progress counter. `state.inPlaceRestarts` keeps pending requests disabled
  across redraws; a later run's `config.restarted_from` replaces the button
  with a link. The server serializes admission and rejects duplicate restarts,
  including requests from another tab, with the existing child's job id.

`tests/frontend/jobs-polling.test.mjs` covers response ordering, late errors,
timeouts, invalid payloads, retained data and selection handoff.
`tests/e2e/test_jobs_polling.py` covers the real page, keyboard Retry,
non-overlapping scheduled polls and automatic recovery in Chromium and WebKit.

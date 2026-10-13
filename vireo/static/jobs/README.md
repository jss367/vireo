# Jobs page JavaScript

`vireo/templates/jobs.html` (the Jobs page at `/jobs`) loads these classic
scripts after the shared navbar scripts. They share page globals: the inline
`onchange="applyFilters()"` handlers on the type and status filters call
`window.applyFilters` (`list.js`), and the page relies on the navbar's
`escapeAttr`, `safeFetch`, `showToast` and `window.formatJobType`.

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
| `state.js` | Shared page state: `activeJobs`, `historyJobs`, the selection (`selectedJobId`, `selectedSource`, `currentView`), the SSE stream, leaf buffers, collapsed steps, optimistic cancels, and workspace names |
| `format.js` | Escaping, elapsed time and "ago" text, status icons, live-status checks, progress percentages, `jobConfig`, and `plural` |
| `move-route.js` | The move-folder route: From/To paths, and the capture-date folder note built from the plan or the finished result |
| `source-cleanup.js` | Original-folder cleanup after a date-organized move (`sourceCleanupReviews`), and the confirm checkbox listener |
| `import-retry.js` | Import Retry, Resume and Restart: in-place restart eligibility and takeover, archive retry gates, `importResumeTakeover` (mirrors `import_resume_takeover` in `services/imports.py`), hints, and the retry request body |
| `job-card.js` | The job card: header controls (pause/resume/cancel), meta, the retry/resume row, pause and interrupted banners, and finished results |
| `steps.js` | A job's step tree: progress bars, throughput and ETA, current file, errors, repair lists, and file leaves |
| `polling.js` | `fetchJobs()` (`/api/jobs`) and `fetchHistory()` (`/api/jobs/history`) |
| `list.js` | The job list pane: the type filter, `applyFilters`, list items, the running badge, and the list click listener |
| `selection.js` | Selecting a job and following a live one over `/api/jobs/<id>/stream` |
| `detail.js` | The detail pane: one job, step toggling, and the Active and History overviews |
| `actions.js` | Detail pane clicks: cleanup buttons, Show that import, Retry/Resume import, step toggles, pause/resume, and cancel |
| `boot.js` | Binding the list and detail-pane listeners, the first fetches, and the pollers |

Load `boot.js` last. The other files only declare functions and initialize
passive state, so their relative order does not matter at load time. The
three listeners that the inline script attached at load are wrapped in
`bindJobListClicks()` (`list.js`), `bindSourceCleanupConfirm()`
(`source-cleanup.js`) and `bindDetailPaneActions()` (`actions.js`). Boot calls
them in that order, then runs `fetchJobs()`, `fetchHistory()`, the 2 s
`fetchJobs` poller (kept in `pollTimer`) and the 10 s `fetchHistory` poller,
as the inline script did. Adding a script requires an explicit template tag
before boot. These are not ES modules and need no bundler.

The page script used to be one IIFE, so this state was private to it. It is
now global; nothing else on the page declares these names, but pick new names
with that in mind.

Keep these request-ordering rules intact when changing this code:

- There are no request sequence numbers. `fetchJobs()` and `fetchHistory()`
  replace `activeJobs` / `historyJobs` wholesale, so whichever response lands
  last wins, and the next poll corrects a stale one.
- A selected active job that leaves `activeJobs` is looked up in
  `historyJobs`; if it is not there yet, `fetchHistory(true)` moves the
  selection to its history row, or back to the Active overview if the row
  never appears.
- Only one SSE stream is open at a time. Changing the selection or the view
  closes `sseSource` first, and the stream handlers only redraw the detail
  pane while their job is still selected. The `complete` event refetches both
  lists.
- `cancellingJobIds` keeps the optimistic "Cancelling…" button across polls
  until the job leaves the active list; a failed cancel clears it.
- `sourceCleanupReviews[id].busy` blocks a second review or cleanup request
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
  the old progress counter. `inPlaceRestarts` keeps pending requests disabled
  across redraws; a later run's `config.restarted_from` replaces the button
  with a link. The server serializes admission and rejects duplicate restarts,
  including requests from another tab, with the existing child's job id.

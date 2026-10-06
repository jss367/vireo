# Jobs page JavaScript

`vireo/templates/jobs.html` (the Jobs page at `/jobs`) loads these classic
scripts after the shared navbar scripts. They share page globals: the inline
`onchange="applyFilters()"` handlers on the type and status filters call
`window.applyFilters` (`list.js`), and the page relies on the navbar's
`escapeAttr`, `safeFetch`, `showToast` and `window.formatJobType`.

| File | Responsibility |
| --- | --- |
| `state.js` | Shared page state: `activeJobs`, `historyJobs`, the selection (`selectedJobId`, `selectedSource`, `currentView`), the SSE stream, leaf buffers, collapsed steps, optimistic cancels, and workspace names |
| `format.js` | Escaping, elapsed time and "ago" text, status icons, live-status checks, progress percentages, `jobConfig`, and `plural` |
| `move-route.js` | The move-folder route: From/To paths, and the capture-date folder note built from the plan or the finished result |
| `source-cleanup.js` | Original-folder cleanup after a date-organized move (`sourceCleanupReviews`), and the confirm checkbox listener |
| `import-retry.js` | Import Retry and Resume: in-flight retries, `importResumeTakeover` (mirrors `import_resume_takeover` in `services/imports.py`), hints, and the retry request body |
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

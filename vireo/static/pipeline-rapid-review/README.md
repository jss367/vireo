# Rapid Review page JavaScript

`vireo/templates/pipeline_rapid_review.html` (Rapid Review, at
`/pipeline/rapid-review`) loads these classic scripts after the shared navbar
scripts, where its inline page script used to be. Functions remain global for
the markup's `onclick=`, `onchange=`, `oninput=` and `onload=` attributes (`requestOpenSession`,
`selectPhoto`, `decideCurrent`, `applyCurrent`, `changeQueue`,
`undoLast`, `rapidOnSpeciesInput`, `rapidImageLoaded`, `toggleRapidZoom`, `openClassic`, ...).

This is not Pipeline Review. `pipeline_review.html` (`/pipeline/review`) has
its own scripts in `vireo/static/pipeline-review/`; Rapid Review is the
one-burst-at-a-time view that `openClassic()` hands back to it.

| File | Responsibility |
| --- | --- |
| `state.js` | `rapid` (results, photo map, sessions, the open burst's staged decisions, zoom, `openToken`, the queue) and `QUEUE_FILTERS` |
| `shortcuts.js` | The Keymap scope, `RAPID_SC_DEFAULTS`, `loadRapidShortcuts()`, and the keydown handler (`bindRapidKeyboard`) |
| `photo-urls.js` | Preview, original and thumbnail URLs, and `burstIds()` |
| `species-list.js` | The species field's list encoding (`parseSpeciesList`, `joinSpecies`, `speciesKey`), override vs encounter precedence, and `locateBurstByPhotoIds()` |
| `sessions.js` | `buildSessions()` from the results, `sessionKey()`, and the per-photo flag and species readers |
| `queue-filters.js` | The queue predicates (`needsSpecies`, `cullingState`, `unreviewed`, `reviewPicks`, `hasMixedSpecies`), `sessionReasonChips()`, `rebuildFilteredSessions()` |
| `queue.js` | Session lookup by key and raw index, the `?enc=&burst=` deep link, `hasStagedChanges()`, the queue controls, the staged-decisions prompt (`bindQueuePromptChoices`), `changeQueue()` |
| `loading.js` | The undo/redo hooks (`registerRapidHistoryHooks`), `initRapidReview()`, `showEmptyQueue()`, `openSession()`, `retryRapidReview()` |
| `render.js` | The burst sidebar, `renderAll()`, the current photo and its metrics, the filmstrip, and the lanes |
| `zoom.js` | 1:1 zoom: layout, toggle, and pointer drag panning of the stage |
| `decisions.js` | `currentPhoto()`, zones, pick/reject/review, clear, undo, `advanceToNext()`, `selectPhoto()` |
| `species-keywords.js` | The seeded per-frame keyword snapshot and its debounced refetch as the species field changes |
| `apply-diff.js` | `rapidComputeApplyDiff()`, the Apply / Apply & Next labels and titles, `renderApplyState()` |
| `apply.js` | `applyCurrent()`: the flags-only `/group/apply` request, the `/api/encounters/species` requests, the cache save, and the next burst; `openClassic()` |
| `boot.js` | The Keymap scope, the listener and hook bindings, the shortcut and results fetches, and the stage's pointer listeners |

Load `boot.js` last. The other files only declare functions and initialize
passive state (`rapidShortcuts` copies `RAPID_SC_DEFAULTS` from its own file),
so their relative order does not matter at load time. The load-time
statements of the inline script are wrapped in `bindQueuePromptChoices()`
(`queue.js`), `registerRapidHistoryHooks()` (`loading.js`) and
`bindRapidKeyboard()` (`shortcuts.js`); boot runs `Keymap.setScope`, those
three, `loadRapidShortcuts()`, `initRapidReview()` and the four `#photoStage`
pointer listeners in the order the inline script did. The scripts sit after
the whole page, including the `#queuePrompt` dialog, so boot can look up any
element. Adding a script requires an explicit template tag before boot. These
are not ES modules and need no bundler.

Page-wide state lives in `rapid` (`state.js`). Domain state stays beside its
functions: `rapidShortcuts` / `rapidShortcutsRegistered` in `shortcuts.js`
and the species refetch timer and token (`_rapidSpeciesRefetch*`) in
`species-keywords.js`.

`vireo/tests/test_species_identity.py` runs `hasMixedSpecies()` under node by
slicing `queue-filters.js` from `function hasMixedSpecies` to
`function needsSpecies`; keep those two adjacent.

This page writes flags and species keywords. Keep these guards intact when
changing this code:

- `openSession()` bumps `rapid.openToken` before its `/group/state` fetch and
  drops the response if the token moved: seeding burst A's flags into burst
  B's queue would mis-flag photos on Apply. `afterHistoryChange` bumps it too.
- `decideCurrent()` and `clearCurrent()` return until `rapid.seeded` is true.
  Deciding before the seed lands would let the seed loop add a flag for a
  photo already rejected by hotkey, so it appears in two zones and Apply
  fails.
- `rapid.applying` disables the decision buttons, Apply, undo and queue
  changes while a request is in flight; `renderApplyState()` is the one place
  that reads it for the buttons.
- `applyCurrent()` snapshots the session, items, picks and rejects before its
  first `await`, and reads the already-confirmed species from `rapid.results`
  (not live `rapid.*`), because the sidebar still allows `openSession()`
  during the request. The species POST is gated on an actual set change or
  frames missing a keyword: a no-op `keyword_add` would be undoable and undo
  would strip the existing keywords.
- Species requests go to `/api/encounters/species` one at a time, unpaired
  removes first, then replacements, then adds; each response's `encounters`
  is adopted and the burst relocated by photo ids (`locateBurstByPhotoIds`)
  before the next request, and the first failure stops the loop and reports
  what did and did not land.
- `rapidRefreshSpeciesKeywordState()` ignores a response whose refetch token,
  `openToken` or session index is stale, so a slow keyword lookup cannot
  overwrite a newer burst's snapshot.
- `changeQueue()` prompts when `hasStagedChanges()`; "apply" runs
  `applyCurrent(false)` and stays put if it fails. `beforeHistoryChange`
  refuses undo/redo while loading, applying, staged or prompting.
- The keydown handler swallows every key while the queue prompt is open and
  ignores `INPUT`, `SELECT`, `TEXTAREA` and contenteditable targets (the
  `#queueFilter` select needs its arrows and Enter).

The page has its own strict program in
`tsconfig.frontend-pages/pipeline-rapid-review.json`, with API/state shapes in
`types/pages/pipeline-rapid-review.d.ts`. It checks every implementation script,
including boot, and compile-only negative contracts. It inherits every strict
option, including unchecked indexed access, without implementation suppressions.

Annotations on indexed reads describe the existing dense-array loop bounds,
nonempty history checks, and seeded-session invariants. The DOM assertions refer
to elements required by this page's template; pointer listeners are bound to the
photo stage. Numeric textContent writes use the DOM setter's existing string
coercion. These annotations do not change executable JavaScript.

# Browse panel requests

Browse loads classic scripts in the order declared by `browse.html`, with
`boot.js` last. Existing page state is in `state.js`. Panel request ownership
is private to `panel-requests.js`, exposed through `Vireo.browse.panelRequests`:

| Lane | Owner |
| --- | --- |
| `keywords` | Multi-selection keyword suggestions in `selection-panel.js` |
| `predictions` | Multi-selection predictions in `prediction-panels.js` |
| `detailPredictions` | Single-photo predictions in `prediction-panels.js` |
| `predictionPhotos` | The prediction row's Show photos action |
| `wildlife` | Multi-selection wildlife inclusion state |

`lane.begin(key)` returns a request with `isCurrent()` and `fail()`. Repeating
the same key returns `null`, both during a request and after success. A current
failure releases the key so the same selection can retry; a stale failure
cannot clear a newer request's cache. Omit the key when every call should
supersede the previous one.

After each asynchronous read, check `request.isCurrent()` before changing
the panel. In a catch block, `request.fail()` returns true only for the current
request. `lane.invalidate()` drops pending results and cached selections;
call it when leaving a panel or when a mutation changes its data.

The Show photos action also captures `predictions.observe()` so a selection
change invalidates a pending lightbox open. It checks both owners between
500-photo batches. Single-photo predictions are invalidated when loading a
different detail view or entering batch mode. These lanes govern reads and
rendering; they do not cancel writes or server-side work.

Request races are tested against the actual controllers in
`vireo/tests/browse_panel_requests.cjs`, run by pytest's browser state tests.

## Selection-panel state and actions

`selection-panel-state.js` owns the keyword and prediction row snapshots in
`Vireo.browse.selectionPanel`. Keyword ids and prediction row indexes resolve
through this store; photo-id arrays are copied and frozen. Starting a new
suggestion request, leaving batch mode, or exceeding the selection cap clears
the old rows immediately. Prediction resets also retire the payload used by
Show more, while keeping the user's expanded/collapsed viewing preference.

`selection-panel-events.js` binds one delegated click listener to
`#selectionPanel` through `bindActions()`, called from `boot.js` before
bootstrap. Rendered buttons carry `data-selection-action` and a keyword id or
prediction row index. Species names and photo-id arrays stay in the store.
The listener ignores disabled buttons, detached elements and missing rows.
Rebinding is safe and does not install a second listener.

`selection-panel.js` renders keyword and wildlife controls and applies their
changes; `prediction-panels.js` renders prediction controls and performs their
actions. Existing grid selection and single-photo detail state still live in
`state.js`; these are separate from the batch panel's row snapshots.

## Dialog focus and keyboard lifecycle

Collection editing, batch keyword entry, and batch collection selection use
`VireoDialog.create(overlay, {initialFocus, onDismiss})` from
`/static/vireo-dialog.js`, loaded after `keymap.js` and before the page controllers.
The templates provide `role="dialog"`, `aria-modal`, and a title via
`aria-labelledby`. The controller moves focus inside on `open()`, recomputes
the Tab order as controls change, makes the background inert, and restores the
opener on `close()`. It preserves pre-existing inert state and uses Keymap's
shared Escape stack and scroll locks, including when managed dialogs stack.

Each owner's `onDismiss` calls its normal hide/cleanup function, which calls
`close()`. Save, Cancel, Escape, and other close paths must use that same function;
removing the `open` class directly would leave focus/inert/scroll state behind.
`open(returnFocus)` accepts an explicit opener for actions that fetch data before
opening and for pointer clicks (WebKit may not focus the clicked button). Focus is
synchronous, so no delayed callback can steal it after closing.
Existing batch collection choices are native buttons with `aria-pressed` state.

The lifecycle follows the [W3C modal dialog pattern](https://www.w3.org/WAI/ARIA/apg/patterns/dialog-modal/).
Real-DOM and Browse integration regressions live in
`tests/e2e/test_dialog_accessibility.py` and run in the Linux/Windows Chromium and
Mac WebKit PR browser gate.

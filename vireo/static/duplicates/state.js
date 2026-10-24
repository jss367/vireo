// Duplicates page state shared by the page's classic scripts: the scan's
// proposals and job, the latest scan result, the filter-bar match set and
// its readiness, and the Trash batch size.
// Classic page script; loads first among the Duplicates page scripts.

/** @type {DuplicateProposal[]} */
var _proposals = [];
/** @type {string | null} */
var _currentJobId = null;
// Set synchronously at the top of startScan() so the async tryRestoreLastScan
// can detect that a fresh scan is already underway and skip its render.
var _scanInProgress = false;

// Latest scan-result snapshot. Used for client-side bucket pruning
// after a successful bulk-resolve so we don't have to round-trip a
// full re-scan to refresh the UI.
/** @type {DuplicateScan | null} */
var _lastScanResult = null;
var _sameShotVisibleLimit = 50;

// Photo ids matching the filter bar's expression (null = no filters).
// Groups come from the library-wide scan and are never re-derived from
// the filtered set — the filter only decides which MEMBERS match; a
// matching member reveals its complete group so cross-workspace
// duplicate sets stay resolvable.
/** @type {Set<number> | null} */
var _filterMatchedIds = null;

// Bumped by every refreshFilterMatches() invocation. Slow queries (a
// visual clause on a large library, in particular) can still be in
// flight when the user edits the chips or hits Pause; a stale response
// must not overwrite _filterMatchedIds and re-render _lastScanResult
// for filters that are no longer shown — that would expose and enable
// apply/bulk-resolve on groups outside the current chips.
var _filterEpoch = 0;

// Resolves the first time ``refreshFilterMatches`` finishes (with or
// without active filters). ``tryRestoreLastScan`` awaits this so a
// persisted/deep-linked filter can't race the last-scan render and
// briefly expose groups the filter should hide. Both paths still work
// when no filter is active — the no-op branch of
// ``refreshFilterMatches`` still resolves this.
/** @type {(() => void) | null} */
var _filterReadyResolve = null;
/** @type {Promise<void>} */
var _filterReady = new Promise(function(resolve) { _filterReadyResolve = resolve; });

// Currently in-flight ``refreshFilterMatches`` promise, or null.
// Scan-complete's ``renderResults`` awaits this so a filter edit made
// mid-scan can't let the initial render use the pre-edit
// ``_filterMatchedIds`` — otherwise groups the newly-edited chips will
// hide would render with apply/bulk actions enabled for the seconds
// between scan-complete and the filter query finally settling.
/** @type {Promise<void> | null} */
var _pendingFilterRefresh = null;

// Keep each request small enough that the UI can report real progress even
// for libraries with thousands of duplicate files. Each completed request
// is also a durable checkpoint: a retry only includes cards that have not
// already been cleaned up in this browser session.
var TRASH_BATCH_SIZE = 25;

/** @param {string} id */
function duplicatesEl(id) {
  return /** @type {HTMLElement} */ (document.getElementById(id));
}

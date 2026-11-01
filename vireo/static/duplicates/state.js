// Duplicates page state: the scan's proposals and job, the latest scan
// result, the filter-bar match set and its readiness, the category and apply
// state, and the Trash and Keep bookkeeping. ES module: the page's other
// modules import `state` and read and write its fields; nothing here is a
// global.

/**
 * @typedef {object} DuplicatesState
 * @property {DuplicateProposal[]} proposals
 *   The groups on screen, indexed by each group checkbox's `data-gi`.
 * @property {string | null} currentJobId
 * @property {boolean} scanInProgress
 *   Set synchronously at the top of startScan() so the async
 *   tryRestoreLastScan can detect that a fresh scan is already underway and
 *   skip its render.
 * @property {DuplicateScan | null} lastScanResult
 *   Latest scan-result snapshot. Used for client-side bucket pruning after a
 *   successful bulk-resolve so we don't have to round-trip a full re-scan to
 *   refresh the UI.
 * @property {number} sameShotVisibleLimit
 * @property {boolean} sameShotShowReviewed
 * @property {boolean} sameShotShowDiffering
 * @property {number} sameShotDifferingVisibleLimit
 * @property {Set<string>} sameShotReviewPending
 * @property {SameShotGroup | null} sameShotComparison
 * @property {Set<number> | null} filterMatchedIds
 *   Photo ids matching the filter bar's expression (null = no filters).
 *   Groups come from the library-wide scan and are never re-derived from the
 *   filtered set — the filter only decides which MEMBERS match; a matching
 *   member reveals its complete group so cross-workspace duplicate sets stay
 *   resolvable.
 * @property {number} filterEpoch
 *   Bumped by every refreshFilterMatches() invocation. Slow queries (a visual
 *   clause on a large library, in particular) can still be in flight when the
 *   user edits the chips or hits Pause; a stale response must not overwrite
 *   filterMatchedIds and re-render lastScanResult for filters that are no
 *   longer shown — that would expose and enable apply/bulk-resolve on groups
 *   outside the current chips.
 * @property {(() => void) | null} filterReadyResolve
 * @property {Promise<void>} filterReady
 *   Resolves the first time refreshFilterMatches finishes (with or without
 *   active filters). tryRestoreLastScan awaits this so a persisted/deep-linked
 *   filter can't race the last-scan render and briefly expose groups the
 *   filter should hide. Both paths still work when no filter is active — the
 *   no-op branch of refreshFilterMatches still resolves this.
 * @property {Promise<void> | null} pendingFilterRefresh
 *   Currently in-flight refreshFilterMatches promise, or null. Scan-complete's
 *   renderResults awaits this so a filter edit made mid-scan can't let the
 *   initial render use the pre-edit filterMatchedIds — otherwise groups the
 *   newly-edited chips will hide would render with apply/bulk actions enabled
 *   for the seconds between scan-complete and the filter query finally
 *   settling.
 * @property {number} duplicateLoadEpoch
 *   Bumped by every last-scan restore and by cancelDuplicateLoad(); a restore
 *   whose epoch is stale drops its response.
 * @property {string} duplicateCategory
 *   The filename/folder pattern category being shown ('all' for every one).
 * @property {Set<string>} excludedDuplicateHashes
 *   Unresolved groups the user unchecked, kept across re-renders.
 * @property {boolean} duplicateApplyInProgress
 * @property {string} duplicateApplyMessage
 * @property {Set<string>} pendingBucketReveals
 *   Bucket folder sets (sorted JSON) whose reveal request is in flight.
 * @property {boolean} duplicateTrashActive
 *   A bulk "Move N extra copies to Trash" is running.
 * @property {Map<Element, {snapshot: DuplicateScan | null, index: number}>} duplicateBucketOperations
 *   Bucket cards pinned above the results while their Keep-for-all runs.
 * @property {Map<number, DuplicateTrashState>} duplicateFileTrashState
 *   Per-copy Trash progress, read by each card when it renders.
 * @property {Set<number>} duplicateKeepPending
 *   Copies of a group whose "Keep this copy instead" is in flight.
 */

/** @type {DuplicatesState} */
export const state = {
  proposals: [],
  currentJobId: null,
  scanInProgress: false,
  lastScanResult: null,
  sameShotVisibleLimit: 50,
  sameShotShowReviewed: false,
  sameShotShowDiffering: false,
  sameShotDifferingVisibleLimit: 50,
  sameShotReviewPending: new Set(),
  sameShotComparison: null,
  filterMatchedIds: null,
  filterEpoch: 0,
  filterReadyResolve: null,
  filterReady: Promise.resolve(),
  pendingFilterRefresh: null,
  duplicateLoadEpoch: 0,
  duplicateCategory: 'all',
  excludedDuplicateHashes: new Set(),
  duplicateApplyInProgress: false,
  duplicateApplyMessage: '',
  pendingBucketReveals: new Set(),
  duplicateTrashActive: false,
  duplicateBucketOperations: new Map(),
  duplicateFileTrashState: new Map(),
  duplicateKeepPending: new Set(),
};
state.filterReady = new Promise(function(resolve) { state.filterReadyResolve = resolve; });

// Keep each request small enough that the UI can report real progress even
// for libraries with thousands of duplicate files. Each completed request
// is also a durable checkpoint: a retry only includes cards that have not
// already been cleaned up in this browser session.
export const TRASH_BATCH_SIZE = 25;

/** @param {string} id */
export function duplicatesEl(id) {
  return /** @type {HTMLElement} */ (document.getElementById(id));
}

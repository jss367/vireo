// Shared results, scope, filters, and persisted view state.
// Classic page script; shared globals are initialized before boot.js runs.

/** @type {ReviewResults | null} */ var pipelineResults = null;
var activeFilter = 'all';
var minConfidence = 40;
var speciesFilter = '';
var speciesFilterText = '';
var speciesFilterSearchOptions = {matchCase: false, wholeWord: false};
var speciesDropdownSearchOptions = {matchCase: false, wholeWord: false};
var hideConfirmed = false;
var hideWithoutSuggestions = false;
// Captured from /api/pipeline/page-init so computeReviewNow() can apply
// the same workspace-override logic the happy path uses on page load.
/** @type {{review_min_confidence?: number} | null} */ var workspaceOverrides = null;
// Captured from /api/pipeline/page-init so computeReviewNow() can re-render
// the degraded-features banner using the current readiness snapshot
// (regroup-live doesn't return readiness, but the missing-features set is
// unchanged by computing the cache).
/** @type {ReviewReadiness | null} */ var reviewReadiness = null;
/** @type {ReviewCacheInfo | null} */ var resultsCacheInfo = null;
/** @type {ReviewResults | null} */ var cachedPipelineResults = null;
/** @type {ReviewCacheInfo | null} */ var cachedResultsCacheInfo = null;
var reviewScopeMode = 'cache';
/** @type {string | null} */ var reviewScopeCollectionId = null;
/** @type {ReviewCollection[]} */ var reviewScopeCollections = [];
/** @type {string | null} */ var reviewScopeFolderId = null;
/** @type {ReviewFolder[]} */ var reviewScopeFolders = [];
// Folder and Collection scopes filter the saved review to these photo IDs
// (null shows every photo). pipelineResults stays the full saved review, so
// edits and saves keep working while the scope is narrowed.
/** @type {Set<number> | null} */ var reviewScopePhotoIds = null;
// Photos in the chosen folder/collection, including ones the saved review
// does not cover, so the page can say how many were never reviewed.
/** @type {number | null} */ var reviewScopeTotal = null;
// True when the user asked to regroup the folder/collection on its own.
// That view comes from regroup-live and is view-only like Workspace scope.
var reviewScopeRegrouped = false;
var reviewScopeRequestSeq = 0;
// Separate counter for the Folder/Collection scope-photos membership load so
// that reflow/regroup-live slider requests (which must still be cancelled by
// a scope change through reviewScopeRequestSeq) don't invalidate an in-flight
// membership response. If the two shared a counter, a slider moved while
// scope membership was loading would drop the valid membership result and
// then render the tuning response against the empty pre-fetch scope set,
// leaving the filtered view at zero photos until the user reselected scope.
var reviewScopeMembershipSeq = 0;
var showPhotoLabels = false;
// Survives re-renders so confirming/filtering doesn't re-expand collapsed
// encounters. Keyed by the encounter's full sorted photo_id set so that
// detach/regroup (which change composition) naturally invalidate the key
// instead of letting it migrate to a sibling encounter that happens to
// share the original first photo_id.
/** @type {Set<string>} */ var collapsedEncounters = new Set();
var PIPELINE_SIDEBAR_STORAGE_KEY = 'vireo.pipelineReview.sidebarCollapsed';
var PIPELINE_VIEW_STATE_STORAGE_KEY = 'vireo.pipelineReview.viewState';
var PIPELINE_REVIEW_FILTERS = ['all', 'KEEP', 'REVIEW', 'REJECT', 'SPECIES_CONFLICT'];
var PIPELINE_ENCOUNTER_SORTS = ['default', 'photos_desc', 'photos_asc', 'bursts_desc', 'time_desc', 'time_asc'];
// Display order for the encounter list. Does NOT reorder currentReviewResults().encounters;
// renderResults() iterates a sorted list of original indices so every encounter's
// canonical index (focus, detach, species overrides, GRM) stays valid.
var encounterSort = 'default';

// Rendering and group actions require the selected snapshot or member to exist.
// Keep that invariant explicit at deferred callback and indexed-read boundaries.
/** @template T @param {T | null | undefined} value @returns {T} */
function requireReviewValue(value) {
  if (value == null) throw new Error('The selected review data is no longer available.');
  return value;
}

/** @returns {ReviewResults} */
function currentReviewResults() {
  return requireReviewValue(pipelineResults);
}

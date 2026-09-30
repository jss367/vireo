// Shared results, scope, filters, and persisted view state.
// Classic page script; shared globals are initialized before boot.js runs.

var pipelineResults = null;
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
var workspaceOverrides = null;
// Captured from /api/pipeline/page-init so computeReviewNow() can re-render
// the degraded-features banner using the current readiness snapshot
// (regroup-live doesn't return readiness, but the missing-features set is
// unchanged by computing the cache).
var reviewReadiness = null;
var resultsCacheInfo = null;
var cachedPipelineResults = null;
var cachedResultsCacheInfo = null;
var reviewScopeMode = 'cache';
var reviewScopeCollectionId = null;
var reviewScopeCollections = [];
var reviewScopeRequestSeq = 0;
var showPhotoLabels = false;
// Survives re-renders so confirming/filtering doesn't re-expand collapsed
// encounters. Keyed by the encounter's full sorted photo_id set so that
// detach/regroup (which change composition) naturally invalidate the key
// instead of letting it migrate to a sibling encounter that happens to
// share the original first photo_id.
var collapsedEncounters = new Set();
var PIPELINE_SIDEBAR_STORAGE_KEY = 'vireo.pipelineReview.sidebarCollapsed';
var PIPELINE_VIEW_STATE_STORAGE_KEY = 'vireo.pipelineReview.viewState';
var PIPELINE_REVIEW_FILTERS = ['all', 'KEEP', 'REVIEW', 'REJECT', 'SPECIES_CONFLICT'];
var PIPELINE_ENCOUNTER_SORTS = ['default', 'photos_desc', 'photos_asc', 'bursts_desc', 'time_desc', 'time_asc'];
// Display order for the encounter list. Does NOT reorder pipelineResults.encounters;
// renderResults() iterates a sorted list of original indices so every encounter's
// canonical index (focus, detach, species overrides, GRM) stays valid.
var encounterSort = 'default';

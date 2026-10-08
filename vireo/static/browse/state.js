/* Browse: page state shared across the browse scripts.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* Google Maps API key — populated from /api/config below (window._cfgPromise).
   Empty string when no key is configured; the Location section then
   degrades to free-text-only (no autocomplete, no script injection). */
window.GOOGLE_MAPS_API_KEY = '';
window.GOOGLE_MAPS_PREFER_ENGLISH = true;

/* ---------- State ---------- */
var photos = [];
var totalPhotos = 0;
var totalUnderlyingPhotos = 0;
var totalBrowseStacks = 0;
var currentPage = 1;
var perPage = 50;
var loading = false;
var allLoaded = false;
var earliestPage = 1;
// True only after an authoritative response has initialized the grid, even
// when that response contains zero photos. Health events can arrive while
// bootstrap or a deep link is still pending; an "unaffected scope" is only
// safe to preserve once there is an initialized dataset to preserve.
var browseDatasetReady = false;
var selectedPhotoId = null;
var selectedIndex = -1;
// The card the user clicked most recently. Usually selectedPhotoId, but a
// cmd-click can fold the focused photo into selectedPhotos and null the focus
// id, and that card is still the one the user is looking at.
var lastClickedPhotoId = null;
var anchorRestoreEpoch = 0;
var anchorScanDepth = 0;
// Focused reloads (sort changes) capture the selection, then clear it and
// tear down the cards before their async page fetch settles. If the user
// changes sort a second time while the first request is in flight, the DOM
// capture the second call needs is already gone — it would clear
// ``selectedPhotoId`` and reload from page 1, losing the selection the
// feature exists to preserve. Hold the in-flight target here so a newer
// focused reload can pick up the older one's anchor (Codex review
// r4021334323). Cleared once every in-flight focused reload has settled.
var pendingFocusAnchor = null;
// Which photo the last focused load was actually placed by (see loadPhotos).
var browseResolvedFocusPhotoId = null;
// How many photos one focused lookup may be asked about, matching
// ``MAX_FOCUS_PHOTO_IDS`` on the server. Requests are kept inside it rather
// than allowed to fail: see buildBrowsePageRequest.
var BROWSE_MAX_FOCUS_CANDIDATES = 200;
// The scope the pending anchor belongs to. Sidebar folder/keyword/collection
// clicks bump ``browseScopeGen``; the photo then belongs to the view the user
// left, so the anchor must not survive into the new one.
var pendingFocusAnchorScopeGen = -1;
var focusReloadInFlight = 0;
var selectedPhotos = new Set(); // multi-select
var expandedBrowseStacks = new Set();
var browseStackMembers = {};
var browseStackErrors = {};
var browseStackHydrationSeq = 0;
// In-flight cover hydrations, keyed by cover id: {seq, memberIds, stale}.
// Same invalidation contract as browseStackExpansionRequests below (a mutation
// marks any request whose members it just changed), but a different ownership
// rule: only one hydration may own a cover key at a time, so `seq` lets a newer
// reconciliation supersede an older one's response instead of both caching.
var browseStackHydrationRequests = {};
// Cover ids whose reconciliation could not finish (the members never loaded).
// The grid is still showing the pre-edit cover for these stacks, so the badge
// says so and expanding one re-runs the reconciliation from the loaded members.
var browseStackCoverRecheck = new Set();
// In-flight stack expansions, keyed by cover id: {memberIds, stale}.
// A tray's header offers "Select all" the moment it opens, so a stack-wide
// edit can be applied while its members are still loading. Those members are
// not in `photos` and not yet in `browseStackMembers`, so findBrowsePhoto()
// cannot patch them — the edit has nowhere to land. Installing the response
// that was already in flight would then repaint pre-edit values in the tray
// and its badges, silently contradicting the edit the user just confirmed.
// markBrowseStackExpansionsStale() flags such a request so toggleBrowseStack
// discards it and refetches instead of guessing at the post-edit state.
//
// Each cacheKey stores a list, not a single request: collapsing does not
// cancel an in-flight expansion, and a subsequent re-expand starts a new
// attempt. Tracking every pending request means a mid-flight edit reaches
// all of them, so an untracked earlier response cannot cache pre-edit
// members while only the newest request was marked stale. Codex P2 on
// PR #1561.
var browseStackExpansionRequests = {};

function _pushBrowseStackExpansionRequest(cacheKey, request) {
  var list = browseStackExpansionRequests[cacheKey];
  if (!list) {
    list = [];
    browseStackExpansionRequests[cacheKey] = list;
  }
  list.push(request);
}

function _removeBrowseStackExpansionRequest(cacheKey, request) {
  var list = browseStackExpansionRequests[cacheKey];
  if (!list) return;
  var idx = list.indexOf(request);
  if (idx >= 0) list.splice(idx, 1);
  if (!list.length) delete browseStackExpansionRequests[cacheKey];
}

function setBrowseTotals(data) {
  totalPhotos = Number(data && data.total) || 0;
  totalBrowseStacks = Number(data && data.stack_count) || 0;
  totalUnderlyingPhotos = data && data.underlying_total != null
    ? Number(data.underlying_total) || 0
    : totalPhotos;
}
var selectionKeywordMissingById = {};
var selectionKeywordPresentById = {};
var selectionKeywordNameById = {};
// Panel request ownership lives in Vireo.browse.panelRequests.
// A photo can carry dozens of low-confidence guesses, and most users never set
// a confidence floor. Show the strongest few and collapse the tail behind a
// counted "show all" rather than either flooding the panel or inventing a
// threshold the user's settings don't specify.
var PREDICTION_COLLAPSE_AT = 5;
var detailPredictionsExpanded = false;
var selectionPredictionsExpanded = false;
var _detailPredictionData = null;
var _selectionPredictionData = null;
var selectionPredictionAcceptableById = {};
// Parallel to selectionPredictionAcceptableById, so the accept call can pass
// the species the button named — the server refuses a row whose current
// consensus has drifted from what the panel rendered (see
// _species_drifted_prediction_ids in app.py).
var selectionPredictionSpeciesByIdx = {};
// Also parallel: the photos the row's "Predicted on N of M" counts, so the
// Show button can open exactly that set in the lightbox. Kept in the side
// table rather than re-derived from the accept ids because those exclude the
// ambiguous photos — and the ambiguous ones are precisely the photos a user
// clicks Show to look at.
var selectionPredictionPhotoIdsByIdx = {};
// The single-photo panel's rendered rows, in render order, so its buttons can
// carry a bare index instead of the row's data. The selection panel has kept
// its ids and species in a side table since it shipped
// (selectionPredictionAcceptableById above); the detail panel now matches it,
// for a reason worth stating: it used to interpolate the species straight
// into an inline handler attribute —
// `onclick='acceptDetailPredictions([12],44,"Say\'s Phoebe")'` — and the
// apostrophe closed the attribute. Accept was broken outright for Say's
// Phoebe, Cooper's Hawk, Steller's Jay, Bewick's Wren, Swainson's Hawk,
// Wilson's Warbler: possessive common names are ordinary in North American
// birds, which is most of what this app is pointed at. Escaping that one
// interpolation would have fixed that one line; keeping row data out of the
// markup is what stops the next button added here from reintroducing it.
var detailPredictionGroups = [];
var detailPredictionGroupsPhotoId = null;
var showDetectionBoxes = false;
var cardFields = ["filename", "location_status", "rating", "flag", "sharpness"]; // default, overridden by config
var inatSubmitted = {};  // {photo_id: true}
var colorLabels = {};    // {photo_id: color_string}
// Which photo ids we've confirmed color-label state for. `colorLabels[id]`
// missing is ambiguous — either the photo has no color set OR we haven't
// fetched its color labels yet — so the batch inspector needs this to avoid
// rendering "everything is uncoloured" for a still-loading selection.
var colorLabelsFetched = new Set();
var colorLabelGen = 0;   // bumped on local edits; guards against stale fetch responses
// Per-id record of the generation each id was last locally edited at, so a
// slow fetch that started before an edit still cannot overwrite that edit for
// the specific ids it touched. `colorLabelGen === gen` alone catches only the
// nothing-changed case; a concurrent edit to one id would otherwise let the
// stale response's values for every other id overwrite them anyway, and the
// stale value for the edited id survive.
var colorLabelEditGen = Object.create(null);
// Stamp ids at a NEW generation. Called twice per write — once before the POST
// and once after it lands. The trailing stamp is what covers a fetch that
// starts while the write is still in flight: it captured the same generation
// the pre-write stamp wrote, so `>` alone would let its response (read from the
// server before the write committed) overwrite the fresh local value once it
// resolves. Re-stamping on completion puts every such fetch strictly behind
// the write (Codex P2 on PR #1668).
function _noteColorLabelEdits(ids) {
  colorLabelGen++;
  ids.forEach(function(id) { colorLabelEditGen[id] = colorLabelGen; });
}

// Re-ask the server when a write fails. Without this the failed write leaves
// the worst of both: its stamp makes any fetch that started before it skip
// these ids, while that fetch still adds them to colorLabelsFetched — so a
// labelled photo the user edited before initial hydration finished reads as a
// definite "no color" until reload (Codex P2 on PR #1668).
//
// The refetch alone is the whole repair; the stamp must NOT be cleared. Stamps
// only ever advance through _noteColorLabelEdits, so a stamp is never greater
// than the generation this refetch captures and can never block its own
// response. Clearing it would instead un-guard a *concurrent* write: with
// overlapping writes on one photo, an earlier request failing after a later one
// succeeded would drop the successful write's completion stamp, letting a GET
// issued before either of them overwrite the label the user actually chose
// (Codex P2, second pass). Dropping the fetched marker is kept — until the
// refetch lands we genuinely do not know this photo's color, and the batch
// inspector should say so rather than assert "no color".
function _recoverColorLabelsAfterFailedWrite(ids) {
  ids.forEach(function(id) { colorLabelsFetched.delete(id); });
  return fetchColorLabels(ids).then(function() {
    refreshGridCards(ids);
    refreshExpandedBrowseStackMembers(ids);
    // fetchColorLabels already re-rendered a batch inspector; only the
    // single-photo panel is left to catch up.
    var detail = document.getElementById('detailContent');
    if (selectedPhotoId != null && ids.indexOf(selectedPhotoId) !== -1
        && !(detail && detail.classList.contains('batch-mode'))) {
      updateDetailColors();
    }
  });
}
// Generation counter for the grid window (photos/currentPage/earliestPage/
// allLoaded). Never compare it directly — go through claimBrowseWindow() /
// observeBrowseWindow() so every async path guards the same way.
var loadEpoch = 0;
var selectAllRequestSeq = 0; // bumped to ignore stale async select-all responses
// rating/flag/color/date/keyword filter state now lives in VireoFilter
// (the universal filter bar); Browse keeps only page-scope state.
var activeFolderId = null;
var activeKeyword = null;
var activeCollectionId = null;
// A Dashboard deep link can combine a collection with date/keyword filters.
// Normal collection clicks keep the historical collection-only behavior.
var dashboardCollectionScope = false;
// Which saved collection (if any) is currently loaded into the filter bar
// as editable chips. Phase 5's ``filterByCollection`` clears
// ``activeCollectionId`` when it hands off to the filter bar (because the
// expression IS the filter and the historical "collection endpoint" mode
// is gone), so we need a separate handle to reload the expression after
// membership edits — refreshActiveCollectionAfterMembershipChange used to
// only reload the sidebar counts, leaving a stale photo_ids list in the
// bar until the user reopened the collection (CodeRabbit review
// r3620473562). Cleared by the filter bar's onChange when the user edits
// the expression (so a user-edited chip set isn't silently reverted to
// the saved collection on the next membership refresh).
var openedCollectionId = null;
// A saved collection's logical membership includes photos whose storage is
// offline. They stay hidden by default and, when revealed, render as read-only
// cards; selection endpoints remain accessible-only.
var showOfflineCollectionPhotos = false;
var collectionInventoryTotal = 0;
var collectionAvailableTotal = 0;
var collectionOfflineTotal = 0;
// Sidebar collections render before the universal filter registry necessarily
// finishes loading. Keep the init promise so an early collection click can
// wait for the filter bar instead of being silently discarded.
var browseFilterInitPromise = null;
// Monotonic counter bumped by every sidebar scope switch
// (filterByFolder/filterByKeyword/filterByCollection). A queued collection
// open captures the gen at entry and aborts on resume if a later scope
// change has advanced it — otherwise the queued click would clobber the
// user's newer selection (Codex review r3624395785).
var browseScopeGen = 0;

var timelineMode = false;
var calendarYear = new Date().getFullYear();
var selectedDay = null;
var calendarData = null;
// CLIP search is now the filter bar's visual clause (VireoFilter.getVisual).

var bestBatchData = null;
var keywordAutocompleteCache = null;
var keywordAutocompletePromise = null;
var keywordAutocompleteStates = {};
var collectionCountRefreshTimer = null;
var collectionCountLoadGen = 0;
// Same superseded-response guard as loadCollectionCounts, extended to the
// summary and calendar loaders so a slow /api/browse/summary or
// /api/photos/calendar response cannot overwrite the panel or heatmap
// after a newer scope selection triggered a fresh load
// (CodeRabbit review r3684913398).
var summaryLoadGen = 0;
var summaryLoadStates = {};
var summaryRenderDecisionGen = 0;
var calendarDataLoadGen = 0;
var calendarDataLoadStates = {};
var calendarRenderDecisionGen = 0;
// Server-side cancellation lanes for the same two loaders. The sequence
// advances only when the params change: a reload with unchanged params can
// still be rendered from an older response, so it must not cancel one.
var summaryLane = { key: null, seq: 0 };
var calendarLane = { key: null, seq: 0 };
function searchLaneSeq(lane, key) {
  if (lane.key !== key) {
    lane.key = key;
    lane.seq++;
  }
  return lane.seq;
}
// Internal generation counters shared across every caller of the sidebar
// loaders. Without these, ``refreshBrowseSidebarCounts()`` and the various
// mutation-triggered ``loadKeywords()``/``loadCollections()`` calls fire
// without any freshness guard, so a slow pre-transition response can arrive
// after a guarded ``refreshBrowseAfterFolderHealthChange`` render and
// repaint the sidebar with data that no longer reflects the current health
// state — reintroducing the offline-folder-in-tree symptom the health
// refresh was meant to fix (Codex review r3686842772). Every loader checks
// its captured generation after ``await`` and skips ``render*`` when a
// newer call has since started, so last-started always wins regardless of
// which call site issued it.
var folderLoadGen = 0;
var folderLoadStates = {};
var folderRenderDecisionGen = 0;
var keywordLoadGen = 0;
var keywordLoadStates = {};
var keywordRenderDecisionGen = 0;
var collectionLoadGen = 0;
var collectionLoadStates = {};
var collectionRenderDecisionGen = 0;
var browseFolderRows = [];
// The workspace ID whose folder tree is currently reflected in the DOM.
// Captured atomically with the tree from /api/browse/init and refreshed
// from the same server snapshot on every /api/folders?with_workspace=1
// response, so ``removeWorkspaceRootFromBrowse`` can bind its DELETE to
// the workspace the user actually saw. Trusting ``/api/workspaces/active``
// at click time would let a cross-tab workspace switch redirect the
// destructive call to the wrong workspace (Codex review r3798912101).
var browseWorkspaceId = null;
var collectionsById = {};

/* Browse: page state, the constants the page's modules share, and the small
   helpers that keep that state consistent. ES module (see browse/README.md).

   An ES module cannot assign another module's variable, so every value more
   than one module writes, and the rest of the page's mutable state, is a
   field of `state` (`state.selectedPhotoId = id`). This module imports
   nothing, so it is evaluated first and `state` exists before any other
   page module runs. */

/* ---------- State ---------- */
export const state = {
  // Google Maps API key — populated from /api/config (config.js's _cfgPromise).
  // Empty string when no key is configured; the Location section then
  // degrades to free-text-only (no autocomplete, no script injection).
  googleMapsApiKey: '',
  googleMapsPreferEnglish: true,
  /** @type {BrowsePhoto[]} */
  photos: [],
  totalPhotos: 0,
  totalUnderlyingPhotos: 0,
  totalBrowseStacks: 0,
  currentPage: 1,
  perPage: 50,
  loading: false,
  allLoaded: false,
  earliestPage: 1,
  // True only after an authoritative response has initialized the grid, even
  // when that response contains zero photos. Health events can arrive while
  // bootstrap or a deep link is still pending; an "unaffected scope" is only
  // safe to preserve once there is an initialized dataset to preserve.
  browseDatasetReady: false,
  /** @type {number | null} */
  selectedPhotoId: null,
  // The photo the single-photo detail panel shows; null while it is hidden or
  // showing the batch inspector. In-flight detail requests compare against it
  // as their owner check.
  /** @type {number | null} */
  detailPhotoId: null,
  selectedIndex: -1,
  // The card the user clicked most recently. Usually selectedPhotoId, but a
  // cmd-click can fold the focused photo into selectedPhotos and null the focus
  // id, and that card is still the one the user is looking at.
  /** @type {number | null} */
  lastClickedPhotoId: null,
  anchorRestoreEpoch: 0,
  anchorScanDepth: 0,
  // Focused reloads (sort changes) capture the selection, then clear it and
  // tear down the cards before their async page fetch settles. If the user
  // changes sort a second time while the first request is in flight, the DOM
  // capture the second call needs is already gone — it would clear
  // ``selectedPhotoId`` and reload from page 1, losing the selection the
  // feature exists to preserve. Hold the in-flight target here so a newer
  // focused reload can pick up the older one's anchor (Codex review
  // r4021334323). Cleared once every in-flight focused reload has settled.
  /** @type {BrowseAnchor | null} */
  pendingFocusAnchor: null,
  // Which photo the last focused load was actually placed by (see loadPhotos).
  /** @type {number | null} */
  browseResolvedFocusPhotoId: null,
  // The scope the pending anchor belongs to. Sidebar folder/keyword/collection
  // clicks bump ``browseScopeGen``; the photo then belongs to the view the user
  // left, so the anchor must not survive into the new one.
  pendingFocusAnchorScopeGen: -1,
  focusReloadInFlight: 0,
  /** @type {Set<number>} */
  selectedPhotos: new Set(), // multi-select
  /** @type {Set<number>} */
  expandedBrowseStacks: new Set(),
  /** @type {Record<string, BrowsePhoto[]>} */
  browseStackMembers: {},
  /** @type {Record<string, string>} */
  browseStackErrors: {},
  browseStackHydrationSeq: 0,
  // In-flight cover hydrations, keyed by cover id: {seq, memberIds, stale}.
  // Same invalidation contract as browseStackExpansionRequests below (a mutation
  // marks any request whose members it just changed), but a different ownership
  // rule: only one hydration may own a cover key at a time, so `seq` lets a newer
  // reconciliation supersede an older one's response instead of both caching.
  /** @type {Record<string, BrowseStackHydration>} */
  browseStackHydrationRequests: {},
  // Cover ids whose reconciliation could not finish (the members never loaded).
  // The grid is still showing the pre-edit cover for these stacks, so the badge
  // says so and expanding one re-runs the reconciliation from the loaded members.
  /** @type {Set<number>} */
  browseStackCoverRecheck: new Set(),
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
  /** @type {Record<string, BrowseStackRequest[]>} */
  browseStackExpansionRequests: {},
  detailPredictionsExpanded: false,
  /** @type {{data: DetailPredictionsResponse, photoId: number} | null} */
  _detailPredictionData: null,
  // The single-photo panel's rendered rows, in render order, so its buttons can
  // carry a bare index instead of the row's data. The selection panel uses
  // selectionPanel for its row snapshots; the detail panel matches it,
  // for a reason worth stating: it used to interpolate the species straight
  // into an inline handler attribute —
  // `onclick='acceptDetailPredictions([12],44,"Say\'s Phoebe")'` — and the
  // apostrophe closed the attribute. Accept was broken outright for Say's
  // Phoebe, Cooper's Hawk, Steller's Jay, Bewick's Wren, Swainson's Hawk,
  // Wilson's Warbler: possessive common names are ordinary in North American
  // birds, which is most of what this app is pointed at. Escaping that one
  // interpolation would have fixed that one line; keeping row data out of the
  // markup is what stops the next button added here from reintroducing it.
  /** @type {BrowsePredictionGroup[]} */
  detailPredictionGroups: [],
  /** @type {number | null} */
  detailPredictionGroupsPhotoId: null,
  showDetectionBoxes: false,
  cardFields: ["filename", "location_status", "rating", "flag", "sharpness"], // default, overridden by config
  /** @type {Record<string, boolean>} */
  inatSubmitted: {}, // {photo_id: true}
  /** @type {Record<string, string>} */
  colorLabels: {}, // {photo_id: color_string}
  // Which photo ids we've confirmed color-label state for. `colorLabels[id]`
  // missing is ambiguous — either the photo has no color set OR we haven't
  // fetched its color labels yet — so the batch inspector needs this to avoid
  // rendering "everything is uncoloured" for a still-loading selection.
  /** @type {Set<number>} */
  colorLabelsFetched: new Set(),
  colorLabelGen: 0, // bumped on local edits; guards against stale fetch responses
  // Per-id record of the generation each id was last locally edited at, so a
  // slow fetch that started before an edit still cannot overwrite that edit for
  // the specific ids it touched. `colorLabelGen === gen` alone catches only the
  // nothing-changed case; a concurrent edit to one id would otherwise let the
  // stale response's values for every other id overwrite them anyway, and the
  // stale value for the edited id survive.
  /** @type {Record<string, number>} */
  colorLabelEditGen: Object.create(null),
  // Generation counter for the grid window (photos/currentPage/earliestPage/
  // allLoaded). Never compare it directly — go through claimBrowseWindow() /
  // observeBrowseWindow() so every async path guards the same way.
  loadEpoch: 0,
  selectAllRequestSeq: 0, // bumped to ignore stale async select-all responses
  // rating/flag/color/date/keyword filter state now lives in VireoFilter
  // (the universal filter bar); Browse keeps only page-scope state.
  /** @type {number | null} */
  activeFolderId: null,
  /** @type {string | null} */
  activeKeyword: null,
  /** @type {number | null} */
  activeCollectionId: null,
  // A Dashboard deep link can combine a collection with date/keyword filters.
  // Normal collection clicks keep the historical collection-only behavior.
  dashboardCollectionScope: false,
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
  /** @type {number | null} */
  openedCollectionId: null,
  // A saved collection's logical membership includes photos whose storage is
  // offline. They stay hidden by default and, when revealed, render as read-only
  // cards; selection endpoints remain accessible-only.
  showOfflineCollectionPhotos: false,
  collectionInventoryTotal: 0,
  collectionAvailableTotal: 0,
  collectionOfflineTotal: 0,
  // Sidebar collections render before the universal filter registry necessarily
  // finishes loading. Keep the init promise so an early collection click can
  // wait for the filter bar instead of being silently discarded.
  /** @type {Promise<unknown> | null} */
  browseFilterInitPromise: null,
  // Monotonic counter bumped by every sidebar scope switch
  // (filterByFolder/filterByKeyword/filterByCollection). A queued collection
  // open captures the gen at entry and aborts on resume if a later scope
  // change has advanced it — otherwise the queued click would clobber the
  // user's newer selection (Codex review r3624395785).
  browseScopeGen: 0,
  timelineMode: false,
  calendarYear: new Date().getFullYear(),
  /** @type {string | null} */
  selectedDay: null,
  /** @type {BrowseCalendar | null} */
  calendarData: null,
  /** @type {BrowseBestBatch | null} */
  bestBatchData: null,
  /** @type {BrowseKeywordOption[] | null} */
  keywordAutocompleteCache: null,
  /** @type {Promise<BrowseKeywordOption[]> | null} */
  keywordAutocompletePromise: null,
  /** @type {Record<string, BrowseKeywordAutocomplete>} */
  keywordAutocompleteStates: {},
  /** @type {number | null} */
  collectionCountRefreshTimer: null,
  collectionCountLoadGen: 0,
  // Same superseded-response guard as loadCollectionCounts, extended to the
  // summary and calendar loaders so a slow /api/browse/summary or
  // /api/photos/calendar response cannot overwrite the panel or heatmap
  // after a newer scope selection triggered a fresh load
  // (CodeRabbit review r3684913398).
  summaryLoadGen: 0,
  /** @type {Record<string, BrowseLoadState<BrowseSummary>>} */
  summaryLoadStates: {},
  summaryRenderDecisionGen: 0,
  calendarDataLoadGen: 0,
  /** @type {Record<string, BrowseLoadState<BrowseCalendar>>} */
  calendarDataLoadStates: {},
  calendarRenderDecisionGen: 0,
  // Server-side cancellation lanes for the same two loaders. The sequence
  // advances only when the params change: a reload with unchanged params can
  // still be rendered from an older response, so it must not cancel one.
  /** @type {BrowseSearchLane} */
  summaryLane: { key: null, seq: 0 },
  /** @type {BrowseSearchLane} */
  calendarLane: { key: null, seq: 0 },
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
  folderLoadGen: 0,
  /** @type {Record<string, BrowseLoadState<BrowseFolder[]>>} */
  folderLoadStates: {},
  folderRenderDecisionGen: 0,
  keywordLoadGen: 0,
  /** @type {Record<string, BrowseLoadState<BrowseKeyword[]>>} */
  keywordLoadStates: {},
  keywordRenderDecisionGen: 0,
  collectionLoadGen: 0,
  /** @type {Record<string, BrowseLoadState<BrowseCollection[]>>} */
  collectionLoadStates: {},
  collectionRenderDecisionGen: 0,
  /** @type {BrowseFolder[]} */
  browseFolderRows: [],
  // The workspace ID whose folder tree is currently reflected in the DOM.
  // Captured atomically with the tree from /api/browse/init and refreshed
  // from the same server snapshot on every /api/folders?with_workspace=1
  // response, so ``removeWorkspaceRootFromBrowse`` can bind its DELETE to
  // the workspace the user actually saw. Trusting ``/api/workspaces/active``
  // at click time would let a cross-tab workspace switch redirect the
  // destructive call to the wrong workspace (Codex review r3798912101).
  /** @type {number | null} */
  browseWorkspaceId: null,
  /** @type {Record<string, BrowseCollection>} */
  collectionsById: {},
  // Bumped by every folder-health refresh. Bootstrap snapshots it before its
  // first await and compares after, so it must hold a number from the start:
  // an ``undefined`` snapshot once made every normal load look like a health
  // change happened during init, and Browse rendered empty (Codex review
  // r3686317674).
  folderHealthRefreshSeq: 0,
  // Track the currently-running refresh so awaiters (the photo deep-link
  // loader in particular) can wait for it to settle before re-snapshotting
  // their generation counter and retrying. Without this, a health event
  // that fires while ``?photo_id=…`` is awaiting ``_cfgPromise`` /
  // ``/api/photos/<id>`` / ``/api/browse/init`` used to cancel the deep
  // link permanently — the refresh has no idea what folder the target
  // photo lives in, so Browse fell back to the unscoped workspace grid
  // without scrolling to the requested photo (and VireoFilter was left
  // uninitialized) (Codex review r3686778061).
  _activeFolderHealthRefresh: Promise.resolve(),
  // The selection as it stood before the first click of the most recent click
  // sequence. Captured in ``selectPhoto`` and consulted by the grid double-click
  // handler so it can tell "these clicks made the selection" from "these clicks
  // reaffirmed a selection the user already had": a deliberate tray Select all
  // + collapse leaves the stack members in ``selectedPhotos``, and a plain
  // click on the collapsed cover then sets the same set again, so the
  // resulting ``selectedPhotos`` is byte-for-byte identical either way. Without
  // this snapshot the dblclick handler would mark that pre-existing batch as
  // gesture-generated and the close handler would then replace it with the
  // finished-on photo. Codex P2 on PR #1672.
  /** @type {Set<number> | null} */
  browseSelectionBeforeStackDblClickStart: null,
};

/* ---------- Constants and state helpers ---------- */
// Request limits. maxFocusCandidates: how many photos one focused lookup may
// be asked about, matching ``MAX_FOCUS_PHOTO_IDS`` on the server. Requests are
// kept inside it rather than allowed to fail: see buildBrowsePageRequest. A
// field, so a browser test can lower it.
export const browseLimits = {maxFocusCandidates: 200};

export function _pushBrowseStackExpansionRequest(/** @type {string} */ cacheKey, /** @type {BrowseStackRequest} */ request) {
  var list = state.browseStackExpansionRequests[cacheKey];
  if (!list) {
    list = [];
    state.browseStackExpansionRequests[cacheKey] = list;
  }
  list.push(request);
}

export function _removeBrowseStackExpansionRequest(/** @type {string} */ cacheKey, /** @type {BrowseStackRequest} */ request) {
  var list = state.browseStackExpansionRequests[cacheKey];
  if (!list) return;
  var idx = list.indexOf(request);
  if (idx >= 0) list.splice(idx, 1);
  if (!list.length) delete state.browseStackExpansionRequests[cacheKey];
}

export function setBrowseTotals(/** @type {BrowseTotals | null} */ data) {
  state.totalPhotos = Number(data && data.total) || 0;
  state.totalBrowseStacks = Number(data && data.stack_count) || 0;
  state.totalUnderlyingPhotos = data && data.underlying_total != null
    ? Number(data.underlying_total) || 0
    : state.totalPhotos;
}
// Panel request ownership lives in panelRequests.
// A photo can carry dozens of low-confidence guesses, and most users never set
// a confidence floor. Show the strongest few and collapse the tail behind a
// counted "show all" rather than either flooding the panel or inventing a
// threshold the user's settings don't specify.
export var PREDICTION_COLLAPSE_AT = 5;
// Stamp ids at a NEW generation. Called twice per write — once before the POST
// and once after it lands. The trailing stamp is what covers a fetch that
// starts while the write is still in flight: it captured the same generation
// the pre-write stamp wrote, so `>` alone would let its response (read from the
// server before the write committed) overwrite the fresh local value once it
// resolves. Re-stamping on completion puts every such fetch strictly behind
// the write (Codex P2 on PR #1668).
export function _noteColorLabelEdits(/** @type {number[]} */ ids) {
  state.colorLabelGen++;
  ids.forEach(function(id) { state.colorLabelEditGen[id] = state.colorLabelGen; });
}


// CLIP search is now the filter bar's visual clause (VireoFilter.getVisual).

export function searchLaneSeq(/** @type {BrowseSearchLane} */ lane, /** @type {string} */ key) {
  if (lane.key !== key) {
    lane.key = key;
    lane.seq++;
  }
  return lane.seq;
}

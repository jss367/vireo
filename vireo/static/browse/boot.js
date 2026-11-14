/* Browse: page startup, the entry module browse.html loads. Importing it
   loads the rest of the page's modules; this file then composes the filter
   bar row, binds the markup's handlers, registers the page hooks and starts
   the page (bootstrap, folder-health subscription, infinite scroll, photo deep
   link). Every module it imports has been evaluated by the time its body
   runs. ES module (see browse/README.md). */
import { pagination } from './pagination.js';
import { selection } from './selection-state.js';
import { state } from './state.js';
import { claimBrowseWindow } from './window-requests.js';
import {
  VireoBrowseCompare, VireoFilter, VireoViewPreferences, _reconcileMissingFoldersInitSnapshot,
  missingFoldersSnapshotVersion, preferredScrollBehavior, registerPageHooks, safeFetch,
  showToast,
} from './shared.js';
import { loadUntilPhotoRendered } from './anchors.js';
import {
  addToCollection, batchAddKeyword, batchDelete, batchSetFlag, batchSetRating,
  openCaptureTimeModal,
} from './batch-actions.js';
import { hydrateColorLabelsForRenderedPage } from './cards.js';
import { _cfgPromise, applyBrowseConfig } from './config.js';
import {
  copyPhotoPaths, revealPhoto, setColorLabelFor, setFlagFor, setRatingFor,
} from './context-menus.js';
import { closeDetail, loadSummary } from './detail.js';
import { refreshPendingSyncBanner, setWildlifeExcludedFor } from './edit-actions.js';
import {
  applyExportPreset, buildExportPreflightRequest, markExportCustom, openExportModal,
  openPhotoExportModal, selectedExportMetadataFields, updateExportFormatControls,
  updateExportPreview,
} from './export.js';
import {
  refreshBrowseAfterFolderHealthChange, waitForFolderHealthRefreshesToSettle,
} from './folder-health.js';
import { getGridCard, renderGrid } from './grid.js';
import { moveBrowseSelection } from './keyboard.js';
import { openBrowseShortcutPhoto, removeDeletedLightboxPhoto } from './lightbox.js';
import {
  onBrowseFilterChange, resetAndLoad,
  updatePreviousPhotosButton,
} from './loading.js';
import { afterHistoryChange } from './prediction-panels.js';
import { ensureViewportHydrated, observer, rearmInfiniteScrollObserver } from './scroll.js';
import { bindBrowseMarkup } from './markup.js';
import { bindSelectionPanelActions } from './selection-panel-events.js';
import { browseCompare, openBrowseCompare } from './selection-panel.js';
import { getActiveSelection } from './selection.js';
import {
  filterByCollection, loadCollectionCounts, loadCollections, renderCollectionList,
  syncSidebarHighlights,
} from './sidebar-collections.js';
import { renderFolderTree } from './sidebar-folders.js';
import { renderKeywordTree } from './sidebar-keywords.js';
import { browseStacksEnabled } from './stacks.js';
import { updateFilterSummary, updateThumbSize } from './view-controls.js';
import { bindBrowseHistogram } from './histogram.js';

// The desktop build resolves template includes only, so compose the filter
// bar's shared row (sort, size, stacks, boxes, calendar) in the browser.
/** @type {HTMLElement} */ (document.querySelector('#vireoFilterBar .vf-secondary')).appendChild(
  /** @type {HTMLElement} */ (document.querySelector('.browse-view-controls'))
);

/* ---------- Page hooks ---------- */
// The shared navbar and lightbox call these through Vireo.pageHook (see
// vireo-page-hooks.js): the native menu's photo commands, the lightbox's
// rating/flag/label/export menu and Delete, and undo/redo. Register them
// before the page starts, as they were globals from the moment the browse
// scripts parsed.
registerPageHooks({
  getActiveSelection: getActiveSelection,
  getSelectedPhotoId: function() { return selection.focusedPhotoId; },
  getPhotos: function() { return state.photos; },
  getDetailPhotoId: function() { return state.detailPhotoId; },
  openBrowseShortcutPhoto: openBrowseShortcutPhoto,
  moveBrowseSelection: moveBrowseSelection,
  batchSetRating: batchSetRating,
  batchSetFlag: batchSetFlag,
  setRatingFor: setRatingFor,
  setFlagFor: setFlagFor,
  setColorLabelFor: setColorLabelFor,
  setWildlifeExcludedFor: setWildlifeExcludedFor,
  openExportModal: openExportModal,
  openPhotoExportModal: openPhotoExportModal,
  revealPhoto: revealPhoto,
  copyPhotoPaths: copyPhotoPaths,
  openBrowseCompare: openBrowseCompare,
  batchAddKeyword: batchAddKeyword,
  addToCollection: addToCollection,
  openCaptureTimeModal: openCaptureTimeModal,
  batchDelete: batchDelete,
  removeDeletedLightboxPhoto: removeDeletedLightboxPhoto,
  afterHistoryChange: afterHistoryChange,
  // The export modal's shared controllers (vireo-export-presets.js,
  // vireo-export-collisions.js).
  applyExportPreset: applyExportPreset,
  markExportCustom: markExportCustom,
  updateExportFormatControls: updateExportFormatControls,
  updateExportPreview: updateExportPreview,
  selectedExportMetadataFields: selectedExportMetadataFields,
  buildExportPreflightRequest: buildExportPreflightRequest,
});

/* ---------- Markup ---------- */
// The controls named by data-browse-on (see markup.js) and the comparison
// overlay's (_photo_compare.html).
bindBrowseMarkup();
VireoBrowseCompare.bindControls(document, function() { return browseCompare; });

/* ---------- Bootstrap ---------- */
bindSelectionPanelActions();
bindBrowseHistogram();
VireoViewPreferences.restoreAll(/** @type {HTMLElement | null} */ (document.querySelector('.browse-view-controls')));
updateThumbSize(/** @type {HTMLInputElement} */ (document.getElementById('thumbSizeSlider')).value);
bootstrapBrowse();

// Subscribe to folder-health changes (dispatched from the navbar's polling)
// here rather than beside refreshBrowseAfterFolderHealthChange in
// folder-health.js: the refresh reaches most of the page, so the listener
// must not be live before every browse script has loaded.
document.addEventListener('vireo:folder-health-changed', function(event) {
  state._activeFolderHealthRefresh = refreshBrowseAfterFolderHealthChange(
    event && event.detail);
});

// Arm infinite scroll only after bootstrapBrowse() has claimed the
// ``loading`` mutex, so the observer's first callback cannot start a
// competing page load ahead of bootstrap (the observer itself is built in
// scroll.js).
observer.observe(/** @type {HTMLElement} */ (document.getElementById('scrollSentinel')));

async function bootstrapBrowse() {
  // If deep-linking to a specific photo, skip normal bootstrap — the deep-link handler takes over
  if (new URLSearchParams(window.location.search).get('photo_id')) return;
  // Prevent the IntersectionObserver from triggering loadPhotos() while bootstrap is in flight
  var finishLoad = pagination.beginLoad();
  var bootstrapSucceeded = false;
  // Declared out here so the finally-region guard below can read it even
  // if /api/browse/init throws before the assignment inside the try.
  var healthChangedDuringInit;
  // Snapshot the health-refresh generation before any await so we can
  // detect a concurrent refresh from BOTH the try's success path and
  // its catch. If a vireo:folder-health-changed event fires while
  // /api/browse/init is in flight, refreshBrowseAfterFolderHealthChange()
  // will have already reloaded folders/keywords/collections and the grid
  // from the fresh post-flip state. Populating ``photos`` / rendering the
  // sidebars from this pre-flip response afterwards would silently
  // overwrite that fresher refresh with stale data — leaving the grid
  // empty until another event or a manual reload. Snapshotting here
  // (outside the try) also lets the catch below recompute the flag when
  // the init request rejects: a throw skips the in-try assignment, which
  // would leave the finally-region guard to release the load lock the
  // concurrent health refresh's ``loadPhotos`` still owns.
  var bootstrapHealthSeq = state.folderHealthRefreshSeq;
  // The sort <select> is in the DOM before init resolves, so the user can
  // change it (or edit a restored filter) while /api/browse/init is pending.
  // onSortChanged() / onBrowseFilterChange() → resetAndLoad() then claims the
  // window and starts its own loadPhotos(); this bootstrap must not paint its
  // now-abandoned dataset over that, nor release the ``loading`` mutex the
  // newer load holds — the same stale-window hazard the deep-link loader
  // guards against.
  var bootstrapWindowIsCurrent = claimBrowseWindow();
  adoptUrlScope();
  try {
    healthChangedDuringInit = await loadBootstrapPage(bootstrapHealthSeq, bootstrapWindowIsCurrent);
    bootstrapSucceeded = true;
    startBrowseFilterBar();
  } catch(e) {
    // /api/browse/init rejected — recompute healthChangedDuringInit so the
    // finally-region guard below correctly defers to a concurrent health
    // refresh that owns the load lock. The throw skipped the assignment
    // inside the try, and without recomputing here we would clear
    // ``loading`` even though the health refresh's ``loadPhotos`` still
    // holds the mutex, letting the intersection observer fire a duplicate
    // page-1 request against the same ``loadEpoch``.
    healthChangedDuringInit = state.folderHealthRefreshSeq !== bootstrapHealthSeq;
    /** @type {HTMLElement} */ (document.getElementById('loadingState')).textContent = 'Error loading.';
  }
  // If a folder-health event fired while /api/browse/init was in flight,
  // refreshBrowseAfterFolderHealthChange() has already taken over the load
  // state: its resetAndLoad() → loadPhotos() chain owns ``loading`` and
  // rearms the intersection observer itself when it settles. Clearing
  // ``loading`` here would release the mutex that concurrent loadPhotos
  // still holds, letting the newly-rearmed observer fire a second page-1
  // request against the same ``loadEpoch``; both responses append and the
  // grid ends up with duplicated cards and off-by-one pagination.
  // The same reasoning applies when a filter or
  // sort change claimed the window mid-init: its loadPhotos owns ``loading``
  // and releases it in its own finally.
  if (!healthChangedDuringInit && bootstrapWindowIsCurrent()) {
    finishLoad();
    if (bootstrapSucceeded) {
      rearmInfiniteScrollObserver();
      // Make sure a tall viewport still gets its second page even when the
      // sentinel callback does not fire after the initial layout.
      requestAnimationFrame(ensureViewportHydrated);
    }
  }
}

// Parse URL scope params BEFORE the first await. A folder-health event that
// fires while ``_cfgPromise`` is still pending runs
// refreshBrowseAfterFolderHealthChange(), which calls resetAndLoad() from
// the current ``activeFolderId`` / ``activeCollectionId``. If those were
// still null the refresh loads the unscoped workspace grid, and then this
// bootstrap's folder-scoped init response is thrown away by the
// healthChangedDuringInit guard below — for a plain collection deep link
// the browseFilterInitPromise.then replay reopens it, but a folder-only
// deep link has no such replay, so Browse would render unscoped despite
// ``?folder_id=N`` in the URL. Assigning here
// means any concurrent refresh sees the right scope.
function adoptUrlScope() {
  var pageParams = new URLSearchParams(window.location.search);
  var initialFolderId = parseInt(pageParams.get('folder_id') || '', 10);
  if (!isNaN(initialFolderId)) state.activeFolderId = initialFolderId;
  var initialCollectionId = parseInt(pageParams.get('collection_id') || '', 10);
  if (!isNaN(initialCollectionId)) {
    state.activeCollectionId = initialCollectionId;
    state.dashboardCollectionScope = pageParams.get('dashboard_scope') === '1';
  }
}

function browseInitQuery() {
  // Legacy filter deep-link params (rating_min/flag/keyword/dates/…) are
  // compiled into an initial rule tree by VireoFilter.init below, which
  // reloads through /api/photos/query — the bootstrap endpoint is
  // scope-only (Phase 5 removed its legacy filter params).
  var initParams = new URLSearchParams();
  initParams.set('sort', /** @type {HTMLSelectElement} */ (document.getElementById('sortSelect')).value);
  if (browseStacksEnabled()) initParams.set('stacks', '1');
  if (state.activeFolderId) initParams.set('folder_id', String(state.activeFolderId));
  // Collection-only deep links keep their historical endpoint behavior,
  // while Dashboard links opt into composable filters via dashboard_scope.
  // The combined init endpoint still needs the collection id for first paint
  // in either case.
  if (state.activeCollectionId) initParams.set('collection_id', String(state.activeCollectionId));
  initParams.set('per_page', String(pagination.perPage));
  return initParams.toString();
}

// Fetch /api/browse/init and paint the first page from it. Resolves to whether
// a folder-health refresh superseded the response while it was in flight.
async function loadBootstrapPage(/** @type {number} */ healthSeq, /** @type {() => boolean} */ windowIsCurrent) {
  applyBrowseConfig(await _cfgPromise);
  var missingSnapshotVersionAtInitStart = missingSnapshotVersionNow();
  var data = await safeFetch('/api/browse/init?' + browseInitQuery());
  var healthChangedDuringInit = state.folderHealthRefreshSeq !== healthSeq;

  // Seed the navbar's missing-folder snapshot from init's workspace-scoped
  // view so the first /api/folders/missing observation has a baseline to
  // compare against. Without this, a background _folder_health_loop flip
  // that runs between /api/browse/init and the first navbar poll leaves
  // the poll's null-baseline branch returning false — later polls then
  // see the same IDs and never dispatch, so Browse stays showing the
  // pre-flip state until another transition or a reload. The navbar
  // helper compares its baseline version at init
  // request start and completion: a poll/POST already applied before the
  // request is older than init, while one applied during the request remains
  // authoritative.
  //
  // When a differing poll snapshot lands while init is in flight, init may
  // contain pre-flip photos/folders. Dispatch the init→now transition so
  // refreshBrowseAfterFolderHealthChange takes over, and mark the
  // health-changed flag so the guarded render path skips init's stale data.
  // Conversely, a poll applied before this request is known older and the
  // helper adopts init directly.
  if (Array.isArray(data.missing_folder_ids)) {
    var initWasStale = _reconcileMissingFoldersInitSnapshot(
      data.missing_folder_ids,
      missingSnapshotVersionAtInitStart,
      'bootstrap-reconcile',
      data.folder_health_version);
    if (initWasStale) healthChangedDuringInit = true;
  }

  if (!healthChangedDuringInit) {
    // The sidebar trees are scope-independent, so render them even if a
    // newer load owns the grid — otherwise a sort change during init would
    // leave Browse with no folder/keyword/collection lists at all.
    renderInitSidebars(data);
    if (windowIsCurrent()) paintBootstrapGrid(data, windowIsCurrent);
    loadSummary();
  }
  refreshPendingSyncBanner();

  /** @type {HTMLElement} */ (document.getElementById('loadingState')).style.display = 'none';
  // Lazy-load collection photo counts to avoid N+1 on init. Skip this
  // when the health refresh already ran — it issued its own
  // loadCollectionCounts() and repeating the call here would just fire
  // another round-trip for the same data.
  if (!healthChangedDuringInit) loadCollectionCounts();
  return healthChangedDuringInit;
}

function paintBootstrapGrid(/** @type {any} */ data, /** @type {() => boolean} */ windowIsCurrent) {
  // Populate everything from a single response
  state.photos = data.photos || [];
  pagination.setTotals(data);
  pagination.place(1, 2, state.photos.length);

  renderGrid();
  updatePreviousPhotosButton();
  updateFilterSummary();
  hydrateColorLabelsForRenderedPage(windowIsCurrent);
}

// Universal filter bar: registry + persisted/deep-linked state load
// async; when it comes up with active filters the unfiltered first
// paint above is stale, so reload through the rules path.
function startBrowseFilterBar() {
  state.browseFilterInitPromise = VireoFilter.init({
    page: 'browse',
    root: document.getElementById('vireoFilterBar'),
    scopeLabel: 'Workspace \u00b7 All available photos',
    onChange: onBrowseFilterChange,
    getScope: browseFilterScope,
    onCollectionSaved: function() { loadCollections(); },
  });
  // Snapshot the scope generation BEFORE we register the .then so we can
  // tell whether a user sidebar click advanced it while init was pending.
  // Running the bootstrap deep-link/persisted replay below unconditionally
  // would call filterByCollection/resetAndLoad, which bumps browseScopeGen
  // and makes any queued sidebar click (e.g. collection B awaiting init)
  // resume as stale \u2014 reopening the URL's collection A over the user's
  // later selection.
  var bootstrapScopeGen = state.browseScopeGen;
  state.browseFilterInitPromise.then(function() {
    // A user sidebar click (folder/keyword/collection) may have changed
    // scope while init was pending. Their newer selection is authoritative
    // for the collection deep-link replay below, which must not reopen the
    // URL's collection over it. But we must still apply any restored/URL
    // filter chips: returning early here would leave them rendered in the
    // bar without ever running through resetAndLoad, so the grid wouldn't
    // match the visible chips until the user edited them.
    replayStartupScope(state.browseScopeGen !== bootstrapScopeGen);
  }).catch(function() {});
}

function replayStartupScope(/** @type {boolean} */ scopeChanged) {
  VireoFilter.setResultTotal(pagination.underlyingTotal);
  settleCollectionLinkScope();
  if (state.activeCollectionId && !state.dashboardCollectionScope && !scopeChanged) {
    // Plain collection deep link: open it into the filter bar as
    // editable chips (rules + visual round-trip), replacing the
    // historical collection-endpoint mode. Skipped when a user sidebar
    // click already claimed a different scope — their queued
    // filterByCollection/filterByFolder/filterByKeyword owns the view.
    filterByCollection(state.activeCollectionId);
    return;
  }
  // A dashboard-scoped collection deep link (?dashboard_scope=1&collection_id=...)
  // paints from /api/browse/init and then falls through both branches above
  // when no explicit filter chips came from the URL. /api/browse/init does
  // not return availability totals, so ``updateOfflineCollectionState`` never
  // runs and the offline-photos notice stays hidden — users cannot reveal
  // the offline members until another action reloads the grid. Force a
  // scoped /api/photos/query here so ``loadPhotos`` populates the
  // availability state from the response.
  if (VireoFilter.hasFilters() ||
      (state.activeCollectionId && state.dashboardCollectionScope && !scopeChanged)) {
    resetAndLoad();
    loadSummary();
  }
}

function settleCollectionLinkScope() {
  // Collection deep links must open showing exactly that collection \u2014
  // whether plain (?collection_id=..., historical endpoint behavior) or
  // dashboard-scoped (?dashboard_scope=1&collection_id=..., composable).
  // A restored persisted Browse filter overrides both: for plain links
  // the resetAndLoad below clears activeCollectionId (see line ~3901
  // \u2014 non-dashboard scopes); for dashboard links it silently
  // intersects the drill-down collection with unrelated saved
  // rules. If the URL itself didn't supply any legacy filter params,
  // treat the collection link as URL-scoped and drop the persisted
  // state so the link is honored.
  var urlParams = new URLSearchParams(window.location.search);
  var LEGACY_FILTER_PARAMS = ['rating_min', 'flag', 'color_label',
    'date_from', 'date_to', 'location_status', 'missing_gps', 'keyword'];
  var hasExplicitFilterParams = LEGACY_FILTER_PARAMS.some(function(p) {
    return urlParams.has(p);
  });
  var collectionLink = !!urlParams.get('collection_id');
  if (collectionLink && !hasExplicitFilterParams && VireoFilter.hasFilters()) {
    VireoFilter.clearAll(true);
  }
  // A ?collection_id=...&rating_min=4 (or &date_from=..., etc.) link
  // compiles the URL filter params into rules above; if we then called
  // filterByCollection() its loadExpression() would REPLACE state.root
  // with the saved collection rules and silently drop the URL filters.
  // Treat the collection as a composable
  // dashboard-style scope for this case: keep the URL filter chips
  // and scope /api/photos/query by the collection alongside them.
  //
  // Exception — visual collections: /api/photos/query's collection
  // restriction evaluates ``rules`` only, not ``visual_json``. Composing
  // URL filters this way would silently drop the visual clause and
  // widen to every metadata match with the URL filter. Fall through to
  // filterByCollection() instead so the
  // saved rules + visual expression loads correctly; the URL filter
  // chips are cleared because loadExpression() replaces state.root, so
  // surface a toast so the drop isn't silent.
  var deepLinkedCollection = collectionLink && state.activeCollectionId != null ? state.collectionsById[state.activeCollectionId] : null;
  var deepLinkedIsVisual = !!(deepLinkedCollection && deepLinkedCollection.visual_json);
  if (collectionLink && hasExplicitFilterParams && !state.dashboardCollectionScope) {
    if (deepLinkedIsVisual) {
      showToast(
        'Opened the visual collection — URL filters (rating/date/etc.) were dropped so the visual clause applies. Edit the filter bar to add them back.',
        'info'
      );
    } else {
      state.dashboardCollectionScope = true;
    }
  }
  // Dashboard-scoped visual collection links (?dashboard_scope=1&collection_id=<visual>
  // [&rating_min=...]): /api/browse/init and /api/photos/query both restrict
  // by the collection's ``rules`` and ignore ``visual_json`` in the dashboard
  // scope path, so keeping dashboardCollectionScope=true would silently widen
  // to every metadata match. Drop the scope so we
  // fall through to filterByCollection() and the saved rules + visual
  // expression loads correctly; any URL filter chips are cleared by
  // loadExpression() replacing state.root, so surface a toast if we dropped
  // filters the user asked for.
  if (collectionLink && state.dashboardCollectionScope && deepLinkedIsVisual) {
    state.dashboardCollectionScope = false;
    showToast(
      hasExplicitFilterParams
        ? 'Opened the visual collection — dashboard scope and URL filters were dropped so the visual clause applies. Edit the filter bar to add them back.'
        : 'Opened the visual collection — dashboard scope was dropped so the visual clause applies.',
      'info'
    );
  }
}

// Typeahead counts otherwise ignore the folder/collection restriction
// the grid uses via /api/photos/query, so picking a suggestion inside
// a folder or dashboard-scoped collection can yield fewer visible
// grid results than the advertised count. Dashboard-scoped collection
// Browse composes collection + rules; the legacy collection view
// clears activeCollectionId on filter apply, so this is a no-op there.
function browseFilterScope() {
  return {
    folder_id: state.activeFolderId,
    collection_id: (state.activeCollectionId && state.dashboardCollectionScope)
      ? state.activeCollectionId : null,
  };
}

function missingSnapshotVersionNow() {
  return typeof missingFoldersSnapshotVersion() !== 'undefined'
    ? missingFoldersSnapshotVersion()
    : null;
}

function renderInitSidebars(/** @type {any} */ data) {
  // Pin the workspace this tree came from so a later Remove action
  // targets the same workspace even on the deep-link entry point.
  if (data.active_workspace_id != null) {
    state.browseWorkspaceId = Number(data.active_workspace_id);
  }
  renderFolderTree(data.folders || []);
  renderKeywordTree(data.keywords || []);
  renderCollectionList(data.collections || []);
}

/* ---------- Deep-link: scroll to photo_id if present in URL ---------- */
// Cap retry loops so a folder that keeps flapping (health event on every
// interval) can't spin this indefinitely — after this many stale
// interruptions, fall through to whatever refreshBrowseAfterFolderHealthChange
// last rendered rather than deep-link forever.
var _DEEP_LINK_MAX_RETRIES = 5;
export async function _runPhotoDeepLink(/** @type {number} */ photoId) {
  // A focused deep link must expose the requested photo as a top-level card.
  // Disable stacks for this view so its later lazy pages use the same shape.
  var stackToggle = /** @type {HTMLInputElement} */ (document.getElementById('browseStacksToggle'));
  if (stackToggle) stackToggle.checked = false;
  // Snapshot the health-refresh generation BEFORE any await. If a
  // vireo:folder-health-changed event fires while this loader is awaiting
  // /api/photos/<id> or its folder-scoped /api/browse/init,
  // refreshBrowseAfterFolderHealthChange() has already reloaded folders,
  // keywords, collections, and the grid from the fresh post-flip state and
  // owns the ``loading`` mutex. Rendering pre-flip data from initData or
  // releasing ``loading`` here would either clobber that fresher refresh
  // or let the intersection observer fire a duplicate page-1 request
  // against the same loadEpoch.
  /** @type {BrowsePhotoDeepLink} */
  var link = {
    photoId: photoId,
    healthSeq: state.folderHealthRefreshSeq,
    sort: '',
    loaded: false,
    // Prevent the IntersectionObserver from triggering loadPhotos() while deep-link is loading
    finishLoad: pagination.beginLoad(),
    datasetIsCurrent: null,
  };
  var stop = '';
  try {
    stop = await loadDeepLinkedPhoto(link);
  } catch(e) { /* ignore deep-link errors silently */ }
  // A link that stopped early leaves ``loading`` to whoever took over. Only a
  // folder-health refresh is worth a retry: any other stop is terminal.
  if (stop) return stop === 'superseded';
  // A concurrent refreshBrowseAfterFolderHealthChange() owns ``loading``
  // and rearms the observer itself when its loadPhotos settles; releasing
  // ``loading`` here would let a second page-1 request race against the
  // same loadEpoch, producing duplicate cards and off-by-one pagination.
  if (!deepLinkStop(link)) {
    link.finishLoad();
    if (link.loaded) {
      rearmInfiniteScrollObserver();
      requestAnimationFrame(ensureViewportHydrated);
    }
  }
  return false;
}

// Why a deep link must stop now, or '' to carry on: 'superseded' when a
// folder-health refresh reloaded the page under it, 'stale' when the user
// has since picked another folder, sort or filter.
function deepLinkStop(/** @type {BrowsePhotoDeepLink} */ link) {
  if (state.folderHealthRefreshSeq !== link.healthSeq) return 'superseded';
  if (link.datasetIsCurrent && !link.datasetIsCurrent()) return 'stale';
  return '';
}

// Resolves to '' when the link ran to the end, otherwise to why it stopped.
async function loadDeepLinkedPhoto(/** @type {BrowsePhotoDeepLink} */ link) {
  applyBrowseConfig(await _cfgPromise);
  var stop = deepLinkStop(link);
  if (stop) return stop;
  var photo = await safeFetch('/api/photos/' + link.photoId, {}, { toast: false });
  stop = deepLinkStop(link);
  if (stop) return stop;
  if (!photo || !photo.folder_id) {
    // A concurrent health refresh that owns ``loading`` was caught by the
    // stop check above, so releasing it here cannot race that refresh's
    // loadPhotos against the intersection observer.
    link.finishLoad();
    return 'missing';
  }
  enterDeepLinkFolder(link, photo.folder_id);
  stop = await loadDeepLinkFolderPage(link, photo.folder_id);
  if (stop) return stop;
  return revealDeepLinkedPhoto(link);
}

function enterDeepLinkFolder(/** @type {BrowsePhotoDeepLink} */ link, /** @type {number} */ folderId) {
  // Take ownership of the grid window BEFORE clearing it. A sort change or
  // folder click that happened while /api/photos/<id> was pending already
  // ran resetAndLoad(), which advanced loadEpoch and started its own
  // loadPhotos(). Merely snapshotting the epoch that reset installed would
  // leave that in-flight load valid: it would land after this deep link had
  // claimed the target folder and append its unscoped workspace rows into
  // the folder-scoped grid (and, once earliestPage is set below, leave the
  // "N earlier photos aren't loaded" banner counting against a dataset that
  // is no longer on screen). Claiming discards every load started before
  // this point.
  var windowIsCurrent = claimBrowseWindow();
  link.finishLoad = pagination.beginLoad();
  // Navigate to the photo's folder
  state.activeFolderId = folderId;
  state.activeKeyword = null;
  state.activeCollectionId = null;
  pagination.reset();
  state.photos = [];
  updatePreviousPhotosButton();
  // Same hazard as filterByCollection: leaving selectedPhotos populated
  // across a dataset switch leaves the batch bar armed against stale ids.
  selection.clear();
  closeDetail();
  /** @type {HTMLElement} */ (document.getElementById('grid')).innerHTML = '';

  // A folder click, sort change, or filter edit calls resetAndLoad(), which
  // advances loadEpoch (and scope changes also advance browseScopeGen).
  // Snapshot the scope generation alongside the window claim above so a
  // later user selection cannot be overwritten by this deep-link response.
  var scopeGen = state.browseScopeGen;
  var sortSelect = /** @type {HTMLSelectElement} */ (document.getElementById('sortSelect'));
  link.sort = sortSelect.value;
  link.datasetIsCurrent = function() {
    return windowIsCurrent() &&
      state.browseScopeGen === scopeGen &&
      state.activeFolderId === folderId &&
      /** @type {HTMLSelectElement} */ (document.getElementById('sortSelect')).value === link.sort;
  };
}

// bootstrapBrowse was skipped, so load the target folder's first page and
// the shared folder/keyword/collection trees before locating the photo.
async function loadDeepLinkFolderPage(/** @type {BrowsePhotoDeepLink} */ link, /** @type {number} */ folderId) {
  var missingSnapshotVersionAtInitStart = missingSnapshotVersionNow();
  var initParams = new URLSearchParams();
  initParams.set('folder_id', String(folderId));
  initParams.set('per_page', String(pagination.perPage));
  initParams.set('sort', link.sort);
  initParams.set('focus_photo_id', String(link.photoId));
  var initData = await safeFetch('/api/browse/init?' + initParams.toString(), {}, { toast: false });
  var stop = deepLinkStop(link);
  if (stop) return stop;
  if (!initData) return '';
  // Seed the navbar's missing-folder snapshot from init's workspace-scoped
  // view so the first /api/folders/missing observation has a baseline to
  // compare against. bootstrapBrowse() does the same seed but returns
  // immediately for ?photo_id=..., so without this the deep-link path
  // leaves _missingFoldersLastIds null: a background _folder_health_loop
  // flip that runs between init and the first navbar poll then lands
  // silently (null-baseline branch), later polls see the same IDs and
  // never dispatch, and Browse stays stuck on the pre-flip state
  // indefinitely. The shared snapshot-version helper adopts init when a
  // baseline was already present before this request, but dispatches an
  // init→current transition when a newer observation landed in flight;
  // the stop check after it then skips the stale render data.
  if (Array.isArray(initData.missing_folder_ids)) {
    _reconcileMissingFoldersInitSnapshot(
      initData.missing_folder_ids,
      missingSnapshotVersionAtInitStart,
      'deep-link-reconcile',
      initData.folder_health_version);
    stop = deepLinkStop(link);
    if (stop) return stop;
  }
  paintDeepLinkPage(link, initData);
  return '';
}

function paintDeepLinkPage(/** @type {BrowsePhotoDeepLink} */ link, /** @type {any} */ initData) {
  renderInitSidebars(initData);
  loadCollectionCounts();
  state.photos = initData.photos || [];
  pagination.setTotals(initData);
  var focusPage = parseInt(initData.focus_page, 10);
  var hasFocusPage = Number.isInteger(focusPage) && focusPage >= 1;
  pagination.place(hasFocusPage ? focusPage : 1, hasFocusPage ? focusPage + 1 : 2, state.photos.length);
  renderGrid();
  updatePreviousPhotosButton();
  /** @type {HTMLElement} */ (document.getElementById('loadingState')).style.display = 'none';
  link.loaded = true;
  hydrateColorLabelsForRenderedPage(function() { return !deepLinkStop(link); });
}

async function revealDeepLinkedPhoto(/** @type {BrowsePhotoDeepLink} */ link) {
  // The focused init response normally includes the bounded page containing
  // the target from one SQLite read snapshot. Later pages remain lazy so a
  // deep target cannot force Browse to render every preceding card. Fall
  // back to the old serial scan for compatibility or if the target vanished.
  var stop = deepLinkStop(link);
  if (stop) return stop;
  var el = getGridCard(link.photoId);
  if (!el) {
    // loadPhotos() honors the setup guard, so release it before the explicit
    // compatibility/reconciliation scan or it would return immediately.
    link.finishLoad();
    el = await loadUntilPhotoRendered(link.photoId, function() { return !deepLinkStop(link); });
  }
  stop = deepLinkStop(link);
  if (stop) return stop;

  syncSidebarHighlights();
  if (el) flashDeepLinkedCard(el);
  startDeepLinkFilterBar();
  return '';
}

// Scroll to and highlight the target photo
function flashDeepLinkedCard(/** @type {HTMLElement} */ el) {
  el.scrollIntoView({ behavior: preferredScrollBehavior(), block: 'center' });
  el.style.outline = '3px solid var(--accent)';
  el.style.outlineOffset = '2px';
  setTimeout(function() {
    el.style.outline = '';
    el.style.outlineOffset = '';
  }, 2000);
}

// bootstrapBrowse (which owns VireoFilter.init) short-circuits when
// ?photo_id=... is present, so without this the filter bar renders but
// has no event handlers: quick-search does nothing, chips can't be
// added, and the total stays at "–". Initialize here so the bar is
// live once the deep-link has finished loading. Drop any persisted
// workspace filter — the explicit ?photo_id link should not be
// silently overridden by whatever the user had saved on Browse.
function startDeepLinkFilterBar() {
  if (VireoFilter && !VireoFilter.isReady()) {
    VireoFilter.init({
      page: 'browse',
      root: document.getElementById('vireoFilterBar'),
      scopeLabel: 'Workspace · All available photos',
      onChange: onBrowseFilterChange,
      getScope: browseFilterScope,
    }).then(function() {
      VireoFilter.setResultTotal(pagination.underlyingTotal);
      if (VireoFilter.hasFilters()) VireoFilter.clearAll(true);
    }).catch(function() {});
  }
}

(async function() {
  var params = new URLSearchParams(window.location.search);
  var rawPhotoId = params.get('photo_id');
  if (!rawPhotoId) return;
  var photoId = parseInt(rawPhotoId, 10);
  if (isNaN(photoId)) return;

  // If a folder-health refresh interrupts us, wait for it to finish and
  // retry — otherwise the deep-link is silently abandoned and Browse is
  // left on whatever the refresh happened to load (usually the unscoped
  // workspace grid, with the target photo never scrolled into view and
  // VireoFilter uninitialized).
  for (var attempt = 0; attempt < _DEEP_LINK_MAX_RETRIES; attempt++) {
    var wasSuperseded = await _runPhotoDeepLink(photoId);
    if (!wasSuperseded) return;
    // Await the refresh that stole the load so the next attempt starts
    // from a settled state and its pre-await snapshot is guaranteed to
    // match the current generation (unless yet another refresh fires
    // during the retry, which the loop handles).
    await waitForFolderHealthRefreshesToSettle();
  }
})();

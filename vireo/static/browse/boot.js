/* Browse: page startup (bootstrap and photo deep link). Loads last:
   it starts the work every other browse script defines, so nothing it
   reaches can be undefined when it runs. */

/* ---------- Bootstrap ---------- */
// ``folderHealthRefreshSeq`` must be *initialized* before ``bootstrapBrowse()``
// is invoked, not merely declared. ``var`` hoists the declaration to the top
// of the script but leaves the value ``undefined`` until this line executes;
// the paired assignment lives further down at the health-refresh helpers.
// Bootstrap synchronously reads ``folderHealthRefreshSeq`` into
// ``bootstrapHealthSeq`` before its first await, so with the initialization
// happening later, the snapshot was ``undefined``, the outer script then
// ran the ``= 0`` assignment while bootstrap was awaiting ``/api/browse/init``,
// and the post-await comparison ``0 !== undefined`` was always true. That
// made ``healthChangedDuringInit`` fire on every normal load, skipping the
// render block and leaving Browse empty until VireoFilter.init happened to
// re-fetch. Initialize here so the pre-await snapshot reads the same ``0``
// the rest of the script sees (Codex review r3686317674).
var folderHealthRefreshSeq = 0;
Vireo.browse.selectionPanel.bindActions();
VireoViewPreferences.restoreAll(document.querySelector('.browse-view-controls'));
updateThumbSize(document.getElementById('thumbSizeSlider').value);
bootstrapBrowse();

// Subscribe to folder-health changes (dispatched from the navbar's polling)
// here rather than beside refreshBrowseAfterFolderHealthChange in
// folder-health.js: the refresh reaches most of the page, so the listener
// must not be live before every browse script has loaded.
document.addEventListener('vireo:folder-health-changed', function(event) {
  _activeFolderHealthRefresh = refreshBrowseAfterFolderHealthChange(
    event && event.detail);
});

// Arm infinite scroll only after bootstrapBrowse() has claimed the
// ``loading`` mutex, so the observer's first callback cannot start a
// competing page load ahead of bootstrap (the observer itself is built in
// scroll.js).
observer.observe(document.getElementById('scrollSentinel'));

async function bootstrapBrowse() {
  // If deep-linking to a specific photo, skip normal bootstrap — the deep-link handler takes over
  if (new URLSearchParams(window.location.search).get('photo_id')) return;
  // Prevent the IntersectionObserver from triggering loadPhotos() while bootstrap is in flight
  loading = true;
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
  // empty until another event or a manual reload (Codex review
  // r3685515800). Snapshotting here (outside the try) also lets the
  // catch below recompute the flag when the init request rejects: the
  // in-try assignment used to be skipped by the throw, leaving the
  // finally-region guard to release the load lock the concurrent health
  // refresh's ``loadPhotos`` still owned (Codex review r3686191138).
  var bootstrapHealthSeq = folderHealthRefreshSeq;
  // The sort <select> is in the DOM before init resolves, so the user can
  // change it (or edit a restored filter) while /api/browse/init is pending.
  // applyFilters() → resetAndLoad() then claims the window and starts its own
  // loadPhotos(); this bootstrap must not paint its now-abandoned dataset over
  // that, nor release the ``loading`` mutex the newer load holds — the same
  // stale-window hazard the deep-link loader guards against (Codex review
  // r3792769108).
  var bootstrapWindowIsCurrent = claimBrowseWindow();
  // Parse URL scope params BEFORE the first await. A folder-health event that
  // fires while ``_cfgPromise`` is still pending runs
  // refreshBrowseAfterFolderHealthChange(), which calls resetAndLoad() from
  // the current ``activeFolderId`` / ``activeCollectionId``. If those were
  // still null the refresh loads the unscoped workspace grid, and then this
  // bootstrap's folder-scoped init response is thrown away by the
  // healthChangedDuringInit guard below — for a plain collection deep link
  // the browseFilterInitPromise.then replay reopens it, but a folder-only
  // deep link has no such replay, so Browse would render unscoped despite
  // ``?folder_id=N`` in the URL (Codex review r3686605296). Assigning here
  // means any concurrent refresh sees the right scope.
  var pageParams = new URLSearchParams(window.location.search);
  var initialFolderId = parseInt(pageParams.get('folder_id'), 10);
  if (!isNaN(initialFolderId)) activeFolderId = initialFolderId;
  var initialCollectionId = parseInt(pageParams.get('collection_id'), 10);
  if (!isNaN(initialCollectionId)) {
    activeCollectionId = initialCollectionId;
    dashboardCollectionScope = pageParams.get('dashboard_scope') === '1';
  }
  try {
    applyBrowseConfig(await _cfgPromise);
    // Legacy filter deep-link params (rating_min/flag/keyword/dates/…) are
    // compiled into an initial rule tree by VireoFilter.init below, which
    // reloads through /api/photos/query — the bootstrap endpoint is
    // scope-only (Phase 5 removed its legacy filter params).
    var initParams = new URLSearchParams();
    initParams.set('sort', document.getElementById('sortSelect').value);
    if (browseStacksEnabled()) initParams.set('stacks', '1');
    if (activeFolderId) initParams.set('folder_id', activeFolderId);
    // Collection-only deep links keep their historical endpoint behavior,
    // while Dashboard links opt into composable filters via dashboard_scope.
    // The combined init endpoint still needs the collection id for first paint
    // in either case.
    if (activeCollectionId) initParams.set('collection_id', activeCollectionId);
    initParams.set('per_page', perPage);
    var missingSnapshotVersionAtInitStart =
      typeof _missingFoldersSnapshotVersion !== 'undefined'
        ? _missingFoldersSnapshotVersion
        : null;
    var data = await safeFetch('/api/browse/init?' + initParams.toString());
    healthChangedDuringInit = folderHealthRefreshSeq !== bootstrapHealthSeq;

    // Seed the navbar's missing-folder snapshot from init's workspace-scoped
    // view so the first /api/folders/missing observation has a baseline to
    // compare against. Without this, a background _folder_health_loop flip
    // that runs between /api/browse/init and the first navbar poll leaves
    // the poll's null-baseline branch returning false — later polls then
    // see the same IDs and never dispatch, so Browse stays showing the
    // pre-flip state until another transition or a reload (Codex review
    // r3686191141). The navbar helper compares its baseline version at init
    // request start and completion: a poll/POST already applied before the
    // request is older than init, while one applied during the request remains
    // authoritative.
    //
    // When a differing poll snapshot lands while init is in flight, init may
    // contain pre-flip photos/folders. Dispatch the init→now transition so
    // refreshBrowseAfterFolderHealthChange takes over, and mark the
    // health-changed flag so the guarded render path skips init's stale data
    // (Codex review r3686452019). Conversely, a poll applied before this
    // request is known older and the helper adopts init directly
    // (Codex review r3687277899).
    if (Array.isArray(data.missing_folder_ids) &&
        typeof _reconcileMissingFoldersInitSnapshot === 'function') {
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
      if (data.active_workspace_id != null) {
        browseWorkspaceId = Number(data.active_workspace_id);
      }
      renderFolderTree(data.folders || []);
      renderKeywordTree(data.keywords || []);
      renderCollectionList(data.collections || []);
      if (bootstrapWindowIsCurrent()) {
        // Populate everything from a single response
        photos = data.photos || [];
        setBrowseTotals(data);
        currentPage = 2; // next page to load
        earliestPage = 1; // bootstrap always starts at the top of the dataset
        if (photos.length >= totalPhotos) allLoaded = true;

        renderGrid();
        updatePreviousPhotosButton();
        browseDatasetReady = true;
        updateFilterSummary();
        hydrateColorLabelsForRenderedPage(bootstrapWindowIsCurrent);
      }
      loadSummary();
    }
    refreshPendingSyncBanner();

    document.getElementById('loadingState').style.display = 'none';
    // Lazy-load collection photo counts to avoid N+1 on init. Skip this
    // when the health refresh already ran — it issued its own
    // loadCollectionCounts() and repeating the call here would just fire
    // another round-trip for the same data.
    if (!healthChangedDuringInit) loadCollectionCounts();
    bootstrapSucceeded = true;

    // Universal filter bar: registry + persisted/deep-linked state load
    // async; when it comes up with active filters the unfiltered first
    // paint above is stale, so reload through the rules path.
    browseFilterInitPromise = VireoFilter.init({
      page: 'browse',
      root: document.getElementById('vireoFilterBar'),
      scopeLabel: 'Workspace \u00b7 All available photos',
      onChange: function(info) {
        if (timelineMode) loadCalendarData();
        if (activeCollectionId && !dashboardCollectionScope) activeCollectionId = null;
        // Any user-driven change to the filter chips means the bar no
        // longer represents the saved collection verbatim — drop the
        // handle so a later membership refresh doesn't silently revert
        // the user's edits back to the collection's saved expression.
        // Our own filterByCollection() → loadExpression() fires onChange
        // with reason 'expressionLoaded' (opening) or 'expressionRefreshed'
        // (membership-change refresh); both paths set openedCollectionId
        // above, so leave it alone.
        if (openedCollectionId && info &&
            info.reason !== 'expressionLoaded' &&
            info.reason !== 'expressionRefreshed') {
          openedCollectionId = null;
          clearOfflineCollectionState();
        }
        resetAndLoad(browseFilterReloadOptions(info));
        // Summary panel (photo count, classified, top species) is a separate
        // endpoint from the grid; without this the filter changes the grid
        // but leaves the summary showing pre-filter numbers.
        loadSummary();
      },
      // Typeahead counts otherwise ignore the folder/collection restriction
      // the grid uses via /api/photos/query, so picking a suggestion inside
      // a folder or dashboard-scoped collection can yield fewer visible
      // grid results than the advertised count. Dashboard-scoped collection
      // Browse composes collection + rules; the legacy collection view
      // clears activeCollectionId on filter apply, so this is a no-op there.
      getScope: function() {
        return {
          folder_id: activeFolderId,
          collection_id: (activeCollectionId && dashboardCollectionScope)
            ? activeCollectionId : null,
        };
      },
      onCollectionSaved: function() { loadCollections(); },
    });
    // Snapshot the scope generation BEFORE we register the .then so we can
    // tell whether a user sidebar click advanced it while init was pending.
    // The bootstrap deep-link/persisted replay below unconditionally called
    // filterByCollection/resetAndLoad, which bumps browseScopeGen and made
    // any queued sidebar click (e.g. collection B awaiting init) resume as
    // stale \u2014 reopening the URL's collection A over the user's later
    // selection (Codex review r3624637674).
    var bootstrapScopeGen = browseScopeGen;
    browseFilterInitPromise.then(function() {
      // A user sidebar click (folder/keyword/collection) may have changed
      // scope while init was pending. Their newer selection is authoritative
      // for the collection deep-link replay below — reopening the URL's
      // collection over the user's later selection is the bug in Codex
      // review r3624637674. But we must still apply any restored/URL filter
      // chips: returning early here left them rendered in the bar without
      // ever running through resetAndLoad, so the grid didn't match the
      // visible chips until the user edited them (Codex review r3624766665).
      var scopeChanged = browseScopeGen !== bootstrapScopeGen;
      VireoFilter.setResultTotal(totalUnderlyingPhotos);
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
      // with the saved collection rules and silently drop the URL filters
      // (Codex review r3620935211). Treat the collection as a composable
      // dashboard-style scope for this case: keep the URL filter chips
      // and scope /api/photos/query by the collection alongside them.
      //
      // Exception — visual collections: /api/photos/query's collection
      // restriction evaluates ``rules`` only, not ``visual_json``. Composing
      // URL filters this way would silently drop the visual clause and
      // widen to every metadata match with the URL filter (Codex review
      // r3621403147). Fall through to filterByCollection() instead so the
      // saved rules + visual expression loads correctly; the URL filter
      // chips are cleared because loadExpression() replaces state.root, so
      // surface a toast so the drop isn't silent.
      var deepLinkedCollection = collectionLink ? collectionsById[activeCollectionId] : null;
      var deepLinkedIsVisual = !!(deepLinkedCollection && deepLinkedCollection.visual_json);
      if (collectionLink && hasExplicitFilterParams && !dashboardCollectionScope) {
        if (deepLinkedIsVisual) {
          if (typeof showToast === 'function') {
            showToast(
              'Opened the visual collection — URL filters (rating/date/etc.) were dropped so the visual clause applies. Edit the filter bar to add them back.',
              'info'
            );
          }
        } else {
          dashboardCollectionScope = true;
        }
      }
      // Dashboard-scoped visual collection links (?dashboard_scope=1&collection_id=<visual>
      // [&rating_min=...]): /api/browse/init and /api/photos/query both restrict
      // by the collection's ``rules`` and ignore ``visual_json`` in the dashboard
      // scope path, so keeping dashboardCollectionScope=true would silently widen
      // to every metadata match (Codex review r3621519730). Drop the scope so we
      // fall through to filterByCollection() and the saved rules + visual
      // expression loads correctly; any URL filter chips are cleared by
      // loadExpression() replacing state.root, so surface a toast if we dropped
      // filters the user asked for.
      if (collectionLink && dashboardCollectionScope && deepLinkedIsVisual) {
        dashboardCollectionScope = false;
        if (typeof showToast === 'function') {
          showToast(
            hasExplicitFilterParams
              ? 'Opened the visual collection — dashboard scope and URL filters were dropped so the visual clause applies. Edit the filter bar to add them back.'
              : 'Opened the visual collection — dashboard scope was dropped so the visual clause applies.',
            'info'
          );
        }
      }
      if (activeCollectionId && !dashboardCollectionScope && !scopeChanged) {
        // Plain collection deep link: open it into the filter bar as
        // editable chips (rules + visual round-trip), replacing the
        // historical collection-endpoint mode. Skipped when a user sidebar
        // click already claimed a different scope — their queued
        // filterByCollection/filterByFolder/filterByKeyword owns the view.
        filterByCollection(activeCollectionId);
        return;
      }
      // A dashboard-scoped collection deep link (?dashboard_scope=1&collection_id=...)
      // paints from /api/browse/init and then falls through both branches above
      // when no explicit filter chips came from the URL. /api/browse/init does
      // not return availability totals, so ``updateOfflineCollectionState`` never
      // runs and the offline-photos notice stays hidden — users cannot reveal
      // the offline members until another action reloads the grid. Force a
      // scoped /api/photos/query here so ``loadPhotos`` populates the
      // availability state from the response (Codex review r3839992513).
      if (VireoFilter.hasFilters() ||
          (activeCollectionId && dashboardCollectionScope && !scopeChanged)) {
        resetAndLoad();
        loadSummary();
      }
    }).catch(function() {});
  } catch(e) {
    // /api/browse/init rejected — recompute healthChangedDuringInit so the
    // finally-region guard below correctly defers to a concurrent health
    // refresh that owns the load lock. The assignment inside the try was
    // skipped by the throw, and without recomputing here we would clear
    // ``loading`` even though the health refresh's ``loadPhotos`` still
    // holds the mutex, letting the intersection observer fire a duplicate
    // page-1 request against the same ``loadEpoch``
    // (Codex review r3686191138).
    healthChangedDuringInit = folderHealthRefreshSeq !== bootstrapHealthSeq;
    document.getElementById('loadingState').textContent = 'Error loading.';
  }
  // If a folder-health event fired while /api/browse/init was in flight,
  // refreshBrowseAfterFolderHealthChange() has already taken over the load
  // state: its resetAndLoad() → loadPhotos() chain owns ``loading`` and
  // rearms the intersection observer itself when it settles. Clearing
  // ``loading`` here would release the mutex that concurrent loadPhotos
  // still holds, letting the newly-rearmed observer fire a second page-1
  // request against the same ``loadEpoch``; both responses append and the
  // grid ends up with duplicated cards and off-by-one pagination
  // (Codex review r3685627307). The same reasoning applies when a filter or
  // sort change claimed the window mid-init: its loadPhotos owns ``loading``
  // and releases it in its own finally.
  if (!healthChangedDuringInit && bootstrapWindowIsCurrent()) {
    loading = false;
    if (bootstrapSucceeded) {
      rearmInfiniteScrollObserver();
      // Make sure a tall viewport still gets its second page even when the
      // sentinel callback does not fire after the initial layout.
      requestAnimationFrame(ensureViewportHydrated);
    }
  }
}

/* ---------- Deep-link: scroll to photo_id if present in URL ---------- */
// Cap retry loops so a folder that keeps flapping (health event on every
// interval) can't spin this indefinitely — after this many stale
// interruptions, fall through to whatever refreshBrowseAfterFolderHealthChange
// last rendered rather than deep-link forever (Codex review r3686778061).
var _DEEP_LINK_MAX_RETRIES = 5;
async function _runPhotoDeepLink(photoId) {
  // A focused deep link must expose the requested photo as a top-level card.
  // Disable stacks for this view so its later lazy pages use the same shape.
  var stackToggle = document.getElementById('browseStacksToggle');
  if (stackToggle) stackToggle.checked = false;
  // Snapshot the health-refresh generation BEFORE any await. If a
  // vireo:folder-health-changed event fires while this loader is awaiting
  // /api/photos/<id> or its folder-scoped /api/browse/init,
  // refreshBrowseAfterFolderHealthChange() has already reloaded folders,
  // keywords, collections, and the grid from the fresh post-flip state and
  // owns the ``loading`` mutex. Rendering pre-flip data from initData or
  // releasing ``loading`` here would either clobber that fresher refresh
  // or let the intersection observer fire a duplicate page-1 request
  // against the same loadEpoch (Codex review r3686696351).
  var deepLinkHealthSeq = folderHealthRefreshSeq;
  var isDeepLinkHealthCurrent = function() {
    return folderHealthRefreshSeq === deepLinkHealthSeq;
  };
  // Signal to the outer retry loop whether we bailed out due to a
  // concurrent health refresh (retryable) versus completed / errored
  // (terminal). Returning a boolean keeps the existing return-early
  // pattern intact — every ``return`` inside the try body is a stale
  // detection.
  var wasSuperseded = false;

  // Prevent the IntersectionObserver from triggering loadPhotos() while deep-link is loading
  loading = true;
  var deepLinkLoaded = false;
  var isDeepLinkDatasetCurrent = null;
  try {
    applyBrowseConfig(await _cfgPromise);
    if (!isDeepLinkHealthCurrent()) { wasSuperseded = true; return wasSuperseded; }
    var photo = await safeFetch('/api/photos/' + photoId, {}, { toast: false });
    if (!isDeepLinkHealthCurrent()) { wasSuperseded = true; return wasSuperseded; }
    if (!photo || !photo.folder_id) {
      // Don't release ``loading`` if a concurrent health refresh owns it
      // (its loadPhotos would then race the intersection observer). The
      // isDeepLinkHealthCurrent check above already returns early in that
      // case, so reaching here means it's safe to release.
      loading = false;
      return wasSuperseded;
    }

    // Take ownership of the grid window BEFORE clearing it. A sort change or
    // folder click that happened while /api/photos/<id> was pending already
    // ran resetAndLoad(), which advanced loadEpoch and started its own
    // loadPhotos(). Merely snapshotting the epoch that reset installed would
    // leave that in-flight load valid: it would land after this deep link had
    // claimed the target folder and append its unscoped workspace rows into
    // the folder-scoped grid (and, once earliestPage is set below, leave the
    // "N earlier photos aren't loaded" banner counting against a dataset that
    // is no longer on screen). Claiming discards every load started before
    // this point (Codex review r3792769108).
    var deepLinkWindowIsCurrent = claimBrowseWindow();

    // Navigate to the photo's folder
    activeFolderId = photo.folder_id;
    activeKeyword = null;
    activeCollectionId = null;
    browseDatasetReady = false;
    photos = [];
    currentPage = 1;
    allLoaded = false;
    earliestPage = 1;
    updatePreviousPhotosButton();
    // Same hazard as filterByCollection: leaving selectedPhotos populated
    // across a dataset switch leaves the batch bar armed against stale ids.
    selectedPhotos.clear();
    selectedPhotoId = null;
    selectedIndex = -1;
    closeDetail();
    document.getElementById('grid').innerHTML = '';

    // A folder click, sort change, or filter edit calls resetAndLoad(), which
    // advances loadEpoch (and scope changes also advance browseScopeGen).
    // Snapshot the scope generation alongside the window claim above so a
    // later user selection cannot be overwritten by this deep-link response.
    var deepLinkScopeGen = browseScopeGen;
    var deepLinkFolderId = photo.folder_id;
    var deepLinkSort = document.getElementById('sortSelect').value;
    isDeepLinkDatasetCurrent = function() {
      return deepLinkWindowIsCurrent() &&
        browseScopeGen === deepLinkScopeGen &&
        activeFolderId === deepLinkFolderId &&
        document.getElementById('sortSelect').value === deepLinkSort;
    };

    // bootstrapBrowse was skipped, so load the target folder's first page and
    // the shared folder/keyword/collection trees before locating the photo.
    var missingSnapshotVersionAtInitStart =
      typeof _missingFoldersSnapshotVersion !== 'undefined'
        ? _missingFoldersSnapshotVersion
        : null;
    var initParams = new URLSearchParams();
    initParams.set('folder_id', photo.folder_id);
    initParams.set('per_page', perPage);
    initParams.set('sort', deepLinkSort);
    initParams.set('focus_photo_id', photoId);
    var initData = await safeFetch('/api/browse/init?' + initParams.toString(), {}, { toast: false });
    if (!isDeepLinkHealthCurrent()) { wasSuperseded = true; return wasSuperseded; }
    if (!isDeepLinkDatasetCurrent()) return wasSuperseded;
    if (initData) {
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
      // the subsequent isDeepLinkHealthCurrent check then skips stale render
      // data (Codex reviews r3686696347 and r3687277899).
      if (Array.isArray(initData.missing_folder_ids) &&
          typeof _reconcileMissingFoldersInitSnapshot === 'function') {
        _reconcileMissingFoldersInitSnapshot(
          initData.missing_folder_ids,
          missingSnapshotVersionAtInitStart,
          'deep-link-reconcile',
          initData.folder_health_version);
        if (!isDeepLinkHealthCurrent()) { wasSuperseded = true; return wasSuperseded; }
        if (!isDeepLinkDatasetCurrent()) return wasSuperseded;
      }
      // Pin the workspace this tree came from so a later Remove action
      // targets the same workspace even on the deep-link entry point.
      if (initData.active_workspace_id != null) {
        browseWorkspaceId = Number(initData.active_workspace_id);
      }
      renderFolderTree(initData.folders || []);
      renderKeywordTree(initData.keywords || []);
      renderCollectionList(initData.collections || []);
      loadCollectionCounts();
      photos = initData.photos || [];
      setBrowseTotals(initData);
      var focusPage = parseInt(initData.focus_page, 10);
      var hasFocusPage = Number.isInteger(focusPage) && focusPage >= 1;
      earliestPage = hasFocusPage ? focusPage : 1;
      currentPage = hasFocusPage ? focusPage + 1 : 2;
      if (loadedWindowOffset() + photos.length >= totalPhotos) allLoaded = true;
      renderGrid();
      updatePreviousPhotosButton();
      browseDatasetReady = true;
      document.getElementById('loadingState').style.display = 'none';
      deepLinkLoaded = true;
      hydrateColorLabelsForRenderedPage(function() {
        return isDeepLinkHealthCurrent() && isDeepLinkDatasetCurrent();
      });
    }

    // The focused init response normally includes the bounded page containing
    // the target from one SQLite read snapshot. Later pages remain lazy so a
    // deep target cannot force Browse to render every preceding card. Fall
    // back to the old serial scan for compatibility or if the target vanished.
    if (!isDeepLinkHealthCurrent()) { wasSuperseded = true; return wasSuperseded; }
    if (!isDeepLinkDatasetCurrent()) return wasSuperseded;
    var el = getGridCard(photoId);
    if (!el) {
      // loadPhotos() honors the setup guard, so release it before the explicit
      // compatibility/reconciliation scan or it would return immediately.
      loading = false;
      el = await loadUntilPhotoRendered(photoId, function() {
        return isDeepLinkHealthCurrent() && isDeepLinkDatasetCurrent();
      });
    }
    if (!isDeepLinkHealthCurrent()) { wasSuperseded = true; return wasSuperseded; }
    if (!isDeepLinkDatasetCurrent()) return wasSuperseded;

    // Update folder tree active state
    document.querySelectorAll('#folderTree .tree-item').forEach(function(el) {
      el.classList.toggle('active', parseInt(el.dataset.folderId) === activeFolderId);
    });

    // Scroll to and highlight the target photo
    if (el) {
      el.scrollIntoView({ behavior: 'smooth', block: 'center' });
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
    if (window.VireoFilter && !VireoFilter.isReady()) {
      VireoFilter.init({
        page: 'browse',
        root: document.getElementById('vireoFilterBar'),
        scopeLabel: 'Workspace · All available photos',
        onChange: function(info) {
          if (timelineMode) loadCalendarData();
          if (activeCollectionId && !dashboardCollectionScope) activeCollectionId = null;
          resetAndLoad(browseFilterReloadOptions(info));
          loadSummary();
        },
        getScope: function() {
          return {
            folder_id: activeFolderId,
            collection_id: (activeCollectionId && dashboardCollectionScope)
              ? activeCollectionId : null,
          };
        },
      }).then(function() {
        VireoFilter.setResultTotal(totalUnderlyingPhotos);
        if (VireoFilter.hasFilters()) VireoFilter.clearAll(true);
      }).catch(function() {});
    }
  } catch(e) { /* ignore deep-link errors silently */ }
  // A concurrent refreshBrowseAfterFolderHealthChange() owns ``loading``
  // and rearms the observer itself when its loadPhotos settles; releasing
  // ``loading`` here would let a second page-1 request race against the
  // same loadEpoch, producing duplicate cards and off-by-one pagination
  // (Codex review r3686696351).
  if (isDeepLinkHealthCurrent() &&
      (!isDeepLinkDatasetCurrent || isDeepLinkDatasetCurrent())) {
    loading = false;
    if (deepLinkLoaded) {
      rearmInfiniteScrollObserver();
      requestAnimationFrame(ensureViewportHydrated);
    }
  }
  return wasSuperseded;
}

(async function() {
  var params = new URLSearchParams(window.location.search);
  var photoId = params.get('photo_id');
  if (!photoId) return;
  photoId = parseInt(photoId, 10);
  if (isNaN(photoId)) return;

  // If a folder-health refresh interrupts us, wait for it to finish and
  // retry — otherwise the deep-link is silently abandoned and Browse is
  // left on whatever the refresh happened to load (usually the unscoped
  // workspace grid, with the target photo never scrolled into view and
  // VireoFilter uninitialized) (Codex review r3686778061).
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

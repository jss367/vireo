/* Browse: photo loading: the loaded window, paging, reset-and-reload.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Photo Loading ---------- */

/* Browse's grid window is four variables that only make sense together:
   ``photos``, ``currentPage``, ``earliestPage`` and ``allLoaded`` describe one
   contiguous slice of one dataset. Any async path that writes them has to
   prove, after every await, that no newer load has taken the window over in
   the meantime — otherwise a response from an abandoned dataset paints into a
   window that has since changed (unscoped workspace rows appended to a
   folder-scoped grid, an ``earliestPage`` banner counting photos from a
   dataset that is no longer on screen). ``loadEpoch`` is that generation
   counter; these two helpers are the only supported way to use it, so every
   guard is written the same way instead of re-deriving the comparison at each
   call site (Codex reviews r3792693920 and r3792769108).

     claimBrowseWindow()   — take ownership. Advances loadEpoch, which
                             invalidates every load already in flight. Use
                             when about to replace the dataset: reset, folder
                             switch, deep link, bootstrap.
     observeBrowseWindow() — follow the current owner without displacing it.
                             Use when extending a window somebody else already
                             claimed (loadPhotos appending the next page).

   Both return a zero-arg predicate; call it after each await, before touching
   window state, releasing ``loading``, or rendering. */
function claimBrowseWindow() {
  return observeBrowseWindow(++loadEpoch);
}
function observeBrowseWindow(epoch) {
  var claimedEpoch = (epoch === undefined) ? loadEpoch : epoch;
  return function() { return loadEpoch === claimedEpoch; };
}

async function refreshBrowseWindowInPlace() {
  var windowIsCurrent = claimBrowseWindow();
  selectAllRequestSeq++;
  loading = true;
  var firstPage = earliestPage;
  var nextPhotos = [];
  var data;
  var page = firstPage;
  var succeeded = false;
  var scrollContainer = null;
  var previousOverflowAnchor;

  function neededEndPage() {
    var viewport = document.getElementById('gridContainer').getBoundingClientRect();
    var anchor = captureBrowseViewportAnchor();
    // No real card on screen means the user reached the placeholder tail;
    // retain the loaded prefix beneath that position rather than truncating it.
    var lastIndex = anchor ? Math.max(0, anchor.index) : Math.max(0, photos.length - 1);
    var cards = document.querySelectorAll('#grid > .grid-card');
    for (var i = lastIndex; i < cards.length; i++) {
      if (cards[i].getBoundingClientRect().top >= viewport.bottom) break;
      lastIndex = i;
    }
    // Keep selected photos too, including selected members of a stack. The
    // rest of the historical tail can return through ordinary lazy loading.
    photos.forEach(function(photo, index) {
      if (photo.id === selectedPhotoId || selectedPhotos.has(photo.id)
          || (photo.browse_stack && photo.browse_stack.photo_ids.some(function(id) {
            return id === selectedPhotoId || selectedPhotos.has(id);
          }))) lastIndex = Math.max(lastIndex, index);
    });
    // One extra page provides room for rows to fill the gaps after removals.
    return Math.max(firstPage, Math.min(currentPage - 1,
      firstPage + Math.floor(lastIndex / perPage) + 1));
  }

  async function stageNeededPages() {
    // Re-evaluate the live viewport after every await. A user can scroll
    // farther while either pages or expanded stack members are being fetched.
    while (windowIsCurrent() && page <= neededEndPage()
        && (!data || (page - 1) * perPage < data.total)) {
      var chunk = Math.max(1, Math.min(Math.floor(500 / perPage), neededEndPage() - page + 1));
      while ((page - 1) % chunk !== 0) chunk--;
      var request = buildBrowsePageRequest((page - 1) / chunk + 1, perPage * chunk);
      data = await safeFetch(request.url, request.options);
      if (!windowIsCurrent()) return false;
      if (data.total === 0) {
        firstPage = 1;
        page = 1;
        nextPhotos = [];
        break;
      }
      // A batch removal may have eliminated the entire focused window.
      // Fetch the new last page instead of leaving a nonempty library blank.
      if (page === firstPage && page > 1 && !data.photos.length && data.total > 0) {
        firstPage = Math.max(1, Math.ceil(data.total / perPage));
        page = firstPage;
        continue;
      }
      nextPhotos = nextPhotos.concat(data.photos);
      page += chunk;
      if ((page - 1) * perPage >= data.total || !data.photos.length) break;
    }
    return windowIsCurrent();
  }

  try {
    // Keep expanded stacks open when their surviving members still form a
    // stack, even if its cover changed. Hydrate them before the atomic paint
    // so a loading tray cannot temporarily shrink the grid under the user.
    function expandedMemberIds() {
      var ids = new Set();
      photos.forEach(function(photo) {
        if (expandedBrowseStacks.has(photo.id) && photo.browse_stack) {
          photo.browse_stack.photo_ids.forEach(function(id) { ids.add(id); });
        }
      });
      return ids;
    }
    var nextMembers = {};
    var matchingIds = null;
    while (true) {
      if (await stageNeededPages() !== true || !windowIsCurrent()) return null;
      // Recheck after each request so opening another stack while waiting
      // doesn't cause that newly opened tray to collapse on commit.
      var expandedIds = expandedMemberIds();
      var photo = nextPhotos.find(function(candidate) {
        return candidate.browse_stack && !nextMembers[candidate.id]
          && candidate.browse_stack.photo_ids.some(function(id) { return expandedIds.has(id); });
      });
      if (!photo) {
        // Selected IDs outside this page may still match: Select all includes
        // unloaded photos, and a confidence change can move a selected photo
        // to another page. Check membership without loading the whole grid.
        var stagedIds = new Set();
        nextPhotos.forEach(function(candidate) {
          if (!browsePhotoIsAvailable(candidate)) return;
          stagedIds.add(candidate.id);
          if (candidate.browse_stack) candidate.browse_stack.photo_ids.forEach(function(id) {
            stagedIds.add(id);
          });
        });
        if (matchingIds === null && data.total > 0 && Array.from(selectedPhotos).some(function(id) {
          return !stagedIds.has(id);
        })) {
          var idsRequest = buildBrowseIdsRequest();
          var idsData = await safeFetch(idsRequest.url, idsRequest.options);
          if (!windowIsCurrent()) return null;
          matchingIds = new Set(idsData.photo_ids || idsData.ids || []);
          // Scrolling, selection, or stack expansion may have changed while
          // waiting. Stage any newly needed pages before the atomic paint.
          continue;
        }
        break;
      }
      var memberIds = photo.browse_stack.photo_ids;
      var members = [];
      for (var offset = 0; offset < memberIds.length; offset += 500) {
        var memberData = await safeFetch('/api/photos/by-ids', {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({photo_ids: memberIds.slice(offset, offset + 500)}),
        });
        if (!windowIsCurrent()) return null;
        members = members.concat(memberData.photos || []);
      }
      nextMembers[photo.id] = members.map(function(member) {
        return member.id === photo.id ? photo : member;
      });
    }

    // Capture position and selection at commit time: scrolling or clicking
    // during the request must not be undone by its eventual response.
    var container = document.getElementById('gridContainer');
    var scrollTop = container.scrollTop;
    // Only the synchronous replacement owns scroll position. Other updates,
    // such as a stack finishing loading above the viewport, still need the
    // browser's normal scroll anchoring.
    scrollContainer = container;
    previousOverflowAnchor = container.style.overflowAnchor;
    container.style.overflowAnchor = 'none';
    var stillExpandedIds = expandedMemberIds();
    expandedBrowseStacks.clear();
    nextPhotos.forEach(function(photo) {
      if (photo.similarity != null) photo._similarity = photo.similarity;
      if (nextMembers[photo.id] && photo.browse_stack.photo_ids.some(function(id) {
        return stillExpandedIds.has(id);
      })) expandedBrowseStacks.add(photo.id);
    });
    browseStackMembers = nextMembers;
    browseStackErrors = {};
    browseStackHydrationRequests = {};
    browseStackExpansionRequests = {};
    browseStackCoverRecheck.clear();
    // The lightbox shares this array; retain its identity across the refresh.
    photos.length = 0;
    nextPhotos.forEach(function(photo) { photos.push(photo); });
    earliestPage = firstPage;
    currentPage = page;
    setBrowseTotals(data);
    allLoaded = loadedWindowOffset() + photos.length >= totalPhotos;
    syncBrowseAvailableLightboxPhotos();

    var availableIds = matchingIds || new Set();
    if (matchingIds === null) photos.forEach(function(photo) {
      if (!browsePhotoIsAvailable(photo)) return;
      availableIds.add(photo.id);
      if (photo.browse_stack) photo.browse_stack.photo_ids.forEach(function(id) {
        availableIds.add(id);
      });
    });
    selectedPhotos.forEach(function(id) {
      if (!availableIds.has(id)) selectedPhotos.delete(id);
    });
    if (selectedPhotoId != null && !availableIds.has(selectedPhotoId)) closeDetail();
    var selectedCover = loadedBrowseStackCoverForPhoto(selectedPhotoId);
    selectedIndex = photos.findIndex(function(photo) {
      return photo.id === selectedPhotoId || (selectedCover && photo.id === selectedCover.id);
    });
    if (selectedPhotoId == null && selectedPhotos.size === 0) hideDetailPanel();

    renderGrid({ preserveThumbnails: true });
    updateGridTail();
    updatePreviousPhotosButton();
    updateOfflineCollectionState(data);
    if (window.VireoFilter && VireoFilter.setResultTotal) VireoFilter.setResultTotal(totalUnderlyingPhotos);
    if (window.VireoFilter && VireoFilter.setVisualInfo) VireoFilter.setVisualInfo(data.visual || null);
    updateFilterSummary();
    updateBatchBar();
    _refreshBatchInspectorIfActive();
    refreshBrowseLightboxCounter();
    // Setting this synchronously prevents an intermediate frame at the top.
    // The browser only clamps it when the remaining results are too short.
    container.scrollTop = scrollTop;
    updateScrollPosition();
    var refreshIds = photos.map(function(photo) { return photo.id; });
    loadInatStatus(refreshIds).then(function() {
      if (windowIsCurrent()) refreshGridCards(refreshIds);
    });
    fetchColorLabels(refreshIds).then(function() {
      if (windowIsCurrent()) refreshGridCards(refreshIds);
    });
    succeeded = true;
    return true;
  } catch (e) {
    // safeFetch reports the error. Leave the existing grid and selection
    // usable rather than replacing them with a partial response.
    return windowIsCurrent() ? false : null;
  } finally {
    if (scrollContainer) scrollContainer.style.overflowAnchor = previousOverflowAnchor;
    if (windowIsCurrent()) {
      loading = false;
      if (succeeded) {
        rearmInfiniteScrollObserver();
        requestAnimationFrame(ensureViewportHydrated);
      }
    }
  }
}

// Both normal Browse and photo deep links initialize the shared filter bar.
// Keep their reload behavior identical for every way of removing filters.
function browseFilterReloadOptions(info) {
  var reason = info && info.reason;
  // Removing a filter should keep the photo in view. A removal in an
  // OR/NOT expression can also exclude it, so ask the server for its new
  // position instead of scanning the catalog to find it.
  if (['quickSearchCleared', 'filtersCleared', 'filterRemoved', 'filtersPaused'].includes(reason)) {
    return { preserveAnchor: true, focusAnchor: true, preserveViewport: true };
  }
  // Loading a saved expression replaces the result set wholesale, so the
  // anchor's old position says nothing about where it landed — or whether
  // the expression contains it at all. Ask the server, the same way a
  // re-sort does; paging towards it would walk the replacement result set
  // (Codex P1 on PR #1695).
  if (reason === 'expressionLoaded') {
    return { preserveAnchor: true, focusAnchor: true };
  }
  // A membership-change refresh re-runs the expression already on screen.
  if (reason === 'expressionRefreshed') return { preserveScroll: true };
}

/* The photos a focused reload may be placed by, in lookup-sized chunks.

   The card the user is holding comes first; for a stack the other frames
   they picked follow, because any of them places the same card. Chunked to
   what one lookup accepts, and never emptied by that bound — the frames
   past the first chunk are asked about in a further lookup, not discarded
   (Codex P2 on PR #1695). Empty when the reload has no anchor to place or
   is not a focused one. */
function browseFocusCandidateChunks(anchor, focusAnchor) {
  if (!anchor || !focusAnchor) return [];
  var candidates = [anchor.photoId];
  if (anchor.stackSelection && anchor.stackIds) {
    anchor.stackIds.forEach(function(id) {
      if (id != null && id !== anchor.photoId) candidates.push(id);
    });
  }
  var chunks = [];
  for (var at = 0; at < candidates.length; at += BROWSE_MAX_FOCUS_CANDIDATES) {
    chunks.push(candidates.slice(at, at + BROWSE_MAX_FOCUS_CANDIDATES));
  }
  return chunks;
}

/* Drop the loaded window — the pages, their stack caches, and the paging
   cursors — so the next load starts a fresh contiguous window. Selection,
   collection scope and anchors are the caller's business. */
function clearBrowseLoadedWindow() {
  browseDatasetReady = false;
  expandedBrowseStacks.clear();
  browseStackMembers = {};
  browseStackErrors = {};
  browseStackHydrationRequests = {};
  browseStackExpansionRequests = {};
  browseStackCoverRecheck.clear();
  photos = [];
  currentPage = 1;
  allLoaded = false;
  earliestPage = 1;
  updatePreviousPhotosButton();
}

async function resetAndLoad(options) {
  // Membership edits keep the current window on screen until its replacement
  // is complete. A reset followed by anchor restoration still paints page 1
  // while the remaining pages load, even if the final position is correct.
  if (options && options.preserveScroll && photos.length && browseDatasetReady) {
    return refreshBrowseWindowInPlace();
  }
  var preserveAnchor = !!(options && options.preserveAnchor);
  // Callers that need the current collection scope to survive a reset
  // (folder-health refresh) pass ``preserveCollection: true``. Without it
  // the default clear below would drop a normal (non-dashboard) collection
  // and send the user back to the unscoped workspace grid.
  var preserveCollection = !!(options && options.preserveCollection);
  var preserveScroll = !!(options && options.preserveScroll);
  // ``focusAnchor``: resolve the anchor's new position server-side instead of
  // paging forward until it shows up. Re-sorts and filter removals can move
  // the photo far from its old index, or remove it from the results entirely.
  var focusAnchor = !!(options && options.focusAnchor);
  // Try the single-photo anchor whenever a caller has asked us to keep the
  // user's place — ``preserveAnchor`` (explicit) or ``preserveScroll`` (the
  // membership-change fallback that has no photo of its own to focus on).
  // ``captureSelectedPhotoAnchor`` declines when a multi-selection is
  // active, so the batch-tagging case still falls through to the viewport
  // anchor below; when a single photo is selected and the edit did not
  // remove it, restoring the selection and detail panel matches the
  // documented fallback behavior (Codex review r4012897727).
  var anchor = (preserveAnchor || preserveScroll)
    ? captureSelectedPhotoAnchor()
    : null;
  // A reload arriving while a focused one is still in flight has nothing to
  // capture from the DOM — the older reload cleared the selection and tore
  // down the cards synchronously before its await. The target it was aiming
  // at is still the target, so forward it rather than dropping back to page 1
  // (Codex review r4021334323).
  //
  // What decides whether the anchor may be forwarded is the *scope*, not who
  // is asking. A sidebar folder/keyword/collection click bumps
  // ``browseScopeGen``, and the photo then belongs to the view the user left
  // — keeping it there would resurrect a selection the scope change
  // deliberately cleared. Everything else is the same dataset: a second sort
  // change, and equally a folder-health refresh or a cleared quick search,
  // both of which ask to keep the user's place via ``preserveAnchor`` and
  // would otherwise invalidate the in-flight sort and lose its selection for
  // good (Codex reviews on PR #1658).
  if (pendingFocusAnchor && pendingFocusAnchorScopeGen !== browseScopeGen) {
    pendingFocusAnchor = null;
  }
  // A reset that isn't focused and asks for no preservation is an intentional
  // selection clear — applying or narrowing a filter, for example. That path
  // does not bump ``browseScopeGen`` (the folder/collection are unchanged),
  // so the scope-generation guard above cannot see it; without dropping the
  // holder here, a following focused sort would adopt the stale anchor and
  // resurrect a selection the intervening reset just cleared, whenever the
  // photo still matched the new filter (Codex review r4021838076).
  if (!focusAnchor && !preserveAnchor && !preserveScroll) {
    pendingFocusAnchor = null;
  }
  if (!anchor && (focusAnchor || preserveAnchor) && pendingFocusAnchor) {
    anchor = pendingFocusAnchor;
  }
  // A person browsing without a single selection still has a place in the
  // grid. Focus the first visible card without turning it into a selection.
  if (!anchor && options && options.preserveViewport) {
    anchor = captureBrowseViewportAnchor();
    if (anchor) anchor.viewportOnly = true;
  }
  if (focusAnchor) {
    pendingFocusAnchor = anchor;
    pendingFocusAnchorScopeGen = browseScopeGen;
    focusReloadInFlight++;
  }
  var restoreEpoch = anchor ? ++anchorRestoreEpoch : null;
  // Membership-change reloads (``preserveScroll``) can arrive after an edit
  // that removed the selected photo from the filter — untagging its only
  // matching keyword, say. Capture the viewport anchor alongside the
  // selected-photo one so that when the paged scan below runs out of
  // budget without finding the id, we can still hold position instead of
  // resetting to ``scrollTop = 0`` (Codex review r4013123608).
  var scrollAnchor = preserveScroll
    ? captureBrowseViewportAnchor()
    : null;
  clearBrowseLoadedWindow();
  var resetWindowIsCurrent = claimBrowseWindow();
  selectAllRequestSeq++;
  loading = false;
  // Dataset is about to change; any prior selection (multi-select Set or
  // single-focus id) points at photos that may not exist in the new view.
  // Leaving them set would let updateBatchBar() keep showing "N selected"
  // and arm batch actions (delete/export/develop) against stale ids.
  selectedPhotos.clear();
  selectedPhotoId = null;
  selectedIndex = -1;
  // Leaving collection mode when applying non-collection filters (sort, rating,
  // date, keyword, folder) so the summary stays in sync with the visible grid.
  // Dashboard-scoped collections are the exception: there the collection is an
  // explicit composable restriction that filters combine WITH, so it must
  // survive filter-driven reloads (dashboard drill-down deep links).
  if (!dashboardCollectionScope && !preserveCollection) activeCollectionId = null;
  var preservedCollectionId = preserveCollection ? activeCollectionId : null;
  if (anchor) hideDetailPanel();
  else closeDetail();
  document.getElementById('grid').innerHTML = '';
  document.getElementById('gridContainer').scrollTop = 0;

  function resetRequestIsCurrent() {
    if (!resetWindowIsCurrent()) return false;
    if (activeCollectionId == null) return true;
    if (dashboardCollectionScope) return true;
    // Explicitly preserved collection: the reset is still current as long as
    // the caller-chosen scope hasn't been replaced by a newer sidebar click.
    if (preserveCollection && activeCollectionId === preservedCollectionId) return true;
    return false;
  }
  function anchorScanShouldContinue() {
    return resetRequestIsCurrent() && anchorRestoreEpoch === restoreEpoch;
  }

  if (anchor || scrollAnchor) anchorScanDepth++;
  try {
    // A stack anchor offers every frame the user picked, so the reload can
    // be placed by one it kept even when the frame we aimed at is gone
    // (Codex P2 on PR #1695). One lookup takes a bounded number of
    // candidates — they become bound parameters in one ``IN`` clause — and
    // nothing bounds how many frames a burst holds, so the frames past that
    // are asked about in a further lookup rather than dropped: a saved
    // expression can keep exactly the tail of a long burst (the few frames
    // the user rated, say) and that is still the selection it should
    // restore. Only a chunk that places nothing costs another request, so
    // every stack short enough to ask about at once — which is all of them,
    // in practice — is one request, as before.
    var focusChunks = browseFocusCandidateChunks(anchor, focusAnchor);
    var initialLoadStatus;
    for (var chunkAt = 0; chunkAt < Math.max(1, focusChunks.length); chunkAt++) {
      var chunk = focusChunks[chunkAt];
      if (chunkAt > 0) {
        // Start the replacement window clean, exactly as this reset did.
        clearBrowseLoadedWindow();
        document.getElementById('grid').innerHTML = '';
      }
      initialLoadStatus = await loadPhotos(
        chunk ? { focusPhotoId: chunk[0], focusPhotoIds: chunk.slice(1) } : undefined
      );
      if (initialLoadStatus !== true) return initialLoadStatus;
      if (!resetRequestIsCurrent()) return null;
      if (!chunk || browseResolvedFocusPhotoId != null) break;
    }
    // The frame the server placed the page by becomes the anchor: it is the
    // card the restore selects around and the one the scroll holds onto.
    // Resolved to the top-level card it is part of, because the anchor is a
    // lookup target as well as a selection: handed a hidden frame,
    // ``loadUntilPhotoRendered`` would expand that stack's tray to reach it,
    // so a plain re-sort would leave the collapsed stack the user selected
    // standing open (Codex P2 on PR #1695).
    if (anchor && focusAnchor && browseResolvedFocusPhotoId != null) {
      var resolvedCover = anchor.stackSelection
        ? loadedBrowseStackCoverForPhoto(browseResolvedFocusPhotoId)
        : null;
      anchor.photoId = resolvedCover
        ? resolvedCover.id
        : browseResolvedFocusPhotoId;
    }
    if (!anchor) {
      if (scrollAnchor) {
        await restoreBrowseViewportAnchor(scrollAnchor, resetRequestIsCurrent);
      }
      return true;
    }

    // A focused load has already been served the anchor's page: the server
    // resolved its position under the same grouping the page fetch uses, so
    // there is nothing left to search for. Budget 0 confines the lookup to
    // the pages in hand — still enough to expand a stack tray around a
    // hidden member — instead of paging towards a photo that is either
    // already here or not in this result set at all.
    //
    // ``preserveScroll`` membership refreshes are the one anchored reload
    // that still pages: they re-run the query already on screen after an
    // edit that may have moved the anchor out of it, so the anchor's old
    // position plus a page is a sound bound — and a bound there must be, or
    // a 60k-photo result set that no longer contains the photo costs ~1,200
    // sequential requests to find that out.
    var anchorScanBudget = focusAnchor
      ? 0
      : Math.max(anchor.index || 0, 0) + perPage;
    var card = await loadUntilPhotoRendered(
      anchor.photoId,
      anchorScanShouldContinue,
      { resolveStackMember: true, budget: anchorScanBudget }
    );
    if (!resetRequestIsCurrent()) return null;

    if (!anchorRestoreIsPending(anchor, restoreEpoch)) return true;
    if (!card) {
      clearActiveSelectionAndDetail();
      if (scrollAnchor) {
        await restoreBrowseViewportAnchor(scrollAnchor, resetRequestIsCurrent);
      }
      return true;
    }

    // Offline placeholders can hold the viewport without becoming a
    // selection or enabling any photo actions.
    if (anchor.viewportOnly) {
      requestAnimationFrame(function() {
        if (!resetRequestIsCurrent() || !anchorRestoreIsPending(anchor, restoreEpoch)) return;
        restorePhotoAnchor(anchor);
        updateScrollPosition();
      });
      return true;
    }

    // The anchor may have been rendered as an offline placeholder — e.g. a
    // folder-health refresh with ``showOfflineCollectionPhotos`` on keeps the
    // just-went-missing photo in the reloaded dataset with ``folder_status``
    // set to a non-``ok``/``partial`` value. Restoring the selection there
    // would let updateBatchBar() expose Develop/Export/Delete against a
    // supposedly read-only photo (Codex review r3839992514).
    var anchorPhoto = findBrowsePhoto(anchor.photoId);
    var anchorCoverId = browseStackCoverIdForPhoto(anchor.photoId);
    var anchorGridId = anchorCoverId == null ? anchor.photoId : anchorCoverId;
    var anchorIndex = photos.findIndex(function(p) { return p.id === anchorGridId; });
    if (anchorIndex < 0 || !anchorPhoto || !browsePhotoIsAvailable(anchorPhoto)) {
      clearActiveSelectionAndDetail();
      if (scrollAnchor) {
        await restoreBrowseViewportAnchor(scrollAnchor, resetRequestIsCurrent);
      }
      return true;
    }

    // A selected stack comes back as a selected stack — as the frames the
    // user actually picked, intersected with the group the reload came back
    // with. Adopting the reloaded group's membership wholesale would widen
    // the selection whenever an edit merged a neighbouring frame into the
    // burst: undo/redo re-runs the query and ``afterHistoryChange`` folds the
    // pre-action ids back in on top, so a later batch export or delete would
    // act on a frame the user never selected (Codex P2 on PR #1695). The
    // intersection is also why the ids cannot simply be replayed: a frame
    // this reload dropped from the group is no longer part of the card the
    // user is looking at, and a stack with only some of its frames selected
    // paints the partial mark that says exactly that.
    var currentStackIds = anchor.stackSelection
      ? browseStackMemberIds(anchorGridId)
      : null;
    var restoredStackIds = currentStackIds
      ? currentStackIds.filter(function(id) {
          return anchor.stackIds.indexOf(id) !== -1;
        })
      : null;
    if (restoredStackIds && !restoredStackIds.length) restoredStackIds = null;
    if (anchor.stackSelection && !restoredStackIds) {
      // The group did not survive the reload — Stacks switched off
      // mid-flight, or the frames no longer group. Hold the position
      // without inventing a single-photo selection the user never made:
      // Delete/Export/Develop act on whatever is selected.
      requestAnimationFrame(function() {
        if (!resetRequestIsCurrent()) return;
        if (!anchorRestoreIsPending(anchor, restoreEpoch)) return;
        restorePhotoAnchor(anchor);
        updateScrollPosition();
      });
      return true;
    }

    if (restoredStackIds) {
      selectedPhotos = new Set(restoredStackIds);
      selectedPhotoId = null;
      selectedIndex = anchorIndex;
      abandonDetailFocusForBatch();
    } else {
      selectedPhotoId = anchor.photoId;
      selectedIndex = anchorIndex;
    }
    refreshCardSelectionVisuals();
    updateBatchBar();
    if (anchor.detailVisible) loadDetail(anchor.photoId);
    requestAnimationFrame(function() {
      if (!resetRequestIsCurrent()) return;
      if (!anchorSelectionIsCurrent(anchor, restoreEpoch)) return;
      restorePhotoAnchor(anchor);
      updateScrollPosition();
    });
    return true;
  } finally {
    if (focusAnchor) {
      focusReloadInFlight = Math.max(0, focusReloadInFlight - 1);
      // Only clear the shared anchor once no focused reload is still in
      // flight — otherwise the first reload's ``finally`` (it exits early
      // when a newer reload has invalidated its window) would drop the
      // anchor the newer reload is still relying on.
      if (focusReloadInFlight === 0) pendingFocusAnchor = null;
    }
    if (anchor || scrollAnchor) {
      anchorScanDepth = Math.max(0, anchorScanDepth - 1);
      if (anchorScanDepth === 0) {
        rearmInfiniteScrollObserver();
        requestAnimationFrame(ensureViewportHydrated);
      }
    }
  }
}

function getBrowseRules() {
  if (!window.VireoFilter || !VireoFilter.getRules) return null;
  var rules = VireoFilter.getRules();
  var count = Array.isArray(rules) ? rules.length : (rules.rules || []).length;
  return count ? rules : null;
}

function buildCurrentBrowseParams() {
  var params = new URLSearchParams();
  params.set('sort', document.getElementById('sortSelect').value);
  if (activeFolderId) params.set('folder_id', activeFolderId);
  if (activeCollectionId && dashboardCollectionScope) {
    params.set('collection_id', activeCollectionId);
  }
  var rules = getBrowseRules();
  if (rules) params.set('rules', JSON.stringify(rules));
  return params;
}

/* How many photos of the current dataset sit before ``photos[0]``. Normally 0
   — Browse loads a contiguous prefix — but a ?photo_id=... deep link starts at
   the target's page, so everything that reasons about how much of the dataset
   is on screen (tail runway, allLoaded gate, position readout) has to add it
   back. */
function loadedWindowOffset() {
  return Math.max(0, (earliestPage - 1) * perPage);
}

function updatePreviousPhotosButton() {
  var banner = document.getElementById('loadPreviousPhotosBanner');
  if (!banner) return;
  var offset = loadedWindowOffset();
  if (earliestPage <= 1 || offset <= 0) {
    banner.style.display = 'none';
    return;
  }
  var textEl = document.getElementById('loadPreviousPhotosText');
  if (textEl) {
    // With Stacks on the grid pages logical items, not photos, so this
    // offset counts cards — and a single card can stand for a whole burst.
    // Calling them photos would undercount, badly: fifty earlier stacks can
    // be hundreds of frames. Say what the number actually counts (Codex
    // review on PR #1658; CORE_PHILOSOPHY, "no black boxes").
    var noun = browseStacksEnabled()
      ? (offset === 1 ? 'card' : 'cards')
      : (offset === 1 ? 'photo' : 'photos');
    var verb = offset === 1 ? 'isn’t' : 'aren’t';
    textEl.textContent =
      offset.toLocaleString() + ' earlier ' + noun + ' ' + verb +
      ' loaded — this grid starts at #' + (offset + 1).toLocaleString() + '.';
  }
  banner.style.display = 'block';
}

async function loadPreviousPhotos() {
  if (loading || earliestPage <= 1) return false;
  // Do not prepend an offset page read from a newer database snapshot to the
  // focused page. Ingestion/deletion between those requests can overlap or
  // skip the boundary. Restarting at page 1 hands control back to the normal
  // contiguous lazy loader while keeping this user-requested transition
  // bounded to one configured-size page.
  earliestPage = 1;
  updatePreviousPhotosButton();
  return resetAndLoad();
}

/* ``requestOptions.focusPhotoId`` asks the server for the page that photo sits
   on rather than the requested one (see ``loadPhotos``). Only the rules
   endpoint can express it, so the returned request reports back which id it
   actually carries — a saved-collection page has to fall back to loading from
   the top rather than silently believing page 1 holds the photo. */
function buildBrowsePageRequest(page, requestPerPage, requestOptions) {
  var focusPhotoId = (requestOptions && requestOptions.focusPhotoId != null)
    ? requestOptions.focusPhotoId
    : null;
  // The other frames of a focused stack card. Any of them places the same
  // card, so a reload that dropped the frame we asked about first can still
  // be placed by one it kept — in the same request, rather than one retry
  // per frame (Codex P2 on PR #1695).
  //
  // Trimmed to what one lookup accepts. Every candidate becomes a bound
  // parameter in one ``IN`` clause, so an over-long list would come back a
  // 400 — and this request runs after the window has been cleared, which
  // would leave the grid empty and the selection gone (Codex P2 on
  // PR #1695). Callers that have more frames than this send the rest in a
  // further lookup (``browseFocusCandidateChunks``); the trim here is what
  // makes *this request* valid, not where the extra frames go.
  var focusPhotoIds = (requestOptions && requestOptions.focusPhotoIds
      ? requestOptions.focusPhotoIds
          .filter(function(id) { return id != null && id !== focusPhotoId; })
          .slice(0, BROWSE_MAX_FOCUS_CANDIDATES - (focusPhotoId == null ? 0 : 1))
      : []);
  if (activeCollectionId && !dashboardCollectionScope) {
    var cparams = new URLSearchParams();
    cparams.set('page', page);
    cparams.set('per_page', requestPerPage);
    cparams.set('sort', document.getElementById('sortSelect').value);
    if (browseStacksEnabled()) cparams.set('stacks', '1');
    if (focusPhotoId != null) cparams.set('focus_photo_id', focusPhotoId);
    if (focusPhotoIds.length) {
      cparams.set('focus_photo_ids', focusPhotoIds.join(','));
    }
    return {
      url: '/api/collections/' + activeCollectionId + '/photos?' + cparams.toString(),
      options: {},
      focusPhotoId: focusPhotoId,
    };
  }

  var body = {
    rules: getBrowseRules() || [],
    sort: document.getElementById('sortSelect').value,
    page: page,
    per_page: requestPerPage,
    stacks: browseStacksEnabled(),
  };
  if (hasCollectionAvailabilityScope()) {
    body.include_availability = true;
    if (showOfflineCollectionPhotos) body.include_offline = true;
  }
  var visual = window.VireoFilter && VireoFilter.getVisual ? VireoFilter.getVisual() : null;
  if (visual) body.visual = visual;
  if (activeFolderId) body.folder_id = activeFolderId;
  if (activeCollectionId && dashboardCollectionScope) body.collection_id = activeCollectionId;
  if (focusPhotoId != null) body.focus_photo_id = focusPhotoId;
  if (focusPhotoIds.length) body.focus_photo_ids = focusPhotoIds;
  // Every caller builds this right after claiming or observing the grid
  // window, and a response from an older ``loadEpoch`` is always dropped, so
  // the server may abandon one as soon as a newer epoch's request arrives.
  var headers = Object.assign(
    { 'Content-Type': 'application/json' },
    Vireo.api.searchLaneHeaders('grid', loadEpoch)
  );
  return {
    url: '/api/photos/query',
    options: {
      method: 'POST',
      headers: headers,
      body: JSON.stringify(body),
    },
    focusPhotoId: focusPhotoId,
  };
}

/* Extend a focused deep-link window upward by one page. Unlike the explicit
   "Browse from beginning" action, this preserves the user's viewport: the
   first previously-loaded card remains at the same screen position while the
   new cards are inserted above it. */
async function prependPreviousPhotos(options) {
  if (loading || earliestPage <= 1) return false;
  loading = true;
  var windowIsCurrent = observeBrowseWindow();
  var loadSucceeded = false;
  var requestedPage = earliestPage - 1;
  var request = buildBrowsePageRequest(requestedPage, perPage);

  try {
    var data = await safeFetch(request.url, request.options, {
      toast: !(options && options.silent),
    });
    if (!windowIsCurrent()) return null;

    setBrowseTotals(data);
    if (window.VireoFilter && VireoFilter.setResultTotal) VireoFilter.setResultTotal(totalUnderlyingPhotos);
    if (window.VireoFilter && VireoFilter.setVisualInfo) {
      VireoFilter.setVisualInfo(data.visual || null);
    }
    (data.photos || []).forEach(function(p) {
      if (p.similarity != null) p._similarity = p.similarity;
    });

    // Offset paging can overlap by a row if an ingestion/deletion lands
    // between the focused init snapshot and this request. Never duplicate a
    // card in the live window; the next dataset reset will reconcile ordering.
    var loadedIds = new Set(photos.map(function(p) { return String(p.id); }));
    var preceding = (data.photos || []).filter(function(p) {
      var key = String(p.id);
      if (loadedIds.has(key)) return false;
      loadedIds.add(key);
      return true;
    });

    // Capture at response time, not request time: the user can continue
    // scrolling while the network request is in flight, and the prepend must
    // preserve the position they reached rather than undoing that movement.
    var oldFirstPhoto = photos.length ? photos[0] : null;
    var oldFirstCard = oldFirstPhoto ? getGridCard(oldFirstPhoto.id) : null;
    var oldFirstTop = oldFirstCard ? oldFirstCard.getBoundingClientRect().top : null;

    // Keep the array identity stable. The shared lightbox holds this same
    // object while it is open, so mutating it lets Previous navigation see
    // the newly prepended page without closing and reopening the viewer.
    Array.prototype.unshift.apply(photos, preceding);
    syncBrowseAvailableLightboxPhotos();
    earliestPage = requestedPage;
    prependGridPhotos(preceding);
    updatePreviousPhotosButton();
    allLoaded = loadedWindowOffset() + photos.length >= totalPhotos;
    updateGridTail();
    updateFilterSummary();
    refreshBrowseLightboxCounter();

    // Adding rows changes scrollHeight. Compensate by the anchor's exact
    // layout delta so there is no visual jump, even with a responsive number
    // of grid columns.
    if (oldFirstPhoto && oldFirstTop != null) {
      var movedFirstCard = getGridCard(oldFirstPhoto.id);
      if (movedFirstCard) {
        document.getElementById('gridContainer').scrollTop +=
          movedFirstCard.getBoundingClientRect().top - oldFirstTop;
      }
    }

    if (preceding.length > 0) {
      var refreshIds = preceding.map(function(p) { return p.id; });
      loadInatStatus(refreshIds).then(function() {
        if (windowIsCurrent()) refreshGridCards(refreshIds);
      });
      fetchColorLabels(refreshIds).then(function() {
        if (windowIsCurrent()) refreshGridCards(refreshIds);
      });
    }
    loadSucceeded = true;
  } catch(e) {
    if (!windowIsCurrent()) return null;
  } finally {
    if (windowIsCurrent()) {
      loading = false;
      if (loadSucceeded && anchorScanDepth === 0) {
        rearmInfiniteScrollObserver();
        requestAnimationFrame(ensureViewportHydrated);
      }
    }
  }
  return loadSucceeded;
}

async function loadPhotos(options) {
  if (loading) return null;
  if (allLoaded) return true;
  loading = true;
  // Extends the window its caller (resetAndLoad / bootstrap / deep link /
  // the intersection observer) already owns — observe, never claim.
  var windowIsCurrent = observeBrowseWindow();
  var loadSucceeded = false;
  var showLoading = !(options && options.silent) && photos.length === 0;
  var loadingState = document.getElementById('loadingState');
  if (showLoading && loadingState) {
    loadingState.textContent = 'Loading...';
    loadingState.style.display = 'block';
  }

  // ``options.focusPhotoId`` turns this into a focused load: the server
  // reports which page of the current query that photo lands on and returns
  // that page instead of the requested one. Multi-page chunking would make
  // the returned ``focus_page`` ambiguous (it counts perPage-sized pages),
  // so a focused load always asks for exactly one page.
  //
  // ``options.focusPhotoIds`` names the other photos that would do just as
  // well — the frames of a selected stack, which all share their card's
  // position. The server places the first of them the result set still
  // contains and names it in ``focus_photo_id``; the caller reads which
  // one answered from ``browseResolvedFocusPhotoId``.
  var focusPhotoId = (options && options.focusPhotoId != null)
    ? options.focusPhotoId
    : null;
  var focusPhotoIds = (options && options.focusPhotoIds) || null;
  if (focusPhotoId != null) browseResolvedFocusPhotoId = null;
  // When placeholder cells are already visible, the user is actively
  // waiting on this load — fetch several pages in one request. Chunks must
  // stay aligned to perPage-sized pages so page/per_page arithmetic holds
  // (server caps per_page at 500).
  var chunk = 1;
  var tailEl = document.getElementById('gridTail');
  if (focusPhotoId == null && tailEl && tailEl.firstElementChild) {
    var contRect = document.getElementById('gridContainer').getBoundingClientRect();
    if (tailEl.firstElementChild.getBoundingClientRect().top < contRect.bottom) {
      var pagesLoaded = currentPage - 1;
      if (perPage * 4 <= 500 && pagesLoaded % 4 === 0) chunk = 4;
      else if (perPage * 2 <= 500 && pagesLoaded % 2 === 0) chunk = 2;
    }
  }
  var reqPage = (currentPage - 1) / chunk + 1;
  var reqPerPage = perPage * chunk;

  var request = buildBrowsePageRequest(
    reqPage, reqPerPage,
    focusPhotoId == null
      ? null
      : { focusPhotoId: focusPhotoId, focusPhotoIds: focusPhotoIds }
  );

  try {
    var data = await safeFetch(request.url, request.options);
    if (!windowIsCurrent()) return null;  // grid was reset mid-flight; drop stale response
    setBrowseTotals(data);
    // Availability totals are photo counts, so they are read from the same
    // response that feeds ``totalUnderlyingPhotos`` — never from the
    // stack-collapsed ``data.total``.
    updateOfflineCollectionState(data);
    if (window.VireoFilter && VireoFilter.setResultTotal) VireoFilter.setResultTotal(totalUnderlyingPhotos);
    if (window.VireoFilter && VireoFilter.setVisualInfo) {
      VireoFilter.setVisualInfo(data.visual || null);
    }
    (data.photos || []).forEach(function(p) {
      if (p.similarity != null) p._similarity = p.similarity;
    });
    // A focused load can come back from the middle of the dataset. Anchor
    // the loaded window on that page — the same shape a ?photo_id deep link
    // produces — so ``loadedWindowOffset`` keeps reporting how much of the
    // result set sits above the grid and the "N earlier photos aren't
    // loaded" banner says so out loud. Without a resolved position the
    // server served the page we asked for, so leave the window alone.
    var focusedPage = null;
    if (request.focusPhotoId != null && (data.photos || []).length > 0
        && Number.isInteger(data.focus_index) && data.focus_index >= 0
        && Number.isInteger(data.focus_page) && data.focus_page >= 1) {
      focusedPage = data.focus_page;
      earliestPage = focusedPage;
      currentPage = focusedPage;
      // Which candidate the server placed the page by. Usually the one we
      // asked about first; a stack whose leading frames this result set
      // dropped answers with a frame it kept.
      browseResolvedFocusPhotoId = Number.isInteger(data.focus_photo_id)
        ? data.focus_photo_id
        : request.focusPhotoId;
    }
    var firstNewIdx = photos.length;

    if (data.photos.length === 0) {
      allLoaded = true;
    } else {
      // Keep the array identity stable. The shared lightbox navigates the
      // same object and can therefore continue into this newly loaded page.
      Array.prototype.push.apply(photos, data.photos);
      syncBrowseAvailableLightboxPhotos();
      currentPage += chunk;
      // A deep-link window starts partway in, so it is exhausted when it
      // reaches the end of the dataset — not when it holds ``totalPhotos``
      // rows, which it never would.
      if (loadedWindowOffset() + photos.length >= totalPhotos) allLoaded = true;
    }
    if (focusedPage !== null) updatePreviousPhotosButton();

    if (firstNewIdx === 0) renderGrid();
    else appendGridPhotos(data.photos, firstNewIdx);
    browseDatasetReady = true;
    updateGridTail();
    updateFilterSummary();
    refreshBrowseLightboxCounter();

    // Refresh appended cards once async metadata resolves — appendGridPhotos
    // only renders each page once, so badges/labels would otherwise stay
    // missing until an unrelated full grid render.
    if (data.photos.length > 0) {
      var refreshIds = data.photos.map(function(p) { return p.id; });
      loadInatStatus(refreshIds).then(function() {
        if (windowIsCurrent()) refreshGridCards(refreshIds);
      });
      fetchColorLabels(refreshIds).then(function() {
        if (windowIsCurrent()) refreshGridCards(refreshIds);
      });
    }
    loadSucceeded = true;
  } catch(e) {
    if (!windowIsCurrent()) return null;
    if (showLoading && loadingState) {
      loadingState.textContent = 'Error loading photos.';
    }
  } finally {
    if (windowIsCurrent()) {
      loading = false;
      if (showLoading && loadingState) loadingState.style.display = 'none';
      // Re-check the scan depth live (not a load-start snapshot): a stale
      // scan can drain during our fetch, and its own hydration rAF then
      // bails on loading=true. In that overlap this load is the current
      // view's only chance to re-arm.
      if (loadSucceeded && anchorScanDepth === 0) {
        rearmInfiniteScrollObserver();
        // Chain: keep loading while the viewport is at or past the loading
        // boundary (rAF so the freshly appended cards have a layout first).
        requestAnimationFrame(ensureViewportHydrated);
      }
    }
  }
  return loadSucceeded;
}

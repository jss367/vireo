/* Browse: batch edits over the selection (rating, flag, keywords, delete, collections, capture time, iNaturalist).
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

var batchKeywordDialog = VireoDialog.create(/** @type {HTMLElement} */ (document.getElementById('batchKeywordModal')), {
  initialFocus: /** @type {HTMLInputElement} */ (document.getElementById('batchKeywordInput')),
  onDismiss: hideBatchKeywordModal,
});
var batchCollectionDialog = VireoDialog.create(/** @type {HTMLElement} */ (document.getElementById('batchCollectionModal')), {
  initialFocus: /** @type {HTMLInputElement} */ (document.getElementById('batchCollectionNewName')),
  onDismiss: hideBatchCollectionModal,
});

/* ---------- iNaturalist ---------- */
async function loadInatStatus(/** @type {number[]} */ photoIds) {
  if (photoIds.length === 0) return;
  try {
    // Chunk the GET URL so oversized stacks (or any large id set) don't
    // exceed the browser/proxy/server request-target limit and drop the
    // whole page's iNaturalist badges when a stack expansion hydrates a
    // large member list (Codex P2 on PR #1561). Matches the 500-id cap
    // the /api/photos/by-ids stack-expansion path already uses.
    for (var offset = 0; offset < photoIds.length; offset += 500) {
      var chunk = photoIds.slice(offset, offset + 500);
      var data = await safeFetch(
        '/api/inat/submissions?photo_ids=' + chunk.join(','),
        {}, { toast: false },
      );
      if (data && !data.error) {
        for (var k in data) inatSubmitted[k] = true;
      }
    }
  } catch(e) {}
}

function batchSubmitInat() {
  var ids = getActiveSelection();
  if (ids.length === 0) {
    if (typeof showToast === 'function') showToast('Select one or more photos to send to iNaturalist.', 'error');
    else alert('Select one or more photos to send to iNaturalist.');
    return;
  }
  submitToInatBatch(ids);
}

// If the batch inspector is visible, keep its Mixed/active state in sync with
// the mutation we just applied. Keyboard shortcuts (rate_*, flag, color_*) go
// straight through batchSetRating/Flag/ColorLabel without touching the sidebar
// re-render path, so without this the stars/flag/color chips display the
// pre-edit state — and clicking the same star clears the value the shortcut
// just applied (batchRate treats matching-unanimous as toggle-off).
function _refreshBatchInspectorIfActive() {
  var detail = /** @type {HTMLElement} */ (document.getElementById('detailContent'));
  if (detail && detail.classList.contains('batch-mode')) {
    // State-only refresh — the selection did not change. Preserve any
    // in-progress location input the user is typing.
    renderBatchInspector(getActiveSelection(), { preserveLocation: true });
  }
}

async function batchSetRating(/** @type {number} */ rating, /** @type {number[] | undefined} */ photoIds = undefined) {
  var ids = photoIds ? photoIds.slice() : getActiveSelection();
  try {
    await safeFetch('/api/batch/rating', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids, rating: rating}),
    });
  } catch(e) { return; }
  ids.forEach(function(id) {
    var p = findBrowsePhoto(id);
    if (p) p.rating = rating;
  });
  await reconcileBrowseStackCovers(ids);
  refreshGridCards(ids);
  refreshExpandedBrowseStackMembers(ids);
  _refreshBatchInspectorIfActive();
  scheduleCollectionCountsRefresh();
  refreshPendingSyncBanner();
  showUndoToast();
}

// Server-side representative eligibility (see get_species_representatives)
// hides rejected photos. Mirror that here so the grid badge and the shared
// representative context menu ("Already representative" hint on the current
// entry) don't keep treating a just-rejected photo as still representative
// until a reload. Un-rejecting is left as-is: the client can't know whether
// the DB preference still points at this photo, so the badge stays hidden
// until the page reloads rather than lighting up incorrectly.
function _clearRepresentativeStateIfIneligible(/** @type {number} */ photoId, /** @type {BrowsePhoto['flag']} */ flag) {
  if (flag !== 'rejected') return;
  var p = findBrowsePhoto(photoId);
  if (!p) return;
  if (p.is_species_representative) p.is_species_representative = false;
  if (Array.isArray(p.life_list)) {
    p.life_list.forEach(function(entry) {
      if (!entry) return;
      if (entry.is_current_photo) entry.is_current_photo = false;
      if (entry.is_species_representative) entry.is_species_representative = false;
    });
  }
}

// Every render path that has (keywordId, name) pairs feeds this map so the
// single-photo remove path can resolve the name even before the autocomplete
// field is ever opened. keywordAutocompleteCache starts as null and only
// populates after that field renders, so on a fresh Browse page the previous
// lookup returned null and _clearRepresentativeStateAfterKeywordRemoval
// no-oped — the grid badge/context-menu state stayed stale until reload.
/** @type {Record<string, string>} */ var _keywordNamesById = {};
function _rememberKeywordNames(/** @type {BrowseKeyword[]} */ keywords) {
  if (!Array.isArray(keywords)) return;
  keywords.forEach(function(k) {
    if (k && typeof k.id === 'number' && typeof k.name === 'string') {
      _keywordNamesById[k.id] = k.name;
    }
  });
}

function _keywordNameFromCache(/** @type {number} */ keywordId) {
  if (Object.prototype.hasOwnProperty.call(_keywordNamesById, keywordId)) {
    return _keywordNamesById[keywordId];
  }
  if (!Array.isArray(keywordAutocompleteCache)) return null;
  for (var i = 0; i < keywordAutocompleteCache.length; i++) {
    var k = keywordAutocompleteCache[i];
    if (k && k.id === keywordId) return k.name;
  }
  return null;
}

// Keyword edits update the detail panel from /api/photos/:id, but Browse card
// badges render from the separate `photos` cache populated by /api/photos.
// Refresh the loaded rows from the authoritative batch endpoint after every
// keyword mutation so taxonomy badges and representative state cannot drift.
//
// Rating and flag ride along for the one caller that has no other way to learn
// them: undo/redo writes on the server and then announces, so nothing applied
// the reversal to the local rows. They are also the only two edit-reversible
// inputs to browseStackCoverCompare, so a stale copy does not just mis-render a
// badge — it leaves a collapsed stack led by a photo the database no longer
// ranks first. Rather than gate that behind a flag, the rows that actually
// moved are recorded and only their stacks are reconciled: keyword callers see
// no rating/flag change and so pay nothing, while any future caller gets the
// cover fixed for free instead of silently drifting.
async function _refreshBrowseKeywordState(/** @type {readonly number[]} */ photoIds, /** @type {BrowseKeywordRefreshOptions} */ opts = {}) {
  opts = opts || {};
  // Species keywords are exactly the input the prediction panels' ambiguity
  // and "N missing" counts are computed from, so repaint them here rather
  // than at each keyword call site — see refreshPredictionPanels. Done first,
  // and before the early returns below, so it still runs when the mutated
  // photos are outside the loaded grid page. `skipPanels` is for the one
  // caller that repaints the whole sidebar itself (undo/redo), which would
  // otherwise fetch the prediction panels twice.
  if (!opts.skipPanels) refreshPredictionPanels(opts);
  if (!Array.isArray(photoIds) || !photoIds.length) return;
  // Invalidate from the full id list, before it is narrowed below to the rows
  // this function can actually repaint. A stack tray offers "Select all" while
  // it is still loading, so a keyword edit can land on members that are in
  // neither `photos` nor `browseStackMembers` yet — exactly the members an
  // in-flight expansion or cover hydration is about to cache from a pre-edit
  // response. Narrowing first dropped those ids, and when every touched member
  // was unloaded the early return below skipped invalidation entirely, letting
  // the pre-edit payload install members missing the species the user had just
  // added. The trailing refreshExpandedBrowseStackMembers() marks again for the
  // rows it repaints; marking is idempotent.
  markBrowseStackExpansionsStale(photoIds);
  markBrowseStackHydrationsStale(photoIds);
  var ids = Array.from(new Set(photoIds)).filter(function(id) {
    return !!findBrowsePhoto(id);
  });
  if (!ids.length) return;

  /** @type {number[]} */ var refreshed = [];
  // Photos whose rating or flag actually moved, i.e. whose stack's cover may
  // need recomputing. Kept separate from `refreshed` so an undo of a keyword
  // edit does not rehydrate every collapsed stack on the page.
  /** @type {number[]} */ var coverRankChanged = [];
  try {
    for (var offset = 0; offset < ids.length; offset += 500) {
      var data = await safeFetch('/api/photos/by-ids', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({photo_ids: ids.slice(offset, offset + 500)}),
      }, {toast: false});
      (data.photos || []).forEach(function(/** @type {BrowsePhoto} */ updated) {
        var local = findBrowsePhoto(updated.id);
        if (!local) return;
        local.species = Array.isArray(updated.species) ? updated.species : [];
        local.life_list = Array.isArray(updated.life_list) ? updated.life_list : [];
        local.species_representatives = Array.isArray(updated.species_representatives)
          ? updated.species_representatives
          : local.life_list;
        local.is_species_representative = !!updated.is_species_representative;
        // A prediction accept/reject moves this even though no keyword
        // changed, and the badge would otherwise keep showing the old score
        // until an unrelated reload (Codex P2 on PR #1670). Skipped for a
        // stack card whose badge is its leading member's score: /by-ids is
        // unstacked, so it would answer with the cover's own number. Those
        // cards only appear under a confidence sort, which reloads the whole
        // grid on a prediction edit anyway.
        if (!local.prediction_confidence_is_stack_lead) {
          local.prediction_confidence = updated.prediction_confidence === undefined
            ? null : updated.prediction_confidence;
        }
        var nextRating = updated.rating === undefined ? null : updated.rating;
        var nextFlag = updated.flag === undefined ? null : updated.flag;
        var localRating = local.rating === undefined ? null : local.rating;
        // `browseStackFlagRank` folds null and 'none' together, which is also
        // how the card badge renders them, so neither a cover nor a badge can
        // change between those two spellings.
        if (localRating !== nextRating
            || browseStackFlagRank(local.flag) !== browseStackFlagRank(nextFlag)) {
          coverRankChanged.push(updated.id);
        }
        local.rating = nextRating;
        local.flag = nextFlag;
        refreshed.push(updated.id);
      });
    }
  } catch (e) {
    // The keyword save already succeeded; a later page reload remains a safe
    // fallback if this non-critical card refresh is interrupted. Undo/redo is
    // the exception and passes `reportFailure`: there the server has already
    // reversed an edit the grid is still displaying, so staying quiet would
    // present a working Undo as a no-op.
    if (opts.reportFailure) {
      showToast(
        'Could not refresh the grid after that undo — cards may still show the '
        + 'previous ratings, flags and stack covers. Reload the page to be sure.',
        'warning'
      );
    }
  }
  // Before the card repaints below, for the reason batchSetFlag orders it this
  // way: a promotion replaces the top-level row, and renderGrid() has to have
  // put the new cover in place before individual cards are refreshed. Failed
  // hydrations inside here report themselves with the '!' recheck marker.
  if (coverRankChanged.length) await reconcileBrowseStackCovers(coverRankChanged);
  if (refreshed.length) {
    refreshGridCards(refreshed);
    refreshExpandedBrowseStackMembers(refreshed);
  }
}

// Server-side eligibility requires the photo to still carry the species
// keyword, so removing a species keyword drops the photo from the eligible
// representative set. Mirror that locally: strip life_list entries whose
// species matches the removed keyword name and recompute the badge flag.
// The context menu reads photo.life_list too, so this also stops the shared
// "Already representative" hint from lingering on a photo the DB no longer
// counts. Non-species keywords never appear in life_list, so the filter
// no-ops for those. If the cache lookup fails to resolve the name, we
// leave state alone rather than clearing the wrong entry.
function _clearRepresentativeStateAfterKeywordRemoval(/** @type {readonly number[]} */ photoIds, /** @type {string | null} */ keywordName) {
  if (!Array.isArray(photoIds) || !photoIds.length) return;
  if (!keywordName) return;
  /** @type {number[]} */ var touched = [];
  photoIds.forEach(function(id) {
    var p = findBrowsePhoto(id);
    if (!p || !Array.isArray(p.life_list)) return;
    var next = p.life_list.filter(function(entry) {
      return !entry || entry.species !== keywordName;
    });
    if (next.length !== p.life_list.length) {
      p.life_list = next;
      var isRep = next.some(function(entry) {
        return entry && entry.is_species_representative;
      });
      if (p.is_species_representative !== isRep) {
        p.is_species_representative = isRep;
      }
      touched.push(id);
    }
  });
  if (touched.length) {
    refreshGridCards(touched);
    refreshExpandedBrowseStackMembers(touched);
  }
}

async function batchSetFlag(/** @type {NonNullable<BrowsePhoto['flag']>} */ flag, /** @type {number[] | undefined} */ photoIds = undefined) {
  var ids = photoIds ? photoIds.slice() : getActiveSelection();
  try {
    await safeFetch('/api/batch/flag', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids, flag: flag}),
    });
  } catch(e) { return; }
  ids.forEach(function(id) {
    var p = findBrowsePhoto(id);
    if (p) p.flag = flag;
    _clearRepresentativeStateIfIneligible(id, flag);
  });
  await reconcileBrowseStackCovers(ids);
  refreshGridCards(ids);
  refreshExpandedBrowseStackMembers(ids);
  if (selectedPhotoId != null && ids.indexOf(selectedPhotoId) !== -1) {
    updateDetailFlagButtons(flag);
  }
  _refreshBatchInspectorIfActive();
  scheduleCollectionCountsRefresh();
  showUndoToast();
}

async function batchSetColorLabel(/** @type {string | null} */ color, /** @type {number[] | undefined} */ photoIds = undefined) {
  var ids = photoIds ? photoIds.slice() : getActiveSelection();
  if (!ids.length) return;
  _noteColorLabelEdits(ids);
  try {
    await safeFetch('/api/batch/color_label', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids, color: color}),
    });
  } catch(e) { _recoverColorLabelsAfterFailedWrite(ids); return; }
  _noteColorLabelEdits(ids);
  ids.forEach(function(id) {
    if (color) colorLabels[id] = color;
    else delete colorLabels[id];
    colorLabelsFetched.add(id);
  });
  refreshGridCards(ids);
  refreshExpandedBrowseStackMembers(ids);
  _refreshBatchInspectorIfActive();
  scheduleCollectionCountsRefresh();
  showUndoToast();
}

function batchAddKeyword(/** @type {Element | null | undefined} */ returnFocus = undefined) {
  /** @type {HTMLElement} */ (document.getElementById('batchKeywordTitle')).textContent = 'Add keyword to ' + getActiveSelection().length + ' photos';
  /** @type {HTMLInputElement} */ (document.getElementById('batchKeywordInput')).value = '';
  getKeywordAutocompleteState('batchKeywordInput').selectedKeyword = null;
  hideKeywordSuggestions('batchKeywordInput');
  batchKeywordDialog.open(returnFocus);
}

function hideBatchKeywordModal() {
  batchKeywordDialog.close();
  hideKeywordSuggestions('batchKeywordInput');
  window._vireoNativeMenuPhotoIdsOverride = null;
}

async function confirmBatchKeyword() {
  var input = /** @type {HTMLInputElement} */ (document.getElementById('batchKeywordInput'));
  var name = input.value.trim();
  if (!name) return;
  var ids = getActiveSelection();
  hideBatchKeywordModal();
  var state = getKeywordAutocompleteState('batchKeywordInput');
  var selectedKeyword = state.selectedKeyword && state.selectedKeyword.name === name
    ? state.selectedKeyword
    : null;
  var payload = selectedKeyword && selectedKeyword.id
    ? {photo_ids: ids, keyword_id: selectedKeyword.id}
    : {photo_ids: ids, name: name};
  try {
    await safeFetch('/api/batch/keyword', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
  } catch(e) { return; }
  await _refreshBrowseKeywordState(ids);
  if (!selectedKeyword) invalidateKeywordAutocompleteCache();
  // Re-read the live selection: it may have moved while the POST and the
  // refresh were in flight. Loading the pre-await ``ids`` here would win the
  // sequence race and leave the panel's Add/Remove buttons bound to photos
  // that are no longer selected.
  /** @type {BrowseInitializedPanels} */ (Vireo && Vireo.browse).panelRequests.keywords.invalidate();
  loadSelectionKeywordSuggestions(getActiveSelection());
  loadKeywords();
  scheduleCollectionCountsRefresh();
  refreshActiveCollectionAfterMembershipChange([MUTATION_KEYWORD]);
  refreshPendingSyncBanner();
  showUndoToast();
}

var _batchDeleteRequestSeq = 0;

async function batchDelete() {
  var ids = getActiveSelection();
  if (ids.length === 0) return;
  // Ask the server how many of these carry a companion file. A selection can
  // hold photos Browse has never loaded — every frame of a collapsed stack,
  // or a Select all that reaches past the loaded page — and counting the
  // loaded ones only hides the "Also delete N companion files" checkbox, so a
  // disk delete silently leaves those companions behind. Refuse to open the
  // dialog rather than open it with a count that cannot be trusted: a wrong
  // number here is a file left on disk. Codex P2 on PR #1672.
  //
  // The request is async and the grid stays interactive, so the selection can
  // move under us while the count is in flight. A confirm on a dialog backed
  // by the old ids would permanently delete photos the user no longer has
  // selected, and a second Delete pressed before the first response arrives
  // could open a dialog and then be overwritten by the earlier reply landing
  // late. Stamp each request with a monotonic seq and snapshot the ids it
  // asked about — a later request retires the earlier one, and a selection
  // that no longer matches the snapshot means the user has moved on and gets
  // a fresh ask rather than a stale dialog. Codex P1 on PR #1672.
  var seq = ++_batchDeleteRequestSeq;
  var capturedIds = ids.slice();
  var companionCount;
  try {
    var counted = await safeFetch('/api/photos/companion-count', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: capturedIds}),
    }, { toast: false });
    companionCount = counted.count || 0;
  } catch (e) {
    if (seq !== _batchDeleteRequestSeq) return;
    showToast(
      'Could not check these photos for companion files, so nothing was '
        + 'deleted. Try again: ' + ((e instanceof Error && e.message) || e),
      'error'
    );
    return;
  }
  if (seq !== _batchDeleteRequestSeq) return;
  // The lightbox's Delete button calls showDeleteDialog() through a
  // different code path (_navbar.html's lightboxDelete), so it does not
  // advance _batchDeleteRequestSeq. If the user pressed E while our count
  // was in flight and clicked the lightbox Delete, the modal is already
  // open backed by a single photo id; calling showDeleteDialog() again
  // here would overwrite that dialog's ids and callback with the batch's,
  // and confirming what looked like a one-photo delete would delete the
  // whole batch. Refuse to open on top of any open delete dialog rather
  // than trying to coordinate seq across every showDeleteDialog caller.
  // Codex P1 on PR #1672.
  var modal = /** @type {HTMLElement} */ (document.getElementById('deleteModal'));
  if (modal && modal.classList.contains('open')) {
    showToast(
      'Another delete dialog is already open. Close it and click Delete again.',
      'info'
    );
    return;
  }
  // Compared as sorted keys, not by scanning one list for each id of the
  // other: a Select all can hand this tens of thousands of ids, and the
  // quadratic version of this check would stall the UI thread on exactly the
  // selections that most need a delete confirmation.
  if (selectionIdsKey(getActiveSelection()) !== selectionIdsKey(capturedIds)) {
    showToast(
      'Selection changed while checking for companion files. Click Delete again.',
      'info'
    );
    return;
  }
  showDeleteDialog(capturedIds, companionCount, function(data) {
    // The server may have retained rows whose Trash step failed (and the
    // user declined the permanent-delete fallback). Keep those rows visible
    // — dropping them from ``photos`` would hide files still on disk until
    // the next reload.
    var retained = new Set((data && data.failed_photo_ids) || []);
    var deletedSet = new Set(ids.filter(function(id) { return !retained.has(id); }));
    // Removing one member can dissolve a stack or change its representative,
    // so the logical item count cannot be updated arithmetically. Reload the
    // current scope from the authoritative projection.
    if (browseStacksEnabled()) {
      selectedPhotos.clear();
      selectedPhotoId = null;
      selectedIndex = -1;
      updateBatchBar();
      resetAndLoad({preserveCollection: true});
      loadSummary();
      refreshBrowseSidebarCounts();
      return;
    }
    photos = photos.filter(function(p) { return !deletedSet.has(p.id); });
    totalPhotos = Math.max(0, totalPhotos - (data.deleted || 0));
    totalUnderlyingPhotos = Math.max(0, totalUnderlyingPhotos - (data.deleted || 0));
    selectedPhotos = new Set(Array.from(selectedPhotos).filter(function(id) {
      return !deletedSet.has(id);
    }));
    if (selectedPhotoId != null && deletedSet.has(selectedPhotoId)) {
      selectedPhotoId = null;
    }
    // Re-sync the batch bar (count, compare/best/burst buttons, selection
    // inspector) against the shrunk selection. Just hiding the bar when the
    // set empties leaves stale state — the count and buttons stayed sized for
    // the pre-delete IDs on a partial deletion.
    updateBatchBar();
    renderGrid();
    updateFilterSummary();
    loadSummary();
    refreshBrowseSidebarCounts();
    // Update total count display
    var countEl = document.getElementById('photoCount');
    if (countEl) {
      var current = parseInt(countEl.textContent) || 0;
      countEl.textContent = String(Math.max(0, current - (data.deleted || 0)));
    }
  });
}

/** @type {BrowseCollection[]} */ var _batchCollections = [];
/** @type {number | null} */ var _batchCollectionSelectedId = null;
// Codex P2: generation counter for addToCollection(). Each invocation
// captures the next value before awaiting /api/collections and bails
// after the await when a newer invocation has superseded it, so a stale
// response can't dismiss overlays the user has since opened or reopen
// the dialog on top of them.
var _addToCollectionInvocation = 0;

function collectionAcceptsManualPhotos(/** @type {BrowseCollection} */ collection) {
  if (typeof collection.can_add_photos === 'boolean') {
    return collection.can_add_photos;
  }

  function rulesAcceptManualPhotos(/** @type {BrowseRule | BrowseRule[] | null | undefined} */ node) {
    if (Array.isArray(node)) {
      return node.every(rulesAcceptManualPhotos);
    }
    if (!node || typeof node !== 'object') return false;
    if (node.field === 'photo_ids') {
      return Array.isArray(node.value || []);
    }
    if (!node.field && Array.isArray(node.rules)) {
      return (node.mode || 'all') === 'all' && node.rules.every(rulesAcceptManualPhotos);
    }
    return false;
  }

  try {
    var rules = typeof collection.rules === 'string'
      ? JSON.parse(collection.rules)
      : collection.rules;
    return rulesAcceptManualPhotos(rules);
  } catch(e) {
    return false;
  }
}

async function addToCollection(/** @type {Element | null | undefined} */ returnFocus = undefined) {
  // WebKit does not focus buttons on pointer clicks. The template passes the
  // opener explicitly; native-menu/shortcut calls retain the current focus.
  returnFocus = returnFocus || document.activeElement;
  var activeIds = getActiveSelection();
  if (activeIds.length === 0) {
    window._vireoNativeMenuPhotoIdsOverride = null;
    return;
  }

  var myInvocation = ++_addToCollectionInvocation;
  /** @type {BrowseCollection[]} */ var collections;
  try {
    collections = await safeFetch('/api/collections', {}, { toast: false });
  } catch(e) {
    window._vireoNativeMenuPhotoIdsOverride = null;
    return;
  }
  // Codex P2: if the user re-triggered Add while this request was pending,
  // the newer invocation will update state and open the dialog on its own
  // response. This stale continuation must not run the overlay dismissals
  // below (which could close a lightbox the user has since opened) or
  // reopen the dialog on top of them.
  if (myInvocation !== _addToCollectionInvocation) return;

  collections = collections.filter(collectionAcceptsManualPhotos);
  _batchCollections = collections;
  _batchCollectionSelectedId = null;
  /** @type {HTMLElement} */ (document.getElementById('batchCollectionTitle')).textContent = 'Add ' + activeIds.length + ' photo(s) to collection';

  var listHtml = '';
  collections.forEach(function(c) {
    listHtml += '<button type="button" class="batch-coll-item" aria-pressed="false" data-id="' + c.id + '" onclick="pickBatchCollection(' + c.id + ')">' + escapeHtml(c.name) + '</button>';
  });
  if (collections.length === 0) {
    listHtml = '<div style="font-size:12px;color:var(--text-dim);padding:4px 10px;">No manual collections yet</div>';
  }
  /** @type {HTMLElement} */ (document.getElementById('batchCollectionList')).innerHTML = listHtml;
  /** @type {HTMLInputElement} */ (document.getElementById('batchCollectionNewName')).value = '';
  // Codex P2: while the /api/collections request was in flight, the help
  // overlay (F1 / ? icon), the Report Issue overlay (⚠ nav icon), the
  // command palette (Ctrl/Cmd+K), and the shared lightbox (grid dblclick)
  // all saw no managed dialog on the stack via hasActive() and could have
  // opened — F1 via openHelpModal's own guard, Ctrl/Cmd+K via
  // openCommandPalette's, and openReportModal/openLightbox have no
  // managed-dialog guard at all. Opening the dialog now would mark every
  // sibling along the dialog's branch inert via updateBackground() but
  // leave each visible at its higher z-index (help: 10000, report: 10000,
  // palette: above the dialog's 500, lightbox overlay: 9999) covering the
  // dialog; Escape would then dismiss the dialog instead of the covering
  // overlay, and closing the dialog would restore focus behind a
  // still-open sibling. Dismiss each before the dialog takes the top of
  // the stack so no opener-guarded sibling races the open path. The
  // lightbox is only closed when actually open (window._lbEscToken is the
  // same signal closeLightbox() uses internally for wasOpen), since
  // closeLightbox() resets viewport/decode state unconditionally.
  if (typeof window.closeHelpModal === 'function') {
    window.closeHelpModal();
  }
  if (typeof window.closeReportModal === 'function') {
    window.closeReportModal();
  }
  if (typeof window.closeCommandPalette === 'function') {
    window.closeCommandPalette();
  }
  if (window._lbEscToken && typeof window.closeLightbox === 'function') {
    window.closeLightbox();
  }
  batchCollectionDialog.open(returnFocus);
}

function pickBatchCollection(/** @type {number} */ id) {
  _batchCollectionSelectedId = id;
  /** @type {HTMLInputElement} */ (document.getElementById('batchCollectionNewName')).value = '';
  /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('.batch-coll-item')).forEach(function(el) {
    el.setAttribute('aria-pressed', String(parseInt(el.dataset.id || '') === id));
  });
}

function hideBatchCollectionModal() {
  batchCollectionDialog.close();
  window._vireoNativeMenuPhotoIdsOverride = null;
}

async function confirmBatchCollection() {
  var newName = /** @type {HTMLInputElement} */ (document.getElementById('batchCollectionNewName')).value.trim();
  var ids = getActiveSelection();
  var collectionId = _batchCollectionSelectedId;
  var collectionName;

  if (newName) {
    // Create a new static collection
    collectionName = newName;
    try {
      var createData = await safeFetch('/api/collections', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          name: collectionName,
          rules: [{"field": "photo_ids", "value": []}],
        }),
      });
      collectionId = createData.id;
    } catch(e) { return; }
  } else if (collectionId != null) {
    var match = _batchCollections.find(function(c) { return c.id === collectionId; });
    collectionName = match ? match.name : '';
  } else {
    return; // nothing selected
  }

  hideBatchCollectionModal();

  // Add photos to the collection
  try {
    await safeFetch('/api/collections/' + collectionId + '/add-photos', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids}),
    });
  } catch(e) { return; }

  showToast('Added ' + ids.length + ' photos to "' + collectionName + '"', 'success');
  loadCollections();
  clearSelection();
  // hideBatchCollectionModal() restored focus to the + Collection button, but
  // clearSelection() then hides the batch bar with display:none, so the opener
  // is now an invisible focus target. Move focus to the grid — the stable
  // page control the user came from — so the next Tab starts somewhere real.
  // Codex P2: the add-photos request between the dialog close and here is
  // async, so the user may have moved focus to another stable control while
  // it was pending. Only move focus when it is actually on something the
  // batch-bar hide just invalidated — the hidden batch bar or document.body
  // (where browsers park focus when its target is removed/hidden). Any
  // other visible control is the user's active choice and we leave it.
  var active = document.activeElement;
  var batchBar = /** @type {HTMLElement} */ (document.getElementById('batchBar'));
  var focusLost = !active || active === document.body ||
      (batchBar && batchBar.contains(active));
  if (focusLost) {
    var grid = /** @type {HTMLElement} */ (document.getElementById('grid'));
    if (grid) grid.focus({preventScroll: true});
  }
}

function reviewLocationsForCollection(/** @type {number} */ collectionId, /** @type {string} */ mode = '') {
  // Same guard as filterByCollection: degraded collections would just
  // re-hit the unresolvable rules while building the review queue.
  var collectionMeta = collectionsById[collectionId];
  if (collectionMeta && collectionMeta.count_error) {
    if (typeof showToast === 'function') {
      showToast(
        (collectionMeta.name || 'This collection') +
          " is unavailable — its rules could not be resolved. Right-click → Edit Rules to fix it.",
        'error'
      );
    }
    return;
  }
  sessionStorage.setItem('vireoLocationReviewReturn', window.location.href);
  sessionStorage.setItem('vireoLocationReviewSource', JSON.stringify({collection_id: collectionId}));
  window.location.href = '/locations/review?collection_id=' + encodeURIComponent(collectionId) + (mode === 'time' ? '&mode=time' : '');
}

function reviewLocationsForSelection(/** @type {string} */ mode = '') {
  var ids = getActiveSelection();
  if (!ids.length) return;
  var payload;
  try {
    payload = JSON.stringify({photo_ids: ids});
  } catch (e) {
    if (typeof showToast === 'function') {
      showToast('Could not open location review for this selection.', 'error');
    }
    return;
  }
  try {
    sessionStorage.setItem('vireoLocationReviewSource', payload);
  } catch (e) {
    // sessionStorage has a per-origin quota (typically ~5MB). Very large
    // selections (e.g. "Select all matching photos" on a big library) can
    // exceed it, in which case setItem throws synchronously. Guide the
    // user to the collection-based flow, which passes an id in the URL
    // and never touches sessionStorage for the id list.
    if (typeof showToast === 'function') {
      showToast(
        'Selection is too large to open location review directly (' +
          ids.length.toLocaleString() +
          ' photos). Save the selection as a collection first, then use Review on Map from the collection menu.',
        'error'
      );
    }
    return;
  }
  sessionStorage.setItem('vireoLocationReviewReturn', window.location.href);
  window.location.href = '/locations/review?source=selection' + (mode === 'time' ? '&mode=time' : '');
}

var captureTimePreviewSeq = 0;

function getCaptureTimeMode() {
  var selected = /** @type {HTMLInputElement | null} */ (document.querySelector('input[name="captureTimeMode"]:checked'));
  return selected ? selected.value : 'preserve_instant';
}

function buildCaptureTimePayload() {
  var ids = getActiveSelection();
  var mode = getCaptureTimeMode();
  var targetOffset = /** @type {HTMLInputElement} */ (document.getElementById('captureTimeTargetOffset')).value.trim();
  var shiftRaw = /** @type {HTMLInputElement} */ (document.getElementById('captureTimeManualShift')).value;
  var shiftMinutes = parseInt(shiftRaw, 10);
  if (isNaN(shiftMinutes)) shiftMinutes = 0;
  return {
    photo_ids: ids,
    mode: mode,
    target_offset: mode === 'manual' ? null : (targetOffset || null),
    shift_minutes: shiftMinutes,
    keep_backups: /** @type {HTMLInputElement} */ (document.getElementById('captureTimeKeepBackups')).checked,
  };
}

function syncCaptureTimeFieldState() {
  var mode = getCaptureTimeMode();
  var targetInput = /** @type {HTMLInputElement} */ (document.getElementById('captureTimeTargetOffset'));
  var shiftInput = /** @type {HTMLInputElement} */ (document.getElementById('captureTimeManualShift'));
  if (!targetInput || !shiftInput) return;
  if (mode === 'manual') {
    targetInput.disabled = true;
    targetInput.title = 'Not used in manual shift mode';
    shiftInput.disabled = false;
    shiftInput.title = '';
  } else {
    targetInput.disabled = false;
    targetInput.title = '';
    shiftInput.disabled = true;
    shiftInput.title = 'Not used in preserve-instant mode';
  }
}

function formatShiftMinutes(/** @type {number} */ mins) {
  var sign = mins >= 0 ? '+' : '';
  var hours = mins / 60;
  var hoursPart = Number.isInteger(hours) ? ' (' + (hours >= 0 ? '+' : '') + hours + ' hours)' : '';
  return sign + mins + ' minutes' + hoursPart;
}

function renderCaptureTimePreview(/** @type {BrowseCaptureTimePreview} */ data) {
  var rows = data.samples || [];
  var shiftsVary = !!data.shifts_vary;
  var html = '<div class="capture-time-row header">' +
    '<div>File</div><div>Current</div><div>After</div></div>';
  rows.forEach(function(row) {
    var before = (row.before_time || 'No capture time') + (row.before_offset ? ' ' + row.before_offset : '');
    var after = (row.after_time || 'No capture time') + (row.after_offset ? ' ' + row.after_offset : '');
    var perRowShift = '';
    if (shiftsVary && typeof row.shift_minutes === 'number') {
      perRowShift = ' <span style="color:var(--text-muted);font-size:11px;">(' + formatShiftMinutes(row.shift_minutes) + ')</span>';
    }
    html += '<div class="capture-time-row">' +
      '<div class="capture-time-filename" title="' + escapeAttr(row.filename || '') + '">' + escapeHtml(row.filename || '') + '</div>' +
      '<div class="capture-time-before">' + escapeHtml(before) + '</div>' +
      '<div class="capture-time-after">' + escapeHtml(after) + perRowShift + '</div>' +
    '</div>';
  });
  var summary;
  if (shiftsVary) {
    summary = 'Shift varies per photo (each photo is shifted to land on the target offset).';
  } else if (typeof data.shift_minutes === 'number') {
    summary = 'Resolved shift: ' + formatShiftMinutes(data.shift_minutes);
  } else {
    summary = 'No shift will be applied.';
  }
  html += '<div class="capture-time-hint" style="padding:0 10px 10px;margin:0;">' + escapeHtml(summary) + '</div>';
  /** @type {HTMLElement} */ (document.getElementById('captureTimePreview')).innerHTML = html;
}

async function updateCaptureTimePreview() {
  var modal = /** @type {HTMLElement} */ (document.getElementById('captureTimeModal'));
  if (!modal.classList.contains('open')) return;
  var payload = buildCaptureTimePayload();
  var previewEl = /** @type {HTMLElement} */ (document.getElementById('captureTimePreview'));
  var applyBtn = /** @type {HTMLButtonElement} */ (document.getElementById('captureTimeApplyBtn'));
  if (!payload.photo_ids.length) {
    previewEl.innerHTML = '<div class="capture-time-hint" style="padding:10px;margin:0;">No photos selected.</div>';
    applyBtn.disabled = true;
    return;
  }
  var seq = ++captureTimePreviewSeq;
  applyBtn.disabled = true;
  previewEl.innerHTML = '<div class="capture-time-hint" style="padding:10px;margin:0;">Loading preview...</div>';
  try {
    var data = await safeFetch('/api/capture-time/preview', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    }, { toast: false });
    if (seq !== captureTimePreviewSeq) return;
    renderCaptureTimePreview(data);
    applyBtn.disabled = false;
    applyBtn.textContent = 'Apply to ' + payload.photo_ids.length + ' photo' + (payload.photo_ids.length === 1 ? '' : 's');
  } catch(e) {
    if (seq !== captureTimePreviewSeq) return;
    previewEl.innerHTML = '<div class="capture-time-hint" style="padding:10px;margin:0;color:var(--danger);">' + escapeHtml((e instanceof Error && e.message) || 'Could not build preview') + '</div>';
    applyBtn.disabled = true;
  }
}

function openCaptureTimeModal() {
  var ids = getActiveSelection();
  if (!ids.length) return;
  /** @type {HTMLElement} */ (document.getElementById('captureTimeTitle')).textContent = 'Adjust Capture Time for ' + ids.length + ' photo' + (ids.length === 1 ? '' : 's');
  /** @type {HTMLButtonElement} */ (document.getElementById('captureTimeApplyBtn')).textContent = 'Apply';
  /** @type {HTMLButtonElement} */ (document.getElementById('captureTimeApplyBtn')).disabled = true;
  /** @type {HTMLElement} */ (document.getElementById('captureTimeModal')).classList.add('open');
  syncCaptureTimeFieldState();
  updateCaptureTimePreview();
}

function hideCaptureTimeModal() {
  /** @type {HTMLElement} */ (document.getElementById('captureTimeModal')).classList.remove('open');
  window._vireoNativeMenuPhotoIdsOverride = null;
}

async function startCaptureTimeJob() {
  var payload = buildCaptureTimePayload();
  if (!payload.photo_ids.length) return;
  var applyBtn = /** @type {HTMLButtonElement} */ (document.getElementById('captureTimeApplyBtn'));
  applyBtn.disabled = true;
  try {
    var data = await safeFetch('/api/jobs/capture-time', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    hideCaptureTimeModal();
    showToast(
      'Adjusting capture time for ' + payload.photo_ids.length + ' photo' +
        (payload.photo_ids.length === 1 ? '' : 's') + '...',
      'info'
    );
    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(/** @type {{current?: number, total?: number, current_file?: string}} */ prog) {
        showToast(
          'Adjusting capture time: ' + prog.current + '/' + prog.total +
            ' — ' + (prog.current_file || ''),
          'info'
        );
      },
      onComplete: function(/** @type {BrowseCaptureTimeCompletion} */ done) {
        if (done.status && done.status !== 'completed') {
          var errors = done.errors || [];
          showToast('Capture time failed: ' + (errors[0] || done.status), 'error');
          return;
        }
        var result = done.result || {};
        var skippedText = result.skipped ? ', ' + result.skipped + ' skipped' : '';
        showToast('Capture time updated: ' + (result.updated || 0) + ' updated' + skippedText + ', ' + (result.failed || 0) + ' failed', result.failed ? 'error' : 'success');
        resetAndLoad();
        loadSummary();
      },
      onError: function() {
        applyBtn.disabled = false;
      },
    });
  } catch(e) {
    applyBtn.disabled = false;
  }
}

function developSelected() {
  var ids = getActiveSelection();
  if (!ids.length) return;
  developPhotos(ids);
}

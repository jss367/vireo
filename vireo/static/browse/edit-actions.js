/* Browse: detail-panel edits (rating, flag, color, wildlife, keywords) and the batch inspector.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

function refreshPendingSyncBanner() {
  if (typeof checkPendingSync === 'function') {
    checkPendingSync();
  }
}

/* ---------- Edit Actions ---------- */
async function setRating(/** @type {number} */ photoId, /** @type {number} */ rating) {
  try {
    await safeFetch('/api/photos/' + photoId + '/rating', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rating: rating }),
    });
    // Update local state — findBrowsePhoto looks in the top-level `photos`
    // array AND in any expanded stack tray's `browseStackMembers`, so a rating
    // set on a hidden member updates the tray-cached row instead of vanishing.
    var p = findBrowsePhoto(photoId);
    if (p) p.rating = rating;
    await reconcileBrowseStackCovers([photoId]);
    refreshGridCards([photoId]);
    refreshExpandedBrowseStackMembers([photoId]);
    scheduleCollectionCountsRefresh();
    // The user may have moved on during the POST. loadDetail(A) while B is
    // open would hide B's panel and null _detailPhotoId, then refuse to render
    // A, leaving the panel blank.
    if (selectedPhotoId === photoId) loadDetail(photoId);
    refreshPendingSyncBanner();
  } catch(e) {}
}

async function setFlag(/** @type {NonNullable<BrowsePhoto['flag']>} */ flag) {
  var sel = getActiveSelection();
  if (sel.length > 1) {
    var st = _batchAllLoaded(sel) ? _batchFlagState(sel) : { unanimous: false, value: null };
    var target = (st.unanimous && st.value === flag) ? 'none' : flag;
    // batchSetFlag runs _refreshBatchInspectorIfActive() against the *current*
    // selection when the request resolves, so the sidebar reflects whatever is
    // selected now. Re-rendering with the captured `sel` here would clobber a
    // fresh render if the user changed the selection during the await.
    await batchSetFlag(target);
    return;
  }
  if (!selectedPhotoId) return;
  var photoId = selectedPhotoId;
  try {
    await safeFetch('/api/photos/' + photoId + '/flag', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ flag: flag }),
    });
    var p = findBrowsePhoto(photoId);
    if (p) p.flag = flag;
    _clearRepresentativeStateIfIneligible(photoId, flag);
    await reconcileBrowseStackCovers([photoId]);
    refreshGridCards([photoId]);
    refreshExpandedBrowseStackMembers([photoId]);
    if (selectedPhotoId === photoId) {
      updateDetailFlagButtons(flag);
      // A hand flag has no source or reason; the note described the old one.
      renderDetailFlagNote(flag, null);
    }
    scheduleCollectionCountsRefresh();
  } catch(e) {}
}

function updateDetailWildlifeExcluded(/** @type {BrowsePhoto | null | undefined} */ photo = undefined) {
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('detailWildlifeExcluded'));
  if (!btn) return;
  var excluded = !!(photo && photo.wildlife_excluded);
  btn.classList.toggle('active', excluded);
  btn.textContent = excluded ? 'Include Wildlife Classification' : 'Not Wildlife';
  btn.title = excluded
    ? 'Include this photo in wildlife detection and classification'
    : 'Exclude this photo from wildlife detection and classification';
}

async function setWildlifeExcludedFor(/** @type {number} */ photoId, /** @type {boolean} */ excluded) {
  await safeFetch('/api/photos/' + photoId + '/wildlife_excluded', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ excluded: excluded }),
  }, { toast: false });
  var p = findBrowsePhoto(photoId);
  if (p) p.wildlife_excluded = excluded ? 1 : 0;
  renderGrid();
  refreshExpandedBrowseStackMembers([photoId]);
  if (selectedPhotoId === photoId) loadDetail(photoId);
  showUndoToast();
}
window.setWildlifeExcludedFor = setWildlifeExcludedFor;

async function toggleDetailWildlifeExcluded() {
  if (!selectedPhotoId) return;
  // Read the current value via findBrowsePhoto so a stack member that lives
  // only in browseStackMembers (not the top-level photos array) still yields
  // its true wildlife_excluded state. photos.find() would return undefined for
  // hidden members and always send excluded=true — clicking "Include Wildlife
  // Classification" on an already-excluded member would silently re-exclude
  // it instead of including it.
  var p = findBrowsePhoto(selectedPhotoId);
  var current = p ? !!p.wildlife_excluded : false;
  try {
    await setWildlifeExcludedFor(selectedPhotoId, !current);
  } catch(e) {}
}

async function fetchColorLabels(/** @type {number[]} */ photoIds) {
  if (!photoIds.length) return;
  var gen = colorLabelGen;
  // Chunk the GET URL so oversized stacks (or any large id set) don't
  // exceed the browser/proxy/server request-target limit and drop the
  // whole page's color labels when a stack expansion hydrates a large
  // member list (Codex P2 on PR #1561). Matches the 500-id cap the
  // /api/photos/by-ids stack-expansion path already uses.
  /** @type {Record<string, string>} */ var combined = {};
  var chunkFailed = false;
  for (var offset = 0; offset < photoIds.length; offset += 500) {
    var chunk = photoIds.slice(offset, offset + 500);
    var data = await safeFetch(
      '/api/photos/color_labels?ids=' + chunk.join(','),
      {}, { toast: false },
    );
    if (!data) { chunkFailed = true; break; }
    for (var id in data) combined[id] = data[id];
  }
  if (chunkFailed) return;
  // Skip per id whose local edit happened after this fetch started: the
  // delete would drop the fresh value, and the merge would overwrite it with
  // the older server truth. Ids the user never touched are still applied.
  photoIds.forEach(function(id) {
    if ((colorLabelEditGen[id] || 0) > gen) return;
    delete colorLabels[id];
  });
  for (var pid in combined) {
    if ((colorLabelEditGen[pid] || 0) > gen) continue;
    var color = combined[pid]; if (color != null) colorLabels[pid] = color;
  }
  // Mark every requested id as fetched — an absent id in `combined` means
  // "no color set" now that we've asked, so the batch inspector can trust
  // colorLabels[id] === undefined for these ids as a definite value.
  photoIds.forEach(function(id) { colorLabelsFetched.add(id); });
  // A batch selection that included any of these ids may have rendered as
  // unanimous no-colour with no Mixed marker; refresh it now that we can
  // trust the state.
  _refreshBatchInspectorIfActive();
}

async function setColorLabel(/** @type {string | null} */ color) {
  var sel = getActiveSelection();
  if (sel.length > 1) {
    var st = _batchAllLoaded(sel) ? _batchColorState(sel) : { unanimous: false, value: null };
    var target = (st.unanimous && st.value === color) ? null : color;
    // See setFlag: batchSetColorLabel already refreshes the inspector for the
    // current selection, so re-rendering with the captured `sel` risks stomping
    // a fresh render if the user changed the selection during the await.
    await batchSetColorLabel(target);
    return;
  }
  if (!selectedPhotoId) return;
  var photoId = selectedPhotoId;
  _noteColorLabelEdits([photoId]);
  var current = colorLabels[photoId] || null;
  var newColor = (current === color) ? null : color;
  try {
    await safeFetch('/api/photos/' + photoId + '/color_label', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({color: newColor}),
    });
  } catch(e) { _recoverColorLabelsAfterFailedWrite([photoId]); return; }
  _noteColorLabelEdits([photoId]);
  if (newColor) {
    colorLabels[photoId] = newColor;
  } else {
    delete colorLabels[photoId];
  }
  colorLabelsFetched.add(photoId);
  if (selectedPhotoId === photoId) updateDetailColors();
  refreshGridCards([photoId]);
  refreshExpandedBrowseStackMembers([photoId]);
  scheduleCollectionCountsRefresh();
}

function updateDetailColorsValue(/** @type {string | null} */ current) {
  /** @type {NodeListOf<HTMLButtonElement>} */ (document.querySelectorAll('.detail-color-btn')).forEach(function(btn) {
    btn.classList.toggle('active', btn.dataset.color === current);
  });
}

function updateDetailColors() {
  updateDetailColorsValue((selectedPhotoId == null ? null : colorLabels[selectedPhotoId]) || null);
}

// --- Multi-select (batch) inspector ---------------------------------------
// When >1 photos are selected, the detail panel is reused as a batch editor
// (see updateSelectionPanel). Rating/Flag/Color reflect the selection's shared
// value when unanimous, or a "Mixed" marker when the selected photos differ,
// and edits apply to the whole selection.

function _batchToggleMixed(/** @type {string} */ id, /** @type {boolean} */ show) {
  var el = document.getElementById(id);
  if (el) el.hidden = !show;
}

// Returns {unanimous, value}. get(id) yields the per-photo value.
/** @template T @param {number[]} ids @param {(id: number) => T} get @returns {{unanimous: boolean, value: T | null}} */
function _batchUnanimous(ids, get) {
  if (!ids.length) return { unanimous: false, value: null };
  var firstId = ids[0]; if (firstId == null) return {unanimous: false, value: null};
  var first = get(firstId);
  for (var i = 1; i < ids.length; i++) {
    var id = ids[i]; if (id == null || get(id) !== first) return { unanimous: false, value: null };
  }
  return { unanimous: true, value: first };
}

function _batchRatingState(/** @type {number[]} */ ids) {
  return _batchUnanimous(ids, function(id) {
    var p = findBrowsePhoto(id);
    return p ? (p.rating || 0) : 0;
  });
}
function _batchFlagState(/** @type {number[]} */ ids) {
  return _batchUnanimous(ids, function(id) {
    var p = findBrowsePhoto(id);
    return p ? (p.flag || 'none') : 'none';
  });
}
function _batchColorState(/** @type {number[]} */ ids) {
  return _batchUnanimous(ids, function(id) { return colorLabels[id] || null; });
}

// Only trust unanimity when every selected photo is loaded in the current grid
// (and thus its rating/flag/color is known). A selection that reaches beyond the
// loaded page — e.g. "select all search results" — is shown as Mixed rather than
// asserting a shared value we can't actually verify.
function _batchAllLoaded(/** @type {number[]} */ ids) {
  return ids.every(function(id) {
    return !!findBrowsePhoto(id);
  });
}

// Colour labels arrive from a separate async /api/photos/color_labels fetch, so
// having each photo row loaded (via _batchAllLoaded) is not enough to trust the
// colour column: a selection made before that fetch returns would otherwise
// render as unanimous no-colour without a Mixed marker.
function _batchColorLoaded(/** @type {number[]} */ ids) {
  return ids.every(function(id) { return colorLabelsFetched.has(id); });
}

function renderBatchInspector(/** @type {number[]} */ ids, /** @type {{preserveExifSuggestion?: boolean, preserveLocation?: boolean}} */ opts = {}) {
  opts = opts || {};
  var known = _batchAllLoaded(ids);

  var coordinateEl = /** @type {HTMLElement} */ (document.getElementById('locationCoordinateStatus'));
  if (coordinateEl) {
    if (!known) {
      coordinateEl.className = 'coordinate-status none';
      coordinateEl.textContent = 'Coordinate sources are mixed or still loading for this selection.';
    } else {
      var counts = {exif: 0, assigned: 0, none: 0};
      ids.forEach(function(id) {
        var p = findBrowsePhoto(id);
        var status = p?.location_status || 'none';
        counts[status]++;
      });
      coordinateEl.className = 'coordinate-status';
      coordinateEl.textContent = [
        counts.exif + ' EXIF GPS',
        counts.assigned + ' assigned map location' + (counts.assigned === 1 ? '' : 's'),
        counts.none + ' without coordinates'
      ].join(' · ');
    }
  }

  var rs = known ? _batchRatingState(ids) : { unanimous: false, value: 0 };
  var r = rs.unanimous ? rs.value || 0 : 0;
  var ratingHtml = '';
  for (var i = 1; i <= 5; i++) {
    ratingHtml += '<span class="' + (i <= r ? 'detail-star active' : 'detail-star') +
      '" onclick="batchRate(' + i + ')">&#9733;</span>';
  }
  /** @type {HTMLElement} */ (document.getElementById('detailRating')).innerHTML = ratingHtml;
  // Show Mixed whenever we can't confirm unanimity — including partially-loaded
  // selections where `known` is false. Hiding Mixed there made "0 stars / None /
  // no color" render identically to a truly unanimous no-value selection.
  _batchToggleMixed('ratingMixed', !(known && rs.unanimous));

  var fs = known ? _batchFlagState(ids) : { unanimous: false, value: 'none' };
  updateDetailFlagButtons(fs.unanimous ? fs.value : 'none');
  _batchToggleMixed('flagMixed', !(known && fs.unanimous));

  var colorKnown = known && _batchColorLoaded(ids);
  var cs = colorKnown ? _batchColorState(ids) : { unanimous: false, value: null };
  updateDetailColorsValue(cs.unanimous ? cs.value : null);
  _batchToggleMixed('colorMixed', !(colorKnown && cs.unanimous));

  // Location input shares the batch-aware submit path (_locationApplyPhotoIds);
  // show the empty input rather than any single photo's saved location.
  // Skip the reset on state-only refreshes (opts.preserveLocation) — the user
  // may be mid-way through typing a batch location when an async event
  // (e.g. a /api/photos/color_labels fetch resolving, a batch rating shortcut)
  // triggers _refreshBatchInspectorIfActive, and we'd otherwise erase it.
  if (!opts.preserveLocation) {
    renderLocationEmpty({
      preserveExifSuggestion: opts.preserveExifSuggestion,
      selectionIds: ids,
    });
  }
}

async function batchRate(/** @type {number} */ rating) {
  var ids = getActiveSelection();
  if (ids.length < 1) return;
  var st = _batchAllLoaded(ids) ? _batchRatingState(ids) : { unanimous: false, value: null };
  var target = (st.unanimous && st.value === rating) ? 0 : rating;
  // batchSetRating already refreshes the inspector for the current selection.
  // A follow-up render using the captured `ids` would clobber that fresh render
  // if the user changed the selection during the await.
  await batchSetRating(target);
}

async function addKeyword(/** @type {BrowseKeywordOption | null} */ keyword = null) {
  var ids = getActiveSelection();
  var useBatch = ids.length > 1;
  var batchSelectionKey = selectionIdsKey(ids);
  if (!selectedPhotoId && !useBatch) return;
  var input = /** @type {HTMLInputElement} */ (document.getElementById('addKeywordInput'));
  var name = input.value.trim();
  if (!name) return;
  var state = getKeywordAutocompleteState('addKeywordInput');
  var selectedKeyword = keyword || (
    state.selectedKeyword && state.selectedKeyword.name === name
      ? state.selectedKeyword
      : null
  );
  /** @type {{keyword_id?: number, name?: string, photo_ids?: number[]}} */ var payload = selectedKeyword && selectedKeyword.id
    ? { keyword_id: selectedKeyword.id }
    : { name: name };
  if (useBatch) payload.photo_ids = ids;
  try {
    var endpoint = useBatch
      ? '/api/batch/keyword'
      : '/api/photos/' + selectedPhotoId + '/keywords';
    await safeFetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    // skipDetail when the single-photo branch below runs a full loadDetail,
    // which re-fetches this photo's predictions anyway.
    await _refreshBrowseKeywordState(ids, {
      skipDetail: !useBatch && !!selectedPhotoId,
    });
    input.value = '';
    state.selectedKeyword = null;
    hideKeywordSuggestions('addKeywordInput');
    if (!selectedKeyword) invalidateKeywordAutocompleteCache();
    // Same reason as _afterLocationMutation: if a multi-selection is still
    // active, re-render the batch inspector instead of collapsing to the
    // anchor photo's single-detail view via loadDetail.
    var currentSel = getActiveSelection();
    if (currentSel.length > 1) {
      // Selection is unchanged after a batch keyword save; keep any in-progress
      // location input the user may have typed.
      renderBatchInspector(currentSel, { preserveLocation: true });
    } else if (selectedPhotoId) {
      loadDetail(selectedPhotoId);
    }
    if (useBatch && selectionIdsKey(getActiveSelection()) === batchSelectionKey) {
      /** @type {BrowseInitializedPanels} */ (Vireo && Vireo.browse).panelRequests.keywords.invalidate();
      loadSelectionKeywordSuggestions(ids);
    }
    loadKeywords();
    scheduleCollectionCountsRefresh();
    refreshActiveCollectionAfterMembershipChange([MUTATION_KEYWORD]);
    refreshPendingSyncBanner();
  } catch(e) {}
}

async function removeKeyword(/** @type {number} */ photoId, /** @type {number} */ keywordId) {
  try {
    await safeFetch('/api/photos/' + photoId + '/keywords/' + keywordId, {
      method: 'DELETE',
    });
    // loadDetail below re-fetches this photo's predictions.
    await _refreshBrowseKeywordState([photoId], { skipDetail: true });
    _clearRepresentativeStateAfterKeywordRemoval(
      [photoId],
      _keywordNameFromCache(keywordId) || null,
    );
    // Same guard as setRating: don't blank the panel of a photo opened since.
    if (selectedPhotoId === photoId) loadDetail(photoId);
    loadKeywords();
    scheduleCollectionCountsRefresh();
    refreshActiveCollectionAfterMembershipChange([MUTATION_KEYWORD]);
    refreshPendingSyncBanner();
  } catch(e) {}
}

/* ---------- Keyword Type Dropdown ---------- */
function toggleTypeDropdown(/** @type {HTMLElement} */ indicator, /** @type {number} */ kwId) {
  // Close all other open dropdowns first
  /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('.keyword-type-dropdown.open')).forEach(function(dd) {
    if (dd.dataset.kwId !== String(kwId)) dd.classList.remove('open');
  });
  var dropdown = /** @type {HTMLElement | null} */ (indicator.querySelector('.keyword-type-dropdown'));
  if (dropdown) dropdown.classList.toggle('open');
}

async function setKeywordType(/** @type {number} */ kwId, /** @type {string} */ newType, /** @type {HTMLElement} */ el) {
  try {
    await safeFetch('/api/keywords/' + kwId, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: newType }),
    });
    // Close dropdown
    var dropdown = /** @type {HTMLElement | null} */ (el.closest('.keyword-type-dropdown'));
    if (dropdown) dropdown.classList.remove('open');
    // Refresh detail panel and keyword tree
    // Retyping a keyword to/from `species` changes which predictions count as
    // disagreements, so the prediction panels move too — _refreshBrowseKeyword
    // State repaints them, except where loadDetail below already will.
    await _refreshBrowseKeywordState(
      loadedBrowsePhotoIds(),
      { skipDetail: !!selectedPhotoId },
    );
    if (selectedPhotoId) loadDetail(selectedPhotoId);
    loadKeywords();
  } catch(e) {}
}

// Close keyword type dropdowns when clicking outside
document.addEventListener('click', function(e) {
  if (!(e.target instanceof Element) || !/** @type {HTMLElement | null} */ (e.target.closest('.keyword-type-indicator'))) {
    /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('.keyword-type-dropdown.open')).forEach(function(dd) {
      dd.classList.remove('open');
    });
  }
});

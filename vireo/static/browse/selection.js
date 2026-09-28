/* Browse: photo selection (click, range, select all, clear).
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Photo Selection & Detail ---------- */
function selectPhoto(e, id, idx, opts) {
  // Snapshot the pre-click selection so the grid dblclick handler can tell
  // "these clicks made the selection" from "these clicks reaffirmed an
  // existing one" — the two produce identical ``selectedPhotos`` when the
  // click lands on the stack that was already selected. Only the first click
  // of a click sequence records: the browser numbers sequential clicks in
  // ``event.detail`` (1 for the first, 2 for the second of a dblclick), so
  // detail > 1 preserves the pre-first-click snapshot through the second.
  // Codex P2 on PR #1672.
  if (!(e && e.detail > 1)) {
    browseSelectionBeforeStackDblClickStart = new Set(selectedPhotos);
  }
  anchorRestoreEpoch++;
  lastClickedPhotoId = id;
  // A collapsed stack card is its whole stack (see browseStackMemberIds), so
  // every branch below acts on this list rather than on `id` alone.
  var clickIds = browseSelectionIdsForClick(id, opts);
  if (e.shiftKey && selectedIndex >= 0) {
    // Shift-click: range select
    var memberRange = browseStackMemberRange(selectedPhotoId, id);
    if (memberRange) {
      memberRange.forEach(function(memberId) { selectedPhotos.add(memberId); });
    } else {
      var start = Math.min(selectedIndex, idx);
      var end = Math.max(selectedIndex, idx);
      for (var i = start; i <= end; i++) {
        if (photos[i] && (!photos[i].folder_status ||
            photos[i].folder_status === 'ok' || photos[i].folder_status === 'partial')) {
          // Each card in the range contributes what clicking it would: a
          // stack card contributes its frames, not just its cover.
          browseSelectionIdsForClick(photos[i].id).forEach(function(rangeId) {
            selectedPhotos.add(rangeId);
          });
        }
      }
    }
    // The anchor of a range is always part of that range. Usually it is
    // photos[selectedIndex] and the loop already covered it, but a focused
    // expanded-stack member is not in the top-level photos array at all:
    // selectBrowseStackMember() anchors selectedIndex on the member's *cover*
    // grid slot. Without this the loop adds the cover and the intervening
    // cards while the member keeps painting "selected" off selectedPhotoId,
    // yet getActiveSelection() prefers the now-nonempty set and drops it —
    // Export/Delete would silently act on the cover the user never focused.
    // Fold the anchor in so the highlighted set and the acted-on set cannot
    // disagree. Codex P2 on PR #1561.
    if (selectedPhotoId != null) selectedPhotos.add(selectedPhotoId);
    // Same reasoning for the shift-click *target*: usually id === photos[idx].id
    // and the loop already added it, but selectBrowseStackMember() passes the
    // hidden member's id with the cover's grid slot as idx — so the loop can
    // only reach the cover. Fold the target in so a Shift-click on a hidden
    // stack member always ends up in the resulting range. Codex P2 on
    // PR #1561.
    clickIds.forEach(function(clickId) { selectedPhotos.add(clickId); });
  } else if (e.metaKey || e.ctrlKey) {
    // Cmd/Ctrl-click: toggle in selection. Fold the focused photo into the
    // set so batch operations include it (otherwise the highlighted
    // selectedPhotoId is silently excluded from keyboard shortcuts).
    if (selectedPhotos.size === 0 && selectedPhotoId !== null) {
      selectedPhotos.add(selectedPhotoId);
    }
    // A stack toggles as a unit, and it only leaves the selection when every
    // one of its frames is in it: Cmd-clicking a stack whose tray contributed
    // three frames adds the rest rather than removing those three.
    var alreadySelected = clickIds.every(function(clickId) {
      return selectedPhotos.has(clickId);
    });
    clickIds.forEach(function(clickId) {
      if (alreadySelected) selectedPhotos.delete(clickId);
      else selectedPhotos.add(clickId);
    });
    // If the toggle dropped the focused photo out of a non-empty set, its
    // highlight/detail focus is now stale: the card still paints "selected"
    // via the selectedPhotoId branch of the highlight rule, yet
    // getActiveSelection() returns the set — so destructive actions
    // (delete/export/develop) would target the set while the user is staring
    // at a different, visibly-focused card. Reconcile by dropping the focus.
    //
    // The same applies when this toggle empties the set entirely and the
    // focused photo is one of the ids it just removed: collapsing an expanded
    // stack pins the focus to its cover, so Cmd-clicking that stack off left
    // getActiveSelection() falling back to the cover — a stack the user
    // deselected as a unit coming back as a one-photo partial selection.
    // Codex P2 on PR #1672.
    if (selectedPhotoId !== null && !selectedPhotos.has(selectedPhotoId)
        && (selectedPhotos.size > 0 || clickIds.indexOf(selectedPhotoId) !== -1)) {
      selectedPhotoId = null;
      selectedIndex = -1;
      var detail = document.getElementById('detailContent');
      if (detail && detail.classList.contains('visible')) {
        detail.classList.remove('visible');
        var summary = document.getElementById('summaryPanel');
        if (summary) summary.classList.remove('hidden');
        loadSummary();
      }
      // Same reason as closeDetail: the dropped anchor's EXIF suggestion is
      // still tagged with its data-photo-id (hidden along with the detail
      // panel). This path bypasses closeDetail entirely, so without an
      // explicit scrub a later Select All (or any batch that still contains
      // the dropped photo) would satisfy renderLocationEmpty's owner-in-
      // selection check and resurrect the anchor's Accept line for the
      // whole batch — clicking it would apply the dropped anchor's GPS
      // place to every selected photo. Codex P2 on PR #1097.
      clearExifSuggestion();
      // And null the ambient detail-photo pointer maybeShowExifSuggestion's
      // post-await guard reads. If A's reverse-geocode is still in flight
      // when we drop A here, the completion runs later; leaving
      // _detailPhotoId pointing at A means a subsequent Select All that
      // contains A would let the async paint path resurrect A's Accept line
      // into the batch inspector. Codex P2 on PR #1097 (17:04Z follow-up).
      window._detailPhotoId = null;
    }
  } else if (clickIds.length > 1) {
    // Normal click on a stack card: select the stack, the same state the
    // tray's "Select all" produces. There is no single photo to focus — the
    // card stands for all of them — so the panel opens as the batch
    // inspector rather than showing one frame's detail as if it were the
    // thing being acted on.
    selectedPhotos = new Set(clickIds);
    selectedPhotoId = null;
    selectedIndex = idx;
    abandonDetailFocusForBatch();
  } else {
    // Normal click: single select
    selectedPhotos.clear();
    selectedPhotoId = id;
    selectedIndex = idx;
    loadDetail(id);
  }

  // Update highlights
  refreshCardSelectionVisuals();

  noteFocusedCardVisibility();
  updateBatchBar();
}

// The batch bar drives Develop/Export/Delete/etc. "Active selection" is
// whichever of {selectedPhotos, selectedPhotoId} has entries — the Set for
// cmd/shift-clicks, the single id for a normal click focus. Single-focused
// photos count as selected for the purposes of batch actions.
function getActiveSelection() {
  if (window._vireoNativeMenuPhotoIdsOverride && window._vireoNativeMenuPhotoIdsOverride.length) {
    return window._vireoNativeMenuPhotoIdsOverride.slice();
  }
  if (selectedPhotos.size > 0) return Array.from(selectedPhotos);
  if (selectedPhotoId != null) return [selectedPhotoId];
  return [];
}

// Use the same membership query for Select all and selection reconciliation.
function buildBrowseIdsRequest() {
  var url;
  var fetchOpts = {};
  if (activeCollectionId && !dashboardCollectionScope) {
    // Mirror the grid's sort AND its Stacks projection so the selection's
    // first photo — which drives Best Batch seed, burst-review order, and
    // export preview — matches the first visible card even when the
    // collection is sorted by name/rating/sharpness/quality or when a
    // stack's quality-ranked cover isn't the earliest member under the
    // sort (Codex P2 on PR #1561).
    var idsParams = new URLSearchParams();
    idsParams.set('sort', document.getElementById('sortSelect').value);
    if (browseStacksEnabled()) idsParams.set('stacks', 'true');
    url = '/api/collections/' + activeCollectionId + '/photo-ids?' + idsParams.toString();
  } else {
    url = '/api/photos/query';
    var idsBody = {
      rules: getBrowseRules() || [],
      sort: document.getElementById('sortSelect').value,
      ids_only: true,
    };
    // Mirror the grid's Stacks projection on the general query path as
    // well — workspace, folder, dashboard-collection, unsaved-filter,
    // and visual-search Select-all now share the same cover-first
    // projection so Best Batch, burst-review, and export preview start
    // from the visible first card even when a stack's quality-ranked
    // cover isn't the earliest member under the selected sort (Codex
    // P2 on PR #1561).
    if (browseStacksEnabled()) idsBody.stacks = true;
    var idsVisual = window.VireoFilter && VireoFilter.getVisual ? VireoFilter.getVisual() : null;
    if (idsVisual) idsBody.visual = idsVisual;
    if (activeFolderId) idsBody.folder_id = activeFolderId;
    if (activeCollectionId && dashboardCollectionScope) idsBody.collection_id = activeCollectionId;
    fetchOpts = {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(idsBody),
    };
  }
  return {url: url, options: fetchOpts};
}

async function selectAllMatchingPhotos() {
  anchorRestoreEpoch++;
  selectedPhotoId = null;
  selectedIndex = -1;
  var requestSeq = ++selectAllRequestSeq;
  var selectionEpoch = anchorRestoreEpoch;
  var windowIsCurrent = observeBrowseWindow();
  var request = buildBrowseIdsRequest();

  showToast('Selecting all matching photos...', 'info');
  try {
    var data = await safeFetch(request.url, request.options, { toast: false });
    if (requestSeq !== selectAllRequestSeq || selectionEpoch !== anchorRestoreEpoch || !windowIsCurrent()) return;
    selectedPhotos.clear();
    (data.photo_ids || data.ids || []).forEach(function(id) { selectedPhotos.add(id); });
    renderGrid();
    updateBatchBar();
    showToast(
      'Selected ' + selectedPhotos.size.toLocaleString() + ' photo' +
        (selectedPhotos.size === 1 ? '' : 's'),
      'success'
    );
  } catch(e) {
    showToast('Could not select all photos: ' + (e.message || e), 'error');
  }
}

function selectionIdsKey(ids) {
  return ids.slice().sort(function(a, b) { return a - b; }).join(',');
}

function updateCompareButton(ids) {
  var btn = document.getElementById('compareBtn');
  if (!btn) return;
  var canCompare = ids.length >= 2;
  btn.style.display = canCompare ? '' : 'none';
  btn.disabled = !canCompare;
}

function updateBestBatchButton(ids) {
  var btn = document.getElementById('bestBatchBtn');
  if (!btn) return;
  var canFind = ids.length >= 1;
  btn.style.display = canFind ? '' : 'none';
  btn.disabled = !canFind;
}

function updateBurstReviewButton(ids) {
  var btn = document.getElementById('burstReviewBtn');
  if (!btn) return;
  var canReview = ids.length >= 2;
  btn.style.display = canReview ? '' : 'none';
  btn.disabled = !canReview;
}

function detailMatchesSelectedPhoto() {
  return selectedPhotoId != null && window._detailPhotoId === selectedPhotoId;
}

// Re-apply the .selected highlight to match the current state of
// selectedPhotos / selectedPhotoId. Mirrors the refresh block at the tail
// of selectPhoto(); factored out so the context-menu handler can coerce
// selection without paying the full selectPhoto side-effects.
function refreshCardSelectionVisuals() {
  var byId = new Map(photos.map(function(photo) { return [photo.id, photo]; }));
  document.querySelectorAll('.grid-card, .browse-stack-member').forEach(function(el) {
    var cardId = parseInt(el.dataset.id, 10);
    // Tray members are single photos even when their cover is a stack card,
    // so they never take the stack-wide rule: address them by id only.
    var state = el.classList.contains('browse-stack-member')
      ? (browseSelectionIncludes(cardId) ? ' selected' : '')
      : browseCardSelectionClass(byId.get(cardId));
    el.classList.toggle('selected', state === ' selected');
    el.classList.toggle('stack-partial', state === ' stack-partial');
  });
}

function clearSelection() {
  anchorRestoreEpoch++;
  // Clear both tracks that feed getActiveSelection(), otherwise the batch bar
  // reappears immediately with the single-focus photo still armed for actions.
  selectedPhotos.clear();
  selectedPhotoId = null;
  selectedIndex = -1;
  // Batch-bar Clear (and every other caller) has to scrub the EXIF suggestion
  // too. Without this, the suggestion element keeps its data-photo-id and
  // Accept button after clearing, so a later Select All that still contains
  // the previously-open anchor satisfies renderLocationEmpty's owner-in-
  // selection check and resurrects the anchor's Accept line for the whole
  // batch — one click would apply the anchor's GPS-derived place to every
  // selected photo. Codex P2 on PR #1097.
  clearExifSuggestion();
  // Also drop the ambient detail-photo pointer maybeShowExifSuggestion's
  // post-await path uses as its owner check. Scrubbing the DOM alone isn't
  // enough: a reverse-geocode fetch that was in flight when the user clicked
  // Clear still resolves later, and if _detailPhotoId still points at the
  // departed anchor a subsequent Select All that contains that photo
  // re-satisfies both async guards and repaints A's Accept line into the
  // batch inspector — clicking it would apply A's EXIF place to every
  // selected photo. Codex P2 on PR #1097 (17:04Z follow-up).
  window._detailPhotoId = null;
  // Repaint from the (now empty) selection rather than stripping one class by
  // hand: a stack card can also be carrying the dashed partial mark, and a
  // hand-rolled scrub that only knew about `selected` left that mark on a
  // cleared grid. One rule, one function. Codex P2 on PR #1672.
  refreshCardSelectionVisuals();
  // If the detail panel is still open after clearing, its actions (setFlag,
  // setColorLabel, addKeyword, etc.) early-return on the null selectedPhotoId
  // and silently do nothing. Hide it so there is no ghost UI to interact with.
  var detail = document.getElementById('detailContent');
  if (detail && detail.classList.contains('visible')) {
    detail.classList.remove('visible');
    var summary = document.getElementById('summaryPanel');
    if (summary) summary.classList.remove('hidden');
    loadSummary();
  }
  updateBatchBar();
}

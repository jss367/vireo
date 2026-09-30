// Photo context menu construction and event routing.
// Classic page script; shared globals are initialized before boot.js runs.

var pipelineReviewContextPhotoIds = [];

function buildPipelinePhotoContextMenu(photoIds, clickedPhotoId, inGroupReview) {
  photoIds = pipelineReviewUniquePhotoIds(photoIds);
  var one = photoIds.length === 1;
  var singleHint = one ? undefined : 'Select a single photo';
  var scopedWriteHint = isScopedReviewView()
    ? 'Switch to Latest review to make changes'
    : undefined;
  var tauriNavigationBlocked = !!(
    inGroupReview &&
    typeof isTauri === 'function' && isTauri() &&
    grmHasPendingUserEdits()
  );
  var navigationHint = tauriNavigationBlocked
    ? 'Apply or close Group Review before leaving this page'
    : singleHint;
  var rateChip = function(n) {
    return {
      label: n === 0 ? '\u2606' : String(n),
      title: n === 0 ? 'No rating' : 'Rate ' + n,
      onClick: function() { setPipelineReviewRating(photoIds, n); },
    };
  };
  var flagChip = function(flag, icon, title) {
    return {
      label: icon,
      title: title,
      onClick: function() {
        if (inGroupReview) {
          if (flag === 'flagged') grmMovePick(photoIds);
          else if (flag === 'rejected') grmMoveReject(photoIds);
          else grmMoveCandidate(photoIds);
        } else {
          photoIds.forEach(function(id) { setPipelineReviewFlag(id, flag); });
        }
      },
    };
  };

  var items = [];
  if (inGroupReview) {
    items = items.concat([
      {label: '\u2B06  Move to Picks', onClick: function() { grmMovePick(photoIds); }},
      {label: '\u2423  Move to Candidates', onClick: function() { grmMoveCandidate(photoIds); }},
      {label: '\u2B07  Move to Rejects', onClick: function() { grmMoveReject(photoIds); }},
      {separator: true},
    ]);
  }
  items.push({chips: [0, 1, 2, 3, 4, 5].map(rateChip)});
  items.push({chips: [
    flagChip('flagged', '\u2691', 'Flag as pick'),
    flagChip('rejected', '\u2715', 'Reject'),
    flagChip('none', '\u25CB', 'Clear flag'),
  ]});
  items.push({separator: true});
  items.push({label: 'Add to Collection\u2026', disabled: !!scopedWriteHint,
    disabledHint: scopedWriteHint,
    onClick: function() { addToCollection(photoIds); }});
  items.push({label: 'Add Keyword\u2026', disabled: !!scopedWriteHint,
    disabledHint: scopedWriteHint,
    onClick: function() { batchAddKeyword(photoIds); }});
  items.push({separator: true});
  items.push({label: 'Open in Lightbox', disabled: !one, disabledHint: singleHint,
    onClick: function() { openPipelineLightbox(clickedPhotoId); }});
  items.push({label: 'Open in Browse', disabled: !one || tauriNavigationBlocked, disabledHint: navigationHint,
    onClick: function() { window.openInBrowse(clickedPhotoId); }});
  if (typeof window.findSimilar === 'function') {
    items.push({label: 'Find Similar', disabled: !one, disabledHint: singleHint,
      onClick: function() { window.findSimilar(clickedPhotoId); }});
  }
  items.push({label: window.VIREO_REVEAL_LABEL, disabled: !one, disabledHint: singleHint,
    onClick: function() { revealPhoto(clickedPhotoId); }});
  items.push({label: one ? 'Copy Path' : 'Copy Paths',
    onClick: function() { copyPhotoPaths(photoIds); }});
  items.push({separator: true});
  if (!scopedWriteHint && typeof window.buildSpeciesHighlightMenuItems === 'function') {
    items = items.concat(window.buildSpeciesHighlightMenuItems(photoIds, {showFetchFallback: true}));
  }
  if (!scopedWriteHint && typeof window.buildSpeciesRepresentativeMenuItems === 'function') {
    items = items.concat(window.buildSpeciesRepresentativeMenuItems(photoIds, {showFetchFallback: true}));
  }
  items.push({separator: true});
  items.push({label: 'Edit Photo', disabled: !one || !!scopedWriteHint || tauriNavigationBlocked,
    disabledHint: scopedWriteHint || navigationHint,
    onClick: function() { openPipelinePhotoEditor(clickedPhotoId); }});
  if (typeof window.buildOpenInEditorMenuItems === 'function') {
    items = items.concat(window.buildOpenInEditorMenuItems(photoIds));
  }
  if (typeof window.developPhotos === 'function') {
    items.push({label: 'Develop in darktable', disabled: !!scopedWriteHint,
      disabledHint: scopedWriteHint,
      onClick: function() { window.developPhotos(photoIds); }});
  }
  if (inGroupReview && grmState && grmState.allowRemove !== false) {
    items.push({separator: true});
    items.push({label: 'Remove from Group', onClick: function() { grmRemoveFromGroup(photoIds); }});
  }
  return items;
}

function pipelineContextPhotoIdFromEvent(e) {
  var card = e.target.closest('.photo-card[data-photo-id], .grm-card[data-photo-id]');
  if (card) {
    var cardPid = parseInt(card.dataset.photoId, 10);
    if (cardPid) {
      return {
        photoId: cardPid,
        card: card,
      };
    }
  }

  // The burst-review loupe is an image too, but it is not nested in a
  // data-photo-id card. Resolve it through the modal's current selection so
  // the same context menu actions work on the large preview.
  var loupe = e.target.closest('#grmLoupeImg, #grmLoupePhoto');
  var overlay = document.getElementById('grmOverlay');
  if (
    loupe &&
    overlay &&
    overlay.classList.contains('open') &&
    typeof grmState !== 'undefined' &&
    grmState &&
    grmState.selected
  ) {
    return {
      photoId: parseInt(grmState.selected, 10),
      card: null,
    };
  }

  return null;
}

function bindPipelineReviewContextMenu() {
    /* --- Right-click menu ---
     * Catches contextmenu on photo cards (main grid), grm-cards (group review
     * modal), and the group-review loupe preview. Browse opens in a new tab
     * (browser) or navigates in place (Tauri app window) via openInBrowse, so the
     * user reaches the photo either way. */
    document.addEventListener('contextmenu', function(e) {
      var target = pipelineContextPhotoIdFromEvent(e);
      if (!target) return;
      var pid = target.photoId;
      var card = target.card;
      e.preventDefault();
      var inGroupReview = !!(card && card.classList.contains('grm-card')) ||
        !!e.target.closest('#grmLoupeImg, #grmLoupePhoto');
      if (inGroupReview && typeof grmState !== 'undefined' && grmState) {
        var prevSelected = grmState.selected;
        grmState.selected = pid;
        if (!grmState.selectedIds) grmState.selectedIds = new Set();
        if (!grmState.selectedIds.has(pid)) {
          grmState.selectedIds = new Set([pid]);
        }
        grmState.selectionAnchor = pid;
        grmSyncSelectionClasses();
        // Keep the right-hand loupe in sync with the newly selected card so it
        // doesn't stay on the previous photo if the user dismisses the menu.
        if (prevSelected !== pid && typeof grmRefreshSelectedLoupe === 'function') {
          grmRefreshSelectedLoupe();
        }
      }
      var ids = inGroupReview ? _grmActionTargetIds() : [pid];
      pipelineReviewContextPhotoIds = pipelineReviewUniquePhotoIds(ids);
      if (typeof openContextMenu === 'function') {
        openContextMenu(e, buildPipelinePhotoContextMenu(ids, pid, inGroupReview));
      } else {
        window.openInBrowse(pid);
      }
    });
}

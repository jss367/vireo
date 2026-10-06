// Right-click menus for review cards and Group Review cards.
// Classic page script; load boot.js after all definitions.

/* ---------- Right-click context menu ----------
 * The review grid has no multi-select — the menu always targets the single
 * card under the cursor. Rating / flag chips POST directly to the batch
 * endpoints so they don't depend on helpers that only exist on browse.html.
 */

function buildReviewCardContextMenu(pred) {
  var photoId = pred.photo_id;
  var predId = pred.id;
  var species = pred.group_id ? getConsensusSpecies(pred) : pred.species;
  var speciesLabel = species || 'species';
  var pending = pred.status === 'pending';

  var rateChip = function(n) {
    return {
      label: n === 0 ? '\u2606' : String(n),
      title: n === 0 ? 'No rating' : 'Rate ' + n,
      onClick: function() { setReviewRating(photoId, n); },
    };
  };
  var flagChip = function(f, icon, title) {
    return {
      label: icon, title: title,
      onClick: function() { setReviewFlag(photoId, f); },
    };
  };

  var items = [];
  items.push({
    label: 'Accept as "' + speciesLabel + '"',
    disabled: !pending,
    disabledHint: pending ? undefined : 'Already resolved',
    onClick: function() { acceptPrediction(predId); },
  });
  items.push({
    label: 'Not "' + speciesLabel + '"',
    disabled: !pending,
    disabledHint: pending ? undefined : 'Already resolved',
    onClick: function() { rejectPrediction(predId); },
  });
  items.push({ separator: true });
  items.push({ chips: [0, 1, 2, 3, 4, 5].map(rateChip) });
  items.push({ chips: [
    flagChip('flagged', '\u2691', 'Flag as pick'),
    flagChip('rejected', '\u2715', 'Reject'),
    flagChip('none', '\u25CB', 'Unflag'),
  ] });
  items.push({ separator: true });
  items.push({
    label: 'Open in Lightbox',
    onClick: function() { openReviewLightbox(photoId, pred.filename); },
  });
  if (typeof window.buildSpeciesHighlightMenuItems === 'function') {
    items = items.concat(window.buildSpeciesHighlightMenuItems([photoId], {
      showFetchFallback: true,
    }));
  }
  if (typeof window.buildSpeciesRepresentativeMenuItems === 'function') {
    items = items.concat(window.buildSpeciesRepresentativeMenuItems([photoId], {
      showFetchFallback: true,
    }));
  }
  items.push({
    label: window.VIREO_REVEAL_LABEL,
    onClick: function() { revealReviewPhoto(photoId); },
  });
  items.push({
    label: 'Copy Path',
    onClick: function() { copyReviewPhotoPath(photoId); },
  });
  return items;
}

function bindReviewCardContextMenu() {
  document.addEventListener('contextmenu', function(e) {
    var card = e.target.closest('.card[data-pred-id]');
    if (!card) return;
    // Only hijack contextmenu for cards that live inside the review grid —
    // the burst-group modal and other surfaces handle their own menus.
    var grid = document.getElementById('grid');
    if (!grid || !grid.contains(card)) return;
    e.preventDefault();
    var predId = parseInt(card.dataset.predId, 10);
    var pred = predictions.find(function(p) { return p.id === predId; });
    if (!pred) return;
    if (typeof openContextMenu !== 'function') return;
    openContextMenu(e, buildReviewCardContextMenu(pred));
  });
}

/* ---------- Burst group modal context menu ----------
 * The grm-card selector (.grm-card[data-photo-id]) does NOT collide with
 * the review grid's .card[data-pred-id] handler above — different class
 * names, different closest() matches.
 *
 * Critical: grmMovePick / grmMoveReject / grmMoveCandidate / grmRemoveFromGroup
 * all operate on the active selection. On right-click we force the clicked
 * card to be the primary selection before opening the menu. If the clicked
 * card is already part of a multi-selection, preserve that set so drag
 * alignment state and batch actions are not lost.
 */
function buildBurstGroupContextMenu(photoId, filename) {
  var rateChip = function(n) {
    return {
      label: n === 0 ? '\u2606' : String(n),
      title: n === 0 ? 'No rating' : 'Rate ' + n,
      onClick: function() { setReviewRating(photoId, n); },
    };
  };
  var flagChip = function(f, icon, title) {
    return {
      label: icon, title: title,
      onClick: function() { setReviewFlag(photoId, f); },
    };
  };
  return [
    { label: '\u2B06  Move to Picks',      onClick: function() { grmMovePick(); } },
    { label: '\u2B07  Move to Rejects',    onClick: function() { grmMoveReject(); } },
    { label: '\u2423  Move to Candidates', onClick: function() { grmMoveCandidate(); } },
    { separator: true },
    { chips: [0, 1, 2, 3, 4, 5].map(rateChip) },
    { chips: [
        flagChip('flagged', '\u2691', 'Flag as pick'),
        flagChip('rejected', '\u2715', 'Reject'),
        flagChip('none', '\u25CB', 'Unflag'),
    ] },
    { separator: true },
    { label: 'Open in Lightbox',        onClick: function() { openReviewLightbox(photoId, filename); } },
  ].concat(
    typeof window.buildSpeciesHighlightMenuItems === 'function'
      ? window.buildSpeciesHighlightMenuItems([photoId], { showFetchFallback: true })
      : []
  ).concat(
    typeof window.buildSpeciesRepresentativeMenuItems === 'function'
      ? window.buildSpeciesRepresentativeMenuItems([photoId], { showFetchFallback: true })
      : []
  ).concat([
    { label: window.VIREO_REVEAL_LABEL, onClick: function() { revealReviewPhoto(photoId); } },
    { label: 'Copy Path',               onClick: function() { copyReviewPhotoPath(photoId); } },
    { separator: true },
    // Send-elsewhere actions: get the photo into another tool without losing
    // burst-review state. Browse opens in a new tab so the modal and current
    // pick/reject decisions stay intact.
    { label: 'Send to iNaturalist',
      onClick: function() {
        if (typeof window.submitToInat === 'function') window.submitToInat(photoId);
      } },
    { label: 'Edit Photo',
      onClick: function() {
        // Hand the group's ordered photo list to the editor for Prev/Next,
        // mirroring the lightbox "Edit Photo" handoff. Use the visible
        // (non-removed) items so the editor can't navigate into frames the
        // user has already removed from the group.
        if (window.vireoEditNav) {
          window.vireoEditNav.setList(
            _grmVisibleItems().map(function(it) { return it.photo_id; }), photoId);
          window.vireoEditNav.setLastPhoto(photoId);
        }
        // Open in a new tab in a real browser so the modal stays alive with
        // any pending pick/reject/remove decisions — navigating in place
        // would tear it down and drop that unsaved work. Tauri has no tabs
        // and ignores window.open(_, '_blank'); navigate in place there,
        // same tradeoff as Open in Browse Mode.
        var url = '/edit/' + photoId;
        if (typeof isTauri === 'function' && isTauri()) {
          window.location.href = url;
        } else {
          window.open(url, '_blank', 'noopener');
        }
      } },
  ]).concat(
    typeof window.buildOpenInEditorMenuItems === 'function'
      ? window.buildOpenInEditorMenuItems([photoId])
      : []
  ).concat([
    { label: 'Develop in darktable',
      onClick: function() {
        if (typeof window.developPhotos === 'function') window.developPhotos([photoId]);
      } },
    { label: 'Open in Browse Mode',
      onClick: function() { window.openInBrowse(photoId); } },
    { separator: true },
    { label: 'Remove from Group',       onClick: function() { grmRemoveFromGroup(); } },
  ]);
}

function bindBurstGroupContextMenu() {
  document.addEventListener('contextmenu', function(e) {
    var card = e.target.closest('.grm-card[data-photo-id]');
    if (!card) return;
    // Only fire when the burst modal is actually open — the handler is on
    // document and grm-cards never exist outside #grmOverlay, but belt-and-
    // suspenders against any future render path that reuses the class.
    var overlay = document.getElementById('grmOverlay');
    if (!overlay || !overlay.classList.contains('open')) return;
    e.preventDefault();
    var photoId = parseInt(card.dataset.photoId, 10);
    if (!photoId) return;
    var item = grmState.items.find(function(it) { return it.photo_id === photoId; });
    var filename = item ? item.filename : '';
    // Force-select the right-clicked card so move/remove actions target it.
    grmState.selected = photoId;
    if (!grmState.selectedIds) grmState.selectedIds = new Set();
    if (!grmState.selectedIds.has(photoId)) {
      grmState.selectedIds = new Set([photoId]);
    }
    grmState.selectionAnchor = photoId;
    renderGroupModal();
    if (typeof openContextMenu !== 'function') return;
    openContextMenu(e, buildBurstGroupContextMenu(photoId, filename));
  });
}

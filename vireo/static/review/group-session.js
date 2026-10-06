// Group Review state, opening and closing the modal, and loading a burst.
// Classic page script; load boot.js after all definitions.

/* ==================== Group Review Modal ==================== */
var grmState = { groupId: null, model: null, items: [], picks: new Set(), rejects: new Set(), removed: new Set(), selected: null, selectedIds: new Set(), selectionAnchor: null, upgraded: false };

function openGroupReview(groupId, model) {
  grmState = { groupId: groupId, model: model, items: [], picks: new Set(), rejects: new Set(), removed: new Set(), selected: null, selectedIds: new Set(), selectionAnchor: null, upgraded: false };
  _grmLoupeLocked = false;
  // Reset zoom state so a high zoom from a prior burst doesn't leak in. With
  // the natural-size layout, _grmZoomLevel is now a multiplier above
  // cover-fit (1 = no zoom).
  _grmZoomLevel = 1;
  _grmLoupeZoomLevel = 1;
  _grmOffsets = {};
  _grmDragging = null;
  _grmLoupeAlignDragging = null;
  document.removeEventListener('mousemove', grmLoupeMouseMove);
  document.removeEventListener('mouseup', grmLoupeMouseUp);
  _grmSuppressNextClick = false;
  _grmSuppressLoupeClick = false;
  _grmLoupeLastX = 50;
  _grmLoupeLastY = 50;
  _grmLoupeHovering = false;
  _grmPendingSnap = false;
  _grmLoupeOneToOne = false;
  _grmLoupePendingOneToOne = false;
  grmRefreshResetAllVisibility();
  document.getElementById('grmOverlay').classList.add('open');
  grmSetThumbSize(GRM_CARD_W, false);
  var resSlider = document.getElementById('grmResSlider');
  if (resSlider) resSlider.value = grmResolutionIdx;
  grmSetLoupeZoom(100, false);
  grmUpdateResLabel();
  loadGroupData(groupId);
}

function closeGroupReview() {
  document.getElementById('grmOverlay').classList.remove('open');
  _grmOffsets = {};
  _grmDragging = null;
  _grmLoupeAlignDragging = null;
  document.removeEventListener('mousemove', grmLoupeMouseMove);
  document.removeEventListener('mouseup', grmLoupeMouseUp);
  grmRefreshResetAllVisibility();
}

async function loadGroupData(groupId) {
  try {
    grmState.items = await safeFetch('/api/predictions/group/' + encodeURIComponent(groupId), {}, { toast: false });

    // Auto-pick the AI best (highest quality score)
    var best = null;
    grmState.items.forEach(function(item) {
      if (!best || (item.quality_score || 0) > (best.quality_score || 0)) best = item;
    });
    if (best) grmState.picks.add(best.photo_id);

    // Auto-reject the lowest quality
    if (grmState.items.length > 2) {
      var sorted = grmState.items.slice().sort(function(a, b) { return (a.quality_score || 0) - (b.quality_score || 0); });
      var worstThird = Math.max(1, Math.floor(sorted.length / 3));
      for (var i = 0; i < worstThird; i++) {
        if (!grmState.picks.has(sorted[i].photo_id)) {
          grmState.rejects.add(sorted[i].photo_id);
        }
      }
    }

    // Auto-select the best photo for the loupe
    if (best) {
      grmState.selected = best.photo_id;
      grmState.selectedIds = new Set([best.photo_id]);
      grmState.selectionAnchor = best.photo_id;
      renderGroupModal();
      grmRefreshSelectedLoupe();
    } else {
      renderGroupModal();
    }
  } catch(e) {
    console.error('Failed to load group:', e);
  }
}

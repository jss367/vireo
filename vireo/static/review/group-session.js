// Group Review state, opening and closing the modal, and loading a burst.
// Classic page script; load boot.js after all definitions.

/* ==================== Group Review Modal ==================== */
/** @type {ReviewGroupState} */ var grmState = { groupId: null, model: null, items: [], picks: new Set(), rejects: new Set(), removed: new Set(), selected: null, selectedIds: new Set(), selectionAnchor: null, upgraded: false };

/**
 * The Group Review selection the shared navbar's native menu acts on while
 * the modal is open (the getGroupModalSelection page hook).
 * @returns {number[]}
 */
function grmNativeMenuSelection() {
  if (grmState.selectedIds && grmState.selectedIds.size) {
    return /** @type {number[]} */ (Array.from(grmState.selectedIds));
  }
  if (grmState.selected != null) return [/** @type {number} */ (grmState.selected)];
  return [];
}

/** @param {string} groupId @param {string} model */ function openGroupReview(groupId, model) {
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
  /** @type {HTMLElement} */ (document.getElementById('grmOverlay')).classList.add('open');
  grmSetThumbSize(GRM_CARD_W, false);
  var resSlider = /** @type {HTMLInputElement} */ (document.getElementById('grmResSlider'));
  if (resSlider) resSlider.value = String(grmResolutionIdx);
  grmSetLoupeZoom(100, false);
  grmUpdateResLabel();
  loadGroupData(groupId);
}

function closeGroupReview() {
  /** @type {HTMLElement} */ (document.getElementById('grmOverlay')).classList.remove('open');
  _grmOffsets = {};
  _grmDragging = null;
  _grmLoupeAlignDragging = null;
  document.removeEventListener('mousemove', grmLoupeMouseMove);
  document.removeEventListener('mouseup', grmLoupeMouseUp);
  grmRefreshResetAllVisibility();
}

/** @param {string} groupId */ async function loadGroupData(groupId) {
  try {
    grmState.items = await safeFetch('/api/predictions/group/' + encodeURIComponent(groupId), {}, { toast: false });

    // Auto-pick the AI best (highest quality score)
    /** @type {ReviewPrediction|null} */ var best = grmState.items.reduce(function(best, item) { return !best || (item.quality_score || 0) > (best.quality_score || 0) ? item : best; }, /** @type {ReviewPrediction|null} */ (null));
    if (best) grmState.picks.add(best.photo_id);

    // Auto-reject the lowest quality
    if (grmState.items.length > 2) {
      var sorted = grmState.items.slice().sort(function(a, b) { return (a.quality_score || 0) - (b.quality_score || 0); });
      var worstThird = Math.max(1, Math.floor(sorted.length / 3));
      for (var i = 0; i < worstThird; i++) {
        var item = sorted[i]; if (item && !grmState.picks.has(item.photo_id)) {
          grmState.rejects.add(item.photo_id);
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

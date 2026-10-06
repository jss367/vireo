// Subject/background adjustments, the mask snapshot, and the mask overlay.
// Classic page script; load boot.js after all definitions.

// --- Local (mask-weighted) adjustments --------------------------------------
// Subject/Background deltas driven by the photo's SAM mask, frozen into an
// edit-mask snapshot on first touch so saved renders never shift under a
// regenerated mask. See docs/plans/2026-07-03-local-adjustments-design.md.

var LOCAL_REGIONS = ['subject', 'background'];
var LOCAL_CONTROL_DEFS = [
  ['exposure', 'Exposure', -5, 5, 0.1, true],
  ['shadows', 'Shadows', -100, 100, 1, false],
  ['highlights', 'Highlights', -100, 100, 1, false],
  ['saturation', 'Saturation', -100, 100, 1, false],
  ['sharpen', 'Sharpen', -100, 100, 1, false],
  ['noise_reduction', 'Denoise', -100, 100, 1, false],
];

function buildLocalControls() {
  LOCAL_REGIONS.forEach(function(region) {
    var host = document.getElementById(region + 'LocalControls');
    if (!host || host.childElementCount) return;
    LOCAL_CONTROL_DEFS.forEach(function(def) {
      var key = def[0];
      var id = 'local_' + region + '_' + key;
      var row = document.createElement('div');
      row.className = 'control-row';
      row.innerHTML =
        '<label for="' + id + 'Range">' + def[1] + '</label>' +
        '<input id="' + id + 'Range" type="range" min="' + def[2] +
        '" max="' + def[3] + '" step="' + def[4] + '" value="0">' +
        '<span class="control-value" id="' + id + 'Value">' +
        (def[5] ? '0.0' : '0') + '</span>';
      host.appendChild(row);
      row.querySelector('input').addEventListener('input', function() {
        setLocalAdjustment(region, key, this.value, def[5]);
      });
    });
  });
}

function localRegionValues(recipe) {
  var out = {subject: {}, background: {}, feather: 0};
  var local = recipe && recipe.local || {};
  (local.regions || []).forEach(function(entry) {
    if (out[entry.region]) {
      out[entry.region] = Object.assign({}, entry.adjustments || {});
    }
  });
  out.feather = Number((local.mask || {}).feather || 0);
  return out;
}

function rebuildLocalSection() {
  var draft = editorState.localDraft;
  var regions = [];
  LOCAL_REGIONS.forEach(function(region) {
    var adj = {};
    LOCAL_CONTROL_DEFS.forEach(function(def) {
      var v = Number(draft[region][def[0]] || 0);
      if (Math.abs(v) > 0.000001) adj[def[0]] = v;
    });
    if (Object.keys(adj).length) regions.push({region: region, adjustments: adj});
  });
  // Mirror normalize_recipe's canonical region order so the dirty-state
  // comparison stays stable against the server's saved form.
  regions.sort(function(a, b) { return a.region < b.region ? -1 : 1; });
  if (!regions.length) {
    delete editorState.recipe.local;
    // No adjustment retains this snapshot. Preview and the next adjustment
    // must both use the current active generation, including after zeroing
    // the last region while a snapshot request is still in flight.
    editorState.localMask = null;
    editorState.localMaskPromise = null;
    editorState.localMaskUpdateSeq++;
    return;
  }
  if (!editorState.localMask) {
    delete editorState.recipe.local;
    return;
  }
  var mask = {
    ref: editorState.localMask.ref,
    source_digest: editorState.localMask.source_digest,
  };
  if (Math.abs(Number(draft.feather || 0)) > 0.000001) {
    mask.feather = Number(draft.feather);
  }
  editorState.recipe.local = {mask: mask, regions: regions};
}

function syncLocalControls() {
  var vals = localRegionValues(editorState.recipe);
  editorState.localDraft = {
    subject: vals.subject,
    background: vals.background,
    feather: vals.feather,
  };
  var savedMask = (editorState.recipe.local || {}).mask;
  if (savedMask && savedMask.ref) {
    editorState.localMask = {
      ref: savedMask.ref,
      source_digest: savedMask.source_digest,
    };
  } else {
    // No saved local recipe: clear the cached snapshot so the next local
    // slider touch re-freezes the CURRENT active mask instead of taking the
    // stale-ref fast path in ensureLocalMask().
    editorState.localMask = null;
    editorState.localMaskPromise = null;
  }
  LOCAL_REGIONS.forEach(function(region) {
    LOCAL_CONTROL_DEFS.forEach(function(def) {
      var id = 'local_' + region + '_' + def[0];
      var input = document.getElementById(id + 'Range');
      var label = document.getElementById(id + 'Value');
      var v = Number(vals[region][def[0]] || 0);
      if (input) input.value = String(v);
      if (label) label.textContent = def[5] ? v.toFixed(1) : String(Math.round(v));
    });
  });
  var feather = document.getElementById('featherRange');
  if (feather) feather.value = String(vals.feather || 0);
  var featherLabel = document.getElementById('featherValue');
  if (featherLabel) featherLabel.textContent = String(Math.round(vals.feather || 0));
  updateLocalBandVisibility();
}

function updateLocalBandVisibility() {
  var available = editorState.localAvailable || !!editorState.recipe.local;
  var band = document.getElementById('localBand');
  var unavailable = document.getElementById('localUnavailableBand');
  if (band) band.style.display = available ? '' : 'none';
  if (unavailable) unavailable.style.display = available ? 'none' : '';
  var banner = document.getElementById('localStaleBanner');
  if (banner) banner.style.display = editorState.localStale ? '' : 'none';
}

function ensureLocalMask() {
  if (editorState.localMask) return Promise.resolve(editorState.localMask);
  if (!editorState.localMaskPromise) {
    var photoId = editorState.photoId;
    // A concurrent resetLocal() / photo change clears localMaskPromise while
    // the snapshot POST is in flight; without the identity guard below, the
    // late resolver would re-cache a pre-reset snapshot and the next slider
    // touch would save that stale ref instead of freezing the current mask.
    var pending = safeFetch(
      '/api/photos/' + photoId + '/local-mask/snapshot',
      {method: 'POST'}, {toast: false}
    ).then(function(data) {
      if (editorState.photoId === photoId && editorState.localMaskPromise === pending) {
        editorState.localMask = data.mask;
      }
      return data.mask;
    }).catch(function(e) {
      if (editorState.photoId === photoId && editorState.localMaskPromise === pending) {
        editorState.localMaskPromise = null;
        editorState.localAvailable = false;
        if (window.renderHistoryControls) window.renderHistoryControls();
        updateLocalBandVisibility();
        if (typeof showToast === 'function') {
          showToast(e.message || 'Could not snapshot the subject mask', 'error');
        }
      }
      throw e;
    });
    editorState.localMaskPromise = pending;
    if (window.renderHistoryControls) window.renderHistoryControls();
  }
  return editorState.localMaskPromise;
}

function setLocalAdjustment(region, key, raw, fixed) {
  if (editorState.loading) return;
  var v = Number(raw) || 0;
  editorState.localDraft[region][key] = v;
  var label = document.getElementById('local_' + region + '_' + key + 'Value');
  if (label) label.textContent = fixed ? v.toFixed(1) : String(Math.round(v));
  if (editorState.localMask) {
    rebuildLocalSection();
    markChanged(true);
  } else {
    // First touch: freeze the active mask into a snapshot, then attach the
    // pending slider state. The snapshot is content-addressed, so repeat
    // calls are idempotent.
    var photoId = editorState.photoId;
    var updateSeq = editorState.localMaskUpdateSeq;
    ensureLocalMask().then(function() {
      if (editorState.photoId !== photoId || editorState.localMaskUpdateSeq !== updateSeq) return;
      rebuildLocalSection();
      markChanged(true);
    }).catch(function() {});
  }
}

function setLocalFeather(raw) {
  if (editorState.loading) return;
  var v = Number(raw) || 0;
  editorState.localDraft.feather = v;
  var label = document.getElementById('featherValue');
  if (label) label.textContent = String(Math.round(v));
  // Feather alone previews the live mask. Freeze it only when a region
  // adjustment begins, so the overlay and that adjustment use the same mask.
  if (!editorState.recipe.local && !editorState.localMask) {
    scheduleMaskOverlayRefresh();
    return;
  }
  if (editorState.localMask) {
    rebuildLocalSection();
    markChanged(true);
  } else {
    var photoId = editorState.photoId;
    var updateSeq = editorState.localMaskUpdateSeq;
    ensureLocalMask().then(function() {
      if (editorState.photoId !== photoId || editorState.localMaskUpdateSeq !== updateSeq) return;
      rebuildLocalSection();
      markChanged(true);
    }).catch(function() {});
  }
}

function resetLocal() {
  editorState.localMaskUpdateSeq++;
  editorState.localDraft = {subject: {}, background: {}, feather: 0};
  delete editorState.recipe.local;
  syncLocalControls();
  markChanged(true);
}

async function updateLocalMask() {
  var photoId = editorState.photoId;
  var updateSeq = ++editorState.localMaskUpdateSeq;
  try {
    var data = await safeFetch(
      '/api/photos/' + photoId + '/local-mask/snapshot',
      {method: 'POST'}, {toast: false}
    );
    if (editorState.photoId !== photoId ||
        editorState.localMaskUpdateSeq !== updateSeq) return;
    editorState.localMask = data.mask;
    editorState.localStale = false;
    rebuildLocalSection();
    updateLocalBandVisibility();
    markChanged(true);
    if (typeof showToast === 'function') {
      showToast('Subject mask updated — save to keep it', 'success');
    }
  } catch (e) {
    if (editorState.photoId !== photoId ||
        editorState.localMaskUpdateSeq !== updateSeq) return;
    if (typeof showToast === 'function') {
      showToast(e.message || 'Could not update the subject mask', 'error');
    }
  }
}

function toggleMaskOverlay() {
  editorState.maskOverlay = !editorState.maskOverlay;
  setButtonActive('maskOverlayBtn', editorState.maskOverlay);
  refreshMaskOverlay();
}

function refreshMaskOverlay() {
  var overlay = document.getElementById('maskOverlayImg');
  if (!overlay) return;
  // Keep crop in the overlay recipe. /edit-mask-preview strips crop only
  // for geometry alignment with the uncropped editor preview, but still
  // uses the ORIGINAL cropped recipe to compute the saved-render feather
  // scale (mirroring what /edit-preview passes to
  // apply_recipe_to_loaded_image). Sending a crop-stripped recipe here
  // would let the endpoint recompute the scale from the uncropped
  // dimensions and the overlay halo would drift from the pixels the
  // saved cropped render actually weights.
  var source = editorState.showBefore ? editorState.savedRecipe : editorState.recipe;
  var recipe = recipeForSave(source || {});
  // With no local section yet, the endpoint previews the photo's active
  // mask at the Feather slider's value — the mask the first local slider
  // would freeze — so the user can check it before adjusting anything.
  var previewActiveMask = !recipe.local && editorState.localAvailable;
  if (!editorState.maskOverlay || !(recipe.local || previewActiveMask)) {
    // Bump the sequence and clear handlers so any in-flight preview load
    // can't win the race and re-show an overlay the user just hid (or one
    // from a previous photo during navigation).
    editorState.maskOverlaySeq++;
    overlay.onload = null;
    overlay.onerror = null;
    overlay.style.display = 'none';
    overlay.removeAttribute('src');
    return;
  }
  // The overlay is stretched to the displayed image, so its render never
  // needs to exceed 3840 even when the preview is a native-resolution 1:1
  // render — a full-res RGBA weight map would just burn memory and encode time.
  var size = Math.min(previewRenderSize(), 3840);
  var url = '/photos/' + editorState.photoId + '/edit-mask-preview?size=' +
    size + '&apply_crop=' + (editorPreviewAppliesCrop() ? '1' : '0') +
    '&recipe=' + encodeURIComponent(JSON.stringify(recipe));
  if (previewActiveMask) {
    url += '&feather=' + encodeURIComponent(Number(editorState.localDraft.feather) || 0);
  }
  if (overlay.getAttribute('src') === url && overlay.complete && overlay.naturalWidth) {
    // Identical overlay already loaded — just make sure it's placed and shown.
    editorState.maskOverlaySeq++;
    positionMaskOverlay();
    overlay.style.display = '';
    return;
  }
  var seq = ++editorState.maskOverlaySeq;
  overlay.onload = function() {
    if (seq !== editorState.maskOverlaySeq) return;
    positionMaskOverlay();
    overlay.style.display = '';
  };
  overlay.onerror = function() {
    if (seq !== editorState.maskOverlaySeq) return;
    overlay.style.display = 'none';
  };
  overlay.src = url;
}

function positionMaskOverlay() {
  var img = document.getElementById('editorImg');
  var overlay = document.getElementById('maskOverlayImg');
  if (!img || !overlay) return;
  overlay.style.width = img.clientWidth + 'px';
  overlay.style.height = img.clientHeight + 'px';
}

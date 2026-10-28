// Profile availability is photo-specific; only the enable flag goes in recipes.
function syncLensProfileControls() {
  var enabled = !!(editorState.recipe.adjustments || {}).lens_profile;
  /** @type {HTMLInputElement} */ (document.getElementById('lensProfileEnabled')).checked = enabled;
  var profile = (editorState.photo || {}).lens_profile;
  var status = /** @type {HTMLElement} */ (document.getElementById('lensProfileStatus'));
  if (!profile) { status.textContent = 'Lens metadata is unavailable.'; return; }
  var message = (profile.lens ? profile.lens + '. ' : '') + profile.message;
  if (profile.matched) {
    if (!enabled) message += ' Profile corrections are off.';
    else {
      var image = /** @type {HTMLImageElement} */ (document.getElementById('editorImg'));
      if (!editorImageMatchesZoomRecipe(image) || !image.dataset.previewSourceKind) message += ' Checking the rendered source…';
      else if (image.dataset.previewSourceKind === 'linear') message += ' Applied to the decoded RAW.';
      else message += ' Skipped: the preview is using a rendered image fallback.';
    }
  } else if (enabled) message += ' No profile correction will be applied.';
  status.textContent = message;
}

/** @param {boolean} enabled */ function setLensProfile(enabled) {
  if (editorState.loading || editorState.showBefore) return;
  var adj = cloneRecipe(editorState.recipe.adjustments || {});
  if (enabled) adj.lens_profile = true;
  else delete adj.lens_profile;
  if (Object.keys(adj).length) editorState.recipe.adjustments = adj;
  else delete editorState.recipe.adjustments;
  markChanged(true);
  syncLensProfileControls();
}

// Image elements cannot read response headers. Query the bounded per-render
// source receipt after decoding; this does not load or render the RAW again.
/** @param {EditorPreviewRequest} request @param {HTMLImageElement} image */ async function loadLensPreviewSource(request, image) {
  var recipe = editorState.showBefore ? editorState.savedRecipe : editorState.recipe;
  if (!(recipe.adjustments || {}).lens_profile || !(editorState.photo || {}).lens_profile?.matched) return;
  if (image.dataset.previewSourceKind) { syncLensProfileControls(); return; }
  var url = request.url + '&preview_session=' + editorPreviewSession +
    '&preview_seq=' + (request.serverSequence ?? image.dataset.previewServerSequence) + '&source_status=1';
  try {
    var result = await safeFetch(url, {}, {toast: false});
    if (document.getElementById('editorImg') !== image || request.seq !== editorState.previewSeq) return;
    if (result.source !== 'linear' && result.source !== 'srgb') throw new Error('Render source receipt expired');
    image.dataset.previewSourceKind = result.source;
    syncLensProfileControls();
    refreshMaskOverlay();
    updateMaskBrushCursor();
  } catch (_) {
    if (document.getElementById('editorImg') === image && request.seq === editorState.previewSeq) {
      /** @type {HTMLElement} */ (document.getElementById('lensProfileStatus')).textContent =
        'Could not verify the preview source. Retry the preview before painting masks.';
      /** @type {HTMLButtonElement} */ (document.getElementById('previewRetryBtn')).hidden = false;
    }
  }
}

/** @param {EditorRecipe} recipe */ function lensProfileSourcePending(recipe) {
  var image = /** @type {HTMLImageElement} */ (document.getElementById('editorImg'));
  return !!(recipe.adjustments || {}).lens_profile && !!(editorState.photo || {}).lens_profile?.matched &&
    !image.dataset.previewSourceKind;
}

/** @param {EditorRecipe} recipe @returns {number[]|null} */ function lensProfileWarp(recipe) {
  var image = /** @type {HTMLImageElement} */ (document.getElementById('editorImg'));
  return (recipe.adjustments || {}).lens_profile && image.dataset.previewSourceKind === 'linear'
    ? (editorState.photo || {}).lens_profile?.warp || null : null;
}

/** @param {number} radius @param {number} amount @param {number[]} warp */ function lensRadialFactor(radius, amount, warp) {
  var manual = 1 - 0.2 * amount / 100 * radius * radius;
  var r = radius * manual, value = 0;
  for (var i = warp.length - 1; i >= 0; i--) value = value * r + (warp[i] || 0);
  return manual * value;
}

/** @param {number} amount @param {number[]} warp */ function lensProfileInset(amount, warp) {
  var lower = 0, upper = 1;
  for (var i = 0; i < 32; i++) {
    var middle = (lower + upper) / 2, largest = 0;
    for (var j = 0; j <= 256; j++) largest = Math.max(largest, lensRadialFactor(j / 256 * middle, amount, warp));
    if (middle * largest <= 1) lower = middle;
    else upper = middle;
  }
  return lower;
}

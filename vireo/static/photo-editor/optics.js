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

// The footer's source line: which file the displayed preview was rendered
// from. Each entry is [text, tooltip, degraded] for an origin the receipt
// reports; the `:raw` variants apply when the photo's original is a RAW file,
// where a JPEG origin means the edit is not running on the sensor data.
/** @type {Record<string,[string,string,boolean]>} */ var EDITOR_PREVIEW_SOURCES = {
  raw: ['Source: RAW sensor data', 'Edits are rendered from the sensor data decoded from the RAW file.', false],
  embedded: ['Source: camera JPEG embedded in the RAW — its sensor data could not be decoded',
    'Vireo could not decode this RAW, so edits are rendered from the JPEG the camera stored inside it.', true],
  'companion:raw': ['Source: companion JPEG, not the RAW',
    'Vireo renders edits from the companion JPEG when the RAW is offline or its sensor data could not be decoded.', true],
  companion: ['Source: companion JPEG', 'Edits are rendered from the JPEG paired with the original file.', false],
  'working_copy:raw': ['Source: Vireo’s working-copy JPEG, not the RAW',
    'Vireo renders edits from its working copy when the RAW is offline or its sensor data could not be decoded.', true],
  working_copy: ['Source: Vireo’s working copy of the original',
    'Edits are rendered from Vireo’s working copy, which holds enough pixels for this preview.', false],
  original: ['Source: original file', 'Edits are rendered from the original file.', false],
};

// `origin` undefined clears the line (no photo, or one still loading); null
// says the receipt could not be read, which is not the same as a fallback.
/** @param {string|null|undefined} origin @param {boolean=} originalIsRaw */ function showPreviewSource(origin, originalIsRaw) {
  var label = /** @type {HTMLElement} */ (document.getElementById('previewSource'));
  var entry = origin ? (originalIsRaw && EDITOR_PREVIEW_SOURCES[origin + ':raw']) || EDITOR_PREVIEW_SOURCES[origin] : null;
  if (origin === undefined) entry = ['', '', false];
  else if (!entry) entry = ['Source: could not be verified', 'Vireo could not confirm which file this preview was rendered from.', false];
  label.textContent = entry[0];
  label.title = entry[1];
  label.classList.toggle('degraded', entry[2]);
}

/** @param {EditorRecipe} recipe */ function lensProfileNeedsSource(recipe) {
  return !!(recipe.adjustments || {}).lens_profile && !!(editorState.photo || {}).lens_profile?.matched;
}

// Image elements cannot read response headers. Query the bounded per-render
// source receipt after decoding; this does not load or render the RAW again.
// Lens geometry needs the receipt for every render. The source line asks only
// for refined renders, and keeps the last answer while quick renders pass.
/** @param {EditorPreviewRequest} request @param {HTMLImageElement} image */ async function loadPreviewSource(request, image) {
  var lens = lensProfileNeedsSource(editorState.showBefore ? editorState.savedRecipe : editorState.recipe);
  if (!lens && request.interactive) return;
  if (image.dataset.previewSourceOrigin && (!lens || image.dataset.previewSourceKind)) {
    // A reused render already has its receipt; the line may have been cleared since.
    showPreviewSource(image.dataset.previewSourceOrigin, image.dataset.previewOriginalRaw === '1');
    syncLensProfileControls();
    return;
  }
  var label = /** @type {HTMLElement} */ (document.getElementById('previewSource'));
  if (!label.textContent) label.textContent = 'Source: checking…';
  var url = request.url + '&preview_session=' + editorPreviewSession +
    '&preview_seq=' + (request.serverSequence ?? image.dataset.previewServerSequence) + '&source_status=1';
  /** @returns {boolean} */ function current() {
    return document.getElementById('editorImg') === image && request.seq === editorState.previewSeq;
  }
  try {
    /** @type {{source:string|null;origin:string|null;original_is_raw:boolean}} */ var result = await safeFetch(url, {}, {toast: false});
    if (!current()) return;
    if (!result.origin || (result.source !== 'linear' && result.source !== 'srgb')) throw new Error('Render source receipt expired');
    image.dataset.previewSourceOrigin = result.origin;
    image.dataset.previewOriginalRaw = result.original_is_raw ? '1' : '';
    showPreviewSource(result.origin, result.original_is_raw);
    if (!lens) return;
    image.dataset.previewSourceKind = result.source;
    syncLensProfileControls();
    refreshMaskOverlay();
    updateMaskBrushCursor();
  } catch (_) {
    if (!current()) return;
    showPreviewSource(null);
    if (!lens) return;
    /** @type {HTMLElement} */ (document.getElementById('lensProfileStatus')).textContent =
      'Could not verify the preview source. Retry the preview before painting masks.';
    /** @type {HTMLButtonElement} */ (document.getElementById('previewRetryBtn')).hidden = false;
  }
}

/** @param {EditorRecipe} recipe */ function lensProfileSourcePending(recipe) {
  var image = /** @type {HTMLImageElement} */ (document.getElementById('editorImg'));
  return lensProfileNeedsSource(recipe) && !image.dataset.previewSourceKind;
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

/** @param {EditorRecipe} recipe @returns {EditorBrushOptics|null} */ function maskBrushOptics(recipe) {
  var amount = (recipe.adjustments || {}).distortion || 0;
  var warp = lensProfileWarp(recipe);
  return amount || warp ? {distortion: amount, warp: warp ? warp.slice() : null} : null;
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

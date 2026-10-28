// Loading a photo, the empty state, and editor initialization.
// Classic page script; load boot.js after all definitions.

function editorEmptyState() {
  /** @type {HTMLElement} */ (document.getElementById('editorFilename')).textContent = 'No photo to edit';
  /** @type {HTMLElement} */ (document.getElementById('editorSubhead')).textContent = 'Open a photo from Browse, then choose Edit.';
  setStatus('');
  var img = /** @type {HTMLImageElement} */ (document.getElementById('editorImg'));
  if (img) img.removeAttribute('src');
  var box = /** @type {HTMLElement} */ (document.getElementById('editorCropBox'));
  if (box) box.style.display = 'none';
  ['prevBtn', 'nextBtn', 'copySettingsBtn', 'beforeBtn', 'fitBtn', 'actualBtn',
   'editorZoomOut', 'editorZoomIn', 'editorZoomSlider', 'saveBtn', 'exportBtn'].forEach(function(id) {
    var b = /** @type {HTMLElement} */ (document.getElementById(id));
    if (b instanceof HTMLButtonElement || b instanceof HTMLInputElement) b.disabled = true;
  });
}

/** @param {number} photoId @param {number} loadSeq @param {number} savedRecipeSeq @param {{local_mask_stale?:boolean}} data */ function applyLoadedLocalStaleness(photoId, loadSeq, savedRecipeSeq, data) {
  if (editorState.photoId !== photoId || editorState.loadSeq !== loadSeq ||
      editorState.savedRecipeSeq !== savedRecipeSeq) return false;
  editorState.savedLocalStale = !!data.local_mask_stale;
  refreshEditorHistoryLocalStaleness();
  updateLocalBandVisibility();
  return true;
}

/** @param {number} photoId @param {{skipUrl?:boolean;versionId?:string|null}=} opts */ function beginEditorPhotoLoad(photoId, opts) {
  cancelPointColorPicker();
  cancelMaskBrush();
  cancelEditorPreview({abortActive: true});
  editorState.previewInputAt = performance.now();
  opts = opts || {};
  photoId = Number(photoId);

  // Rapid Next/Prev can leave multiple /api/photos/:id requests in flight; the
  // seq lets a stale resolution bail out instead of overwriting newer state.
  var seq = ++editorState.loadSeq;
  editorState.localMaskUpdateSeq++;
  editorState.photoId = photoId;
  editorState.versionId = opts.versionId || null;
  editorState.version = null;
  var versionId = editorState.versionId;
  editorState.photo = null;
  // Reset recipe state so a Save click during the load window can't write the
  // previous (possibly dirty) photo's recipe onto the new photoId. syncControls
  // re-runs updateDirtyState after the new photo loads, restoring Save state.
  editorState.savedRecipe = {};
  editorState.savedRecipeSeq++;
  editorState.recipe = {};
  resetEditorHistory();
  var saveBtn = /** @type {HTMLButtonElement} */ (document.getElementById('saveBtn'));
  if (saveBtn) saveBtn.disabled = true;
  ['copySettingsBtn', 'beforeBtn', 'fitBtn', 'actualBtn', 'editorZoomOut',
   'editorZoomIn', 'editorZoomSlider', 'exportBtn'].forEach(function(id) {
    var b = /** @type {HTMLElement} */ (document.getElementById(id));
    if (b instanceof HTMLButtonElement || b instanceof HTMLInputElement) b.disabled = false;
  });
  editorState.showBefore = false;
  editorState.cropEditing = true;
  // Restore only the resizing constraint; opening a photo must not change
  // its saved crop or create an unsaved edit. Sync the last committed
  // preference first so navigation adopts a newer choice made in another tab
  // even before its storage event has been delivered.
  adoptCommittedCropRatioPreference();
  editorState.cropAspect = cropRatioPreference.enabled ? cropRatioPreference.aspect : null;
  updateAspectButtons();
  if (window.vireoEditNav) window.vireoEditNav.setLastPhoto(photoId);
  // Keep the URL in sync so refresh / bookmark / Back behave sensibly.
  if (!opts.skipUrl) {
    try { window.history.replaceState({photoId: photoId}, '', '/edit/' + photoId + (versionId ? '?version=' + versionId : '')); } catch (_) {}
  }
  if (typeof window.vireoRefreshNavigationHrefs === 'function') {
    window.vireoRefreshNavigationHrefs();
  }
  setStatus('Loading...');
  /** @type {HTMLElement} */ (document.getElementById('editorFilename')).textContent = 'Loading photo...';
  updateNavControls();
  setEditorLoading(true);
  return {seq: seq, versionId: versionId};
}

/** @param {number} photoId @param {{skipUrl?:boolean;versionId?:string|null}=} opts */
async function loadPhoto(photoId, opts) {
  photoId = Number(photoId);
  if (!Number.isInteger(photoId) || photoId <= 0) return;
  var started = beginEditorPhotoLoad(photoId, opts);
  var seq = started.seq, versionId = started.versionId;
  try {
    /** @type {EditorPhoto} */ var photo = await safeFetch('/api/photos/' + photoId, {}, {toast: false});
    if (seq !== editorState.loadSeq) return;
    if (versionId) {
      /** @type {{version:EditorVersion;recipe:EditorRecipe|null}} */ var versionData = await safeFetch(editorRecipeUrl(photoId, versionId), {}, {toast: false});
      if (seq !== editorState.loadSeq) return;
      if (versionData.version.kind !== 'copy') throw new Error('Open an editable copy to restore this snapshot.');
      editorState.version = versionData.version;
      photo.edit_recipe = versionData.recipe || undefined;
    }
    editorState.photo = photo;
    editorState.savedRecipe = cloneRecipe(photo.edit_recipe || {});
    editorState.savedRecipeSeq++;
    editorState.recipe = cloneRecipe(photo.edit_recipe || {});
    ensureCrop(editorState.recipe);
    editorState.cropEditing = !recipeForSave(editorState.recipe).crop;
    editorState.localMask = null;
    editorState.localMaskPromise = null;
    editorState.localAvailable = false;
    editorState.localStale = false;
    editorState.savedLocalStale = false;
    resetEditorHistory();
    editorState.maskOverlay = false;
    setButtonActive('maskOverlayBtn', false);
    refreshMaskOverlay();
    safeFetch('/api/photos/' + photoId + '/masks', {}, {toast: false})
      .then(/** @param {{active?:unknown;local_mask_stale?:boolean}} d */ function(d) {
        if (editorState.photoId !== photoId) return;
        var becameAvailable = !editorState.localAvailable && !!d.active;
        editorState.localAvailable = !!d.active;
        updateLocalBandVisibility();
        // If the user already clicked Show Mask before this resolved,
        // the overlay was cleared because availability was still false.
        // Retry the overlay now that we know an active mask exists.
        if (becameAvailable && editorState.maskOverlay) {
          refreshMaskOverlay();
        }
      }).catch(function() {});
    if (photo.edit_recipe && photo.edit_recipe.local) {
      var localStaleSavedRecipeSeq = editorState.savedRecipeSeq;
      safeFetch(editorRecipeUrl(photoId, versionId), {}, {toast: false})
        .then(/** @param {{active?:unknown;local_mask_stale?:boolean}} d */ function(d) {
          applyLoadedLocalStaleness(
            photoId, seq, localStaleSavedRecipeSeq, d
          );
        }).catch(function() {});
    }
    /** @type {HTMLElement} */ (document.getElementById('editorFilename')).textContent = photo.filename || ('Photo ' + photoId);
    var dim = photo.width && photo.height ? photo.width + ' x ' + photo.height : 'Photo ' + photoId;
    /** @type {HTMLElement} */ (document.getElementById('editorSubhead')).textContent = dim + (editorState.version ? ' · ' + editorState.version.name : ' · Original');
    updateFeedbackControls();
    setEditorLoading(false);
    syncControls();
    updatePreview();
    loadHistory(photoId);
    loadEditorVersions();
  } catch (e) {
    if (seq !== editorState.loadSeq) return;
    // Keep the editing surface frozen (editor-loading) on failure: the stage
    // still shows the previous photo's pixels, the history panel still lists
    // its checkpoints, and the recipe state was reset above — re-enabling the
    // panels would let a slider tweak build a recipe from scratch and save it
    // over this photo's real saved recipe. Prev/Next/Back in the topbar stay
    // usable to retry or leave.
    /** @type {HTMLElement} */ (document.getElementById('editorFilename')).textContent = 'Photo unavailable';
    setStatus(editorErrorMessage(e) || 'Could not load photo', true);
  }
}

async function initEditor() {
  setEditorLoading(true);
  var pending = pendingCropRatioPreference();
  if (pending) cropRatioPreference = pending;
  try {
    /** @type {EditorPendingCropRatio} */ var saved = await safeFetch('/api/editor/crop-ratio', {}, {toast: false});
    // Another tab can acknowledge and clear the shared pending slot while
    // this GET is in flight. Keep the newer evidence captured before it.
    var latestPending = pendingCropRatioPreference();
    if (latestPending && (!pending || latestPending.revision > pending.revision)) {
      pending = latestPending;
    }
    if (pending && pending.revision > (saved.revision || 0)) {
      cropRatioPreference = pending;
      saveCropRatioPreference(pending);
    } else {
      cropRatioPreference = saved;
      acknowledgeCropRatioPreference(saved.revision || 0);
    }
    // Seed the shared committed key so any other tab open on this origin can
    // see the authoritative revision even before its next PUT round-trips.
    writeCommittedCropRatioPreference(cropRatioPreference);
    // Then adopt any newer committed value that arrived from another tab
    // while this GET was in flight.
    adoptCommittedCropRatioPreference();
  } catch (_) {
    showToast('Could not load your remembered crop ratio.', 'error');
  }
  window.addEventListener('storage', function(event) {
    if (event.key !== COMMITTED_CROP_RATIO_KEY) return;
    // A sibling editor tab persisted a newer preference. Reflect it in
    // memory so subsequent Next/Prev navigation opens photos with the
    // updated resizing constraint without needing a reload.
    adoptCommittedCropRatioPreference();
  });
  setEditorLoading(false);
  initCropDrag();
  initEditorKeyboard();
  initEditorContextMenu();
  initUnloadGuard();
  buildLocalControls();
  clearHistogramFeedback();
  setZoomMode('fit');
  loadPresets();
  if (window.vireoEditNav) editorState.navIds = window.vireoEditNav.getList() || [];
  editorState.baseNavIds = (editorState.navIds || []).slice();
  var match = window.location.pathname.match(/\/edit\/(\d+)/);
  var photoId = match
    ? Number(match[1])
    : (window.vireoEditNav ? window.vireoEditNav.getLastPhoto() : null);
  if (!photoId) {
    editorEmptyState();
    return;
  }
  await loadPhoto(photoId, {skipUrl: !!match, versionId: new URLSearchParams(window.location.search).get('version')});
}

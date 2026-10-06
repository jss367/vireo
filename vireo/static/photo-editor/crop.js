// Crop box, crop fields, rotate/flip/straighten, aspect ratios, and committing a crop.
// Classic page script; load boot.js after all definitions.

function renderCropBox(forceInputs) {
  var img = document.getElementById('editorImg');
  var box = document.getElementById('editorCropBox');
  if (!box) return;
  var crop = editorState.showBefore
    ? Object.assign({x: 0, y: 0, w: 1, h: 1}, editorState.savedRecipe.crop || {})
    : ensureCrop(editorState.recipe);
  var showCropBox = editorState.cropEditing && !editorState.showBefore &&
    editorImageMatchesZoomRecipe(img);
  box.style.display = showCropBox ? '' : 'none';
  var editBtn = document.getElementById('editCropBtn');
  if (editBtn) {
    editBtn.disabled = !!editorState.loading || !!editorState.showBefore || editorState.cropEditing;
    editBtn.classList.toggle('active', editorState.cropEditing);
  }
  ['x', 'y', 'w', 'h'].forEach(function(key) {
    var input = document.getElementById('crop' + key.toUpperCase());
    if (!input) return;
    // While the user is typing in a field, don't overwrite it from a stray
    // redraw — but a forced pass (after commit) snaps it to the clamped value.
    if (forceInputs || document.activeElement !== input) {
      input.value = String(Math.round(crop[key] * 1000) / 10);
    }
    input.disabled = !!editorState.showBefore;
  });
  if (!showCropBox || !img || !img.complete || !img.clientWidth || !img.clientHeight) return;
  box.style.left = (crop.x * img.clientWidth) + 'px';
  box.style.top = (crop.y * img.clientHeight) + 'px';
  box.style.width = (crop.w * img.clientWidth) + 'px';
  box.style.height = (crop.h * img.clientHeight) + 'px';
}

function activateCropEditing() {
  if (editorState.loading || editorState.showBefore || editorState.cropEditing) return false;
  editorState.cropEditing = true;
  editorState.zoomMode = 'fit';
  updateEditorZoomControl();
  renderCropBox();
  return true;
}

function beginCropEdit() {
  if (!activateCropEditing()) return;
  updatePreview();
}

function setCropField(key, raw) {
  if (editorState.loading || editorState.showBefore) return;
  var reopenedCrop = activateCropEditing();
  var v = Number(raw);
  if (!Number.isFinite(v)) { renderCropBox(true); return; }
  v = Math.max(0, Math.min(100, v)) / 100;
  var crop = cloneRecipe({crop: ensureCrop(editorState.recipe)}).crop;
  crop[key] = v;
  if (editorState.cropAspect && (key === 'w' || key === 'h')) {
    var img = document.getElementById('editorImg');
    var dims = editorNativeRecipeDimensions(editorState.recipe || {}, false);
    var useLoadedImage = img && img.clientWidth && img.clientHeight &&
      editorImageMatchesZoomRecipe(img);
    var imageWidth = useLoadedImage ? img.clientWidth : (dims && dims.width);
    var imageHeight = useLoadedImage ? img.clientHeight : (dims && dims.height);
    if (imageWidth && imageHeight) {
      var k = editorState.cropAspect * imageHeight / imageWidth;
      // Cap the edited dimension so the paired one still fits in the frame.
      // Otherwise clampCrop would clamp only the overflowing side (w=100 on
      // a 1:1 lock over a 2:1 landscape → h clipped to 1, w stays 1) and
      // silently break the lock while the ratio button remains active.
      if (key === 'w') {
        crop.w = Math.min(crop.w, k);
        crop.h = crop.w / k;
      } else {
        crop.h = Math.min(crop.h, 1 / k);
        crop.w = crop.h * k;
      }
    }
  }
  editorState.recipe.crop = clampCrop(crop);
  markChanged(reopenedCrop);
  renderCropBox(true);
}

function currentCropAspect() {
  var img = document.getElementById('editorImg');
  var dims = editorNativeRecipeDimensions(editorState.recipe || {}, false);
  var useLoadedImage = img && img.clientWidth && img.clientHeight &&
    editorImageMatchesZoomRecipe(img);
  var imageWidth = useLoadedImage ? img.clientWidth : (dims && dims.width);
  var imageHeight = useLoadedImage ? img.clientHeight : (dims && dims.height);
  if (!imageWidth || !imageHeight) return null;
  var crop = ensureCrop(editorState.recipe);
  var aspect = (crop.w * imageWidth) / (crop.h * imageHeight);
  return Number.isFinite(aspect) && aspect > 0 ? aspect : null;
}

function cropBoxForAspect(crop, normalizedRatio) {
  var current = clampCrop(crop);
  if (!Number.isFinite(normalizedRatio) || normalizedRatio <= 0) return current;
  var centerX = current.x + current.w / 2;
  var centerY = current.y + current.h / 2;
  var w = current.w;
  var h = current.h;
  // Fit the requested ratio inside the crop the user most recently shaped.
  // This keeps its center and avoids restoring discarded pixels except when
  // the crop must grow to satisfy the editor's minimum selectable size.
  if (w / h > normalizedRatio) w = h * normalizedRatio;
  else h = w / normalizedRatio;
  // clampCrop enforces a 2% floor on each axis independently. Scale both
  // dimensions together first so that floor cannot break the selected ratio.
  var minScale = Math.max(1, 0.02 / w, 0.02 / h);
  w *= minScale;
  h *= minScale;
  return clampCrop({
    x: centerX - w / 2,
    y: centerY - h / 2,
    w: w,
    h: h,
  });
}

function markChanged(render) {
  if (editorState.loading) return;
  recordEditorHistory();
  var wasShowingBefore = editorState.showBefore;
  if (wasShowingBefore) editorState.showBefore = false;
  updateFeedbackControls();
  updateDirtyState();
  renderCropBox();
  if (render || wasShowingBefore) schedulePreview();
  else {
    // Crop-only edits (resetCrop, setAspect, drag pointermove) pass
    // render=false, so the preview isn't rescheduled — but the histogram
    // samples inside the crop, and the mask overlay depends on the current
    // crop for its feather scale, so both must refresh anyway.
    scheduleHistogramRefresh();
    if (editorState.maskOverlay) scheduleMaskOverlayRefresh();
  }
}

function rotateRecipe(delta) {
  var crop = ensureCrop(editorState.recipe);
  var current = Number(editorState.recipe.rotation || 0);
  var next = (current + delta) % 360;
  if (next < 0) next += 360;
  editorState.recipe.rotation = next;
  editorState.recipe.crop = transformCropForRotation(crop, delta, editorState.recipe.flip || {});
  if (editorState.cropAspect && ((Math.abs(delta) % 180) !== 0)) {
    // A 90° rotation turns a 3:2 box into 2:3 — the lock no longer matches
    // any button, so release it rather than silently hold an invisible ratio.
    editorState.cropAspect = null;
    updateAspectButtons();
  }
  markChanged(true);
}

function flipRecipe(axis) {
  var crop = ensureCrop(editorState.recipe);
  var flip = editorState.recipe.flip || {};
  flip[axis] = !flip[axis];
  editorState.recipe.flip = flip;
  editorState.recipe.crop = transformCropForFlip(crop, axis);
  markChanged(true);
}

function setStraighten(value) {
  var v = Number(value);
  if (!Number.isFinite(v)) v = 0;
  v = Math.max(-45, Math.min(45, v));
  editorState.recipe.straighten = v;
  document.getElementById('straightenValue').textContent = v.toFixed(1);
  markChanged(true);
}

function updateAspectButtons() {
  document.getElementById('rememberCropRatio').checked = cropRatioPreference.enabled;
  var map = { aspect32Btn: 1.5, aspect43Btn: 1.3333333333, aspect11Btn: 1 };
  setButtonActive('aspectLockBtn', !!editorState.cropAspect);
  setButtonDisabled('aspectLockBtn', !!editorState.showBefore || !!editorState.loading);
  Object.keys(map).forEach(function(id) {
    setButtonActive(id, Math.abs(Number(editorState.cropAspect || 0) - map[id]) < 0.0001);
    setButtonDisabled(id, !!editorState.showBefore || !!editorState.loading);
  });
}

function resetCrop() {
  var reopenedCrop = activateCropEditing();
  editorState.cropAspect = null;
  rememberCropRatio();
  updateAspectButtons();
  editorState.recipe.crop = { x: 0, y: 0, w: 1, h: 1 };
  markChanged(reopenedCrop);
}

function setAspect(aspect) {
  if (editorState.loading || editorState.showBefore) return;
  var reopenedCrop = activateCropEditing();
  var img = document.getElementById('editorImg');
  var dims = editorNativeRecipeDimensions(editorState.recipe || {}, false);
  var useLoadedImage = !reopenedCrop && img && img.clientWidth &&
    img.clientHeight && editorImageMatchesZoomRecipe(img);
  var imageWidth = useLoadedImage ? img.clientWidth : (dims && dims.width);
  var imageHeight = useLoadedImage ? img.clientHeight : (dims && dims.height);
  if (!imageWidth || !imageHeight) return;
  if (Math.abs(Number(editorState.cropAspect || 0) - aspect) < 0.0001) {
    // Second click on the active ratio unlocks it and leaves the crop alone.
    editorState.cropAspect = null;
    rememberCropRatio();
    updateAspectButtons();
    if (reopenedCrop) updatePreview();
    return;
  }
  editorState.cropAspect = aspect;
  rememberCropRatio();
  updateAspectButtons();
  var imageAspect = imageWidth / imageHeight;
  var normalizedRatio = aspect / imageAspect;
  editorState.recipe.crop = cropBoxForAspect(
    ensureCrop(editorState.recipe), normalizedRatio
  );
  markChanged(reopenedCrop);
}

function toggleAspectLock() {
  if (editorState.loading || editorState.showBefore) return;
  var reopenedCrop = activateCropEditing();
  if (editorState.cropAspect) {
    editorState.cropAspect = null;
  } else {
    var aspect = currentCropAspect();
    if (!aspect) return;
    editorState.cropAspect = aspect;
  }
  rememberCropRatio();
  updateAspectButtons();
  if (reopenedCrop) updatePreview();
}

function commitCropView() {
  var saveBtn = document.getElementById('saveBtn');
  if (saveBtn && !saveBtn.disabled) {
    saveRecipe();
    return;
  }
  // Re-entering an existing saved crop does not make the recipe dirty. Enter
  // still means "accept this crop", so return to the fitted committed view
  // without issuing a redundant history-writing PUT.
  if (editorState.cropEditing && recipeForSave(editorState.recipe).crop) {
    editorState.cropEditing = false;
    editorState.zoomMode = 'fit';
    syncControls();
    updatePreview();
  }
}

// Zoom levels, fit/actual sizing, orientation, and the before/after toggle.
// Classic page script; load boot.js after all definitions.

var EDITOR_MAX_ZOOM_PERCENT = 400;

function editorZoomRecipe() {
  return editorState.showBefore ? editorState.savedRecipe : editorState.recipe;
}

function editorPreviewAppliesCrop() {
  return editorState.showBefore || !editorState.cropEditing;
}

function editorImageMatchesZoomRecipe(img) {
  if (!img || !editorState.photoId) return false;
  try {
    var url = new URL(img.currentSrc || img.src, window.location.href);
    if (url.pathname !== '/photos/' + editorState.photoId + '/edit-preview') {
      return false;
    }
    var loadedRecipe = JSON.parse(url.searchParams.get('recipe') || '{}');
    var loadedAppliesCrop = url.searchParams.get('apply_crop') === '1';
    return loadedAppliesCrop === editorPreviewAppliesCrop() &&
      recipeKey(loadedRecipe) === recipeKey(previewRecipeFor(
        editorZoomRecipe() || {}, loadedAppliesCrop
      ));
  } catch (_) {
    return false;
  }
}

function editorMetadataOrientation(metadata) {
  if (!metadata || typeof metadata !== 'object') return null;
  var groups = ['EXIF', 'IFD0', 'TIFF', 'File'];
  for (var i = 0; i < groups.length; i++) {
    var group = metadata[groups[i]];
    if (group && Object.prototype.hasOwnProperty.call(group, 'Orientation')) {
      return group.Orientation;
    }
  }
  if (Object.prototype.hasOwnProperty.call(metadata, 'Orientation')) {
    return metadata.Orientation;
  }
  return null;
}

function editorOrientationSwapsAxes(orientation) {
  if (orientation == null || typeof orientation === 'boolean') return false;
  if (typeof orientation === 'number') {
    return [5, 6, 7, 8].indexOf(Math.round(orientation)) !== -1;
  }
  var text = String(orientation).trim().toLowerCase();
  if (!text) return false;
  var parsed = Number(text);
  if (Number.isFinite(parsed)) {
    return [5, 6, 7, 8].indexOf(Math.round(parsed)) !== -1;
  }
  return text.indexOf('90') !== -1 || text.indexOf('270') !== -1;
}

function editorNativeRecipeDimensions(recipe, applyCrop) {
  var photo = editorState.photo || {};
  var width = Number(photo.width) || 0;
  var height = Number(photo.height) || 0;
  if (!width || !height) return null;
  // Stored dimensions use the sensor axes, while load_image applies EXIF
  // orientation before the editor's recipe. Match the server's
  // recipe_source_dimensions contract before applying rotation and crop.
  if (editorOrientationSwapsAxes(editorMetadataOrientation(photo.metadata))) {
    var exifSwap = width;
    width = height;
    height = exifSwap;
  }
  var rotation = Number((recipe || {}).rotation || 0);
  rotation = ((rotation % 360) + 360) % 360;
  if (rotation === 90 || rotation === 270) {
    var swap = width;
    width = height;
    height = swap;
  }
  var crop = applyCrop && recipe && recipe.crop;
  if (crop && !isFullCrop(crop)) {
    width *= Number(crop.w) || 1;
    height *= Number(crop.h) || 1;
  }
  return {width: width, height: height};
}

function editorNativeDisplayDimensions() {
  var recipe = editorZoomRecipe() || {};
  var dims = editorNativeRecipeDimensions(recipe, editorPreviewAppliesCrop());
  var width = dims ? dims.width : 0;
  var height = dims ? dims.height : 0;
  if (!width || !height) {
    var fallback = document.getElementById('editorImg');
    return fallback && fallback.naturalWidth && fallback.naturalHeight
      ? {width: fallback.naturalWidth, height: fallback.naturalHeight}
      : null;
  }
  // Straightening can alter the rendered aspect slightly. Preserve the
  // source's native long edge while following the preview's authoritative
  // post-transform aspect ratio once it is available.
  var img = document.getElementById('editorImg');
  if (img && img.complete && img.naturalWidth && img.naturalHeight &&
      editorImageMatchesZoomRecipe(img)) {
    var nativeLong = Math.max(width, height);
    var loadedSize = 0;
    try {
      loadedSize = Number(new URL(img.currentSrc || img.src, window.location.href)
        .searchParams.get('size')) || 0;
    } catch (_) {}
    // If the complete native tier was delivered, it is the authority for
    // 1:1. For sources above the render safety cap, the capped preview is only
    // the pixel source: keep the metadata long edge below so zoom continues
    // scaling toward the native 100% size.
    if (nativeLong <= 16384 && loadedSize >= nativeLong) {
      return {width: img.naturalWidth, height: img.naturalHeight};
    }
    var longEdge = Math.max(width, height);
    var ratio = img.naturalWidth / img.naturalHeight;
    if (ratio >= 1) {
      width = longEdge;
      height = longEdge / ratio;
    } else {
      height = longEdge;
      width = longEdge * ratio;
    }
  }
  return {width: width, height: height};
}

function editorFitZoomPercent() {
  var wrap = document.getElementById('editorCanvasWrap');
  var dims = editorNativeDisplayDimensions();
  if (!wrap || !dims || !dims.width || !dims.height) return 100;
  var style = getComputedStyle(wrap);
  var availableW = wrap.clientWidth - (parseFloat(style.paddingLeft) || 0) -
    (parseFloat(style.paddingRight) || 0);
  var availableH = wrap.clientHeight - (parseFloat(style.paddingTop) || 0) -
    (parseFloat(style.paddingBottom) || 0);
  if (availableW <= 0 || availableH <= 0) return 100;
  return Math.max(0.1, Math.min(100, availableW / dims.width * 100,
    availableH / dims.height * 100));
}

function editorZoomSliderPosition(percent) {
  var fit = editorFitZoomPercent();
  if (!Number.isFinite(percent) || percent <= fit) return 0;
  if (EDITOR_MAX_ZOOM_PERCENT <= fit) return 0;
  return Math.max(0, Math.min(1000,
    Math.log(percent / fit) / Math.log(EDITOR_MAX_ZOOM_PERCENT / fit) * 1000));
}

function editorZoomFromSliderPosition(position) {
  var pos = Math.max(0, Math.min(1000, Number(position) || 0));
  if (pos <= 0) return null;
  var fit = editorFitZoomPercent();
  return fit * Math.exp(Math.log(EDITOR_MAX_ZOOM_PERCENT / fit) * pos / 1000);
}

function editorZoomText() {
  return editorState.zoomMode === 'fit'
    ? 'Fit (' + Math.round(editorFitZoomPercent()) + '%)'
    : Math.round(editorState.zoomPercent) + '%';
}

function editorIsActualZoom() {
  return editorState.zoomMode !== 'fit' &&
    Math.abs(editorState.zoomPercent - 100) < 0.5;
}

function editorCanDragToPan() {
  return editorState.zoomMode !== 'fit';
}

function editorClampCustomZoomToFit() {
  if (editorState.zoomMode === 'fit') return false;
  var fit = editorFitZoomPercent();
  if (editorState.zoomPercent >= fit) return false;
  editorState.zoomPercent = fit;
  // A retained percentage that no longer fills the viewport is Fit, both
  // visually and semantically. Exact equality that the user selected is
  // still kept as a custom zoom.
  editorState.zoomMode = 'fit';
  return true;
}

function editorFitUsesNativeRender() {
  var dims = editorNativeRecipeDimensions(
    editorZoomRecipe() || {}, editorPreviewAppliesCrop()
  );
  var native = dims ? Math.max(dims.width, dims.height) : 0;
  return native > 0 && editorFitRenderSize() >= Math.min(16384, native);
}

function updateEditorZoomControl() {
  var slider = document.getElementById('editorZoomSlider');
  var fitStop = document.getElementById('fitBtn');
  var actualStop = document.getElementById('actualBtn');
  var zoomOut = document.getElementById('editorZoomOut');
  var zoomIn = document.getElementById('editorZoomIn');
  var value = document.getElementById('editorZoomValue');
  var hasPhoto = !!editorState.photoId && !!editorState.photo;
  var disabled = !hasPhoto || !!editorState.loading;
  var fit = editorFitZoomPercent();
  var combinedFitActual = fit >= 100 && editorFitUsesNativeRender();
  var percent = editorState.zoomMode === 'fit' ? fit : editorState.zoomPercent;
  var text = editorZoomText();

  if (slider) {
    slider.value = String(editorState.zoomMode === 'fit'
      ? 0 : Math.round(editorZoomSliderPosition(percent)));
    slider.setAttribute('aria-valuetext', text);
    slider.disabled = disabled;
  }
  if (fitStop) {
    fitStop.textContent = combinedFitActual ? 'Fit · 100%' : 'Fit';
    fitStop.classList.toggle('active', editorState.zoomMode === 'fit');
    fitStop.disabled = disabled;
  }
  if (actualStop) {
    actualStop.style.display = combinedFitActual ? 'none' : '';
    actualStop.style.left = Math.max(9, editorZoomSliderPosition(100) / 10) + '%';
    actualStop.classList.toggle('active', editorIsActualZoom());
    actualStop.disabled = disabled;
  }
  if (zoomOut) {
    zoomOut.disabled = disabled || editorState.zoomMode === 'fit';
  }
  if (zoomIn) {
    zoomIn.disabled = disabled || (editorState.zoomMode !== 'fit' &&
      editorState.zoomPercent >= EDITOR_MAX_ZOOM_PERCENT - 0.5);
  }
  if (value) value.textContent = text;
}

function applyEditorZoom() {
  var wrap = document.getElementById('editorCanvasWrap');
  var img = document.getElementById('editorImg');
  if (!wrap || !img) return;
  editorClampCustomZoomToFit();
  var custom = editorState.zoomMode !== 'fit';
  var wasCustom = wrap.classList.contains('zoom-custom');
  var focusX = 0.5;
  var focusY = 0.5;
  if (wasCustom && img.clientWidth && img.clientHeight) {
    var oldImgRect = img.getBoundingClientRect();
    var oldWrapRect = wrap.getBoundingClientRect();
    focusX = (oldWrapRect.left + wrap.clientWidth / 2 - oldImgRect.left) /
      oldImgRect.width;
    focusY = (oldWrapRect.top + wrap.clientHeight / 2 - oldImgRect.top) /
      oldImgRect.height;
    focusX = Math.max(0, Math.min(1, focusX));
    focusY = Math.max(0, Math.min(1, focusY));
  }
  wrap.classList.toggle('zoom-custom', custom);
  // Keep the semantic 1:1 state separate from the broader custom-zoom
  // layout. Panning and existing integrations use this marker to distinguish
  // the exact 100% stop from intermediate slider values.
  wrap.classList.toggle('zoom-actual', editorIsActualZoom());
  if (!custom) {
    var fitDims = editorNativeDisplayDimensions();
    var fitPercent = editorFitZoomPercent();
    if (fitDims) {
      img.style.width = (fitDims.width * fitPercent / 100) + 'px';
      img.style.height = (fitDims.height * fitPercent / 100) + 'px';
    }
    wrap.scrollLeft = 0;
    wrap.scrollTop = 0;
  } else {
    var dims = editorNativeDisplayDimensions();
    if (dims) {
      img.style.width = (dims.width * editorState.zoomPercent / 100) + 'px';
      img.style.height = (dims.height * editorState.zoomPercent / 100) + 'px';
      // Keep the point at the middle of the viewport stable as the slider
      // moves. Entering zoom from Fit begins at the image centre instead of
      // jumping to its top-left corner.
      var newImgRect = img.getBoundingClientRect();
      var newWrapRect = wrap.getBoundingClientRect();
      wrap.scrollLeft += newImgRect.left + focusX * newImgRect.width -
        (newWrapRect.left + wrap.clientWidth / 2);
      wrap.scrollTop += newImgRect.top + focusY * newImgRect.height -
        (newWrapRect.top + wrap.clientHeight / 2);
    }
  }
  renderCropBox();
  positionMaskOverlay();
}

function setEditorZoom(percent) {
  var wasMode = editorState.zoomMode;
  var wasPercent = editorState.zoomPercent;
  if (percent === null || percent === 'fit') {
    editorState.zoomMode = 'fit';
  } else {
    var numeric = Number(percent);
    if (!Number.isFinite(numeric)) return;
    editorState.zoomMode = 'custom';
    editorState.zoomPercent = Math.max(editorFitZoomPercent(),
      Math.min(EDITOR_MAX_ZOOM_PERCENT, numeric));
  }
  applyEditorZoom();
  updateEditorZoomControl();
  var changed = wasMode !== editorState.zoomMode ||
    wasPercent !== editorState.zoomPercent;
  if (changed && editorState.photoId && !editorState.loading) schedulePreview();
}

function setEditorZoomFromSlider(position) {
  setEditorZoom(editorZoomFromSliderPosition(position));
}

function setEditorZoomToFit() {
  setEditorZoom(null);
}

function setEditorZoomToActual() {
  setEditorZoom(100);
}

function stepEditorZoom(direction) {
  var fit = editorFitZoomPercent();
  var current = editorState.zoomMode === 'fit' ? fit : editorState.zoomPercent;
  var next = current * (direction > 0 ? 1.25 : 0.8);
  if (direction < 0 && next <= fit * 1.02) setEditorZoomToFit();
  else setEditorZoom(next);
}

// Backward-compatible entry point for the old two-button control and any
// saved browser automation that invokes it directly.
function setZoomMode(mode) {
  if (mode === 'actual') setEditorZoomToActual();
  else setEditorZoomToFit();
}

function toggleBeforePreview() {
  editorState.showBefore = !editorState.showBefore;
  updateFeedbackControls();
  updatePreview();
}

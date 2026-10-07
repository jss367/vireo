// Brush strokes publish new immutable snapshots; recipes and history retain refs.
var maskBrush = {mode: null, stroke: null, busy: false, sequence: 0};

function maskBrushStatus(text) {
  document.getElementById('maskBrushStatus').textContent = text;
}

function cancelMaskBrush() {
  maskBrush.sequence++;
  maskBrush.mode = null;
  maskBrush.stroke = null;
  maskBrush.busy = false;
  document.getElementById('maskBrushCanvas').style.display = 'none';
  syncMaskBrushButtons();
}

function syncMaskBrushButtons() {
  ['add', 'subtract'].forEach(function(mode) {
    var button = document.getElementById(mode === 'add' ? 'maskBrushAdd' : 'maskBrushSubtract');
    button.classList.toggle('active', maskBrush.mode === mode);
    button.setAttribute('aria-pressed', String(maskBrush.mode === mode));
    button.disabled = maskBrush.busy;
  });
  document.getElementById('editorCanvasWrap').classList.toggle('mask-brushing', !!maskBrush.mode);
  if (window.renderHistoryControls) window.renderHistoryControls();
}

async function setMaskBrushMode(mode) {
  if (maskBrush.busy || editorState.loading) return;
  cancelMaskBrush();
  if (!mode) { maskBrushStatus('Mask corrections stay with your edits. Save Changes to keep them.'); return; }
  cancelPointColorPicker();
  if (editorState.showBefore) toggleBeforePreview();
  var sequence = maskBrush.sequence;
  var loadSeq = editorState.loadSeq;
  maskBrush.busy = true;
  syncMaskBrushButtons();
  maskBrushStatus('Preparing subject mask…');
  try {
    await ensureLocalMask();
    if (sequence !== maskBrush.sequence || loadSeq !== editorState.loadSeq) return;
    maskBrush.mode = mode;
    editorState.maskOverlay = true;
    setButtonActive('maskOverlayBtn', true);
    refreshMaskOverlay();
    maskBrushStatus('Drag on the photo to ' + (mode === 'add' ? 'add to' : 'subtract from') + ' the subject. Hold Space to pan. Escape finishes.');
  } catch (_) {
    if (sequence === maskBrush.sequence) maskBrushStatus('Could not prepare the subject mask. Try again.');
  } finally {
    if (sequence === maskBrush.sequence) { maskBrush.busy = false; syncMaskBrushButtons(); }
  }
}

// Invert crop, straighten, flip, then clockwise quarter-turn rotation. These
// coordinates refer to the EXIF-oriented source, exactly as the mask does.
function maskBrushSourcePoint(x, y, recipe, applyCrop, native) {
  var rotation = Number(recipe.rotation || 0);
  var w = native.width, h = native.height;
  if (rotation === 90 || rotation === 270) { w = native.height; h = native.width; }
  if (applyCrop && recipe.crop) {
    x = recipe.crop.x + x * recipe.crop.w;
    y = recipe.crop.y + y * recipe.crop.h;
  }
  var angle = Number(recipe.straighten || 0) * Math.PI / 180;
  var px = (x - 0.5) * w, py = (y - 0.5) * h;
  x = (Math.cos(angle) * px + Math.sin(angle) * py) / w + 0.5;
  y = (-Math.sin(angle) * px + Math.cos(angle) * py) / h + 0.5;
  if ((recipe.flip || {}).horizontal) x = 1 - x;
  if ((recipe.flip || {}).vertical) y = 1 - y;
  if (rotation === 90) return [y, 1 - x];
  if (rotation === 180) return [1 - x, 1 - y];
  if (rotation === 270) return [1 - y, x];
  return [x, y];
}

function addMaskBrushPoint(event) {
  var stroke = maskBrush.stroke;
  if (!stroke || stroke.points.length >= 2048) return;
  var rect = stroke.rect;
  var x = (event.clientX - rect.left) / rect.width;
  var y = (event.clientY - rect.top) / rect.height;
  if (x < 0 || x > 1 || y < 0 || y > 1) return;
  var point = maskBrushSourcePoint(x, y, stroke.recipe, stroke.applyCrop, stroke.native);
  if (point.some(function(v) { return v < 0 || v > 1; })) return;
  var last = stroke.points[stroke.points.length - 1];
  if (last && Math.hypot((point[0] - last[0]) * stroke.native.width,
      (point[1] - last[1]) * stroke.native.height) < stroke.radius * Math.min(stroke.native.width, stroke.native.height) / 3) return;
  stroke.points.push(point);
  var canvas = document.getElementById('maskBrushCanvas');
  var ctx = canvas.getContext('2d');
  ctx.strokeStyle = ctx.fillStyle = stroke.mode === 'add' ? 'rgba(80,210,160,0.65)' : 'rgba(255,110,100,0.65)';
  ctx.lineWidth = stroke.screenSize;
  ctx.lineCap = 'round';
  var cx = x * canvas.width, cy = y * canvas.height;
  if (stroke.lastScreen) {
    ctx.beginPath(); ctx.moveTo(stroke.lastScreen[0], stroke.lastScreen[1]); ctx.lineTo(cx, cy); ctx.stroke();
  }
  ctx.beginPath(); ctx.arc(cx, cy, stroke.screenSize / 2, 0, 2 * Math.PI); ctx.fill();
  stroke.lastScreen = [cx, cy];
}

async function finishMaskBrush(event) {
  var stroke = maskBrush.stroke;
  if (!stroke || stroke.pointerId !== event.pointerId) return;
  maskBrush.stroke = null;
  var wrap = document.getElementById('editorCanvasWrap');
  if (wrap.hasPointerCapture(event.pointerId)) wrap.releasePointerCapture(event.pointerId);
  if (event.type !== 'pointerup' || !stroke.points.length) {
    document.getElementById('maskBrushCanvas').style.display = 'none';
    syncMaskBrushButtons();
    return;
  }
  var sequence = ++maskBrush.sequence;
  var photoId = editorState.photoId;
  var loadSeq = editorState.loadSeq;
  var recipeAtStart = recipeKey(editorState.recipe);
  maskBrush.busy = true;
  syncMaskBrushButtons();
  maskBrushStatus('Applying mask correction…');
  try {
    // Feather-only edits and zeroing the last local adjustment deliberately
    // release an unreferenced snapshot. Brush mode can outlive that snapshot,
    // so reacquire it before publishing rather than sending a null mask.
    var mask = await ensureLocalMask();
    if (sequence !== maskBrush.sequence || loadSeq !== editorState.loadSeq) return;
    if (recipeAtStart !== recipeKey(editorState.recipe)) {
      maskBrushStatus('Edits changed while preparing the mask. Paint the stroke again.');
      return;
    }
    var data = await safeFetch('/api/photos/' + photoId + '/local-mask/correct', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({mask: mask, mode: stroke.mode,
        radius: stroke.radius, points: stroke.points}),
    }, {toast: false});
    if (sequence !== maskBrush.sequence || loadSeq !== editorState.loadSeq) return;
    if (recipeAtStart !== recipeKey(editorState.recipe)) {
      maskBrushStatus('Edits changed while applying the stroke. Paint it again.');
      return;
    }
    finishEditorHistoryGesture();
    editorState.localMask = data.mask;
    rebuildLocalSection();
    markChanged(true);
    refreshMaskOverlay();
    maskBrushStatus('Mask corrected. Undo restores the previous stroke. Save Changes to keep it.');
  } catch (error) {
    if (sequence === maskBrush.sequence) maskBrushStatus(error.message || 'Could not apply mask correction. Try again.');
  } finally {
    if (sequence === maskBrush.sequence) {
      maskBrush.busy = false;
      document.getElementById('maskBrushCanvas').style.display = 'none';
      syncMaskBrushButtons();
    }
  }
}

(function bindMaskBrush() {
  var wrap = document.getElementById('editorCanvasWrap');
  wrap.addEventListener('pointerdown', function(event) {
    if (!maskBrush.mode || editorState.spacePan || event.button !== 0) return;
    event.preventDefault(); event.stopImmediatePropagation();
    if (maskBrush.busy || editorState.loading || editorState.showBefore) return;
    var img = document.getElementById('editorImg');
    if (!img.complete || !img.naturalWidth || !editorImageMatchesZoomRecipe(img)) {
      maskBrushStatus('Wait for the current photo preview, then paint.'); return;
    }
    var rect = img.getBoundingClientRect();
    if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) return;
    var native = editorNativeRecipeDimensions({}, false);
    if (!native) return;
    var recipe = cloneRecipe(editorState.recipe);
    var applyCrop = editorPreviewAppliesCrop();
    var dims = editorNativeRecipeDimensions(recipe, applyCrop);
    var screenSize = Number(document.getElementById('maskBrushSize').value);
    var radius = Math.max(0.001, Math.min(0.25, screenSize / 2 * dims.width / rect.width / Math.min(native.width, native.height)));
    screenSize = radius * 2 * Math.min(native.width, native.height) * rect.width / dims.width;
    var canvas = document.getElementById('maskBrushCanvas');
    canvas.width = Math.ceil(rect.width); canvas.height = Math.ceil(rect.height);
    canvas.style.width = rect.width + 'px'; canvas.style.height = rect.height + 'px';
    canvas.style.display = '';
    maskBrush.stroke = {pointerId: event.pointerId, mode: maskBrush.mode,
      rect: rect, recipe: recipe, applyCrop: applyCrop, native: native,
      radius: radius, screenSize: screenSize, points: []};
    wrap.setPointerCapture(event.pointerId);
    addMaskBrushPoint(event);
    syncMaskBrushButtons();
  }, true);
  wrap.addEventListener('pointermove', function(event) {
    if (!maskBrush.stroke || maskBrush.stroke.pointerId !== event.pointerId) return;
    event.preventDefault(); event.stopImmediatePropagation(); addMaskBrushPoint(event);
  }, true);
  ['pointerup', 'pointercancel', 'lostpointercapture'].forEach(function(type) {
    wrap.addEventListener(type, finishMaskBrush, true);
  });
  document.addEventListener('keydown', function(event) {
    if (event.key === 'Escape' && maskBrush.mode) {
      cancelMaskBrush(); event.preventDefault(); event.stopImmediatePropagation();
    }
  }, true);
  window.addEventListener('blur', function() { if (maskBrush.stroke) cancelMaskBrush(); });
})();

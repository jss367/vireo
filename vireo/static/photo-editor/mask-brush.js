// Brush strokes publish new immutable snapshots; recipes and history retain refs.
var maskBrush = {mode: null, stroke: null, busy: false, sequence: 0, pointer: null, erase: false};

function maskBrushFootprint(img) {
  var rect = img.getBoundingClientRect();
  var native = editorNativeRecipeDimensions({}, false);
  var dims = editorNativeRecipeDimensions(editorState.recipe, editorPreviewAppliesCrop());
  if (!native || !dims || !rect.width) return null;
  var size = Number(document.getElementById('maskBrushSize').value);
  var radius = Math.max(0.001, Math.min(0.25, size / 2 * dims.width / rect.width / Math.min(native.width, native.height)));
  return {rect: rect, native: native, radius: radius,
    screenSize: radius * 2 * Math.min(native.width, native.height) * rect.width / dims.width};
}

function updateMaskBrushCursor() {
  var cursor = document.getElementById('maskBrushCursor');
  var pointer = maskBrush.pointer;
  var img = document.getElementById('editorImg');
  cursor.hidden = true;
  if (!maskBrush.mode || !pointer || maskBrush.busy || editorState.loading ||
      editorState.showBefore || editorState.spacePan || !img.naturalWidth ||
      document.querySelector('.modal-overlay.open')) return;
  var footprint = maskBrush.stroke || maskBrushFootprint(img);
  if (!footprint) return;
  var rect = footprint.rect;
  var wrap = document.getElementById('editorCanvasWrap').getBoundingClientRect();
  if (pointer.x < Math.max(rect.left, wrap.left) || pointer.x > Math.min(rect.right, wrap.right) ||
      pointer.y < Math.max(rect.top, wrap.top) || pointer.y > Math.min(rect.bottom, wrap.bottom)) return;
  var softness = maskBrush.stroke ? maskBrush.stroke.softness : Number(document.getElementById('maskBrushSoftness').value) / 100;
  var mode = maskBrush.stroke ? maskBrush.stroke.mode : (maskBrush.erase ? 'subtract' : maskBrush.mode);
  cursor.style.left = pointer.x + 'px'; cursor.style.top = pointer.y + 'px';
  cursor.style.width = cursor.style.height = footprint.screenSize + 'px';
  cursor.style.setProperty('--brush-soft-inset', (softness * 50) + '%');
  cursor.classList.toggle('subtract', mode === 'subtract');
  cursor.hidden = false;
}

function updateMaskBrushControls() {
  ['Size', 'Softness', 'Strength'].forEach(function(name) {
    document.getElementById('maskBrush' + name + 'Value').textContent =
      document.getElementById('maskBrush' + name).value + (name === 'Size' ? '' : '%');
  });
  updateMaskBrushCursor();
}

function maskBrushStatus(text) {
  document.getElementById('maskBrushStatus').textContent = text;
}

function cancelMaskBrush() {
  var stroke = maskBrush.stroke;
  maskBrush.sequence++;
  maskBrush.mode = null;
  maskBrush.stroke = null;
  maskBrush.busy = false;
  maskBrush.erase = false;
  maskBrush.pointer = null;
  var wrap = document.getElementById('editorCanvasWrap');
  if (stroke && wrap.hasPointerCapture(stroke.pointerId)) wrap.releasePointerCapture(stroke.pointerId);
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
  updateMaskBrushCursor();
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
    maskBrushStatus('Drag to ' + (mode === 'add' ? 'add to' : 'subtract from') + ' the subject. Space pans. Escape finishes.');
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
  // The colored path is a stroke guide; the server renders the soft mask on
  // release. Opacity is applied to the whole canvas, never accumulated per dab.
  ctx.strokeStyle = ctx.fillStyle = stroke.mode === 'add' ? '#50d2a0' : '#ff6e64';
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
        radius: stroke.radius, points: stroke.points,
        softness: stroke.softness, strength: stroke.strength}),
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
    var footprint = maskBrushFootprint(img);
    if (!footprint) return;
    var native = footprint.native;
    var recipe = cloneRecipe(editorState.recipe);
    var applyCrop = editorPreviewAppliesCrop();
    var screenSize = footprint.screenSize;
    var radius = footprint.radius;
    var softness = Number(document.getElementById('maskBrushSoftness').value) / 100;
    var strength = Number(document.getElementById('maskBrushStrength').value) / 100;
    var canvas = document.getElementById('maskBrushCanvas');
    canvas.width = Math.ceil(rect.width); canvas.height = Math.ceil(rect.height);
    canvas.style.width = rect.width + 'px'; canvas.style.height = rect.height + 'px';
    canvas.style.display = '';
    canvas.style.opacity = 0.65 * strength;
    maskBrush.erase = event.altKey;
    maskBrush.pointer = {x: event.clientX, y: event.clientY};
    maskBrush.stroke = {pointerId: event.pointerId, mode: event.altKey ? 'subtract' : maskBrush.mode,
      rect: rect, recipe: recipe, applyCrop: applyCrop, native: native,
      radius: radius, screenSize: screenSize, points: [], softness: softness, strength: strength};
    wrap.setPointerCapture(event.pointerId);
    addMaskBrushPoint(event);
    syncMaskBrushButtons();
  }, true);
  wrap.addEventListener('pointermove', function(event) {
    maskBrush.pointer = {x: event.clientX, y: event.clientY};
    maskBrush.erase = event.altKey;
    updateMaskBrushCursor();
    if (!maskBrush.stroke || maskBrush.stroke.pointerId !== event.pointerId) return;
    event.preventDefault(); event.stopImmediatePropagation(); addMaskBrushPoint(event);
  }, true);
  wrap.addEventListener('pointerleave', function() {
    maskBrush.pointer = null; updateMaskBrushCursor();
  });
  wrap.addEventListener('scroll', updateMaskBrushCursor);
  window.addEventListener('resize', updateMaskBrushCursor);
  ['pointerup', 'pointercancel', 'lostpointercapture'].forEach(function(type) {
    wrap.addEventListener(type, finishMaskBrush, true);
  });
  document.addEventListener('keydown', function(event) {
    if (!maskBrush.mode) return;
    // Escape finishes painting even from a text field; an open dialog still
    // owns Escape. Ignore only the focus target when checking that boundary.
    if (event.key === 'Escape' && editorHistoryOwnsEvent({target: null})) {
      cancelMaskBrush(); event.preventDefault(); event.stopImmediatePropagation();
      return;
    }
    if (!editorHistoryOwnsEvent(event)) return;
    if (event.key === 'Alt') { maskBrush.erase = true; updateMaskBrushCursor(); }
    if (event.code === 'Space') document.getElementById('maskBrushCursor').hidden = true;
    if (event.metaKey || event.ctrlKey || event.altKey || maskBrush.stroke || maskBrush.busy) return;
    if (event.key === '[' || event.key === ']') {
      var size = document.getElementById('maskBrushSize');
      size.value = Math.max(Number(size.min), Math.min(Number(size.max), Number(size.value) + (event.key === '[' ? -4 : 4)));
      updateMaskBrushControls();
      event.preventDefault(); event.stopImmediatePropagation();
    }
  }, true);
  document.addEventListener('keyup', function(event) {
    if (event.key === 'Alt') { maskBrush.erase = false; updateMaskBrushCursor(); }
    if (event.code === 'Space') setTimeout(updateMaskBrushCursor, 0);
  }, true);
  window.addEventListener('blur', function() {
    maskBrush.erase = false; maskBrush.pointer = null;
    if (maskBrush.stroke) cancelMaskBrush();
    else updateMaskBrushCursor();
  });
})();

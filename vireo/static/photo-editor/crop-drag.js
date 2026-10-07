// Pointer dragging of crop handles and panning the zoomed image.
// Classic page script; load boot.js after all definitions.

function initCropDrag() {
  var box = document.getElementById('editorCropBox');
  var wrap = document.getElementById('editorCanvasWrap');

  function updatePanCursor() {
    wrap.classList.toggle('space-pan', editorState.spacePan);
    wrap.classList.toggle('panning', !!editorState.pan);
    if (typeof updateMaskBrushCursor === 'function') updateMaskBrushCursor();
  }

  function startPan(e) {
    if (editorState.loading || editorState.pan || e.button !== 0) return;
    // Leave touch scrolling to the browser; this gesture fills the missing
    // mouse interaction without replacing the trackpad/touch behavior.
    if (e.pointerType && e.pointerType !== 'mouse') return;
    editorState.pan = {
      pointerId: e.pointerId,
      startX: e.clientX,
      startY: e.clientY,
      scrollLeft: wrap.scrollLeft,
      scrollTop: wrap.scrollTop,
    };
    wrap.setPointerCapture(e.pointerId);
    updatePanCursor();
    e.preventDefault();
  }

  function movePan(e) {
    var pan = editorState.pan;
    if (!pan || e.pointerId !== pan.pointerId) return;
    wrap.scrollLeft = pan.scrollLeft - (e.clientX - pan.startX);
    wrap.scrollTop = pan.scrollTop - (e.clientY - pan.startY);
    e.preventDefault();
  }

  function finishPan(e) {
    var pan = editorState.pan;
    if (!pan || (e && e.pointerId != null && e.pointerId !== pan.pointerId)) return;
    editorState.pan = null;
    if (wrap.hasPointerCapture && wrap.hasPointerCapture(pan.pointerId)) {
      wrap.releasePointerCapture(pan.pointerId);
    }
    updatePanCursor();
  }

  window.setEditorSpacePan = function(active) {
    editorState.spacePan = !!active;
    updatePanCursor();
  };

  wrap.addEventListener('pointerdown', function(e) {
    if (editorCanDragToPan() || editorState.spacePan) startPan(e);
  });
  wrap.addEventListener('pointermove', movePan);
  wrap.addEventListener('pointerup', finishPan);
  wrap.addEventListener('pointercancel', finishPan);
  wrap.addEventListener('lostpointercapture', finishPan);

  box.addEventListener('pointerdown', function(e) {
    if (editorState.loading || e.button !== 0) return;
    var img = document.getElementById('editorImg');
    if (!img || !img.clientWidth || !img.clientHeight) return;
    // pointerdown is prevented below so the crop gesture does not select or
    // drag the underlying image. That also suppresses the browser's normal
    // focus change, however, which can leave a search/number input active.
    // In that state Enter belongs to the stale input instead of the editor's
    // "Save Changes" shortcut. Explicitly focus the crop surface when the
    // gesture starts so Enter commits the crop the user just adjusted.
    try { box.focus({preventScroll: true}); }
    catch (_) { box.focus(); }
    var handle = e.target && e.target.dataset ? e.target.dataset.handle : '';
    // At any zoom above Fit, dragging the photo pans it. Corner handles keep
    // resizing the crop, unless Space is held as an explicit pan override.
    if (editorState.spacePan || (editorCanDragToPan() && !handle)) return;
    editorState.drag = {
      handle: handle || 'move',
      startX: e.clientX,
      startY: e.clientY,
      crop: cloneRecipe({crop: ensureCrop(editorState.recipe)}).crop,
      imgW: img.clientWidth,
      imgH: img.clientHeight,
    };
    box.setPointerCapture(e.pointerId);
    e.preventDefault();
    e.stopPropagation();
  });
  document.addEventListener('pointermove', function(e) {
    var drag = editorState.drag;
    if (!drag) return;
    var dx = (e.clientX - drag.startX) / drag.imgW;
    var dy = (e.clientY - drag.startY) / drag.imgH;
    var c = cloneRecipe({crop: drag.crop}).crop;
    var min = 0.02;
    if (drag.handle === 'move') {
      c.x += dx;
      c.y += dy;
    } else {
      var west = drag.handle.indexOf('w') !== -1;
      var north = drag.handle.indexOf('n') !== -1;
      // The corner opposite the dragged handle stays fixed.
      var anchorX = west ? drag.crop.x + drag.crop.w : drag.crop.x;
      var anchorY = north ? drag.crop.y + drag.crop.h : drag.crop.y;
      c.w += west ? -dx : dx;
      c.h += north ? -dy : dy;
      // Locked ratio (3:2 / 4:3 / 1:1 buttons) converted from displayed
      // pixels into the normalized 0..1 crop space.
      var k = editorState.cropAspect
        ? editorState.cropAspect * drag.imgH / drag.imgW
        : 0;
      if (k > 0) {
        // The axis the pointer moved most drives the other.
        if (Math.abs(dx) >= Math.abs(dy)) c.h = c.w / k;
        else c.w = c.h * k;
        // Cap against the image edges on the anchor's side so clampCrop
        // below can't knock the box back off-ratio.
        c.w = Math.min(c.w, west ? anchorX : 1 - anchorX,
                       (north ? anchorY : 1 - anchorY) * k);
        c.w = Math.max(c.w, min, min * k);
        c.h = c.w / k;
      } else {
        if (c.w < min) c.w = min;
        if (c.h < min) c.h = min;
      }
      c.x = west ? anchorX - c.w : anchorX;
      c.y = north ? anchorY - c.h : anchorY;
    }
    editorState.recipe.crop = clampCrop(c);
    markChanged(false);
    e.preventDefault();
  });
  document.addEventListener('pointerup', function() {
    editorState.drag = null;
  });
  // pointercancel (touch interrupted, pointer grabbed by the browser) ends
  // the gesture without a pointerup; without this the drag stays armed and
  // the next stray pointermove keeps resizing the crop.
  document.addEventListener('pointercancel', function() {
    editorState.drag = null;
  });
  window.addEventListener('resize', function() {
    editorClampCustomZoomToFit();
    applyEditorZoom();
    updateEditorZoomControl();
    // Fit can cross into a larger source-render bucket when the viewport
    // grows. The debounced preview update is a no-op when the URL is already
    // correct, so also schedule it when no custom-zoom clamp was needed.
    if (editorState.photoId && !editorState.loading) {
      schedulePreview();
    }
    renderCropBox();
    updateHistogramFeedback();
    positionMaskOverlay();
  });
  window.addEventListener('blur', function() {
    window.setEditorSpacePan(false);
    finishPan();
  });
}

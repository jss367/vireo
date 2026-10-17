// 1:1 zoom on the stage: layout, toggle, and pointer drag panning.
// Classic page script; load boot.js after all definitions.

function resetRapidZoom() {
  rapid.zoomed = false;
  rapid.zoomPhotoId = null;
  rapid.zoomAnchor = null;
  rapid.zoomDrag = null;
  rapid.suppressZoomClick = false;
  var stage = document.getElementById('photoStage');
  var img = document.getElementById('currentPhoto');
  if (stage) {
    stage.classList.remove('zoomed', 'dragging');
    stage.scrollLeft = 0;
    stage.scrollTop = 0;
  }
  if (img) {
    img.style.width = '';
    img.style.height = '';
  }
}

function applyRapidZoomLayout() {
  var p = currentPhoto();
  var stage = document.getElementById('photoStage');
  var img = document.getElementById('currentPhoto');
  var shouldZoom = !!(p && rapid.zoomed && rapid.zoomPhotoId === p.id);
  stage.classList.toggle('zoomed', shouldZoom);
  if (!shouldZoom) {
    img.style.width = '';
    img.style.height = '';
    stage.scrollLeft = 0;
    stage.scrollTop = 0;
    return;
  }
  if (img.complete && img.naturalWidth && img.naturalHeight) {
    img.style.width = img.naturalWidth + 'px';
    img.style.height = img.naturalHeight + 'px';
    if (rapid.zoomAnchor) {
      var anchor = rapid.zoomAnchor;
      rapid.zoomAnchor = null;
      requestAnimationFrame(function() {
        stage.scrollLeft = Math.max(0, anchor.x * img.offsetWidth - stage.clientWidth / 2);
        stage.scrollTop = Math.max(0, anchor.y * img.offsetHeight - stage.clientHeight / 2);
      });
    }
  }
}

function rapidImageLoaded() {
  applyRapidZoomLayout();
}

function toggleRapidZoom(e) {
  if (e) e.stopPropagation();
  if (rapid.suppressZoomClick) {
    rapid.suppressZoomClick = false;
    return;
  }
  var p = currentPhoto();
  if (!p) return;
  if (rapid.zoomed && rapid.zoomPhotoId === p.id) {
    resetRapidZoom();
    renderCurrent();
    return;
  }
  var img = document.getElementById('currentPhoto');
  var rect = img.getBoundingClientRect();
  var anchorX = 0.5;
  var anchorY = 0.5;
  if (e && typeof e.clientX === 'number' && typeof e.clientY === 'number') {
    anchorX = rect.width ? Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width)) : 0.5;
    anchorY = rect.height ? Math.max(0, Math.min(1, (e.clientY - rect.top) / rect.height)) : 0.5;
  }
  rapid.zoomed = true;
  rapid.zoomPhotoId = p.id;
  rapid.zoomAnchor = {
    x: anchorX,
    y: anchorY,
  };
  renderCurrent();
}

function rapidZoomPointerDown(e) {
  var p = currentPhoto();
  if (!p || !rapid.zoomed || rapid.zoomPhotoId !== p.id || e.button !== 0) return;
  var stage = document.getElementById('photoStage');
  rapid.zoomDrag = {
    pointerId: e.pointerId,
    startX: e.clientX,
    startY: e.clientY,
    scrollLeft: stage.scrollLeft,
    scrollTop: stage.scrollTop,
    moved: false,
  };
  stage.classList.add('dragging');
  e.currentTarget.setPointerCapture(e.pointerId);
}

function rapidZoomPointerMove(e) {
  if (!rapid.zoomDrag) return;
  var stage = document.getElementById('photoStage');
  var dx = e.clientX - rapid.zoomDrag.startX;
  var dy = e.clientY - rapid.zoomDrag.startY;
  if (!rapid.zoomDrag.moved && (Math.abs(dx) > 3 || Math.abs(dy) > 3)) {
    rapid.zoomDrag.moved = true;
  }
  if (rapid.zoomDrag.moved) {
    stage.scrollLeft = rapid.zoomDrag.scrollLeft - dx;
    stage.scrollTop = rapid.zoomDrag.scrollTop - dy;
  }
}

function rapidZoomPointerUp(e) {
  if (!rapid.zoomDrag) return;
  var moved = rapid.zoomDrag.moved;
  rapid.zoomDrag = null;
  document.getElementById('photoStage').classList.remove('dragging');
  if (moved) rapid.suppressZoomClick = true;
  try {
    e.currentTarget.releasePointerCapture(e.pointerId);
  } catch (_) {}
}

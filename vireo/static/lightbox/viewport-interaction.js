/* The viewport's lifecycle: what a new photo inherits, the wheel, drag and
 * resize listeners, and what closing retires.
 */
import { applyTransform, clampPan, recomputeNativeZoom } from './viewport-geometry.js';
import { cancelRestore, cloneView, saveView, viewForOpen } from './viewport-views.js';
import { applyPendingOneToOne, setZoom } from './viewport-zoom.js';
import { setPopoverOpen } from './viewport-zoom-control.js';

var DRAG_THRESHOLD = 5;

export function beginPhoto(/** @type {LightboxViewportContext} */ vp, /** @type {number} */ photoId, /** @type {LightboxOpenOptions | undefined} */ openOptions) {
  openOptions = openOptions || {};
  var fallbackViewportState = openOptions.fallbackViewportState || null;
  var eyeTrackAnchor = openOptions.eyeTrackAnchor || null;
  var preserveOneToOne = !!openOptions.preserveOneToOne;
  var restoreViewportState = viewForOpen(vp, photoId, fallbackViewportState);
  if (!restoreViewportState && preserveOneToOne) {
    restoreViewportState = {
      zoom: Math.max(1.0, vp.zoom || vp.nativeZoom || 1.0),
      centerX: 0.5,
      centerY: 0.5,
      oneToOne: true,
      pending1To1: true,
    };
  }
  if (preserveOneToOne && restoreViewportState) {
    restoreViewportState.oneToOne = true;
    restoreViewportState.pending1To1 = true;
  }
  var restoreWantsOneToOne = !!(
    restoreViewportState &&
    (restoreViewportState.oneToOne || restoreViewportState.pending1To1)
  );
  if (preserveOneToOne && restoreWantsOneToOne && restoreViewportState && restoreViewportState.zoom <= 1.001) {
    restoreViewportState.zoom = Math.max(
      1.002,
      fallbackViewportState && fallbackViewportState.zoom || 1.0,
      vp.zoom || 1.0,
      vp.nativeZoom || 1.0
    );
  }
  var transitionZoom = restoreViewportState
    ? Math.max(1.0, restoreViewportState.zoom || 1.0)
    : 1.0;

  vp.dragging = false;
  vp.didDrag = false;
  // Carry a resize's intent across navigation, but never apply the outgoing
  // timer against the incoming photo's metadata.
  if (vp.resizeTimer !== null) {
    vp.window.clearTimeout(vp.resizeTimer);
    vp.resizeTimer = null;
    vp.deferredRefreshPending = true;
  }
  vp.zoom = transitionZoom;
  vp.panX = 0;
  vp.panY = 0;
  vp.nativeZoom = null;
  vp.pendingOneToOne = restoreWantsOneToOne;
  vp.pendingOneToOneAnchor = null;
  vp.pendingRestore = cloneView(restoreViewportState);
  vp.pendingEye = (vp.options.photo().trackEyeEnabled && eyeTrackAnchor) ? {
    photoId: photoId,
    offsetX: Number(eyeTrackAnchor.offsetX) || 0,
    offsetY: Number(eyeTrackAnchor.offsetY) || 0
  } : null;
  start(vp);
  return Object.freeze({zoom: transitionZoom, oneToOne: restoreWantsOneToOne});
}

export function clearEyeTracking(/** @type {LightboxViewportContext} */ vp) {
  vp.pendingEye = null;
  vp.eyeAnchor = null;
}

export function cancelPendingZoom(/** @type {LightboxViewportContext} */ vp) {
  vp.pendingOneToOne = false;
  vp.pendingOneToOneAnchor = null;
  vp.pendingRestore = null;
}

export function resetForEdit(/** @type {LightboxViewportContext} */ vp) {
  vp.dragging = false;
  vp.zoom = 1;
  vp.panX = 0;
  vp.panY = 0;
  vp.nativeZoom = null;
  cancelPendingZoom(vp);
}

function onWheel(/** @type {LightboxViewportContext} */ vp, /** @type {WheelEvent} */ e) {
  if (!(e.target instanceof Element)) return;
  var wrap = vp.document.getElementById('lightboxWrap');
  if (!vp.active || !wrap || (!wrap.contains(e.target) && e.target !== wrap)) return;
  if (e.target.closest('.lightbox-zoom-control')) return;
  e.preventDefault();
  if (vp.options.photo().transitionPending) return;
  var scale = e.ctrlKey ? 0.02 : 0.0015;
  cancelRestore(vp);
  setZoom(vp, vp.zoom * Math.exp(-e.deltaY * scale), e.clientX, e.clientY);
}

function scheduleLayoutRefresh(/** @type {LightboxViewportContext} */ vp, /** @type {boolean} */ updateSourceTier) {
  if (!vp.active) return;
  vp.refreshSourceTier = vp.refreshSourceTier || !!updateSourceTier;
  if (vp.resizeTimer !== null) vp.window.clearTimeout(vp.resizeTimer);
  var timer = vp.window.setTimeout(function() {
    if (!vp.active || vp.resizeTimer !== timer) return;
    vp.resizeTimer = null;
    var shouldUpdateSourceTier = vp.refreshSourceTier;
    vp.refreshSourceTier = false;
    // A resize must not lay out the frozen outgoing bitmap with incoming dimensions.
    if (vp.options.photo().transitionPending) {
      vp.refreshSourceTier = shouldUpdateSourceTier || vp.refreshSourceTier;
      vp.deferredRefreshPending = true;
      return;
    }
    recomputeNativeZoom(vp);
    // Re-clamping during a deferred 1:1 must preserve both intent and its anchor.
    var pendingDesiredSource = vp.options.photo().desiredSrcKey;
    if (vp.pendingOneToOne) {
      clampPan(vp);
      applyTransform(vp);
      if (shouldUpdateSourceTier) {
        if (!pendingDesiredSource || pendingDesiredSource === vp.options.photo().currentSrcKey) {
          vp.options.scheduleSource(vp.nativeZoom || 4);
        } else {
          vp.options.keepSource(pendingDesiredSource);
        }
        if (vp.options.photo().desiredSrcKey === vp.options.photo().currentSrcKey) {
          applyPendingOneToOne(vp);
        }
      }
      return;
    }
    if (shouldUpdateSourceTier) {
      setZoom(vp, vp.zoom, null, null);
    } else {
      clampPan(vp);
      applyTransform(vp);
      saveView(vp, vp.options.photoId());
    }
  }, 100);
  vp.resizeTimer = timer;
}

export function flushDeferredLayout(/** @type {LightboxViewportContext} */ vp) {
  if (!vp.deferredRefreshPending) return;
  vp.deferredRefreshPending = false;
  scheduleLayoutRefresh(vp, false);
}

function onMouseDown(/** @type {LightboxViewportContext} */ vp, /** @type {MouseEvent} */ e) {
  if (!(e.target instanceof Element)) return;
  if (!vp.active || e.button !== 0 || vp.options.photo().transitionPending) return;
  var wrap = vp.document.getElementById('lightboxWrap');
  if (!wrap || vp.zoom <= 1.001) return;
  if (e.target.tagName === 'BUTTON' || e.target.closest('button')) return;
  if (e.target.closest('.lightbox-zoom-control')) return;
  if (!wrap.contains(e.target) && e.target !== wrap) return;
  vp.dragging = true;
  vp.didDrag = false;
  vp.dragStartX = e.clientX;
  vp.dragStartY = e.clientY;
  vp.panStartX = vp.panX;
  vp.panStartY = vp.panY;
  e.preventDefault();
}

function onMouseMove(/** @type {LightboxViewportContext} */ vp, /** @type {MouseEvent} */ e) {
  if (!vp.active || !vp.dragging) return;
  var dx = e.clientX - vp.dragStartX;
  var dy = e.clientY - vp.dragStartY;
  if (!vp.didDrag && (Math.abs(dx) > DRAG_THRESHOLD || Math.abs(dy) > DRAG_THRESHOLD)) {
    vp.didDrag = true;
    cancelRestore(vp);
  }
  vp.panX = vp.panStartX + dx;
  vp.panY = vp.panStartY + dy;
  clampPan(vp);
  applyTransform(vp);
}

function onMouseUp(/** @type {LightboxViewportContext} */ vp) {
  if (!vp.active || !vp.dragging) return;
  vp.dragging = false;
  if (!vp.didDrag && vp.zoom > 1.001) {
    cancelRestore(vp);
    setZoom(vp, 1, null, null);
    vp.options.handledClick();
  } else if (vp.didDrag) {
    saveView(vp, vp.options.photoId());
  }
}

// One set of listener functions per controller, so close() removes exactly the
// functions start() added.
function listenersFor(/** @type {LightboxViewportContext} */ vp) {
  if (!vp.listeners) {
    vp.listeners = {
      wheel: function(e) { onWheel(vp, e); },
      mousedown: function(e) { onMouseDown(vp, e); },
      mousemove: function(e) { onMouseMove(vp, e); },
      mouseup: function() { onMouseUp(vp); },
      resize: function() { scheduleLayoutRefresh(vp, true); },
    };
  }
  return vp.listeners;
}

function start(/** @type {LightboxViewportContext} */ vp) {
  if (vp.active) return;
  vp.active = true;
  var on = listenersFor(vp);
  vp.document.addEventListener('wheel', on.wheel, {passive: false});
  vp.document.addEventListener('mousedown', on.mousedown);
  vp.document.addEventListener('mousemove', on.mousemove);
  vp.document.addEventListener('mouseup', on.mouseup);
  vp.window.addEventListener('resize', on.resize);
  var wrap = vp.document.getElementById('lightboxWrap');
  if (vp.window.ResizeObserver && wrap) {
    var observer = new vp.window.ResizeObserver(function() {
      if (vp.layoutObserver === observer) scheduleLayoutRefresh(vp, false);
    });
    vp.layoutObserver = observer;
    observer.observe(wrap);
  }
}

export function close(/** @type {LightboxViewportContext} */ vp) {
  var wasActive = vp.active;
  vp.active = false;
  vp.dragging = false;
  vp.didDrag = false;
  if (vp.resizeTimer !== null) vp.window.clearTimeout(vp.resizeTimer);
  vp.resizeTimer = null;
  vp.refreshSourceTier = false;
  vp.deferredRefreshPending = false;
  if (vp.layoutObserver) vp.layoutObserver.disconnect();
  vp.layoutObserver = null;
  if (wasActive) {
    var on = listenersFor(vp);
    vp.document.removeEventListener('wheel', on.wheel);
    vp.document.removeEventListener('mousedown', on.mousedown);
    vp.document.removeEventListener('mousemove', on.mousemove);
    vp.document.removeEventListener('mouseup', on.mouseup);
    vp.window.removeEventListener('resize', on.resize);
  }
  resetForEdit(vp);
  cancelRestore(vp);
  var wrap = vp.document.getElementById('lightboxWrap');
  if (wrap) wrap.classList.remove('zoomed');
  setPopoverOpen(vp, false);
}

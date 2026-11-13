/* Per-photo saved views, the view restored after navigation, and eye-tracking
 * alignment. A view is a zoom plus the normalized image point at the center of
 * the wrap, so it survives a change of source tier or window size.
 */
import { applyTransform, clampPan, isOneToOne, layoutMetrics } from './viewport-geometry.js';
import { zoomLimit } from './viewport-zoom-control.js';

export function cloneView(/** @type {LightboxViewportState | null | undefined} */ state) {
  if (!state) return null;
  return {
    zoom: Number(state.zoom) || 1.0,
    centerX: Number(state.centerX),
    centerY: Number(state.centerY),
    oneToOne: !!state.oneToOne,
    pending1To1: !!state.pending1To1,
  };
}

export function currentView(/** @type {LightboxViewportContext} */ vp) {
  var metrics = layoutMetrics(vp);
  var state = {
    zoom: vp.zoom || 1.0,
    centerX: 0.5,
    centerY: 0.5,
    oneToOne: isOneToOne(vp),
    pending1To1: !!vp.pendingOneToOne,
  };
  var wrap = vp.document.getElementById('lightboxWrap');
  var t = vp.document.getElementById('lightboxTransform');
  if (metrics && wrap && t) {
    var wrapRect = wrap.getBoundingClientRect();
    var rect = t.getBoundingClientRect();
    var scale = metrics.scale || 1;
    if (scale > 0 && metrics.w && metrics.h) {
      var imgX = (wrapRect.left + wrapRect.width / 2 - rect.left) / scale;
      var imgY = (wrapRect.top + wrapRect.height / 2 - rect.top) / scale;
      state.centerX = Math.max(0, Math.min(1, imgX / metrics.w));
      state.centerY = Math.max(0, Math.min(1, imgY / metrics.h));
    }
  }
  return state;
}

export function captureEyeAnchor(/** @type {LightboxViewportContext} */ vp) {
  if (!vp.options.photo().trackEyeEnabled || (vp.zoom <= 1.001 && !vp.pendingOneToOne)) return null;
  // During rapid navigation the DOM can still contain the outgoing bitmap.
  // Reuse the last trustworthy screen anchor instead of measuring that bitmap
  // against the incoming photo id.
  if (vp.options.photo().transitionPending) {
    return vp.eyeAnchor
      ? { offsetX: vp.eyeAnchor.offsetX, offsetY: vp.eyeAnchor.offsetY }
      : null;
  }
  var photo = vp.options.photoData(vp.options.photoId());
  var point = vp.options.eyePoint(vp.options.photoId(), photo);
  var overlaysAvailable = vp.options.overlaysAvailable();
  var metrics = layoutMetrics(vp);
  var state = point && overlaysAvailable ? currentView(vp) : null;
  if (!point || !overlaysAvailable || !metrics || !state) {
    // Current frame is unusable for a fresh measurement — either its metadata
    // has resolved without an eye, the active source cannot align overlays
    // (JPEG pair or geometric edit), the destination metadata is not yet
    // cached, or the layout has not been measured. Reuse the last trustworthy
    // anchor so a later eyed frame can still resume alignment across the
    // paused sequence. User pan/zoom paths clear the cached anchor via
    // cancelRestore, so a manual viewport change on this
    // paused frame will not carry a stale eye offset to the next eyed photo.
    return vp.eyeAnchor
      ? { offsetX: vp.eyeAnchor.offsetX, offsetY: vp.eyeAnchor.offsetY }
      : null;
  }
  var anchor = {
    offsetX: (point.x - state.centerX) * metrics.w * metrics.scale,
    offsetY: (point.y - state.centerY) * metrics.h * metrics.scale,
  };
  vp.eyeAnchor = anchor;
  return { offsetX: anchor.offsetX, offsetY: anchor.offsetY };
}

export function applyPendingEye(/** @type {LightboxViewportContext} */ vp, /** @type {LightboxPhoto | null | undefined} */ photo) {
  var pending = vp.pendingEye;
  if (!pending || !vp.options.photo().trackEyeEnabled) return false;
  if (String(pending.photoId) !== String(vp.options.photoId())) return false;
  photo = photo || vp.options.photoData(vp.options.photoId());
  // A missing object means the detail request has not resolved yet. Keep the
  // alignment armed so its callback can apply it after the image is laid out.
  // Navigation lists on some pages contain only {id, filename}; those are
  // also "unknown", not evidence that the destination lacks an eye.
  if (!photo || !Object.prototype.hasOwnProperty.call(photo, 'eye_x')) return false;
  var point = vp.options.eyePoint(vp.options.photoId(), photo);
  var metrics = layoutMetrics(vp);
  if (!point || !vp.options.overlaysAvailable()) {
    vp.pendingEye = null;
    vp.options.updateEyeControl();
    return false;
  }
  if (!metrics || vp.options.photo().transitionPending) return false;

  // A deferred 1:1 snap is still armed — the loader is holding the current
  // display upscaled while a sharper source (2560/3840 or /original) swap
  // completes. Applying the viewport here would re-enter
  // applyView(vp), whose one-to-one branch would either clear
  // vp.pendingOneToOne (when nativeZoom is known) or leave it armed but whose
  // trailing vp.options.scheduleSource() can retarget back to /full when
  // /original is unavailable and nativeZoom is not yet known — cancelling
  // the sharp fallback and leaving the user on a soft view. Wait for the
  // deferred source swap to land and applyPendingOneToOne(vp) to
  // finish before aligning the eye.
  if (vp.pendingOneToOne) return false;

  var state = currentView(vp);
  if (!state || vp.zoom <= 1.001 || !metrics.scale) return false;
  state.centerX = point.x - (Number(pending.offsetX) || 0) / (metrics.w * metrics.scale);
  state.centerY = point.y - (Number(pending.offsetY) || 0) / (metrics.h * metrics.scale);
  applyView(vp, state);

  // Native 1:1 can be learned after the first layout pass. Retain the pending
  // alignment until then so a later 1:1 snap does not move the eye.
  if (!state.oneToOne || vp.nativeZoom) {
    vp.pendingEye = null;
  }
  vp.eyeAnchor = {
    offsetX: Number(pending.offsetX) || 0,
    offsetY: Number(pending.offsetY) || 0,
  };
  saveView(vp, vp.options.photoId());
  vp.options.updateEyeControl();
  return true;
}

export function saveView(/** @type {LightboxViewportContext} */ vp, /** @type {number | null} */ photoId) {
  if (photoId == null) return null;
  // Mid-navigation the DOM transform still belongs to the outgoing photo but
  // vp.options.photoId() has already advanced to the incoming id. Reading from
  // the DOM here would misattribute the frozen bitmap to the incoming photo
  // and stomp its intended inspection point. Prefer the pending restore
  // state (what handleInitialImageLoad is about to apply), and otherwise
  // leave any previously saved state alone.
  if (vp.options.photo().transitionPending && String(photoId) === String(vp.options.photoId())) {
    var pending = vp.pendingRestore;
    if (pending) {
      var pendingClone = cloneView(pending);
      vp.savedViews[String(photoId)] = pendingClone;
      return cloneView(pendingClone);
    }
    var existing = vp.savedViews[String(photoId)];
    return existing ? cloneView(existing) : null;
  }
  var state = currentView(vp);
  vp.savedViews[String(photoId)] = state;
  return cloneView(state);
}

export function viewForOpen(/** @type {LightboxViewportContext} */ vp, /** @type {number} */ photoId, /** @type {LightboxViewportState | null} */ fallbackState) {
  // Arrow navigation supplies the viewport from the photo being left. Treat
  // that handoff as the current browse-session viewport, even when the target
  // photo has an older cached viewport from an earlier visit. This keeps zoom
  // continuous while moving left/right: zooming back out on one photo also
  // means a previously visited photo opens zoomed out when navigating back.
  if (fallbackState) {
    return cloneView(fallbackState);
  }
  var key = String(photoId);
  if (Object.prototype.hasOwnProperty.call(vp.savedViews, key)) {
    return cloneView(vp.savedViews[key]);
  }
  return null;
}

export function applyView(/** @type {LightboxViewportContext} */ vp, /** @type {LightboxViewportState | null} */ state) {
  state = cloneView(state);
  if (!state) return false;
  var metrics = layoutMetrics(vp);
  if (!metrics) return false;
  var wrap = vp.document.getElementById('lightboxWrap');
  if (!wrap) return false;

  var maxZoom = zoomLimit(vp);
  var targetZoom = state.zoom || 1.0;
  if (state.oneToOne) {
    if (vp.nativeZoom) {
      targetZoom = vp.nativeZoom;
      vp.pendingOneToOne = false;
    } else {
      vp.pendingOneToOne = true;
      targetZoom = Math.max(1.0, targetZoom);
    }
  } else {
    vp.pendingOneToOne = false;
  }
  targetZoom = Math.max(1.0, Math.min(maxZoom, targetZoom));
  vp.zoom = targetZoom;

  if (vp.zoom <= 1.001) {
    vp.panX = 0;
    vp.panY = 0;
  } else {
    var scale = vp.fitScale * vp.zoom;
    var centerX = Number.isFinite(state.centerX) ? state.centerX : 0.5;
    var centerY = Number.isFinite(state.centerY) ? state.centerY : 0.5;
    centerX = Math.max(0, Math.min(1, centerX));
    centerY = Math.max(0, Math.min(1, centerY));
    var baseLeft = (metrics.wrapW - metrics.w * scale) / 2;
    var baseTop = (metrics.wrapH - metrics.h * scale) / 2;
    vp.panX = (metrics.wrapW / 2) - (centerX * metrics.w * scale) - baseLeft;
    vp.panY = (metrics.wrapH / 2) - (centerY * metrics.h * scale) - baseTop;
    clampPan(vp);
  }

  wrap.classList.toggle('zoomed', vp.zoom > 1.001);
  applyTransform(vp);
  vp.options.scheduleSource();
  return true;
}

export function cancelRestore(/** @type {LightboxViewportContext} */ vp) {
  vp.options.cancelProgressiveLoad();
  // Called from user-driven viewport mutations (wheel/keyboard/click zoom,
  // drag pan) so a still-armed restore carried over by arrow navigation
  // cannot be reapplied by a later async metadata/image-load callback and
  // stomp the user's manual zoom/pan. Also drops any deferred eye-tracking
  // alignment for the same reason: when a destination image finishes
  // loading before /api/photos/<id> returns, vp.pendingEye stays
  // armed until the metadata callback fires applyPendingEye,
  // and a user pan/zoom in that window must not be overwritten by the
  // previous photo's eye anchor. Dropping the cached screen anchor here
  // — instead of eagerly on every known no-eye frame — lets tracking
  // resume across an eyed → no-eye → eyed sequence when the user did not
  // pan or zoom the paused frame, while still preventing a stale offset
  // from being carried onto the next eyed photo after a manual pan/zoom
  // on the paused frame. Programmatic restore paths
  // (applyPendingRestore, applyPendingEye) own
  // clearing their own state and must not route through here.
  vp.pendingRestore = null;
  vp.pendingEye = null;
  vp.eyeAnchor = null;
}

export function applyPendingRestore(/** @type {LightboxViewportContext} */ vp) {
  if (!vp.pendingRestore) return false;
  var state = vp.pendingRestore;
  if (!applyView(vp, state)) return false;
  // Keep the pending state until native zoom is known. Until then
  // applyView clamps with a fallback max of 4, so a saved
  // zoom > 4 would be permanently degraded if cleared here and never
  // retried once the image load event establishes vp.nativeZoom.
  if (vp.nativeZoom) {
    vp.pendingRestore = null;
  }
  return true;
}

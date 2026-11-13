/* Zoom changes: the anchored zoom itself, the source tier each zoom needs, the
 * deferred 1:1 that waits for a sharp source, and the user's zoom commands.
 */
import { applyTransform, clampPan, layoutMetrics, recomputeNativeZoom } from './viewport-geometry.js';
import { cancelRestore, saveView } from './viewport-views.js';
import { zoomFromSliderPosition, zoomLimit } from './viewport-zoom-control.js';

export function _lbSrcRank(/** @type {string | null} */ key) {
  // Tier ordering by available source resolution (higher = sharper).
  if (key === 'original') return 3;
  if (key === '3840') return 2;
  if (key === '2560') return 1;
  return 0; // 'full', '1920', or unknown
}

function syncSourceForZoom(/** @type {LightboxViewportContext} */ vp, /** @type {number} */ targetZoom, /** @type {boolean | undefined} */ preserveSharperSource = undefined) {
  var desiredSource = vp.options.pickSource(targetZoom);
  if (preserveSharperSource && _lbSrcRank(vp.options.photo().currentSrcKey) > _lbSrcRank(desiredSource)) {
    // The current pixels already exceed this zoom's requirement. Cancel any
    // queued downgrade and retain them; this is especially important for the
    // explicit 1:1 stop, where replacing /original with a lower tier adds work
    // and can leave a failed non-original swap stranded.
    vp.options.cancelSourceSwap();
    vp.options.keepSource(vp.options.photo().currentSrcKey);
    vp.options.scheduleAdjacent(vp.options.photo().currentSrcKey);
    return;
  }
  vp.options.scheduleSource(targetZoom);
}

export function setZoom(/** @type {LightboxViewportContext} */ vp, /** @type {number} */ newZoom, /** @type {number | null | undefined} */ anchorClientX, /** @type {number | null | undefined} */ anchorClientY, /** @type {boolean | undefined} */ preserveSharperSource = undefined) {
  // Any zoom mutation cancels a pending "upgrade to 1:1 on original-load" intent.
  // Callers that still want it (toggleZoom's fallback path) re-set the flag
  // after calling us.
  vp.pendingOneToOne = false;
  vp.pendingOneToOneAnchor = null;
  // Clamp to valid range. Max is 4x native (or 4x fit if no nativeZoom).
  var maxZoom = zoomLimit(vp);
  newZoom = Math.max(1.0, Math.min(maxZoom, newZoom));
  var wrap = vp.document.getElementById('lightboxWrap');
  var t = vp.document.getElementById('lightboxTransform');
  if (!wrap || !t) return;

  var metrics = layoutMetrics(vp);
  if (!metrics) {
    vp.zoom = newZoom;
    if (vp.zoom <= 1.001) {
      vp.panX = 0;
      vp.panY = 0;
    }
    wrap.classList.toggle('zoomed', vp.zoom > 1.001);
    applyTransform(vp);
    syncSourceForZoom(vp, newZoom, preserveSharperSource);
    saveView(vp, vp.options.photoId());
    return;
  }
  // Cursor at clientX has image-local coord (clientX - rect.left) / scale.
  // Anchor invariant: that image coord must still land under the cursor after zoom.
  var rect = t.getBoundingClientRect();
  var anchorX, anchorY;
  if (anchorClientX != null && anchorClientY != null) {
    anchorX = anchorClientX;
    anchorY = anchorClientY;
  } else {
    // Center anchor: use wrap's visual center
    var wrapRect = wrap.getBoundingClientRect();
    anchorX = wrapRect.left + wrapRect.width / 2;
    anchorY = wrapRect.top + wrapRect.height / 2;
  }
  var currentScale = metrics.scale;
  var imgX = (anchorX - rect.left) / currentScale;
  var imgY = (anchorY - rect.top) / currentScale;
  var newScale = vp.fitScale * newZoom;
  var newBaseX = anchorX - imgX * newScale;
  var newBaseY = anchorY - imgY * newScale;
  var wrapRectForBase = wrap.getBoundingClientRect();
  vp.panX = newBaseX - (wrapRectForBase.left + (wrapRectForBase.width - metrics.w * newScale) / 2);
  vp.panY = newBaseY - (wrapRectForBase.top + (wrapRectForBase.height - metrics.h * newScale) / 2);
  vp.zoom = newZoom;

  if (vp.zoom <= 1.001) {
    vp.panX = 0;
    vp.panY = 0;
  }

  clampPan(vp);
  wrap.classList.toggle('zoomed', vp.zoom > 1.001);
  applyTransform(vp);
  syncSourceForZoom(vp, newZoom, preserveSharperSource);
  saveView(vp, vp.options.photoId());
}

export function applyPendingOneToOne(/** @type {LightboxViewportContext} */ vp) {
  if (!vp.pendingOneToOne || !vp.nativeZoom) return;
  // After /original fails, native zoom describes the current preview's pixels.
  // A late metadata callback must keep waiting for the sharper fallback already
  // requested by the error path; snapping now would retarget that swap to /full.
  if (vp.options.photo().originalUnavailable && _lbSrcRank(vp.options.photo().desiredSrcKey) > _lbSrcRank(vp.options.photo().currentSrcKey)) return;
  // Don't snap to true 1:1 until the displayed source is at least the tier
  // that 1:1 needs. Otherwise the /api/photos/<id> metadata fetch resolving
  // before the held /original load would learn vp.nativeZoom, clear the
  // pending state, and snap on the upscaled lower tier — the soft-1:1 flash
  // this deferral exists to prevent. Compare by tier rank (not equality) so a
  // source that is already sharper than required — e.g. /original kept during
  // 1:1-preserving navigation — still applies immediately instead of waiting
  // forever for a downgrade that never comes.
  var requiredSource = vp.options.pickSource(vp.nativeZoom);
  if (_lbSrcRank(vp.options.photo().currentSrcKey) < _lbSrcRank(requiredSource)) {
    vp.options.scheduleSource(vp.nativeZoom);
    return;
  }
  vp.pendingOneToOne = false;
  var anchor = vp.pendingOneToOneAnchor;
  vp.pendingOneToOneAnchor = null;
  setZoom(vp, vp.nativeZoom, anchor ? anchor.x : null, anchor ? anchor.y : null);
}

export function deferOneToOne(/** @type {LightboxViewportContext} */ vp, /** @type {number} */ targetZoom) {
  if (!vp.pendingOneToOne) return false;
  vp.pendingRestore = null;
  applyTransform(vp);
  vp.options.scheduleSource(targetZoom);
  if (vp.options.photo().desiredSrcKey === vp.options.photo().currentSrcKey) {
    applyPendingOneToOne(vp);
  }
  return true;
}

export function deferOneToOneFallback(/** @type {LightboxViewportContext} */ vp) {
  // /original is unavailable but the user still wants true 1:1. Keep the
  // deferral pending and aim at the sharpest remaining preview tier instead of
  // snapping on an upscaled /full.
  return deferOneToOne(vp, 4);
}

export function setFromSlider(/** @type {LightboxViewportContext} */ vp, /** @type {string | number} */ position) {
  if (vp.options.photo().transitionPending) return;
  cancelRestore(vp);
  setZoom(vp, zoomFromSliderPosition(vp, position), null, null);
}

export function stepZoom(/** @type {LightboxViewportContext} */ vp, /** @type {number} */ direction) {
  if (vp.options.photo().transitionPending) return;
  cancelRestore(vp);
  setZoom(vp, vp.zoom * (direction > 0 ? 1.25 : 0.8), null, null);
}

export function zoomToFit(/** @type {LightboxViewportContext} */ vp) {
  if (vp.options.photo().transitionPending) return;
  cancelRestore(vp);
  setZoom(vp, 1.0, null, null);
}

export function zoomToOneToOne(/** @type {LightboxViewportContext} */ vp, /** @type {Pick<MouseEvent, "clientX" | "clientY"> | undefined} */ e) {
  if (vp.options.photo().transitionPending) return;
  var img = /** @type {HTMLImageElement | null} */ (vp.document.getElementById('lightboxImg'));
  if (!img) return;
  cancelRestore(vp);

  // If we don't know native zoom yet (e.g. dimensions fetch still pending),
  // fall back to old two-tier behavior: load original, no continuous control.
  if (!vp.nativeZoom) {
    recomputeNativeZoom(vp);
  }

  // Anchor zoom on click position (or center for the slider's 1:1 stop).
  var rect = img.getBoundingClientRect();
  var anchorX = e && e.clientX ? e.clientX : rect.left + rect.width / 2;
  var anchorY = e && e.clientY ? e.clientY : rect.top + rect.height / 2;
  if (vp.nativeZoom) {
    var desiredSource = vp.options.pickSource(vp.nativeZoom);
    // Compare source ranks (not exact key equality) so an already-sharper
    // current tier — e.g. /original loaded during a previous 1:1 view when
    // the picked tier for vp.nativeZoom is /2560 — satisfies 1:1 immediately.
    // The exact-key check would otherwise enter the deferred path, leave the
    // badge at 'Loading 1:1' while a lower tier is fetched, and could strand
    // vp.pendingOneToOne if that fetch failed.
    if (_lbSrcRank(vp.options.photo().currentSrcKey) < _lbSrcRank(desiredSource)) {
      vp.pendingOneToOne = true;
      vp.pendingOneToOneAnchor = { x: anchorX, y: anchorY };
      vp.options.scheduleSource(vp.nativeZoom);
      applyTransform(vp);
    } else {
      setZoom(vp, vp.nativeZoom, anchorX, anchorY, true);
      vp.pendingOneToOne = false;
      vp.pendingOneToOneAnchor = null;
    }
  } else {
    // Dimensions unknown. Load the high-resolution source first, then apply
    // 1:1 after the browser has decoded it; otherwise the user sees an
    // enlarged preview and reads that as a soft 1:1 view.
    vp.pendingOneToOne = true;
    vp.pendingOneToOneAnchor = { x: anchorX, y: anchorY };
    vp.options.scheduleSource(4);
    applyTransform(vp);
  }
}

export function toggleZoom(/** @type {LightboxViewportContext} */ vp, /** @type {Pick<MouseEvent, "clientX" | "clientY"> | undefined} */ e) {
  // Fit to 1:1 toggle. Preserved for the existing 'z' shortcut and image click.
  // A deferred 1:1 leaves vp.zoom at fit while the sharp source loads, so treat
  // that pending state as zoomed and allow a second toggle to cancel it.
  if (vp.zoom > 1.001 || vp.pendingOneToOne) {
    zoomToFit(vp);
  } else {
    zoomToOneToOne(vp, e);
  }
}

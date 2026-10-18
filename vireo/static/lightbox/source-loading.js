import { lbState } from './shared-state.js';
import { _lbDisplayDimsForRecipe, _lbSrcRank } from './viewport.js';
import { vireoLightboxSession, vireoLightboxViewport } from './state.js';
import {
  _lbEditVersionByPhoto, _vireoPairKnownByPhoto, _vireoPairSource, _vireoUrlWithQueryParam,
  _vireoUrlWithRenderVersion, vireoRenderedUrl,
} from './edits.js';
import { _lbMetadataOrientation, _lbOrientationSwapsAxes } from './overlays.js';


export function _lbSetPreviewLoading(/** @type {boolean} */ loading) {
  lbState.previewLoading = !!loading;
  _lbRenderDetailStatus();
}

// ---- Detail-load status chip ------------------------------------------------
// The lightbox paints a lower-resolution tier first and swaps a sharper one in
// behind it, so "this photo is soft" and "this photo is still arriving" look
// identical on screen. The chip names which one it is: Loading (the incoming
// photo has not decoded yet), Sharpening (a sharper tier is in flight), then a
// brief Full detail once the tier the current zoom needs is the one displayed.
// Loads that finish inside lbDetailStatus.showDelayMs never show anything, so
// arrowing through cached photos stays quiet instead of strobing.
export const lbDetailStatus = {
  showDelayMs: 250,
  settledMs: 900,
  fadeMs: 250,
  // chip is on screen (past the show delay)
  shown: false,
  // showing the Full detail confirmation
  settled: false,
  /** @type {number | null} */
  showTimer: null,
  /** @type {number | null} */
  settleTimer: null,
  /** @type {number | null} */
  fadeTimer: null,
};

export function _lbDetailSharpeningPending() {
  // Derived rather than read off _lbPreviewLoading: the first drag of a pan
  // calls vireoLightboxViewport.cancelRestore, which drops that flag even though the
  // queued swap keeps running. Panning around while the original loads at 1:1
  // is the pixel-peeping path, so the chip has to survive it.
  if (lbState.progressiveTargetKey) return true;
  if (!lbState.desiredSrcKey || lbState.desiredSrcKey === lbState.currentSrcKey) return false;
  // A known-missing original is never going to arrive; claiming otherwise would
  // leave the chip spinning forever.
  if (lbState.originalUnavailable && lbState.desiredSrcKey === 'original') return false;
  return _lbSrcRank(lbState.desiredSrcKey) > _lbSrcRank(lbState.currentSrcKey);
}

function _lbDetailStatusPhase() {
  // _lbVisualTransitionPending is navigation-only -- it stays false when the
  // lightbox opens from closed, which is the case with the emptiest overlay and
  // the most need for a signal. _lbInitialDecodePending covers both.
  if (lbState.visualTransitionPending || lbState.initialDecodePending) return 'loading';
  if (lbState.previewLoading || _lbDetailSharpeningPending()) return 'sharpening';
  if (lbDetailStatus.settled) return 'full';
  return 'off';
}

function _lbDetailStatusLabel(/** @type {string} */ phase) {
  if (phase === 'loading') return 'Loading\u2026';
  if (phase === 'sharpening') return 'Sharpening\u2026';
  return 'Full detail';
}

function _lbPaintDetailStatus(/** @type {string} */ phase) {
  var el = document.getElementById('lightboxPreviewStatus');
  if (!el) return;
  var text = document.getElementById('lightboxPreviewStatusText');
  if (phase === 'off') {
    el.style.display = 'none';
    el.classList.remove('is-full');
    el.classList.remove('is-fading');
    el.removeAttribute('data-phase');
    if (text) text.textContent = '';
    return;
  }
  el.style.display = 'inline-flex';
  el.setAttribute('data-phase', phase);
  el.classList.toggle('is-full', phase === 'full');
  if (phase !== 'full') el.classList.remove('is-fading');
  if (text) text.textContent = _lbDetailStatusLabel(phase);
}

function _lbCancelDetailConfirmation() {
  if (lbDetailStatus.settleTimer) { clearTimeout(lbDetailStatus.settleTimer); lbDetailStatus.settleTimer = null; }
  if (lbDetailStatus.fadeTimer) { clearTimeout(lbDetailStatus.fadeTimer); lbDetailStatus.fadeTimer = null; }
  lbDetailStatus.settled = false;
}

export function _lbResetDetailStatus() {
  if (lbDetailStatus.showTimer) { clearTimeout(lbDetailStatus.showTimer); lbDetailStatus.showTimer = null; }
  _lbCancelDetailConfirmation();
  lbDetailStatus.shown = false;
  _lbPaintDetailStatus('off');
}

export function _lbRenderDetailStatus() {
  if (lbState.detailStatusHold > 0) return;
  var phase = _lbDetailStatusPhase();
  if (phase === 'off') {
    _lbResetDetailStatus();
    return;
  }
  if (phase === 'full') {
    // Only confirm a load the user actually watched happen.
    if (!lbDetailStatus.shown) { _lbResetDetailStatus(); return; }
    _lbPaintDetailStatus('full');
    return;
  }
  // A fresh load started: the previous confirmation is stale.
  _lbCancelDetailConfirmation();
  if (lbDetailStatus.shown) { _lbPaintDetailStatus(phase); return; }
  if (lbDetailStatus.showTimer) return;  // delay already armed; do not restart it
  lbDetailStatus.showTimer = setTimeout(function() {
    lbDetailStatus.showTimer = null;
    var next = _lbDetailStatusPhase();
    if (next !== 'loading' && next !== 'sharpening') { _lbResetDetailStatus(); return; }
    lbDetailStatus.shown = true;
    _lbPaintDetailStatus(next);
  }, lbDetailStatus.showDelayMs);
}

export function _lbMarkDetailSettled() {
  // Nothing was announced, so there is nothing to confirm.
  if (!lbDetailStatus.shown) return;
  // /original failed for this photo, so the displayed tier may hold fewer pixels
  // than the file does -- and when it does, vireoLightboxViewport.layoutDims rebases onto the
  // fallback, making 1:1 mean 1:1 of the preview while the zoom badge still
  // reads 100%. Rather than work out whether this particular view loses detail
  // (crop, rotation, JPEG companion, viewport and DPR all change the answer),
  // withhold the claim whenever the original is gone. Absence asserts nothing;
  // "Full detail" would assert something we cannot vouch for.
  if (lbState.originalUnavailable) return;
  _lbCancelDetailConfirmation();
  lbDetailStatus.settled = true;
  lbDetailStatus.fadeTimer = setTimeout(function() {
    lbDetailStatus.fadeTimer = null;
    var el = document.getElementById('lightboxPreviewStatus');
    if (el && lbDetailStatus.settled) el.classList.add('is-fading');
  }, Math.max(0, lbDetailStatus.settledMs - lbDetailStatus.fadeMs));
  lbDetailStatus.settleTimer = setTimeout(function() {
    lbDetailStatus.settleTimer = null;
    lbDetailStatus.settled = false;
    lbDetailStatus.shown = false;
    _lbRenderDetailStatus();
  }, lbDetailStatus.settledMs);
}

export function _lbFullPreviewLimit() {
  if (lbState.previewMaxSize === 0 || lbState.fullUsesOriginal === true || lbState.sessionFullUsesOriginal === true) return Infinity;
  return lbState.previewMaxSize || 1920;
}

export function _lbInitialSourceKey(/** @type {LightboxPhoto} */ photo, /** @type {number} */ zoom, /** @type {boolean} */ oneToOne) {
  var w = Number(photo.width), h = Number(photo.height);
  if (!w || !h) return oneToOne || zoom > 1.001 ? 'original' : 'full';
  if (_vireoPairKnownByPhoto[String(photo.id)] && _vireoPairSource(photo.id) === 'jpeg') {
    // A paired JPEG can have different geometry from its RAW catalog row.
    return oneToOne || zoom > 1.001 ? 'original' : 'full';
  }
  if (_lbOrientationSwapsAxes(_lbMetadataOrientation(photo.metadata))) {
    var tmp = w; w = h; h = tmp;
  }
  var dims = _lbDisplayDimsForRecipe(w, h, photo.edit_recipe);
  var wrap = document.getElementById('lightboxWrap');
  if (!wrap || !wrap.clientWidth || !wrap.clientHeight) return oneToOne || zoom > 1.001 ? 'original' : 'full';
  if (!dims.w || !dims.h) return oneToOne || zoom > 1.001 ? 'original' : 'full';
  var fit = Math.min(1, wrap.clientWidth / dims.w, wrap.clientHeight / dims.h);
  var needed = oneToOne ? Math.max(dims.w, dims.h)
    : Math.max(dims.w, dims.h) * fit * zoom * (window.devicePixelRatio || 1);
  var fullEdge = _lbFullPreviewLimit();
  if (needed <= fullEdge) return 'full';
  if (needed <= 2560) return '2560';
  if (needed <= 3840) return '3840';
  return 'original';
}

export function _lbPickSourceKey(/** @type {number | null | undefined} */ targetZoom = undefined) {
  // Returns 'full' | '2560' | '3840' | 'original' based on current display needs.
  // Fall back to /full at fit and /original when zoomed whenever we can't compute
  // a proper displayed-long-edge (photo dims missing, or nativeZoom not yet
  // established — the latter happens briefly when API dims arrive before the
  // initial image load completes).
  var dims = vireoLightboxViewport.layoutDims();
  var zoom = targetZoom != null ? targetZoom : vireoLightboxViewport.zoom();
  if (!dims || !dims.w || !dims.h || !vireoLightboxViewport.nativeZoom()) {
    return (zoom > 1.001 && !lbState.originalUnavailable) ? 'original' : 'full';
  }
  var longEdge = Math.max(dims.w, dims.h);
  var displayedLong = longEdge * vireoLightboxViewport.fitScale() * zoom;
  // DPR consideration: high-DPI screens need more pixels for a crisp display.
  var dpr = window.devicePixelRatio || 1;
  var needed = displayedLong * dpr;
  // /full's actual long edge depends on the server's preview_max_size config (default 1920).
  // Prefer this photo's decoded size. Before it loads, use the configured cap;
  // a previous small photo may never have reached that cap.
  var fullLongEdge = lbState.fullLongEdge || _lbFullPreviewLimit();
  if (needed <= fullLongEdge) return 'full';
  if (needed <= 2560) return '2560';
  if (needed <= 3840) return '3840';
  return lbState.originalUnavailable ? '3840' : 'original';
}

export function _lbSrcUrl(/** @type {number | null} */ photoId, /** @type {string} */ key, /** @type {boolean | undefined} */ speculative = undefined) {
  var url;
  if (key === 'original') url = '/photos/' + photoId + '/original';
  else if (key === 'full') url = '/photos/' + photoId + '/full';
  else url = '/photos/' + photoId + '/preview?size=' + key;
  url = vireoRenderedUrl
    ? vireoRenderedUrl(url, photoId)
    : _vireoUrlWithRenderVersion(url, photoId);
  var version = _lbEditVersionByPhoto[String(photoId)];
  if (version) url = _vireoUrlWithQueryParam(url, 'editv', version);
  var speculativeUrl = _vireoUrlWithQueryParam(url, 'prefetch', '1');
  if (speculative) return speculativeUrl;

  // Keep the exact URL of a successfully decoded warmup. Browser caches are
  // URL-keyed, so dropping the prefetch marker here would force a second
  // transfer/decode and throw away the retained Image object's main benefit.
  if (photoId != null && vireoLightboxSession.hasDecodedSource(photoId, key, speculativeUrl)) return speculativeUrl;
  return url;
}

export function _lbScheduleSourceSwap(/** @type {number | undefined} */ targetZoom = undefined, /** @type {boolean | undefined} */ immediate = undefined) {
  if (!vireoLightboxSession.requestedPhotoId()) return;
  // Was a sharper tier already requested for the displayed bitmap? Read this
  // before retargeting: if the new target is the tier already on screen, that
  // request is being abandoned rather than fulfilled. _lbProgressiveTargetKey
  // deliberately does not count -- it is the initial pick being evaluated for
  // the first time, not a request in flight.
  var abandoningRequest = !!lbState.desiredSrcKey
    && lbState.desiredSrcKey !== lbState.currentSrcKey
    && _lbSrcRank(lbState.desiredSrcKey) > _lbSrcRank(lbState.currentSrcKey);
  var desired = _lbPickSourceKey(targetZoom);
  // Metadata may still be in flight when a warm preview first becomes visible.
  // Keep the initial conservative tier until geometry can refine it.
  if (lbState.progressiveTargetKey && !vireoLightboxViewport.nativeZoom()) desired = lbState.progressiveTargetKey;
  lbState.desiredSrcKey = desired;
  if (vireoLightboxViewport.zoom() > 1.001 && desired !== 'original') {
    vireoLightboxSession.cancelOriginal();
  }
  if (desired === lbState.currentSrcKey) {
    lbState.progressiveTargetKey = null;
    // Zooming back to a level the current tier already satisfies cancels the
    // sharper request. Nothing loaded, so go quiet rather than claim the pixels
    // the user was waiting on arrived.
    if (!abandoningRequest) _lbMarkDetailSettled();
    _lbSetPreviewLoading(false);
    vireoLightboxSession.scheduleAdjacent(desired);
    return;
  }

  _lbSetPreviewLoading(_lbSrcRank(desired) > _lbSrcRank(lbState.currentSrcKey) || !!lbState.progressiveTargetKey);
  vireoLightboxSession.scheduleSwap(function() {
    if (lbState.desiredSrcKey !== desired) return;          // newer request won
    if (lbState.desiredSrcKey === lbState.currentSrcKey) return; // already there
    var photoId = vireoLightboxSession.requestedPhotoId();
    var swapToken = vireoLightboxSession.capture();
    var key = lbState.desiredSrcKey; if (photoId == null || key == null) return;
    var url = _lbSrcUrl(photoId, key);

    vireoLightboxSession.loadSource(url, function() {
      // Ignore stale: different photo, or desired key changed to something else
      if (vireoLightboxSession.requestedPhotoId() !== photoId) return;
      if (!vireoLightboxSession.isCurrent(swapToken)) return;
      if (lbState.desiredSrcKey !== key) return;
      const img = /** @type {HTMLImageElement | null} */ (document.getElementById('lightboxImg'));
      if (!img) return;
      // After the new source loads, recompute nativeZoom — the aspect ratio is
      // nominally identical across source tiers but subpixel rounding can shift
      // fit dimensions slightly, and if API dims were missing the original's
      // naturalWidth is now authoritative.
      vireoLightboxSession.watchImage(img, function() {
        if (vireoLightboxSession.requestedPhotoId() !== photoId) return;
        if (!vireoLightboxSession.isCurrent(swapToken)) return;
        // A newer swap may have preempted us before this load fired — in that
        // case img.naturalWidth belongs to a DIFFERENT source than our closure's
        // `key`, so bail rather than record wrong dims or recalibrate stale state.
        if (lbState.currentSrcKey !== key) return;
        lbState.progressiveTargetKey = null;
        _lbMarkDetailSettled();
        _lbSetPreviewLoading(false);
        // Keep the decoded background original alive until the visible image
        // has adopted it; releasing it earlier can make some WebViews decode
        // the same full-resolution bytes a second time on the first 100% click.
        if (key === 'original') vireoLightboxSession.cancelOriginal();
        if (!lbState.photoW && key === 'original' && img.naturalWidth) {
          lbState.photoW = img.naturalWidth;
          lbState.photoH = img.naturalHeight;
        }
        vireoLightboxViewport.recomputeNativeZoom();
        // If the user pressed z/click when nativeZoom was unknown, upgrade to
        // true 1:1 now. Route through the shared helper so the stored anchor is
        // honored — completing with a recentered zoom would make a deferred 1:1
        // jump away from the point the user clicked.
        if (!vireoLightboxViewport.applyPendingRestore()) vireoLightboxViewport.applyPendingOneToOne();
        // Track Eye alignment is deferred while vireoLightboxViewport.pendingOneToOne() is armed so it
        // cannot cancel the deferred sharp-source fallback. Re-apply now that
        // the source has landed and any pending 1:1 has snapped.
        vireoLightboxViewport.applyPendingEye();
        vireoLightboxViewport.applyTransform();
        vireoLightboxSession.scheduleAdjacent(key);
      });
      img.src = url;
      lbState.currentSrcKey = key;
    }, function() {
      if (vireoLightboxSession.requestedPhotoId() !== photoId) return;
      if (!vireoLightboxSession.isCurrent(swapToken)) return;
      if (lbState.desiredSrcKey !== key) return;
      lbState.progressiveTargetKey = null;
      // Record the failure BEFORE clearing the loading flag. The chip's
      // sharpening state is derived from the desired/current tiers, so a tier
      // that is never going to arrive has to stop counting as in flight first
      // -- clearing the flag while _lbDesiredSrcKey still names the failed tier
      // leaves the spinner up with no request behind it.
      if (key === 'original') {
        var had1To1Pending = vireoLightboxViewport.pendingOneToOne();
        lbState.originalUnavailable = true;
        _lbSetPreviewLoading(false);
        vireoLightboxViewport.recomputeNativeZoom();
        if (had1To1Pending) {
          vireoLightboxViewport.deferOneToOneFallback();
        } else {
          vireoLightboxViewport.applyPendingOneToOne();
          vireoLightboxViewport.applyTransform();
          // /original is gone, so re-pick the best remaining tier rather than
          // staying stuck on the lower current source.
          _lbScheduleSourceSwap();
        }
      } else {
        // The failed request is no longer pending. Keep the usable bitmap
        // and resume its neighbors without automatically retrying this tier.
        lbState.desiredSrcKey = lbState.currentSrcKey;
        _lbSetPreviewLoading(false);
        if (vireoLightboxViewport.pendingOneToOne()) {
          vireoLightboxViewport.cancelPendingZoom();
          vireoLightboxViewport.updateControls();
          vireoLightboxViewport.save(photoId);
        }
        vireoLightboxSession.scheduleAdjacent(lbState.currentSrcKey);
      }
      // Keep current source on failure
    });
  }, immediate ? 0 : 150);
}

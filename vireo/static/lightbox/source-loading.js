
var _lbDesiredSrcKey = null;

function _lbSetPreviewLoading(loading) {
  _lbPreviewLoading = !!loading;
  _lbRenderDetailStatus();
}

// ---- Detail-load status chip ------------------------------------------------
// The lightbox paints a lower-resolution tier first and swaps a sharper one in
// behind it, so "this photo is soft" and "this photo is still arriving" look
// identical on screen. The chip names which one it is: Loading (the incoming
// photo has not decoded yet), Sharpening (a sharper tier is in flight), then a
// brief Full detail once the tier the current zoom needs is the one displayed.
// Loads that finish inside LB_DETAIL_SHOW_DELAY_MS never show anything, so
// arrowing through cached photos stays quiet instead of strobing.
var LB_DETAIL_SHOW_DELAY_MS = 250;
var LB_DETAIL_SETTLED_MS = 900;
var LB_DETAIL_FADE_MS = 250;
var _lbInitialDecodePending = false; // this photo's first bitmap has not decoded
var _lbDetailStatusShown = false;    // chip is on screen (past the show delay)
var _lbDetailStatusSettled = false;  // showing the Full detail confirmation
var _lbDetailStatusHold = 0;         // >0 suppresses paints mid-transition
var _lbDetailShowTimer = null;
var _lbDetailSettleTimer = null;
var _lbDetailFadeTimer = null;

function _lbDetailSharpeningPending() {
  // Derived rather than read off _lbPreviewLoading: the first drag of a pan
  // calls vireoLightboxViewport.cancelRestore, which drops that flag even though the
  // queued swap keeps running. Panning around while the original loads at 1:1
  // is the pixel-peeping path, so the chip has to survive it.
  if (_lbProgressiveTargetKey) return true;
  if (!_lbDesiredSrcKey || _lbDesiredSrcKey === _lbCurrentSrcKey) return false;
  // A known-missing original is never going to arrive; claiming otherwise would
  // leave the chip spinning forever.
  if (_lbOriginalUnavailable && _lbDesiredSrcKey === 'original') return false;
  return _lbSrcRank(_lbDesiredSrcKey) > _lbSrcRank(_lbCurrentSrcKey);
}

function _lbDetailStatusPhase() {
  // _lbVisualTransitionPending is navigation-only -- it stays false when the
  // lightbox opens from closed, which is the case with the emptiest overlay and
  // the most need for a signal. _lbInitialDecodePending covers both.
  if (_lbVisualTransitionPending || _lbInitialDecodePending) return 'loading';
  if (_lbPreviewLoading || _lbDetailSharpeningPending()) return 'sharpening';
  if (_lbDetailStatusSettled) return 'full';
  return 'off';
}

function _lbDetailStatusLabel(phase) {
  if (phase === 'loading') return 'Loading\u2026';
  if (phase === 'sharpening') return 'Sharpening\u2026';
  return 'Full detail';
}

function _lbPaintDetailStatus(phase) {
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
  if (_lbDetailSettleTimer) { clearTimeout(_lbDetailSettleTimer); _lbDetailSettleTimer = null; }
  if (_lbDetailFadeTimer) { clearTimeout(_lbDetailFadeTimer); _lbDetailFadeTimer = null; }
  _lbDetailStatusSettled = false;
}

function _lbResetDetailStatus() {
  if (_lbDetailShowTimer) { clearTimeout(_lbDetailShowTimer); _lbDetailShowTimer = null; }
  _lbCancelDetailConfirmation();
  _lbDetailStatusShown = false;
  _lbPaintDetailStatus('off');
}

function _lbRenderDetailStatus() {
  if (_lbDetailStatusHold > 0) return;
  var phase = _lbDetailStatusPhase();
  if (phase === 'off') {
    _lbResetDetailStatus();
    return;
  }
  if (phase === 'full') {
    // Only confirm a load the user actually watched happen.
    if (!_lbDetailStatusShown) { _lbResetDetailStatus(); return; }
    _lbPaintDetailStatus('full');
    return;
  }
  // A fresh load started: the previous confirmation is stale.
  _lbCancelDetailConfirmation();
  if (_lbDetailStatusShown) { _lbPaintDetailStatus(phase); return; }
  if (_lbDetailShowTimer) return;  // delay already armed; do not restart it
  _lbDetailShowTimer = setTimeout(function() {
    _lbDetailShowTimer = null;
    var next = _lbDetailStatusPhase();
    if (next !== 'loading' && next !== 'sharpening') { _lbResetDetailStatus(); return; }
    _lbDetailStatusShown = true;
    _lbPaintDetailStatus(next);
  }, LB_DETAIL_SHOW_DELAY_MS);
}

function _lbMarkDetailSettled() {
  // Nothing was announced, so there is nothing to confirm.
  if (!_lbDetailStatusShown) return;
  // /original failed for this photo, so the displayed tier may hold fewer pixels
  // than the file does -- and when it does, vireoLightboxViewport.layoutDims rebases onto the
  // fallback, making 1:1 mean 1:1 of the preview while the zoom badge still
  // reads 100%. Rather than work out whether this particular view loses detail
  // (crop, rotation, JPEG companion, viewport and DPR all change the answer),
  // withhold the claim whenever the original is gone. Absence asserts nothing;
  // "Full detail" would assert something we cannot vouch for.
  if (_lbOriginalUnavailable) return;
  _lbCancelDetailConfirmation();
  _lbDetailStatusSettled = true;
  _lbDetailFadeTimer = setTimeout(function() {
    _lbDetailFadeTimer = null;
    var el = document.getElementById('lightboxPreviewStatus');
    if (el && _lbDetailStatusSettled) el.classList.add('is-fading');
  }, Math.max(0, LB_DETAIL_SETTLED_MS - LB_DETAIL_FADE_MS));
  _lbDetailSettleTimer = setTimeout(function() {
    _lbDetailSettleTimer = null;
    _lbDetailStatusSettled = false;
    _lbDetailStatusShown = false;
    _lbRenderDetailStatus();
  }, LB_DETAIL_SETTLED_MS);
}

function _lbFullPreviewLimit() {
  if (_lbPreviewMaxSize === 0 || _lbFullUsesOriginal === true || _lbSessionFullUsesOriginal === true) return Infinity;
  return _lbPreviewMaxSize || 1920;
}

function _lbInitialSourceKey(photo, zoom, oneToOne) {
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
  var fit = Math.min(1, wrap.clientWidth / dims.w, wrap.clientHeight / dims.h);
  var needed = oneToOne ? Math.max(dims.w, dims.h)
    : Math.max(dims.w, dims.h) * fit * zoom * (window.devicePixelRatio || 1);
  var fullEdge = _lbFullPreviewLimit();
  if (needed <= fullEdge) return 'full';
  if (needed <= 2560) return '2560';
  if (needed <= 3840) return '3840';
  return 'original';
}

function _lbPickSourceKey(targetZoom) {
  // Returns 'full' | '2560' | '3840' | 'original' based on current display needs.
  // Fall back to /full at fit and /original when zoomed whenever we can't compute
  // a proper displayed-long-edge (photo dims missing, or nativeZoom not yet
  // established — the latter happens briefly when API dims arrive before the
  // initial image load completes).
  var dims = vireoLightboxViewport.layoutDims();
  var zoom = targetZoom != null ? targetZoom : vireoLightboxViewport.zoom();
  if (!dims || !vireoLightboxViewport.nativeZoom()) {
    return (zoom > 1.001 && !_lbOriginalUnavailable) ? 'original' : 'full';
  }
  var longEdge = Math.max(dims.w, dims.h);
  var displayedLong = longEdge * vireoLightboxViewport.fitScale() * zoom;
  // DPR consideration: high-DPI screens need more pixels for a crisp display.
  var dpr = window.devicePixelRatio || 1;
  var needed = displayedLong * dpr;
  // /full's actual long edge depends on the server's preview_max_size config (default 1920).
  // Prefer this photo's decoded size. Before it loads, use the configured cap;
  // a previous small photo may never have reached that cap.
  var fullLongEdge = _lbFullLongEdge || _lbFullPreviewLimit();
  if (needed <= fullLongEdge) return 'full';
  if (needed <= 2560) return '2560';
  if (needed <= 3840) return '3840';
  return _lbOriginalUnavailable ? '3840' : 'original';
}

function _lbSrcUrl(photoId, key, speculative) {
  var url;
  if (key === 'original') url = '/photos/' + photoId + '/original';
  else if (key === 'full') url = '/photos/' + photoId + '/full';
  else url = '/photos/' + photoId + '/preview?size=' + key;
  url = window.vireoRenderedUrl
    ? window.vireoRenderedUrl(url, photoId)
    : _vireoUrlWithRenderVersion(url, photoId);
  var version = _lbEditVersionByPhoto[String(photoId)];
  if (version) url = _vireoUrlWithQueryParam(url, 'editv', version);
  var speculativeUrl = _vireoUrlWithQueryParam(url, 'prefetch', '1');
  if (speculative) return speculativeUrl;

  // Keep the exact URL of a successfully decoded warmup. Browser caches are
  // URL-keyed, so dropping the prefetch marker here would force a second
  // transfer/decode and throw away the retained Image object's main benefit.
  if (vireoLightboxSession.hasDecodedSource(photoId, key, speculativeUrl)) return speculativeUrl;
  return url;
}

function _lbScheduleSourceSwap(targetZoom, immediate) {
  if (!vireoLightboxSession.requestedPhotoId()) return;
  // Was a sharper tier already requested for the displayed bitmap? Read this
  // before retargeting: if the new target is the tier already on screen, that
  // request is being abandoned rather than fulfilled. _lbProgressiveTargetKey
  // deliberately does not count -- it is the initial pick being evaluated for
  // the first time, not a request in flight.
  var abandoningRequest = !!_lbDesiredSrcKey
    && _lbDesiredSrcKey !== _lbCurrentSrcKey
    && _lbSrcRank(_lbDesiredSrcKey) > _lbSrcRank(_lbCurrentSrcKey);
  var desired = _lbPickSourceKey(targetZoom);
  // Metadata may still be in flight when a warm preview first becomes visible.
  // Keep the initial conservative tier until geometry can refine it.
  if (_lbProgressiveTargetKey && !vireoLightboxViewport.nativeZoom()) desired = _lbProgressiveTargetKey;
  _lbDesiredSrcKey = desired;
  if (vireoLightboxViewport.zoom() > 1.001 && desired !== 'original') {
    vireoLightboxSession.cancelOriginal();
  }
  if (desired === _lbCurrentSrcKey) {
    _lbProgressiveTargetKey = null;
    // Zooming back to a level the current tier already satisfies cancels the
    // sharper request. Nothing loaded, so go quiet rather than claim the pixels
    // the user was waiting on arrived.
    if (!abandoningRequest) _lbMarkDetailSettled();
    _lbSetPreviewLoading(false);
    vireoLightboxSession.scheduleAdjacent(desired);
    return;
  }

  _lbSetPreviewLoading(_lbSrcRank(desired) > _lbSrcRank(_lbCurrentSrcKey) || !!_lbProgressiveTargetKey);
  vireoLightboxSession.scheduleSwap(function() {
    if (_lbDesiredSrcKey !== desired) return;          // newer request won
    if (_lbDesiredSrcKey === _lbCurrentSrcKey) return; // already there
    var photoId = vireoLightboxSession.requestedPhotoId();
    var swapToken = vireoLightboxSession.capture();
    var key = _lbDesiredSrcKey;
    var url = _lbSrcUrl(photoId, key);

    vireoLightboxSession.loadSource(url, function() {
      // Ignore stale: different photo, or desired key changed to something else
      if (vireoLightboxSession.requestedPhotoId() !== photoId) return;
      if (!vireoLightboxSession.isCurrent(swapToken)) return;
      if (_lbDesiredSrcKey !== key) return;
      var img = document.getElementById('lightboxImg');
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
        if (_lbCurrentSrcKey !== key) return;
        _lbProgressiveTargetKey = null;
        _lbMarkDetailSettled();
        _lbSetPreviewLoading(false);
        // Keep the decoded background original alive until the visible image
        // has adopted it; releasing it earlier can make some WebViews decode
        // the same full-resolution bytes a second time on the first 100% click.
        if (key === 'original') vireoLightboxSession.cancelOriginal();
        if (!_lbPhotoW && key === 'original' && img.naturalWidth) {
          _lbPhotoW = img.naturalWidth;
          _lbPhotoH = img.naturalHeight;
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
      _lbCurrentSrcKey = key;
    }, function() {
      if (vireoLightboxSession.requestedPhotoId() !== photoId) return;
      if (!vireoLightboxSession.isCurrent(swapToken)) return;
      if (_lbDesiredSrcKey !== key) return;
      _lbProgressiveTargetKey = null;
      // Record the failure BEFORE clearing the loading flag. The chip's
      // sharpening state is derived from the desired/current tiers, so a tier
      // that is never going to arrive has to stop counting as in flight first
      // -- clearing the flag while _lbDesiredSrcKey still names the failed tier
      // leaves the spinner up with no request behind it.
      if (key === 'original') {
        var had1To1Pending = vireoLightboxViewport.pendingOneToOne();
        _lbOriginalUnavailable = true;
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
        _lbDesiredSrcKey = _lbCurrentSrcKey;
        _lbSetPreviewLoading(false);
        if (vireoLightboxViewport.pendingOneToOne()) {
          vireoLightboxViewport.cancelPendingZoom();
          vireoLightboxViewport.updateControls();
          vireoLightboxViewport.save(photoId);
        }
        vireoLightboxSession.scheduleAdjacent(_lbCurrentSrcKey);
      }
      // Keep current source on failure
    });
  }, immediate ? 0 : 150);
}

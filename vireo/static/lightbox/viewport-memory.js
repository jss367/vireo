
function _lbIsOneToOneZoom() {
  if (_lbPending1To1) return true;
  if (!_lbNativeZoom || _lbNativeZoom <= 1.001) return false;
  var tolerance = Math.max(0.01, _lbNativeZoom * 0.01);
  return Math.abs(_lbZoom - _lbNativeZoom) <= tolerance;
}

function _lbSrcRank(key) {
  // Tier ordering by available source resolution (higher = sharper).
  if (key === 'original') return 3;
  if (key === '3840') return 2;
  if (key === '2560') return 1;
  return 0; // 'full', '1920', or unknown
}

function _lbCloneViewportState(state) {
  if (!state) return null;
  return {
    zoom: Number(state.zoom) || 1.0,
    centerX: Number(state.centerX),
    centerY: Number(state.centerY),
    oneToOne: !!state.oneToOne,
    pending1To1: !!state.pending1To1,
  };
}

function _lbViewportStateFromCurrent() {
  var metrics = _lbUpdateLayoutMetrics();
  var state = {
    zoom: _lbZoom || 1.0,
    centerX: 0.5,
    centerY: 0.5,
    oneToOne: _lbIsOneToOneZoom(),
    pending1To1: !!_lbPending1To1,
  };
  var wrap = document.getElementById('lightboxWrap');
  var t = document.getElementById('lightboxTransform');
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

function _lbCaptureEyeTrackingAnchor() {
  if (!_lbTrackEyeEnabled || (_lbZoom <= 1.001 && !_lbPending1To1)) return null;
  // During rapid navigation the DOM can still contain the outgoing bitmap.
  // Reuse the last trustworthy screen anchor instead of measuring that bitmap
  // against the incoming photo id.
  if (_lbVisualTransitionPending) {
    return _lbEyeTrackScreenAnchor
      ? { offsetX: _lbEyeTrackScreenAnchor.offsetX, offsetY: _lbEyeTrackScreenAnchor.offsetY }
      : null;
  }
  var photo = _lbPhotoData(vireoLightboxSession.requestedPhotoId());
  var point = _lbPhotoEyePoint(vireoLightboxSession.requestedPhotoId(), photo);
  var overlaysAvailable = _lbSourceOverlaysAvailable();
  var metrics = _lbUpdateLayoutMetrics();
  var state = point && overlaysAvailable ? _lbViewportStateFromCurrent() : null;
  if (!point || !overlaysAvailable || !metrics || !state) {
    // Current frame is unusable for a fresh measurement — either its metadata
    // has resolved without an eye, the active source cannot align overlays
    // (JPEG pair or geometric edit), the destination metadata is not yet
    // cached, or the layout has not been measured. Reuse the last trustworthy
    // anchor so a later eyed frame can still resume alignment across the
    // paused sequence. User pan/zoom paths clear the cached anchor via
    // _lbClearPendingViewportRestore, so a manual viewport change on this
    // paused frame will not carry a stale eye offset to the next eyed photo.
    return _lbEyeTrackScreenAnchor
      ? { offsetX: _lbEyeTrackScreenAnchor.offsetX, offsetY: _lbEyeTrackScreenAnchor.offsetY }
      : null;
  }
  var anchor = {
    offsetX: (point.x - state.centerX) * metrics.w * metrics.scale,
    offsetY: (point.y - state.centerY) * metrics.h * metrics.scale,
  };
  _lbEyeTrackScreenAnchor = anchor;
  return { offsetX: anchor.offsetX, offsetY: anchor.offsetY };
}

function _lbTryApplyPendingEyeTrack(photo) {
  var pending = _lbPendingEyeTrack;
  if (!pending || !_lbTrackEyeEnabled) return false;
  if (String(pending.photoId) !== String(vireoLightboxSession.requestedPhotoId())) return false;
  photo = photo || _lbPhotoData(vireoLightboxSession.requestedPhotoId());
  // A missing object means the detail request has not resolved yet. Keep the
  // alignment armed so its callback can apply it after the image is laid out.
  // Navigation lists on some pages contain only {id, filename}; those are
  // also "unknown", not evidence that the destination lacks an eye.
  if (!photo || !Object.prototype.hasOwnProperty.call(photo, 'eye_x')) return false;
  var point = _lbPhotoEyePoint(vireoLightboxSession.requestedPhotoId(), photo);
  var metrics = _lbUpdateLayoutMetrics();
  if (!point || !_lbSourceOverlaysAvailable()) {
    _lbPendingEyeTrack = null;
    _lbApplyTrackEyeState();
    return false;
  }
  if (!metrics || _lbVisualTransitionPending) return false;

  // A deferred 1:1 snap is still armed — the loader is holding the current
  // display upscaled while a sharper source (2560/3840 or /original) swap
  // completes. Applying the viewport here would re-enter
  // _lbApplyViewportState(), whose one-to-one branch would either clear
  // _lbPending1To1 (when nativeZoom is known) or leave it armed but whose
  // trailing _lbScheduleSourceSwap() can retarget back to /full when
  // /original is unavailable and nativeZoom is not yet known — cancelling
  // the sharp fallback and leaving the user on a soft view. Wait for the
  // deferred source swap to land and _lbApplyPendingOneToOneZoom() to
  // finish before aligning the eye.
  if (_lbPending1To1) return false;

  var state = _lbViewportStateFromCurrent();
  if (!state || _lbZoom <= 1.001 || !metrics.scale) return false;
  state.centerX = point.x - (Number(pending.offsetX) || 0) / (metrics.w * metrics.scale);
  state.centerY = point.y - (Number(pending.offsetY) || 0) / (metrics.h * metrics.scale);
  _lbApplyViewportState(state);

  // Native 1:1 can be learned after the first layout pass. Retain the pending
  // alignment until then so a later 1:1 snap does not move the eye.
  if (!state.oneToOne || _lbNativeZoom) {
    _lbPendingEyeTrack = null;
  }
  _lbEyeTrackScreenAnchor = {
    offsetX: Number(pending.offsetX) || 0,
    offsetY: Number(pending.offsetY) || 0,
  };
  _lbSaveViewportState(vireoLightboxSession.requestedPhotoId());
  _lbApplyTrackEyeState();
  return true;
}

function _lbSaveViewportState(photoId) {
  if (photoId == null) return null;
  // Mid-navigation the DOM transform still belongs to the outgoing photo but
  // vireoLightboxSession.requestedPhotoId() has already advanced to the incoming id. Reading from
  // the DOM here would misattribute the frozen bitmap to the incoming photo
  // and stomp its intended inspection point. Prefer the pending restore
  // state (what handleInitialImageLoad is about to apply), and otherwise
  // leave any previously saved state alone.
  if (_lbVisualTransitionPending && String(photoId) === String(vireoLightboxSession.requestedPhotoId())) {
    var pending = _lbPendingViewportState;
    if (pending) {
      var pendingClone = _lbCloneViewportState(pending);
      _lbViewportByPhotoId[String(photoId)] = pendingClone;
      return _lbCloneViewportState(pendingClone);
    }
    var existing = _lbViewportByPhotoId[String(photoId)];
    return existing ? _lbCloneViewportState(existing) : null;
  }
  var state = _lbViewportStateFromCurrent();
  _lbViewportByPhotoId[String(photoId)] = state;
  return _lbCloneViewportState(state);
}

function _lbViewportStateForOpen(photoId, fallbackState) {
  // Arrow navigation supplies the viewport from the photo being left. Treat
  // that handoff as the current browse-session viewport, even when the target
  // photo has an older cached viewport from an earlier visit. This keeps zoom
  // continuous while moving left/right: zooming back out on one photo also
  // means a previously visited photo opens zoomed out when navigating back.
  if (fallbackState) {
    return _lbCloneViewportState(fallbackState);
  }
  var key = String(photoId);
  if (Object.prototype.hasOwnProperty.call(_lbViewportByPhotoId, key)) {
    return _lbCloneViewportState(_lbViewportByPhotoId[key]);
  }
  return null;
}

function _lbApplyViewportState(state) {
  state = _lbCloneViewportState(state);
  if (!state) return false;
  var metrics = _lbUpdateLayoutMetrics();
  if (!metrics) return false;
  var wrap = document.getElementById('lightboxWrap');
  if (!wrap) return false;

  var maxZoom = _lbMaxZoom();
  var targetZoom = state.zoom || 1.0;
  if (state.oneToOne) {
    if (_lbNativeZoom) {
      targetZoom = _lbNativeZoom;
      _lbPending1To1 = false;
    } else {
      _lbPending1To1 = true;
      targetZoom = Math.max(1.0, targetZoom);
    }
  } else {
    _lbPending1To1 = false;
  }
  targetZoom = Math.max(1.0, Math.min(maxZoom, targetZoom));
  _lbZoom = targetZoom;

  if (_lbZoom <= 1.001) {
    _lbPanX = 0;
    _lbPanY = 0;
  } else {
    var scale = _lbFitScale * _lbZoom;
    var centerX = Number.isFinite(state.centerX) ? state.centerX : 0.5;
    var centerY = Number.isFinite(state.centerY) ? state.centerY : 0.5;
    centerX = Math.max(0, Math.min(1, centerX));
    centerY = Math.max(0, Math.min(1, centerY));
    var baseLeft = (metrics.wrapW - metrics.w * scale) / 2;
    var baseTop = (metrics.wrapH - metrics.h * scale) / 2;
    _lbPanX = (metrics.wrapW / 2) - (centerX * metrics.w * scale) - baseLeft;
    _lbPanY = (metrics.wrapH / 2) - (centerY * metrics.h * scale) - baseTop;
    _lbClampPan();
  }

  wrap.classList.toggle('zoomed', _lbZoom > 1.001);
  _lbApplyTransform();
  _lbScheduleSourceSwap();
  return true;
}

function _lbFlushDeferredOverlayApply() {
  // Called from handleInitialImageLoad and the terminal handleInitialImageError
  // branch, i.e. wherever _lbVisualTransitionPending flips back to false. The
  // metadata callback stashes the detection/eye render here so overlays don't
  // paint against the still-frozen outgoing transform. Re-apply the mask
  // overlay too because _lbApplyMaskVisibility only adds the `show` class when
  // the transition is clear, so an in-flight mask load could have been gated.
  var pending = _lbDeferredOverlayApply;
  _lbDeferredOverlayApply = null;
  if (typeof pending === 'function') {
    try { pending(); } catch (_) {}
  }
  _lbApplyMaskVisibility();
}

function _lbClearPendingViewportRestore() {
  _lbProgressiveTargetKey = null;
  _lbSetPreviewLoading(false);
  // Called from user-driven viewport mutations (wheel/keyboard/click zoom,
  // drag pan) so a still-armed restore carried over by arrow navigation
  // cannot be reapplied by a later async metadata/image-load callback and
  // stomp the user's manual zoom/pan. Also drops any deferred eye-tracking
  // alignment for the same reason: when a destination image finishes
  // loading before /api/photos/<id> returns, _lbPendingEyeTrack stays
  // armed until the metadata callback fires _lbTryApplyPendingEyeTrack,
  // and a user pan/zoom in that window must not be overwritten by the
  // previous photo's eye anchor. Dropping the cached screen anchor here
  // — instead of eagerly on every known no-eye frame — lets tracking
  // resume across an eyed → no-eye → eyed sequence when the user did not
  // pan or zoom the paused frame, while still preventing a stale offset
  // from being carried onto the next eyed photo after a manual pan/zoom
  // on the paused frame. Programmatic restore paths
  // (_lbTryApplyPendingViewportState, _lbTryApplyPendingEyeTrack) own
  // clearing their own state and must not route through here.
  _lbPendingViewportState = null;
  _lbPendingEyeTrack = null;
  _lbEyeTrackScreenAnchor = null;
}

function _lbTryApplyPendingViewportState() {
  if (!_lbPendingViewportState) return false;
  var state = _lbPendingViewportState;
  if (!_lbApplyViewportState(state)) return false;
  // Keep the pending state until native zoom is known. Until then
  // _lbApplyViewportState clamps with a fallback max of 4, so a saved
  // zoom > 4 would be permanently degraded if cleared here and never
  // retried once the image load event establishes _lbNativeZoom.
  if (_lbNativeZoom) {
    _lbPendingViewportState = null;
  }
  return true;
}

function _lbApplyPendingOneToOneZoom() {
  if (!_lbPending1To1 || !_lbNativeZoom) return;
  // After /original fails, native zoom describes the current preview's pixels.
  // A late metadata callback must keep waiting for the sharper fallback already
  // requested by the error path; snapping now would retarget that swap to /full.
  if (_lbOriginalUnavailable && _lbSrcRank(_lbDesiredSrcKey) > _lbSrcRank(_lbCurrentSrcKey)) return;
  // Don't snap to true 1:1 until the displayed source is at least the tier
  // that 1:1 needs. Otherwise the /api/photos/<id> metadata fetch resolving
  // before the held /original load would learn _lbNativeZoom, clear the
  // pending state, and snap on the upscaled lower tier — the soft-1:1 flash
  // this deferral exists to prevent. Compare by tier rank (not equality) so a
  // source that is already sharper than required — e.g. /original kept during
  // 1:1-preserving navigation — still applies immediately instead of waiting
  // forever for a downgrade that never comes.
  var requiredSource = _lbPickSourceKey(_lbNativeZoom);
  if (_lbSrcRank(_lbCurrentSrcKey) < _lbSrcRank(requiredSource)) {
    _lbScheduleSourceSwap(_lbNativeZoom);
    return;
  }
  _lbPending1To1 = false;
  var anchor = _lbPending1To1Anchor;
  _lbPending1To1Anchor = null;
  _lbSetZoom(
    _lbNativeZoom,
    anchor ? anchor.x : null,
    anchor ? anchor.y : null
  );
}

function _lbDeferPendingOneToOneUntilSourceReady(targetZoom) {
  if (!_lbPending1To1) return false;
  _lbPendingViewportState = null;
  _lbApplyTransform();
  _lbScheduleSourceSwap(targetZoom);
  if (_lbDesiredSrcKey === _lbCurrentSrcKey) {
    _lbApplyPendingOneToOneZoom();
  }
  return true;
}

function _lbDeferPendingOneToOneToPreviewFallback() {
  // /original is unavailable but the user still wants true 1:1. Keep the
  // deferral pending and aim at the sharpest remaining preview tier instead of
  // snapping on an upscaled /full.
  return _lbDeferPendingOneToOneUntilSourceReady(4);
}

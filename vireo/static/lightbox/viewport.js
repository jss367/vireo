function _lbDisplayDimsForRecipe(width, height, recipe) {
  var w = Number(width) || 0;
  var h = Number(height) || 0;
  if (!w || !h) return { w: null, h: null };
  recipe = recipe || {};
  var rotation = Number(recipe.rotation || 0);
  if (rotation === 90 || rotation === 270) {
    var tmp = w; w = h; h = tmp;
  }
  var crop = recipe.crop;
  if (crop && Number(crop.w) > 0 && Number(crop.h) > 0) {
    w = Math.max(1, Math.round(w * Number(crop.w)));
    h = Math.max(1, Math.round(h * Number(crop.h)));
  }
  return { w: w, h: h };
}

function _lbSrcRank(key) {
  // Tier ordering by available source resolution (higher = sharper).
  if (key === 'original') return 3;
  if (key === '3840') return 2;
  if (key === '2560') return 1;
  return 0; // 'full', '1920', or unknown
}

/* Viewport state and interaction lifecycle. Constructing the factory is inert;
 * beginPhoto attaches listeners, and close retires drag/resize work.
 */
(function(root) {
  'use strict';
  function create(options) {
    var window = options.window || root;
    var document = window.document;
    var _lbZoom = 1.0;          // current zoom (1.0 = fit)
    var _lbPanX = 0;            // pan translation in CSS pixels
    var _lbPanY = 0;
    var _lbNativeZoom = null;   // zoom value corresponding to 1:1 for current photo
    var _lbFitScale = 1.0;      // natural image scale at zoom=1.0
    var _lbPending1To1 = false;  // true when z/click was pressed with unknown nativeZoom; upgrade to true 1:1 once learned
    var _lbPending1To1Anchor = null; // optional client-space anchor for a deferred 1:1 snap
    var _lbViewportByPhotoId = {};  // per-session lightbox viewport cache keyed by photo id
    var _lbPendingViewportState = null;
    var _lbPendingEyeTrack = null; // destination alignment waiting for image metadata/layout
    var _lbEyeTrackScreenAnchor = null; // eye offset from viewport center in CSS pixels

    function _lbApplyTransform() {
      var t = document.getElementById('lightboxTransform');
      if (!t) return;
      var metrics = _lbUpdateLayoutMetrics();
      if (!metrics) {
        _lbUpdateZoomControl();
        return;
      }
      var renderedW = metrics.w * metrics.scale;
      var renderedH = metrics.h * metrics.scale;
      var tx = (metrics.wrapW - renderedW) / 2 + _lbPanX;
      var ty = (metrics.wrapH - renderedH) / 2 + _lbPanY;
      t.style.transform = 'translate(' + tx + 'px, ' + ty + 'px) scale(' + metrics.scale + ')';
      _lbUpdateZoomControl();
    }

    function _lbOrientedPhotoDims(img) {
      if (!options.photo().width || !options.photo().height) return null;
      var w = options.photo().width;
      var h = options.photo().height;
      if (options.orientationSwapsAxes(options.photo().orientation)) {
        w = options.photo().height;
        h = options.photo().width;
      } else if (img && img.naturalWidth && img.naturalHeight) {
        var storedAspect = w / h;
        var imgAspect = img.naturalWidth / img.naturalHeight;
        if (Math.abs(storedAspect - imgAspect) > Math.abs((1 / storedAspect) - imgAspect)) {
          w = options.photo().height;
          h = options.photo().width;
        }
      }
      return { w: w, h: h };
    }

    function _lbLayoutDims() {
      var img = document.getElementById('lightboxImg');
      if (!img) return null;
      // A developed companion may have been cropped or resized independently of
      // its RAW primary. Use the pixels that actually loaded so the lightbox never
      // stretches the JPEG into the RAW row's catalog dimensions.
      if (
        options.photoId() != null &&
        options.photo().pairKnown &&
        options.photo().pairSource === 'jpeg' &&
        img.naturalWidth && img.naturalHeight
      ) {
        return { w: img.naturalWidth, h: img.naturalHeight };
      }
      // While /original is available, keep the transform layer in original photo
      // coordinates even if the currently displayed tier is /full. That matches the
      // sharp natural-layout path and avoids recalibrating pan/zoom when the source swaps.
      var originalDims = !options.photo().originalUnavailable ? _lbOrientedPhotoDims(img) : null;
      if (originalDims) {
        return _lbDisplayDimsForRecipe(
          originalDims.w,
          originalDims.h,
          options.photo().recipe
        );
      }
      if (img.naturalWidth && img.naturalHeight) {
        return { w: img.naturalWidth, h: img.naturalHeight };
      }
      return null;
    }

    function _lbUpdateLayoutMetrics() {
      var wrap = document.getElementById('lightboxWrap');
      var t = document.getElementById('lightboxTransform');
      if (!wrap || !t) return null;
      var dims = _lbLayoutDims();
      if (!dims || !dims.w || !dims.h) return null;
      var wrapW = wrap.clientWidth;
      var wrapH = wrap.clientHeight;
      if (!wrapW || !wrapH) return null;
      _lbFitScale = Math.min(1.0, wrapW / dims.w, wrapH / dims.h);
      if (!_lbFitScale || _lbFitScale <= 0) _lbFitScale = 1.0;
      t.style.width = dims.w + 'px';
      t.style.height = dims.h + 'px';
      var scale = _lbFitScale * _lbZoom;
      return { w: dims.w, h: dims.h, wrapW: wrapW, wrapH: wrapH, scale: scale };
    }

    function _lbRecomputeNativeZoom() {
      // Compute zoom value that corresponds to 1:1: one decoded source pixel per
      // physical display pixel. On Retina/HiDPI displays this is intentionally
      // smaller than CSS 1:1 by devicePixelRatio.
      var img = document.getElementById('lightboxImg');
      if (!img || !img.complete || !img.naturalWidth || !img.naturalHeight) {
        _lbNativeZoom = null;
        return;
      }
      if (!options.photo().width && !options.photo().height && !options.photo().originalUnavailable && options.photo().currentSrcKey !== 'original') {
        // A preview/full tier can load before the metadata request returns. Do not
        // treat that tier's decoded size as true 1:1; keep z/click in pending mode
        // so the later original dimensions can upgrade the zoom accurately.
        _lbNativeZoom = null;
        return;
      }
      var metrics = _lbUpdateLayoutMetrics();
      if (!metrics || !_lbFitScale) {
        _lbNativeZoom = null;
        return;
      }
      var dpr = window.devicePixelRatio || 1;
      // Guard: very small images (nativeZoom < 1) clamp to 1.0 so fit == 1:1
      _lbNativeZoom = Math.max(1.0, (1 / dpr) / _lbFitScale);
      _lbApplyTransform();
    }

    function _lbSyncSourceForZoom(targetZoom, preserveSharperSource) {
      var desiredSource = options.pickSource(targetZoom);
      if (preserveSharperSource && _lbSrcRank(options.photo().currentSrcKey) > _lbSrcRank(desiredSource)) {
        // The current pixels already exceed this zoom's requirement. Cancel any
        // queued downgrade and retain them; this is especially important for the
        // explicit 1:1 stop, where replacing /original with a lower tier adds work
        // and can leave a failed non-original swap stranded.
        options.cancelSourceSwap();
        options.keepSource(options.photo().currentSrcKey);
        options.scheduleAdjacent(options.photo().currentSrcKey);
        return;
      }
      options.scheduleSource(targetZoom);
    }

    function _lbSetZoom(newZoom, anchorClientX, anchorClientY, preserveSharperSource) {
      // Any zoom mutation cancels a pending "upgrade to 1:1 on original-load" intent.
      // Callers that still want it (toggleLightboxZoom's fallback path) re-set the flag
      // after calling us.
      _lbPending1To1 = false;
      _lbPending1To1Anchor = null;
      // Clamp to valid range. Max is 4x native (or 4x fit if no nativeZoom).
      var maxZoom = _lbMaxZoom();
      newZoom = Math.max(1.0, Math.min(maxZoom, newZoom));
      var wrap = document.getElementById('lightboxWrap');
      var t = document.getElementById('lightboxTransform');
      if (!wrap || !t) return;

      var metrics = _lbUpdateLayoutMetrics();
      if (!metrics) {
        _lbZoom = newZoom;
        if (_lbZoom <= 1.001) {
          _lbPanX = 0;
          _lbPanY = 0;
        }
        wrap.classList.toggle('zoomed', _lbZoom > 1.001);
        _lbApplyTransform();
        _lbSyncSourceForZoom(newZoom, preserveSharperSource);
        _lbSaveViewportState(options.photoId());
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
      var newScale = _lbFitScale * newZoom;
      var newBaseX = anchorX - imgX * newScale;
      var newBaseY = anchorY - imgY * newScale;
      var wrapRectForBase = wrap.getBoundingClientRect();
      _lbPanX = newBaseX - (wrapRectForBase.left + (wrapRectForBase.width - metrics.w * newScale) / 2);
      _lbPanY = newBaseY - (wrapRectForBase.top + (wrapRectForBase.height - metrics.h * newScale) / 2);
      _lbZoom = newZoom;

      if (_lbZoom <= 1.001) {
        _lbPanX = 0;
        _lbPanY = 0;
      }

      _lbClampPan();
      wrap.classList.toggle('zoomed', _lbZoom > 1.001);
      _lbApplyTransform();
      _lbSyncSourceForZoom(newZoom, preserveSharperSource);
      _lbSaveViewportState(options.photoId());
    }

    function _lbClampPan() {
      var metrics = _lbUpdateLayoutMetrics();
      if (!metrics) return;
      // Effective rendered size after scale
      var scaledW = metrics.w * metrics.scale;
      var scaledH = metrics.h * metrics.scale;
      var baseLeft = (metrics.wrapW - scaledW) / 2;
      var baseTop = (metrics.wrapH - scaledH) / 2;
      // After applying transform, rendered top-left is base + pan.
      // Clamp so at least minVisible px of image stays inside the wrap, per axis.
      var minVisible = 80;
      // X: rendered right >= minVisible; rendered left <= wrapW - minVisible
      //   panX >= minVisible - baseLeft - scaledW
      //   panX <= wrapW - minVisible - baseLeft
      var minPanX = minVisible - baseLeft - scaledW;
      var maxPanX = metrics.wrapW - minVisible - baseLeft;
      if (scaledW <= metrics.wrapW) {
        // Image fits horizontally; center it
        _lbPanX = 0;
      } else {
        _lbPanX = Math.max(minPanX, Math.min(maxPanX, _lbPanX));
      }
      var minPanY = minVisible - baseTop - scaledH;
      var maxPanY = metrics.wrapH - minVisible - baseTop;
      if (scaledH <= metrics.wrapH) {
        _lbPanY = 0;
      } else {
        _lbPanY = Math.max(minPanY, Math.min(maxPanY, _lbPanY));
      }
    }

    function _lbIsOneToOneZoom() {
      if (_lbPending1To1) return true;
      if (!_lbNativeZoom || _lbNativeZoom <= 1.001) return false;
      var tolerance = Math.max(0.01, _lbNativeZoom * 0.01);
      return Math.abs(_lbZoom - _lbNativeZoom) <= tolerance;
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
      if (!options.photo().trackEyeEnabled || (_lbZoom <= 1.001 && !_lbPending1To1)) return null;
      // During rapid navigation the DOM can still contain the outgoing bitmap.
      // Reuse the last trustworthy screen anchor instead of measuring that bitmap
      // against the incoming photo id.
      if (options.photo().transitionPending) {
        return _lbEyeTrackScreenAnchor
          ? { offsetX: _lbEyeTrackScreenAnchor.offsetX, offsetY: _lbEyeTrackScreenAnchor.offsetY }
          : null;
      }
      var photo = options.photoData(options.photoId());
      var point = options.eyePoint(options.photoId(), photo);
      var overlaysAvailable = options.overlaysAvailable();
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
      if (!pending || !options.photo().trackEyeEnabled) return false;
      if (String(pending.photoId) !== String(options.photoId())) return false;
      photo = photo || options.photoData(options.photoId());
      // A missing object means the detail request has not resolved yet. Keep the
      // alignment armed so its callback can apply it after the image is laid out.
      // Navigation lists on some pages contain only {id, filename}; those are
      // also "unknown", not evidence that the destination lacks an eye.
      if (!photo || !Object.prototype.hasOwnProperty.call(photo, 'eye_x')) return false;
      var point = options.eyePoint(options.photoId(), photo);
      var metrics = _lbUpdateLayoutMetrics();
      if (!point || !options.overlaysAvailable()) {
        _lbPendingEyeTrack = null;
        options.updateEyeControl();
        return false;
      }
      if (!metrics || options.photo().transitionPending) return false;

      // A deferred 1:1 snap is still armed — the loader is holding the current
      // display upscaled while a sharper source (2560/3840 or /original) swap
      // completes. Applying the viewport here would re-enter
      // _lbApplyViewportState(), whose one-to-one branch would either clear
      // _lbPending1To1 (when nativeZoom is known) or leave it armed but whose
      // trailing options.scheduleSource() can retarget back to /full when
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
      _lbSaveViewportState(options.photoId());
      options.updateEyeControl();
      return true;
    }

    function _lbSaveViewportState(photoId) {
      if (photoId == null) return null;
      // Mid-navigation the DOM transform still belongs to the outgoing photo but
      // options.photoId() has already advanced to the incoming id. Reading from
      // the DOM here would misattribute the frozen bitmap to the incoming photo
      // and stomp its intended inspection point. Prefer the pending restore
      // state (what handleInitialImageLoad is about to apply), and otherwise
      // leave any previously saved state alone.
      if (options.photo().transitionPending && String(photoId) === String(options.photoId())) {
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
      options.scheduleSource();
      return true;
    }

    function _lbClearPendingViewportRestore() {
      options.cancelProgressiveLoad();
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
      if (options.photo().originalUnavailable && _lbSrcRank(options.photo().desiredSrcKey) > _lbSrcRank(options.photo().currentSrcKey)) return;
      // Don't snap to true 1:1 until the displayed source is at least the tier
      // that 1:1 needs. Otherwise the /api/photos/<id> metadata fetch resolving
      // before the held /original load would learn _lbNativeZoom, clear the
      // pending state, and snap on the upscaled lower tier — the soft-1:1 flash
      // this deferral exists to prevent. Compare by tier rank (not equality) so a
      // source that is already sharper than required — e.g. /original kept during
      // 1:1-preserving navigation — still applies immediately instead of waiting
      // forever for a downgrade that never comes.
      var requiredSource = options.pickSource(_lbNativeZoom);
      if (_lbSrcRank(options.photo().currentSrcKey) < _lbSrcRank(requiredSource)) {
        options.scheduleSource(_lbNativeZoom);
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
      options.scheduleSource(targetZoom);
      if (options.photo().desiredSrcKey === options.photo().currentSrcKey) {
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

    function _lbMaxZoom() {
      return _lbNativeZoom ? _lbNativeZoom * 4 : 4;
    }

    function _lbZoomSliderPosition(zoom) {
      var maxZoom = _lbMaxZoom();
      if (maxZoom <= 1.001) return 0;
      return Math.max(0, Math.min(1000, Math.log(Math.max(1, zoom)) / Math.log(maxZoom) * 1000));
    }

    function _lbZoomFromSliderPosition(position) {
      var maxZoom = _lbMaxZoom();
      if (maxZoom <= 1.001) return 1;
      var ratio = Math.max(0, Math.min(1, Number(position) / 1000));
      return Math.exp(Math.log(maxZoom) * ratio);
    }

    function _lbZoomDisplayText() {
      if (_lbPending1To1) return 'Loading 1:1';
      if (_lbZoom <= 1.001) return 'Fit';
      if (_lbNativeZoom) return Math.round((_lbZoom / _lbNativeZoom) * 100) + '%';
      var fitMultiple = Math.round(_lbZoom * 10) / 10;
      return fitMultiple + '\u00d7 Fit';
    }

    function _lbNativeSliderPercent() {
      return _lbNativeZoom ? _lbZoomSliderPosition(_lbNativeZoom) / 10 : 0;
    }

    function _lbNativeCoincidesWithFit() {
      return !!_lbNativeZoom && _lbNativeZoom <= 1.001;
    }

    function _lbUpdateZoomControl() {
      var badge = document.getElementById('lightboxZoomBadge');
      var slider = document.getElementById('lightboxZoomSlider');
      var nativeStop = document.getElementById('lightboxZoomNativeStop');
      var fitStop = document.querySelector('.lb-zoom-stop-fit');
      var maxLabel = document.getElementById('lightboxZoomMaxLabel');
      var zoomOut = document.getElementById('lightboxZoomOut');
      var zoomIn = document.getElementById('lightboxZoomIn');
      var text = _lbZoomDisplayText();

      if (badge) {
        badge.textContent = text;
        badge.title = 'Zoom: ' + text + '. Open zoom controls';
      }
      if (slider) {
        slider.value = String(Math.round(_lbZoomSliderPosition(_lbZoom)));
        slider.setAttribute('aria-valuetext', text);
        slider.disabled = !!options.photo().transitionPending;
      }
      if (zoomOut) zoomOut.disabled = options.photo().transitionPending || _lbZoom <= 1.001;
      if (zoomIn) zoomIn.disabled = options.photo().transitionPending || _lbZoom >= _lbMaxZoom() - 0.001;

      var combinedFitNative = _lbNativeCoincidesWithFit();
      if (fitStop) {
        fitStop.textContent = combinedFitNative ? 'Fit \u00b7 1:1' : 'Fit';
        fitStop.disabled = !!options.photo().transitionPending;
      }
      if (nativeStop) {
        var nativePosition = _lbNativeSliderPercent();
        nativeStop.disabled = !!options.photo().transitionPending;
        // When fit and 1:1 are visually indistinguishable, combine the labels at the
        // left edge. Otherwise keep 1:1 at least 8% along the track so a near-fit
        // native stop remains separate and clickable without overlapping Fit.
        nativeStop.style.display = (_lbNativeZoom && !combinedFitNative) ? 'inline-block' : 'none';
        nativeStop.style.left = Math.max(8, nativePosition) + '%';
      }
      if (maxLabel) {
        maxLabel.textContent = _lbNativeZoom ? '400%' : '4\u00d7 Fit';
        maxLabel.disabled = !!options.photo().transitionPending;
      }
    }

    function _lbSetZoomPopoverOpen(open) {
      var popover = document.getElementById('lightboxZoomPopover');
      var badge = document.getElementById('lightboxZoomBadge');
      if (popover) popover.classList.toggle('open', !!open);
      if (badge) badge.setAttribute('aria-expanded', open ? 'true' : 'false');
    }

    function toggleLightboxZoomPopover() {
      var popover = document.getElementById('lightboxZoomPopover');
      _lbSetZoomPopoverOpen(!(popover && popover.classList.contains('open')));
    }

    function setLightboxZoomFromSlider(position) {
      if (options.photo().transitionPending) return;
      _lbClearPendingViewportRestore();
      _lbSetZoom(_lbZoomFromSliderPosition(position), null, null);
    }

    function stepLightboxZoom(direction) {
      if (options.photo().transitionPending) return;
      _lbClearPendingViewportRestore();
      _lbSetZoom(_lbZoom * (direction > 0 ? 1.25 : 0.8), null, null);
    }

    function setLightboxZoomToFit() {
      if (options.photo().transitionPending) return;
      _lbClearPendingViewportRestore();
      _lbSetZoom(1.0, null, null);
    }

    function setLightboxZoomToOneToOne(e) {
      if (options.photo().transitionPending) return;
      var img = document.getElementById('lightboxImg');
      if (!img) return;
      _lbClearPendingViewportRestore();

      // If we don't know native zoom yet (e.g. dimensions fetch still pending),
      // fall back to old two-tier behavior: load original, no continuous control.
      if (!_lbNativeZoom) {
        _lbRecomputeNativeZoom();
      }

      // Anchor zoom on click position (or center for the slider's 1:1 stop).
      var rect = img.getBoundingClientRect();
      var anchorX = e && e.clientX ? e.clientX : rect.left + rect.width / 2;
      var anchorY = e && e.clientY ? e.clientY : rect.top + rect.height / 2;
      if (_lbNativeZoom) {
        var desiredSource = options.pickSource(_lbNativeZoom);
        // Compare source ranks (not exact key equality) so an already-sharper
        // current tier — e.g. /original loaded during a previous 1:1 view when
        // the picked tier for _lbNativeZoom is /2560 — satisfies 1:1 immediately.
        // The exact-key check would otherwise enter the deferred path, leave the
        // badge at 'Loading 1:1' while a lower tier is fetched, and could strand
        // _lbPending1To1 if that fetch failed.
        if (_lbSrcRank(options.photo().currentSrcKey) < _lbSrcRank(desiredSource)) {
          _lbPending1To1 = true;
          _lbPending1To1Anchor = { x: anchorX, y: anchorY };
          options.scheduleSource(_lbNativeZoom);
          _lbApplyTransform();
        } else {
          _lbSetZoom(_lbNativeZoom, anchorX, anchorY, true);
          _lbPending1To1 = false;
          _lbPending1To1Anchor = null;
        }
      } else {
        // Dimensions unknown. Load the high-resolution source first, then apply
        // 1:1 after the browser has decoded it; otherwise the user sees an
        // enlarged preview and reads that as a soft 1:1 view.
        _lbPending1To1 = true;
        _lbPending1To1Anchor = { x: anchorX, y: anchorY };
        options.scheduleSource(4);
        _lbApplyTransform();
      }
    }

    function toggleLightboxZoom(e) {
      // Fit to 1:1 toggle. Preserved for the existing 'z' shortcut and image click.
      // A deferred 1:1 leaves _lbZoom at fit while the sharp source loads, so treat
      // that pending state as zoomed and allow a second toggle to cancel it.
      if (_lbZoom > 1.001 || _lbPending1To1) {
        setLightboxZoomToFit();
      } else {
        setLightboxZoomToOneToOne(e);
      }
    }

    var active = false;
    var resizeTimer = null;
    var refreshSourceTier = false;
    var deferredRefreshPending = false;
    var layoutObserver = null;
    var dragging = false, didDrag = false, startX, startY, panStartX, panStartY;
    var DRAG_THRESHOLD = 5;

    function beginPhoto(photoId, openOptions) {
      openOptions = openOptions || {};
      var fallbackViewportState = openOptions.fallbackViewportState || null;
      var eyeTrackAnchor = openOptions.eyeTrackAnchor || null;
      var preserveOneToOne = !!openOptions.preserveOneToOne;
      var restoreViewportState = _lbViewportStateForOpen(photoId, fallbackViewportState);
      if (!restoreViewportState && preserveOneToOne) {
        restoreViewportState = {
          zoom: Math.max(1.0, _lbZoom || _lbNativeZoom || 1.0),
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
      if (preserveOneToOne && restoreWantsOneToOne && restoreViewportState.zoom <= 1.001) {
        restoreViewportState.zoom = Math.max(
          1.002,
          fallbackViewportState && fallbackViewportState.zoom || 1.0,
          _lbZoom || 1.0,
          _lbNativeZoom || 1.0
        );
      }
      var transitionZoom = restoreViewportState
        ? Math.max(1.0, restoreViewportState.zoom || 1.0)
        : 1.0;

      dragging = false;
      didDrag = false;
      // Carry a resize's intent across navigation, but never apply the outgoing
      // timer against the incoming photo's metadata.
      if (resizeTimer !== null) {
        window.clearTimeout(resizeTimer);
        resizeTimer = null;
        deferredRefreshPending = true;
      }
      _lbZoom = transitionZoom;
      _lbPanX = 0;
      _lbPanY = 0;
      _lbNativeZoom = null;
      _lbPending1To1 = restoreWantsOneToOne;
      _lbPending1To1Anchor = null;
      _lbPendingViewportState = _lbCloneViewportState(restoreViewportState);
      _lbPendingEyeTrack = (options.photo().trackEyeEnabled && eyeTrackAnchor) ? {
        photoId: photoId,
        offsetX: Number(eyeTrackAnchor.offsetX) || 0,
        offsetY: Number(eyeTrackAnchor.offsetY) || 0
      } : null;
      start();
      return Object.freeze({zoom: transitionZoom, oneToOne: restoreWantsOneToOne});
    }

    function clearEyeTracking() {
      _lbPendingEyeTrack = null;
      _lbEyeTrackScreenAnchor = null;
    }

    function cancelPendingZoom() {
      _lbPending1To1 = false;
      _lbPending1To1Anchor = null;
      _lbPendingViewportState = null;
    }

    function resetForEdit() {
      dragging = false;
      _lbZoom = 1;
      _lbPanX = 0;
      _lbPanY = 0;
      _lbNativeZoom = null;
      cancelPendingZoom();
    }

    function onWheel(e) {
      var wrap = document.getElementById('lightboxWrap');
      if (!active || !wrap || (!wrap.contains(e.target) && e.target !== wrap)) return;
      if (e.target.closest('.lightbox-zoom-control')) return;
      e.preventDefault();
      if (options.photo().transitionPending) return;
      var scale = e.ctrlKey ? 0.02 : 0.0015;
      _lbClearPendingViewportRestore();
      _lbSetZoom(_lbZoom * Math.exp(-e.deltaY * scale), e.clientX, e.clientY);
    }

    function scheduleLayoutRefresh(updateSourceTier) {
      if (!active) return;
      refreshSourceTier = refreshSourceTier || !!updateSourceTier;
      if (resizeTimer !== null) window.clearTimeout(resizeTimer);
      var timer = window.setTimeout(function() {
        if (!active || resizeTimer !== timer) return;
        resizeTimer = null;
        var shouldUpdateSourceTier = refreshSourceTier;
        refreshSourceTier = false;
        // A resize must not lay out the frozen outgoing bitmap with incoming dimensions.
        if (options.photo().transitionPending) {
          refreshSourceTier = shouldUpdateSourceTier || refreshSourceTier;
          deferredRefreshPending = true;
          return;
        }
        _lbRecomputeNativeZoom();
        // Re-clamping during a deferred 1:1 must preserve both intent and its anchor.
        var pendingDesiredSource = options.photo().desiredSrcKey;
        if (_lbPending1To1) {
          _lbClampPan();
          _lbApplyTransform();
          if (shouldUpdateSourceTier) {
            if (!pendingDesiredSource || pendingDesiredSource === options.photo().currentSrcKey) {
              options.scheduleSource(_lbNativeZoom || 4);
            } else {
              options.keepSource(pendingDesiredSource);
            }
            if (options.photo().desiredSrcKey === options.photo().currentSrcKey) {
              _lbApplyPendingOneToOneZoom();
            }
          }
          return;
        }
        if (shouldUpdateSourceTier) {
          _lbSetZoom(_lbZoom, null, null);
        } else {
          _lbClampPan();
          _lbApplyTransform();
          _lbSaveViewportState(options.photoId());
        }
      }, 100);
      resizeTimer = timer;
    }

    function onResize() { scheduleLayoutRefresh(true); }
    function flushDeferredLayout() {
      if (!deferredRefreshPending) return;
      deferredRefreshPending = false;
      scheduleLayoutRefresh(false);
    }

    function onMouseDown(e) {
      if (!active || e.button !== 0 || options.photo().transitionPending) return;
      var wrap = document.getElementById('lightboxWrap');
      if (!wrap || _lbZoom <= 1.001) return;
      if (e.target.tagName === 'BUTTON' || e.target.closest('button')) return;
      if (e.target.closest('.lightbox-zoom-control')) return;
      if (!wrap.contains(e.target) && e.target !== wrap) return;
      dragging = true;
      didDrag = false;
      startX = e.clientX;
      startY = e.clientY;
      panStartX = _lbPanX;
      panStartY = _lbPanY;
      e.preventDefault();
    }

    function onMouseMove(e) {
      if (!active || !dragging) return;
      var dx = e.clientX - startX;
      var dy = e.clientY - startY;
      if (!didDrag && (Math.abs(dx) > DRAG_THRESHOLD || Math.abs(dy) > DRAG_THRESHOLD)) {
        didDrag = true;
        _lbClearPendingViewportRestore();
      }
      _lbPanX = panStartX + dx;
      _lbPanY = panStartY + dy;
      _lbClampPan();
      _lbApplyTransform();
    }

    function onMouseUp() {
      if (!active || !dragging) return;
      dragging = false;
      if (!didDrag && _lbZoom > 1.001) {
        _lbClearPendingViewportRestore();
        _lbSetZoom(1, null, null);
        options.handledClick();
      } else if (didDrag) {
        _lbSaveViewportState(options.photoId());
      }
    }

    function start() {
      if (active) return;
      active = true;
      document.addEventListener('wheel', onWheel, {passive: false});
      document.addEventListener('mousedown', onMouseDown);
      document.addEventListener('mousemove', onMouseMove);
      document.addEventListener('mouseup', onMouseUp);
      window.addEventListener('resize', onResize);
      var wrap = document.getElementById('lightboxWrap');
      if (window.ResizeObserver && wrap) {
        var observer = new window.ResizeObserver(function() {
          if (layoutObserver === observer) scheduleLayoutRefresh(false);
        });
        layoutObserver = observer;
        observer.observe(wrap);
      }
    }

    function close() {
      var wasActive = active;
      active = false;
      dragging = false;
      didDrag = false;
      if (resizeTimer !== null) window.clearTimeout(resizeTimer);
      resizeTimer = null;
      refreshSourceTier = false;
      deferredRefreshPending = false;
      if (layoutObserver) layoutObserver.disconnect();
      layoutObserver = null;
      if (wasActive) {
        document.removeEventListener('wheel', onWheel);
        document.removeEventListener('mousedown', onMouseDown);
        document.removeEventListener('mousemove', onMouseMove);
        document.removeEventListener('mouseup', onMouseUp);
        window.removeEventListener('resize', onResize);
      }
      resetForEdit();
      _lbClearPendingViewportRestore();
      var wrap = document.getElementById('lightboxWrap');
      if (wrap) wrap.classList.remove('zoomed');
      _lbSetZoomPopoverOpen(false);
    }

    function frozenCopy(value) { return value ? Object.freeze(Object.assign({}, value)) : null; }
    function snapshot() {
      return Object.freeze({
        zoom: _lbZoom, panX: _lbPanX, panY: _lbPanY, nativeZoom: _lbNativeZoom, fitScale: _lbFitScale,
        pendingOneToOne: _lbPending1To1, oneToOneAnchor: frozenCopy(_lbPending1To1Anchor),
        pendingRestore: frozenCopy(_lbPendingViewportState), pendingEye: frozenCopy(_lbPendingEyeTrack),
        eyeAnchor: frozenCopy(_lbEyeTrackScreenAnchor), active: active
      });
    }
    return Object.freeze({
      beginPhoto: beginPhoto, close: close, snapshot: snapshot,
      resetForEdit: resetForEdit, cancelPendingZoom: cancelPendingZoom,
      clearEyeTracking: clearEyeTracking, flushDeferredLayout: flushDeferredLayout,
      invalidateGeometry: function() { _lbNativeZoom = null; },
      zoom: function() { return _lbZoom; },
      nativeZoom: function() { return _lbNativeZoom; },
      fitScale: function() { return _lbFitScale; },
      pendingOneToOne: function() { return _lbPending1To1; },
      hasPendingRestore: function() { return _lbPendingViewportState !== null; },
      savedView: function(id) { return frozenCopy(_lbViewportByPhotoId[String(id)]); },
      applyTransform: _lbApplyTransform,
      layoutDims: _lbLayoutDims,
      layoutMetrics: _lbUpdateLayoutMetrics,
      recomputeNativeZoom: _lbRecomputeNativeZoom,
      setZoom: _lbSetZoom,
      isOneToOne: _lbIsOneToOneZoom,
      currentView: _lbViewportStateFromCurrent,
      captureEyeAnchor: _lbCaptureEyeTrackingAnchor,
      applyPendingEye: _lbTryApplyPendingEyeTrack,
      save: _lbSaveViewportState,
      applyView: _lbApplyViewportState,
      cancelRestore: _lbClearPendingViewportRestore,
      applyPendingRestore: _lbTryApplyPendingViewportState,
      applyPendingOneToOne: _lbApplyPendingOneToOneZoom,
      deferOneToOne: _lbDeferPendingOneToOneUntilSourceReady,
      deferOneToOneFallback: _lbDeferPendingOneToOneToPreviewFallback,
      updateControls: _lbUpdateZoomControl,
      setPopoverOpen: _lbSetZoomPopoverOpen,
      togglePopover: toggleLightboxZoomPopover,
      setFromSlider: setLightboxZoomFromSlider,
      stepZoom: stepLightboxZoom,
      fit: setLightboxZoomToFit,
      oneToOne: setLightboxZoomToOneToOne,
      toggleZoom: toggleLightboxZoom,
    });
  }
  root.VireoLightboxViewport = Object.freeze({create: create});
})(window);

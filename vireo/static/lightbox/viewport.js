
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
  if (!_lbPhotoW || !_lbPhotoH) return null;
  var w = _lbPhotoW;
  var h = _lbPhotoH;
  if (_lbOrientationSwapsAxes(_lbPhotoOrientation)) {
    w = _lbPhotoH;
    h = _lbPhotoW;
  } else if (img && img.naturalWidth && img.naturalHeight) {
    var storedAspect = w / h;
    var imgAspect = img.naturalWidth / img.naturalHeight;
    if (Math.abs(storedAspect - imgAspect) > Math.abs((1 / storedAspect) - imgAspect)) {
      w = _lbPhotoH;
      h = _lbPhotoW;
    }
  }
  return { w: w, h: h };
}

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

function _lbLayoutDims() {
  var img = document.getElementById('lightboxImg');
  if (!img) return null;
  // A developed companion may have been cropped or resized independently of
  // its RAW primary. Use the pixels that actually loaded so the lightbox never
  // stretches the JPEG into the RAW row's catalog dimensions.
  if (
    vireoLightboxSession.requestedPhotoId() != null &&
    _vireoPairKnownByPhoto[String(vireoLightboxSession.requestedPhotoId())] &&
    _vireoPairSource(vireoLightboxSession.requestedPhotoId()) === 'jpeg' &&
    img.naturalWidth && img.naturalHeight
  ) {
    return { w: img.naturalWidth, h: img.naturalHeight };
  }
  // While /original is available, keep the transform layer in original photo
  // coordinates even if the currently displayed tier is /full. That matches the
  // sharp natural-layout path and avoids recalibrating pan/zoom when the source swaps.
  var originalDims = !_lbOriginalUnavailable ? _lbOrientedPhotoDims(img) : null;
  if (originalDims) {
    return _lbDisplayDimsForRecipe(
      originalDims.w,
      originalDims.h,
      _lbCurrentEditRecipe
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
  if (!_lbPhotoW && !_lbPhotoH && !_lbOriginalUnavailable && _lbCurrentSrcKey !== 'original') {
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
  var desiredSource = _lbPickSourceKey(targetZoom);
  if (preserveSharperSource && _lbSrcRank(_lbCurrentSrcKey) > _lbSrcRank(desiredSource)) {
    // The current pixels already exceed this zoom's requirement. Cancel any
    // queued downgrade and retain them; this is especially important for the
    // explicit 1:1 stop, where replacing /original with a lower tier adds work
    // and can leave a failed non-original swap stranded.
    vireoLightboxSession.cancelSwap();
    _lbDesiredSrcKey = _lbCurrentSrcKey;
    vireoLightboxSession.scheduleAdjacent(_lbCurrentSrcKey);
    return;
  }
  _lbScheduleSourceSwap(targetZoom);
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
    _lbSaveViewportState(vireoLightboxSession.requestedPhotoId());
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
  _lbSaveViewportState(vireoLightboxSession.requestedPhotoId());
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

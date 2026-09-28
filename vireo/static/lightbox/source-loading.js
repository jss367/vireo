
var _lbSwapTimer = null;
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
// openLightbox publishes its completion here so anything that takes the image
// loader over mid-load can still end that load properly. Replacing
// img.onload/img.onerror without this leaves the navigation transition frozen:
// the action bar stays inert, deferred overlays never paint, and the status chip
// never clears, all long after the replacement bitmap has rendered.
var _lbPendingInitialLoadCommit = null;
var _lbPendingInitialLoadAbandon = null;

function _lbClearPendingInitialLoad() {
  _lbPendingInitialLoadCommit = null;
  _lbPendingInitialLoadAbandon = null;
}

function _lbFinishInitialLoad() {
  var finish = _lbPendingInitialLoadCommit;
  if (!finish) return false;
  _lbClearPendingInitialLoad();
  finish();
  return true;
}

// The failure counterpart. A displaced load that will never produce a bitmap
// still has to hand over: the incoming photo's identity must be committed
// before its controls come back, or the user can act on a photo the filename,
// counter and _lightboxCommittedId are not showing.
function _lbAbandonInitialLoad() {
  var abandon = _lbPendingInitialLoadAbandon;
  if (!abandon) return false;
  _lbClearPendingInitialLoad();
  abandon();
  return true;
}
var _lbDetailStatusShown = false;    // chip is on screen (past the show delay)
var _lbDetailStatusSettled = false;  // showing the Full detail confirmation
var _lbDetailStatusHold = 0;         // >0 suppresses paints mid-transition
var _lbDetailShowTimer = null;
var _lbDetailSettleTimer = null;
var _lbDetailFadeTimer = null;

function _lbDetailSharpeningPending() {
  // Derived rather than read off _lbPreviewLoading: the first drag of a pan
  // calls _lbClearPendingViewportRestore, which drops that flag even though the
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
  // than the file does -- and when it does, _lbLayoutDims rebases onto the
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

function _lbDecodedInitialPreview(photoId, desired) {
  var entries = Object.values(_lbAdjacentPreloads);
  if (_lbOriginalPreload) entries.push(_lbOriginalPreload);
  var ready = entries.filter(function(entry) {
    return entry.photoId === photoId && entry.status === 'decoded' &&
      entry.url === _lbSrcUrl(photoId, entry.sourceKey, true);
  });
  var exact = ready.find(function(entry) { return entry.sourceKey === desired; });
  if (exact) return exact;
  // A ready smaller preview is safe only for this exact render/source version.
  // Do not flash an old crop, an unedited thumbnail, or the other RAW/JPEG pair.
  return ready.filter(function(entry) {
    return _lbSrcRank(entry.sourceKey) <= _lbSrcRank(desired);
  }).sort(function(a, b) { return _lbSrcRank(b.sourceKey) - _lbSrcRank(a.sourceKey); })[0] || null;
}

function _lbPickSourceKey(targetZoom) {
  // Returns 'full' | '2560' | '3840' | 'original' based on current display needs.
  // Fall back to /full at fit and /original when zoomed whenever we can't compute
  // a proper displayed-long-edge (photo dims missing, or nativeZoom not yet
  // established — the latter happens briefly when API dims arrive before the
  // initial image load completes).
  var dims = _lbLayoutDims();
  var zoom = targetZoom != null ? targetZoom : _lbZoom;
  if (!dims || !_lbNativeZoom) {
    return (zoom > 1.001 && !_lbOriginalUnavailable) ? 'original' : 'full';
  }
  var longEdge = Math.max(dims.w, dims.h);
  var displayedLong = longEdge * _lbFitScale * zoom;
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
  var decodedWarmup = Object.values(_lbAdjacentPreloads).some(function(entry) {
    return entry.photoId === photoId && entry.sourceKey === key &&
      entry.status === 'decoded' && entry.url === speculativeUrl;
  });
  if (
    !decodedWarmup && _lbOriginalPreload && key === 'original' &&
    _lbOriginalPreload.photoId === photoId &&
    _lbOriginalPreload.status === 'decoded' &&
    _lbOriginalPreload.url === speculativeUrl
  ) decodedWarmup = true;
  if (decodedWarmup) return speculativeUrl;
  return url;
}

function _lbCancelAdjacentPreloadTimer() {
  if (_lbAdjacentPreloadTimer) {
    clearTimeout(_lbAdjacentPreloadTimer);
    _lbAdjacentPreloadTimer = null;
  }
}

function _lbClearAdjacentPreloads() {
  _lbCancelAdjacentPreloadTimer();
  Object.keys(_lbAdjacentPreloads).forEach(function(cacheKey) {
    var entry = _lbAdjacentPreloads[cacheKey];
    _lbReleasePreloadImage(entry);
  });
  _lbAdjacentPreloads = {};
  _lbAdjacentPreloadRetry = {};
}

function _lbCancelOriginalPreload() {
  if (_lbOriginalPreloadTimer) {
    clearTimeout(_lbOriginalPreloadTimer);
    _lbOriginalPreloadTimer = null;
  }
  _lbReleasePreloadImage(_lbOriginalPreload);
  _lbOriginalPreload = null;
  _lbOriginalPreloadWaiting = null;
}

function _lbShouldPreloadOriginal(photoId) {
  if (
    photoId == null ||
    _lightboxCurrentId !== photoId ||
    _lbCurrentSrcKey !== 'full' ||
    _lbFullUsesOriginal !== false ||
    _lbOriginalUnavailable ||
    _lbZoom > 1.001 ||
    (_lbDesiredSrcKey && _lbDesiredSrcKey !== 'full') ||
    !_lbNativeZoom ||
    _lbPickSourceKey(_lbNativeZoom) !== 'original'
  ) return false;
  var img = document.getElementById('lightboxImg');
  return !!(img && img.complete && img.naturalWidth && img.naturalHeight);
}

function _lbReleasePreloadImage(entry) {
  // Clearing src does not cancel synchronous server-side generation. Keep the
  // outstanding requests observable even if their decoded-cache entries go.
  if (!entry || !entry.img || _lbSpeculativeLoads.has(entry)) return;
  entry.img.onload = null;
  entry.img.onerror = null;
  entry.img.removeAttribute('src');
}

function _lbSmallPreviewKey() {
  // /full is configurable, including an original-resolution mode. Never use
  // that mode (or a large custom preview) for the wide navigation window.
  return _lbFullPreviewLimit() > 1920 ? '1920' : 'full';
}

function _lbEstimatePreloadBytes(photo, key) {
  var cached = _lbPhotoDataByPhoto[String(photo.id)] || photo;
  var w = Number(cached.width || photo.width) || _lbPhotoW || 6000;
  var h = Number(cached.height || photo.height) || _lbPhotoH || 4000;
  var size = key === 'original' ? Math.max(w, h)
    : key === 'full' ? _lbFullPreviewLimit() : Number(key);
  var scale = Math.min(1, size / Math.max(w, h));
  return Math.ceil(w * scale) * Math.ceil(h * scale) * 4;
}

function _lbPreloadBytes() {
  var entries = new Set(Object.values(_lbAdjacentPreloads));
  _lbSpeculativeLoads.forEach(function(entry) { entries.add(entry); });
  if (_lbOriginalPreload) entries.add(_lbOriginalPreload);
  var bytes = 0;
  entries.forEach(function(entry) { bytes += entry.bytes || 0; });
  return bytes;
}

function _lbTrimPreloadBudget() {
  // Estimates reserve space before starting each decode. Correct them from
  // actual dimensions afterward, dropping distant bitmaps first. This bounds
  // retained pixel buffers, not the WebView's independent graphics/HTTP cache.
  // A larger-than-estimated original should not evict the whole small-preview
  // window before being discarded itself.
  if (_lbPreloadBytes() > _lbPreloadBudgetBytes && _lbOriginalPreload &&
      _lbOriginalPreload.status === 'decoded') _lbCancelOriginalPreload();
  var currentIdx = _lightboxPhotoList.findIndex(function(p) { return p.id === _lightboxCurrentId; });
  var entries = Object.entries(_lbAdjacentPreloads).filter(function(item) {
    return item[1].status === 'decoded';
  });
  entries.sort(function(a, b) {
    function distance(entry) {
      return Math.abs(_lightboxPhotoList.findIndex(function(p) { return p.id === entry.photoId; }) - currentIdx);
    }
    return Number(b[1].bytes > _lbPreloadBudgetBytes) - Number(a[1].bytes > _lbPreloadBudgetBytes) ||
      distance(b[1]) - distance(a[1]) || b[1].bytes - a[1].bytes;
  });
  while (_lbPreloadBytes() > _lbPreloadBudgetBytes && entries.length) {
    var item = entries.shift();
    _lbReleasePreloadImage(item[1]);
    delete _lbAdjacentPreloads[item[0]];
    // Do not immediately re-request an underestimated image we just evicted.
    _lbAdjacentPreloadRetry[item[0]] = { openSeq: _lbOpenSeq, count: 2, at: 0 };
  }
}

function _lbRunSpeculativePreload(entry, onSettled) {
  _lbSpeculativeLoads.add(entry);
  _lbSpeculativeInFlight = _lbSpeculativeLoads.values().next().value || null;
  var preload = entry.img;
  preload.fetchPriority = 'low';
  function settle(decoded) {
    if (entry.status !== 'loading') return;
    entry.status = decoded ? 'decoded' : 'failed';
    _lbSpeculativeLoads.delete(entry);
    _lbSpeculativeInFlight = _lbSpeculativeLoads.values().next().value || null;
    if (decoded) entry.bytes = preload.naturalWidth * preload.naturalHeight * 4;
    onSettled(decoded);
    _lbTrimPreloadBudget();
    // A navigation/close may have retired this request while it was decoding.
    if (!Object.values(_lbAdjacentPreloads).includes(entry) && _lbOriginalPreload !== entry) {
      _lbReleasePreloadImage(entry);
    }
    var overlay = document.getElementById('lightboxOverlay');
    if (overlay && overlay.classList.contains('active')) {
      _lbScheduleAdjacentPhoto(_lbCurrentSrcKey);
    }
  }
  var supportsDecode = typeof preload.decode === 'function';
  preload.onload = function() { if (!supportsDecode) settle(true); };
  preload.onerror = function() { settle(false); };
  preload.src = entry.url;
  if (supportsDecode) {
    preload.decode().then(function() { settle(true); }).catch(function() {
      // Incomplete WebView decode support must not strand the shared slot.
      settle(!!(preload.complete && preload.naturalWidth));
    });
  }
}

function _lbScheduleOriginalPreload(photoId, retryCount) {
  _lbCancelOriginalPreload();
  if (!_lbShouldPreloadOriginal(photoId)) return;
  // Dwell prevents fast arrowing from preparing every full-resolution RAW.
  // Once eligible, wait behind ready neighbors without spending a retry on
  // our own occupied server slot.
  _lbOriginalPreloadTimer = setTimeout(function() {
    _lbOriginalPreloadTimer = null;
    if (!_lbShouldPreloadOriginal(photoId)) return;
    _lbOriginalPreloadWaiting = { photoId: photoId, retryCount: retryCount || 0 };
    _lbScheduleAdjacentPhoto(_lbCurrentSrcKey);
  }, 1400);
}

function _lbStartPendingOriginalPreload() {
  if (_lbSpeculativeInFlight || !_lbOriginalPreloadWaiting) return;
  var pending = _lbOriginalPreloadWaiting;
  _lbOriginalPreloadWaiting = null;
  if (!_lbShouldPreloadOriginal(pending.photoId)) return;
  var entry = {
    img: new Image(),
    photoId: pending.photoId,
    sourceKey: 'original',
    url: _lbSrcUrl(pending.photoId, 'original', true),
    status: 'loading',
    bytes: _lbEstimatePreloadBytes({ id: pending.photoId, width: _lbPhotoW, height: _lbPhotoH }, 'original')
  };
  if (_lbPreloadBytes() + entry.bytes > _lbPreloadBudgetBytes) return;
  _lbOriginalPreload = entry;
  _lbRunSpeculativePreload(entry, function(decoded) {
    if (_lbOriginalPreload !== entry) return;
    if (!decoded) {
      // A failed image retains no pixels and must not reserve its estimate.
      _lbOriginalPreload = null;
      if (pending.retryCount < 1 && _lbShouldPreloadOriginal(pending.photoId)) {
        _lbScheduleOriginalPreload(pending.photoId, pending.retryCount + 1);
      }
    }
  });
}

function _lbPrimeAdjacentPhotos(sourceKey) {
  var overlay = document.getElementById('lightboxOverlay');
  var visibleImg = document.getElementById('lightboxImg');
  if (!overlay || !overlay.classList.contains('active') ||
      _lbVisualTransitionPending || !visibleImg ||
      !visibleImg.complete || !visibleImg.naturalWidth) return;
  // The old bitmap remains visible during a zoom source upgrade. Give that
  // interactive request priority over queued neighbors of the outgoing tier.
  // The source-load handler resumes warmups once the new pixels are ready.
  if (_lbDesiredSrcKey && _lbDesiredSrcKey !== _lbCurrentSrcKey) return;
  if (_lightboxPhotoList.length < 2 || _lightboxCurrentId == null) {
    _lbClearAdjacentPreloads();
    _lbStartPendingOriginalPreload();
    return;
  }
  var currentIdx = _lightboxPhotoList.findIndex(function(photo) {
    return photo.id === _lightboxCurrentId;
  });
  if (currentIdx === -1) {
    _lbStartPendingOriginalPreload();
    return;
  }

  _lbCancelAdjacentPreloadTimer();
  var key = sourceKey || 'full';
  var baseKey = _lbSmallPreviewKey();
  var direction = _lbLastNavDelta < 0 ? -1 : 1;
  var candidates = [];
  var keep = {};
  function addCandidate(offset, tier) {
    var photo = _lightboxPhotoList[currentIdx + offset];
    if (!photo) return;
    if (Object.prototype.hasOwnProperty.call(photo, 'edit_recipe')) {
      _lbRememberEditRecipe(photo.id, photo.edit_recipe);
      window.vireoRememberPhotoRenderKey(photo.id, photo.render_key);
    }
    var url = _lbSrcUrl(photo.id, tier, true);
    var cacheKey = String(photo.id) + '|' + url;
    if (keep[cacheKey]) return;
    keep[cacheKey] = true;
    candidates.push({
      photoId: photo.id, sourceKey: tier, url: url, cacheKey: cacheKey,
      bytes: _lbEstimatePreloadBytes(photo, tier)
    });
  }
  // Always prepare a small, immediately usable image on both sides, even at
  // 1:1. Only the immediate neighbors also receive the current sharper tier.
  addCandidate(direction, baseKey);
  addCandidate(-direction, baseKey);
  for (var distance = 2; distance <= 8; distance++) {
    addCandidate(distance * direction, baseKey);
    if (distance <= 4) addCandidate(-distance * direction, baseKey);
  }
  if (key !== baseKey) {
    addCandidate(direction, key);
    addCandidate(-direction, key);
  }
  Object.keys(_lbAdjacentPreloads).forEach(function(cacheKey) {
    var entry = _lbAdjacentPreloads[cacheKey];
    // Retaining the just-displayed warmup makes an immediate reversal cheap.
    if (keep[cacheKey] || (entry.photoId === _lightboxCurrentId && entry.status === 'decoded')) return;
    _lbReleasePreloadImage(entry);
    delete _lbAdjacentPreloads[cacheKey];
  });
  Object.keys(_lbAdjacentPreloadRetry).forEach(function(cacheKey) {
    if (!keep[cacheKey]) delete _lbAdjacentPreloadRetry[cacheKey];
  });
  _lbTrimPreloadBudget();

  // Cache hits can transfer/decode concurrently. The server independently
  // admits just one speculative cache-miss producer; declined work backs off.
  // Count retired in-flight requests too: dropping an Image does not cancel
  // synchronous server generation or immediately reclaim its decode memory.
  var now = Date.now();
  var retryAt = null;
  candidates.forEach(function(candidate) {
    if (_lbSpeculativeLoads.size >= _lbPreloadConcurrency) return;
    if (_lbAdjacentPreloads[candidate.cacheKey]) return;
    // Once a miss has been declined, retry it alone after active transfers
    // finish. Parallel retries would simply collide with the same server slot.
    if (Array.from(_lbSpeculativeLoads).some(function(entry) { return entry.retrying; })) return;
    var retry = _lbAdjacentPreloadRetry[candidate.cacheKey];
    if (retry && retry.openSeq === _lbOpenSeq) {
      if (retry.count > 1) return;
      if (_lbSpeculativeLoads.size) return;
      if (retry.at > now) {
        retryAt = retryAt == null ? retry.at : Math.min(retryAt, retry.at);
        return;
      }
    }
    if (_lbPreloadBytes() + candidate.bytes > _lbPreloadBudgetBytes) return;
    var entry = {
      img: new Image(), photoId: candidate.photoId, sourceKey: candidate.sourceKey,
      url: candidate.url, status: 'loading', bytes: candidate.bytes, retrying: !!retry
    };
    _lbAdjacentPreloads[candidate.cacheKey] = entry;
    _lbRunSpeculativePreload(entry, function(decoded) {
      if (_lbAdjacentPreloads[candidate.cacheKey] !== entry) return;
      if (!decoded) {
        delete _lbAdjacentPreloads[candidate.cacheKey];
        var retry = _lbAdjacentPreloadRetry[candidate.cacheKey];
        if (!retry || retry.openSeq !== _lbOpenSeq) retry = { openSeq: _lbOpenSeq, count: 0 };
        retry.count += 1;
        retry.at = Date.now() + 750;
        _lbAdjacentPreloadRetry[candidate.cacheKey] = retry;
      }
    });
  });
  if (retryAt != null) _lbScheduleAdjacentPhoto(key, retryAt - now);
  else if (!_lbSpeculativeLoads.size) _lbStartPendingOriginalPreload();
}

function _lbScheduleAdjacentPhoto(sourceKey, delay) {
  _lbCancelAdjacentPreloadTimer();
  var openSeq = _lbOpenSeq;
  _lbAdjacentPreloadTimer = setTimeout(function() {
    _lbAdjacentPreloadTimer = null;
    if (_lbOpenSeq !== openSeq || _lbVisualTransitionPending) return;
    _lbPrimeAdjacentPhotos(sourceKey);
  }, delay || 0);
}

function _lbScheduleSourceSwap(targetZoom, immediate) {
  if (!_lightboxCurrentId) return;
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
  if (_lbProgressiveTargetKey && !_lbNativeZoom) desired = _lbProgressiveTargetKey;
  _lbDesiredSrcKey = desired;
  if (_lbZoom > 1.001 && desired !== 'original') {
    _lbCancelOriginalPreload();
  }
  if (desired === _lbCurrentSrcKey) {
    _lbProgressiveTargetKey = null;
    // Zooming back to a level the current tier already satisfies cancels the
    // sharper request. Nothing loaded, so go quiet rather than claim the pixels
    // the user was waiting on arrived.
    if (!abandoningRequest) _lbMarkDetailSettled();
    _lbSetPreviewLoading(false);
    _lbScheduleAdjacentPhoto(desired);
    return;
  }

  _lbSetPreviewLoading(_lbSrcRank(desired) > _lbSrcRank(_lbCurrentSrcKey) || !!_lbProgressiveTargetKey);
  if (_lbSwapTimer) clearTimeout(_lbSwapTimer);
  _lbSwapTimer = setTimeout(function() {
    _lbSwapTimer = null;
    if (_lbDesiredSrcKey !== desired) return;          // newer request won
    if (_lbDesiredSrcKey === _lbCurrentSrcKey) return; // already there
    var photoId = _lightboxCurrentId;
    var swapOpenSeq = _lbOpenSeq;
    var key = _lbDesiredSrcKey;
    var url = _lbSrcUrl(photoId, key);

    var preloader = new Image();
    preloader.onload = function() {
      // Ignore stale: different photo, or desired key changed to something else
      if (_lightboxCurrentId !== photoId) return;
      if (_lbOpenSeq !== swapOpenSeq) return;
      if (_lbDesiredSrcKey !== key) return;
      var img = document.getElementById('lightboxImg');
      if (!img) return;
      // After the new source loads, recompute nativeZoom — the aspect ratio is
      // nominally identical across source tiers but subpixel rounding can shift
      // fit dimensions slightly, and if API dims were missing the original's
      // naturalWidth is now authoritative.
      img.addEventListener('load', function onSwapLoad() {
        img.removeEventListener('load', onSwapLoad);
        if (_lightboxCurrentId !== photoId) return;
        if (_lbOpenSeq !== swapOpenSeq) return;
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
        if (key === 'original') _lbCancelOriginalPreload();
        if (!_lbPhotoW && key === 'original' && img.naturalWidth) {
          _lbPhotoW = img.naturalWidth;
          _lbPhotoH = img.naturalHeight;
        }
        _lbRecomputeNativeZoom();
        // If the user pressed z/click when nativeZoom was unknown, upgrade to
        // true 1:1 now. Route through the shared helper so the stored anchor is
        // honored — completing with a recentered zoom would make a deferred 1:1
        // jump away from the point the user clicked.
        if (!_lbTryApplyPendingViewportState()) _lbApplyPendingOneToOneZoom();
        // Track Eye alignment is deferred while _lbPending1To1 is armed so it
        // cannot cancel the deferred sharp-source fallback. Re-apply now that
        // the source has landed and any pending 1:1 has snapped.
        _lbTryApplyPendingEyeTrack();
        _lbApplyTransform();
        _lbScheduleAdjacentPhoto(key);
      });
      img.src = url;
      _lbCurrentSrcKey = key;
    };
    preloader.onerror = function() {
      if (_lightboxCurrentId !== photoId) return;
      if (_lbOpenSeq !== swapOpenSeq) return;
      if (_lbDesiredSrcKey !== key) return;
      _lbProgressiveTargetKey = null;
      // Record the failure BEFORE clearing the loading flag. The chip's
      // sharpening state is derived from the desired/current tiers, so a tier
      // that is never going to arrive has to stop counting as in flight first
      // -- clearing the flag while _lbDesiredSrcKey still names the failed tier
      // leaves the spinner up with no request behind it.
      if (key === 'original') {
        var had1To1Pending = _lbPending1To1;
        _lbOriginalUnavailable = true;
        _lbSetPreviewLoading(false);
        _lbRecomputeNativeZoom();
        if (had1To1Pending) {
          _lbDeferPendingOneToOneToPreviewFallback();
        } else {
          _lbApplyPendingOneToOneZoom();
          _lbApplyTransform();
          // /original is gone, so re-pick the best remaining tier rather than
          // staying stuck on the lower current source.
          _lbScheduleSourceSwap();
        }
      } else {
        // The failed request is no longer pending. Keep the usable bitmap
        // and resume its neighbors without automatically retrying this tier.
        _lbDesiredSrcKey = _lbCurrentSrcKey;
        _lbSetPreviewLoading(false);
        if (_lbPending1To1) {
          _lbPending1To1 = false;
          _lbPending1To1Anchor = null;
          _lbPendingViewportState = null;
          _lbUpdateZoomControl();
          _lbSaveViewportState(photoId);
        }
        _lbScheduleAdjacentPhoto(_lbCurrentSrcKey);
      }
      // Keep current source on failure
    };
    preloader.src = url;
  }, immediate ? 0 : 150);
}

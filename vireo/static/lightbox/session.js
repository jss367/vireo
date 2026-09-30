/* Owns lightbox photo identity, asynchronous image work and preload resources.
 * Creation is inert. The host supplies render policy and its live navigation list;
 * viewport, editing and DOM presentation remain with their existing owners.
 */
(function(root) {
  'use strict';
  function create(options) {
    var window = options.window || root;
    var document = window.document;
    var currentId = null;
    var displayedId = null;
    var generation = 0;
    var preloadSerial = 0;
    var swapTimer = null;
    var initialLoad = null;
    var cleanups = new Set();

    function capture() { return Object.freeze({photoId: currentId, generation: generation}); }
    function isCurrent(token) {
      return currentId != null && token.photoId === currentId && token.generation === generation;
    }
    function cancelSwap() {
      if (swapTimer !== null) window.clearTimeout(swapTimer);
      swapTimer = null;
    }
    function retire() {
      ++generation;
      cancelSwap();
      initialLoad = null;
      cleanups.forEach(function(cleanup) { cleanup(); });
      cleanups.clear();
      _lbCancelOriginalPreload();
      _lbCancelAdjacentPreloadTimer();
    }
    function begin(photoId) {
      if (currentId == null) _lbLastNavDelta = 1;
      retire();
      currentId = photoId;
      return capture();
    }
    function commit(token) {
      if (!isCurrent(token)) return false;
      displayedId = token.photoId;
      return true;
    }
    function close() {
      var closedId = displayedId != null ? displayedId : currentId;
      retire();
      currentId = null;
      displayedId = null;
      _lbClearAdjacentPreloads();
      return closedId;
    }
    function scheduleSwap(callback, delay) {
      cancelSwap();
      var token = capture();
      var timer = window.setTimeout(function() {
        if (swapTimer !== timer) return;
        swapTimer = null;
        if (isCurrent(token)) callback();
      }, delay);
      swapTimer = timer;
    }
    // Both property handlers and event listeners are detached on navigation or
    // close. A queued callback also checks its token before touching the host.
    function watchImage(img, onload, onerror, properties, persistent) {
      var token = capture();
      function cleanup() {
        if (properties) {
          if (img.onload === loaded) img.onload = null;
          if (img.onerror === failed) img.onerror = null;
        } else {
          img.removeEventListener('load', loaded);
          img.removeEventListener('error', failed);
        }
        cleanups.delete(cleanup);
      }
      function loaded() {
        if (!persistent) cleanup();
        if (isCurrent(token)) onload();
      }
      function failed() {
        if (!persistent) cleanup();
        if (isCurrent(token) && onerror) onerror();
      }
      if (properties) { img.onload = loaded; img.onerror = failed; }
      else { img.addEventListener('load', loaded); img.addEventListener('error', failed); }
      cleanups.add(cleanup);
      return cleanup;
    }
    function loadSource(url, onload, onerror) {
      var img = new window.Image();
      watchImage(img, onload, onerror, true);
      img.src = url;
    }
    function setInitialLoad(commitLoad, abandonLoad) {
      initialLoad = {token: capture(), commit: commitLoad, abandon: abandonLoad};
    }
    function clearInitialLoad(commitLoad) {
      if (!commitLoad || (initialLoad && initialLoad.commit === commitLoad)) initialLoad = null;
    }
    function finishInitialLoad(abandon) {
      var pending = initialLoad;
      initialLoad = null;
      if (!pending || !isCurrent(pending.token)) return false;
      (abandon ? pending.abandon : pending.commit)();
      return true;
    }
    function snapshotEntry(entry) {
      if (!entry) return null;
      return Object.freeze({requestId: entry.requestId, photoId: entry.photoId, sourceKey: entry.sourceKey,
        url: entry.url, status: entry.status, bytes: entry.bytes});
    }
    // Diagnostics expose values, never Image objects, callbacks or mutable caches.
    function preloadStatus() {
      return Object.freeze({
        adjacent: Object.freeze(Object.values(_lbAdjacentPreloads).map(snapshotEntry)),
        original: snapshotEntry(_lbOriginalPreload),
        inFlight: snapshotEntry(_lbSpeculativeInFlight),
        activeCount: _lbSpeculativeLoads.size,
        bytes: _lbPreloadBytes(), budgetBytes: _lbPreloadBudgetBytes,
        retryCount: Object.keys(_lbAdjacentPreloadRetry).length,
        exhaustedRetries: Object.values(_lbAdjacentPreloadRetry).filter(function(retry) { return retry.count > 1; }).length,
        adjacentScheduled: _lbAdjacentPreloadTimer !== null,
        originalScheduled: _lbOriginalPreloadTimer !== null,
        originalWaiting: _lbOriginalPreloadWaiting !== null
      });
    }
    function hasDecodedSource(photoId, key, url) {
      var entries = Object.values(_lbAdjacentPreloads);
      if (_lbOriginalPreload) entries.push(_lbOriginalPreload);
      return entries.some(function(entry) {
        return entry.photoId === photoId && entry.sourceKey === key &&
          entry.status === 'decoded' && entry.url === url;
      });
    }

    var _lbAdjacentPreloads = {}; // bounded navigation window, keyed by photo id + source URL
    var _lbAdjacentPreloadTimer = null;
    var _lbAdjacentPreloadRetry = {};
    var _lbLastNavDelta = 1;
    var _lbOriginalPreloadTimer = null; // short dwell before warming the current photo's 100% source
    var _lbOriginalPreload = null; // retained decoded original for an instant first 100% click
    var _lbOriginalPreloadWaiting = null; // dwell completed; waiting for the shared slot
    var _lbSpeculativeInFlight = null; // oldest outstanding request (including pruned entries)
    var _lbSpeculativeLoads = new Set();
    var _lbPreloadConcurrency = options.preloadConcurrency || 3;
    var _lbPreloadBudgetBytes = options.preloadBudgetBytes || 128 * 1024 * 1024;

    function _lbDecodedInitialPreview(photoId, desired) {
      var entries = Object.values(_lbAdjacentPreloads);
      if (_lbOriginalPreload) entries.push(_lbOriginalPreload);
      var ready = entries.filter(function(entry) {
        return entry.photoId === photoId && entry.status === 'decoded' &&
          entry.url === options.sourceUrl(photoId, entry.sourceKey, true);
      });
      var exact = ready.find(function(entry) { return entry.sourceKey === desired; });
      if (exact) return snapshotEntry(exact);
      // A ready smaller preview is safe only for this exact render/source version.
      // Do not flash an old crop, an unedited thumbnail, or the other RAW/JPEG pair.
      return snapshotEntry(ready.filter(function(entry) {
        return options.sourceRank(entry.sourceKey) <= options.sourceRank(desired);
      }).sort(function(a, b) { return options.sourceRank(b.sourceKey) - options.sourceRank(a.sourceKey); })[0] || null);
    }

    function _lbCancelAdjacentPreloadTimer() {
      if (_lbAdjacentPreloadTimer) {
        window.clearTimeout(_lbAdjacentPreloadTimer);
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
        window.clearTimeout(_lbOriginalPreloadTimer);
        _lbOriginalPreloadTimer = null;
      }
      _lbReleasePreloadImage(_lbOriginalPreload);
      _lbOriginalPreload = null;
      _lbOriginalPreloadWaiting = null;
    }

    function _lbShouldPreloadOriginal(photoId) {
      if (
        photoId == null ||
        currentId !== photoId ||
        options.view().currentSrcKey !== 'full' ||
        options.view().fullUsesOriginal !== false ||
        options.view().originalUnavailable ||
        options.view().zoom > 1.001 ||
        (options.view().desiredSrcKey && options.view().desiredSrcKey !== 'full') ||
        !options.view().nativeZoom ||
        options.pickSourceKey(options.view().nativeZoom) !== 'original'
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
      return options.fullPreviewLimit() > 1920 ? '1920' : 'full';
    }

    function _lbEstimatePreloadBytes(photo, key) {
      var cached = options.photoData(photo.id) || photo;
      var w = Number(cached.width || photo.width) || options.view().photoW || 6000;
      var h = Number(cached.height || photo.height) || options.view().photoH || 4000;
      var size = key === 'original' ? Math.max(w, h)
        : key === 'full' ? options.fullPreviewLimit() : Number(key);
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
      var currentIdx = options.photos().findIndex(function(p) { return p.id === currentId; });
      var entries = Object.entries(_lbAdjacentPreloads).filter(function(item) {
        return item[1].status === 'decoded';
      });
      entries.sort(function(a, b) {
        function distance(entry) {
          return Math.abs(options.photos().findIndex(function(p) { return p.id === entry.photoId; }) - currentIdx);
        }
        return Number(b[1].bytes > _lbPreloadBudgetBytes) - Number(a[1].bytes > _lbPreloadBudgetBytes) ||
          distance(b[1]) - distance(a[1]) || b[1].bytes - a[1].bytes;
      });
      while (_lbPreloadBytes() > _lbPreloadBudgetBytes && entries.length) {
        var item = entries.shift();
        _lbReleasePreloadImage(item[1]);
        delete _lbAdjacentPreloads[item[0]];
        // Do not immediately re-request an underestimated image we just evicted.
        _lbAdjacentPreloadRetry[item[0]] = { openSeq: generation, count: 2, at: 0 };
      }
    }

    function _lbRunSpeculativePreload(entry, onSettled) {
      entry.requestId = ++preloadSerial;
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
        if (currentId != null && overlay && overlay.classList.contains('active')) {
          _lbScheduleAdjacentPhoto(options.view().currentSrcKey);
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
      var token = capture();
      var timer = window.setTimeout(function() {
        if (_lbOriginalPreloadTimer !== timer) return;
        _lbOriginalPreloadTimer = null;
        if (!isCurrent(token) || !_lbShouldPreloadOriginal(photoId)) return;
        _lbOriginalPreloadWaiting = { photoId: photoId, retryCount: retryCount || 0 };
        _lbScheduleAdjacentPhoto(options.view().currentSrcKey);
      }, 1400);
      _lbOriginalPreloadTimer = timer;
    }

    function _lbStartPendingOriginalPreload() {
      if (_lbSpeculativeInFlight || !_lbOriginalPreloadWaiting) return;
      var pending = _lbOriginalPreloadWaiting;
      _lbOriginalPreloadWaiting = null;
      if (!_lbShouldPreloadOriginal(pending.photoId)) return;
      var entry = {
        img: new window.Image(),
        photoId: pending.photoId,
        sourceKey: 'original',
        url: options.sourceUrl(pending.photoId, 'original', true),
        status: 'loading',
        bytes: _lbEstimatePreloadBytes({ id: pending.photoId, width: options.view().photoW, height: options.view().photoH }, 'original')
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
      if (currentId == null || !overlay || !overlay.classList.contains('active') ||
          options.view().visualTransitionPending || !visibleImg ||
          !visibleImg.complete || !visibleImg.naturalWidth) return;
      // The old bitmap remains visible during a zoom source upgrade. Give that
      // interactive request priority over queued neighbors of the outgoing tier.
      // The source-load handler resumes warmups once the new pixels are ready.
      if (options.view().desiredSrcKey && options.view().desiredSrcKey !== options.view().currentSrcKey) return;
      if (options.photos().length < 2 || currentId == null) {
        _lbClearAdjacentPreloads();
        _lbStartPendingOriginalPreload();
        return;
      }
      var currentIdx = options.photos().findIndex(function(photo) {
        return photo.id === currentId;
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
        var photo = options.photos()[currentIdx + offset];
        if (!photo) return;
        if (Object.prototype.hasOwnProperty.call(photo, 'edit_recipe')) {
          options.rememberEditRecipe(photo.id, photo.edit_recipe);
          options.rememberRenderKey(photo.id, photo.render_key);
        }
        var url = options.sourceUrl(photo.id, tier, true);
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
        if (keep[cacheKey] || (entry.photoId === currentId && entry.status === 'decoded')) return;
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
        if (retry && retry.openSeq === generation) {
          if (retry.count > 1) return;
          if (_lbSpeculativeLoads.size) return;
          if (retry.at > now) {
            retryAt = retryAt == null ? retry.at : Math.min(retryAt, retry.at);
            return;
          }
        }
        if (_lbPreloadBytes() + candidate.bytes > _lbPreloadBudgetBytes) return;
        var entry = {
          img: new window.Image(), photoId: candidate.photoId, sourceKey: candidate.sourceKey,
          url: candidate.url, status: 'loading', bytes: candidate.bytes, retrying: !!retry
        };
        _lbAdjacentPreloads[candidate.cacheKey] = entry;
        _lbRunSpeculativePreload(entry, function(decoded) {
          if (_lbAdjacentPreloads[candidate.cacheKey] !== entry) return;
          if (!decoded) {
            delete _lbAdjacentPreloads[candidate.cacheKey];
            var retry = _lbAdjacentPreloadRetry[candidate.cacheKey];
            if (!retry || retry.openSeq !== generation) retry = { openSeq: generation, count: 0 };
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
      var openSeq = generation;
      var timer = window.setTimeout(function() {
        if (_lbAdjacentPreloadTimer !== timer) return;
        _lbAdjacentPreloadTimer = null;
        if (generation !== openSeq || options.view().visualTransitionPending) return;
        _lbPrimeAdjacentPhotos(sourceKey);
      }, delay || 0);
      _lbAdjacentPreloadTimer = timer;
    }

    return Object.freeze({
      begin: begin, commit: commit, close: close, capture: capture, isCurrent: isCurrent,
      requestedPhotoId: function() { return currentId; },
      displayedPhotoId: function() { return displayedId; },
      noteDirection: function(delta) { _lbLastNavDelta = delta < 0 ? -1 : 1; },
      cancelSwap: cancelSwap, scheduleSwap: scheduleSwap,
      hasScheduledSwap: function() { return swapTimer !== null; },
      watchImage: watchImage, loadSource: loadSource,
      watchInitialImage: function(img, onload, onerror) { return watchImage(img, onload, onerror, true, true); },
      setInitialLoad: setInitialLoad, clearInitialLoad: clearInitialLoad,
      hasInitialLoad: function() { return initialLoad !== null; },
      finishInitialLoad: function() { return finishInitialLoad(false); },
      abandonInitialLoad: function() { return finishInitialLoad(true); },
      preloadStatus: preloadStatus, hasDecodedSource: hasDecodedSource,
      decodedPreview: _lbDecodedInitialPreview,
      cancelAdjacent: _lbCancelAdjacentPreloadTimer,
      clearAdjacent: _lbClearAdjacentPreloads,
      cancelOriginal: _lbCancelOriginalPreload,
      scheduleOriginal: _lbScheduleOriginalPreload,
      scheduleAdjacent: _lbScheduleAdjacentPhoto,
    });
  }
  root.VireoLightboxSession = Object.freeze({create: create});
})(window);

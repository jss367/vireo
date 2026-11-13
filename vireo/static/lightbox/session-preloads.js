/* The lightbox's speculative image loads: the bounded window of neighbors warmed
 * around the current photo, and the current photo's original warmed for a first
 * 1:1 click. Both share one concurrency limit and one decoded-byte budget.
 */
import { captureToken, isCurrent } from './session-requests.js';

function snapshotEntry(/** @type {LightboxPreloadEntry | null | undefined} */ entry) {
  if (!entry) return null;
  return Object.freeze({requestId: entry.requestId, photoId: entry.photoId, sourceKey: entry.sourceKey,
    url: entry.url, status: entry.status, bytes: entry.bytes});
}

// Diagnostics expose values, never Image objects, callbacks or mutable caches.
export function preloadStatus(/** @type {LightboxSessionContext} */ session) {
  return Object.freeze({
    adjacent: Object.freeze(Object.values(session.adjacentPreloads).map(snapshotEntry)),
    original: snapshotEntry(session.originalPreload),
    inFlight: snapshotEntry(session.speculativeInFlight),
    activeCount: session.speculativeLoads.size,
    bytes: preloadBytes(session), budgetBytes: session.preloadBudgetBytes,
    retryCount: Object.keys(session.adjacentRetry).length,
    exhaustedRetries: Object.values(session.adjacentRetry).filter(function(retry) { return retry.count > 1; }).length,
    adjacentScheduled: session.adjacentTimer !== null,
    originalScheduled: session.originalTimer !== null,
    originalWaiting: session.originalWaiting !== null
  });
}

export function hasDecodedSource(/** @type {LightboxSessionContext} */ session, /** @type {number} */ photoId, /** @type {string} */ key, /** @type {string} */ url) {
  var entries = Object.values(session.adjacentPreloads);
  if (session.originalPreload) entries.push(session.originalPreload);
  return entries.some(function(entry) {
    return entry.photoId === photoId && entry.sourceKey === key &&
      entry.status === 'decoded' && entry.url === url;
  });
}

export function decodedPreview(/** @type {LightboxSessionContext} */ session, /** @type {number} */ photoId, /** @type {string} */ desired) {
  var entries = Object.values(session.adjacentPreloads);
  if (session.originalPreload) entries.push(session.originalPreload);
  var ready = entries.filter(function(entry) {
    return entry.photoId === photoId && entry.status === 'decoded' &&
      entry.url === session.options.sourceUrl(photoId, entry.sourceKey, true);
  });
  var exact = ready.find(function(entry) { return entry.sourceKey === desired; });
  if (exact) return snapshotEntry(exact);
  // A ready smaller preview is safe only for this exact render/source version.
  // Do not flash an old crop, an unedited thumbnail, or the other RAW/JPEG pair.
  return snapshotEntry(ready.filter(function(entry) {
    return session.options.sourceRank(entry.sourceKey) <= session.options.sourceRank(desired);
  }).sort(function(a, b) { return session.options.sourceRank(b.sourceKey) - session.options.sourceRank(a.sourceKey); })[0] || null);
}

export function cancelAdjacent(/** @type {LightboxSessionContext} */ session) {
  if (session.adjacentTimer) {
    session.window.clearTimeout(session.adjacentTimer);
    session.adjacentTimer = null;
  }
}

export function clearAdjacent(/** @type {LightboxSessionContext} */ session) {
  cancelAdjacent(session);
  Object.keys(session.adjacentPreloads).forEach(function(cacheKey) {
    var entry = session.adjacentPreloads[cacheKey];
    releasePreloadImage(session, entry);
  });
  session.adjacentPreloads = {};
  session.adjacentRetry = {};
}

export function cancelOriginal(/** @type {LightboxSessionContext} */ session) {
  if (session.originalTimer) {
    session.window.clearTimeout(session.originalTimer);
    session.originalTimer = null;
  }
  releasePreloadImage(session, session.originalPreload);
  session.originalPreload = null;
  session.originalWaiting = null;
}

function shouldPreloadOriginal(/** @type {LightboxSessionContext} */ session, /** @type {number | null} */ photoId) {
  if (
    photoId == null ||
    session.currentId !== photoId ||
    session.options.view().currentSrcKey !== 'full' ||
    session.options.view().fullUsesOriginal !== false ||
    session.options.view().originalUnavailable ||
    session.options.view().zoom > 1.001 ||
    (session.options.view().desiredSrcKey && session.options.view().desiredSrcKey !== 'full') ||
    !session.options.view().nativeZoom ||
    session.options.pickSourceKey(session.options.view().nativeZoom || 1) !== 'original'
  ) return false;
  var img = /** @type {HTMLImageElement | null} */ (session.document.getElementById('lightboxImg'));
  return !!(img && img.complete && img.naturalWidth && img.naturalHeight);
}

function releasePreloadImage(/** @type {LightboxSessionContext} */ session, /** @type {LightboxPreloadEntry | null | undefined} */ entry) {
  // Clearing src does not cancel synchronous server-side generation. Keep the
  // outstanding requests observable even if their decoded-cache entries go.
  if (!entry || !entry.img || session.speculativeLoads.has(entry)) return;
  entry.img.onload = null;
  entry.img.onerror = null;
  entry.img.removeAttribute('src');
}

function smallPreviewKey(/** @type {LightboxSessionContext} */ session) {
  // /full is configurable, including an original-resolution mode. Never use
  // that mode (or a large custom preview) for the wide navigation window.
  return session.options.fullPreviewLimit() > 1920 ? '1920' : 'full';
}

function estimatePreloadBytes(/** @type {LightboxSessionContext} */ session, /** @type {LightboxPhoto} */ photo, /** @type {string} */ key) {
  var cached = session.options.photoData(photo.id) || photo;
  var w = Number(cached.width || photo.width) || session.options.view().photoW || 6000;
  var h = Number(cached.height || photo.height) || session.options.view().photoH || 4000;
  var size = key === 'original' ? Math.max(w, h)
    : key === 'full' ? session.options.fullPreviewLimit() : Number(key);
  var scale = Math.min(1, size / Math.max(w, h));
  return Math.ceil(w * scale) * Math.ceil(h * scale) * 4;
}

function preloadBytes(/** @type {LightboxSessionContext} */ session) {
  var entries = new Set(Object.values(session.adjacentPreloads));
  session.speculativeLoads.forEach(function(entry) { entries.add(entry); });
  if (session.originalPreload) entries.add(session.originalPreload);
  var bytes = 0;
  entries.forEach(function(entry) { bytes += entry.bytes || 0; });
  return bytes;
}

function trimPreloadBudget(/** @type {LightboxSessionContext} */ session) {
  // Estimates reserve space before starting each decode. Correct them from
  // actual dimensions afterward, dropping distant bitmaps first. This bounds
  // retained pixel buffers, not the WebView's independent graphics/HTTP cache.
  // A larger-than-estimated original should not evict the whole small-preview
  // window before being discarded itself.
  if (preloadBytes(session) > session.preloadBudgetBytes && session.originalPreload &&
      session.originalPreload.status === 'decoded') cancelOriginal(session);
  var currentIdx = session.options.photos().findIndex(function(p) { return p.id === session.currentId; });
  var entries = Object.entries(session.adjacentPreloads).filter(function(item) {
    return item[1].status === 'decoded';
  });
  entries.sort(function(a, b) {
    function distance(/** @type {LightboxPreloadEntry} */ entry) {
      return Math.abs(session.options.photos().findIndex(function(p) { return p.id === entry.photoId; }) - currentIdx);
    }
    return Number(b[1].bytes > session.preloadBudgetBytes) - Number(a[1].bytes > session.preloadBudgetBytes) ||
      distance(b[1]) - distance(a[1]) || b[1].bytes - a[1].bytes;
  });
  while (preloadBytes(session) > session.preloadBudgetBytes && entries.length) {
    var item = entries.shift(); if (!item) break;
    releasePreloadImage(session, item[1]);
    delete session.adjacentPreloads[item[0]];
    // Do not immediately re-request an underestimated image we just evicted.
    session.adjacentRetry[item[0]] = { openSeq: session.generation, count: 2, at: 0 };
  }
}

function runSpeculativePreload(/** @type {LightboxSessionContext} */ session, /** @type {LightboxPreloadEntry} */ entry, /** @type {(decoded: boolean) => void} */ onSettled) {
  entry.requestId = ++session.preloadSerial;
  session.speculativeLoads.add(entry);
  session.speculativeInFlight = session.speculativeLoads.values().next().value || null;
  var preload = entry.img;
  preload.fetchPriority = 'low';
  /** @param {boolean} decoded */ function settle(decoded) {
    if (entry.status !== 'loading') return;
    entry.status = decoded ? 'decoded' : 'failed';
    session.speculativeLoads.delete(entry);
    session.speculativeInFlight = session.speculativeLoads.values().next().value || null;
    if (decoded) entry.bytes = preload.naturalWidth * preload.naturalHeight * 4;
    onSettled(decoded);
    trimPreloadBudget(session);
    // A navigation/close may have retired this request while it was decoding.
    if (!Object.values(session.adjacentPreloads).includes(entry) && session.originalPreload !== entry) {
      releasePreloadImage(session, entry);
    }
    var overlay = session.document.getElementById('lightboxOverlay');
    if (session.currentId != null && overlay && overlay.classList.contains('active')) {
      scheduleAdjacent(session, session.options.view().currentSrcKey, undefined);
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

export function scheduleOriginal(/** @type {LightboxSessionContext} */ session, /** @type {number} */ photoId, /** @type {number | undefined} */ retryCount) {
  cancelOriginal(session);
  if (!shouldPreloadOriginal(session, photoId)) return;
  // Dwell prevents fast arrowing from preparing every full-resolution RAW.
  // Once eligible, wait behind ready neighbors without spending a retry on
  // our own occupied server slot.
  var token = captureToken(session);
  var timer = session.window.setTimeout(function() {
    if (session.originalTimer !== timer) return;
    session.originalTimer = null;
    if (!isCurrent(session, token) || !shouldPreloadOriginal(session, photoId)) return;
    session.originalWaiting = { photoId: photoId, retryCount: retryCount || 0 };
    scheduleAdjacent(session, session.options.view().currentSrcKey, undefined);
  }, 1400);
  session.originalTimer = timer;
}

function startPendingOriginal(/** @type {LightboxSessionContext} */ session) {
  if (session.speculativeInFlight || !session.originalWaiting) return;
  var pending = session.originalWaiting;
  session.originalWaiting = null;
  if (!shouldPreloadOriginal(session, pending.photoId)) return;
  var entry = {
    img: new session.window.Image(),
    photoId: pending.photoId,
    sourceKey: 'original',
    url: session.options.sourceUrl(pending.photoId, 'original', true),
    status: 'loading',
    bytes: estimatePreloadBytes(session, { id: pending.photoId, width: session.options.view().photoW, height: session.options.view().photoH }, 'original')
  };
  if (preloadBytes(session) + entry.bytes > session.preloadBudgetBytes) return;
  session.originalPreload = entry;
  runSpeculativePreload(session, entry, function(decoded) {
    if (session.originalPreload !== entry) return;
    if (!decoded) {
      // A failed image retains no pixels and must not reserve its estimate.
      session.originalPreload = null;
      if (pending.retryCount < 1 && shouldPreloadOriginal(session, pending.photoId)) {
        scheduleOriginal(session, pending.photoId, pending.retryCount + 1);
      }
    }
  });
}

// The neighbors worth warming around the current photo, most useful first,
// and the cache keys they occupy.
function adjacentCandidates(/** @type {LightboxSessionContext} */ session, /** @type {number} */ currentIdx, /** @type {string} */ key) {
  var baseKey = smallPreviewKey(session);
  var direction = session.lastNavDelta < 0 ? -1 : 1;
  /** @type {LightboxPreloadCandidate[]} */ var candidates = [];
  /** @type {Record<string, boolean>} */ var keep = {};
  /** @param {number} offset @param {string} tier */ function addCandidate(offset, tier) {
    var photo = session.options.photos()[currentIdx + offset];
    if (!photo) return;
    if (Object.prototype.hasOwnProperty.call(photo, 'edit_recipe')) {
      session.options.rememberEditRecipe(photo.id, photo.edit_recipe);
      session.options.rememberRenderKey(photo.id, photo.render_key);
    }
    var url = session.options.sourceUrl(photo.id, tier, true);
    var cacheKey = String(photo.id) + '|' + url;
    if (keep[cacheKey]) return;
    keep[cacheKey] = true;
    candidates.push({
      photoId: photo.id, sourceKey: tier, url: url, cacheKey: cacheKey,
      bytes: estimatePreloadBytes(session, photo, tier)
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
  return {candidates: candidates, keep: keep};
}

// Drop warmups and retry records that fell out of the navigation window.
function pruneAdjacentPreloads(/** @type {LightboxSessionContext} */ session, /** @type {Record<string, boolean>} */ keep) {
  Object.keys(session.adjacentPreloads).forEach(function(cacheKey) {
    var entry = session.adjacentPreloads[cacheKey]; if (!entry) return;
    // Retaining the just-displayed warmup makes an immediate reversal cheap.
    if (keep[cacheKey] || (entry.photoId === session.currentId && entry.status === 'decoded')) return;
    releasePreloadImage(session, entry);
    delete session.adjacentPreloads[cacheKey];
  });
  Object.keys(session.adjacentRetry).forEach(function(cacheKey) {
    if (!keep[cacheKey]) delete session.adjacentRetry[cacheKey];
  });
  trimPreloadBudget(session);
}

function startAdjacentPreload(/** @type {LightboxSessionContext} */ session, /** @type {LightboxPreloadCandidate} */ candidate, /** @type {boolean} */ retrying) {
  var entry = {
    img: new session.window.Image(), photoId: candidate.photoId, sourceKey: candidate.sourceKey,
    url: candidate.url, status: 'loading', bytes: candidate.bytes, retrying: retrying
  };
  session.adjacentPreloads[candidate.cacheKey] = entry;
  runSpeculativePreload(session, entry, function(decoded) {
    if (session.adjacentPreloads[candidate.cacheKey] !== entry) return;
    if (!decoded) {
      delete session.adjacentPreloads[candidate.cacheKey];
      var retry = session.adjacentRetry[candidate.cacheKey];
      if (!retry || retry.openSeq !== session.generation) retry = { openSeq: session.generation, count: 0 };
      retry.count += 1;
      retry.at = Date.now() + 750;
      session.adjacentRetry[candidate.cacheKey] = retry;
    }
  });
}

// Start the candidates the concurrency and byte budgets admit. Returns how long
// until the earliest backed-off candidate may be retried, or null when none is
// waiting.
//
// Cache hits can transfer/decode concurrently. The server independently
// admits just one speculative cache-miss producer; declined work backs off.
// Count retired in-flight requests too: dropping an Image does not cancel
// synchronous server generation or immediately reclaim its decode memory.
function startAdjacentPreloads(/** @type {LightboxSessionContext} */ session, /** @type {LightboxPreloadCandidate[]} */ candidates) {
  var now = Date.now();
  /** @type {number | null} */ var retryAt = null;
  candidates.forEach(function(candidate) {
    if (session.speculativeLoads.size >= session.preloadConcurrency) return;
    if (session.adjacentPreloads[candidate.cacheKey]) return;
    // Once a miss has been declined, retry it alone after active transfers
    // finish. Parallel retries would simply collide with the same server slot.
    if (Array.from(session.speculativeLoads).some(function(entry) { return entry.retrying; })) return;
    var retry = session.adjacentRetry[candidate.cacheKey];
    if (retry && retry.openSeq === session.generation) {
      if (retry.count > 1) return;
      if (session.speculativeLoads.size) return;
      if (retry.at != null && retry.at > now) {
        retryAt = retryAt == null ? retry.at : Math.min(retryAt, retry.at);
        return;
      }
    }
    if (preloadBytes(session) + candidate.bytes > session.preloadBudgetBytes) return;
    startAdjacentPreload(session, candidate, !!retry);
  });
  return retryAt == null ? null : retryAt - now;
}

function primeAdjacent(/** @type {LightboxSessionContext} */ session, /** @type {string} */ sourceKey) {
  var overlay = session.document.getElementById('lightboxOverlay');
  var visibleImg = /** @type {HTMLImageElement | null} */ (session.document.getElementById('lightboxImg'));
  if (session.currentId == null || !overlay || !overlay.classList.contains('active') ||
      session.options.view().visualTransitionPending || !visibleImg ||
      !visibleImg.complete || !visibleImg.naturalWidth) return;
  // The old bitmap remains visible during a zoom source upgrade. Give that
  // interactive request priority over queued neighbors of the outgoing tier.
  // The source-load handler resumes warmups once the new pixels are ready.
  if (session.options.view().desiredSrcKey && session.options.view().desiredSrcKey !== session.options.view().currentSrcKey) return;
  if (session.options.photos().length < 2 || session.currentId == null) {
    clearAdjacent(session);
    startPendingOriginal(session);
    return;
  }
  var currentIdx = session.options.photos().findIndex(function(photo) {
    return photo.id === session.currentId;
  });
  if (currentIdx === -1) {
    startPendingOriginal(session);
    return;
  }

  cancelAdjacent(session);
  var key = sourceKey || 'full';
  var wanted = adjacentCandidates(session, currentIdx, key);
  pruneAdjacentPreloads(session, wanted.keep);
  var retryDelay = startAdjacentPreloads(session, wanted.candidates);
  if (retryDelay != null) scheduleAdjacent(session, key, retryDelay);
  else if (!session.speculativeLoads.size) startPendingOriginal(session);
}

export function scheduleAdjacent(/** @type {LightboxSessionContext} */ session, /** @type {string | null} */ sourceKey, /** @type {number | undefined} */ delay) {
  cancelAdjacent(session);
  var openSeq = session.generation;
  var timer = session.window.setTimeout(function() {
    if (session.adjacentTimer !== timer) return;
    session.adjacentTimer = null;
    if (session.generation !== openSeq || session.options.view().visualTransitionPending) return;
    primeAdjacent(session, sourceKey || 'full');
  }, delay || 0);
  session.adjacentTimer = timer;
}

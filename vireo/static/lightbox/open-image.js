/* The incoming photo's first image: what the lightbox shows while it loads, the
 * handoff of visible identity and viewport when it decodes, and the source
 * fallbacks when it fails. Each function takes the record openLightbox builds
 * for one open (`open`); a callback from an earlier open checks its token
 * before it touches the page.
 */
import { lbState } from './shared-state.js';
import {
  _lbPhotoDataByPhoto, _lbSetPhotoTransitionPending, vireoLightboxSession, vireoLightboxViewport,
} from './state.js';
import { _lbDisplayedFlagFor, _lbSetFlagStatus } from './flags.js';
import { _lbFlushDeferredOverlayApply } from './overlays.js';
import {
  _lbDetailSharpeningPending, _lbMarkDetailSettled, _lbRenderDetailStatus, _lbScheduleSourceSwap,
  _lbSetPreviewLoading, _lbSrcUrl,
} from './source-loading.js';

export function _lbRenderKeywords(/** @type {{name: string}[] | undefined} */ keywords) {
  const container = document.getElementById('lightboxKeywords');
  if (!container) return;
  container.innerHTML = '';

  var label = document.createElement('span');
  label.className = 'lightbox-keywords-label';
  label.textContent = 'Keywords';
  label.setAttribute('aria-hidden', 'true');
  container.appendChild(label);

  // A photo can carry two keywords with one name (a place under its region
  // and a top-level keyword of the same name). A pill shows only the name, so
  // the second one would repeat the first.
  /** @type {Record<string, boolean>} */ var shown = Object.create(null);
  var entries = Array.isArray(keywords) ? keywords.filter(function(keyword) {
    if (!keyword || !keyword.name) return false;
    var key = String(keyword.name).trim().toLowerCase();
    if (shown[key]) return false;
    shown[key] = true;
    return true;
  }) : [];
  if (!entries.length) {
    var empty = document.createElement('span');
    empty.className = 'lightbox-keywords-empty';
    empty.textContent = 'None';
    container.appendChild(empty);
    return;
  }

  entries.forEach(function(keyword) {
    var pill = document.createElement('span');
    pill.className = 'lightbox-keyword';
    pill.textContent = keyword.name;
    pill.title = keyword.name;
    container.appendChild(pill);
  });
}

// Shutter, aperture, ISO and focal length come with the photo's detail, so the
// line is empty until that arrives; an earlier visit's detail fills it at once.
export function _lbRenderExposure(/** @type {LightboxPhoto | null | undefined} */ detail) {
  var el = document.getElementById('lightboxExposure');
  if (!el) return;
  var text = exposureSummaryParts(detail && detail.metadata).join(' \u00b7 ');
  el.textContent = text;
  el.title = text;
}

// Where a photo sits among the photos the user is paging through. The
// lightbox's list can be a loaded window of a longer result (Browse loads a
// page at a time); the page then answers with the place in the whole result,
// so the counter never reads a page size as the number of photos.
/** @returns {{position: number, total: number}} */
function listPosition(/** @type {number} */ photoId) {
  var idx = lbState.photoList.findIndex(function(p) { return p.id === photoId; });
  var getListPosition = Vireo.pageHook('getLightboxListPosition');
  var paged = getListPosition ? getListPosition(photoId, lbState.photoList) : null;
  if (paged && paged.position >= 1 && paged.total >= paged.position) return paged;
  return { position: idx + 1, total: lbState.photoList.length };
}

function renderCounter(/** @type {number} */ photoId, /** @type {string} */ filename) {
  var counter = document.getElementById('lightboxCounter');
  if (!counter) return false;
  // Resolve the whole-result position before deciding visibility: a loaded
  // window of one photo (the last page of a 101-photo result, or a Browse page
  // of size one) is still part of a larger result, and the counter has to say
  // so.
  var place = listPosition(photoId);
  if (place.total > 1) {
    counter.textContent = place.position.toLocaleString() + ' / ' + place.total.toLocaleString()
      + (filename ? ' · ' + filename : '');
    counter.title = filename;
    counter.style.display = '';
  } else {
    counter.title = '';
    counter.style.display = 'none';
  }
  return true;
}

function renderPhotoCounter(/** @type {LightboxOpenContext} */ open) {
  return renderCounter(open.photoId, open.filename || (open.listed && open.listed.filename) || '');
}

// Redraw the counter for the photo on screen after the page's list changed
// under the open lightbox (Browse loading another page). The loaded window's
// length is not a visibility gate: renderCounter decides from the resolved
// whole-result total, so a one-photo window of a longer result still refreshes.
export function refreshLightboxCounter() {
  var visibleId = vireoLightboxSession.displayedPhotoId();
  if (visibleId == null) visibleId = vireoLightboxSession.requestedPhotoId();
  var listed = lbState.photoList.find(function(p) { return p.id === visibleId; });
  if (visibleId == null || !listed) return;
  renderCounter(visibleId, listed.filename || '');
}

// The browser keeps painting the outgoing bitmap while a newly assigned
// image source loads. Keep the visible filename and position paired with
// that bitmap, then advance all three in the image-load task before the next
// paint. Internal navigation state still advances immediately, so rapid
// arrow presses continue to target the right photo.
export function commitVisiblePhotoIdentity(/** @type {LightboxOpenContext} */ open) {
  open.label.textContent = open.filename || '';
  _lbRenderKeywords(
    open.detail && open.detail.keywords || open.listed && open.listed.keywords || []
  );
  _lbRenderExposure(open.detail || _lbPhotoDataByPhoto[String(open.photoId)]);
  _lbSetFlagStatus(_lbDisplayedFlagFor(open.photoId));
  if (!renderPhotoCounter(open)) return;
  if (!open.identityCommitted) {
    open.identityCommitted = true;
    vireoLightboxSession.commit(open.token);
    try {
      document.dispatchEvent(new CustomEvent('lightbox:photochanged', {
        detail: { photoId: open.photoId }
      }));
    } catch(_) {}
  }
}

// Give up on ever displaying this photo, without leaving the lightbox frozen:
// while the transition is pending the metadata callback skips layout updates
// and vireoLightboxViewport.save treats the incoming photo as mid-flight. The
// identity commit comes first on purpose -- releasing the controls while the
// filename, counter and vireoLightboxSession.displayedPhotoId() still name the outgoing photo
// would let the user act on a photo the UI is not showing. The outgoing
// bitmap is dropped for the same reason: leaving it painted under the
// incoming filename would let a flag or delete land on a photo the user
// cannot see.
export function abandonInitialLoad(/** @type {LightboxOpenContext} */ open) {
  if (!vireoLightboxSession.isCurrent(open.token)) return;
  commitVisiblePhotoIdentity(open);
  open.img.onload = null;
  open.img.onerror = null;
  open.img.removeAttribute('src');
  lbState.initialDecodePending = false;
  lbState.visualTransitionPending = false;
  _lbSetPhotoTransitionPending(false);
  vireoLightboxViewport.recomputeNativeZoom();
  // Apply any deferred viewport state so the layout reflects the incoming
  // photo instead of the frozen outgoing bitmap.
  if (!vireoLightboxViewport.applyPendingRestore()) vireoLightboxViewport.applyPendingOneToOne();
  vireoLightboxViewport.applyPendingEye();
  vireoLightboxViewport.applyTransform();
  _lbFlushDeferredOverlayApply();
  vireoLightboxViewport.flushDeferredLayout();
  _lbRenderDetailStatus();
}

export function handleInitialImageLoad(/** @type {LightboxOpenContext} */ open) {
  if (!vireoLightboxSession.isCurrent(open.token)) return;
  vireoLightboxSession.clearInitialLoad(open.onLoad);
  // The commit below momentarily clears the transition before deciding whether
  // a sharper tier is still needed. Paint once, at the end, so the chip does
  // not blink off and restart its show delay between those two states.
  lbState.detailStatusHold += 1;
  try {
    commitInitialImageLoad(open);
  } finally {
    lbState.detailStatusHold -= 1;
    _lbRenderDetailStatus();
  }
}

// Only record the /full tier size if the currently loaded source is still /full.
// A quick user zoom can trigger a debounced swap to a higher tier before /full
// finishes loading, which would otherwise make us record the swapped source's
// dimensions as the /full threshold.
function recordLoadedImageSize(/** @type {LightboxOpenContext} */ open) {
  var img = open.img;
  if (vireoLightboxSession.requestedPhotoId() === open.photoId && lbState.currentSrcKey === 'full' && img.naturalWidth) {
    lbState.fullLongEdge = Math.max(img.naturalWidth, img.naturalHeight);
  }
  if (vireoLightboxSession.requestedPhotoId() === open.photoId && lbState.currentSrcKey === 'original' && !lbState.photoW && img.naturalWidth) {
    lbState.photoW = img.naturalWidth;
    lbState.photoH = img.naturalHeight;
  }
}

// Apply the view this photo inherits, now that its own pixels are laid out.
function applyViewportForLoadedImage() {
  vireoLightboxViewport.recomputeNativeZoom();
  if (lbState.progressiveTargetKey && vireoLightboxViewport.hasPendingRestore()) {
    // A ready preview may be soft, but it must show the same crop/position
    // immediately. Explicit zoom clicks still use the sharp-source deferral.
    vireoLightboxViewport.applyPendingRestore();
  } else if (
    lbState.currentSrcKey === 'full' &&
    vireoLightboxViewport.pendingOneToOne() &&
    (
      lbState.originalUnavailable
        ? vireoLightboxViewport.deferOneToOneFallback()
        : vireoLightboxViewport.deferOneToOne(vireoLightboxViewport.nativeZoom() || 4)
    )
  ) {
    // The helper keeps 1:1 pending until the required source tier is current.
  } else if (!vireoLightboxViewport.applyPendingRestore()) {
    vireoLightboxViewport.applyPendingOneToOne();
  }
  vireoLightboxViewport.applyPendingEye();
  vireoLightboxViewport.applyTransform();
  _lbFlushDeferredOverlayApply();
  vireoLightboxViewport.flushDeferredLayout();
}

function commitInitialImageLoad(/** @type {LightboxOpenContext} */ open) {
  if (!vireoLightboxSession.isCurrent(open.token)) return;
  open.img.onload = null;
  open.img.onerror = null;
  commitVisiblePhotoIdentity(open);
  recordLoadedImageSize(open);
  lbState.initialDecodePending = false;
  lbState.visualTransitionPending = false;
  _lbSetPhotoTransitionPending(false);
  applyViewportForLoadedImage();
  if (lbState.progressiveTargetKey) {
    _lbSetPreviewLoading(true);
    _lbScheduleSourceSwap(vireoLightboxViewport.pendingOneToOne() ? (vireoLightboxViewport.nativeZoom() || open.transitionZoom) : vireoLightboxViewport.zoom(), true);
  } else {
    // No sharper tier is wanted for the current zoom, so what is on screen is
    // everything this view can show. (vireoLightboxSession.scheduleOriginal only warms a
    // background copy for a later 1:1 -- it does not change these pixels.)
    // A user zoom during the initial /full load (e.g. clicking 1:1) can leave
    // lbState.desiredSrcKey/lbState.previewLoading describing an upgrade in flight even
    // though lbState.progressiveTargetKey is null. Settling here would arm the fade
    // timer and race the pending upgrade -- confirming a load whose pixels
    // have not arrived yet. The upgrade's own preloader will settle when it
    // lands, or clear the chip when it fails. Match the phase predicate so
    // the chip stays on 'Sharpening…' until then.
    if (!lbState.previewLoading && !_lbDetailSharpeningPending()) _lbMarkDetailSettled();
    vireoLightboxSession.scheduleAdjacent(lbState.currentSrcKey);
    if (lbState.currentSrcKey === 'full') vireoLightboxSession.scheduleOriginal(open.photoId);
  }
}

export function handleInitialImageError(/** @type {LightboxOpenContext} */ open) {
  if (!vireoLightboxSession.isCurrent(open.token)) return;
  var img = open.img;
  if (lbState.progressiveTargetKey) {
    lbState.currentSrcKey = lbState.progressiveTargetKey;
    lbState.progressiveTargetKey = null;
    _lbSetPreviewLoading(false);
    img.src = _lbSrcUrl(open.photoId, lbState.currentSrcKey || 'full');
    return;
  }
  if (['1920', '2560', '3840'].indexOf(lbState.currentSrcKey || '') !== -1) {
    // Sized tiers can now be the initial source. A failed render must not
    // leave navigation on a broken image: try the original, whose existing
    // error path falls back to /full if the original is unavailable too.
    lbState.currentSrcKey = 'original';
    lbState.desiredSrcKey = null;
    img.src = _lbSrcUrl(open.photoId, 'original');
    return;
  }
  if (lbState.currentSrcKey !== 'original') {
    img.onload = null;
    img.onerror = null;
    vireoLightboxSession.clearInitialLoad(open.onLoad);
    // No further fallback tier is available.
    abandonInitialLoad(open);
    return;
  }
  lbState.originalUnavailable = true;
  lbState.currentSrcKey = 'full';
  lbState.desiredSrcKey = null;
  vireoLightboxSession.watchInitialImage(img, open.onLoad, open.onError);
  img.src = _lbSrcUrl(open.photoId, 'full');
}

export function startInitialImage(/** @type {LightboxOpenContext} */ open) {
  var img = open.img;
  vireoLightboxSession.watchInitialImage(img, open.onLoad, open.onError);
  var initialSrc = _lbSrcUrl(open.photoId, lbState.currentSrcKey || 'full');
  // Reopening the photo already on screen at the same source starts no load:
  // the bitmap is decoded and still displayed. Reassigning an identical src is
  // not guaranteed to emit a fresh load event, so arming a pending decode here
  // could leave a wait that nothing ever ends. A reopen that lands on a
  // different tier -- restoring a 1:1 viewport, say -- really is loading.
  var reusingVisibleBitmap = !!(
    open.reopeningVisiblePhoto &&
    img.getAttribute('src') === initialSrc &&
    img.complete &&
    img.naturalWidth > 0
  );
  if (!reusingVisibleBitmap) {
    vireoLightboxSession.setInitialLoad(open.onLoad, open.onAbandon);
    lbState.initialDecodePending = true;
  }
  _lbRenderDetailStatus();
  img.src = initialSrc;
  if (!lbState.visualTransitionPending) commitVisiblePhotoIdentity(open);
}

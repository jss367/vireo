import { navbarState } from '../navbar-state.js';
import { lbState } from './shared-state.js';
import { vireoEditNav } from '../navbar-edit-nav.js';
import {
  _lbApplyReadOnlyState, _lbGuardReadOnly, _lbPageBrowseDisabledHint, _lbPhotoDataByPhoto,
  _lbSetPhotoTransitionPending, vireoLightboxSession, vireoLightboxViewport,
} from './state.js';
import {
  _lbApplyEditButtonState, _lbClearAdjustmentPreview, _lbEditVersionByPhoto,
  _lbFlushPendingAdjustmentSave, _lbMarkEditRecipeWrite, _lbReloadCurrentRenderAfterEdit,
  _lbRememberEditRecipe, _lbRenderAdjustmentControls, _lbSetAdjustmentControlsDisabled,
  _lbSetAdjustmentStatus, _vireoBumpRenderVersion, _vireoRenderedImageSource,
  _vireoSetRenderedImageSource, vireoCacheBustedPhotoUrl, vireoRefreshPhotoRenders,
  vireoRememberPhotoPair, vireoRememberPhotoRenderKey, vireoUpdatePairSourceControls,
} from './edits.js';
import {
  _lbDisplayedFlagFor, _lbForgetConfirmedFlag, _lbRememberConfirmedFlag, _lbSetFlagStatus,
} from './flags.js';
import {
  _lbApplyBoxesVisibility, _lbApplyChromeVisibility, _lbApplyEyeVisibility,
  _lbApplyInfoVisibility, _lbApplyMaskToggleButton, _lbApplyTrackEyeState, _lbLoadMaskVariants,
  _lbMetadataOrientation,
} from './overlays.js';
import { beginLightboxInteraction, closeLightbox } from './controls.js';
import { submitToInat } from './inat.js';
import { matchesShortcut } from './keyboard.js';
import { loadPhotoDetail } from './open-detail.js';
import {
  abandonInitialLoad, handleInitialImageError, handleInitialImageLoad, startInitialImage,
} from './open-image.js';
import { _lbInitialSourceKey, _lbResetDetailStatus, _lbSetPreviewLoading } from './source-loading.js';
import { showToast } from '../navbar-shared-helpers.js';

function _lbThumbnailUrl(/** @type {LightboxPhotoId} */ photoId, /** @type {string | undefined} */ version) {
  var url = '/thumbnails/' + photoId + '.jpg';
  if (version) url += '?editv=' + encodeURIComponent(version);
  return vireoCacheBustedPhotoUrl(photoId, url);
}

export function _lbRefreshThumbnailCache(/** @type {LightboxPhotoId} */ photoId, /** @type {string} */ version) {
  var id = String(photoId);
  var refreshedUrl = _lbThumbnailUrl(id, version);
  document.querySelectorAll('img').forEach(function(img) {
    var rawSrc = _vireoRenderedImageSource(img);
    if (!rawSrc) return;
    var url;
    try {
      url = new URL(rawSrc, window.location.href);
    } catch (_) {
      return;
    }
    if (url.pathname !== '/thumbnails/' + id + '.jpg') return;
    _vireoSetRenderedImageSource(img, refreshedUrl);
  });
  try {
    document.dispatchEvent(new CustomEvent('vireo:thumbnail-invalidated', {
      detail: { photoId: Number(photoId), version: version }
    }));
  } catch (_) {}
}

export function _lbRefreshEditRecipeCache(/** @type {Record<string, LightboxRecipe | null | undefined> | undefined} */ updates) {
  if (!updates || typeof updates !== 'object') return;
  var version = String(Date.now());
  /** @type {number[]} */ var refreshedPhotoIds = [];
  Object.keys(updates).forEach(function(photoId) {
    var numericId = Number(photoId);
    _lbEditVersionByPhoto[String(photoId)] = version;
    _lbMarkEditRecipeWrite(numericId);
    _lbRememberEditRecipe(numericId, updates[photoId]);
    _vireoBumpRenderVersion(numericId);
    _lbRefreshThumbnailCache(photoId, version);
    refreshedPhotoIds.push(numericId);
  });
  if (refreshedPhotoIds.length && typeof vireoRefreshPhotoRenders === 'function') {
    vireoRefreshPhotoRenders(refreshedPhotoIds);
  }
  if (vireoLightboxSession.requestedPhotoId() != null && refreshedPhotoIds.indexOf(Number(vireoLightboxSession.requestedPhotoId())) !== -1) {
    _lbReloadCurrentRenderAfterEdit(Number(vireoLightboxSession.requestedPhotoId()));
  }
}

export var vireoRefreshEditRecipeCache = _lbRefreshEditRecipeCache;

// Retire the photo being left and begin the next one: its read-only state, the
// Escape handler, and the session and viewport requests every later step of
// this open is checked against.
function beginOpen(/** @type {number} */ photoId, /** @type {LightboxOpenOptions} */ options) {
  var alreadyOpen = !!lbState.escToken;
  var reopeningVisiblePhoto = !!(
    alreadyOpen &&
    !lbState.visualTransitionPending &&
    vireoLightboxSession.requestedPhotoId() != null &&
    String(vireoLightboxSession.requestedPhotoId()) === String(photoId)
  );
  var nextReadOnly = Object.prototype.hasOwnProperty.call(options, 'readOnly')
    ? !!options.readOnly
    : (alreadyOpen && lbState.readOnly);
  var nextReadOnlyMessage = Object.prototype.hasOwnProperty.call(options, 'readOnlyMessage')
    ? options.readOnlyMessage
    : lbState.readOnlyMessage;
  lbState.progressiveTargetKey = null;
  // Each photo earns its own quiet period. The outgoing photo's swap is dead as
  // soon as the session begins below, so neither its pending tier nor an
  // already-visible chip may carry over -- inheriting them would let a photo
  // that decodes in 100ms still announce Loading and then Full detail.
  _lbResetDetailStatus();
  _lbSetPreviewLoading(false);
  _lbFlushPendingAdjustmentSave();
  lbState.readOnly = nextReadOnly;
  lbState.readOnlyMessage = nextReadOnlyMessage || 'This lightbox is read-only';
  _lbApplyReadOnlyState();
  if (alreadyOpen && vireoLightboxSession.requestedPhotoId() != null) {
    vireoLightboxViewport.save(vireoLightboxSession.requestedPhotoId());
  }
  if (alreadyOpen) Keymap.popEsc(lbState.escToken);
  lbState.escToken = Keymap.pushEsc(function() { closeLightbox(); });
  if (!alreadyOpen) beginLightboxInteraction();
  var token = vireoLightboxSession.begin(photoId);
  var viewportOpen = vireoLightboxViewport.beginPhoto(photoId, {
    fallbackViewportState: options.fallbackViewportState || null,
    preserveOneToOne: !!options.preserveOneToOne,
    eyeTrackAnchor: options.eyeTrackAnchor || null
  });
  // During arrow navigation the <img> still paints the outgoing bitmap until
  // the replacement has decoded. Keep its transform frozen too. The incoming
  // photo's dimensions, true 1:1 scale, and normalized viewport center are
  // applied together from handleInitialImageLoad, before the browser's next
  // paint, so an off-center view never flashes through a centered state.
  // Re-running an "Open in Lightbox" command for the photo that is already
  // visible does not require an identity handoff. In particular, assigning
  // the same img.src again is not guaranteed to emit load/error, so arming the
  // transition lock here could leave every photo control inert indefinitely.
  // Do retain the lock when this call supersedes a transition already in
  // flight: vireoLightboxSession.requestedPhotoId() then names the incoming photo, not necessarily
  // the bitmap that is still visible.
  lbState.visualTransitionPending = alreadyOpen && !reopeningVisiblePhoto;
  _lbSetPhotoTransitionPending(lbState.visualTransitionPending);
  // A previous open's deferred overlay closure captured the previous photo id
  // and metadata. If this open's image resolves before its own metadata does,
  // draining that stale closure would render the previous photo's detections
  // and eye marker over the incoming bitmap.
  lbState.deferredOverlayApply = null;
  _lbApplyReadOnlyState();
  return {
    alreadyOpen: alreadyOpen, reopeningVisiblePhoto: reopeningVisiblePhoto, token: token,
    transitionZoom: viewportOpen.zoom, restoreWantsOneToOne: viewportOpen.oneToOne
  };
}

// Take what the page's navigation list already knows about the photo: its
// RAW/JPEG pair, flag and wildlife exclusion. Returns the list's entry, if any.
function adoptListedPhoto(/** @type {number} */ photoId, /** @type {LightboxPhoto[] | undefined} */ photoList, /** @type {boolean} */ alreadyOpen) {
  if (photoList) lbState.photoList = photoList;
  // Remember the photo being viewed so the Edit page (/edit with no id) can
  // resolve "the photo I was just looking at".
  if (vireoEditNav) vireoEditNav.setLastPhoto(photoId);
  _lbApplyEditButtonState();
  var listed = lbState.photoList.find(function(x) { return x.id === photoId; });
  if (listed && typeof vireoRememberPhotoPair === 'function') {
    vireoRememberPhotoPair(listed);
  }
  if (typeof vireoUpdatePairSourceControls === 'function') {
    vireoUpdatePairSourceControls(photoId);
  }
  if (listed && Object.prototype.hasOwnProperty.call(listed, 'flag')) {
    _lbRememberConfirmedFlag(photoId, listed.flag);
  } else {
    // Flagless opens (e.g. review/misses/cull push only {id, filename}) must
    // not reuse a flag remembered from an earlier open; rely on the
    // /api/photos/:id fetch to repopulate the confirmed flag instead.
    _lbForgetConfirmedFlag(photoId);
  }
  lbState.currentWildlifeExcluded = !!(listed && listed.wildlife_excluded);
  if (!alreadyOpen) _lbSetFlagStatus(_lbDisplayedFlagFor(photoId));
  return listed;
}

// Reset pan for the new photo, but keep a 1:1 transform during arrow
// navigation so the outgoing image does not visibly snap back to fit before
// the replacement finishes loading.
function clearOutgoingPhoto(/** @type {HTMLElement} */ wrap, /** @type {number} */ transitionZoom) {
  wrap.classList.toggle('zoomed', transitionZoom > 1.001);
  wrap.scrollTop = 0;
  wrap.scrollLeft = 0;
  document.dispatchEvent(new Event('lightbox:subjectreset'));
  // Clear previous detection overlays
  var detContainer = document.getElementById('lightboxDetections');
  if (detContainer) detContainer.innerHTML = '';
  // Clear previous eye crosshair; re-rendered when /api/photos/<id> resolves.
  var eyeContainer = document.getElementById('lightboxEye');
  if (eyeContainer) { eyeContainer.innerHTML = ''; eyeContainer.style.display = 'none'; }
}

// Pick the first source to show from what the catalog already says about the
// photo, preferring a preview the session has decoded.
function chooseInitialSource(/** @type {LightboxOpenContext} */ open, /** @type {boolean} */ restoreWantsOneToOne) {
  var photoId = open.photoId;
  var listed = open.listed;
  lbState.photoW = null;
  lbState.photoH = null;
  lbState.photoOrientation = null;
  lbState.currentEditRecipe = listed && listed.edit_recipe || null;
  if (listed && Object.prototype.hasOwnProperty.call(listed, 'edit_recipe')) {
    _lbRememberEditRecipe(photoId, listed.edit_recipe);
    vireoRememberPhotoRenderKey(photoId, listed.render_key);
  }
  /** @type {LightboxPhoto} */ var incomingPhoto = Object.assign({id: photoId}, _lbPhotoDataByPhoto[String(photoId)] || {}, listed || {});
  // Catalog geometry lets us select a tier before the next image has loaded.
  // Never consult the outgoing bitmap for the incoming photo's dimensions.
  lbState.photoW = incomingPhoto.width || null;
  lbState.photoH = incomingPhoto.height || null;
  lbState.photoOrientation = _lbMetadataOrientation(incomingPhoto.metadata);
  var initialTarget = _lbInitialSourceKey(incomingPhoto, open.transitionZoom, restoreWantsOneToOne);
  var warmInitial = vireoLightboxSession.decodedPreview(photoId, initialTarget);
  lbState.currentSrcKey = warmInitial ? warmInitial.sourceKey : initialTarget;
  if (lbState.currentSrcKey !== initialTarget) lbState.progressiveTargetKey = initialTarget;
  lbState.fullLongEdge = null;
  vireoLightboxViewport.updateControls();
}

// Forget the outgoing photo's source availability and recipe. The adjustment
// controls stay disabled until this photo's own recipe arrives.
function resetEditState() {
  lbState.originalUnavailable = false;
  lbState.fullUsesOriginal = null;
  lbState.editRecipe = null;
  lbState.editRecipeLoaded = false;
  lbState.adjustmentSource = null;
  if (lbState.adjustSaveTimer) {
    clearTimeout(lbState.adjustSaveTimer);
    lbState.adjustSaveTimer = null;
  }
  lbState.adjustSeq += 1;
  _lbClearAdjustmentPreview();
  _lbRenderAdjustmentControls();
  _lbSetAdjustmentControlsDisabled(true);
  _lbSetAdjustmentStatus('');
}

function bindPhotoButtons(/** @type {number} */ photoId, /** @type {HTMLElement} */ inatBtn) {
  inatBtn.onclick = function() { submitToInat(photoId); };
  var editPhotoBtn = document.getElementById('lightboxEditPhoto');
  if (!editPhotoBtn) return;
  editPhotoBtn.onclick = function(e) {
    if (e) e.stopPropagation();
    if (_lbGuardReadOnly()) return false;
    var editHint = _lbPageBrowseDisabledHint(photoId);
    if (editHint) {
      if (typeof showToast === 'function') showToast(editHint, 'warning');
      return false;
    }
    // Hand the current ordered photo list to the editor so it can offer
    // Prev/Next just like the lightbox does.
    if (vireoEditNav) {
      vireoEditNav.setList(lbState.photoList, photoId);
      vireoEditNav.setLastPhoto(photoId);
    }
    window.location.href = '/edit/' + photoId;
  };
}

function showLightbox(/** @type {HTMLElement} */ overlay, /** @type {number} */ photoId) {
  overlay.classList.add('active');
  _lbApplyInfoVisibility();
  _lbApplyChromeVisibility();
  _lbApplyEyeVisibility();
  _lbApplyTrackEyeState();
  _lbApplyMaskToggleButton();

  _lbApplyBoxesVisibility();

  // Fetch the photo's SAM mask variants and (if any) populate the
  // overlay-variant dropdown. Hidden when the photo has no masks, so
  // pages that don't run the pipeline still get the lightbox unchanged.
  _lbLoadMaskVariants(photoId);
}

// Open a photo, or move an open lightbox to another one. The session and
// viewport requests begin at once, so rapid navigation always targets the
// right photo; what the user sees changes only when the incoming image has
// decoded (open-image.js), and the photo's detail arrives on its own schedule
// (open-detail.js).
export function openLightbox(/** @type {number} */ photoId, /** @type {string | null | undefined} */ filename, /** @type {LightboxPhoto[] | undefined} */ photoList = undefined, /** @type {LightboxOpenOptions | undefined} */ options = undefined) {
  var begun = beginOpen(photoId, options || {});
  var listed = adoptListedPhoto(photoId, photoList, begun.alreadyOpen);

  var overlay = document.getElementById('lightboxOverlay');
  var img = /** @type {HTMLImageElement | null} */ (document.getElementById('lightboxImg'));
  var label = document.getElementById('lightboxFilename');
  var inatBtn = document.getElementById('lightboxInat');
  var wrap = document.getElementById('lightboxWrap');
  if (!overlay || !img || !label || !inatBtn || !wrap) return;

  /** @type {LightboxOpenContext} */ var open = {
    photoId: photoId, filename: filename, token: begun.token, listed: listed, img: img, label: label,
    transitionZoom: begun.transitionZoom, reopeningVisiblePhoto: begun.reopeningVisiblePhoto,
    detail: null, identityCommitted: false,
    onLoad: function() { handleInitialImageLoad(open); },
    onError: function() { handleInitialImageError(open); },
    onAbandon: function() { abandonInitialLoad(open); }
  };
  clearOutgoingPhoto(wrap, begun.transitionZoom);
  chooseInitialSource(open, begun.restoreWantsOneToOne);
  resetEditState();
  vireoLightboxSession.cancelSwap();
  lbState.desiredSrcKey = null;
  if (!lbState.visualTransitionPending) {
    vireoLightboxViewport.applyTransform();
  }
  loadPhotoDetail(open);
  startInitialImage(open);
  bindPhotoButtons(photoId, inatBtn);
  showLightbox(overlay, photoId);
}

export function lightboxNav(/** @type {number} */ delta) {
  // A one-photo list can still be one page of a larger lazy-loaded dataset.
  // Let the normal boundary event fire so the owning page can fetch the next
  // or previous page instead of swallowing the first navigation attempt.
  if (lbState.photoList.length === 0) return;
  var idx = lbState.photoList.findIndex(function(p) { return p.id === vireoLightboxSession.requestedPhotoId(); });
  if (idx === -1) return;
  var newIdx = idx + delta;
  if (newIdx < 0 || newIdx >= lbState.photoList.length) {
    try {
      document.dispatchEvent(new CustomEvent('lightbox:navigationboundary', {
        detail: {
          delta: delta,
          photoId: vireoLightboxSession.requestedPhotoId(),
          index: idx,
          photoCount: lbState.photoList.length
        }
      }));
    } catch (_) {}
    return;
  }
  var currentViewportState = vireoLightboxViewport.save(vireoLightboxSession.requestedPhotoId());
  var eyeTrackAnchor = vireoLightboxViewport.captureEyeAnchor();
  var next = lbState.photoList[newIdx]; if (!next) return;
  vireoLightboxSession.noteDirection(delta);
  /** @type {LightboxOpenOptions} */ var navOptions = {
    fallbackViewportState: currentViewportState,
    preserveOneToOne: vireoLightboxViewport.isOneToOne(),
    eyeTrackAnchor: eyeTrackAnchor
  };
  // Re-evaluate the owning page's read-only decision for the new photo. The
  // list may mix in-scope and out-of-scope photos (Find Similar in Pipeline
  // Review), and openLightbox otherwise inherits the previous photo's
  // _lbReadOnly, which would leave destructive controls enabled when arrowing
  // from an in-scope result to an out-of-scope one (or stuck read-only in the
  // other direction). Group Review's open-burst exception is preserved by
  // isPhotoActionableInReviewScope().
  var getLightboxOpenOptions = Vireo.pageHook('getLightboxOpenOptions');
  if (getLightboxOpenOptions) {
    var opened = getLightboxOpenOptions(next.id);
    if (opened && Object.prototype.hasOwnProperty.call(opened, 'readOnly')) {
      navOptions.readOnly = opened.readOnly;
      navOptions.readOnlyMessage = opened.readOnlyMessage;
    }
  }
  openLightbox(next.id, next.filename, lbState.photoList, navOptions);
}

export function requestLightboxFullscreen() {
  var overlayCandidate = /** @type {HTMLElement | null} */ (document.getElementById('lightboxOverlay')); if (!overlayCandidate) return; const overlay = overlayCandidate;
  if (!overlay || !overlay.classList.contains('active')) return;
  if (document.fullscreenElement === overlay || document.webkitFullscreenElement === overlay) return;
  var request = overlay.requestFullscreen || overlay.webkitRequestFullscreen;
  if (!request) return;
  try {
    var result = request.call(overlay);
    if (result && typeof result.catch === 'function') result.catch(function() {});
  } catch(e) {}
}

export function exitLightboxFullscreen() {
  if (!document.fullscreenElement && !document.webkitFullscreenElement) return;
  var exit = document.exitFullscreen || document.webkitExitFullscreen;
  if (!exit) return;
  try {
    var result = exit.call(document);
    if (result && typeof result.catch === 'function') result.catch(function() {});
  } catch(e) {}
}

export function lightboxKeyMatchesConfiguredBrowseShortcut(/** @type {KeyboardEvent} */ e) {
  var browse = navbarState.shortcuts && navbarState.shortcuts.browse;
  if (!browse) return false;
  for (var action in browse) {
    if (Object.prototype.hasOwnProperty.call(browse, action) && matchesShortcut(e, browse[action])) {
      return true;
    }
  }
  return false;
}

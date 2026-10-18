import { navbarState } from '../navbar-state.js';
import { lbState } from './shared-state.js';
import { vireoEditNav } from '../navbar-edit-nav.js';
import {
  _lbApplyReadOnlyState, _lbEditRecipeByPhoto, _lbGuardReadOnly, _lbPageBrowseDisabledHint,
  _lbPhotoDataByPhoto, _lbRawEditRecipeByPhoto, _lbSetPhotoTransitionPending,
  vireoLightboxSession, vireoLightboxViewport,
} from './state.js';
import { _lbRenderLifeListPanel } from './curation.js';
import {
  _lbApplyEditButtonState, _lbClearAdjustmentPreview, _lbCloneEditRecipe, _lbCloneRecipe,
  _lbEditRecipeWriteSeqFor, _lbEditVersionByPhoto, _lbFlushPendingAdjustmentSave,
  _lbMarkEditRecipeWrite, _lbRecipeHasEdits, _lbReloadCurrentRenderAfterEdit,
  _lbRememberEditRecipe, _lbRenderAdjustmentControls, _lbSetAdjustmentControlsDisabled,
  _lbSetAdjustmentStatus, _vireoBumpRenderVersion, _vireoEditRecipeCacheKey,
  _vireoPairKnownByPhoto, _vireoPairSourceImageLoaded, _vireoRenderedImageSource,
  _vireoSetRenderedImageSource, vireoCacheBustedPhotoUrl, vireoRefreshPhotoRenders,
  vireoRememberPhotoPair, vireoRememberPhotoRenderKey, vireoUpdatePairSourceControls,
} from './edits.js';
import {
  _lbDisplayedFlagFor, _lbForgetConfirmedFlag, _lbRecordFetchedFlag, _lbRememberConfirmedFlag,
  _lbSetFlagStatus,
} from './flags.js';
import {
  _lbApplyBoxesVisibility, _lbApplyChromeVisibility, _lbApplyEyeVisibility,
  _lbApplyInfoVisibility, _lbApplyMaskToggleButton, _lbApplyMaskVisibility,
  _lbApplyTrackEyeState, _lbFlushDeferredOverlayApply, _lbLoadDetections, _lbLoadMaskVariants,
  _lbMetadataOrientation, _lbRenderEyeCrosshair, _lbResetMaskOverlay,
  _lbSourceOverlaysAvailable,
} from './overlays.js';
import { closeLightbox } from './controls.js';
import { submitToInat } from './inat.js';
import { matchesShortcut } from './keyboard.js';
import {
  _lbDetailSharpeningPending, _lbInitialSourceKey, _lbMarkDetailSettled, _lbRenderDetailStatus,
  _lbResetDetailStatus, _lbScheduleSourceSwap, _lbSetPreviewLoading, _lbSrcUrl,
} from './source-loading.js';
import { openPipeline } from '../navbar-pipeline-inspector.js';
import { findSimilar } from '../navbar-similar-photos.js';
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

export function _lbRenderKeywords(/** @type {{name: string}[] | undefined} */ keywords) {
  const container = document.getElementById('lightboxKeywords');
  if (!container) return;
  container.innerHTML = '';

  var label = document.createElement('span');
  label.className = 'lightbox-keywords-label';
  label.textContent = 'Keywords';
  label.setAttribute('aria-hidden', 'true');
  container.appendChild(label);

  var entries = Array.isArray(keywords) ? keywords.filter(function(keyword) {
    return keyword && keyword.name;
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

export function openLightbox(/** @type {number} */ photoId, /** @type {string | null | undefined} */ filename, /** @type {LightboxPhoto[] | undefined} */ photoList = undefined, /** @type {LightboxOpenOptions | undefined} */ options = undefined) {
  var alreadyOpen = !!lbState.escToken;
  var transitionAlreadyPending = lbState.visualTransitionPending;
  var reopeningVisiblePhoto = !!(
    alreadyOpen &&
    !transitionAlreadyPending &&
    vireoLightboxSession.requestedPhotoId() != null &&
    String(vireoLightboxSession.requestedPhotoId()) === String(photoId)
  );
  options = options || {};
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
  var fallbackViewportState = options.fallbackViewportState || null;
  var eyeTrackAnchor = options.eyeTrackAnchor || null;
  _lbFlushPendingAdjustmentSave();
  lbState.readOnly = nextReadOnly;
  lbState.readOnlyMessage = nextReadOnlyMessage || 'This lightbox is read-only';
  _lbApplyReadOnlyState();
  if (alreadyOpen && vireoLightboxSession.requestedPhotoId() != null) {
    vireoLightboxViewport.save(vireoLightboxSession.requestedPhotoId());
  }
  if (alreadyOpen) Keymap.popEsc(lbState.escToken);
  lbState.escToken = Keymap.pushEsc(function() { closeLightbox(); });
  if (!alreadyOpen) Keymap.lockBodyScroll();
  var openToken = vireoLightboxSession.begin(photoId);
  var viewportOpen = vireoLightboxViewport.beginPhoto(photoId, {
    fallbackViewportState: fallbackViewportState,
    preserveOneToOne: !!options.preserveOneToOne,
    eyeTrackAnchor: eyeTrackAnchor
  });
  var transitionZoom = viewportOpen.zoom;
  var restoreWantsOneToOne = viewportOpen.oneToOne;
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
  if (photoList) lbState.photoList = photoList;
  // Remember the photo being viewed so the Edit page (/edit with no id) can
  // resolve "the photo I was just looking at".
  if (vireoEditNav) vireoEditNav.setLastPhoto(photoId);
  _lbApplyEditButtonState();
  var currentFromList = lbState.photoList.find(function(x) { return x.id === photoId; });
  if (currentFromList && typeof vireoRememberPhotoPair === 'function') {
    vireoRememberPhotoPair(currentFromList);
  }
  if (typeof vireoUpdatePairSourceControls === 'function') {
    vireoUpdatePairSourceControls(photoId);
  }
  if (currentFromList && Object.prototype.hasOwnProperty.call(currentFromList, 'flag')) {
    _lbRememberConfirmedFlag(photoId, currentFromList.flag);
  } else {
    // Flagless opens (e.g. review/misses/cull push only {id, filename}) must
    // not reuse a flag remembered from an earlier open; rely on the
    // /api/photos/:id fetch to repopulate the confirmed flag instead.
    _lbForgetConfirmedFlag(photoId);
  }
  lbState.currentWildlifeExcluded = !!(currentFromList && currentFromList.wildlife_excluded);
  if (!alreadyOpen) _lbSetFlagStatus(_lbDisplayedFlagFor(photoId));

  var overlayCandidate = /** @type {HTMLElement | null} */ (document.getElementById('lightboxOverlay')); if (!overlayCandidate) return; const overlay = overlayCandidate;
  var imgCandidate = /** @type {HTMLImageElement | null} */ (document.getElementById('lightboxImg')); if (!imgCandidate) return; const img = imgCandidate;
  var labelCandidate = /** @type {HTMLElement | null} */ (document.getElementById('lightboxFilename')); if (!labelCandidate) return; const label = labelCandidate;
  var inspectBtnCandidate = /** @type {HTMLElement | null} */ (document.getElementById('lightboxInspect')); if (!inspectBtnCandidate) return; const inspectBtn = inspectBtnCandidate;
  var similarBtnCandidate = /** @type {HTMLElement | null} */ (document.getElementById('lightboxSimilar')); if (!similarBtnCandidate) return; const similarBtn = similarBtnCandidate;
  var inatBtnCandidate = /** @type {HTMLElement | null} */ (document.getElementById('lightboxInat')); if (!inatBtnCandidate) return; const inatBtn = inatBtnCandidate;
  var wrapCandidate = /** @type {HTMLElement | null} */ (document.getElementById('lightboxWrap')); if (!wrapCandidate) return; const wrap = wrapCandidate;
  /** @type {LightboxPhoto | null} */ var detailData = null;

  // The browser keeps painting the outgoing bitmap while a newly assigned
  // image source loads. Keep the visible filename and position paired with
  // that bitmap, then advance all three in the image-load task before the next
  // paint. Internal navigation state still advances immediately, so rapid
  // arrow presses continue to target the right photo.
  var visibleIdentityCommitted = false;
  function commitVisiblePhotoIdentity() {
    label.textContent = filename || '';
    _lbRenderKeywords(
      detailData && detailData.keywords || currentFromList && currentFromList.keywords || []
    );
    _lbSetFlagStatus(_lbDisplayedFlagFor(photoId));
    var counterCandidate = /** @type {HTMLElement | null} */ (document.getElementById('lightboxCounter')); if (!counterCandidate) return; const counter = counterCandidate;
    if (lbState.photoList.length > 1) {
      var idx = lbState.photoList.findIndex(function(p) { return p.id === photoId; });
      var counterFilename = filename || (currentFromList && currentFromList.filename) || '';
      counter.textContent = (idx + 1) + ' / ' + lbState.photoList.length
        + (counterFilename ? ' \u00b7 ' + counterFilename : '');
      counter.title = counterFilename;
      counter.style.display = '';
    } else {
      counter.title = '';
      counter.style.display = 'none';
    }
    if (!visibleIdentityCommitted) {
      visibleIdentityCommitted = true;
      vireoLightboxSession.commit(openToken);
      try {
        document.dispatchEvent(new CustomEvent('lightbox:photochanged', {
          detail: { photoId: photoId }
        }));
      } catch(_) {}
    }
  }

  // Reset pan for the new photo, but keep a 1:1 transform during arrow
  // navigation so the outgoing image does not visibly snap back to fit before
  // the replacement finishes loading.
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

  // Reset continuous zoom state for new photo
  lbState.photoW = null;
  lbState.photoH = null;
  lbState.photoOrientation = null;
  lbState.currentEditRecipe = currentFromList && currentFromList.edit_recipe || null;
  if (currentFromList && Object.prototype.hasOwnProperty.call(currentFromList, 'edit_recipe')) {
    _lbRememberEditRecipe(photoId, currentFromList.edit_recipe);
    vireoRememberPhotoRenderKey(photoId, currentFromList.render_key);
  }
  /** @type {LightboxPhoto} */ var incomingPhoto = Object.assign({id: photoId}, _lbPhotoDataByPhoto[String(photoId)] || {}, currentFromList || {});
  // Catalog geometry lets us select a tier before the next image has loaded.
  // Never consult the outgoing bitmap for the incoming photo's dimensions.
  lbState.photoW = incomingPhoto.width || null;
  lbState.photoH = incomingPhoto.height || null;
  lbState.photoOrientation = _lbMetadataOrientation(incomingPhoto.metadata);
  var initialTarget = _lbInitialSourceKey(incomingPhoto, transitionZoom, restoreWantsOneToOne);
  var warmInitial = vireoLightboxSession.decodedPreview(photoId, initialTarget);
  lbState.currentSrcKey = warmInitial ? warmInitial.sourceKey : initialTarget;
  if (lbState.currentSrcKey !== initialTarget) lbState.progressiveTargetKey = initialTarget;
  lbState.fullLongEdge = null;
  vireoLightboxViewport.updateControls();
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
  vireoLightboxSession.cancelSwap();
  lbState.desiredSrcKey = null;
  if (!lbState.visualTransitionPending) {
    vireoLightboxViewport.applyTransform();
  }

  // Fetch original dimensions (async, non-blocking for image display)
  var flagFetchSeq = lbState.flagEditSeq;
  var editRecipeFetchSeq = _lbEditRecipeWriteSeqFor(photoId);
  var editFetchSeq = lbState.adjustSeq;
  fetch('/api/photos/' + photoId)
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(/** @type {LightboxPhoto} */ data) {
      if (!data || !vireoLightboxSession.isCurrent(openToken)) return;
      var pairWasKnown = !!_vireoPairKnownByPhoto[String(photoId)];
      if (typeof vireoRememberPhotoPair === 'function') {
        vireoRememberPhotoPair(data);
      }
      if (typeof vireoUpdatePairSourceControls === 'function') {
        vireoUpdatePairSourceControls(photoId);
      }
      if (!pairWasKnown && _vireoPairKnownByPhoto[String(photoId)]) {
        var pairImgCandidate = /** @type {HTMLImageElement | null} */ (document.getElementById('lightboxImg')); if (!pairImgCandidate) return; const pairImg = pairImgCandidate;
        vireoLightboxSession.watchImage(pairImg, function() {
          _vireoPairSourceImageLoaded(photoId, 'jpeg', pairImg);
        });
        _vireoBumpRenderVersion(photoId);
        vireoRefreshPhotoRenders([photoId]);
      }
      var editRecipeFresh = _lbEditRecipeWriteSeqFor(photoId) === editRecipeFetchSeq;
      // Fingerprint the unclipped recipe on both sides of this comparison:
      // ``data.edit_recipe`` arrives straight from the server, so reading the
      // clipped ``_lbEditRecipeByPhoto`` view here would report a change on
      // every refetch of any photo carrying a section the lightbox does not
      // model (``local``), and bump the render version for nothing.
      var recipeBefore = _lbRawEditRecipeByPhoto[String(photoId)];
      if (recipeBefore === undefined) recipeBefore = _lbEditRecipeByPhoto[String(photoId)];
      var recipeKeyBefore = _vireoEditRecipeCacheKey(recipeBefore);
      if (!editRecipeFresh) {
        var currentRecipe = _lbEditRecipeByPhoto[String(photoId)];
        data.edit_recipe = _lbRecipeHasEdits(currentRecipe) ? _lbCloneEditRecipe(currentRecipe) : null;
      }
      _lbPhotoDataByPhoto[String(photoId)] = data;
      detailData = data;
      if (visibleIdentityCommitted) _lbRenderKeywords(data.keywords);
      _lbRenderLifeListPanel(photoId);
      if (editRecipeFresh) {
        _lbRememberEditRecipe(photoId, data.edit_recipe);
        if (typeof vireoRememberPhotoRenderKey === 'function') {
          vireoRememberPhotoRenderKey(photoId, data.render_key);
        }
        if (_vireoEditRecipeCacheKey(data.edit_recipe) !== recipeKeyBefore) {
          _vireoBumpRenderVersion(photoId);
          if (typeof vireoRefreshPhotoRenders === 'function') {
            vireoRefreshPhotoRenders([photoId]);
          }
          _lbReloadCurrentRenderAfterEdit(photoId);
        }
      }
      _lbApplyMaskVisibility();
      lbState.photoW = data.width || null;
      lbState.photoH = data.height || null;
      lbState.fullUsesOriginal = !!data.full_uses_original;
      lbState.sessionFullUsesOriginal = lbState.fullUsesOriginal;
      if (data.full_preview_max_size != null &&
          Number.isFinite(Number(data.full_preview_max_size)) && Number(data.full_preview_max_size) >= 0) {
        lbState.previewMaxSize = Number(data.full_preview_max_size);
      }
      lbState.photoOrientation = _lbMetadataOrientation(data.metadata);
      var currentLayoutRecipe = _lbEditRecipeByPhoto[String(photoId)];
      lbState.currentEditRecipe = _lbRecipeHasEdits(currentLayoutRecipe)
        ? currentLayoutRecipe || null
        : null;
      if (!_lbSourceOverlaysAvailable()) {
        var detContainer2 = document.getElementById('lightboxDetections');
        if (detContainer2) detContainer2.innerHTML = '';
        var eyeContainer2 = document.getElementById('lightboxEye');
        if (eyeContainer2) eyeContainer2.innerHTML = '';
        _lbResetMaskOverlay();
      }
      lbState.currentWildlifeExcluded = !!data.wildlife_excluded;
      if (editFetchSeq === lbState.adjustSeq) {
        lbState.editRecipe = _lbCloneRecipe(data.edit_recipe || {});
        lbState.editRecipeLoaded = true;
        _lbRenderAdjustmentControls();
        _lbSetAdjustmentControlsDisabled(false);
        _lbSetAdjustmentStatus('');
      }
      _lbRecordFetchedFlag(photoId, data.flag, flagFetchSeq);
      // Metadata commonly wins the race with image decoding. Updating the
      // transform here would lay out the still-visible outgoing bitmap using
      // the incoming photo's dimensions, producing the navigation jerk. Let
      // the image-load handler commit image + viewport atomically instead.
      if (!lbState.visualTransitionPending) {
        vireoLightboxViewport.recomputeNativeZoom();
        if (!vireoLightboxViewport.applyPendingRestore()) vireoLightboxViewport.applyPendingOneToOne();
        vireoLightboxViewport.applyPendingEye(data);
        vireoLightboxSession.scheduleAdjacent(lbState.currentSrcKey);
        if (lbState.currentSrcKey === 'full') vireoLightboxSession.scheduleOriginal(photoId);
      }
      // Detection boxes, the eye marker, and the mask overlay are children of
      // lightboxTransform. While _lbVisualTransitionPending is true the
      // transform is intentionally frozen on the outgoing bitmap, so painting
      // the incoming photo's overlays now would layer them over the previous
      // image until handleInitialImageLoad swaps in the new bitmap. Defer the
      // render calls that actually inject DOM content until the transition
      // clears; the visibility/toggle helpers are safe to run either way
      // because they only touch empty containers or button labels.
      var applyOverlays = function() {
        _lbLoadDetections(photoId);
        _lbRenderEyeCrosshair(data);
      };
      _lbApplyBoxesVisibility();
      _lbApplyEyeVisibility();
      _lbApplyMaskToggleButton();
      _lbApplyMaskVisibility();
      if (lbState.visualTransitionPending) {
        lbState.deferredOverlayApply = applyOverlays;
      } else {
        lbState.deferredOverlayApply = null;
        applyOverlays();
      }
      _lbApplyTrackEyeState();
    })
    .catch(function() {
      if (!vireoLightboxSession.isCurrent(openToken)) return;
      _lbSetAdjustmentStatus('Could not load recipe', true);
    });

  // Give up on ever displaying this photo, without leaving the lightbox frozen:
  // while the transition is pending the metadata callback skips layout updates
  // and vireoLightboxViewport.save treats the incoming photo as mid-flight. The
  // identity commit comes first on purpose -- releasing the controls while the
  // filename, counter and vireoLightboxSession.displayedPhotoId() still name the outgoing photo
  // would let the user act on a photo the UI is not showing. The outgoing
  // bitmap is dropped for the same reason: leaving it painted under the
  // incoming filename would let a flag or delete land on a photo the user
  // cannot see.
  function abandonInitialLoad() {
    if (!vireoLightboxSession.isCurrent(openToken)) return;
    commitVisiblePhotoIdentity();
    img.onload = null;
    img.onerror = null;
    img.removeAttribute('src');
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

  function handleInitialImageLoad() {
    if (!vireoLightboxSession.isCurrent(openToken)) return;
    vireoLightboxSession.clearInitialLoad(handleInitialImageLoad);
    // The commit below momentarily clears the transition before deciding whether
    // a sharper tier is still needed. Paint once, at the end, so the chip does
    // not blink off and restart its show delay between those two states.
    lbState.detailStatusHold += 1;
    try {
      commitInitialImageLoad();
    } finally {
      lbState.detailStatusHold -= 1;
      _lbRenderDetailStatus();
    }
  }
  function commitInitialImageLoad() {
    if (!vireoLightboxSession.isCurrent(openToken)) return;
    img.onload = null;
    img.onerror = null;
    commitVisiblePhotoIdentity();
    // Only record the /full tier size if the currently loaded source is still /full.
    // A quick user zoom can trigger a debounced swap to a higher tier before /full
    // finishes loading, which would otherwise make us record the swapped source's
    // dimensions as the /full threshold.
    if (vireoLightboxSession.requestedPhotoId() === photoId && lbState.currentSrcKey === 'full' && img.naturalWidth) {
      lbState.fullLongEdge = Math.max(img.naturalWidth, img.naturalHeight);
    }
    if (vireoLightboxSession.requestedPhotoId() === photoId && lbState.currentSrcKey === 'original' && !lbState.photoW && img.naturalWidth) {
      lbState.photoW = img.naturalWidth;
      lbState.photoH = img.naturalHeight;
    }
    lbState.initialDecodePending = false;
    lbState.visualTransitionPending = false;
    _lbSetPhotoTransitionPending(false);
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
    if (lbState.progressiveTargetKey) {
      _lbSetPreviewLoading(true);
      _lbScheduleSourceSwap(vireoLightboxViewport.pendingOneToOne() ? (vireoLightboxViewport.nativeZoom() || transitionZoom) : vireoLightboxViewport.zoom(), true);
    } else {
      // No sharper tier is wanted for the current zoom, so what is on screen is
      // everything this view can show. (vireoLightboxSession.scheduleOriginal only warms a
      // background copy for a later 1:1 -- it does not change these pixels.)
      // A user zoom during the initial /full load (e.g. clicking 1:1) can leave
      // _lbDesiredSrcKey/_lbPreviewLoading describing an upgrade in flight even
      // though _lbProgressiveTargetKey is null. Settling here would arm the fade
      // timer and race the pending upgrade -- confirming a load whose pixels
      // have not arrived yet. The upgrade's own preloader will settle when it
      // lands, or clear the chip when it fails. Match the phase predicate so
      // the chip stays on 'Sharpening…' until then.
      if (!lbState.previewLoading && !_lbDetailSharpeningPending()) _lbMarkDetailSettled();
      vireoLightboxSession.scheduleAdjacent(lbState.currentSrcKey);
      if (lbState.currentSrcKey === 'full') vireoLightboxSession.scheduleOriginal(photoId);
    }
  }
  function handleInitialImageError() {
    if (!vireoLightboxSession.isCurrent(openToken)) return;
    if (lbState.progressiveTargetKey) {
      lbState.currentSrcKey = lbState.progressiveTargetKey;
      lbState.progressiveTargetKey = null;
      _lbSetPreviewLoading(false);
      img.src = _lbSrcUrl(photoId, lbState.currentSrcKey || 'full');
      return;
    }
    if (['1920', '2560', '3840'].indexOf(lbState.currentSrcKey || '') !== -1) {
      // Sized tiers can now be the initial source. A failed render must not
      // leave navigation on a broken image: try the original, whose existing
      // error path falls back to /full if the original is unavailable too.
      lbState.currentSrcKey = 'original';
      lbState.desiredSrcKey = null;
      img.src = _lbSrcUrl(photoId, 'original');
      return;
    }
    if (lbState.currentSrcKey !== 'original') {
      img.onload = null;
      img.onerror = null;
      vireoLightboxSession.clearInitialLoad(handleInitialImageLoad);
      // No further fallback tier is available.
      abandonInitialLoad();
      return;
    }
    lbState.originalUnavailable = true;
    lbState.currentSrcKey = 'full';
    lbState.desiredSrcKey = null;
    vireoLightboxSession.watchInitialImage(img, handleInitialImageLoad, handleInitialImageError);
    img.src = _lbSrcUrl(photoId, 'full');
  }
  vireoLightboxSession.watchInitialImage(img, handleInitialImageLoad, handleInitialImageError);
  var initialSrc = _lbSrcUrl(photoId, lbState.currentSrcKey || 'full');
  // Reopening the photo already on screen at the same source starts no load:
  // the bitmap is decoded and still displayed. Reassigning an identical src is
  // not guaranteed to emit a fresh load event, so arming a pending decode here
  // could leave a wait that nothing ever ends. A reopen that lands on a
  // different tier -- restoring a 1:1 viewport, say -- really is loading.
  var reusingVisibleBitmap = !!(
    reopeningVisiblePhoto &&
    img.getAttribute('src') === initialSrc &&
    img.complete &&
    img.naturalWidth > 0
  );
  if (!reusingVisibleBitmap) {
    vireoLightboxSession.setInitialLoad(handleInitialImageLoad, abandonInitialLoad);
    lbState.initialDecodePending = true;
  }
  _lbRenderDetailStatus();
  img.src = initialSrc;
  if (!lbState.visualTransitionPending) commitVisiblePhotoIdentity();
  inspectBtn.onclick = function() { closeLightbox(); openPipeline(photoId); };
  similarBtn.onclick = function() { closeLightbox(); findSimilar(photoId); };
  inatBtn.onclick = function() { submitToInat(photoId); };
  var editPhotoBtn = document.getElementById('lightboxEditPhoto');
  if (editPhotoBtn) {
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

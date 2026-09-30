
function _lbThumbnailUrl(photoId, version) {
  var url = '/thumbnails/' + photoId + '.jpg';
  if (version) url += '?editv=' + encodeURIComponent(version);
  return vireoCacheBustedPhotoUrl(photoId, url);
}

function _lbRefreshThumbnailCache(photoId, version) {
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

function _lbRefreshEditRecipeCache(updates) {
  if (!updates || typeof updates !== 'object') return;
  var version = String(Date.now());
  var refreshedPhotoIds = [];
  Object.keys(updates).forEach(function(photoId) {
    var numericId = Number(photoId);
    _lbEditVersionByPhoto[String(photoId)] = version;
    _lbMarkEditRecipeWrite(numericId);
    _lbRememberEditRecipe(numericId, updates[photoId]);
    _vireoBumpRenderVersion(numericId);
    _lbRefreshThumbnailCache(photoId, version);
    refreshedPhotoIds.push(numericId);
  });
  if (refreshedPhotoIds.length && typeof window.vireoRefreshPhotoRenders === 'function') {
    window.vireoRefreshPhotoRenders(refreshedPhotoIds);
  }
  if (vireoLightboxSession.requestedPhotoId() != null && refreshedPhotoIds.indexOf(Number(vireoLightboxSession.requestedPhotoId())) !== -1) {
    _lbReloadCurrentRenderAfterEdit(Number(vireoLightboxSession.requestedPhotoId()));
  }
}

window.vireoRefreshEditRecipeCache = _lbRefreshEditRecipeCache;

function _lbRenderKeywords(keywords) {
  var container = document.getElementById('lightboxKeywords');
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

function openLightbox(photoId, filename, photoList, options) {
  var alreadyOpen = !!window._lbEscToken;
  var transitionAlreadyPending = _lbVisualTransitionPending;
  var reopeningVisiblePhoto = !!(
    alreadyOpen &&
    !transitionAlreadyPending &&
    vireoLightboxSession.requestedPhotoId() != null &&
    String(vireoLightboxSession.requestedPhotoId()) === String(photoId)
  );
  options = options || {};
  var nextReadOnly = Object.prototype.hasOwnProperty.call(options, 'readOnly')
    ? !!options.readOnly
    : (alreadyOpen && _lbReadOnly);
  var nextReadOnlyMessage = Object.prototype.hasOwnProperty.call(options, 'readOnlyMessage')
    ? options.readOnlyMessage
    : _lbReadOnlyMessage;
  _lbProgressiveTargetKey = null;
  // Each photo earns its own quiet period. The outgoing photo's swap is dead as
  // soon as the session begins below, so neither its pending tier nor an
  // already-visible chip may carry over -- inheriting them would let a photo
  // that decodes in 100ms still announce Loading and then Full detail.
  _lbResetDetailStatus();
  _lbSetPreviewLoading(false);
  var fallbackViewportState = options.fallbackViewportState || null;
  var eyeTrackAnchor = options.eyeTrackAnchor || null;
  _lbFlushPendingAdjustmentSave();
  _lbReadOnly = nextReadOnly;
  _lbReadOnlyMessage = nextReadOnlyMessage || 'This lightbox is read-only';
  _lbApplyReadOnlyState();
  if (alreadyOpen && vireoLightboxSession.requestedPhotoId() != null) {
    _lbSaveViewportState(vireoLightboxSession.requestedPhotoId());
  }
  if (alreadyOpen) Keymap.popEsc(window._lbEscToken);
  window._lbEscToken = Keymap.pushEsc(function() { closeLightbox(); });
  if (!alreadyOpen) Keymap.lockBodyScroll();
  var openToken = vireoLightboxSession.begin(photoId);
  var preserveOneToOne = !!options.preserveOneToOne;
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
  _lbVisualTransitionPending = alreadyOpen && !reopeningVisiblePhoto;
  _lbSetPhotoTransitionPending(_lbVisualTransitionPending);
  // A previous open's deferred overlay closure captured the previous photo id
  // and metadata. If this open's image resolves before its own metadata does,
  // draining that stale closure would render the previous photo's detections
  // and eye marker over the incoming bitmap.
  _lbDeferredOverlayApply = null;

  _lbApplyReadOnlyState();
  if (photoList) _lightboxPhotoList = photoList;
  // Remember the photo being viewed so the Edit page (/edit with no id) can
  // resolve "the photo I was just looking at".
  if (window.vireoEditNav) window.vireoEditNav.setLastPhoto(photoId);
  _lbApplyEditButtonState();
  var currentFromList = _lightboxPhotoList.find(function(x) { return x.id === photoId; });
  if (currentFromList && typeof window.vireoRememberPhotoPair === 'function') {
    window.vireoRememberPhotoPair(currentFromList);
  }
  if (typeof window.vireoUpdatePairSourceControls === 'function') {
    window.vireoUpdatePairSourceControls(photoId);
  }
  if (currentFromList && Object.prototype.hasOwnProperty.call(currentFromList, 'flag')) {
    _lbRememberConfirmedFlag(photoId, currentFromList.flag);
  } else {
    // Flagless opens (e.g. review/misses/cull push only {id, filename}) must
    // not reuse a flag remembered from an earlier open; rely on the
    // /api/photos/:id fetch to repopulate the confirmed flag instead.
    _lbForgetConfirmedFlag(photoId);
  }
  _lbCurrentWildlifeExcluded = !!(currentFromList && currentFromList.wildlife_excluded);
  if (!alreadyOpen) _lbSetFlagStatus(_lbDisplayedFlagFor(photoId));

  var overlay = document.getElementById('lightboxOverlay');
  var img = document.getElementById('lightboxImg');
  var label = document.getElementById('lightboxFilename');
  var inspectBtn = document.getElementById('lightboxInspect');
  var similarBtn = document.getElementById('lightboxSimilar');
  var inatBtn = document.getElementById('lightboxInat');
  var wrap = document.getElementById('lightboxWrap');
  var detailData = null;

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
    var counter = document.getElementById('lightboxCounter');
    if (_lightboxPhotoList.length > 1) {
      var idx = _lightboxPhotoList.findIndex(function(p) { return p.id === photoId; });
      var counterFilename = filename || (currentFromList && currentFromList.filename) || '';
      counter.textContent = (idx + 1) + ' / ' + _lightboxPhotoList.length
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
  _lbZoom = transitionZoom;
  _lbPanX = 0;
  _lbPanY = 0;
  _lbNativeZoom = null;
  _lbPhotoW = null;
  _lbPhotoH = null;
  _lbPhotoOrientation = null;
  _lbCurrentEditRecipe = currentFromList && currentFromList.edit_recipe || null;
  if (currentFromList && Object.prototype.hasOwnProperty.call(currentFromList, 'edit_recipe')) {
    _lbRememberEditRecipe(photoId, currentFromList.edit_recipe);
    window.vireoRememberPhotoRenderKey(photoId, currentFromList.render_key);
  }
  var incomingPhoto = Object.assign({}, _lbPhotoDataByPhoto[String(photoId)] || {}, currentFromList || {});
  // Catalog geometry lets us select a tier before the next image has loaded.
  // Never consult the outgoing bitmap for the incoming photo's dimensions.
  _lbPhotoW = incomingPhoto.width || null;
  _lbPhotoH = incomingPhoto.height || null;
  _lbPhotoOrientation = _lbMetadataOrientation(incomingPhoto.metadata);
  var initialTarget = _lbInitialSourceKey(incomingPhoto, transitionZoom, restoreWantsOneToOne);
  var warmInitial = vireoLightboxSession.decodedPreview(photoId, initialTarget);
  _lbCurrentSrcKey = warmInitial ? warmInitial.sourceKey : initialTarget;
  if (_lbCurrentSrcKey !== initialTarget) _lbProgressiveTargetKey = initialTarget;
  _lbFullLongEdge = null;
  _lbPending1To1 = restoreWantsOneToOne;
  _lbUpdateZoomControl();
  // The deferred-1:1 anchor is a client-space click coordinate scoped to the
  // PREVIOUS photo. Clear it on every open so a 1:1-preserving navigation (or a
  // swap still in flight from the prior photo) snaps centered instead of reusing
  // a stale edge anchor on the new image — consistent with the pan reset above.
  _lbPending1To1Anchor = null;
  _lbPendingViewportState = restoreViewportState;
  _lbPendingEyeTrack = (_lbTrackEyeEnabled && eyeTrackAnchor) ? {
    photoId: photoId,
    offsetX: Number(eyeTrackAnchor.offsetX) || 0,
    offsetY: Number(eyeTrackAnchor.offsetY) || 0,
  } : null;
  _lbOriginalUnavailable = false;
  _lbFullUsesOriginal = null;
  _lbEditRecipe = null;
  _lbEditRecipeLoaded = false;
  _lbAdjustmentSource = null;
  if (_lbAdjustSaveTimer) {
    clearTimeout(_lbAdjustSaveTimer);
    _lbAdjustSaveTimer = null;
  }
  _lbAdjustSeq += 1;
  _lbClearAdjustmentPreview();
  _lbRenderAdjustmentControls();
  _lbSetAdjustmentControlsDisabled(true);
  _lbSetAdjustmentStatus('');
  vireoLightboxSession.cancelSwap();
  _lbDesiredSrcKey = null;
  if (!_lbVisualTransitionPending) {
    _lbApplyTransform();
  }

  // Fetch original dimensions (async, non-blocking for image display)
  var flagFetchSeq = _lbFlagEditSeq;
  var editRecipeFetchSeq = _lbEditRecipeWriteSeqFor(photoId);
  var editFetchSeq = _lbAdjustSeq;
  fetch('/api/photos/' + photoId)
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(data) {
      if (!data || !vireoLightboxSession.isCurrent(openToken)) return;
      var pairWasKnown = !!_vireoPairKnownByPhoto[String(photoId)];
      if (typeof window.vireoRememberPhotoPair === 'function') {
        window.vireoRememberPhotoPair(data);
      }
      if (typeof window.vireoUpdatePairSourceControls === 'function') {
        window.vireoUpdatePairSourceControls(photoId);
      }
      if (!pairWasKnown && _vireoPairKnownByPhoto[String(photoId)]) {
        var pairImg = document.getElementById('lightboxImg');
        vireoLightboxSession.watchImage(pairImg, function() {
          _vireoPairSourceImageLoaded(photoId, 'jpeg', pairImg);
        });
        _vireoBumpRenderVersion(photoId);
        window.vireoRefreshPhotoRenders([photoId]);
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
        if (typeof window.vireoRememberPhotoRenderKey === 'function') {
          window.vireoRememberPhotoRenderKey(photoId, data.render_key);
        }
        if (_vireoEditRecipeCacheKey(data.edit_recipe) !== recipeKeyBefore) {
          _vireoBumpRenderVersion(photoId);
          if (typeof window.vireoRefreshPhotoRenders === 'function') {
            window.vireoRefreshPhotoRenders([photoId]);
          }
          _lbReloadCurrentRenderAfterEdit(photoId);
        }
      }
      _lbApplyMaskVisibility();
      _lbPhotoW = data.width || null;
      _lbPhotoH = data.height || null;
      _lbFullUsesOriginal = !!data.full_uses_original;
      _lbSessionFullUsesOriginal = _lbFullUsesOriginal;
      if (data.full_preview_max_size != null &&
          Number.isFinite(Number(data.full_preview_max_size)) && Number(data.full_preview_max_size) >= 0) {
        _lbPreviewMaxSize = Number(data.full_preview_max_size);
      }
      _lbPhotoOrientation = _lbMetadataOrientation(data.metadata);
      var currentLayoutRecipe = _lbEditRecipeByPhoto[String(photoId)];
      _lbCurrentEditRecipe = _lbRecipeHasEdits(currentLayoutRecipe)
        ? currentLayoutRecipe
        : null;
      if (!_lbSourceOverlaysAvailable()) {
        var detContainer2 = document.getElementById('lightboxDetections');
        if (detContainer2) detContainer2.innerHTML = '';
        var eyeContainer2 = document.getElementById('lightboxEye');
        if (eyeContainer2) eyeContainer2.innerHTML = '';
        _lbResetMaskOverlay();
      }
      _lbCurrentWildlifeExcluded = !!data.wildlife_excluded;
      if (editFetchSeq === _lbAdjustSeq) {
        _lbEditRecipe = _lbCloneRecipe(data.edit_recipe || {});
        _lbEditRecipeLoaded = true;
        _lbRenderAdjustmentControls();
        _lbSetAdjustmentControlsDisabled(false);
        _lbSetAdjustmentStatus('');
      }
      _lbRecordFetchedFlag(photoId, data.flag, flagFetchSeq);
      // Metadata commonly wins the race with image decoding. Updating the
      // transform here would lay out the still-visible outgoing bitmap using
      // the incoming photo's dimensions, producing the navigation jerk. Let
      // the image-load handler commit image + viewport atomically instead.
      if (!_lbVisualTransitionPending) {
        _lbRecomputeNativeZoom();
        if (!_lbTryApplyPendingViewportState()) _lbApplyPendingOneToOneZoom();
        _lbTryApplyPendingEyeTrack(data);
        vireoLightboxSession.scheduleAdjacent(_lbCurrentSrcKey);
        if (_lbCurrentSrcKey === 'full') vireoLightboxSession.scheduleOriginal(photoId);
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
      if (_lbVisualTransitionPending) {
        _lbDeferredOverlayApply = applyOverlays;
      } else {
        _lbDeferredOverlayApply = null;
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
  // and _lbSaveViewportState treats the incoming photo as mid-flight. The
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
    _lbInitialDecodePending = false;
    _lbVisualTransitionPending = false;
    _lbSetPhotoTransitionPending(false);
    _lbRecomputeNativeZoom();
    // Apply any deferred viewport state so the layout reflects the incoming
    // photo instead of the frozen outgoing bitmap.
    if (!_lbTryApplyPendingViewportState()) _lbApplyPendingOneToOneZoom();
    _lbTryApplyPendingEyeTrack();
    _lbApplyTransform();
    _lbFlushDeferredOverlayApply();
    if (typeof window._lbFlushDeferredLightboxLayoutRefresh === 'function') {
      window._lbFlushDeferredLightboxLayoutRefresh();
    }
    _lbRenderDetailStatus();
  }

  function handleInitialImageLoad() {
    if (!vireoLightboxSession.isCurrent(openToken)) return;
    vireoLightboxSession.clearInitialLoad(handleInitialImageLoad);
    // The commit below momentarily clears the transition before deciding whether
    // a sharper tier is still needed. Paint once, at the end, so the chip does
    // not blink off and restart its show delay between those two states.
    _lbDetailStatusHold += 1;
    try {
      commitInitialImageLoad();
    } finally {
      _lbDetailStatusHold -= 1;
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
    if (vireoLightboxSession.requestedPhotoId() === photoId && _lbCurrentSrcKey === 'full' && img.naturalWidth) {
      _lbFullLongEdge = Math.max(img.naturalWidth, img.naturalHeight);
    }
    if (vireoLightboxSession.requestedPhotoId() === photoId && _lbCurrentSrcKey === 'original' && !_lbPhotoW && img.naturalWidth) {
      _lbPhotoW = img.naturalWidth;
      _lbPhotoH = img.naturalHeight;
    }
    _lbInitialDecodePending = false;
    _lbVisualTransitionPending = false;
    _lbSetPhotoTransitionPending(false);
    _lbRecomputeNativeZoom();
    if (_lbProgressiveTargetKey && _lbPendingViewportState) {
      // A ready preview may be soft, but it must show the same crop/position
      // immediately. Explicit zoom clicks still use the sharp-source deferral.
      _lbTryApplyPendingViewportState();
    } else if (
      _lbCurrentSrcKey === 'full' &&
      _lbPending1To1 &&
      (
        _lbOriginalUnavailable
          ? _lbDeferPendingOneToOneToPreviewFallback()
          : _lbDeferPendingOneToOneUntilSourceReady(_lbNativeZoom || 4)
      )
    ) {
      // The helper keeps 1:1 pending until the required source tier is current.
    } else if (!_lbTryApplyPendingViewportState()) {
      _lbApplyPendingOneToOneZoom();
    }
    _lbTryApplyPendingEyeTrack();
    _lbApplyTransform();
    _lbFlushDeferredOverlayApply();
    if (typeof window._lbFlushDeferredLightboxLayoutRefresh === 'function') {
      window._lbFlushDeferredLightboxLayoutRefresh();
    }
    if (_lbProgressiveTargetKey) {
      _lbSetPreviewLoading(true);
      _lbScheduleSourceSwap(_lbPending1To1 ? (_lbNativeZoom || transitionZoom) : _lbZoom, true);
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
      if (!_lbPreviewLoading && !_lbDetailSharpeningPending()) _lbMarkDetailSettled();
      vireoLightboxSession.scheduleAdjacent(_lbCurrentSrcKey);
      if (_lbCurrentSrcKey === 'full') vireoLightboxSession.scheduleOriginal(photoId);
    }
  }
  function handleInitialImageError() {
    if (!vireoLightboxSession.isCurrent(openToken)) return;
    if (_lbProgressiveTargetKey) {
      _lbCurrentSrcKey = _lbProgressiveTargetKey;
      _lbProgressiveTargetKey = null;
      _lbSetPreviewLoading(false);
      img.src = _lbSrcUrl(photoId, _lbCurrentSrcKey);
      return;
    }
    if (['1920', '2560', '3840'].indexOf(_lbCurrentSrcKey) !== -1) {
      // Sized tiers can now be the initial source. A failed render must not
      // leave navigation on a broken image: try the original, whose existing
      // error path falls back to /full if the original is unavailable too.
      _lbCurrentSrcKey = 'original';
      _lbDesiredSrcKey = null;
      img.src = _lbSrcUrl(photoId, 'original');
      return;
    }
    if (_lbCurrentSrcKey !== 'original') {
      img.onload = null;
      img.onerror = null;
      vireoLightboxSession.clearInitialLoad(handleInitialImageLoad);
      // No further fallback tier is available.
      abandonInitialLoad();
      return;
    }
    _lbOriginalUnavailable = true;
    _lbCurrentSrcKey = 'full';
    _lbDesiredSrcKey = null;
    vireoLightboxSession.watchInitialImage(img, handleInitialImageLoad, handleInitialImageError);
    img.src = _lbSrcUrl(photoId, 'full');
  }
  vireoLightboxSession.watchInitialImage(img, handleInitialImageLoad, handleInitialImageError);
  var initialSrc = _lbSrcUrl(photoId, _lbCurrentSrcKey);
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
    _lbInitialDecodePending = true;
  }
  _lbRenderDetailStatus();
  img.src = initialSrc;
  if (!_lbVisualTransitionPending) commitVisiblePhotoIdentity();
  inspectBtn.onclick = function() { closeLightbox(); openPipeline(photoId); };
  similarBtn.onclick = function() { closeLightbox(); findSimilar(photoId); };
  inatBtn.onclick = function() { submitToInat(photoId); };
  var editPhotoBtn = document.getElementById('lightboxEditPhoto');
  if (editPhotoBtn) {
    editPhotoBtn.onclick = function(e) {
      if (e) e.stopPropagation();
      if (_lbGuardReadOnly()) return false;
      var editHint = typeof window.getLightboxBrowseDisabledHint === 'function'
        ? window.getLightboxBrowseDisabledHint(photoId, true)
        : null;
      if (editHint) {
        if (typeof showToast === 'function') showToast(editHint, 'warning');
        return false;
      }
      // Hand the current ordered photo list to the editor so it can offer
      // Prev/Next just like the lightbox does.
      if (window.vireoEditNav) {
        window.vireoEditNav.setList(_lightboxPhotoList, photoId);
        window.vireoEditNav.setLastPhoto(photoId);
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

function lightboxNav(delta) {
  // A one-photo list can still be one page of a larger lazy-loaded dataset.
  // Let the normal boundary event fire so the owning page can fetch the next
  // or previous page instead of swallowing the first navigation attempt.
  if (_lightboxPhotoList.length === 0) return;
  var idx = _lightboxPhotoList.findIndex(function(p) { return p.id === vireoLightboxSession.requestedPhotoId(); });
  if (idx === -1) return;
  var newIdx = idx + delta;
  if (newIdx < 0 || newIdx >= _lightboxPhotoList.length) {
    try {
      document.dispatchEvent(new CustomEvent('lightbox:navigationboundary', {
        detail: {
          delta: delta,
          photoId: vireoLightboxSession.requestedPhotoId(),
          index: idx,
          photoCount: _lightboxPhotoList.length
        }
      }));
    } catch (_) {}
    return;
  }
  var currentViewportState = _lbSaveViewportState(vireoLightboxSession.requestedPhotoId());
  var eyeTrackAnchor = _lbCaptureEyeTrackingAnchor();
  var next = _lightboxPhotoList[newIdx];
  vireoLightboxSession.noteDirection(delta);
  openLightbox(next.id, next.filename, _lightboxPhotoList, {
    fallbackViewportState: currentViewportState,
    preserveOneToOne: _lbIsOneToOneZoom(),
    eyeTrackAnchor: eyeTrackAnchor
  });
}

function requestLightboxFullscreen() {
  var overlay = document.getElementById('lightboxOverlay');
  if (!overlay || !overlay.classList.contains('active')) return;
  if (document.fullscreenElement === overlay || document.webkitFullscreenElement === overlay) return;
  var request = overlay.requestFullscreen || overlay.webkitRequestFullscreen;
  if (!request) return;
  try {
    var result = request.call(overlay);
    if (result && typeof result.catch === 'function') result.catch(function() {});
  } catch(e) {}
}

function exitLightboxFullscreen() {
  if (!document.fullscreenElement && !document.webkitFullscreenElement) return;
  var exit = document.exitFullscreen || document.webkitExitFullscreen;
  if (!exit) return;
  try {
    var result = exit.call(document);
    if (result && typeof result.catch === 'function') result.catch(function() {});
  } catch(e) {}
}

function lightboxKeyMatchesConfiguredBrowseShortcut(e) {
  var browse = window._vireoShortcuts && window._vireoShortcuts.browse;
  if (!browse) return false;
  for (var action in browse) {
    if (Object.prototype.hasOwnProperty.call(browse, action) && matchesShortcut(e, browse[action])) {
      return true;
    }
  }
  return false;
}

/* The incoming photo's detail request (/api/photos/<id>): the RAW/JPEG pair,
 * edit recipe, geometry, flag and overlays it brings. The response can land
 * before or after the photo's first image, and after the user has already
 * edited or flagged it, so each step applies only what is still current.
 */
import { lbState } from './shared-state.js';
import {
  _lbEditRecipeByPhoto, _lbPhotoDataByPhoto, _lbRawEditRecipeByPhoto, vireoLightboxSession,
  vireoLightboxViewport,
} from './state.js';
import { _lbRenderLifeListPanel } from './curation.js';
import {
  _lbCloneEditRecipe, _lbCloneRecipe, _lbEditRecipeWriteSeqFor, _lbRecipeHasEdits,
  _lbReloadCurrentRenderAfterEdit, _lbRememberEditRecipe, _lbRenderAdjustmentControls,
  _lbSetAdjustmentControlsDisabled, _lbSetAdjustmentStatus, _vireoBumpRenderVersion,
  _vireoEditRecipeCacheKey, _vireoPairKnownByPhoto, _vireoPairSourceImageLoaded,
  vireoRefreshPhotoRenders, vireoRememberPhotoPair, vireoRememberPhotoRenderKey,
  vireoUpdatePairSourceControls,
} from './edits.js';
import { _lbRecordFetchedFlag } from './flags.js';
import {
  _lbApplyBoxesVisibility, _lbApplyEyeVisibility, _lbApplyMaskToggleButton,
  _lbApplyMaskVisibility, _lbApplyTrackEyeState, _lbLoadDetections, _lbMetadataOrientation,
  _lbRenderEyeCrosshair, _lbResetMaskOverlay, _lbSourceOverlaysAvailable,
} from './overlays.js';
import { _lbRenderKeywords } from './open-image.js';

// Learn whether the photo has a developed companion. A pair first learned here
// changes which source the displayed image came from, so its renders refresh.
// Returns false when the lightbox image is gone and nothing more can be applied.
function adoptDetailPair(/** @type {LightboxOpenContext} */ open, /** @type {LightboxPhoto} */ data) {
  var photoId = open.photoId;
  var pairWasKnown = !!_vireoPairKnownByPhoto[String(photoId)];
  if (typeof vireoRememberPhotoPair === 'function') {
    vireoRememberPhotoPair(data);
  }
  if (typeof vireoUpdatePairSourceControls === 'function') {
    vireoUpdatePairSourceControls(photoId);
  }
  if (!pairWasKnown && _vireoPairKnownByPhoto[String(photoId)]) {
    var pairImgCandidate = /** @type {HTMLImageElement | null} */ (document.getElementById('lightboxImg')); if (!pairImgCandidate) return false; const pairImg = pairImgCandidate;
    vireoLightboxSession.watchImage(pairImg, function() {
      _vireoPairSourceImageLoaded(photoId, 'jpeg', pairImg);
    });
    _vireoBumpRenderVersion(photoId);
    vireoRefreshPhotoRenders([photoId]);
  }
  return true;
}

// The fingerprint of the recipe held before this response is stored.
//
// Fingerprint the unclipped recipe on both sides of this comparison:
// ``data.edit_recipe`` arrives straight from the server, so reading the
// clipped ``_lbEditRecipeByPhoto`` view here would report a change on
// every refetch of any photo carrying a section the lightbox does not
// model (``local``), and bump the render version for nothing.
function heldRecipeKey(/** @type {number} */ photoId) {
  var recipeBefore = _lbRawEditRecipeByPhoto[String(photoId)];
  if (recipeBefore === undefined) recipeBefore = _lbEditRecipeByPhoto[String(photoId)];
  return _vireoEditRecipeCacheKey(recipeBefore);
}

function rememberDetailRecipe(/** @type {number} */ photoId, /** @type {LightboxPhoto} */ data, /** @type {string} */ recipeKeyBefore) {
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

function applyDetailGeometry(/** @type {number} */ photoId, /** @type {LightboxPhoto} */ data) {
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
    var detContainer = document.getElementById('lightboxDetections');
    if (detContainer) detContainer.innerHTML = '';
    var eyeContainer = document.getElementById('lightboxEye');
    if (eyeContainer) eyeContainer.innerHTML = '';
    _lbResetMaskOverlay();
  }
}

// Metadata commonly wins the race with image decoding. Updating the
// transform here would lay out the still-visible outgoing bitmap using
// the incoming photo's dimensions, producing the navigation jerk. Let
// the image-load handler commit image + viewport atomically instead.
function applyDetailViewport(/** @type {number} */ photoId, /** @type {LightboxPhoto} */ data) {
  if (lbState.visualTransitionPending) return;
  vireoLightboxViewport.recomputeNativeZoom();
  if (!vireoLightboxViewport.applyPendingRestore()) vireoLightboxViewport.applyPendingOneToOne();
  vireoLightboxViewport.applyPendingEye(data);
  vireoLightboxSession.scheduleAdjacent(lbState.currentSrcKey);
  if (lbState.currentSrcKey === 'full') vireoLightboxSession.scheduleOriginal(photoId);
}

// Detection boxes, the eye marker, and the mask overlay are children of
// lightboxTransform. While lbState.visualTransitionPending is true the
// transform is intentionally frozen on the outgoing bitmap, so painting
// the incoming photo's overlays now would layer them over the previous
// image until handleInitialImageLoad swaps in the new bitmap. Defer the
// render calls that actually inject DOM content until the transition
// clears; the visibility/toggle helpers are safe to run either way
// because they only touch empty containers or button labels.
function applyDetailOverlays(/** @type {number} */ photoId, /** @type {LightboxPhoto} */ data) {
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
}

function applyPhotoDetail(/** @type {LightboxOpenContext} */ open, /** @type {LightboxPhoto} */ data, /** @type {LightboxDetailSeqs} */ seqs) {
  var photoId = open.photoId;
  if (!adoptDetailPair(open, data)) return;
  var editRecipeFresh = _lbEditRecipeWriteSeqFor(photoId) === seqs.recipeWrite;
  var recipeKeyBefore = heldRecipeKey(photoId);
  if (!editRecipeFresh) {
    var currentRecipe = _lbEditRecipeByPhoto[String(photoId)];
    data.edit_recipe = _lbRecipeHasEdits(currentRecipe) ? _lbCloneEditRecipe(currentRecipe) : null;
  }
  _lbPhotoDataByPhoto[String(photoId)] = data;
  open.detail = data;
  if (open.identityCommitted) _lbRenderKeywords(data.keywords);
  _lbRenderLifeListPanel(photoId);
  if (editRecipeFresh) rememberDetailRecipe(photoId, data, recipeKeyBefore);
  _lbApplyMaskVisibility();
  applyDetailGeometry(photoId, data);
  lbState.currentWildlifeExcluded = !!data.wildlife_excluded;
  if (seqs.adjust === lbState.adjustSeq) {
    lbState.editRecipe = _lbCloneRecipe(data.edit_recipe || {});
    lbState.editRecipeLoaded = true;
    _lbRenderAdjustmentControls();
    _lbSetAdjustmentControlsDisabled(false);
    _lbSetAdjustmentStatus('');
  }
  _lbRecordFetchedFlag(photoId, data.flag, seqs.flag);
  applyDetailViewport(photoId, data);
  applyDetailOverlays(photoId, data);
}

// Fetch the photo's detail (async, non-blocking for image display). The
// sequence numbers captured here let the response tell whether the user
// flagged the photo, wrote its recipe or moved its sliders while it was in
// flight; what they changed is newer than what the response carries.
export function loadPhotoDetail(/** @type {LightboxOpenContext} */ open) {
  /** @type {LightboxDetailSeqs} */ var seqs = {
    flag: lbState.flagEditSeq,
    recipeWrite: _lbEditRecipeWriteSeqFor(open.photoId),
    adjust: lbState.adjustSeq,
  };
  fetch('/api/photos/' + open.photoId)
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(/** @type {LightboxPhoto} */ data) {
      if (!data || !vireoLightboxSession.isCurrent(open.token)) return;
      applyPhotoDetail(open, data, seqs);
    })
    .catch(function() {
      if (!vireoLightboxSession.isCurrent(open.token)) return;
      _lbSetAdjustmentStatus('Could not load recipe', true);
    });
}

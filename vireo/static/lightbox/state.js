/** @type {LightboxViewport} */ var vireoLightboxViewport = VireoLightboxViewport.create({
  photoId: function() { return vireoLightboxSession.requestedPhotoId(); },
  photo: function() {
    var id = vireoLightboxSession.requestedPhotoId();
    return {
      width: _lbPhotoW, height: _lbPhotoH, orientation: _lbPhotoOrientation,
      recipe: _lbCurrentEditRecipe, originalUnavailable: _lbOriginalUnavailable,
      currentSrcKey: _lbCurrentSrcKey, desiredSrcKey: _lbDesiredSrcKey,
      transitionPending: _lbVisualTransitionPending, trackEyeEnabled: _lbTrackEyeEnabled,
      pairKnown: !!_vireoPairKnownByPhoto[String(id)], pairSource: _vireoPairSource(id)
    };
  },
  orientationSwapsAxes: function(orientation) { return _lbOrientationSwapsAxes(orientation); },
  pickSource: function(zoom) { return _lbPickSourceKey(zoom); },
  scheduleSource: function(zoom) { _lbScheduleSourceSwap(zoom); },
  cancelSourceSwap: function() { vireoLightboxSession.cancelSwap(); },
  keepSource: function(key) { _lbDesiredSrcKey = key; },
  scheduleAdjacent: function(key) { vireoLightboxSession.scheduleAdjacent(key); },
  photoData: function(id) { return _lbPhotoData(id); },
  eyePoint: function(id, photo) { return _lbPhotoEyePoint(id, photo); },
  overlaysAvailable: function() { return _lbSourceOverlaysAvailable(); },
  updateEyeControl: function() { _lbApplyTrackEyeState(); },
  cancelProgressiveLoad: function() { _lbProgressiveTargetKey = null; _lbSetPreviewLoading(false); },
  handledClick: function() { window._lightboxZoomHandled = true; }
});

/** @type {LightboxSession} */ var vireoLightboxSession = VireoLightboxSession.create({
  photos: function() { return _lightboxPhotoList; },
  photoData: function(id) { return _lbPhotoDataByPhoto[String(id)]; },
  view: function() {
    return {
      currentSrcKey: _lbCurrentSrcKey, fullUsesOriginal: _lbFullUsesOriginal,
      originalUnavailable: _lbOriginalUnavailable, zoom: vireoLightboxViewport.zoom(),
      desiredSrcKey: _lbDesiredSrcKey, nativeZoom: vireoLightboxViewport.nativeZoom(),
      photoW: _lbPhotoW, photoH: _lbPhotoH,
      visualTransitionPending: _lbVisualTransitionPending
    };
  },
  sourceUrl: function(id, key, speculative) { return _lbSrcUrl(id, key, speculative); },
  sourceRank: function(key) { return _lbSrcRank(key); },
  fullPreviewLimit: function() { return _lbFullPreviewLimit(); },
  pickSourceKey: function(zoom) { return _lbPickSourceKey(zoom); },
  rememberEditRecipe: function(id, recipe) { _lbRememberEditRecipe(id, recipe); },
  rememberRenderKey: function(id, key) { window.vireoRememberPhotoRenderKey(id, key); }
});

/** @type {LightboxPhoto[]} */ var _lightboxPhotoList = [];  // list of {id, filename} for arrow navigation
var _lbReadOnly = false;
var _lbReadOnlyMessage = 'This lightbox is read-only';

/** @type {number | null} */ var _lbPhotoW = null;       // original photo width (px)
/** @type {number | null} */ var _lbPhotoH = null;       // original photo height (px)
/** @type {unknown} */ var _lbPhotoOrientation = null; // original EXIF orientation, when API metadata has it
/** @type {LightboxRecipe | null} */ var _lbCurrentEditRecipe = null; // active non-destructive recipe for layout math
/** @type {string | null} */ var _lbCurrentSrcKey = null; // 'full' | '2560' | '3840' | 'original'
/** @type {number | null} */ var _lbFullLongEdge = null;  // actual long edge of /full for current photo (may be < 1920 if preview_max_size is configured low)
// Bootstrap before opening any photo; metadata refreshes the workspace cap later.
var _lbPreviewMaxSize = window.VIREO_FULL_PREVIEW_MAX_SIZE ?? null; // 0 means original
var _lbOriginalUnavailable = false;  // true after /original fails; fall back to current decoded source dimensions
/** @type {boolean | null} */ var _lbFullUsesOriginal = null; // metadata-backed: preview_max_size=0 makes /full redirect to /original
var _lbCurrentWildlifeExcluded = false;
var _lbFlagEditSeq = 0;      // increments for local flag writes so stale metadata fetches cannot overwrite the chip
var _lbFlagPendingWrites = 0;
/** @type {Record<string, number>} */ var _lbFlagPendingByPhoto = {};  // count of in-flight flag writes PER photo; lightbox:flagchanged is emitted once a photo's count hits 0 so listeners see its settled flag, not a guessed per-write one
/** @type {Record<string, LightboxFlag>} */ var _lbConfirmedFlags = {};   // last server-confirmed flag per photo, isolated from optimistic page helpers
/** @type {Record<string, LightboxFlag>} */ var _lbProvisionalFlags = {}; // page-owned staged flags (for example Group Review before Apply)
/** @type {Record<string, number>} */ var _lbProvisionalFlagSeq = {}; // edit sequence that produced each staged flag
var _lbVisualTransitionPending = false; // keep the outgoing bitmap/transform frozen until the incoming image is decoded
/** @type {(() => void) | null} */ var _lbDeferredOverlayApply = null; // detections/eye render withheld while _lbVisualTransitionPending; drained when the transition clears
var _lbPreviewLoading = false;
/** @type {string | null} */ var _lbProgressiveTargetKey = null;
/** @type {boolean | null} */ var _lbSessionFullUsesOriginal = null;
/** @type {Record<string, LightboxRecipe>} */ var _lbEditRecipeByPhoto = {};
// The recipe exactly as the server sent it, kept alongside the clipped
// ``_lbEditRecipeByPhoto`` view above. ``_lbCloneEditRecipe`` only models the
// fields the lightbox itself can edit, so it drops sections such as ``local``;
// a cache fingerprint computed from the clipped copy cannot tell two different
// local-adjustment recipes apart. Fingerprints read this map instead.
/** @type {Record<string, LightboxRecipe | null>} */ var _lbRawEditRecipeByPhoto = {};
// Server-computed render keys (``photo_payload.render_key_for_recipe``),
// remembered per photo from whichever payload delivered the photo dict. The
// server derives its key from the whole canonical recipe, so preferring it
// over the client's own fingerprint keeps the two from drifting.
/** @type {Record<string, string>} */ var _lbRenderKeyByPhoto = {};
/** @type {Record<string, boolean>} */ var _lbEditRecipeKnownByPhoto = {};
var _lbEditRecipeWriteSeq = 0;
/** @type {Record<string, number>} */ var _lbEditRecipeWriteSeqByPhoto = {};
/** @type {Record<string, LightboxPhoto>} */ var _lbPhotoDataByPhoto = {};
/** @type {Record<string, string>} */ var _lbRenderVersionByPhoto = {};

/**
 * Why the open page keeps Edit / Open in Browse disabled for a photo, from
 * its getLightboxBrowseDisabledHint page hook (Pipeline Review), or null.
 * @param {number | null} photoId
 */
function _lbPageBrowseDisabledHint(photoId) {
  var hint = Vireo.pageHook('getLightboxBrowseDisabledHint');
  return hint ? hint(photoId, true) : null;
}

/** The photo in the open page's detail panel (Browse), whose image edits refresh too. */
function _lbPageDetailPhotoId() {
  var detailPhotoId = Vireo.pageHook('getDetailPhotoId');
  return detailPhotoId ? detailPhotoId() : null;
}

function _lbGuardReadOnly() {
  if (!_lbReadOnly) return false;
  if (typeof showToast === 'function') showToast(_lbReadOnlyMessage, 'warning');
  return true;
}

function _lbApplyReadOnlyState() {
  /** @type {[string, string][]} */ var controls = [
    ['lightboxFlagBtn', 'Flag photo (p)'],
    ['lightboxRejectBtn', 'Reject photo (x)'],
    ['lightboxInat', 'Submit to iNaturalist'],
    ['lightboxAdjustBtn', 'Quick non-destructive adjustments'],
    ['lightboxDeleteBtn', 'Delete photo'],
  ];
  controls.forEach(function(entry) {
    var button = /** @type {HTMLButtonElement | null} */ (document.getElementById(entry[0]));
    if (!button) return;
    button.disabled = _lbReadOnly;
    button.title = _lbReadOnly ? _lbReadOnlyMessage : entry[1];
  });
  var adjustmentHint = _lbAdjustmentSourceHint();
  var adjustmentButton = /** @type {HTMLButtonElement | null} */ (document.getElementById('lightboxAdjustBtn'));
  if (adjustmentButton && adjustmentHint && !_lbReadOnly) {
    adjustmentButton.disabled = true;
    adjustmentButton.title = adjustmentHint;
  }
  var panel = document.getElementById('lightboxAdjustPanel');
  if ((_lbReadOnly || adjustmentHint) && panel) {
    panel.classList.remove('open');
    if (adjustmentButton) adjustmentButton.setAttribute('aria-expanded', 'false');
  }
  var editButton = /** @type {HTMLButtonElement | null} */ (document.getElementById('lightboxEditPhoto'));
  if (editButton) {
    var editHint = _lbReadOnly
      ? _lbReadOnlyMessage
      : _lbPageBrowseDisabledHint(vireoLightboxSession.requestedPhotoId());
    editButton.disabled = !!editHint;
    editButton.title = editHint || 'Edit photo';
  }
}

window.setLightboxReadOnlyMode = function(readOnly, message) {
  _lbReadOnly = !!readOnly;
  _lbReadOnlyMessage = message || 'This lightbox is read-only';
  _lbApplyReadOnlyState();
};

/** @param {boolean} pending */ function _lbSetPhotoTransitionPending(pending) {
  ['lightboxActions', 'lightboxAdjustPanel', 'syncLightboxPanel'].forEach(function(id) {
    var controls = document.getElementById(id);
    if (!controls) return;
    controls.classList.toggle('lb-photo-transition-pending', !!pending);
    controls.inert = !!pending;
    controls.setAttribute('aria-busy', pending ? 'true' : 'false');
  });
  // Every caller assigns _lbVisualTransitionPending immediately before this, so
  // the phase derived here is already current.
  _lbRenderDetailStatus();
}

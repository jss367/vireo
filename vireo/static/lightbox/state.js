var _lightboxPhotoList = [];  // list of {id, filename} for arrow navigation
var _lightboxCurrentId = null;
var _lbReadOnly = false;
var _lbReadOnlyMessage = 'This lightbox is read-only';
// `_lightboxCurrentId` advances immediately when navigation targets a new
// photo; the visible bitmap is deliberately held on the previous frame until
// the replacement finishes decoding. `_lightboxCommittedId` tracks that
// visible identity — the photo the user is actually looking at — and is what
// `lightbox:closed` reports so a close mid-navigation reconciles Browse to
// the photo that was on screen, not the one that was still loading.
var _lightboxCommittedId = null;

var _lbZoom = 1.0;          // current zoom (1.0 = fit)
var _lbPanX = 0;            // pan translation in CSS pixels
var _lbPanY = 0;
var _lbNativeZoom = null;   // zoom value corresponding to 1:1 for current photo
var _lbFitScale = 1.0;      // natural image scale at zoom=1.0
var _lbPhotoW = null;       // original photo width (px)
var _lbPhotoH = null;       // original photo height (px)
var _lbPhotoOrientation = null; // original EXIF orientation, when API metadata has it
var _lbCurrentEditRecipe = null; // active non-destructive recipe for layout math
var _lbCurrentSrcKey = null; // 'full' | '2560' | '3840' | 'original'
var _lbFullLongEdge = null;  // actual long edge of /full for current photo (may be < 1920 if preview_max_size is configured low)
// Bootstrap before opening any photo; metadata refreshes the workspace cap later.
var _lbPreviewMaxSize = window.VIREO_FULL_PREVIEW_MAX_SIZE ?? null; // 0 means original
var _lbPending1To1 = false;  // true when z/click was pressed with unknown nativeZoom; upgrade to true 1:1 once learned
var _lbPending1To1Anchor = null; // optional client-space anchor for a deferred 1:1 snap
var _lbOriginalUnavailable = false;  // true after /original fails; fall back to current decoded source dimensions
var _lbFullUsesOriginal = null; // metadata-backed: preview_max_size=0 makes /full redirect to /original
var _lbCurrentWildlifeExcluded = false;
var _lbFlagEditSeq = 0;      // increments for local flag writes so stale metadata fetches cannot overwrite the chip
var _lbOpenSeq = 0;          // increments for every lightbox open so old metadata fetches cannot reapply after reopen
var _lbFlagPendingWrites = 0;
var _lbFlagPendingByPhoto = {};  // count of in-flight flag writes PER photo; lightbox:flagchanged is emitted once a photo's count hits 0 so listeners see its settled flag, not a guessed per-write one
var _lbConfirmedFlags = {};   // last server-confirmed flag per photo, isolated from optimistic page helpers
var _lbProvisionalFlags = {}; // page-owned staged flags (for example Group Review before Apply)
var _lbProvisionalFlagSeq = {}; // edit sequence that produced each staged flag
var _lbViewportByPhotoId = {};  // per-session lightbox viewport cache keyed by photo id
var _lbPendingViewportState = null;
var _lbPendingEyeTrack = null; // destination alignment waiting for image metadata/layout
var _lbEyeTrackScreenAnchor = null; // eye offset from viewport center in CSS pixels
var _lbVisualTransitionPending = false; // keep the outgoing bitmap/transform frozen until the incoming image is decoded
var _lbDeferredOverlayApply = null; // detections/eye render withheld while _lbVisualTransitionPending; drained when the transition clears
var _lbAdjacentPreloads = {}; // bounded navigation window, keyed by photo id + source URL
var _lbAdjacentPreloadTimer = null;
var _lbAdjacentPreloadRetry = {};
var _lbLastNavDelta = 1;
var _lbOriginalPreloadTimer = null; // short dwell before warming the current photo's 100% source
var _lbOriginalPreload = null; // retained decoded original for an instant first 100% click
var _lbOriginalPreloadWaiting = null; // dwell completed; waiting for the shared slot
var _lbSpeculativeInFlight = null; // oldest outstanding request (including pruned entries)
var _lbSpeculativeLoads = new Set();
var _lbPreloadConcurrency = 3;
var _lbPreloadBudgetBytes = 128 * 1024 * 1024;
var _lbPreviewLoading = false;
var _lbProgressiveTargetKey = null;
var _lbSessionFullUsesOriginal = null;
var _lbEditRecipeByPhoto = {};
// The recipe exactly as the server sent it, kept alongside the clipped
// ``_lbEditRecipeByPhoto`` view above. ``_lbCloneEditRecipe`` only models the
// fields the lightbox itself can edit, so it drops sections such as ``local``;
// a cache fingerprint computed from the clipped copy cannot tell two different
// local-adjustment recipes apart. Fingerprints read this map instead.
var _lbRawEditRecipeByPhoto = {};
// Server-computed render keys (``photo_payload.render_key_for_recipe``),
// remembered per photo from whichever payload delivered the photo dict. The
// server derives its key from the whole canonical recipe, so preferring it
// over the client's own fingerprint keeps the two from drifting.
var _lbRenderKeyByPhoto = {};
var _lbEditRecipeKnownByPhoto = {};
var _lbEditRecipeWriteSeq = 0;
var _lbEditRecipeWriteSeqByPhoto = {};
var _lbPhotoDataByPhoto = {};
var _lbRenderVersionByPhoto = {};

function _lbGuardReadOnly() {
  if (!_lbReadOnly) return false;
  if (typeof showToast === 'function') showToast(_lbReadOnlyMessage, 'warning');
  return true;
}

function _lbApplyReadOnlyState() {
  var controls = [
    ['lightboxFlagBtn', 'Flag photo (p)'],
    ['lightboxRejectBtn', 'Reject photo (x)'],
    ['lightboxInat', 'Submit to iNaturalist'],
    ['lightboxAdjustBtn', 'Quick non-destructive adjustments'],
    ['lightboxDeleteBtn', 'Delete photo'],
  ];
  controls.forEach(function(entry) {
    var button = document.getElementById(entry[0]);
    if (!button) return;
    button.disabled = _lbReadOnly;
    button.title = _lbReadOnly ? _lbReadOnlyMessage : entry[1];
  });
  var adjustmentHint = _lbAdjustmentSourceHint();
  var adjustmentButton = document.getElementById('lightboxAdjustBtn');
  if (adjustmentButton && adjustmentHint && !_lbReadOnly) {
    adjustmentButton.disabled = true;
    adjustmentButton.title = adjustmentHint;
  }
  var panel = document.getElementById('lightboxAdjustPanel');
  if ((_lbReadOnly || adjustmentHint) && panel) {
    panel.classList.remove('open');
    if (adjustmentButton) adjustmentButton.setAttribute('aria-expanded', 'false');
  }
  var editButton = document.getElementById('lightboxEditPhoto');
  if (editButton) {
    var editHint = _lbReadOnly
      ? _lbReadOnlyMessage
      : (typeof window.getLightboxBrowseDisabledHint === 'function'
        ? window.getLightboxBrowseDisabledHint(_lightboxCurrentId, true)
        : null);
    editButton.disabled = !!editHint;
    editButton.title = editHint || 'Edit photo';
  }
}

window.setLightboxReadOnlyMode = function(readOnly, message) {
  _lbReadOnly = !!readOnly;
  _lbReadOnlyMessage = message || 'This lightbox is read-only';
  _lbApplyReadOnlyState();
};

function _lbSetPhotoTransitionPending(pending) {
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

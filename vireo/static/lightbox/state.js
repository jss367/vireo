import { lbState } from './shared-state.js';
import { VireoLightboxSession } from './session.js';
import { VireoLightboxViewport, _lbSrcRank } from './viewport.js';
import {
  _lbAdjustmentSourceHint, _lbRememberEditRecipe, _vireoPairKnownByPhoto, _vireoPairSource,
  vireoRememberPhotoRenderKey,
} from './edits.js';
import {
  _lbApplyTrackEyeState, _lbOrientationSwapsAxes, _lbPhotoData, _lbPhotoEyePoint,
  _lbSourceOverlaysAvailable, lbOverlays,
} from './overlays.js';
import {
  _lbFullPreviewLimit, _lbPickSourceKey, _lbRenderDetailStatus, _lbScheduleSourceSwap,
  _lbSetPreviewLoading, _lbSrcUrl,
} from './source-loading.js';
import { showToast } from '../navbar-shared-helpers.js';

// The viewport calls these through the object on every use, so a browser test
// can replace one (tests/e2e/navbar_modules.py), as it once replaced the global
// function behind it.
/** @type {LightboxViewportOptions} */ export const lbViewportHost = {
  photoId: function() { return vireoLightboxSession.requestedPhotoId(); },
  photo: function() {
    var id = vireoLightboxSession.requestedPhotoId();
    return {
      width: lbState.photoW, height: lbState.photoH, orientation: lbState.photoOrientation,
      recipe: lbState.currentEditRecipe, originalUnavailable: lbState.originalUnavailable,
      currentSrcKey: lbState.currentSrcKey, desiredSrcKey: lbState.desiredSrcKey,
      transitionPending: lbState.visualTransitionPending, trackEyeEnabled: lbOverlays.trackEyeEnabled,
      pairKnown: !!_vireoPairKnownByPhoto[String(id)], pairSource: _vireoPairSource(id)
    };
  },
  orientationSwapsAxes: function(orientation) { return _lbOrientationSwapsAxes(orientation); },
  pickSource: function(zoom) { return _lbPickSourceKey(zoom); },
  scheduleSource: function(zoom) { _lbScheduleSourceSwap(zoom); },
  cancelSourceSwap: function() { vireoLightboxSession.cancelSwap(); },
  keepSource: function(key) { lbState.desiredSrcKey = key; },
  scheduleAdjacent: function(key) { vireoLightboxSession.scheduleAdjacent(key); },
  photoData: function(id) { return _lbPhotoData(id); },
  eyePoint: function(id, photo) { return _lbPhotoEyePoint(id, photo); },
  overlaysAvailable: function() { return _lbSourceOverlaysAvailable(); },
  updateEyeControl: function() { _lbApplyTrackEyeState(); },
  cancelProgressiveLoad: function() { lbState.progressiveTargetKey = null; _lbSetPreviewLoading(false); },
  handledClick: function() { lbState.zoomHandled = true; }
};
/** @type {LightboxViewport} */ export var vireoLightboxViewport = VireoLightboxViewport.create(lbViewportHost);

// The session's view of the lightbox, likewise one object a browser test can
// build a replacement session from (tests/e2e/test_browse_lightbox.py).
/** @type {LightboxSessionOptions} */ export const lbSessionHost = {
  photos: function() { return lbState.photoList; },
  photoData: function(id) { return _lbPhotoDataByPhoto[String(id)]; },
  view: function() {
    return {
      currentSrcKey: lbState.currentSrcKey, fullUsesOriginal: lbState.fullUsesOriginal,
      originalUnavailable: lbState.originalUnavailable, zoom: vireoLightboxViewport.zoom(),
      desiredSrcKey: lbState.desiredSrcKey, nativeZoom: vireoLightboxViewport.nativeZoom(),
      photoW: lbState.photoW, photoH: lbState.photoH,
      visualTransitionPending: lbState.visualTransitionPending
    };
  },
  sourceUrl: function(id, key, speculative) { return _lbSrcUrl(id, key, speculative); },
  sourceRank: function(key) { return _lbSrcRank(key); },
  fullPreviewLimit: function() { return _lbFullPreviewLimit(); },
  pickSourceKey: function(zoom) { return _lbPickSourceKey(zoom); },
  rememberEditRecipe: function(id, recipe) { _lbRememberEditRecipe(id, recipe); },
  rememberRenderKey: function(id, key) { vireoRememberPhotoRenderKey(id, key); }
};
/** @type {LightboxSession} */ export var vireoLightboxSession = VireoLightboxSession.create(lbSessionHost);

/**
 * Replace the session or viewport controller. Browser tests use this (through
 * tests/e2e/navbar_modules.py) to stub a controller method, such as keeping
 * the session from preloading originals, the way they once reassigned the
 * global. Every lightbox module reads the controllers through these live
 * bindings, so a replacement reaches all of them.
 * @param {{session?: LightboxSession, viewport?: LightboxViewport}} controllers
 */
export function replaceLightboxControllers(controllers) {
  if (controllers.session) vireoLightboxSession = controllers.session;
  if (controllers.viewport) vireoLightboxViewport = controllers.viewport;
}


/** @type {Record<string, number>} */ export var _lbFlagPendingByPhoto = {};  // count of in-flight flag writes PER photo; lightbox:flagchanged is emitted once a photo's count hits 0 so listeners see its settled flag, not a guessed per-write one
/** @type {Record<string, LightboxFlag>} */ export var _lbConfirmedFlags = {};   // last server-confirmed flag per photo, isolated from optimistic page helpers
/** @type {Record<string, LightboxFlag>} */ export var _lbProvisionalFlags = {}; // page-owned staged flags (for example Group Review before Apply)
/** @type {Record<string, number>} */ export var _lbProvisionalFlagSeq = {}; // edit sequence that produced each staged flag
/** @type {Record<string, LightboxRecipe>} */ export var _lbEditRecipeByPhoto = {};
// The recipe exactly as the server sent it, kept alongside the clipped
// ``_lbEditRecipeByPhoto`` view above. ``_lbCloneEditRecipe`` only models the
// fields the lightbox itself can edit, so it drops sections such as ``local``;
// a cache fingerprint computed from the clipped copy cannot tell two different
// local-adjustment recipes apart. Fingerprints read this map instead.
/** @type {Record<string, LightboxRecipe | null>} */ export var _lbRawEditRecipeByPhoto = {};
// Server-computed render keys (``photo_payload.render_key_for_recipe``),
// remembered per photo from whichever payload delivered the photo dict. The
// server derives its key from the whole canonical recipe, so preferring it
// over the client's own fingerprint keeps the two from drifting.
/** @type {Record<string, string>} */ export var _lbRenderKeyByPhoto = {};
/** @type {Record<string, boolean>} */ export var _lbEditRecipeKnownByPhoto = {};
/** @type {Record<string, number>} */ export var _lbEditRecipeWriteSeqByPhoto = {};
/** @type {Record<string, LightboxPhoto>} */ export var _lbPhotoDataByPhoto = {};
/** @type {Record<string, string>} */ export var _lbRenderVersionByPhoto = {};

/**
 * Why the open page keeps Edit / Open in Browse disabled for a photo, from
 * its getLightboxBrowseDisabledHint page hook (Pipeline Review), or null.
 * @param {number | null} photoId
 */
export function _lbPageBrowseDisabledHint(photoId) {
  var hint = Vireo.pageHook('getLightboxBrowseDisabledHint');
  return hint ? hint(photoId, true) : null;
}

/** The photo in the open page's detail panel (Browse), whose image edits refresh too. */
export function _lbPageDetailPhotoId() {
  var detailPhotoId = Vireo.pageHook('getDetailPhotoId');
  return detailPhotoId ? detailPhotoId() : null;
}

export function _lbGuardReadOnly() {
  if (!lbState.readOnly) return false;
  if (typeof showToast === 'function') showToast(lbState.readOnlyMessage, 'warning');
  return true;
}

export function _lbApplyReadOnlyState() {
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
    button.disabled = lbState.readOnly;
    button.title = lbState.readOnly ? lbState.readOnlyMessage : entry[1];
  });
  var adjustmentHint = _lbAdjustmentSourceHint();
  var adjustmentButton = /** @type {HTMLButtonElement | null} */ (document.getElementById('lightboxAdjustBtn'));
  if (adjustmentButton && adjustmentHint && !lbState.readOnly) {
    adjustmentButton.disabled = true;
    adjustmentButton.title = adjustmentHint;
  }
  var panel = document.getElementById('lightboxAdjustPanel');
  if ((lbState.readOnly || adjustmentHint) && panel) {
    panel.classList.remove('open');
    if (adjustmentButton) adjustmentButton.setAttribute('aria-expanded', 'false');
  }
  var editButton = /** @type {HTMLButtonElement | null} */ (document.getElementById('lightboxEditPhoto'));
  if (editButton) {
    var editHint = lbState.readOnly
      ? lbState.readOnlyMessage
      : _lbPageBrowseDisabledHint(vireoLightboxSession.requestedPhotoId());
    editButton.disabled = !!editHint;
    editButton.title = editHint || 'Edit photo';
  }
}

/** @type {LightboxApi['setLightboxReadOnlyMode']} */ export function setLightboxReadOnlyMode(readOnly, message) {
  lbState.readOnly = !!readOnly;
  lbState.readOnlyMessage = message || 'This lightbox is read-only';
  _lbApplyReadOnlyState();
}

/** @param {boolean} pending */ export function _lbSetPhotoTransitionPending(pending) {
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

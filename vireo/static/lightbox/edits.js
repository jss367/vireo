import { lbState } from './shared-state.js';
import {
  _lbApplyReadOnlyState, _lbEditRecipeByPhoto, _lbEditRecipeKnownByPhoto,
  _lbEditRecipeWriteSeqByPhoto, _lbGuardReadOnly, _lbPageDetailPhotoId, _lbPhotoDataByPhoto,
  _lbRawEditRecipeByPhoto, _lbRenderKeyByPhoto, _lbRenderVersionByPhoto, vireoLightboxSession,
  vireoLightboxViewport,
} from './state.js';
import {
  _lbApplyBoxesVisibility, _lbApplyEyeVisibility, _lbApplyMaskVisibility, _lbLoadDetections,
  _lbLoadMaskVariants, _lbRecipeHasGeometricEdit, _lbRenderEyeCrosshair,
} from './overlays.js';
import { _lbRenderDetailStatus, _lbSetPreviewLoading, _lbSrcUrl } from './source-loading.js';
import { showToast } from '../navbar-shared-helpers.js';

var _lbAdjustmentPreviewSeq = 0;
/** @type {number | null} */ var _lbAdjustmentPreviewTimer = null;
/** @type {Record<string, number>} */ var _lbAdjustmentInputSeqByPhoto = {};
/** @type {Record<string, boolean>} */ var _lbAdjustSaveInFlightByPhoto = {};
/** @type {Record<string, {recipe: LightboxRecipe, inputSeq: number}>} */ var _lbQueuedAdjustmentSaveByPhoto = {};
/** @type {Record<string, number>} */ var _lbRecipeBustByPhotoId = {};
/** @type {Record<string, string>} */ export var _lbEditVersionByPhoto = {};
export var _lbEditWritePending = false;

export function _vireoUrlWithQueryParam(/** @type {string} */ url, /** @type {string} */ key, /** @type {string | number | boolean | null} */ value) {
  if (!url || !key || value == null || value === '') return url;
  var hash = '';
  var hashIdx = url.indexOf('#');
  if (hashIdx !== -1) {
    hash = url.slice(hashIdx);
    url = url.slice(0, hashIdx);
  }
  var joiner = url.indexOf('?') === -1 ? '?' : '&';
  return url + joiner + encodeURIComponent(key) + '=' + encodeURIComponent(value) + hash;
}

export function _vireoUrlWithRenderVersion(/** @type {string} */ url, /** @type {LightboxPhotoId | null} */ photoId) {
  if (!url || photoId == null) return url;
  var version = _lbRenderVersionByPhoto[String(photoId)];
  if (!version) return url;
  return _vireoUrlWithQueryParam(url, 'rv', version);
}

function _vireoUrlWithPhotoSource(/** @type {string} */ url, /** @type {string | null} */ source) {
  // Source is a mutable choice, unlike cache-bust params. Remove any prior
  // value before appending so toggling JPEG -> RAW cannot leave Flask reading
  // the first of two conflicting ``source`` query parameters.
  url = String(url || '')
    .replace(/([?&])source=[^&]*(&?)/, function(match, prefix, suffix) {
      return suffix ? prefix : '';
    })
    .replace(/[?&]$/, '');
  return source ? _vireoUrlWithQueryParam(url, 'source', source) : url;
}

function _vireoCleanRenderSearch(/** @type {string} */ search) {
  if (!search) return '';
  try {
    var params = new URLSearchParams(search);
    params.delete('rv');
    params.delete('er');
    params.delete('editv');
    params.delete('v');
    var qs = params.toString();
    return qs ? '?' + qs : '';
  } catch (_) {
    return String(search)
      .replace(/([?&])(rv|er|editv|v)=[^&]*&?/g, '$1')
      .replace(/\?&/, '?')
      .replace(/[?&]$/, '');
  }
}

export function _vireoBaseRenderedUrl(/** @type {string} */ src) {
  if (!src) return '';
  try {
    var parsed = new URL(src, window.location.origin);
    return parsed.pathname + _vireoCleanRenderSearch(parsed.search);
  } catch (_) {
    return String(src)
      .replace(/([?&])(rv|er|editv|v)=[^&]*&?/g, '$1')
      .replace(/\?&/, '?')
      .replace(/[?&]$/, '');
  }
}

export function _vireoBumpRenderVersion(/** @type {LightboxPhotoId | null} */ photoId) {
  if (photoId == null) return null;
  var version = Date.now() + '-' + Math.floor(Math.random() * 1000000);
  _lbRenderVersionByPhoto[String(photoId)] = version;
  return version;
}

// A RAW+JPEG pair stays one logical photo, but the user can choose which
// physical file supplies the displayed pixels. A newly discovered pair starts
// on JPEG because that file is commonly the photographer's developed result.
/** @type {Record<string, boolean>} */ export var _vireoPairKnownByPhoto = {};
/** @type {Record<string, string>} */ var _vireoPairSourceByPhoto = {};
/** @type {Record<string, string>} */ export var _vireoPairPendingSourceByPhoto = {};

/** @type {LightboxApi['vireoPhotoIsRawJpegPair']} */ export function vireoPhotoIsRawJpegPair(photo) {
  if (!photo || !photo.companion_path) return false;
  /** @type {Record<string, boolean>} */ var rawExtensions = {
    '.nef': true,
    '.cr2': true,
    '.cr3': true,
    '.arw': true,
    '.raf': true,
    '.dng': true,
    '.rw2': true,
    '.orf': true
  };
  var primaryName = photo.filename || '';
  var primaryMatch = String(primaryName).toLowerCase().match(/(\.[^./\\]+)$/);
  var primaryExt = primaryMatch ? primaryMatch[1] : '';
  if (!primaryExt) {
    primaryExt = String(photo.extension || '').toLowerCase();
    if (primaryExt && primaryExt.charAt(0) !== '.') primaryExt = '.' + primaryExt;
  }
  var companionMatch = String(photo.companion_path).toLowerCase().match(/(\.[^./\\]+)$/);
  var companionExt = companionMatch ? companionMatch[1] || '' : '';
  return !!rawExtensions[primaryExt] && (companionExt === '.jpg' || companionExt === '.jpeg');
}

/** @type {LightboxApi['vireoRememberPhotoPair']} */ export function vireoRememberPhotoPair(photo) {
  if (!photo || photo.id == null) return;
  var key = String(photo.id);
  if (vireoPhotoIsRawJpegPair(photo)) {
    _vireoPairKnownByPhoto[key] = true;
    if (!_vireoPairSourceByPhoto[key]) _vireoPairSourceByPhoto[key] = 'jpeg';
  } else if (Object.prototype.hasOwnProperty.call(photo, 'companion_path')) {
    delete _vireoPairKnownByPhoto[key];
    delete _vireoPairSourceByPhoto[key];
  }
}

export function _vireoPairSource(/** @type {LightboxPhotoId | null} */ photoId) {
  return _vireoPairSourceByPhoto[String(photoId)] || null;
}

/** @type {LightboxApi['vireoUpdatePairSourceControls']} */ export function vireoUpdatePairSourceControls(photoId) {
  if (photoId == null) return;
  var key = String(photoId);
  var paired = !!_vireoPairKnownByPhoto[key];
  var source = _vireoPairSourceByPhoto[key] || 'jpeg';
  var pending = _vireoPairPendingSourceByPhoto[key] || null;
  var text = pending
    ? 'Viewing ' + source.toUpperCase() + ' · Loading ' + pending.toUpperCase() + '…'
    : (source === 'jpeg'
      ? 'Viewing JPEG · Show RAW'
      : 'Viewing RAW · Show JPEG');

  var lightboxControl = /** @type {HTMLButtonElement | null} */ (document.getElementById('lightboxSourceControl'));
  if (lightboxControl && String(vireoLightboxSession.requestedPhotoId()) === key) {
    lightboxControl.style.display = paired ? '' : 'none';
    lightboxControl.textContent = text;
    lightboxControl.disabled = !!pending;
    _lbApplyReadOnlyState();
    _lbSetAdjustmentControlsDisabled(!lbState.editRecipeLoaded);
    if (_lbAdjustmentSourceHint()) _lbClearAdjustmentPreview();
  }
  var detailControl = /** @type {HTMLButtonElement | null} */ (document.getElementById('detailSourceControl'));
  if (detailControl && String(_lbPageDetailPhotoId()) === key) {
    detailControl.style.display = paired ? 'block' : 'none';
    detailControl.textContent = paired
      ? 'RAW + JPEG pair · ' + text
      : '';
    detailControl.disabled = !!pending;
  }
  document.querySelectorAll('[data-pair-source-id="' + key + '"]').forEach(function(badge) {
    badge.textContent = source === 'jpeg'
      ? 'JPEG · RAW pair'
      : 'RAW · JPEG pair';
  });
}

// Queued grid images display blob URLs; retain the canonical URL for edits
// and RAW/JPEG switches, and send replacements through the same bounded queue.
export function _vireoRenderedImageSource(/** @type {HTMLImageElement} */ img) {
  return img.getAttribute('data-thumbnail-src') || img.getAttribute('src') || '';
}

export function _vireoSetRenderedImageSource(/** @type {HTMLImageElement} */ img, /** @type {string} */ url) {
  if (img.hasAttribute('data-thumbnail-src')) img.setAttribute('data-thumbnail-src', url);
  else img.src = url;
}

function _vireoPairAnchorImage(/** @type {LightboxPhotoId | null} */ photoId) {
  var key = String(photoId);
  if (String(vireoLightboxSession.requestedPhotoId()) === key) {
    return /** @type {HTMLImageElement | null} */ (document.getElementById('lightboxImg'));
  }
  if (String(_lbPageDetailPhotoId()) === key) {
    return /** @type {HTMLImageElement | null} */ (document.getElementById('detailImg'));
  }
  return /** @type {HTMLImageElement | null} */ (document.querySelector('.grid-card[data-id="' + key + '"] img'));
}

function _vireoPairAnchorStillCurrent(/** @type {HTMLImageElement | null} */ anchor, /** @type {LightboxPhotoId | null} */ photoId) {
  if (!anchor) return false;
  var key = String(photoId);
  if (anchor.id === 'lightboxImg') return String(vireoLightboxSession.requestedPhotoId()) === key;
  if (anchor.id === 'detailImg') return String(_lbPageDetailPhotoId()) === key;
  if (anchor.hasAttribute('data-thumbnail-src')) {
    // A source probe can finish after the user scrolls away. The queue will
    // not load that offscreen anchor, so do not wait for its load event while
    // leaving the source control permanently pending.
    var root = document.getElementById('gridContainer');
    var rect = anchor.getBoundingClientRect();
    var bounds = root && root.getBoundingClientRect();
    if (!bounds || !rect.width || !rect.height ||
        rect.bottom <= bounds.top || rect.top >= bounds.bottom) return false;
  }
  var card = anchor.closest ? anchor.closest('.grid-card') : null;
  return !card || String(card.getAttribute('data-id')) === key;
}

function _vireoCancelPairSourceChange(/** @type {LightboxPhotoId | null} */ photoId, /** @type {string} */ requested) {
  var key = String(photoId);
  if (_vireoPairPendingSourceByPhoto[key] !== requested) return;
  delete _vireoPairPendingSourceByPhoto[key];
  vireoUpdatePairSourceControls(photoId);
}

function _vireoPairSourceLoadFailed(/** @type {LightboxPhotoId | null} */ photoId, /** @type {string} */ requested, /** @type {HTMLImageElement | null} */ anchor, /** @type {string} */ oldSrc) {
  var key = String(photoId);
  if (_vireoPairPendingSourceByPhoto[key] !== requested) return;
  var anchorStillCurrent = (
    anchor && anchor.isConnected &&
    _vireoPairAnchorStillCurrent(anchor, photoId)
  );
  delete _vireoPairPendingSourceByPhoto[key];
  vireoUpdatePairSourceControls(photoId);
  if (anchorStillCurrent && anchor && oldSrc) {
    // A RAW edit may have finished saving while the target was loading.
    // Restore the selected source with its current recipe/render version,
    // rather than the stale URL captured before that save.
    _vireoSetRenderedImageSource(anchor, vireoRenderedUrl(
      _vireoBaseRenderedUrl(oldSrc), photoId
    ));
  }
  if (anchorStillCurrent && typeof showToast === 'function') {
    showToast('Could not load the ' + requested.toUpperCase() + ' source', 'error');
  }
}

export function _vireoPairSourceImageLoaded(/** @type {number} */ photoId, /** @type {string} */ requested, /** @type {HTMLImageElement | null} */ anchor) {
  var key = String(photoId);
  if (
    !anchor || anchor.id !== 'lightboxImg' ||
    String(vireoLightboxSession.requestedPhotoId()) !== key
  ) return;
  lbState.fullLongEdge = lbState.currentSrcKey === 'full'
    ? (Math.max(anchor.naturalWidth || 0, anchor.naturalHeight || 0) || null)
    : null;
  vireoLightboxViewport.invalidateGeometry();
  vireoLightboxViewport.recomputeNativeZoom();
  vireoLightboxViewport.applyTransform();
  if (requested === 'raw') {
    _lbLoadDetections(photoId);
    _lbRenderEyeCrosshair(_lbPhotoDataByPhoto[key]);
    _lbLoadMaskVariants(photoId);
  }
  _lbApplyBoxesVisibility();
  _lbApplyEyeVisibility();
  _lbApplyMaskVisibility();
}

function _vireoCommitPairSource(/** @type {number} */ photoId, /** @type {string} */ requested, /** @type {HTMLImageElement | null} */ anchor) {
  var key = String(photoId);
  if (_vireoPairPendingSourceByPhoto[key] !== requested) return;
  _vireoPairSourceByPhoto[key] = requested;
  delete _vireoPairPendingSourceByPhoto[key];
  vireoUpdatePairSourceControls(photoId);
  _vireoPairSourceImageLoaded(photoId, requested, anchor);
  if (typeof vireoRefreshPhotoRenders === 'function') {
    vireoRefreshPhotoRenders([photoId]);
  }
}

/** @type {LightboxApi['vireoTogglePairSource']} */ export function vireoTogglePairSource(photoId) {
  if (photoId == null || !_vireoPairKnownByPhoto[String(photoId)]) return;
  var key = String(photoId);
  if (_vireoPairPendingSourceByPhoto[key]) return;
  var requested = _vireoPairSourceByPhoto[key] === 'raw'
    ? 'jpeg'
    : 'raw';
  // Commit input made on the RAW before source switching disables controls.
  if (String(vireoLightboxSession.requestedPhotoId()) === key) _lbFlushPendingAdjustmentSave();
  const anchor = _vireoPairAnchorImage(photoId);
  var oldSrc = anchor ? _vireoRenderedImageSource(anchor) : '';
  var base = oldSrc ? _vireoBaseRenderedUrl(oldSrc) : '/thumbnails/' + key + '.jpg';
  _vireoBumpRenderVersion(photoId);
  var targetUrl = vireoRenderedUrl(base, photoId, requested);
  _vireoPairPendingSourceByPhoto[key] = requested;
  vireoUpdatePairSourceControls(photoId);

  // Validate the target without disturbing the currently displayed pixels.
  // Once it is decoded, swap the visible anchor and commit the label/state only
  // from that image's load event. A failed request leaves the old source intact.
  var probe = new Image();
  probe.onload = function() {
    if (_vireoPairPendingSourceByPhoto[key] !== requested) return;
    if (
      !anchor || !anchor.isConnected ||
      !_vireoPairAnchorStillCurrent(anchor, photoId)
    ) {
      _vireoCancelPairSourceChange(photoId, requested);
      return;
    }
    function cleanup() { if (!anchor) return;
      anchor.removeEventListener('load', loaded);
      anchor.removeEventListener('error', failed);
      anchor.removeEventListener('vireo:thumbnail-cancelled', cancelled);
    }
    function loaded() {
      cleanup();
      if (!_vireoPairAnchorStillCurrent(anchor, photoId)) {
        _vireoCancelPairSourceChange(photoId, requested);
        return;
      }
      _vireoCommitPairSource(photoId, requested, anchor);
    }
    function cancelled() {
      cleanup();
      _vireoCancelPairSourceChange(photoId, requested);
      if (anchor) _vireoSetRenderedImageSource(anchor, oldSrc);
    }
    function failed() {
      cleanup();
      _vireoPairSourceLoadFailed(photoId, requested, anchor, oldSrc);
    }
    anchor.addEventListener('load', loaded);
    anchor.addEventListener('error', failed);
    anchor.addEventListener('vireo:thumbnail-cancelled', cancelled);
    if (_vireoPairAnchorStillCurrent(anchor, photoId)) {
      _vireoSetRenderedImageSource(anchor, targetUrl);
    } else {
      cleanup();
      _vireoCancelPairSourceChange(photoId, requested);
    }
  };
  probe.onerror = function() {
    _vireoPairSourceLoadFailed(photoId, requested, anchor, oldSrc);
  };
  probe.src = targetUrl;
}

/** @type {LightboxApi['vireoRefreshPhotoRenders']} */ export function vireoRefreshPhotoRenders(photoIds) {
  if (!Array.isArray(photoIds)) photoIds = [photoIds];
  /** @type {Record<string, boolean>} */ var idSet = {};
  photoIds.forEach(function(id) {
    if (id == null) return;
    idSet[String(id)] = true;
  });
  if (!Object.keys(idSet).length) return;
  var imgs = /** @type {NodeListOf<HTMLImageElement>} */ (document.querySelectorAll('img[src], img[data-thumbnail-src]'));
  imgs.forEach(function(img) {
    var attr = _vireoRenderedImageSource(img);
    var base = _vireoBaseRenderedUrl(attr);
    Object.keys(idSet).forEach(function(id) {
      if (
        base.indexOf('/thumbnails/' + id + '.jpg') === 0 ||
        base.indexOf('/photos/' + id + '/full') === 0 ||
        base.indexOf('/photos/' + id + '/original') === 0 ||
        base.indexOf('/photos/' + id + '/preview?') === 0
      ) {
        _vireoSetRenderedImageSource(img, vireoRenderedUrl
          ? vireoRenderedUrl(base, id)
          : _vireoUrlWithRenderVersion(base, id));
      }
    });
  });
  try {
    document.dispatchEvent(new CustomEvent('lightbox:renderchanged', {
      detail: { photoIds: Object.keys(idSet).map(function(id) { return parseInt(id, 10); }) },
    }));
  } catch (_) {}
}

/** @type {LightboxApi['vireoRenderedUrl']} */ export function vireoRenderedUrl(url, photoId, sourceOverride) {
  var source = sourceOverride || (photoId == null ? null : _vireoPairSource(photoId));
  if (source) url = _vireoUrlWithPhotoSource(url, source);
  var recipeKey = _vireoPhotoRenderKey(photoId);
  if (recipeKey) url = _vireoUrlWithQueryParam(url, 'er', recipeKey);
  return _vireoUrlWithRenderVersion(url, photoId);
}

// The `er` fingerprint for a photo: the server's key when the payload carried
// one, otherwise a fingerprint of the recipe we hold. Both exist because they
// go stale at different moments — the server key is authoritative on page load
// and after a refetch, while a recipe this page just wrote is newer than any
// key the last payload carried.
function _vireoPhotoRenderKey(/** @type {LightboxPhotoId | null} */ photoId) {
  if (photoId == null) return '';
  var serverKey = _lbRenderKeyByPhoto[String(photoId)];
  if (serverKey) return serverKey;
  var recipe = _lbRawEditRecipeByPhoto[String(photoId)];
  if (recipe === undefined) recipe = _lbEditRecipeByPhoto[String(photoId)];
  return _vireoEditRecipeCacheKey(recipe);
}

/** @returns {string} */ export function vireoThumbnailUrl(/** @type {LightboxPhoto | LightboxPhotoId | null} */ photoOrId) {
  var photo = photoOrId && typeof photoOrId === 'object' ? photoOrId : null;
  var photoId = photo ? (photo.photo_id != null ? photo.photo_id : photo.id) : (typeof photoOrId === 'object' ? null : photoOrId);
  if (photoId == null) return '';
  if (photo && typeof vireoRememberPhotoPair === 'function') {
    vireoRememberPhotoPair(photo);
  }
  if (
    photo &&
    Object.prototype.hasOwnProperty.call(photo, 'edit_recipe') &&
    typeof photo.edit_recipe !== 'undefined' &&
    typeof vireoRememberPhotoEditRecipe === 'function'
  ) {
    vireoRememberPhotoEditRecipe(photoId, photo.edit_recipe, {
      skipIfLocallyWritten: true
    });
  }
  if (
    photo &&
    Object.prototype.hasOwnProperty.call(photo, 'render_key') &&
    typeof vireoRememberPhotoRenderKey === 'function'
  ) {
    vireoRememberPhotoRenderKey(photoId, photo.render_key, {
      skipIfLocallyWritten: true
    });
  }
  var url = '/thumbnails/' + photoId + '.jpg';
  return vireoRenderedUrl
    ? vireoRenderedUrl(url, photoId)
    : _vireoUrlWithRenderVersion(url, photoId);
}

export function vireoCacheBustedPhotoUrl(/** @type {LightboxPhotoId | null} */ photoId, /** @type {string} */ url) {
  url = String(url || '')
    .replace(/([?&])editv=[^&]*(&?)/, function(match, prefix, suffix) {
      return suffix ? prefix : '';
    })
    .replace(/([?&])v=[^&]*(&?)/, function(match, prefix, suffix) {
      return suffix ? prefix : '';
    })
    .replace(/[?&]$/, '');
  url = vireoRenderedUrl
    ? vireoRenderedUrl(url, photoId)
    : _vireoUrlWithRenderVersion(url, photoId);
  var version = _lbEditVersionByPhoto[String(photoId)];
  if (version) url = _vireoUrlWithQueryParam(url, 'editv', version);
  var bust = _lbRecipeBustByPhotoId[String(photoId)];
  if (bust) url = _vireoUrlWithQueryParam(url, 'v', bust);
  return url;
}

export function vireoPreviewUrl(/** @type {LightboxPhotoId | null} */ photoId, /** @type {number | undefined} */ size = undefined) {
  return vireoCacheBustedPhotoUrl(photoId, '/photos/' + photoId + '/preview?size=' + (size || 1920));
}


/** @returns {LightboxRecipe} */ export function _lbCloneEditRecipe(/** @type {LightboxRecipe | null | undefined} */ recipe) {
  if (!recipe || typeof recipe !== 'object') return {};
  /** @type {LightboxRecipe} */ var out = {};
  if (recipe.rotation) out.rotation = recipe.rotation;
  if (Math.abs(Number(recipe.straighten || 0)) > 1e-9) {
    out.straighten = Number(recipe.straighten);
  }
  if (recipe.flip && typeof recipe.flip === 'object') {
    out.flip = {};
    if (recipe.flip.horizontal) out.flip.horizontal = true;
    if (recipe.flip.vertical) out.flip.vertical = true;
    if (!Object.keys(out.flip).length) delete out.flip;
  }
  if (recipe.crop && typeof recipe.crop === 'object') {
    out.crop = {
      x: recipe.crop.x,
      y: recipe.crop.y,
      w: recipe.crop.w,
      h: recipe.crop.h,
    };
  }
  if (recipe.adjustments && typeof recipe.adjustments === 'object') {
    out.adjustments = Object.assign({}, recipe.adjustments);
  }
  return out;
}

function _vireoHashString(/** @type {string} */ s) {
  var hash = 5381;
  for (var i = 0; i < s.length; i++) {
    hash = ((hash << 5) + hash) ^ s.charCodeAt(i);
  }
  return (hash >>> 0).toString(36);
}

// Must equal image_edits.EDIT_MATH_VERSION. Folded into the `er` query
// param below so a math bump invalidates the browser cache for every
// edited render — server-side purges alone are not enough because the
// thumbnail response is `Cache-Control: public, max-age=86400`, so a user
// who viewed an edited photo before the deploy would otherwise keep
// seeing the old bytes from their own browser cache until they expire.
// test_edit_math_version_template_constant_matches_python locks the two
// constants together.
var _VIREO_EDIT_MATH_VERSION = 10;

// Stable stringify: sort object keys at every depth so a fingerprint tracks
// the recipe's *content*. Two payloads can serialize the same recipe with
// different property order, and JSON.stringify would hash them differently —
// which costs a needless refetch of an image that did not change.
/** @returns {string} */ function _vireoCanonicalRecipeJson(/** @type {unknown} */ value) {
  if (Array.isArray(value)) {
    return '[' + value.map(_vireoCanonicalRecipeJson).join(',') + ']';
  }
  if (value && typeof value === 'object') {
    return '{' + Object.keys(value).sort().map(function(key) {
      return JSON.stringify(key) + ':' + _vireoCanonicalRecipeJson(/** @type {Record<string, unknown>} */ (value)[key]);
    }).join(',') + '}';
  }
  return JSON.stringify(value === undefined ? null : value);
}

// Fingerprint the *whole* recipe, not the subset the lightbox can edit.
// This used to hash ``_lbCloneEditRecipe(recipe)``, which models only
// rotation/straighten/flip/crop/adjustments — so a recipe whose only edit
// lived in the ``local`` section fingerprinted identically to no edit at all,
// and the grid kept serving the pre-edit thumbnail out of the browser cache
// for the full 24h max-age. Prefer ``render_key`` from the server (see
// ``_vireoPhotoRenderKey``); this is the fallback for payloads that predate
// it and for recipes this page wrote itself.
export function _vireoEditRecipeCacheKey(/** @type {LightboxRecipe | null | undefined} */ recipe) {
  if (!_lbRecipeHasEdits(recipe)) return '';
  return _vireoHashString(_vireoCanonicalRecipeJson(recipe))
    + '.m' + _VIREO_EDIT_MATH_VERSION;
}

export function _lbRecipeHasEdits(/** @type {LightboxRecipe | null | undefined} */ recipe) {
  return !!(recipe && typeof recipe === 'object' && Object.keys(recipe).some(function(k) {
    return k !== 'version';
  }));
}

function _lbRecipeHasOrientation(/** @type {LightboxRecipe | null | undefined} */ recipe) {
  return !!(
    recipe &&
    typeof recipe === 'object' &&
    (recipe.rotation || (recipe.flip && (recipe.flip.horizontal || recipe.flip.vertical)))
  );
}

export function _lbCurrentRecipeHasOrientation() {
  if (vireoLightboxSession.requestedPhotoId() == null) return false;
  return _lbRecipeHasOrientation(_lbEditRecipeByPhoto[String(vireoLightboxSession.requestedPhotoId())]);
}

/** @type {LightboxApi['vireoPhotoHasOrientationEdit']} */ export function vireoPhotoHasOrientationEdit(photoId) {
  if (photoId == null) return false;
  return _lbRecipeHasGeometricEdit(_lbEditRecipeByPhoto[String(photoId)]);
}

/** @type {LightboxApi['vireoRememberPhotoEditRecipe']} */ export function vireoRememberPhotoEditRecipe(photoId, recipe, options) {
  if (options && options.skipIfLocallyWritten && _lbEditRecipeWriteSeqFor(photoId)) return;
  _lbRememberEditRecipe(photoId, recipe);
}

// Record the server's render key for a photo. Call this *after* remembering
// the recipe it belongs to: ``_lbRememberEditRecipe`` drops any key it holds,
// because every other caller of it is a local write whose recipe is newer than
// the key the last payload carried.
/** @type {LightboxApi['vireoRememberPhotoRenderKey']} */
export function vireoRememberPhotoRenderKey(photoId, renderKey, options) {
  if (photoId == null) return;
  if (options && options.skipIfLocallyWritten && _lbEditRecipeWriteSeqFor(photoId)) return;
  if (renderKey) _lbRenderKeyByPhoto[String(photoId)] = String(renderKey);
  else delete _lbRenderKeyByPhoto[String(photoId)];
}


export function _lbRememberEditRecipe(/** @type {LightboxPhotoId | null} */ photoId, /** @type {LightboxRecipe | null | undefined} */ recipe, /** @type {boolean} */ preserveAdjustmentInput = false) {
  if (photoId == null) return;
  var numericId = Number(photoId);
  // Keep the unclipped recipe for fingerprinting, and drop any server render
  // key we were holding: this recipe is the newer of the two.
  _lbRawEditRecipeByPhoto[String(photoId)] = recipe || null;
  delete _lbRenderKeyByPhoto[String(photoId)];
  var storedRecipe = _lbCloneEditRecipe(recipe);
  var currentRecipe = _lbRecipeHasEdits(storedRecipe) ? storedRecipe : null;
  _lbEditRecipeByPhoto[String(photoId)] = storedRecipe;
  _lbEditRecipeKnownByPhoto[String(photoId)] = true;
  // The list entry is re-read as the photo's recipe on the next open, so give
  // it the unclipped recipe. The clipped clone drops ``version`` and ``local``;
  // fingerprinting it against the server's copy reports an edit that never
  // happened, and that reload resets a 1:1 view to fit on arrow navigation.
  var p = lbState.photoList.find(function(x) { return x.id === numericId; });
  if (p) p.edit_recipe = _lbRecipeHasEdits(recipe) ? recipe : null;
  if (vireoLightboxSession.requestedPhotoId() === numericId) {
    lbState.currentEditRecipe = currentRecipe;
    if (!preserveAdjustmentInput) {
      lbState.editRecipe = _lbCloneRecipe(storedRecipe);
      lbState.editRecipeLoaded = true;
      _lbRenderAdjustmentControls();
      _lbSetAdjustmentControlsDisabled(false);
    }
  }
  _lbApplyEditButtonState();
}

export function _lbEditRecipeWriteSeqFor(/** @type {LightboxPhotoId | null} */ photoId) {
  if (photoId == null) return 0;
  return _lbEditRecipeWriteSeqByPhoto[String(photoId)] || 0;
}

export function _lbMarkEditRecipeWrite(/** @type {LightboxPhotoId | null} */ photoId) {
  if (photoId == null) return;
  lbState.editRecipeWriteSeq += 1;
  _lbEditRecipeWriteSeqByPhoto[String(photoId)] = lbState.editRecipeWriteSeq;
}

function _lbCurrentEditRecipeClone() {
  if (vireoLightboxSession.requestedPhotoId() == null) return {};
  return _lbCloneEditRecipe(_lbEditRecipeByPhoto[String(vireoLightboxSession.requestedPhotoId())]);
}








export function _lbApplyEditButtonState() {
  var recipe = _lbCurrentEditRecipeClone();
  var hasOrientation = _lbRecipeHasOrientation(recipe);
  var known = vireoLightboxSession.requestedPhotoId() != null && !!_lbEditRecipeKnownByPhoto[String(vireoLightboxSession.requestedPhotoId())];
  var ids = [
    'lightboxRotateLeft',
    'lightboxRotateRight',
    'lightboxFlipHorizontal',
    'lightboxFlipVertical',
    'lightboxResetEdit',
  ];
  ids.forEach(function(id) {
    var btn = /** @type {HTMLButtonElement | null} */ (document.getElementById(id));
    if (!btn) return;
    btn.disabled = _lbEditWritePending || vireoLightboxSession.requestedPhotoId() == null || !known || (id === 'lightboxResetEdit' && !hasOrientation);
  });
}

export function _lbSetEditBusy(/** @type {boolean} */ busy) {
  _lbEditWritePending = !!busy;
  _lbApplyEditButtonState();
}

export function _lbReloadCurrentRenderAfterEdit(/** @type {number} */ photoId) {
  if (vireoLightboxSession.requestedPhotoId() !== photoId) return;
  _lbClearAdjustmentPreview();
  vireoLightboxSession.cancelOriginal();
  const img = /** @type {HTMLImageElement | null} */ (document.getElementById('lightboxImg'));
  var wrap = document.getElementById('lightboxWrap');
  if (!img) return;
  vireoLightboxViewport.resetForEdit();
  lbState.currentSrcKey = 'full';
  lbState.fullLongEdge = null;
  lbState.originalUnavailable = false;
  vireoLightboxSession.cancelSwap();
  lbState.desiredSrcKey = null;
  lbState.progressiveTargetKey = null;
  // Cancelling the scheduled swap and clearing the desired key makes the old
  // preloader callbacks return as stale, so nothing else will ever turn this
  // off. Left set, the phase stays 'sharpening' with no request behind it.
  _lbSetPreviewLoading(false);
  if (wrap) wrap.classList.remove('zoomed');
  vireoLightboxViewport.applyTransform();
  // Replacing img.onload/onerror orphans handleInitialImageLoad when the
  // metadata fetch beats the initial image, so this reload inherits the job of
  // ending that photo's initial load -- otherwise nothing ever clears the
  // pending decode and the chip stays on Loading indefinitely. The flag is
  // cleared on completion, not here: the reload is itself a load in flight.
  img.onload = function() {
    img.onload = null;
    img.onerror = null;
    if (vireoLightboxSession.requestedPhotoId() !== photoId) return;
    lbState.initialDecodePending = false;
    if (img.naturalWidth) {
      lbState.fullLongEdge = Math.max(img.naturalWidth, img.naturalHeight);
    }
    vireoLightboxViewport.recomputeNativeZoom();
    vireoLightboxViewport.applyTransform();
    // Settle the load this reload displaced. It schedules the neighbours and
    // renders the status itself, so only do that work when there was nothing
    // pending (a reload from the adjustments panel, long after the open).
    if (!vireoLightboxSession.finishInitialLoad()) {
      _lbRenderDetailStatus();
      vireoLightboxSession.scheduleOriginal(photoId);
    }
  };
  img.onerror = function() {
    img.onload = null;
    img.onerror = null;
    if (vireoLightboxSession.requestedPhotoId() !== photoId) return;
    lbState.initialDecodePending = false;
    // Nothing is going to render. Hand the displaced load its failure path,
    // which commits the incoming photo's identity before bringing the controls
    // back -- unfreezing without that hands the user live controls against the
    // outgoing photo's filename and counter.
    if (!vireoLightboxSession.abandonInitialLoad()) _lbRenderDetailStatus();
    if (typeof showToast === 'function') {
      showToast('Could not reload edited photo', 'error');
    }
  };
  img.src = _lbSrcUrl(photoId, 'full');
  _lbLoadDetections(photoId);
  _lbRenderEyeCrosshair(_lbPhotoDataByPhoto[String(photoId)]);
  _lbApplyMaskVisibility();
  _lbLoadMaskVariants(photoId);
}


export function _lbTransformPointByRecipe(/** @type {number} */ x, /** @type {number} */ y, /** @type {LightboxRecipe | null | undefined} */ recipe) {
  recipe = recipe || {};
  var rotation = Number(recipe.rotation) || 0;
  var nx = x;
  var ny = y;
  if (rotation === 90) {
    nx = 1 - y;
    ny = x;
  } else if (rotation === 180) {
    nx = 1 - x;
    ny = 1 - y;
  } else if (rotation === 270) {
    nx = y;
    ny = 1 - x;
  }
  var flip = recipe.flip || {};
  if (flip.horizontal) nx = 1 - nx;
  if (flip.vertical) ny = 1 - ny;
  return {
    x: Math.max(0, Math.min(1, nx)),
    y: Math.max(0, Math.min(1, ny)),
  };
}

export function _lbTransformBoxByRecipe(/** @type {LightboxCrop} */ box, /** @type {LightboxRecipe | null | undefined} */ recipe) {
  recipe = recipe || {};
  var x = Number(box.x);
  var y = Number(box.y);
  var w = Number(box.w);
  var h = Number(box.h);
  if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(w) || !Number.isFinite(h)) return box;
  var rotation = Number(recipe.rotation) || 0;
  var nx = x;
  var ny = y;
  var nw = w;
  var nh = h;
  if (rotation === 90) {
    nx = 1 - (y + h);
    ny = x;
    nw = h;
    nh = w;
  } else if (rotation === 180) {
    nx = 1 - (x + w);
    ny = 1 - (y + h);
  } else if (rotation === 270) {
    nx = y;
    ny = 1 - (x + w);
    nw = h;
    nh = w;
  }
  var flip = recipe.flip || {};
  if (flip.horizontal) nx = 1 - (nx + nw);
  if (flip.vertical) ny = 1 - (ny + nh);
  nx = Math.max(0, Math.min(1, nx));
  ny = Math.max(0, Math.min(1, ny));
  nw = Math.max(0, Math.min(1 - nx, nw));
  nh = Math.max(0, Math.min(1 - ny, nh));
  return { x: nx, y: ny, w: nw, h: nh };
}

/** @param {LightboxRecipe | null | undefined} recipe @returns {LightboxRecipe} */ export function _lbCloneRecipe(recipe) {
  if (!recipe) return {};
  try {
    return JSON.parse(JSON.stringify(recipe));
  } catch (_) {
    return {};
  }
}

function _lbAdjustmentInputSeqFor(/** @type {LightboxPhotoId | null} */ photoId) {
  return _lbAdjustmentInputSeqByPhoto[String(photoId)] || 0;
}

function _lbBumpAdjustmentInputSeq(/** @type {LightboxPhotoId | null} */ photoId) {
  var key = String(photoId);
  _lbAdjustmentInputSeqByPhoto[key] = (_lbAdjustmentInputSeqByPhoto[key] || 0) + 1;
  return _lbAdjustmentInputSeqByPhoto[key];
}

function _lbAdjustmentValues(/** @type {LightboxRecipe | null | undefined} */ recipe) {
  var adj = (recipe && recipe.adjustments) || {};
  var wb = adj.white_balance || {};
  return {
    exposure: Number(adj.exposure || 0),
    highlights: Number(adj.highlights || 0),
    shadows: Number(adj.shadows || 0),
    whites: Number(adj.whites || 0),
    blacks: Number(adj.blacks || 0),
    contrast: Number(adj.contrast || 0),
    temperature: Number(wb.temperature || adj.temperature || 0),
    tint: Number(wb.tint || adj.tint || 0),
    vibrance: Number(adj.vibrance || 0),
    saturation: Number(adj.saturation || 0),
  };
}

export function _lbSetAdjustmentStatus(/** @type {string} */ text, /** @type {boolean | undefined} */ isError = undefined) {
  var el = document.getElementById('lightboxAdjustStatus');
  if (!el) return;
  el.textContent = text || '';
  el.classList.toggle('error', !!isError);
}

function _lbFormatAdjustmentValue(/** @type {string | undefined} */ name, /** @type {number | string} */ value) {
  if (name === 'exposure') return Number(value || 0).toFixed(1);
  return String(Math.round(Number(value || 0)));
}

function _lbSetAdjustmentControl(/** @type {string} */ id, /** @type {number} */ value) {
  var input = /** @type {HTMLInputElement | null} */ (document.getElementById(id));
  var label = document.getElementById(id + 'Value');
  if (input) input.value = String(value || 0);
  if (label && input) {
    label.textContent = _lbFormatAdjustmentValue(input.dataset.adjustment, value);
  }
}

export function _lbSetAdjustmentControlsDisabled(/** @type {boolean} */ disabled) {
  disabled = !!disabled || lbState.readOnly || !!_lbAdjustmentSourceHint();
  [
    'lbAdjExposure',
    'lbAdjHighlights',
    'lbAdjShadows',
    'lbAdjWhites',
    'lbAdjBlacks',
    'lbAdjContrast',
    'lbAdjTemperature',
    'lbAdjTint',
    'lbAdjVibrance',
    'lbAdjSaturation',
  ].forEach(function(id) {
    var input = /** @type {HTMLInputElement | null} */ (document.getElementById(id));
    if (input) input.disabled = !!disabled;
  });
  var reset = /** @type {HTMLButtonElement | null} */ (document.querySelector('#lightboxAdjustPanel .lb-adjust-reset'));
  if (reset) reset.disabled = !!disabled;
}

export function _lbAdjustmentSourceHint() {
  if (vireoLightboxSession.requestedPhotoId() == null) return '';
  if (_vireoPairPendingSourceByPhoto[String(vireoLightboxSession.requestedPhotoId())]) {
    return 'Wait for the photo source to finish loading';
  }
  // A developed companion JPEG is displayed as-authored; recipes belong to
  // the primary RAW and may use entirely different image coordinates.
  return _vireoPairSource(vireoLightboxSession.requestedPhotoId()) === 'jpeg'
    ? 'Switch to RAW to use quick adjustments'
    : '';
}

function _lbGuardAdjustmentSource() {
  if (_lbGuardReadOnly()) return true;
  var hint = _lbAdjustmentSourceHint();
  if (!hint) return false;
  if (typeof showToast === 'function') showToast(hint, 'warning');
  return true;
}

export function _lbRenderAdjustmentControls() {
  var values = _lbAdjustmentValues(lbState.editRecipe);
  _lbSetAdjustmentControl('lbAdjExposure', values.exposure);
  _lbSetAdjustmentControl('lbAdjHighlights', values.highlights);
  _lbSetAdjustmentControl('lbAdjShadows', values.shadows);
  _lbSetAdjustmentControl('lbAdjWhites', values.whites);
  _lbSetAdjustmentControl('lbAdjBlacks', values.blacks);
  _lbSetAdjustmentControl('lbAdjContrast', values.contrast);
  _lbSetAdjustmentControl('lbAdjTemperature', values.temperature);
  _lbSetAdjustmentControl('lbAdjTint', values.tint);
  _lbSetAdjustmentControl('lbAdjVibrance', values.vibrance);
  _lbSetAdjustmentControl('lbAdjSaturation', values.saturation);
}

function _lbReadAdjustmentControls() {
  function val(/** @type {string} */ id) {
    var el = /** @type {HTMLInputElement | null} */ (document.getElementById(id));
    return el ? Number(el.value || 0) : 0;
  }
  return {
    exposure: val('lbAdjExposure'),
    highlights: val('lbAdjHighlights'),
    shadows: val('lbAdjShadows'),
    whites: val('lbAdjWhites'),
    blacks: val('lbAdjBlacks'),
    contrast: val('lbAdjContrast'),
    temperature: val('lbAdjTemperature'),
    tint: val('lbAdjTint'),
    vibrance: val('lbAdjVibrance'),
    saturation: val('lbAdjSaturation'),
  };
}

function _lbRecipeWithAdjustments(/** @type {LightboxAdjustmentValues} */ values) {
  var recipe = _lbCloneRecipe(lbState.editRecipe);
  delete recipe.version;
  var adj = Object.assign({}, recipe.adjustments || {});
  /** @type {(keyof LightboxAdjustmentValues)[]} */ (['exposure', 'highlights', 'shadows', 'whites', 'blacks', 'contrast', 'vibrance', 'saturation']).forEach(function(key) {
    var value = Number(values[key] || 0);
    if (Math.abs(value) > 0.000001) adj[key] = value;
    else delete adj[key];
  });

  var wb = Object.assign({}, adj.white_balance || {});
  delete adj.temperature;
  delete adj.tint;
  /** @type {('temperature' | 'tint')[]} */ (['temperature', 'tint']).forEach(function(key) {
    var value = Number(values[key] || 0);
    if (Math.abs(value) > 0.000001) wb[key] = value;
    else delete wb[key];
  });
  if (Object.keys(wb).length) adj.white_balance = wb;
  else delete adj.white_balance;

  if (Object.keys(adj).length) recipe.adjustments = adj;
  else delete recipe.adjustments;
  return recipe;
}

// Mirror tone.white_balance_gains: per-channel linear-light gains. Keep these
// coefficients in sync with the server so the live preview matches the render.
function _lbWhiteBalanceGains(/** @type {number} */ temperature, /** @type {number} */ tint) {
  var t = Number(temperature || 0) / 100;
  var ti = Number(tint || 0) / 100;
  return {
    r: Math.max(0.05, 1 + 0.26 * t + 0.06 * ti),
    g: Math.max(0.05, 1 - 0.18 * ti),
    b: Math.max(0.05, 1 - 0.26 * t + 0.06 * ti),
  };
}

// GPU transcription of vireo/tone.py, applied to a neutral source with the
// complete adjustment values. Keep shader math in sync with the server.
// Shadows/Highlights read a neighborhood base, so they always use the server
// preview (_lbPreviewNeedsServer) and have no counterpart here.
export var VireoToneGL = (function() {
  var KNEE = 0.85; // == tone.HIGHLIGHT_KNEE
  /** @type {HTMLCanvasElement | null} */ var canvas = null;
  /** @type {WebGLRenderingContext | null} */ var gl = null;
  /** @type {WebGLProgram | null} */ var prog = null;
  /** @type {WebGLTexture | null} */ var tex = null;
  /** @type {Record<'tex' | 'exposure' | 'wbGain' | 'whites' | 'blacks' | 'contrast' | 'vibrance' | 'saturation' | 'rolloff' | 'knee', WebGLUniformLocation | null> | null} */ var locs = null;
  /** @type {string | null} */ var texKey = null; var failed = false, maxTextureSize = 0;

  var VERT = [
    'attribute vec2 aPos;',
    'varying vec2 vUv;',
    'void main(){ vUv = aPos * 0.5 + 0.5; gl_Position = vec4(aPos, 0.0, 1.0); }'
  ].join('\n');

  var FRAG = [
    'precision highp float;',
    'varying vec2 vUv;',
    'uniform sampler2D uTex;',
    'uniform float uExposure;',   // stops (linear gain = exp2(uExposure))
    'uniform vec3 uWbGain;',      // per-channel linear gains
    'uniform float uWhites;',
    'uniform float uBlacks;',
    'uniform float uContrast;',   // display-space contrast factor about 0.5
    'uniform float uVibrance;',
    'uniform float uSaturation;', // display-space luma-preserving factor
    'uniform float uRolloff;',    // 1.0 -> apply highlight shoulder, else 0.0
    'uniform float uKnee;',
    'vec3 srgbToLinear(vec3 c){',
    '  return mix(c / 12.92, pow((c + 0.055) / 1.055, vec3(2.4)), step(0.04045, c));',
    '}',
    'vec3 linearToSrgb(vec3 c){',
    '  c = max(c, 0.0);',
    '  return mix(c * 12.92, 1.055 * pow(c, vec3(1.0 / 2.4)) - 0.055, step(0.0031308, c));',
    '}',
    'float roll(float x){',
    '  float h = 1.0 - uKnee;',
    '  float o = max(x - uKnee, 0.0);',
    '  float r = uKnee + h * (o / (o + h));',
    '  return x > uKnee ? r : x;',
    '}',
    'float blackLevelCurve(float x, float amount){',
    '  float a = amount / 100.0;',
    '  float positive = max(a, 0.0);',
    '  float negative = max(-a, 0.0);',
    '  float t = clamp(x / 0.30, 0.0, 1.0);',
    '  float shoulder = pow(1.0 - t, 2.0);',
    '  float lift = 0.30 * 0.40 * positive * shoulder;',
    '  float deepen = 0.30 * 0.90 * negative * t * shoulder;',
    '  return x < 0.30 ? x + lift - deepen : x;',
    '}',
    'vec3 applyRangeCurves(vec3 rangeLin){',
    '  float luminance = dot(rangeLin, vec3(0.2126, 0.7152, 0.0722));',
    '  float level = linearToSrgb(vec3(luminance)).r;',
    '  float sourceLevel = level;',
    '  level = blackLevelCurve(level, uBlacks);',
    '  level = 1.0 - blackLevelCurve(1.0 - level, -uWhites);',
    '  float targetLuminance = srgbToLinear(vec3(clamp(level, 0.0, 1.0))).r;',
    '  vec3 mapped = luminance > 0.0000001',
    '    ? rangeLin * (targetLuminance / luminance)',
    '    : vec3(targetLuminance);',
    '  float chromaRetention = targetLuminance > luminance',
    '    ? smoothstep(0.0, 0.0156862745, sourceLevel)',
    '    : 1.0;',
    '  chromaRetention *= chromaRetention;',
    '  mapped = vec3(targetLuminance) +',
    '    (mapped - vec3(targetLuminance)) * chromaRetention;',
    '  float maxChannel = max(max(mapped.r, mapped.g), mapped.b);',
    '  float denominator = max(maxChannel - targetLuminance, 0.0000001);',
    '  float chromaScale = clamp((1.0 - targetLuminance) / denominator, 0.0, 1.0);',
    '  mapped = vec3(targetLuminance) + (mapped - vec3(targetLuminance)) * chromaScale;',
    '  return clamp(mapped, 0.0, 1.0);',
    '}',
    'vec3 applyVibrance(vec3 rgb, float amount){',
    '  float a = amount / 100.0;',
    '  if (abs(a) < 0.000001) return rgb;',
    '  float l = dot(rgb, vec3(0.2126, 0.7152, 0.0722));',
    '  float hi = max(max(rgb.r, rgb.g), rgb.b);',
    '  float lo = min(min(rgb.r, rgb.g), rgb.b);',
    '  float chroma = clamp(hi - lo, 0.0, 1.0);',
    '  float factor = a > 0.0 ? 1.0 + a * (1.0 - chroma) * 0.85 : 1.0 + a * 0.65;',
    '  return clamp(vec3(l) + (rgb - vec3(l)) * factor, 0.0, 1.0);',
    '}',
    'void main(){',
    '  vec4 src = texture2D(uTex, vUv);',
    '  vec3 linPre = srgbToLinear(src.rgb);',
    '  vec3 lin = linPre;',
    '  lin *= exp2(uExposure);',
    '  lin *= uWbGain;',
    // Mirror tone.apply_adjustments' monotonicity clamp: the shoulder is below
    // identity in (knee, ∞), so a 0.95-linear pixel at +0.1 EV would otherwise
    // darken to ~0.929. Clamp per channel to min(linPre, lin) so the rolloff
    // can only raise values toward white, never below the natural floor.
    '  if (uRolloff > 0.5) {',
    '    vec3 rolled = vec3(roll(lin.r), roll(lin.g), roll(lin.b));',
    '    lin = max(rolled, min(linPre, lin));',
    '  }',
    '  if (abs(uBlacks) + abs(uWhites) > 0.000001) {',
    '    lin = applyRangeCurves(lin);',
    '  }',
    '  vec3 disp = linearToSrgb(lin);',
    '  disp = (disp - 0.5) * uContrast + 0.5;',
    '  disp = applyVibrance(disp, uVibrance);',
    '  float luma = dot(disp, vec3(0.2126, 0.7152, 0.0722));',
    '  disp = vec3(luma) + (disp - vec3(luma)) * uSaturation;',
    // Pass through the source alpha (don't force 1.0) so transparent or
    // semi-transparent sources preview correctly, matching tone.py which
    // merges alpha back unchanged. Lightbox images are opaque JPEGs in
    // practice, so src.a == 1.0 there and the common case is unchanged.
    '  gl_FragColor = vec4(clamp(disp, 0.0, 1.0), src.a);',
    '}'
  ].join('\n');

  function compile(/** @type {number} */ type, /** @type {string} */ src) {
    if (!gl) return null;
    var s = gl.createShader(type); if (!s) return null;
    gl.shaderSource(s, src);
    gl.compileShader(s);
    if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) {
      console.warn('VireoToneGL shader compile error:', gl.getShaderInfoLog(s));
      return null;
    }
    return s;
  }

  function init() {
    if (gl) return true;
    if (failed) return false;
    canvas = /** @type {HTMLCanvasElement | null} */ (document.getElementById('lightboxToneCanvas'));
    if (!canvas) { failed = true; return false; }
    try {
      var opts = { premultipliedAlpha: false, preserveDrawingBuffer: false };
      gl = /** @type {WebGLRenderingContext | null} */ (canvas.getContext('webgl', opts)) || /** @type {WebGLRenderingContext | null} */ (canvas.getContext('experimental-webgl', opts));
    } catch (e) { gl = null; }
    if (!gl) { failed = true; return false; }
    var vs = compile(gl.VERTEX_SHADER, VERT);
    var fs = compile(gl.FRAGMENT_SHADER, FRAG);
    if (!vs || !fs) { failed = true; gl = null; return false; }
    prog = gl.createProgram(); if (!prog) { failed = true; gl = null; return false; }
    gl.attachShader(prog, vs);
    gl.attachShader(prog, fs);
    gl.linkProgram(prog);
    if (!gl.getProgramParameter(prog, gl.LINK_STATUS)) {
      console.warn('VireoToneGL link error:', gl.getProgramInfoLog(prog));
      failed = true; gl = null; return false;
    }
    gl.useProgram(prog);
    var buf = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, buf);
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1, -1, 1, -1, -1, 1, 1, 1]), gl.STATIC_DRAW);
    var aPos = gl.getAttribLocation(prog, 'aPos');
    gl.enableVertexAttribArray(aPos);
    gl.vertexAttribPointer(aPos, 2, gl.FLOAT, false, 0, 0);
    tex = gl.createTexture();
    gl.bindTexture(gl.TEXTURE_2D, tex);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
    gl.pixelStorei(gl.UNPACK_FLIP_Y_WEBGL, true);
    locs = {
      tex: gl.getUniformLocation(prog, 'uTex'),
      exposure: gl.getUniformLocation(prog, 'uExposure'),
      wbGain: gl.getUniformLocation(prog, 'uWbGain'),
      whites: gl.getUniformLocation(prog, 'uWhites'),
      blacks: gl.getUniformLocation(prog, 'uBlacks'),
      contrast: gl.getUniformLocation(prog, 'uContrast'),
      vibrance: gl.getUniformLocation(prog, 'uVibrance'),
      saturation: gl.getUniformLocation(prog, 'uSaturation'),
      rolloff: gl.getUniformLocation(prog, 'uRolloff'),
      knee: gl.getUniformLocation(prog, 'uKnee')
    };
    gl.uniform1i(locs.tex, 0);
    gl.uniform1f(locs.knee, KNEE);
    maxTextureSize = gl.getParameter(gl.MAX_TEXTURE_SIZE) || 0;
    return true;
  }

  function uploadSource(/** @type {HTMLImageElement} */ img) {
    if (!canvas || !gl) return false;
    var w = img.naturalWidth, h = img.naturalHeight;
    if (!w || !h) return false;
    // GPU limit: oversized images make texImage2D emit a GL error (e.g.
    // INVALID_VALUE) rather than throw, so the upload would otherwise look
    // successful and the canvas would render black. Bail to the server preview.
    if (maxTextureSize && (w > maxTextureSize || h > maxTextureSize)) return false;
    if (canvas.width !== w || canvas.height !== h) { canvas.width = w; canvas.height = h; }
    var key = img.currentSrc || img.src;
    if (key !== texKey) {
      gl.bindTexture(gl.TEXTURE_2D, tex);
      // Drain any prior error so getError() below reflects only this upload.
      while (gl.getError() !== gl.NO_ERROR) { /* Drain the GL error queue before uploading. */ }
      try {
        gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, img);
      } catch (e) {
        console.warn('VireoToneGL texture upload failed:', e);
        return false;
      }
      // texImage2D signals failures (e.g. cross-origin canvas tainting,
      // GPU limits the MAX_TEXTURE_SIZE check above didn't already catch)
      // through the GL error flag, not exceptions. Leaving texKey unset
      // forces a retry on the next frame.
      if (gl.getError() !== gl.NO_ERROR) {
        console.warn('VireoToneGL texture upload reported a GL error; '
          + 'falling back to server preview');
        return false;
      }
      texKey = key;
    }
    return true;
  }

  return {
    supported: function() { return init(); },
    // Force a texture re-upload (call when the underlying image pixels change).
    invalidate: function() { texKey = null; },
    render: function(/** @type {HTMLImageElement} */ img, /** @type {Omit<LightboxAdjustmentValues, "temperature" | "tint" | "highlights" | "shadows"> & {wbGain: [number, number, number], rolloff: boolean}} */ u) {
      if (!init() || !uploadSource(img) || !gl || !canvas || !locs) return false;
      gl.viewport(0, 0, canvas.width, canvas.height);
      gl.uniform1f(locs.exposure, u.exposure);
      gl.uniform3f(locs.wbGain, u.wbGain[0], u.wbGain[1], u.wbGain[2]);
      gl.uniform1f(locs.whites, u.whites);
      gl.uniform1f(locs.blacks, u.blacks);
      gl.uniform1f(locs.contrast, u.contrast);
      gl.uniform1f(locs.vibrance, u.vibrance);
      gl.uniform1f(locs.saturation, u.saturation);
      gl.uniform1f(locs.rolloff, u.rolloff ? 1.0 : 0.0);
      gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
      return true;
    }
  };
})();

// Always preview from a neutral, geometry-matched source. Applying deltas to
// the last saved render is history-dependent: rolloff and clipping cannot be
// undone, and contrast/saturation do not commute with the other tone controls.
function _lbAdjustmentPreviewUrl(/** @type {LightboxRecipe | null | undefined} */ recipe, /** @type {boolean} */ neutral) {
  const img = /** @type {HTMLImageElement | null} */ (document.getElementById('lightboxImg'));
  var size = Math.max(img ? img.naturalWidth : 0, img ? img.naturalHeight : 0) || 1920;
  return '/photos/' + vireoLightboxSession.requestedPhotoId() + '/edit-preview?size=' + size +
    '&apply_crop=1' + (neutral ? '&analysis=1' : '') +
    '&recipe=' + encodeURIComponent(JSON.stringify(recipe));
}

function _lbLoadAdjustmentSource() {
  var recipe = _lbCloneRecipe(lbState.editRecipe);
  delete recipe.version;
  delete recipe.adjustments;
  delete recipe.local;
  var url = _lbAdjustmentPreviewUrl(recipe, true);
  if (lbState.adjustmentSource && lbState.adjustmentSource.url === url) return lbState.adjustmentSource;
  /** @type {LightboxAdjustmentSource} */ var source = {url: url, image: new Image(), ready: Promise.resolve(false)};
  source.ready = new Promise(function(resolve) {
    source.image.onload = function() { resolve(true); };
    source.image.onerror = function() { resolve(false); };
  });
  source.image.src = url;
  lbState.adjustmentSource = source;
  return source;
}

// Advanced color, detail, and local edits must run in their canonical order
// with the global adjustments. Use the full server recipe for those, and for
// devices where WebGL cannot render the source, instead of stacking filters.
function _lbPreviewNeedsServer(/** @type {LightboxRecipe | null | undefined} */ recipe) {
  // RAW controls must run before display encoding; a JPEG WebGL texture has
  // already lost the scene highlight headroom and cannot match saved renders.
  var photo = lbState.photoList.find(function(p) { return p.id === vireoLightboxSession.requestedPhotoId(); });
  var filename = (photo && photo.filename) || document.getElementById('lightboxFilename')?.textContent;
  if (/\.(nef|cr2|cr3|arw|raf|dng|rw2|orf)$/i.test(filename || '')) return true;
  // Shadows/Highlights are a local exposure driven by neighboring luminance.
  // The single-pass shader cannot reproduce this shared preview/export math.
  var adjustments = (recipe && recipe.adjustments) || {};
  if (adjustments.shadows || adjustments.highlights) return true;
  var supported = ['exposure', 'highlights', 'shadows', 'whites', 'blacks',
    'contrast', 'vibrance', 'saturation', 'white_balance', 'temperature', 'tint'];
  return !!(recipe && recipe.local) || Object.keys((recipe && recipe.adjustments) || {}).some(function(key) {
    return supported.indexOf(key) === -1;
  });
}

function _lbApplyServerAdjustmentPreview(/** @type {LightboxRecipe | null | undefined} */ recipe, /** @type {number} */ seq) {
  var url = _lbAdjustmentPreviewUrl(recipe, false);
  // Coalesce slider events before starting an expensive server render.
  _lbAdjustmentPreviewTimer = setTimeout(function() {
    _lbAdjustmentPreviewTimer = null;
    var preview = new Image();
    preview.onload = function() {
      if (seq !== _lbAdjustmentPreviewSeq) return;
      var overlay = /** @type {HTMLImageElement | null} */ (document.getElementById('lightboxAdjustmentImage')); if (!overlay) return;
      overlay.src = url;
      overlay.classList.add('show');
      /** @type {HTMLCanvasElement | null} */ (document.getElementById('lightboxToneCanvas'))?.classList.remove('show');
    };
    preview.src = url;
  }, 80);
}

function _lbApplyAdjustmentPreview(/** @type {LightboxAdjustmentValues} */ values) {
  var seq = ++_lbAdjustmentPreviewSeq;
  if (_lbAdjustmentPreviewTimer != null) clearTimeout(_lbAdjustmentPreviewTimer);
  var recipe = _lbCloneRecipe(lbState.editRecipe);
  if (_lbPreviewNeedsServer(recipe) || !VireoToneGL.supported()) {
    _lbApplyServerAdjustmentPreview(recipe, seq);
    return;
  }
  var source = _lbLoadAdjustmentSource();
  source.ready.then(function(loaded) {
    if (seq !== _lbAdjustmentPreviewSeq) return;
    var gains = _lbWhiteBalanceGains(values.temperature, values.tint);
    var exposure = Number(values.exposure || 0);
    var pushed = Math.pow(2, exposure) * Math.max(gains.r, gains.g, gains.b) > 1.000001;
    var ok = loaded && VireoToneGL.render(source.image, {
      exposure: exposure,
      wbGain: [gains.r, gains.g, gains.b],
      whites: Number(values.whites || 0),
      blacks: Number(values.blacks || 0),
      contrast: Math.max(0, 1 + Number(values.contrast || 0) / 100),
      vibrance: Number(values.vibrance || 0),
      saturation: Math.max(0, 1 + Number(values.saturation || 0) / 100),
      rolloff: pushed
    });
    if (ok) {
      /** @type {HTMLImageElement | null} */ (document.getElementById('lightboxAdjustmentImage'))?.classList.remove('show');
      /** @type {HTMLCanvasElement | null} */ (document.getElementById('lightboxToneCanvas'))?.classList.add('show');
    } else {
      _lbApplyServerAdjustmentPreview(recipe, seq);
    }
  });
}

export function _lbClearAdjustmentPreview() {
  ++_lbAdjustmentPreviewSeq;
  if (_lbAdjustmentPreviewTimer != null) clearTimeout(_lbAdjustmentPreviewTimer);
  _lbAdjustmentPreviewTimer = null;
  /** @type {HTMLCanvasElement | null} */ (document.getElementById('lightboxToneCanvas'))?.classList.remove('show');
  /** @type {HTMLImageElement | null} */ (document.getElementById('lightboxAdjustmentImage'))?.classList.remove('show');
}

function _lbBumpRecipeBust(/** @type {LightboxPhotoId | null} */ photoId) {
  var key = String(photoId);
  _lbRecipeBustByPhotoId[key] = Date.now();
}

function _lbRefreshVisiblePhotoImages(/** @type {LightboxPhotoId | null} */ photoId) {
  var bust = _lbRecipeBustByPhotoId[String(photoId)];
  if (!bust) return;
  var thumbUrl = vireoThumbnailUrl(photoId);
  /** @type {HTMLImageElement[]} */ var refreshed = [];

  function refreshImg(/** @type {HTMLImageElement | null} */ img, /** @type {string | undefined} */ url = undefined) {
    if (!img || refreshed.indexOf(img) !== -1) return;
    refreshed.push(img);
    // Route through _vireoSetRenderedImageSource so grid cards update
    // data-thumbnail-src; the queue then cancels any in-flight fetch and
    // fetches the new URL under the concurrency limit, preventing a stale
    // response from overwriting the edited pixels.
    _vireoSetRenderedImageSource(img, url || thumbUrl);
  }

  refreshImg(/** @type {HTMLImageElement | null} */ (document.querySelector('.grid-card[data-id="' + photoId + '"] img')));
  /** @type {NodeListOf<HTMLImageElement>} */ (document.querySelectorAll('img[data-photo-id="' + photoId + '"]')).forEach(function(img) {
    refreshImg(img);
  });
  /** @type {NodeListOf<HTMLImageElement>} */ (document.querySelectorAll('[data-photo-id="' + photoId + '"] img')).forEach(function(img) {
    var src = img.getAttribute('src') || '';
    if (src.indexOf('/thumbnails/' + photoId + '.jpg') !== -1) {
      refreshImg(img);
    } else if (
      src.indexOf('/photos/' + photoId + '/preview') !== -1 ||
      src.indexOf('/photos/' + photoId + '/full') !== -1
    ) {
      refreshImg(img, vireoCacheBustedPhotoUrl(photoId, src));
    }
  });

  var detailImg = /** @type {HTMLImageElement | null} */ (document.getElementById('detailImg'));
  if (detailImg && _lbPageDetailPhotoId() === photoId) {
    refreshImg(detailImg);
  }
}

function _lbReloadEditedSource(/** @type {number} */ photoId, /** @type {number} */ seq) {
  if (vireoLightboxSession.requestedPhotoId() !== photoId) return;
  // The source switch owns the visible image until its target has decoded.
  if (_vireoPairPendingSourceByPhoto[String(photoId)]) return;
  vireoLightboxSession.cancelOriginal();
  const img = /** @type {HTMLImageElement | null} */ (document.getElementById('lightboxImg'));
  if (!img) return;
  var key = lbState.currentSrcKey || 'full';
  img.addEventListener('load', function onEditedReload() {
    img.removeEventListener('load', onEditedReload);
    if (vireoLightboxSession.requestedPhotoId() !== photoId || seq !== lbState.adjustSeq) return;
    _lbClearAdjustmentPreview();
    vireoLightboxViewport.recomputeNativeZoom();
    vireoLightboxViewport.applyTransform();
    if (key === 'full') vireoLightboxSession.scheduleOriginal(photoId);
  });
  img.src = _lbSrcUrl(photoId, key);
}

function _lbStartAdjustmentRecipeSave(/** @type {number} */ photoId, /** @type {LightboxRecipe | null | undefined} */ recipe, /** @type {number} */ inputSeq) {
  var seq = vireoLightboxSession.requestedPhotoId() === photoId ? ++lbState.adjustSeq : lbState.adjustSeq;
  var key = String(photoId);
  _lbAdjustSaveInFlightByPhoto[key] = true;
  if (vireoLightboxSession.requestedPhotoId() === photoId) _lbSetAdjustmentStatus('Saving...');
  fetch('/api/photos/' + photoId + '/edit-recipe', {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({recipe: recipe}),
  })
    .then(function(r) {
      return r.json().then(function(data) {
        if (!r.ok) throw new Error((data && data.error) || 'Save failed');
        return data;
      });
    })
    .then(function(data) {
      _lbMarkEditRecipeWrite(photoId);
      // Record the saved recipe without overwriting input made during the request.
      _lbRememberEditRecipe(photoId, data.recipe || null, true);
      _vireoBumpRenderVersion(photoId);
      _lbBumpRecipeBust(photoId);
      _lbRefreshVisiblePhotoImages(photoId);
      try {
        document.dispatchEvent(new CustomEvent('lightbox:editchanged', {
          detail: {photoId: photoId, recipe: data.recipe || null}
        }));
      } catch (_) {}
      if (vireoLightboxSession.requestedPhotoId() !== photoId || inputSeq !== _lbAdjustmentInputSeqFor(photoId)) return;
      var activeSeq = ++lbState.adjustSeq;
      lbState.editRecipe = _lbCloneRecipe(data.recipe || {});
      lbState.editRecipeLoaded = true;
      _lbRenderAdjustmentControls();
      _lbSetAdjustmentControlsDisabled(false);
      _lbReloadEditedSource(photoId, activeSeq);
      _lbSetAdjustmentStatus('Saved');
    })
    .catch(function(err) {
      if (vireoLightboxSession.requestedPhotoId() !== photoId || seq !== lbState.adjustSeq) return;
      _lbSetAdjustmentStatus(err.message || 'Save failed', true);
    })
    .then(function() {
      delete _lbAdjustSaveInFlightByPhoto[key];
      var queued = _lbQueuedAdjustmentSaveByPhoto[key];
      if (!queued) return;
      delete _lbQueuedAdjustmentSaveByPhoto[key];
      _lbStartAdjustmentRecipeSave(photoId, queued.recipe, queued.inputSeq);
    });
}

export function _lbSaveAdjustmentRecipe(/** @type {LightboxAdjustmentValues} */ values) {
  if (_lbGuardAdjustmentSource()) return false;
  var photoId = vireoLightboxSession.requestedPhotoId();
  if (photoId == null || !lbState.editRecipeLoaded) return;
  var key = String(photoId);
  var inputSeq = _lbAdjustmentInputSeqFor(photoId);
  var recipe = _lbRecipeWithAdjustments(values);
  lbState.editRecipe = _lbCloneRecipe(recipe);
  if (_lbAdjustSaveInFlightByPhoto[key]) {
    _lbQueuedAdjustmentSaveByPhoto[key] = {
      recipe: _lbCloneRecipe(recipe),
      inputSeq: inputSeq,
    };
    _lbSetAdjustmentStatus('Saving...');
    return;
  }
  _lbStartAdjustmentRecipeSave(photoId, recipe, inputSeq);
}

export function _lbFlushPendingAdjustmentSave() {
  if (!lbState.adjustSaveTimer) return;
  clearTimeout(lbState.adjustSaveTimer);
  lbState.adjustSaveTimer = null;
  _lbSaveAdjustmentRecipe(_lbReadAdjustmentControls());
}

export function _lbWaitForAdjustmentSaveIdle(/** @type {LightboxPhotoId | null} */ photoId) {
  var key = String(photoId);
  return new Promise(function(/** @type {(value?: void) => void} */ resolve, reject) {
    var started = Date.now();
    function check() {
      if (
        !lbState.adjustSaveTimer &&
        !_lbAdjustSaveInFlightByPhoto[key] &&
        !_lbQueuedAdjustmentSaveByPhoto[key]
      ) {
        resolve();
        return;
      }
      if (Date.now() - started > 15000) {
        reject(new Error('Timed out waiting for adjustment save'));
        return;
      }
      setTimeout(check, 25);
    }
    check();
  });
}

export function onLightboxAdjustmentInput(/** @type {HTMLInputElement} */ input) {
  if (_lbGuardAdjustmentSource()) return false;
  if (!lbState.editRecipeLoaded) {
    _lbSetAdjustmentStatus('Loading recipe...');
    _lbRenderAdjustmentControls();
    return;
  }
  var label = document.getElementById(input.id + 'Value');
  if (label) label.textContent = _lbFormatAdjustmentValue(input.dataset.adjustment, input.value);
  lbState.adjustSeq += 1;
  _lbBumpAdjustmentInputSeq(vireoLightboxSession.requestedPhotoId());
  var values = _lbReadAdjustmentControls();
  lbState.editRecipe = _lbRecipeWithAdjustments(values);
  _lbApplyAdjustmentPreview(values);
  _lbSetAdjustmentStatus('Previewing...');
  if (lbState.adjustSaveTimer) clearTimeout(lbState.adjustSaveTimer);
  lbState.adjustSaveTimer = setTimeout(function() {
    lbState.adjustSaveTimer = null;
    _lbSaveAdjustmentRecipe(_lbReadAdjustmentControls());
  }, 350);
}

export function resetLightboxAdjustments() {
  if (_lbGuardAdjustmentSource()) return false;
  if (!lbState.editRecipeLoaded) {
    _lbSetAdjustmentStatus('Loading recipe...');
    return;
  }
  _lbSetAdjustmentControl('lbAdjExposure', 0);
  _lbSetAdjustmentControl('lbAdjHighlights', 0);
  _lbSetAdjustmentControl('lbAdjShadows', 0);
  _lbSetAdjustmentControl('lbAdjWhites', 0);
  _lbSetAdjustmentControl('lbAdjBlacks', 0);
  _lbSetAdjustmentControl('lbAdjContrast', 0);
  _lbSetAdjustmentControl('lbAdjTemperature', 0);
  _lbSetAdjustmentControl('lbAdjTint', 0);
  _lbSetAdjustmentControl('lbAdjVibrance', 0);
  _lbSetAdjustmentControl('lbAdjSaturation', 0);
  var values = _lbReadAdjustmentControls();
  lbState.editRecipe = _lbRecipeWithAdjustments(values);
  _lbApplyAdjustmentPreview(values);
  _lbBumpAdjustmentInputSeq(vireoLightboxSession.requestedPhotoId());
  if (lbState.adjustSaveTimer) {
    clearTimeout(lbState.adjustSaveTimer);
    lbState.adjustSaveTimer = null;
  }
  _lbSaveAdjustmentRecipe(values);
}

export function toggleLightboxAdjustPanel() {
  if (_lbGuardAdjustmentSource()) return false;
  var panel = document.getElementById('lightboxAdjustPanel');
  var btn = document.getElementById('lightboxAdjustBtn');
  if (!panel) return;
  var open = !panel.classList.contains('open');
  panel.classList.toggle('open', open);
  if (btn) btn.setAttribute('aria-expanded', open ? 'true' : 'false');
  if (open) {
    _lbRenderAdjustmentControls();
    _lbSetAdjustmentControlsDisabled(!lbState.editRecipeLoaded);
    _lbSetAdjustmentStatus(lbState.editRecipeLoaded ? '' : 'Loading recipe...');
    if (lbState.editRecipeLoaded && !_lbPreviewNeedsServer(lbState.editRecipe) && VireoToneGL.supported()) {
      _lbLoadAdjustmentSource();
    }
  }
}

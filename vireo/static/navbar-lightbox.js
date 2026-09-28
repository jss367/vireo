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

// --- Species Representative panel, shared across every page's lightbox.
// Driven by the `life_list` block on GET /api/photos/<id> (cached in
// _lbPhotoDataByPhoto), so the action works wherever a photo is opened — not
// just on the /life-list page. Each eligible species gets its own row; the
// button reflects real state.
function _lbEnsureLifeListPanel() {
  var actions = document.getElementById('lightboxActions');
  if (!actions) return null;
  var panel = document.getElementById('lifeListLightboxPanel');
  if (!panel) {
    panel = document.createElement('div');
    panel.id = 'lifeListLightboxPanel';
    panel.className = 'lifelist-lb-panel';
    actions.insertBefore(panel, actions.firstChild);
  }
  return panel;
}

function _lbRenderLifeListPanel(photoId) {
  var panel = _lbEnsureLifeListPanel();
  if (!panel) return;
  var data = _lbPhotoDataByPhoto[String(photoId)];
  var entries = (data && data.life_list) || [];
  if (!entries.length) {
    panel.innerHTML = '';
    // Keep an invisible, fixed-size placeholder in the wrapping action row.
    // Otherwise navigating to an ineligible photo (or rejecting the current
    // one) changes the bottom bar's height and makes a fit-to-window image
    // visibly resize after its pixels have already loaded.
    panel.style.visibility = 'hidden';
    panel.setAttribute('aria-hidden', 'true');
    return;
  }
  panel.style.visibility = 'visible';
  panel.setAttribute('aria-hidden', 'false');
  panel.innerHTML = '';
  entries.forEach(function(entry) {
    var row = document.createElement('div');
    row.className = 'lifelist-lb-row';
    var label = document.createElement('span');
    label.className = 'lifelist-lb-species';
    label.textContent = entry.species;
    var btn = document.createElement('button');
    btn.type = 'button';
    if (entry.is_current_photo) {
      btn.className = 'primary';
      btn.textContent = 'Representative';
    } else {
      btn.textContent = 'Set Representative';
    }
    btn.disabled = _lbReadOnly;
    if (_lbReadOnly) btn.title = _lbReadOnlyMessage;
    btn.addEventListener('click', function(event) {
      event.stopPropagation();
      setLifeListPhoto(entry.species, photoId, btn);
    });
    row.appendChild(label);
    row.appendChild(btn);
    panel.appendChild(row);
  });
}

async function setLifeListPhoto(species, photoId, button) {
  if (_lbGuardReadOnly()) return false;
  if (!species || !photoId) return;
  if (button) button.disabled = true;
  try {
    await window.safeFetch('/api/photo-preferences', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ purpose: 'species_representative', species: species, photo_id: photoId }),
    });
    // Re-read this photo's block so the panel shows the honest new state, and
    // refresh the lightbox's cache so re-renders stay consistent.
    try {
      var fresh = await window.safeFetch('/api/photos/' + photoId, {}, { toast: false });
      if (fresh) _lbPhotoDataByPhoto[String(photoId)] = fresh;
    } catch (e) { /* keep the panel usable even if the refresh fails */ }
    if (_lightboxCurrentId === photoId) _lbRenderLifeListPanel(photoId);
    // Let the /life-list page (or any listener) refresh its grid + ribbons.
    document.dispatchEvent(new CustomEvent('lifelist:changed', {
      detail: { species: species, photoId: photoId },
    }));
    if (typeof showToast === 'function') {
      showToast('Species representative set for ' + species, 'success');
    }
  } finally {
    if (button) button.disabled = false;
  }
}
window.setLifeListPhoto = setLifeListPhoto;

function _lifeListEntriesFromPhotoLike(photo) {
  if (!photo) return [];
  if (Array.isArray(photo.life_list)) {
    return photo.life_list.filter(function(entry) {
      return entry && entry.species;
    });
  }
  if (Array.isArray(photo.species)) {
    if (photo.flag === 'rejected') return [];
    return photo.species.filter(Boolean).map(function(species) {
      return { species: species, is_current_photo: false, is_species_representative: false };
    });
  }
  if (typeof photo.species === 'string' && photo.species && photo.flag !== 'rejected') {
    return [{ species: photo.species, is_current_photo: false, is_species_representative: false }];
  }
  return [];
}

function _lifeListMenuEntriesForPhotoId(photoId, opts) {
  opts = opts || {};
  var photo = null;
  if (typeof opts.getPhoto === 'function') {
    photo = opts.getPhoto(photoId);
  }
  if (!photo && opts.photoById) {
    photo = opts.photoById[String(photoId)] || opts.photoById[photoId];
  }
  return _lifeListEntriesFromPhotoLike(photo);
}

async function chooseSpeciesRepresentativeForPhoto(photoId) {
  if (!photoId) return;
  var data;
  try {
    data = await window.safeFetch('/api/photos/' + photoId);
  } catch (err) {
    return;
  }
  var entries = _lifeListEntriesFromPhotoLike(data);
  if (!entries.length) {
    if (typeof showToast === 'function') {
      showToast('No eligible species keyword on this photo', 'warning');
    }
    return;
  }
  var entry = entries[0];
  if (entries.length > 1) {
    var names = entries.map(function(e) { return e.species; });
    var choice = window.prompt('Set as representative for which species?\n' + names.join('\n'), names[0]);
    if (!choice) return;
    entry = entries.find(function(e) { return e.species === choice; }) || { species: choice };
  }
  await setLifeListPhoto(entry.species, photoId);
}
window.chooseSpeciesRepresentativeForPhoto = chooseSpeciesRepresentativeForPhoto;

window.buildSpeciesRepresentativeMenuItems = function(photoIds, opts) {
  opts = opts || {};
  photoIds = (photoIds || []).filter(function(id) { return id != null; });
  if (!photoIds.length) return [];
  if (photoIds.length !== 1) {
    var anyEligible = photoIds.some(function(id) {
      return _lifeListMenuEntriesForPhotoId(id, opts).length > 0;
    });
    if (!anyEligible && !opts.showFetchFallback) return [];
    return [{
      label: 'Set Representative',
      disabled: true,
      disabledHint: 'Select a single photo',
    }];
  }
  var photoId = photoIds[0];
  var entries = _lifeListMenuEntriesForPhotoId(photoId, opts);
  if (entries.length) {
    return entries.map(function(entry) {
      var isCurrent = !!entry.is_current_photo;
      return {
        label: 'Set Representative \u2014 ' + entry.species,
        disabled: isCurrent,
        disabledHint: isCurrent ? 'Already representative' : null,
        onClick: function() { setLifeListPhoto(entry.species, photoId); },
      };
    });
  }
  if (!opts.showFetchFallback) return [];
  return [{
    label: 'Set Representative\u2026',
    onClick: function() { chooseSpeciesRepresentativeForPhoto(photoId); },
  }];
};

function _highlightEntriesFromPhotoLike(photo) {
  if (!photo) return [];
  if (Array.isArray(photo.highlight_list)) {
    return photo.highlight_list.filter(function(entry) {
      return entry && entry.species;
    });
  }
  return [];
}

function _highlightMenuEntriesForPhotoId(photoId, opts) {
  opts = opts || {};
  var photo = null;
  if (typeof opts.getPhoto === 'function') {
    photo = opts.getPhoto(photoId);
  }
  if (!photo && opts.photoById) {
    photo = opts.photoById[String(photoId)] || opts.photoById[photoId];
  }
  return _highlightEntriesFromPhotoLike(photo);
}

async function setSpeciesHighlightFromMenu(species, photoId) {
  if (!species || !photoId) return;
  await window.safeFetch('/api/species-highlights', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ species: species, photo_id: photoId }),
  });
  try {
    var fresh = await window.safeFetch('/api/photos/' + photoId, {}, { toast: false });
    if (fresh && typeof _lbPhotoDataByPhoto !== 'undefined') {
      _lbPhotoDataByPhoto[String(photoId)] = fresh;
    }
  } catch (e) {}
  document.dispatchEvent(new CustomEvent('highlights:changed', {
    detail: { species: species, photoId: photoId },
  }));
  if (typeof window.loadHighlights === 'function') {
    await window.loadHighlights();
    if (typeof window.updateHighlightsLightboxControls === 'function') {
      window.updateHighlightsLightboxControls(photoId);
    }
  }
  if (typeof showToast === 'function') {
    showToast('Added to Highlights', 'success');
  }
}
window.setSpeciesHighlightFromMenu = setSpeciesHighlightFromMenu;

async function chooseSpeciesHighlightForPhoto(photoId) {
  if (!photoId) return;
  var data;
  try {
    data = await window.safeFetch('/api/photos/' + photoId);
  } catch (err) {
    return;
  }
  var entries = _highlightEntriesFromPhotoLike(data);
  if (!entries.length) {
    if (typeof showToast === 'function') {
      showToast('No eligible highlight species on this photo', 'warning');
    }
    return;
  }
  var entry = entries[0];
  if (entries.length > 1) {
    var names = entries.map(function(e) { return e.species; });
    var choice = window.prompt('Add as a highlight for which species?\n' + names.join('\n'), names[0]);
    if (!choice) return;
    entry = entries.find(function(e) { return e.species === choice; }) || { species: choice };
  }
  await setSpeciesHighlightFromMenu(entry.species, photoId);
}
window.chooseSpeciesHighlightForPhoto = chooseSpeciesHighlightForPhoto;

window.buildSpeciesHighlightMenuItems = function(photoIds, opts) {
  opts = opts || {};
  photoIds = (photoIds || []).filter(function(id) { return id != null; });
  if (!photoIds.length) return [];
  if (photoIds.length !== 1) {
    return [{
      label: 'Add to Highlights',
      disabled: true,
      disabledHint: 'Select a single photo',
    }];
  }
  var photoId = photoIds[0];
  var entries = _highlightMenuEntriesForPhotoId(photoId, opts);
  if (entries.length) {
    return entries.map(function(entry) {
      return {
        label: (entry.is_highlighted ? 'Highlighted' : 'Add to Highlights') + ' \u2014 ' + entry.species,
        disabled: !!entry.is_highlighted,
        disabledHint: entry.is_highlighted ? 'Already highlighted' : undefined,
        onClick: function() { setSpeciesHighlightFromMenu(entry.species, photoId); },
      };
    });
  }
  if (!opts.showFetchFallback) return [];
  return [{
    label: 'Add to Highlights\u2026',
    onClick: function() { chooseSpeciesHighlightForPhoto(photoId); },
  }];
};

document.addEventListener('lightbox:photochanged', function(event) {
  var pid = event.detail ? event.detail.photoId : null;
  // Render from cache now (instant when revisiting a loaded photo); the panel
  // re-renders again once the fresh /api/photos fetch resolves.
  if (pid != null) _lbRenderLifeListPanel(pid);
});

// A photo's flag decides representative eligibility (a rejected photo can't be
// a representative, per _photo_can_be_life_list_preference). The cached
// `life_list` block goes stale the moment the user changes the flag from the
// lightbox, so re-fetch it and re-render — this hides the panel when the photo
// becomes rejected and re-shows it when the flag is cleared again.
document.addEventListener('lightbox:flagchanged', async function(event) {
  var pid = event.detail ? event.detail.photoId : null;
  if (pid == null || _lightboxCurrentId !== pid) return;
  try {
    var fresh = await window.safeFetch('/api/photos/' + pid, {}, { toast: false });
    // Guard against a stale fetch clobbering the panel after navigation.
    if (!fresh || _lightboxCurrentId !== pid) return;
    _lbPhotoDataByPhoto[String(pid)] = fresh;
    _lbRenderLifeListPanel(pid);
  } catch (e) { /* leave the last-rendered panel in place if the refresh fails */ }
});

var _lbEditRecipe = null;
var _lbEditRecipeLoaded = false;
var _lbAdjustmentSource = null;
var _lbAdjustmentPreviewSeq = 0;
var _lbAdjustmentPreviewTimer = null;
var _lbAdjustSaveTimer = null;
var _lbAdjustSeq = 0;
var _lbAdjustmentInputSeq = 0;
var _lbAdjustmentInputSeqByPhoto = {};
var _lbAdjustSaveInFlightByPhoto = {};
var _lbQueuedAdjustmentSaveByPhoto = {};
var _lbRecipeBustByPhotoId = {};
var _lbEditVersionByPhoto = {};
var _lbEditWritePending = false;
var _cropRecipe = null;
var _cropPhotoId = null;
var _cropPreviewSeq = 0;
var _cropSessionSeq = 0;
var _cropDrag = null;
var _cropPreviewTimer = null;
var _cropEscToken = null;

function _vireoUrlWithQueryParam(url, key, value) {
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

function _vireoUrlWithRenderVersion(url, photoId) {
  if (!url || photoId == null) return url;
  var version = _lbRenderVersionByPhoto[String(photoId)];
  if (!version) return url;
  return _vireoUrlWithQueryParam(url, 'rv', version);
}

function _vireoUrlWithPhotoSource(url, source) {
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

function _vireoCleanRenderSearch(search) {
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
    return search
      .replace(/([?&])(rv|er|editv|v)=[^&]*&?/g, '$1')
      .replace(/\?&/, '?')
      .replace(/[?&]$/, '');
  }
}

function _vireoBaseRenderedUrl(src) {
  if (!src) return '';
  try {
    var parsed = new URL(src, window.location.origin);
    return parsed.pathname + _vireoCleanRenderSearch(parsed.search);
  } catch (_) {
    return src
      .replace(/([?&])(rv|er|editv|v)=[^&]*&?/g, '$1')
      .replace(/\?&/, '?')
      .replace(/[?&]$/, '');
  }
}

function _vireoBumpRenderVersion(photoId) {
  if (photoId == null) return null;
  var version = Date.now() + '-' + Math.floor(Math.random() * 1000000);
  _lbRenderVersionByPhoto[String(photoId)] = version;
  return version;
}

// A RAW+JPEG pair stays one logical photo, but the user can choose which
// physical file supplies the displayed pixels. A newly discovered pair starts
// on JPEG because that file is commonly the photographer's developed result.
var _vireoPairKnownByPhoto = {};
var _vireoPairSourceByPhoto = {};
var _vireoPairPendingSourceByPhoto = {};

window.vireoPhotoIsRawJpegPair = function(photo) {
  if (!photo || !photo.companion_path) return false;
  var rawExtensions = {
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
  var primaryMatch = String(primaryName).toLowerCase().match(/(\.[^.\/\\]+)$/);
  var primaryExt = primaryMatch ? primaryMatch[1] : '';
  if (!primaryExt) {
    primaryExt = String(photo.extension || '').toLowerCase();
    if (primaryExt && primaryExt.charAt(0) !== '.') primaryExt = '.' + primaryExt;
  }
  var companionMatch = String(photo.companion_path).toLowerCase().match(/(\.[^.\/\\]+)$/);
  var companionExt = companionMatch ? companionMatch[1] : '';
  return !!rawExtensions[primaryExt] && (companionExt === '.jpg' || companionExt === '.jpeg');
};

window.vireoRememberPhotoPair = function(photo) {
  if (!photo || photo.id == null) return;
  var key = String(photo.id);
  if (window.vireoPhotoIsRawJpegPair(photo)) {
    _vireoPairKnownByPhoto[key] = true;
    if (!_vireoPairSourceByPhoto[key]) _vireoPairSourceByPhoto[key] = 'jpeg';
  } else if (Object.prototype.hasOwnProperty.call(photo, 'companion_path')) {
    delete _vireoPairKnownByPhoto[key];
    delete _vireoPairSourceByPhoto[key];
  }
};

function _vireoPairSource(photoId) {
  return _vireoPairSourceByPhoto[String(photoId)] || null;
}

window.vireoUpdatePairSourceControls = function(photoId) {
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

  var lightboxControl = document.getElementById('lightboxSourceControl');
  if (lightboxControl && String(_lightboxCurrentId) === key) {
    lightboxControl.style.display = paired ? '' : 'none';
    lightboxControl.textContent = text;
    lightboxControl.disabled = !!pending;
    _lbApplyReadOnlyState();
    _lbSetAdjustmentControlsDisabled(!_lbEditRecipeLoaded);
    if (_lbAdjustmentSourceHint()) _lbClearAdjustmentPreview();
  }
  var detailControl = document.getElementById('detailSourceControl');
  if (detailControl && String(window._detailPhotoId) === key) {
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
};

// Queued grid images display blob URLs; retain the canonical URL for edits
// and RAW/JPEG switches, and send replacements through the same bounded queue.
function _vireoRenderedImageSource(img) {
  return img.getAttribute('data-thumbnail-src') || img.getAttribute('src') || '';
}

function _vireoSetRenderedImageSource(img, url) {
  if (img.hasAttribute('data-thumbnail-src')) img.setAttribute('data-thumbnail-src', url);
  else img.src = url;
}

function _vireoPairAnchorImage(photoId) {
  var key = String(photoId);
  if (String(_lightboxCurrentId) === key) {
    return document.getElementById('lightboxImg');
  }
  if (String(window._detailPhotoId) === key) {
    return document.getElementById('detailImg');
  }
  return document.querySelector('.grid-card[data-id="' + key + '"] img');
}

function _vireoPairAnchorStillCurrent(anchor, photoId) {
  if (!anchor) return false;
  var key = String(photoId);
  if (anchor.id === 'lightboxImg') return String(_lightboxCurrentId) === key;
  if (anchor.id === 'detailImg') return String(window._detailPhotoId) === key;
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

function _vireoCancelPairSourceChange(photoId, requested) {
  var key = String(photoId);
  if (_vireoPairPendingSourceByPhoto[key] !== requested) return;
  delete _vireoPairPendingSourceByPhoto[key];
  window.vireoUpdatePairSourceControls(photoId);
}

function _vireoPairSourceLoadFailed(photoId, requested, anchor, oldSrc) {
  var key = String(photoId);
  if (_vireoPairPendingSourceByPhoto[key] !== requested) return;
  var anchorStillCurrent = (
    anchor && anchor.isConnected &&
    _vireoPairAnchorStillCurrent(anchor, photoId)
  );
  delete _vireoPairPendingSourceByPhoto[key];
  window.vireoUpdatePairSourceControls(photoId);
  if (anchorStillCurrent && oldSrc) {
    // A RAW edit may have finished saving while the target was loading.
    // Restore the selected source with its current recipe/render version,
    // rather than the stale URL captured before that save.
    _vireoSetRenderedImageSource(anchor, window.vireoRenderedUrl(
      _vireoBaseRenderedUrl(oldSrc), photoId
    ));
  }
  if (anchorStillCurrent && typeof showToast === 'function') {
    showToast('Could not load the ' + requested.toUpperCase() + ' source', 'error');
  }
}

function _vireoPairSourceImageLoaded(photoId, requested, anchor) {
  var key = String(photoId);
  if (
    !anchor || anchor.id !== 'lightboxImg' ||
    String(_lightboxCurrentId) !== key
  ) return;
  _lbFullLongEdge = _lbCurrentSrcKey === 'full'
    ? (Math.max(anchor.naturalWidth || 0, anchor.naturalHeight || 0) || null)
    : null;
  _lbNativeZoom = null;
  _lbRecomputeNativeZoom();
  _lbApplyTransform();
  if (requested === 'raw') {
    _lbLoadDetections(photoId);
    _lbRenderEyeCrosshair(_lbPhotoDataByPhoto[key]);
    _lbLoadMaskVariants(photoId);
  }
  _lbApplyBoxesVisibility();
  _lbApplyEyeVisibility();
  _lbApplyMaskVisibility();
}

function _vireoCommitPairSource(photoId, requested, anchor) {
  var key = String(photoId);
  if (_vireoPairPendingSourceByPhoto[key] !== requested) return;
  _vireoPairSourceByPhoto[key] = requested;
  delete _vireoPairPendingSourceByPhoto[key];
  window.vireoUpdatePairSourceControls(photoId);
  _vireoPairSourceImageLoaded(photoId, requested, anchor);
  if (typeof window.vireoRefreshPhotoRenders === 'function') {
    window.vireoRefreshPhotoRenders([photoId]);
  }
}

window.vireoTogglePairSource = function(photoId) {
  if (photoId == null || !_vireoPairKnownByPhoto[String(photoId)]) return;
  var key = String(photoId);
  if (_vireoPairPendingSourceByPhoto[key]) return;
  var requested = _vireoPairSourceByPhoto[key] === 'raw'
    ? 'jpeg'
    : 'raw';
  // Commit input made on the RAW before source switching disables controls.
  if (String(_lightboxCurrentId) === key) _lbFlushPendingAdjustmentSave();
  var anchor = _vireoPairAnchorImage(photoId);
  var oldSrc = anchor ? _vireoRenderedImageSource(anchor) : '';
  var base = oldSrc ? _vireoBaseRenderedUrl(oldSrc) : '/thumbnails/' + key + '.jpg';
  _vireoBumpRenderVersion(photoId);
  var targetUrl = window.vireoRenderedUrl(base, photoId, requested);
  _vireoPairPendingSourceByPhoto[key] = requested;
  window.vireoUpdatePairSourceControls(photoId);

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
    function cleanup() {
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
      _vireoSetRenderedImageSource(anchor, oldSrc);
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
};

window.vireoRefreshPhotoRenders = function(photoIds) {
  if (!Array.isArray(photoIds)) photoIds = [photoIds];
  var idSet = {};
  photoIds.forEach(function(id) {
    if (id == null) return;
    idSet[String(id)] = true;
  });
  if (!Object.keys(idSet).length) return;
  var imgs = document.querySelectorAll('img[src], img[data-thumbnail-src]');
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
        _vireoSetRenderedImageSource(img, window.vireoRenderedUrl
          ? window.vireoRenderedUrl(base, id)
          : _vireoUrlWithRenderVersion(base, id));
      }
    });
  });
  try {
    document.dispatchEvent(new CustomEvent('lightbox:renderchanged', {
      detail: { photoIds: Object.keys(idSet).map(function(id) { return parseInt(id, 10); }) },
    }));
  } catch (_) {}
};

window.vireoRenderedUrl = function(url, photoId, sourceOverride) {
  var source = sourceOverride || (photoId == null ? null : _vireoPairSource(photoId));
  if (source) url = _vireoUrlWithPhotoSource(url, source);
  var recipeKey = _vireoPhotoRenderKey(photoId);
  if (recipeKey) url = _vireoUrlWithQueryParam(url, 'er', recipeKey);
  return _vireoUrlWithRenderVersion(url, photoId);
};

// The `er` fingerprint for a photo: the server's key when the payload carried
// one, otherwise a fingerprint of the recipe we hold. Both exist because they
// go stale at different moments — the server key is authoritative on page load
// and after a refetch, while a recipe this page just wrote is newer than any
// key the last payload carried.
function _vireoPhotoRenderKey(photoId) {
  if (photoId == null) return '';
  var serverKey = _lbRenderKeyByPhoto[String(photoId)];
  if (serverKey) return serverKey;
  var recipe = _lbRawEditRecipeByPhoto[String(photoId)];
  if (recipe === undefined) recipe = _lbEditRecipeByPhoto[String(photoId)];
  return _vireoEditRecipeCacheKey(recipe);
}

window.vireoThumbnailUrl = function(photoOrId) {
  var photo = photoOrId && typeof photoOrId === 'object' ? photoOrId : null;
  var photoId = photo ? (photo.photo_id != null ? photo.photo_id : photo.id) : photoOrId;
  if (photoId == null) return '';
  if (photo && typeof window.vireoRememberPhotoPair === 'function') {
    window.vireoRememberPhotoPair(photo);
  }
  if (
    photo &&
    Object.prototype.hasOwnProperty.call(photo, 'edit_recipe') &&
    typeof photo.edit_recipe !== 'undefined' &&
    typeof window.vireoRememberPhotoEditRecipe === 'function'
  ) {
    window.vireoRememberPhotoEditRecipe(photoId, photo.edit_recipe, {
      skipIfLocallyWritten: true
    });
  }
  if (
    photo &&
    Object.prototype.hasOwnProperty.call(photo, 'render_key') &&
    typeof window.vireoRememberPhotoRenderKey === 'function'
  ) {
    window.vireoRememberPhotoRenderKey(photoId, photo.render_key, {
      skipIfLocallyWritten: true
    });
  }
  var url = '/thumbnails/' + photoId + '.jpg';
  return window.vireoRenderedUrl
    ? window.vireoRenderedUrl(url, photoId)
    : _vireoUrlWithRenderVersion(url, photoId);
};

function vireoThumbnailUrl(photoOrId) {
  return window.vireoThumbnailUrl(photoOrId);
}

function vireoCacheBustedPhotoUrl(photoId, url) {
  url = String(url || '')
    .replace(/([?&])editv=[^&]*(&?)/, function(match, prefix, suffix) {
      return suffix ? prefix : '';
    })
    .replace(/([?&])v=[^&]*(&?)/, function(match, prefix, suffix) {
      return suffix ? prefix : '';
    })
    .replace(/[?&]$/, '');
  url = window.vireoRenderedUrl
    ? window.vireoRenderedUrl(url, photoId)
    : _vireoUrlWithRenderVersion(url, photoId);
  var version = _lbEditVersionByPhoto[String(photoId)];
  if (version) url = _vireoUrlWithQueryParam(url, 'editv', version);
  var bust = _lbRecipeBustByPhotoId[String(photoId)];
  if (bust) url = _vireoUrlWithQueryParam(url, 'v', bust);
  return url;
}

function vireoPreviewUrl(photoId, size) {
  return vireoCacheBustedPhotoUrl(photoId, '/photos/' + photoId + '/preview?size=' + (size || 1920));
}

window.vireoPreviewUrl = vireoPreviewUrl;
window.vireoCacheBustedPhotoUrl = vireoCacheBustedPhotoUrl;

function _lbCloneEditRecipe(recipe) {
  if (!recipe || typeof recipe !== 'object') return {};
  var out = {};
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

function _vireoHashString(s) {
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
var _VIREO_EDIT_MATH_VERSION = 7;

// Stable stringify: sort object keys at every depth so a fingerprint tracks
// the recipe's *content*. Two payloads can serialize the same recipe with
// different property order, and JSON.stringify would hash them differently —
// which costs a needless refetch of an image that did not change.
function _vireoCanonicalRecipeJson(value) {
  if (Array.isArray(value)) {
    return '[' + value.map(_vireoCanonicalRecipeJson).join(',') + ']';
  }
  if (value && typeof value === 'object') {
    return '{' + Object.keys(value).sort().map(function(key) {
      return JSON.stringify(key) + ':' + _vireoCanonicalRecipeJson(value[key]);
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
function _vireoEditRecipeCacheKey(recipe) {
  if (!_lbRecipeHasEdits(recipe)) return '';
  return _vireoHashString(_vireoCanonicalRecipeJson(recipe))
    + '.m' + _VIREO_EDIT_MATH_VERSION;
}

function _lbRecipeHasEdits(recipe) {
  return !!(recipe && typeof recipe === 'object' && Object.keys(recipe).some(function(k) {
    return k !== 'version';
  }));
}

function _lbRecipeHasOrientation(recipe) {
  return !!(
    recipe &&
    typeof recipe === 'object' &&
    (recipe.rotation || (recipe.flip && (recipe.flip.horizontal || recipe.flip.vertical)))
  );
}

function _lbCurrentRecipeHasOrientation() {
  if (_lightboxCurrentId == null) return false;
  return _lbRecipeHasOrientation(_lbEditRecipeByPhoto[String(_lightboxCurrentId)]);
}

window.vireoPhotoHasOrientationEdit = function(photoId) {
  if (photoId == null) return false;
  return _lbRecipeHasGeometricEdit(_lbEditRecipeByPhoto[String(photoId)]);
};

window.vireoRememberPhotoEditRecipe = function(photoId, recipe, options) {
  if (options && options.skipIfLocallyWritten && _lbEditRecipeWriteSeqFor(photoId)) return;
  _lbRememberEditRecipe(photoId, recipe);
};

// Record the server's render key for a photo. Call this *after* remembering
// the recipe it belongs to: ``_lbRememberEditRecipe`` drops any key it holds,
// because every other caller of it is a local write whose recipe is newer than
// the key the last payload carried.
window.vireoRememberPhotoRenderKey = function(photoId, renderKey, options) {
  if (photoId == null) return;
  if (options && options.skipIfLocallyWritten && _lbEditRecipeWriteSeqFor(photoId)) return;
  if (renderKey) _lbRenderKeyByPhoto[String(photoId)] = String(renderKey);
  else delete _lbRenderKeyByPhoto[String(photoId)];
};

window.vireoPhotoEditRecipe = function(photoId) {
  if (photoId == null) return {};
  return _lbCloneEditRecipe(_lbEditRecipeByPhoto[String(photoId)]);
};

function _lbRememberEditRecipe(photoId, recipe, preserveAdjustmentInput) {
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
  var p = _lightboxPhotoList.find(function(x) { return x.id === numericId; });
  if (p) p.edit_recipe = currentRecipe;
  if (_lightboxCurrentId === numericId) {
    _lbCurrentEditRecipe = currentRecipe;
    if (!preserveAdjustmentInput) {
      _lbEditRecipe = _lbCloneRecipe(storedRecipe);
      _lbEditRecipeLoaded = true;
      _lbRenderAdjustmentControls();
      _lbSetAdjustmentControlsDisabled(false);
    }
  }
  _lbApplyEditButtonState();
}

function _lbEditRecipeWriteSeqFor(photoId) {
  if (photoId == null) return 0;
  return _lbEditRecipeWriteSeqByPhoto[String(photoId)] || 0;
}

function _lbMarkEditRecipeWrite(photoId) {
  if (photoId == null) return;
  _lbEditRecipeWriteSeq += 1;
  _lbEditRecipeWriteSeqByPhoto[String(photoId)] = _lbEditRecipeWriteSeq;
}

function _lbCurrentEditRecipeClone() {
  if (_lightboxCurrentId == null) return {};
  return _lbCloneEditRecipe(_lbEditRecipeByPhoto[String(_lightboxCurrentId)]);
}

function _lbNormalizeClientRecipe(recipe) {
  var out = _lbCloneEditRecipe(recipe);
  if (!out.rotation) delete out.rotation;
  if (Math.abs(Number(out.straighten || 0)) < 0.0001) delete out.straighten;
  else out.straighten = Math.round(Number(out.straighten) * 10000) / 10000;
  if (out.flip) {
    if (!out.flip.horizontal) delete out.flip.horizontal;
    if (!out.flip.vertical) delete out.flip.vertical;
    if (!Object.keys(out.flip).length) delete out.flip;
  }
  return _lbRecipeHasEdits(out) ? out : null;
}

function _lbRecipeAfterEditOperation(recipe, op) {
  var next = _lbCloneEditRecipe(recipe);
  if (op === 'reset') {
    var removedFlip = next.flip || {};
    if (next.crop && typeof next.crop === 'object') {
      next.crop = _lbTransformBoxByRecipe(
        next.crop,
        _lbInverseOrientationRecipe(next)
      );
    }
    if (
      (!!removedFlip.horizontal !== !!removedFlip.vertical) &&
      Math.abs(Number(next.straighten || 0)) > 0.0001
    ) {
      next.straighten = -Number(next.straighten);
    }
    delete next.rotation;
    delete next.flip;
    return _lbNormalizeClientRecipe(next);
  }
  var orientation = _lbComposeDisplayedOrientation(next, op);
  if (next.crop && typeof next.crop === 'object') {
    next.crop = _lbTransformBoxByEditOperation(next.crop, op);
  }
  if (
    (op === 'flip-horizontal' || op === 'flip-vertical') &&
    Math.abs(Number(next.straighten || 0)) > 0.0001
  ) {
    next.straighten = -Number(next.straighten);
  }
  delete next.rotation;
  delete next.flip;
  if (orientation.rotation) next.rotation = orientation.rotation;
  if (orientation.flip) next.flip = orientation.flip;
  return _lbNormalizeClientRecipe(next);
}

function _lbApplyDisplayedOrientationOperation(point, op) {
  var x = point.x;
  var y = point.y;
  if (op === 'rotate-right') return { x: 1 - y, y: x };
  if (op === 'rotate-left') return { x: y, y: 1 - x };
  if (op === 'flip-horizontal') return { x: 1 - x, y: y };
  if (op === 'flip-vertical') return { x: x, y: 1 - y };
  return { x: x, y: y };
}

function _lbTransformBoxByEditOperation(box, op) {
  var opRecipe = null;
  if (op === 'rotate-right') opRecipe = { rotation: 90 };
  else if (op === 'rotate-left') opRecipe = { rotation: 270 };
  else if (op === 'flip-horizontal') opRecipe = { flip: { horizontal: true } };
  else if (op === 'flip-vertical') opRecipe = { flip: { vertical: true } };
  if (!opRecipe) return box;
  return _lbTransformBoxByRecipe(box, opRecipe);
}

function _lbOrientationCandidates() {
  var candidates = [];
  [0, 90, 180, 270].forEach(function(rotation) {
    [false, true].forEach(function(horizontal) {
      [false, true].forEach(function(vertical) {
        var candidate = {};
        if (rotation) candidate.rotation = rotation;
        if (horizontal || vertical) {
          candidate.flip = {};
          if (horizontal) candidate.flip.horizontal = true;
          if (vertical) candidate.flip.vertical = true;
        }
        candidates.push(candidate);
      });
    });
  });
  return candidates;
}

function _lbComposeDisplayedOrientation(recipe, op) {
  var samplePoints = [
    { x: 0.125, y: 0.25 },
    { x: 0.8, y: 0.2 },
    { x: 0.3, y: 0.85 },
  ];
  var desired = samplePoints.map(function(point) {
    return _lbApplyDisplayedOrientationOperation(
      _lbTransformPointByRecipe(point.x, point.y, recipe),
      op
    );
  });
  var candidates = _lbOrientationCandidates();
  for (var i = 0; i < candidates.length; i++) {
    var candidate = candidates[i];
    var matches = true;
    for (var j = 0; j < samplePoints.length; j++) {
      var actual = _lbTransformPointByRecipe(
        samplePoints[j].x,
        samplePoints[j].y,
        candidate
      );
      if (
        Math.abs(actual.x - desired[j].x) > 0.000001 ||
        Math.abs(actual.y - desired[j].y) > 0.000001
      ) {
        matches = false;
        break;
      }
    }
    if (matches) return candidate;
  }
  return {};
}

function _lbInverseOrientationRecipe(recipe) {
  var samplePoints = [
    { x: 0.125, y: 0.25 },
    { x: 0.8, y: 0.2 },
    { x: 0.3, y: 0.85 },
  ];
  var candidates = _lbOrientationCandidates();
  for (var i = 0; i < candidates.length; i++) {
    var candidate = candidates[i];
    var matches = true;
    for (var j = 0; j < samplePoints.length; j++) {
      var original = samplePoints[j];
      var oriented = _lbTransformPointByRecipe(original.x, original.y, recipe);
      var restored = _lbTransformPointByRecipe(oriented.x, oriented.y, candidate);
      if (
        Math.abs(restored.x - original.x) > 0.000001 ||
        Math.abs(restored.y - original.y) > 0.000001
      ) {
        matches = false;
        break;
      }
    }
    if (matches) return candidate;
  }
  return {};
}

function _lbApplyEditButtonState() {
  var recipe = _lbCurrentEditRecipeClone();
  var hasOrientation = _lbRecipeHasOrientation(recipe);
  var known = _lightboxCurrentId != null && !!_lbEditRecipeKnownByPhoto[String(_lightboxCurrentId)];
  var ids = [
    'lightboxRotateLeft',
    'lightboxRotateRight',
    'lightboxFlipHorizontal',
    'lightboxFlipVertical',
    'lightboxResetEdit',
  ];
  ids.forEach(function(id) {
    var btn = document.getElementById(id);
    if (!btn) return;
    btn.disabled = _lbEditWritePending || _lightboxCurrentId == null || !known || (id === 'lightboxResetEdit' && !hasOrientation);
  });
}

function _lbSetEditBusy(busy) {
  _lbEditWritePending = !!busy;
  _lbApplyEditButtonState();
}

function _lbReloadCurrentRenderAfterEdit(photoId) {
  if (_lightboxCurrentId !== photoId) return;
  _lbClearAdjustmentPreview();
  _lbCancelOriginalPreload();
  var img = document.getElementById('lightboxImg');
  var wrap = document.getElementById('lightboxWrap');
  if (!img) return;
  _lbZoom = 1.0;
  _lbPanX = 0;
  _lbPanY = 0;
  _lbNativeZoom = null;
  _lbCurrentSrcKey = 'full';
  _lbFullLongEdge = null;
  _lbPending1To1 = false;
  _lbPending1To1Anchor = null;
  _lbPendingViewportState = null;
  _lbOriginalUnavailable = false;
  if (_lbSwapTimer) {
    clearTimeout(_lbSwapTimer);
    _lbSwapTimer = null;
  }
  _lbDesiredSrcKey = null;
  _lbProgressiveTargetKey = null;
  // Cancelling _lbSwapTimer and clearing the desired key makes the old
  // preloader callbacks return as stale, so nothing else will ever turn this
  // off. Left set, the phase stays 'sharpening' with no request behind it.
  _lbSetPreviewLoading(false);
  if (wrap) wrap.classList.remove('zoomed');
  _lbApplyTransform();
  // Replacing img.onload/onerror orphans handleInitialImageLoad when the
  // metadata fetch beats the initial image, so this reload inherits the job of
  // ending that photo's initial load -- otherwise nothing ever clears the
  // pending decode and the chip stays on Loading indefinitely. The flag is
  // cleared on completion, not here: the reload is itself a load in flight.
  img.onload = function() {
    img.onload = null;
    img.onerror = null;
    if (_lightboxCurrentId !== photoId) return;
    _lbInitialDecodePending = false;
    if (img.naturalWidth) {
      _lbFullLongEdge = Math.max(img.naturalWidth, img.naturalHeight);
    }
    _lbRecomputeNativeZoom();
    _lbApplyTransform();
    // Settle the load this reload displaced. It schedules the neighbours and
    // renders the status itself, so only do that work when there was nothing
    // pending (a reload from the adjustments panel, long after the open).
    if (!_lbFinishInitialLoad()) {
      _lbRenderDetailStatus();
      _lbScheduleOriginalPreload(photoId);
    }
  };
  img.onerror = function() {
    img.onload = null;
    img.onerror = null;
    if (_lightboxCurrentId !== photoId) return;
    _lbInitialDecodePending = false;
    // Nothing is going to render. Hand the displaced load its failure path,
    // which commits the incoming photo's identity before bringing the controls
    // back -- unfreezing without that hands the user live controls against the
    // outgoing photo's filename and counter.
    if (!_lbAbandonInitialLoad()) _lbRenderDetailStatus();
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

async function lightboxApplyEdit(op) {
  if (_lightboxCurrentId == null || _lbEditWritePending) return;
  var photoId = _lightboxCurrentId;
  _lbSetEditBusy(true);
  try {
    _lbFlushPendingAdjustmentSave();
    await _lbWaitForAdjustmentSaveIdle(photoId);
    if (_lightboxCurrentId !== photoId) return;
    var baseRecipe = _lbEditRecipeLoaded ? _lbCloneEditRecipe(_lbEditRecipe) : _lbCurrentEditRecipeClone();
    var nextRecipe = _lbRecipeAfterEditOperation(baseRecipe, op);
    var data;
    if (nextRecipe) {
      data = await safeFetch('/api/photos/' + photoId + '/edit-recipe', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ recipe: nextRecipe }),
      }, { toast: false });
    } else {
      data = await safeFetch('/api/photos/' + photoId + '/edit-recipe', {
        method: 'DELETE',
      }, { toast: false });
    }
    _lbMarkEditRecipeWrite(photoId);
    _lbRememberEditRecipe(photoId, data ? data.recipe : null);
    _vireoBumpRenderVersion(photoId);
    if (typeof window.vireoRefreshPhotoRenders === 'function') {
      window.vireoRefreshPhotoRenders([photoId]);
    }
    if (_lightboxCurrentId === photoId) {
      _lbReloadCurrentRenderAfterEdit(photoId);
      if (typeof showToast === 'function') showToast('Updated photo edit', 'success');
    }
  } catch (e) {
    if (typeof showToast === 'function') showToast(e.message || 'Could not update photo edit', 'error');
  } finally {
    _lbSetEditBusy(false);
  }
}

function _lbTransformPointByRecipe(x, y, recipe) {
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

function _lbTransformBoxByRecipe(box, recipe) {
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

function _lbCloneRecipe(recipe) {
  if (!recipe) return {};
  try {
    return JSON.parse(JSON.stringify(recipe));
  } catch (_) {
    return {};
  }
}

function _lbAdjustmentInputSeqFor(photoId) {
  return _lbAdjustmentInputSeqByPhoto[String(photoId)] || 0;
}

function _lbBumpAdjustmentInputSeq(photoId) {
  var key = String(photoId);
  _lbAdjustmentInputSeq += 1;
  _lbAdjustmentInputSeqByPhoto[key] = (_lbAdjustmentInputSeqByPhoto[key] || 0) + 1;
  return _lbAdjustmentInputSeqByPhoto[key];
}

function _lbAdjustmentValues(recipe) {
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

function _lbSetAdjustmentStatus(text, isError) {
  var el = document.getElementById('lightboxAdjustStatus');
  if (!el) return;
  el.textContent = text || '';
  el.classList.toggle('error', !!isError);
}

function _lbFormatAdjustmentValue(name, value) {
  if (name === 'exposure') return Number(value || 0).toFixed(1);
  return String(Math.round(Number(value || 0)));
}

function _lbSetAdjustmentControl(id, value) {
  var input = document.getElementById(id);
  var label = document.getElementById(id + 'Value');
  if (input) input.value = String(value || 0);
  if (label && input) {
    label.textContent = _lbFormatAdjustmentValue(input.dataset.adjustment, value);
  }
}

function _lbSetAdjustmentControlsDisabled(disabled) {
  disabled = !!disabled || _lbReadOnly || !!_lbAdjustmentSourceHint();
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
    var input = document.getElementById(id);
    if (input) input.disabled = !!disabled;
  });
  var reset = document.querySelector('#lightboxAdjustPanel .lb-adjust-reset');
  if (reset) reset.disabled = !!disabled;
}

function _lbAdjustmentSourceHint() {
  if (_lightboxCurrentId == null) return '';
  if (_vireoPairPendingSourceByPhoto[String(_lightboxCurrentId)]) {
    return 'Wait for the photo source to finish loading';
  }
  // A developed companion JPEG is displayed as-authored; recipes belong to
  // the primary RAW and may use entirely different image coordinates.
  return _vireoPairSource(_lightboxCurrentId) === 'jpeg'
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

function _lbRenderAdjustmentControls() {
  var values = _lbAdjustmentValues(_lbEditRecipe);
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
  function val(id) {
    var el = document.getElementById(id);
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

function _lbRecipeWithAdjustments(values) {
  var recipe = _lbCloneRecipe(_lbEditRecipe);
  delete recipe.version;
  var adj = _lbCloneRecipe(recipe.adjustments || {});
  ['exposure', 'highlights', 'shadows', 'whites', 'blacks', 'contrast', 'vibrance', 'saturation'].forEach(function(key) {
    var value = Number(values[key] || 0);
    if (Math.abs(value) > 0.000001) adj[key] = value;
    else delete adj[key];
  });

  var wb = _lbCloneRecipe(adj.white_balance || {});
  delete adj.temperature;
  delete adj.tint;
  ['temperature', 'tint'].forEach(function(key) {
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
function _lbWhiteBalanceGains(temperature, tint) {
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
var VireoToneGL = (function() {
  var KNEE = 0.85; // == tone.HIGHLIGHT_KNEE
  var canvas = null, gl = null, prog = null, tex = null, locs = null;
  var texKey = null, failed = false, maxTextureSize = 0;

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
    'uniform float uHighlights;',
    'uniform float uShadows;',
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
    'float shadowLevelCurve(float x, float amount){',
    '  float a = amount / 100.0;',
    '  float positive = max(a, 0.0);',
    '  float negative = max(-a, 0.0);',
    '  float t = clamp(x / 0.65, 0.0, 1.0);',
    '  float basis = t * pow(1.0 - t, 3.0);',
    '  float delta = 0.65 * basis * (3.0 * positive - 0.85 * negative);',
    '  return x < 0.65 ? x + delta : x;',
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
    '  level = shadowLevelCurve(level, uShadows);',
    '  level = 1.0 - shadowLevelCurve(1.0 - level, -uHighlights);',
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
    '  if (abs(uShadows) + abs(uHighlights) + abs(uBlacks) + abs(uWhites) > 0.000001) {',
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

  function compile(type, src) {
    var s = gl.createShader(type);
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
    canvas = document.getElementById('lightboxToneCanvas');
    if (!canvas) { failed = true; return false; }
    try {
      var opts = { premultipliedAlpha: false, preserveDrawingBuffer: false };
      gl = canvas.getContext('webgl', opts) || canvas.getContext('experimental-webgl', opts);
    } catch (e) { gl = null; }
    if (!gl) { failed = true; return false; }
    var vs = compile(gl.VERTEX_SHADER, VERT);
    var fs = compile(gl.FRAGMENT_SHADER, FRAG);
    if (!vs || !fs) { failed = true; gl = null; return false; }
    prog = gl.createProgram();
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
      highlights: gl.getUniformLocation(prog, 'uHighlights'),
      shadows: gl.getUniformLocation(prog, 'uShadows'),
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

  function uploadSource(img) {
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
      while (gl.getError() !== gl.NO_ERROR) {}
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
    render: function(img, u) {
      if (!init() || !uploadSource(img)) return false;
      gl.viewport(0, 0, canvas.width, canvas.height);
      gl.uniform1f(locs.exposure, u.exposure);
      gl.uniform3f(locs.wbGain, u.wbGain[0], u.wbGain[1], u.wbGain[2]);
      gl.uniform1f(locs.highlights, u.highlights);
      gl.uniform1f(locs.shadows, u.shadows);
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
function _lbAdjustmentPreviewUrl(recipe, neutral) {
  var img = document.getElementById('lightboxImg');
  var size = Math.max(img.naturalWidth, img.naturalHeight) || 1920;
  return '/photos/' + _lightboxCurrentId + '/edit-preview?size=' + size +
    '&apply_crop=1' + (neutral ? '&analysis=1' : '') +
    '&recipe=' + encodeURIComponent(JSON.stringify(recipe));
}

function _lbLoadAdjustmentSource() {
  var recipe = _lbCloneRecipe(_lbEditRecipe);
  delete recipe.version;
  delete recipe.adjustments;
  delete recipe.local;
  var url = _lbAdjustmentPreviewUrl(recipe, true);
  if (_lbAdjustmentSource && _lbAdjustmentSource.url === url) return _lbAdjustmentSource;
  var source = {url: url, image: new Image()};
  source.ready = new Promise(function(resolve) {
    source.image.onload = function() { resolve(true); };
    source.image.onerror = function() { resolve(false); };
  });
  source.image.src = url;
  _lbAdjustmentSource = source;
  return source;
}

// Advanced color, detail, and local edits must run in their canonical order
// with the global adjustments. Use the full server recipe for those, and for
// devices where WebGL cannot render the source, instead of stacking filters.
function _lbPreviewNeedsServer(recipe) {
  // RAW controls must run before display encoding; a JPEG WebGL texture has
  // already lost the scene highlight headroom and cannot match saved renders.
  var photo = _lightboxPhotoList.find(function(p) { return p.id === _lightboxCurrentId; });
  var filename = (photo && photo.filename) || document.getElementById('lightboxFilename').textContent;
  if (/\.(nef|cr2|cr3|arw|raf|dng|rw2|orf)$/i.test(filename || '')) return true;
  // Range adjustments preserve texture using neighboring luminance samples.
  // The single-pass shader cannot reproduce this shared preview/export math.
  var adjustments = recipe.adjustments || {};
  if (adjustments.shadows || adjustments.highlights) return true;
  var supported = ['exposure', 'highlights', 'shadows', 'whites', 'blacks',
    'contrast', 'vibrance', 'saturation', 'white_balance', 'temperature', 'tint'];
  return !!recipe.local || Object.keys(recipe.adjustments || {}).some(function(key) {
    return supported.indexOf(key) === -1;
  });
}

function _lbApplyServerAdjustmentPreview(recipe, seq) {
  var url = _lbAdjustmentPreviewUrl(recipe, false);
  // Coalesce slider events before starting an expensive server render.
  _lbAdjustmentPreviewTimer = setTimeout(function() {
    _lbAdjustmentPreviewTimer = null;
    var preview = new Image();
    preview.onload = function() {
      if (seq !== _lbAdjustmentPreviewSeq) return;
      var overlay = document.getElementById('lightboxAdjustmentImage');
      overlay.src = url;
      overlay.classList.add('show');
      document.getElementById('lightboxToneCanvas').classList.remove('show');
    };
    preview.src = url;
  }, 80);
}

function _lbApplyAdjustmentPreview(values) {
  var seq = ++_lbAdjustmentPreviewSeq;
  clearTimeout(_lbAdjustmentPreviewTimer);
  var recipe = _lbCloneRecipe(_lbEditRecipe);
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
      highlights: Number(values.highlights || 0),
      shadows: Number(values.shadows || 0),
      whites: Number(values.whites || 0),
      blacks: Number(values.blacks || 0),
      contrast: Math.max(0, 1 + Number(values.contrast || 0) / 100),
      vibrance: Number(values.vibrance || 0),
      saturation: Math.max(0, 1 + Number(values.saturation || 0) / 100),
      rolloff: pushed
    });
    if (ok) {
      document.getElementById('lightboxAdjustmentImage').classList.remove('show');
      document.getElementById('lightboxToneCanvas').classList.add('show');
    } else {
      _lbApplyServerAdjustmentPreview(recipe, seq);
    }
  });
}

function _lbClearAdjustmentPreview() {
  ++_lbAdjustmentPreviewSeq;
  clearTimeout(_lbAdjustmentPreviewTimer);
  _lbAdjustmentPreviewTimer = null;
  document.getElementById('lightboxToneCanvas').classList.remove('show');
  document.getElementById('lightboxAdjustmentImage').classList.remove('show');
}

function _lbBumpRecipeBust(photoId) {
  var key = String(photoId);
  _lbRecipeBustByPhotoId[key] = Date.now();
}

function _lbRefreshVisiblePhotoImages(photoId) {
  var bust = _lbRecipeBustByPhotoId[String(photoId)];
  if (!bust) return;
  var thumbUrl = vireoThumbnailUrl(photoId);
  var refreshed = [];

  function refreshImg(img, url) {
    if (!img || refreshed.indexOf(img) !== -1) return;
    refreshed.push(img);
    // Route through _vireoSetRenderedImageSource so grid cards update
    // data-thumbnail-src; the queue then cancels any in-flight fetch and
    // fetches the new URL under the concurrency limit, preventing a stale
    // response from overwriting the edited pixels.
    _vireoSetRenderedImageSource(img, url || thumbUrl);
  }

  refreshImg(document.querySelector('.grid-card[data-id="' + photoId + '"] img'));
  document.querySelectorAll('img[data-photo-id="' + photoId + '"]').forEach(function(img) {
    refreshImg(img);
  });
  document.querySelectorAll('[data-photo-id="' + photoId + '"] img').forEach(function(img) {
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

  var detailImg = document.getElementById('detailImg');
  if (detailImg && window._detailPhotoId === photoId) {
    refreshImg(detailImg);
  }
}

function _lbReloadEditedSource(photoId, seq) {
  if (_lightboxCurrentId !== photoId) return;
  // The source switch owns the visible image until its target has decoded.
  if (_vireoPairPendingSourceByPhoto[String(photoId)]) return;
  _lbCancelOriginalPreload();
  var img = document.getElementById('lightboxImg');
  if (!img) return;
  var key = _lbCurrentSrcKey || 'full';
  img.addEventListener('load', function onEditedReload() {
    img.removeEventListener('load', onEditedReload);
    if (_lightboxCurrentId !== photoId || seq !== _lbAdjustSeq) return;
    _lbClearAdjustmentPreview();
    _lbRecomputeNativeZoom();
    _lbApplyTransform();
    if (key === 'full') _lbScheduleOriginalPreload(photoId);
  });
  img.src = _lbSrcUrl(photoId, key);
}

function _lbStartAdjustmentRecipeSave(photoId, recipe, inputSeq) {
  var seq = _lightboxCurrentId === photoId ? ++_lbAdjustSeq : _lbAdjustSeq;
  var key = String(photoId);
  _lbAdjustSaveInFlightByPhoto[key] = true;
  if (_lightboxCurrentId === photoId) _lbSetAdjustmentStatus('Saving...');
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
      if (_lightboxCurrentId !== photoId || inputSeq !== _lbAdjustmentInputSeqFor(photoId)) return;
      var activeSeq = ++_lbAdjustSeq;
      _lbEditRecipe = _lbCloneRecipe(data.recipe || {});
      _lbEditRecipeLoaded = true;
      _lbRenderAdjustmentControls();
      _lbSetAdjustmentControlsDisabled(false);
      _lbReloadEditedSource(photoId, activeSeq);
      _lbSetAdjustmentStatus('Saved');
    })
    .catch(function(err) {
      if (_lightboxCurrentId !== photoId || seq !== _lbAdjustSeq) return;
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

function _lbSaveAdjustmentRecipe(values) {
  if (_lbGuardAdjustmentSource()) return false;
  if (!_lightboxCurrentId || !_lbEditRecipeLoaded) return;
  var photoId = _lightboxCurrentId;
  var key = String(photoId);
  var inputSeq = _lbAdjustmentInputSeqFor(photoId);
  var recipe = _lbRecipeWithAdjustments(values);
  _lbEditRecipe = _lbCloneRecipe(recipe);
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

function _lbFlushPendingAdjustmentSave() {
  if (!_lbAdjustSaveTimer) return;
  clearTimeout(_lbAdjustSaveTimer);
  _lbAdjustSaveTimer = null;
  _lbSaveAdjustmentRecipe(_lbReadAdjustmentControls());
}

function _lbWaitForAdjustmentSaveIdle(photoId) {
  var key = String(photoId);
  return new Promise(function(resolve, reject) {
    var started = Date.now();
    function check() {
      if (
        !_lbAdjustSaveTimer &&
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

function onLightboxAdjustmentInput(input) {
  if (_lbGuardAdjustmentSource()) return false;
  if (!_lbEditRecipeLoaded) {
    _lbSetAdjustmentStatus('Loading recipe...');
    _lbRenderAdjustmentControls();
    return;
  }
  var label = document.getElementById(input.id + 'Value');
  if (label) label.textContent = _lbFormatAdjustmentValue(input.dataset.adjustment, input.value);
  _lbAdjustSeq += 1;
  _lbBumpAdjustmentInputSeq(_lightboxCurrentId);
  var values = _lbReadAdjustmentControls();
  _lbEditRecipe = _lbRecipeWithAdjustments(values);
  _lbApplyAdjustmentPreview(values);
  _lbSetAdjustmentStatus('Previewing...');
  if (_lbAdjustSaveTimer) clearTimeout(_lbAdjustSaveTimer);
  _lbAdjustSaveTimer = setTimeout(function() {
    _lbAdjustSaveTimer = null;
    _lbSaveAdjustmentRecipe(_lbReadAdjustmentControls());
  }, 350);
}

function resetLightboxAdjustments() {
  if (_lbGuardAdjustmentSource()) return false;
  if (!_lbEditRecipeLoaded) {
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
  _lbEditRecipe = _lbRecipeWithAdjustments(values);
  _lbApplyAdjustmentPreview(values);
  _lbBumpAdjustmentInputSeq(_lightboxCurrentId);
  if (_lbAdjustSaveTimer) {
    clearTimeout(_lbAdjustSaveTimer);
    _lbAdjustSaveTimer = null;
  }
  _lbSaveAdjustmentRecipe(values);
}

function toggleLightboxAdjustPanel() {
  if (_lbGuardAdjustmentSource()) return false;
  var panel = document.getElementById('lightboxAdjustPanel');
  var btn = document.getElementById('lightboxAdjustBtn');
  if (!panel) return;
  var open = !panel.classList.contains('open');
  panel.classList.toggle('open', open);
  if (btn) btn.setAttribute('aria-expanded', open ? 'true' : 'false');
  if (open) {
    _lbRenderAdjustmentControls();
    _lbSetAdjustmentControlsDisabled(!_lbEditRecipeLoaded);
    _lbSetAdjustmentStatus(_lbEditRecipeLoaded ? '' : 'Loading recipe...');
    if (_lbEditRecipeLoaded && !_lbPreviewNeedsServer(_lbEditRecipe) && VireoToneGL.supported()) {
      _lbLoadAdjustmentSource();
    }
  }
}

function _lbIsOneToOneZoom() {
  if (_lbPending1To1) return true;
  if (!_lbNativeZoom || _lbNativeZoom <= 1.001) return false;
  var tolerance = Math.max(0.01, _lbNativeZoom * 0.01);
  return Math.abs(_lbZoom - _lbNativeZoom) <= tolerance;
}

function _lbSrcRank(key) {
  // Tier ordering by available source resolution (higher = sharper).
  if (key === 'original') return 3;
  if (key === '3840') return 2;
  if (key === '2560') return 1;
  return 0; // 'full', '1920', or unknown
}

function _lbCloneViewportState(state) {
  if (!state) return null;
  return {
    zoom: Number(state.zoom) || 1.0,
    centerX: Number(state.centerX),
    centerY: Number(state.centerY),
    oneToOne: !!state.oneToOne,
    pending1To1: !!state.pending1To1,
  };
}

function _lbViewportStateFromCurrent() {
  var metrics = _lbUpdateLayoutMetrics();
  var state = {
    zoom: _lbZoom || 1.0,
    centerX: 0.5,
    centerY: 0.5,
    oneToOne: _lbIsOneToOneZoom(),
    pending1To1: !!_lbPending1To1,
  };
  var wrap = document.getElementById('lightboxWrap');
  var t = document.getElementById('lightboxTransform');
  if (metrics && wrap && t) {
    var wrapRect = wrap.getBoundingClientRect();
    var rect = t.getBoundingClientRect();
    var scale = metrics.scale || 1;
    if (scale > 0 && metrics.w && metrics.h) {
      var imgX = (wrapRect.left + wrapRect.width / 2 - rect.left) / scale;
      var imgY = (wrapRect.top + wrapRect.height / 2 - rect.top) / scale;
      state.centerX = Math.max(0, Math.min(1, imgX / metrics.w));
      state.centerY = Math.max(0, Math.min(1, imgY / metrics.h));
    }
  }
  return state;
}

function _lbCaptureEyeTrackingAnchor() {
  if (!_lbTrackEyeEnabled || (_lbZoom <= 1.001 && !_lbPending1To1)) return null;
  // During rapid navigation the DOM can still contain the outgoing bitmap.
  // Reuse the last trustworthy screen anchor instead of measuring that bitmap
  // against the incoming photo id.
  if (_lbVisualTransitionPending) {
    return _lbEyeTrackScreenAnchor
      ? { offsetX: _lbEyeTrackScreenAnchor.offsetX, offsetY: _lbEyeTrackScreenAnchor.offsetY }
      : null;
  }
  var photo = _lbPhotoData(_lightboxCurrentId);
  var point = _lbPhotoEyePoint(_lightboxCurrentId, photo);
  var overlaysAvailable = _lbSourceOverlaysAvailable();
  var metrics = _lbUpdateLayoutMetrics();
  var state = point && overlaysAvailable ? _lbViewportStateFromCurrent() : null;
  if (!point || !overlaysAvailable || !metrics || !state) {
    // Current frame is unusable for a fresh measurement — either its metadata
    // has resolved without an eye, the active source cannot align overlays
    // (JPEG pair or geometric edit), the destination metadata is not yet
    // cached, or the layout has not been measured. Reuse the last trustworthy
    // anchor so a later eyed frame can still resume alignment across the
    // paused sequence. User pan/zoom paths clear the cached anchor via
    // _lbClearPendingViewportRestore, so a manual viewport change on this
    // paused frame will not carry a stale eye offset to the next eyed photo.
    return _lbEyeTrackScreenAnchor
      ? { offsetX: _lbEyeTrackScreenAnchor.offsetX, offsetY: _lbEyeTrackScreenAnchor.offsetY }
      : null;
  }
  var anchor = {
    offsetX: (point.x - state.centerX) * metrics.w * metrics.scale,
    offsetY: (point.y - state.centerY) * metrics.h * metrics.scale,
  };
  _lbEyeTrackScreenAnchor = anchor;
  return { offsetX: anchor.offsetX, offsetY: anchor.offsetY };
}

function _lbTryApplyPendingEyeTrack(photo) {
  var pending = _lbPendingEyeTrack;
  if (!pending || !_lbTrackEyeEnabled) return false;
  if (String(pending.photoId) !== String(_lightboxCurrentId)) return false;
  photo = photo || _lbPhotoData(_lightboxCurrentId);
  // A missing object means the detail request has not resolved yet. Keep the
  // alignment armed so its callback can apply it after the image is laid out.
  // Navigation lists on some pages contain only {id, filename}; those are
  // also "unknown", not evidence that the destination lacks an eye.
  if (!photo || !Object.prototype.hasOwnProperty.call(photo, 'eye_x')) return false;
  var point = _lbPhotoEyePoint(_lightboxCurrentId, photo);
  var metrics = _lbUpdateLayoutMetrics();
  if (!point || !_lbSourceOverlaysAvailable()) {
    _lbPendingEyeTrack = null;
    _lbApplyTrackEyeState();
    return false;
  }
  if (!metrics || _lbVisualTransitionPending) return false;

  // A deferred 1:1 snap is still armed — the loader is holding the current
  // display upscaled while a sharper source (2560/3840 or /original) swap
  // completes. Applying the viewport here would re-enter
  // _lbApplyViewportState(), whose one-to-one branch would either clear
  // _lbPending1To1 (when nativeZoom is known) or leave it armed but whose
  // trailing _lbScheduleSourceSwap() can retarget back to /full when
  // /original is unavailable and nativeZoom is not yet known — cancelling
  // the sharp fallback and leaving the user on a soft view. Wait for the
  // deferred source swap to land and _lbApplyPendingOneToOneZoom() to
  // finish before aligning the eye.
  if (_lbPending1To1) return false;

  var state = _lbViewportStateFromCurrent();
  if (!state || _lbZoom <= 1.001 || !metrics.scale) return false;
  state.centerX = point.x - (Number(pending.offsetX) || 0) / (metrics.w * metrics.scale);
  state.centerY = point.y - (Number(pending.offsetY) || 0) / (metrics.h * metrics.scale);
  _lbApplyViewportState(state);

  // Native 1:1 can be learned after the first layout pass. Retain the pending
  // alignment until then so a later 1:1 snap does not move the eye.
  if (!state.oneToOne || _lbNativeZoom) {
    _lbPendingEyeTrack = null;
  }
  _lbEyeTrackScreenAnchor = {
    offsetX: Number(pending.offsetX) || 0,
    offsetY: Number(pending.offsetY) || 0,
  };
  _lbSaveViewportState(_lightboxCurrentId);
  _lbApplyTrackEyeState();
  return true;
}

function _lbSaveViewportState(photoId) {
  if (photoId == null) return null;
  // Mid-navigation the DOM transform still belongs to the outgoing photo but
  // _lightboxCurrentId has already advanced to the incoming id. Reading from
  // the DOM here would misattribute the frozen bitmap to the incoming photo
  // and stomp its intended inspection point. Prefer the pending restore
  // state (what handleInitialImageLoad is about to apply), and otherwise
  // leave any previously saved state alone.
  if (_lbVisualTransitionPending && String(photoId) === String(_lightboxCurrentId)) {
    var pending = _lbPendingViewportState;
    if (pending) {
      var pendingClone = _lbCloneViewportState(pending);
      _lbViewportByPhotoId[String(photoId)] = pendingClone;
      return _lbCloneViewportState(pendingClone);
    }
    var existing = _lbViewportByPhotoId[String(photoId)];
    return existing ? _lbCloneViewportState(existing) : null;
  }
  var state = _lbViewportStateFromCurrent();
  _lbViewportByPhotoId[String(photoId)] = state;
  return _lbCloneViewportState(state);
}

function _lbViewportStateForOpen(photoId, fallbackState) {
  // Arrow navigation supplies the viewport from the photo being left. Treat
  // that handoff as the current browse-session viewport, even when the target
  // photo has an older cached viewport from an earlier visit. This keeps zoom
  // continuous while moving left/right: zooming back out on one photo also
  // means a previously visited photo opens zoomed out when navigating back.
  if (fallbackState) {
    return _lbCloneViewportState(fallbackState);
  }
  var key = String(photoId);
  if (Object.prototype.hasOwnProperty.call(_lbViewportByPhotoId, key)) {
    return _lbCloneViewportState(_lbViewportByPhotoId[key]);
  }
  return null;
}

function _lbApplyViewportState(state) {
  state = _lbCloneViewportState(state);
  if (!state) return false;
  var metrics = _lbUpdateLayoutMetrics();
  if (!metrics) return false;
  var wrap = document.getElementById('lightboxWrap');
  if (!wrap) return false;

  var maxZoom = _lbMaxZoom();
  var targetZoom = state.zoom || 1.0;
  if (state.oneToOne) {
    if (_lbNativeZoom) {
      targetZoom = _lbNativeZoom;
      _lbPending1To1 = false;
    } else {
      _lbPending1To1 = true;
      targetZoom = Math.max(1.0, targetZoom);
    }
  } else {
    _lbPending1To1 = false;
  }
  targetZoom = Math.max(1.0, Math.min(maxZoom, targetZoom));
  _lbZoom = targetZoom;

  if (_lbZoom <= 1.001) {
    _lbPanX = 0;
    _lbPanY = 0;
  } else {
    var scale = _lbFitScale * _lbZoom;
    var centerX = Number.isFinite(state.centerX) ? state.centerX : 0.5;
    var centerY = Number.isFinite(state.centerY) ? state.centerY : 0.5;
    centerX = Math.max(0, Math.min(1, centerX));
    centerY = Math.max(0, Math.min(1, centerY));
    var baseLeft = (metrics.wrapW - metrics.w * scale) / 2;
    var baseTop = (metrics.wrapH - metrics.h * scale) / 2;
    _lbPanX = (metrics.wrapW / 2) - (centerX * metrics.w * scale) - baseLeft;
    _lbPanY = (metrics.wrapH / 2) - (centerY * metrics.h * scale) - baseTop;
    _lbClampPan();
  }

  wrap.classList.toggle('zoomed', _lbZoom > 1.001);
  _lbApplyTransform();
  _lbScheduleSourceSwap();
  return true;
}

function _lbFlushDeferredOverlayApply() {
  // Called from handleInitialImageLoad and the terminal handleInitialImageError
  // branch, i.e. wherever _lbVisualTransitionPending flips back to false. The
  // metadata callback stashes the detection/eye render here so overlays don't
  // paint against the still-frozen outgoing transform. Re-apply the mask
  // overlay too because _lbApplyMaskVisibility only adds the `show` class when
  // the transition is clear, so an in-flight mask load could have been gated.
  var pending = _lbDeferredOverlayApply;
  _lbDeferredOverlayApply = null;
  if (typeof pending === 'function') {
    try { pending(); } catch (_) {}
  }
  _lbApplyMaskVisibility();
}

function _lbClearPendingViewportRestore() {
  _lbProgressiveTargetKey = null;
  _lbSetPreviewLoading(false);
  // Called from user-driven viewport mutations (wheel/keyboard/click zoom,
  // drag pan) so a still-armed restore carried over by arrow navigation
  // cannot be reapplied by a later async metadata/image-load callback and
  // stomp the user's manual zoom/pan. Also drops any deferred eye-tracking
  // alignment for the same reason: when a destination image finishes
  // loading before /api/photos/<id> returns, _lbPendingEyeTrack stays
  // armed until the metadata callback fires _lbTryApplyPendingEyeTrack,
  // and a user pan/zoom in that window must not be overwritten by the
  // previous photo's eye anchor. Dropping the cached screen anchor here
  // — instead of eagerly on every known no-eye frame — lets tracking
  // resume across an eyed → no-eye → eyed sequence when the user did not
  // pan or zoom the paused frame, while still preventing a stale offset
  // from being carried onto the next eyed photo after a manual pan/zoom
  // on the paused frame. Programmatic restore paths
  // (_lbTryApplyPendingViewportState, _lbTryApplyPendingEyeTrack) own
  // clearing their own state and must not route through here.
  _lbPendingViewportState = null;
  _lbPendingEyeTrack = null;
  _lbEyeTrackScreenAnchor = null;
}

function _lbTryApplyPendingViewportState() {
  if (!_lbPendingViewportState) return false;
  var state = _lbPendingViewportState;
  if (!_lbApplyViewportState(state)) return false;
  // Keep the pending state until native zoom is known. Until then
  // _lbApplyViewportState clamps with a fallback max of 4, so a saved
  // zoom > 4 would be permanently degraded if cleared here and never
  // retried once the image load event establishes _lbNativeZoom.
  if (_lbNativeZoom) {
    _lbPendingViewportState = null;
  }
  return true;
}

function _lbApplyPendingOneToOneZoom() {
  if (!_lbPending1To1 || !_lbNativeZoom) return;
  // After /original fails, native zoom describes the current preview's pixels.
  // A late metadata callback must keep waiting for the sharper fallback already
  // requested by the error path; snapping now would retarget that swap to /full.
  if (_lbOriginalUnavailable && _lbSrcRank(_lbDesiredSrcKey) > _lbSrcRank(_lbCurrentSrcKey)) return;
  // Don't snap to true 1:1 until the displayed source is at least the tier
  // that 1:1 needs. Otherwise the /api/photos/<id> metadata fetch resolving
  // before the held /original load would learn _lbNativeZoom, clear the
  // pending state, and snap on the upscaled lower tier — the soft-1:1 flash
  // this deferral exists to prevent. Compare by tier rank (not equality) so a
  // source that is already sharper than required — e.g. /original kept during
  // 1:1-preserving navigation — still applies immediately instead of waiting
  // forever for a downgrade that never comes.
  var requiredSource = _lbPickSourceKey(_lbNativeZoom);
  if (_lbSrcRank(_lbCurrentSrcKey) < _lbSrcRank(requiredSource)) {
    _lbScheduleSourceSwap(_lbNativeZoom);
    return;
  }
  _lbPending1To1 = false;
  var anchor = _lbPending1To1Anchor;
  _lbPending1To1Anchor = null;
  _lbSetZoom(
    _lbNativeZoom,
    anchor ? anchor.x : null,
    anchor ? anchor.y : null
  );
}

function _lbDeferPendingOneToOneUntilSourceReady(targetZoom) {
  if (!_lbPending1To1) return false;
  _lbPendingViewportState = null;
  _lbApplyTransform();
  _lbScheduleSourceSwap(targetZoom);
  if (_lbDesiredSrcKey === _lbCurrentSrcKey) {
    _lbApplyPendingOneToOneZoom();
  }
  return true;
}

function _lbDeferPendingOneToOneToPreviewFallback() {
  // /original is unavailable but the user still wants true 1:1. Keep the
  // deferral pending and aim at the sharpest remaining preview tier instead of
  // snapping on an upscaled /full.
  return _lbDeferPendingOneToOneUntilSourceReady(4);
}

function _lbNormalizeFlag(flag) {
  if (flag === 'flagged' || flag === 'rejected' || flag === 'none') return flag;
  if (flag == null || flag === '') return 'none';
  return null;
}

function _lbSetFlagStatus(flag) {
  var normalized = typeof flag === 'undefined' ? null : _lbNormalizeFlag(flag);
  var flagBtn = document.getElementById('lightboxFlagBtn');
  var rejectBtn = document.getElementById('lightboxRejectBtn');
  if (flagBtn) flagBtn.setAttribute('aria-pressed', normalized === 'flagged' ? 'true' : 'false');
  if (rejectBtn) rejectBtn.setAttribute('aria-pressed', normalized === 'rejected' ? 'true' : 'false');
  var el = document.getElementById('lightboxFlagStatus');
  if (!el) return;
  el.className = 'lightbox-flag-status';
  if (!normalized) {
    el.textContent = '';
    return;
  }
  if (normalized === 'flagged') {
    el.textContent = 'Flagged';
    el.classList.add('visible', 'flagged');
  } else if (normalized === 'rejected') {
    el.textContent = 'Rejected';
    el.classList.add('visible', 'rejected');
  } else {
    el.textContent = 'No flag';
    el.classList.add('visible');
  }
}

function _lbPendingFlagLabel(flag) {
  if (flag === 'flagged') return 'Flagging...';
  if (flag === 'rejected') return 'Rejecting...';
  if (flag === 'none') return 'Clearing flag...';
  return 'Saving...';
}

function _lbSetPendingFlagStatus(flag) {
  var el = document.getElementById('lightboxFlagStatus');
  if (!el) return;
  el.className = 'lightbox-flag-status visible pending';
  el.textContent = _lbPendingFlagLabel(flag);
}

function _lbFlagFromPhotoList(photoId) {
  var p = _lightboxPhotoList.find(function(x) { return x.id === photoId; });
  if (!p || !Object.prototype.hasOwnProperty.call(p, 'flag')) return undefined;
  return p.flag;
}

function _lbRememberConfirmedFlag(photoId, flag) {
  var normalized = _lbNormalizeFlag(flag);
  if (!normalized || photoId == null) return;
  _lbConfirmedFlags[String(photoId)] = normalized;
}

function _lbForgetConfirmedFlag(photoId) {
  if (photoId == null) return;
  delete _lbConfirmedFlags[String(photoId)];
}

function _lbConfirmedFlagFor(photoId) {
  var key = String(photoId);
  if (Object.prototype.hasOwnProperty.call(_lbConfirmedFlags, key)) {
    return _lbConfirmedFlags[key];
  }
  return _lbFlagFromPhotoList(photoId);
}

function _lbDisplayedFlagFor(photoId) {
  var key = String(photoId);
  if (Object.prototype.hasOwnProperty.call(_lbProvisionalFlags, key)) {
    return _lbProvisionalFlags[key];
  }
  return _lbConfirmedFlagFor(photoId);
}

window.setLightboxProvisionalFlag = function(photoId, flag, editSeq) {
  var normalized = _lbNormalizeFlag(flag);
  if (photoId == null || !normalized) return;
  var key = String(photoId);
  var seq = Number.isInteger(editSeq) ? editSeq : (_lbFlagEditSeq + 1);
  if (seq < (_lbProvisionalFlagSeq[key] || 0)) return;
  _lbFlagEditSeq = Math.max(_lbFlagEditSeq, seq);
  _lbProvisionalFlags[key] = normalized;
  _lbProvisionalFlagSeq[key] = seq;
  if (_lightboxCurrentId === parseInt(photoId, 10)) {
    _lbSetFlagStatus(normalized);
  }
};

// A page that stages flag edits can clear its lightbox-only provisional
// display when the staging session ends. Forget the confirmed memo too so
// the page's now-authoritative photo list (persisted on Apply, unchanged on
// discard) supplies the next visible value.
window.clearLightboxProvisionalFlags = function(photoIds) {
  (photoIds || []).forEach(function(photoId) {
    delete _lbProvisionalFlags[String(photoId)];
    delete _lbProvisionalFlagSeq[String(photoId)];
    _lbForgetConfirmedFlag(photoId);
  });
  if (_lightboxCurrentId != null && (photoIds || []).some(function(photoId) {
    return parseInt(photoId, 10) === _lightboxCurrentId;
  })) {
    _lbSetFlagStatus(_lbDisplayedFlagFor(_lightboxCurrentId));
  }
};

function _lbRecordFlag(photoId, flag) {
  var normalized = _lbNormalizeFlag(flag);
  if (!normalized) return;
  var p = _lightboxPhotoList.find(function(x) { return x.id === photoId; });
  if (p) p.flag = normalized;
  _lbRememberConfirmedFlag(photoId, normalized);
  if (_lightboxCurrentId === photoId && !_lbVisualTransitionPending) {
    _lbSetFlagStatus(_lbDisplayedFlagFor(photoId));
  }
}

function _lbCacheFlag(photoId, flag) {
  var normalized = _lbNormalizeFlag(flag);
  if (!normalized) return;
  var p = _lightboxPhotoList.find(function(x) { return x.id === photoId; });
  if (p) p.flag = normalized;
  _lbRememberConfirmedFlag(photoId, normalized);
}

function _lbRecordFetchedFlag(photoId, flag, flagFetchSeq) {
  if (_lbFlagEditSeq !== flagFetchSeq) return;
  _lbRecordFlag(photoId, flag);
}

function _lbApplyFlag(photoId, flag) {
  var normalized = _lbNormalizeFlag(flag);
  if (!normalized || photoId == null) return false;
  var previousFlag = _lbNormalizeFlag(_lbConfirmedFlagFor(photoId)) || 'none';
  var seq = _lbFlagEditSeq + 1;
  _lbFlagEditSeq = seq;
  _lbFlagPendingWrites += 1;
  _lbFlagPendingByPhoto[photoId] = (_lbFlagPendingByPhoto[photoId] || 0) + 1;
  _lbSetPendingFlagStatus(normalized);
  var write;

  if (typeof window.setFlagFor === 'function') {
    write = window.setFlagFor(photoId, normalized);
  } else if (typeof window.setReviewFlag === 'function') {
    write = window.setReviewFlag(photoId, normalized);
  } else if (typeof window.safeFetch === 'function') {
    write = window.safeFetch('/api/photos/' + photoId + '/flag', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({flag: normalized}),
    }, { toast: false }).then(function() { return true; }).catch(function() { return false; });
  } else {
    _lbSetFlagStatus(_lbConfirmedFlagFor(photoId));
    return false;
  }

  // Resolve both success and rejection through one settle path so the per-photo
  // pending count is always balanced and the quiescence event always fires.
  function settle(result) {
    // Page-local flag helpers may deliberately keep a change provisional
    // (for example, Process Review's Group Review stages picks/rejects until
    // Apply). Show that choice in the open lightbox, but do not promote it to
    // the confirmed cache, mutate the shared photo list, or emit the
    // persisted-change event. Plain true/undefined remain successful writes;
    // false remains a failed write for backward compatibility.
    var provisional = !!(
      result && typeof result === 'object' && result.provisional === true
    );
    var landed = result !== false && !provisional;
    _lbFlagPendingWrites = Math.max(0, _lbFlagPendingWrites - 1);
    var remaining = (_lbFlagPendingByPhoto[photoId] || 1) - 1;
    if (remaining > 0) _lbFlagPendingByPhoto[photoId] = remaining;
    else delete _lbFlagPendingByPhoto[photoId];

    if (provisional) {
      window.setLightboxProvisionalFlag(photoId, normalized, seq);
      if (_lightboxCurrentId === photoId && (_lbFlagEditSeq === seq || _lbFlagPendingWrites === 0)) {
        _lbSetFlagStatus(_lbDisplayedFlagFor(photoId));
      }
    } else if (landed) {
      var provisionalSeq = _lbProvisionalFlagSeq[String(photoId)] || 0;
      if (provisionalSeq <= seq) {
        delete _lbProvisionalFlags[String(photoId)];
        delete _lbProvisionalFlagSeq[String(photoId)];
      }
      // Cache the confirmed flag even if the user has navigated away, so the
      // quiescence emit below reflects what actually landed for this photo.
      _lbCacheFlag(photoId, normalized);
      if (_lightboxCurrentId === photoId && (_lbFlagEditSeq === seq || _lbFlagPendingWrites === 0)) {
        _lbSetFlagStatus(_lbDisplayedFlagFor(photoId));
      }
    } else if (_lightboxCurrentId === photoId && _lbFlagEditSeq === seq) {
      // Write failed: the prior confirmed flag stands; restore the chip to it.
      _lbSetFlagStatus(_lbConfirmedFlagFor(photoId));
    }

    // Once every in-flight write for THIS photo has settled, tell listeners
    // (e.g. Highlights) the photo's final confirmed flag. Emitting on
    // quiescence — rather than per write — means a landed reject whose
    // superseding clear/flag write later FAILED is still reflected, and rapid
    // same-photo toggling collapses to one correct event instead of a guess.
    if (!provisional && !Object.prototype.hasOwnProperty.call(_lbFlagPendingByPhoto, photoId)) {
      try {
        document.dispatchEvent(new CustomEvent('lightbox:flagchanged', {
          detail: {
            photoId: photoId,
            flag: _lbConfirmedFlagFor(photoId),
            previousFlag: previousFlag,
          },
        }));
      } catch (_) {}
    }
  }

  Promise.resolve(write)
    .then(function(result) { settle(result); })
    .catch(function() { settle(false); });

  return true;
}

function _lbStoredBool(key, fallback) {
  try {
    var value = localStorage.getItem(key);
    if (value === '0') return false;
    if (value === '1') return true;
  } catch (e) {}
  return fallback;
}

function _lbPersistBool(key, value) {
  try { localStorage.setItem(key, value ? '1' : '0'); } catch (e) {}
}

var _lbBoxesVisible = _lbStoredBool('vireo.lb.boxesVisible', false);
var _lbMasksVisible = _lbStoredBool('vireo.lb.masksVisible', false);
var _lbEyeVisible = _lbStoredBool('vireo.lb.eyeVisible', false);
var _lbTrackEyeEnabled = _lbStoredBool('vireo.lb.trackEye', false);
var _lbInfoVisible = _lbStoredBool('vireo.lb.infoVisible', true);
var _lbChromeVisible = _lbStoredBool('vireo.lb.chromeVisible', true);

function _lbRecipeHasCrop(recipe) {
  return !!(recipe && recipe.crop && Number(recipe.crop.w) > 0 && Number(recipe.crop.h) > 0);
}

function _lbRecipeHasGeometricEdit(recipe) {
  if (!recipe || typeof recipe !== 'object') return false;
  if (_lbRecipeHasCrop(recipe)) return true;
  if (Number(recipe.rotation || 0)) return true;
  if (Math.abs(Number(recipe.straighten || 0)) > 1e-9) return true;
  var flip = recipe.flip || {};
  return !!(flip.horizontal || flip.vertical);
}

function _lbMetadataOrientation(metadata) {
  if (!metadata || typeof metadata !== 'object') return null;
  var groups = ['EXIF', 'IFD0', 'TIFF', 'File'];
  for (var i = 0; i < groups.length; i++) {
    var group = metadata[groups[i]];
    if (group && Object.prototype.hasOwnProperty.call(group, 'Orientation')) {
      return group.Orientation;
    }
  }
  if (Object.prototype.hasOwnProperty.call(metadata, 'Orientation')) {
    return metadata.Orientation;
  }
  return null;
}

function _lbOrientationSwapsAxes(orientation) {
  if (orientation == null || typeof orientation === 'boolean') return false;
  if (typeof orientation === 'number') {
    return [5, 6, 7, 8].indexOf(Math.round(orientation)) !== -1;
  }
  var text = String(orientation).trim().toLowerCase();
  if (!text) return false;
  var parsed = Number(text);
  if (Number.isFinite(parsed)) return [5, 6, 7, 8].indexOf(Math.round(parsed)) !== -1;
  return text.indexOf('90') !== -1 || text.indexOf('270') !== -1;
}

function _lbSourceOverlaysAvailable() {
  var pairUsesJpeg = (
    _lightboxCurrentId != null &&
    _vireoPairKnownByPhoto[String(_lightboxCurrentId)] &&
    _vireoPairSource(_lightboxCurrentId) === 'jpeg'
  );
  return !pairUsesJpeg && !_lbRecipeHasGeometricEdit(_lbCurrentEditRecipe);
}

function _lbApplyBoxesVisibility() {
  var container = document.getElementById('lightboxDetections');
  var available = _lbSourceOverlaysAvailable();
  if (container) container.style.display = (_lbBoxesVisible && available) ? '' : 'none';
  var input = document.getElementById('lightboxToggleBoxes');
  if (input) {
    input.disabled = !available;
    input.checked = _lbBoxesVisible;
  }
  var note = document.getElementById('lightboxBoxesNote');
  if (note) note.textContent = available ? '' : 'unavailable for edited photos';
}

function toggleLightboxBoxes() {
  _lbBoxesVisible = !_lbBoxesVisible;
  _lbPersistBool('vireo.lb.boxesVisible', _lbBoxesVisible);
  _lbApplyBoxesVisibility();
}

function _lbApplyInfoVisibility() {
  var overlay = document.getElementById('lightboxOverlay');
  if (overlay) overlay.classList.toggle('lb-hide-info', !_lbInfoVisible);
  var input = document.getElementById('lightboxToggleInfo');
  if (input) input.checked = _lbInfoVisible;
}

function toggleLightboxInfo() {
  _lbInfoVisible = !_lbInfoVisible;
  if (!_lbInfoVisible) _lbSetZoomPopoverOpen(false);
  _lbPersistBool('vireo.lb.infoVisible', _lbInfoVisible);
  _lbApplyInfoVisibility();
}

function _lbApplyChromeVisibility() {
  var overlay = document.getElementById('lightboxOverlay');
  if (overlay) overlay.classList.toggle('lb-hide-chrome', !_lbChromeVisible);
  var input = document.getElementById('lightboxToggleChrome');
  if (input) input.checked = _lbChromeVisible;
}

function toggleLightboxChrome() {
  _lbChromeVisible = !_lbChromeVisible;
  if (!_lbChromeVisible) _lbSetZoomPopoverOpen(false);
  _lbPersistBool('vireo.lb.chromeVisible', _lbChromeVisible);
  _lbApplyChromeVisibility();
}

function _lbPositionViewPanel() {
  var panel = document.getElementById('lightboxViewPanel');
  var btn = document.getElementById('lightboxViewBtn');
  if (!panel || !btn || !panel.classList.contains('open')) return;
  // The actions wrap and conditionally gain controls as photos change. Keep
  // the panel attached to its button even when that happens while it is open.
  var gutter = 8;
  var btnRect = btn.getBoundingClientRect();
  var maxLeft = Math.max(gutter, window.innerWidth - panel.offsetWidth - gutter);
  var desiredBottom = window.innerHeight - btnRect.top + gutter;
  var maxBottom = Math.max(gutter, window.innerHeight - panel.offsetHeight - gutter);
  panel.style.left = Math.max(gutter, Math.min(btnRect.left, maxLeft)) + 'px';
  panel.style.bottom = Math.max(gutter, Math.min(desiredBottom, maxBottom)) + 'px';
}

var _lbViewPanelPositionFrame = null;
function _lbScheduleViewPanelPosition() {
  if (_lbViewPanelPositionFrame !== null) return;
  _lbViewPanelPositionFrame = requestAnimationFrame(function() {
    _lbViewPanelPositionFrame = null;
    _lbPositionViewPanel();
  });
}

function toggleLightboxViewMenu(force) {
  var panel = document.getElementById('lightboxViewPanel');
  var btn = document.getElementById('lightboxViewBtn');
  if (!panel || !btn) return;
  var open = typeof force === 'boolean' ? force : !panel.classList.contains('open');
  panel.classList.toggle('open', open);
  btn.setAttribute('aria-expanded', open ? 'true' : 'false');
  if (!open) return;
  // Refresh every row so the menu reflects the current photo's state.
  _lbApplyBoxesVisibility();
  _lbApplyMaskToggleButton();
  _lbApplyEyeVisibility();
  _lbApplyTrackEyeState();
  _lbApplyInfoVisibility();
  _lbApplyChromeVisibility();
  // The overlay is position:fixed inset:0, so viewport coords map directly.
  _lbPositionViewPanel();
}

(function() {
  var actions = document.getElementById('lightboxActions');
  var viewBtn = document.getElementById('lightboxViewBtn');
  window.addEventListener('resize', _lbScheduleViewPanelPosition);
  document.addEventListener('fullscreenchange', _lbScheduleViewPanelPosition);
  document.addEventListener('webkitfullscreenchange', _lbScheduleViewPanelPosition);
  if (window.ResizeObserver) {
    var viewLayoutObserver = new ResizeObserver(_lbScheduleViewPanelPosition);
    if (actions) viewLayoutObserver.observe(actions);
    if (viewBtn) viewLayoutObserver.observe(viewBtn);
  }
  if (window.MutationObserver && actions) {
    var viewLayoutMutationObserver = new MutationObserver(_lbScheduleViewPanelPosition);
    viewLayoutMutationObserver.observe(actions, {
      attributes: true,
      childList: true,
      characterData: true,
      subtree: true
    });
  }
})();

function lightboxSetFlagAction(flag) {
  if (_lightboxCurrentId == null) return;
  var pid = _lightboxCurrentId;
  // Clicking the button for the photo's current state clears it, so the pair
  // covers flag/reject/unflag without a third button. Match against the
  // *displayed* flag (which includes provisional edits, e.g. Group Review
  // stages picks/rejects before Apply) so a second click on an already-lit
  // button clears the shown state instead of re-applying the same edit.
  var current = _lbNormalizeFlag(_lbDisplayedFlagFor(pid)) || 'none';
  var target = (current === flag) ? 'none' : flag;
  // Same dispatch order as the keyboard shortcuts: the misses page intercepts
  // flag writes to keep its local model in sync.
  if (
    typeof window.handleMissesLightboxFlagShortcut === 'function' &&
    window.handleMissesLightboxFlagShortcut(pid, target) === true
  ) return;
  _lbApplyFlag(pid, target);
}

function _lbApplyEyeVisibility() {
  var container = document.getElementById('lightboxEye');
  var available = _lbSourceOverlaysAvailable();
  if (container) {
    container.style.display =
      (_lbEyeVisible && available && container.childElementCount > 0) ? 'block' : 'none';
  }
  var input = document.getElementById('lightboxToggleEye');
  if (input) {
    input.disabled = !available;
    input.checked = _lbEyeVisible;
  }
  var note = document.getElementById('lightboxEyeNote');
  if (note) note.textContent = available ? '' : 'unavailable for edited photos';
}

function toggleLightboxEye() {
  _lbEyeVisible = !_lbEyeVisible;
  _lbPersistBool('vireo.lb.eyeVisible', _lbEyeVisible);
  _lbApplyEyeVisibility();
}

function _lbPhotoData(photoId) {
  if (photoId == null) return null;
  var cached = _lbPhotoDataByPhoto[String(photoId)];
  if (cached) return cached;
  return _lightboxPhotoList.find(function(photo) {
    return String(photo.id) === String(photoId);
  }) || null;
}

function _lbPhotoEyePoint(photoId, photo) {
  photo = photo || _lbPhotoData(photoId);
  if (!photo || photo.eye_x == null || photo.eye_y == null) return null;
  var x = Number(photo.eye_x);
  var y = Number(photo.eye_y);
  if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
  return _lbTransformPointByRecipe(
    x,
    y,
    _lbEditRecipeByPhoto[String(photoId)] || photo.edit_recipe
  );
}

function _lbApplyTrackEyeState() {
  var input = document.getElementById('lightboxTrackEye');
  if (!input) return;
  var photo = _lbPhotoData(_lightboxCurrentId);
  var eyeKnown = !!(
    photo && Object.prototype.hasOwnProperty.call(photo, 'eye_x')
  );
  var hasEye = !!_lbPhotoEyePoint(_lightboxCurrentId, photo);
  var available = hasEye && _lbSourceOverlaysAvailable();
  input.checked = _lbTrackEyeEnabled;
  var note = document.getElementById('lightboxTrackEyeNote');
  if (note) {
    if (_lbTrackEyeEnabled && !available) {
      note.textContent = eyeKnown ? 'paused for this photo' : 'waiting for eye';
    } else {
      note.textContent = '';
    }
  }
  var row = input.closest('.lb-view-row');
  if (row) {
    if (_lbTrackEyeEnabled && !available) {
      row.title = eyeKnown
        ? 'Eye tracking is on but unavailable for this photo'
        : 'Eye tracking is on; waiting for this photo\'s eye metadata';
    } else if (!_lbTrackEyeEnabled && !available) {
      row.title = 'Turn on eye tracking; it will start when a usable eye keypoint is available';
    } else {
      row.title = 'Keep the detected eye in the same screen position while navigating';
    }
  }
}

function toggleLightboxTrackEye() {
  _lbTrackEyeEnabled = !_lbTrackEyeEnabled;
  _lbPersistBool('vireo.lb.trackEye', _lbTrackEyeEnabled);
  _lbPendingEyeTrack = null;
  _lbEyeTrackScreenAnchor = null;
  if (_lbTrackEyeEnabled) _lbCaptureEyeTrackingAnchor();
  _lbApplyTrackEyeState();
}

/* Species color palette for lightbox bounding boxes */
var _lbSpeciesColors = {};
var _lbColorPalette = [
  '#24E5CA', '#f0c040', '#e74c3c', '#3498db', '#9b59b6',
  '#1abc9c', '#e67e22', '#2ecc71', '#e84393', '#00cec9'
];
var _lbNextColorIdx = 0;

function _lbGetSpeciesColor(species) {
  if (!_lbSpeciesColors[species]) {
    _lbSpeciesColors[species] = _lbColorPalette[_lbNextColorIdx % _lbColorPalette.length];
    _lbNextColorIdx++;
  }
  return _lbSpeciesColors[species];
}

function _lbRenderEyeCrosshair(photo) {
  // Draws a crosshair at the detected eye keypoint on the current lightbox
  // photo. eye_x / eye_y are stored normalized 0-1 against the oriented
  // (EXIF-transposed) image, same as detection boxes, so we can map
  // directly to percentage without touching photo.width/photo.height
  // (which come from the un-oriented sensor tag and would swap axes on
  // orientation 6/8 rotations).
  var container = document.getElementById('lightboxEye');
  if (!container) return;
  container.innerHTML = '';
  if (!photo || photo.eye_x == null || photo.eye_y == null) {
    container.style.display = 'none';
    return;
  }
  var point = _lbTransformPointByRecipe(
    Number(photo.eye_x),
    Number(photo.eye_y),
    _lbEditRecipeByPhoto[String(photo.id || _lightboxCurrentId)]
  );
  var xPct = point.x * 100;
  var yPct = point.y * 100;
  var marker = document.createElement('div');
  marker.className = 'lb-eye-crosshair';
  marker.style.left = xPct.toFixed(3) + '%';
  marker.style.top = yPct.toFixed(3) + '%';
  if (photo.eye_conf != null) {
    var label = document.createElement('span');
    label.textContent = 'eye ' + Math.round(photo.eye_conf * 100) + '%';
    marker.appendChild(label);
  }
  container.appendChild(marker);
  _lbApplyEyeVisibility();
  _lbApplyTrackEyeState();
}

function _lbLoadDetections(photoId) {
  var container = document.getElementById('lightboxDetections');
  if (!container) return;
  container.innerHTML = '';
  if (!_lbSourceOverlaysAvailable()) {
    _lbApplyBoxesVisibility();
    return;
  }
  fetch('/api/detections/' + photoId)
    .then(function(r) { return r.json(); })
    .then(function(detections) {
      if (!detections || detections.length === 0) return;
      // Only render if still viewing the same photo
      if (_lightboxCurrentId !== photoId) return;
      if (!_lbSourceOverlaysAvailable()) {
        container.innerHTML = '';
        _lbApplyBoxesVisibility();
        return;
      }
      var recipe = _lbEditRecipeByPhoto[String(photoId)] || _lbCurrentEditRecipe;
      detections.forEach(function(det) {
        if (det.box_x == null || det.box_y == null) return;
        var color = _lbGetSpeciesColor(det.category || 'animal');
        var transformed = _lbTransformBoxByRecipe({
          x: det.box_x,
          y: det.box_y,
          w: det.box_w,
          h: det.box_h,
        }, recipe);
        var box = document.createElement('div');
        box.className = 'lb-detection-box';
        box.style.cssText = 'left:' + (transformed.x * 100).toFixed(2) + '%;top:' + (transformed.y * 100).toFixed(2) + '%;width:' + (transformed.w * 100).toFixed(2) + '%;height:' + (transformed.h * 100).toFixed(2) + '%;border-color:' + color + ';';
        var conf = det.detector_confidence ? Math.round(det.detector_confidence * 100) + '%' : '';
        if (conf) {
          box.innerHTML = '<span class="lb-detection-label" style="background:' + color + ';color:#0A1F2E;">' + conf + '</span>';
        }
        container.appendChild(box);
      });
    })
    .catch(function() {});
}

/* ---- SAM mask overlay ---------------------------------------------------
 * The lightbox lets the user A/B-compare SAM mask variants without
 * changing which one drives downstream scoring. The dropdown defaults
 * to "active" (whatever photos.active_mask_variant points at) and lists
 * one option per variant the photo has under photo_masks. Selecting a
 * variant only swaps the overlay src; it never POSTs back to the server.
 *
 * Pages that don't surface masks at all (e.g. browse before the pipeline
 * has run) still get the lightbox — the dropdown stays hidden until at
 * least one variant comes back from /api/photos/<id>/masks, mirroring
 * how the detection-box toggle stays present but inert when there are
 * no detections.
 */
var _lbMaskActiveVariant = null;     // name of the photo's currently-active variant
var _lbMaskAvailable = [];           // [{variant, url, created_at}, ...]
var _lbMaskCurrentUrl = null;
var _lbMaskLoadSeq = 0;

function _lbResetMaskOverlay() {
  var img = document.getElementById('lightboxMaskOverlay');
  _lbMaskLoadSeq++;
  if (img) {
    img.classList.remove('show');
    img.onload = null;
    img.onerror = null;
    img.removeAttribute('src');
  }
  var ctrls = document.getElementById('lightboxMaskControls');
  if (ctrls) ctrls.classList.remove('has-variants');
  var sel = document.getElementById('lightboxMaskSelect');
  if (sel) sel.innerHTML = '';
  _lbMaskActiveVariant = null;
  _lbMaskAvailable = [];
  _lbMaskCurrentUrl = null;
  _lbApplyMaskToggleButton();
}

function _lbApplyMaskVisibility() {
  var img = document.getElementById('lightboxMaskOverlay');
  if (!img) return;
  _lbApplyMaskToggleButton();
  if (!_lbSourceOverlaysAvailable()) {
    var ctrls = document.getElementById('lightboxMaskControls');
    if (ctrls) ctrls.classList.remove('has-variants');
  }
  if (!_lbSourceOverlaysAvailable() || !_lbMasksVisible || !_lbMaskCurrentUrl) {
    img.classList.remove('show');
    // A previous call may have assigned onload to re-add `show` once
    // the mask image finished loading. If the user toggles masks off
    // while that request is still in flight, the pending callback
    // would override the hide as soon as it fires. Clear both handlers
    // so an in-flight load can't resurrect the overlay.
    _lbMaskLoadSeq++;
    img.onload = null;
    img.onerror = null;
    // Detach the decoded mask image while hidden. The class normally
    // controls display, but removing src avoids stale blended pixels in
    // WebView/compositor edge cases and forces a clean reload on show.
    img.removeAttribute('src');
    return;
  }
  var expectedUrl = _lbMaskCurrentUrl;
  var seq = ++_lbMaskLoadSeq;
  // onerror guards against the file being deleted out from under us
  // between the /api/photos/<id>/masks call and the GET. The DB row
  // exists but the file is gone → silently hide instead of leaving a
  // broken-image icon over the photo.
  img.onerror = function() {
    if (seq === _lbMaskLoadSeq && _lbMaskCurrentUrl === expectedUrl) {
      img.classList.remove('show');
    }
  };
  img.onload = function() {
    // Re-check the toggle: the user may have hidden masks between
    // src assignment and the load resolving. Also re-check the visual
    // transition: while the outgoing bitmap is still frozen on screen,
    // painting the incoming photo's mask would show it over the previous
    // image. handleInitialImageLoad re-runs _lbApplyMaskVisibility once
    // the transition clears, so the mask surfaces as soon as it is safe.
    if (
      seq === _lbMaskLoadSeq
      && _lbMasksVisible
      && _lbMaskCurrentUrl === expectedUrl
      && !_lbVisualTransitionPending
    ) {
      img.classList.add('show');
    }
  };
  if (img.getAttribute('src') !== expectedUrl) {
    img.classList.remove('show');
    img.src = expectedUrl;
  } else if (img.naturalWidth > 0 && !_lbVisualTransitionPending) {
    // Same URL and we know it loaded successfully — safe to re-show
    // without re-fetching, once the visual transition has cleared.
    img.classList.add('show');
  }
  // If src matches but naturalWidth is 0, the previous load either failed
  // (onerror already hid the overlay; re-adding `show` would surface a
  // broken-image icon) or is still in flight (the onload assigned above
  // will add `show` when the request resolves). Either way, don't add
  // `show` here.
}

function _lbApplyMaskUrl(url) {
  // Set the selected overlay URL. Visibility is controlled separately so
  // users can keep their mask preference across photos and sessions.
  var img = document.getElementById('lightboxMaskOverlay');
  _lbMaskCurrentUrl = url || null;
  if (!url && img) img.removeAttribute('src');
  _lbApplyMaskVisibility();
}

function _lbApplyMaskToggleButton() {
  var input = document.getElementById('lightboxToggleMasks');
  if (!input) return;
  var available = _lbSourceOverlaysAvailable();
  input.disabled = !available;
  input.checked = _lbMasksVisible;
  var note = document.getElementById('lightboxMasksNote');
  if (note) note.textContent = available ? '' : 'unavailable for edited photos';
}

function toggleLightboxMasks() {
  _lbMasksVisible = !_lbMasksVisible;
  _lbPersistBool('vireo.lb.masksVisible', _lbMasksVisible);
  _lbApplyMaskToggleButton();
  _lbApplyMaskVisibility();
}

function _lbOnMaskVariantChange() {
  var sel = document.getElementById('lightboxMaskSelect');
  if (!sel) return;
  if (_lbCurrentRecipeHasOrientation()) {
    _lbApplyMaskUrl(null);
    return;
  }
  var choice = sel.value;
  if (choice === '__active__') {
    if (!_lbMaskActiveVariant) {
      // No active variant → nothing to overlay.
      _lbApplyMaskUrl(null);
      return;
    }
    var match = _lbMaskAvailable.find(function(v) {
      return v.variant === _lbMaskActiveVariant;
    });
    _lbApplyMaskUrl(match ? match.url : null);
    return;
  }
  var picked = _lbMaskAvailable.find(function(v) { return v.variant === choice; });
  _lbApplyMaskUrl(picked ? picked.url : null);
}

function _lbLoadMaskVariants(photoId) {
  // Fetch the photo's available mask variants and populate the
  // dropdown. Defaults the selection to "active" (which displays the
  // photo's active_mask_variant overlay if one exists).
  var sel = document.getElementById('lightboxMaskSelect');
  var ctrls = document.getElementById('lightboxMaskControls');
  if (!sel || !ctrls) return;
  // Reset before the async fetch resolves so a fast nav doesn't show
  // stale options for the previous photo.
  _lbResetMaskOverlay();

  fetch('/api/photos/' + photoId + '/masks')
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(data) {
      // Bail if the user navigated to a different photo while the
      // request was in flight.
      if (!data || _lightboxCurrentId !== photoId) return;
      if (!_lbSourceOverlaysAvailable()) return;
      _lbMaskActiveVariant = data.active || null;
      _lbMaskAvailable = data.variants || [];
      if (_lbCurrentRecipeHasOrientation()) {
        _lbApplyMaskUrl(null);
        return;
      }
      if (_lbMaskAvailable.length === 0) {
        // No masks for this photo — keep the control hidden.
        return;
      }
      // Build options: "active" first, then one per variant. Variant
      // names come from workspace config and must be treated as
      // untrusted — render via DOM APIs so a name containing HTML
      // can't escape into the page as stored DOM XSS.
      var activeLabel = _lbMaskActiveVariant
        ? 'active (' + _lbMaskActiveVariant + ')'
        : 'active (n/a)';
      sel.textContent = '';
      var activeOpt = document.createElement('option');
      activeOpt.value = '__active__';
      activeOpt.textContent = activeLabel;
      sel.appendChild(activeOpt);
      _lbMaskAvailable.forEach(function(v) {
        var opt = document.createElement('option');
        opt.value = v.variant;
        opt.textContent = v.variant;
        sel.appendChild(opt);
      });
      sel.value = '__active__';
      ctrls.classList.add('has-variants');
      // Apply the default ("active") overlay so the user sees something
      // the moment masks become available.
      _lbOnMaskVariantChange();
    })
    .catch(function() { /* network error → leave control hidden */ });
}

function _cropCloneRecipe(recipe) {
  if (!recipe || typeof recipe !== 'object') return {};
  try { return JSON.parse(JSON.stringify(recipe)); }
  catch (_) { return {}; }
}

function _cropDefaultCrop(recipe) {
  if (!recipe.crop || typeof recipe.crop !== 'object') {
    recipe.crop = { x: 0, y: 0, w: 1, h: 1 };
  }
  recipe.crop.x = Number(recipe.crop.x) || 0;
  recipe.crop.y = Number(recipe.crop.y) || 0;
  recipe.crop.w = Number(recipe.crop.w) || 1;
  recipe.crop.h = Number(recipe.crop.h) || 1;
  recipe.crop = _cropClampBox(recipe.crop);
  return recipe.crop;
}

function _cropClampBox(crop) {
  var min = 0.02;
  var x = Number(crop.x) || 0;
  var y = Number(crop.y) || 0;
  var w = Number(crop.w) || 1;
  var h = Number(crop.h) || 1;
  w = Math.max(min, Math.min(1, w));
  h = Math.max(min, Math.min(1, h));
  x = Math.max(0, Math.min(1 - w, x));
  y = Math.max(0, Math.min(1 - h, y));
  return { x: x, y: y, w: w, h: h };
}

function _cropIsFullFrame(crop) {
  return !crop || (
    Math.abs((Number(crop.x) || 0)) < 0.0005 &&
    Math.abs((Number(crop.y) || 0)) < 0.0005 &&
    Math.abs((Number(crop.w) || 1) - 1) < 0.0005 &&
    Math.abs((Number(crop.h) || 1) - 1) < 0.0005
  );
}

function _cropSetStatus(message, isError) {
  var el = document.getElementById('cropEditorStatus');
  if (!el) return;
  el.textContent = message || '';
  el.classList.toggle('error', !!isError);
}

function _cropIsActiveSession(photoId, session) {
  return _cropPhotoId === photoId && _cropSessionSeq === session;
}

function _cropDisplayRecipe() {
  var recipe = _cropCloneRecipe(_cropRecipe);
  delete recipe.crop;
  return recipe;
}

function _cropSyncControls() {
  if (!_cropRecipe) return;
  var value = Number(_cropRecipe.straighten || 0);
  var range = document.getElementById('cropStraightenRange');
  var input = document.getElementById('cropStraightenInput');
  if (range) range.value = Math.max(-10, Math.min(10, value));
  if (input) input.value = String(Math.round(value * 10) / 10);
}

function _cropRenderBox() {
  if (!_cropRecipe) return;
  var img = document.getElementById('cropEditorImg');
  var box = document.getElementById('cropBox');
  if (!img || !box || !img.complete || !img.clientWidth || !img.clientHeight) return;
  var crop = _cropDefaultCrop(_cropRecipe);
  box.style.left = (crop.x * img.clientWidth) + 'px';
  box.style.top = (crop.y * img.clientHeight) + 'px';
  box.style.width = (crop.w * img.clientWidth) + 'px';
  box.style.height = (crop.h * img.clientHeight) + 'px';
}

function _cropUpdatePreview() {
  if (!_cropPhotoId || !_cropRecipe) return;
  var img = document.getElementById('cropEditorImg');
  if (!img) return;
  var seq = ++_cropPreviewSeq;
  var recipe = _cropDisplayRecipe();
  _cropSetStatus('Loading preview...');
  img.onload = function() {
    if (seq !== _cropPreviewSeq) return;
    _cropRenderBox();
    _cropSetStatus('');
  };
  img.onerror = function() {
    if (seq !== _cropPreviewSeq) return;
    _cropSetStatus('Could not render preview', true);
  };
  img.src = '/photos/' + _cropPhotoId + '/edit-preview?size=1920&recipe=' +
    encodeURIComponent(JSON.stringify(recipe)) + '&v=' + seq;
}

function _cropSchedulePreview() {
  if (_cropPreviewTimer) clearTimeout(_cropPreviewTimer);
  _cropPreviewTimer = setTimeout(function() {
    _cropPreviewTimer = null;
    _cropUpdatePreview();
  }, 120);
}

function _cropNormalizeRecipeForSave() {
  var recipe = _cropCloneRecipe(_cropRecipe);
  var crop = _cropDefaultCrop(recipe);
  delete recipe.version;
  if (!recipe.rotation) delete recipe.rotation;
  if (Math.abs(Number(recipe.straighten || 0)) < 0.0001) delete recipe.straighten;
  else recipe.straighten = Math.round(Number(recipe.straighten) * 10000) / 10000;
  if (_cropIsFullFrame(crop)) delete recipe.crop;
  else {
    recipe.crop = {
      x: Math.round(crop.x * 1000000) / 1000000,
      y: Math.round(crop.y * 1000000) / 1000000,
      w: Math.round(crop.w * 1000000) / 1000000,
      h: Math.round(crop.h * 1000000) / 1000000,
    };
  }
  return recipe;
}

function _cropInitEvents() {
  if (window._cropEventsReady) return;
  window._cropEventsReady = true;
  var range = document.getElementById('cropStraightenRange');
  var input = document.getElementById('cropStraightenInput');
  function setStraighten(value) {
    if (!_cropRecipe) return;
    var v = Number(value);
    if (!Number.isFinite(v)) v = 0;
    v = Math.max(-45, Math.min(45, v));
    _cropRecipe.straighten = v;
    _cropSyncControls();
    _cropSchedulePreview();
  }
  if (range) range.addEventListener('input', function() { setStraighten(range.value); });
  if (input) input.addEventListener('input', function() { setStraighten(input.value); });

  var box = document.getElementById('cropBox');
  if (box) {
    box.addEventListener('pointerdown', function(e) {
      if (!_cropRecipe) return;
      var img = document.getElementById('cropEditorImg');
      if (!img || !img.clientWidth || !img.clientHeight) return;
      var handle = e.target && e.target.dataset ? e.target.dataset.handle : '';
      _cropDrag = {
        handle: handle || 'move',
        startX: e.clientX,
        startY: e.clientY,
        startCrop: _cropCloneRecipe({ crop: _cropDefaultCrop(_cropRecipe) }).crop,
        imgW: img.clientWidth,
        imgH: img.clientHeight,
      };
      box.setPointerCapture(e.pointerId);
      e.preventDefault();
      e.stopPropagation();
    });
  }

  document.addEventListener('pointermove', function(e) {
    if (!_cropDrag || !_cropRecipe) return;
    var dx = (e.clientX - _cropDrag.startX) / _cropDrag.imgW;
    var dy = (e.clientY - _cropDrag.startY) / _cropDrag.imgH;
    var c = _cropCloneRecipe({ crop: _cropDrag.startCrop }).crop;
    var min = 0.02;
    if (_cropDrag.handle === 'move') {
      c.x += dx;
      c.y += dy;
    } else {
      if (_cropDrag.handle.indexOf('w') !== -1) {
        c.x += dx;
        c.w -= dx;
      }
      if (_cropDrag.handle.indexOf('e') !== -1) c.w += dx;
      if (_cropDrag.handle.indexOf('n') !== -1) {
        c.y += dy;
        c.h -= dy;
      }
      if (_cropDrag.handle.indexOf('s') !== -1) c.h += dy;
      if (c.w < min) {
        if (_cropDrag.handle.indexOf('w') !== -1) c.x = _cropDrag.startCrop.x + _cropDrag.startCrop.w - min;
        c.w = min;
      }
      if (c.h < min) {
        if (_cropDrag.handle.indexOf('n') !== -1) c.y = _cropDrag.startCrop.y + _cropDrag.startCrop.h - min;
        c.h = min;
      }
    }
    _cropRecipe.crop = _cropClampBox(c);
    _cropRenderBox();
    e.preventDefault();
  });
  document.addEventListener('pointerup', function() {
    _cropDrag = null;
  });
  window.addEventListener('resize', function() {
    if (_cropPhotoId) _cropRenderBox();
  });
}

async function openCropEditor() {
  if (_lbGuardReadOnly()) return false;
  if (!_lightboxCurrentId) return;
  _cropInitEvents();
  var requestedPhotoId = _lightboxCurrentId;
  _cropPhotoId = requestedPhotoId;
  var session = ++_cropSessionSeq;
  var saveBtn = document.getElementById('cropSaveBtn');
  if (saveBtn) saveBtn.disabled = false;
  _cropSetStatus('Loading recipe...');
  try {
    _lbFlushPendingAdjustmentSave();
    await _lbWaitForAdjustmentSaveIdle(requestedPhotoId);
    if (_lightboxCurrentId !== requestedPhotoId || !_cropIsActiveSession(requestedPhotoId, session)) return;
    var data = await safeFetch('/api/photos/' + requestedPhotoId + '/edit-recipe', {}, { toast: false });
    if (_lightboxCurrentId !== requestedPhotoId || !_cropIsActiveSession(requestedPhotoId, session)) return;
    _cropRecipe = _cropCloneRecipe(_lbEditRecipeLoaded ? _lbEditRecipe : (data.recipe || {}));
    _cropDefaultCrop(_cropRecipe);
    _cropSyncControls();
    var modal = document.getElementById('cropEditorModal');
    if (_cropEscToken) Keymap.popEsc(_cropEscToken);
    _cropEscToken = Keymap.pushEsc(function() { closeCropEditor(); });
    if (modal) modal.classList.add('open');
    _cropUpdatePreview();
  } catch (e) {
    if (_cropIsActiveSession(requestedPhotoId, session)) {
      _cropSetStatus(e.message || 'Could not load recipe', true);
    }
  }
}

function closeCropEditor(event) {
  if (event) {
    event.preventDefault();
    event.stopPropagation();
  }
  var modal = document.getElementById('cropEditorModal');
  if (modal) modal.classList.remove('open');
  if (_cropEscToken) {
    Keymap.popEsc(_cropEscToken);
    _cropEscToken = null;
  }
  _cropSessionSeq++;
  _cropPhotoId = null;
  _cropRecipe = null;
  _cropDrag = null;
  if (_cropPreviewTimer) {
    clearTimeout(_cropPreviewTimer);
    _cropPreviewTimer = null;
  }
}

function cropRotate(delta) {
  if (!_cropRecipe) return;
  var current = Number(_cropRecipe.rotation || 0);
  var next = (current + delta) % 360;
  if (next < 0) next += 360;
  _cropRecipe.rotation = next;
  cropResetFrame();
  _cropSchedulePreview();
}

function cropResetFrame() {
  if (!_cropRecipe) return;
  _cropRecipe.crop = { x: 0, y: 0, w: 1, h: 1 };
  _cropRenderBox();
}

function _cropBoxForAspect(crop, normalizedRatio) {
  var current = _cropClampBox(crop);
  if (!Number.isFinite(normalizedRatio) || normalizedRatio <= 0) return current;
  var centerX = current.x + current.w / 2;
  var centerY = current.y + current.h / 2;
  var w = current.w;
  var h = current.h;
  // Fit the requested ratio inside the user's current crop instead of
  // replacing their latest drag with a new full-frame-centered crop. Only
  // the minimum-size floor below is allowed to grow beyond that selection.
  if (w / h > normalizedRatio) w = h * normalizedRatio;
  else h = w / normalizedRatio;
  // Keep the shared editor's per-axis 2% clamp from changing the ratio.
  var minScale = Math.max(1, 0.02 / w, 0.02 / h);
  w *= minScale;
  h *= minScale;
  return _cropClampBox({
    x: centerX - w / 2,
    y: centerY - h / 2,
    w: w,
    h: h,
  });
}

function cropSetAspect(aspect) {
  if (!_cropRecipe) return;
  var img = document.getElementById('cropEditorImg');
  if (!img || !img.clientWidth || !img.clientHeight) return;
  var imageAspect = img.clientWidth / img.clientHeight;
  var normalizedRatio = aspect / imageAspect;
  _cropRecipe.crop = _cropBoxForAspect(
    _cropDefaultCrop(_cropRecipe), normalizedRatio
  );
  _cropRenderBox();
}

async function saveCropEditor() {
  if (_lbGuardReadOnly()) return false;
  if (!_cropPhotoId || !_cropRecipe) return;
  var pid = _cropPhotoId;
  var session = _cropSessionSeq;
  var btn = document.getElementById('cropSaveBtn');
  if (btn) btn.disabled = true;
  _cropSetStatus('Saving...');
  try {
    var recipe = _cropNormalizeRecipeForSave();
    var data = await safeFetch('/api/photos/' + pid + '/edit-recipe', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ recipe: recipe }),
    }, { toast: false });
    var updates = {};
    updates[String(pid)] = data.recipe;
    _lbRefreshEditRecipeCache(updates);
    var p = _lightboxPhotoList.find(function(x) { return x.id === pid; });
    if (_cropIsActiveSession(pid, session)) {
      closeCropEditor();
      if (_lightboxCurrentId === pid) {
        var filename = p ? p.filename : document.getElementById('lightboxFilename').textContent;
        openLightbox(pid, filename, _lightboxPhotoList);
      }
      showToast('Crop saved', 'success');
    }
  } catch (e) {
    if (_cropIsActiveSession(pid, session)) {
      _cropSetStatus(e.message || 'Could not save crop', true);
    }
  } finally {
    if (btn && _cropIsActiveSession(pid, session)) btn.disabled = false;
  }
}

async function cropClearEdits() {
  if (!_cropPhotoId) return;
  var pid = _cropPhotoId;
  var session = _cropSessionSeq;
  _cropSetStatus('Clearing...');
  try {
    await safeFetch('/api/photos/' + pid + '/edit-recipe', {
      method: 'DELETE',
    }, { toast: false });
    var updates = {};
    updates[String(pid)] = null;
    _lbRefreshEditRecipeCache(updates);
    var p = _lightboxPhotoList.find(function(x) { return x.id === pid; });
    if (_cropIsActiveSession(pid, session)) {
      closeCropEditor();
      if (_lightboxCurrentId === pid) {
        var filename = p ? p.filename : document.getElementById('lightboxFilename').textContent;
        openLightbox(pid, filename, _lightboxPhotoList);
      }
      showToast('Edits cleared', 'success');
    }
  } catch (e) {
    if (_cropIsActiveSession(pid, session)) {
      _cropSetStatus(e.message || 'Could not clear edits', true);
    }
  }
}

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
  if (_lightboxCurrentId != null && refreshedPhotoIds.indexOf(Number(_lightboxCurrentId)) !== -1) {
    _lbReloadCurrentRenderAfterEdit(Number(_lightboxCurrentId));
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
    _lightboxCurrentId != null &&
    String(_lightboxCurrentId) === String(photoId)
  );
  options = options || {};
  var nextReadOnly = Object.prototype.hasOwnProperty.call(options, 'readOnly')
    ? !!options.readOnly
    : (alreadyOpen && _lbReadOnly);
  var nextReadOnlyMessage = Object.prototype.hasOwnProperty.call(options, 'readOnlyMessage')
    ? options.readOnlyMessage
    : _lbReadOnlyMessage;
  if (!alreadyOpen) _lbLastNavDelta = 1;
  _lbCancelOriginalPreload();
  _lbCancelAdjacentPreloadTimer();
  _lbProgressiveTargetKey = null;
  // Each photo earns its own quiet period. The outgoing photo's swap is dead as
  // soon as _lbOpenSeq bumps below, so neither its pending tier nor an
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
  if (alreadyOpen && _lightboxCurrentId != null) {
    _lbSaveViewportState(_lightboxCurrentId);
  }
  if (alreadyOpen) Keymap.popEsc(window._lbEscToken);
  window._lbEscToken = Keymap.pushEsc(function() { closeLightbox(); });
  if (!alreadyOpen) Keymap.lockBodyScroll();
  _lbOpenSeq += 1;
  var openSeq = _lbOpenSeq;
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
  // flight: _lightboxCurrentId then names the incoming photo, not necessarily
  // the bitmap that is still visible.
  _lbVisualTransitionPending = alreadyOpen && !reopeningVisiblePhoto;
  _lbSetPhotoTransitionPending(_lbVisualTransitionPending);
  // A previous open's deferred overlay closure captured the previous photo id
  // and metadata. If this open's image resolves before its own metadata does,
  // draining that stale closure would render the previous photo's detections
  // and eye marker over the incoming bitmap.
  _lbDeferredOverlayApply = null;

  _lightboxCurrentId = photoId;
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
      _lightboxCommittedId = photoId;
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
  var warmInitial = _lbDecodedInitialPreview(photoId, initialTarget);
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
  if (_lbSwapTimer) { clearTimeout(_lbSwapTimer); _lbSwapTimer = null; }
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
      if (!data || _lightboxCurrentId !== photoId || _lbOpenSeq !== openSeq) return;
      var pairWasKnown = !!_vireoPairKnownByPhoto[String(photoId)];
      if (typeof window.vireoRememberPhotoPair === 'function') {
        window.vireoRememberPhotoPair(data);
      }
      if (typeof window.vireoUpdatePairSourceControls === 'function') {
        window.vireoUpdatePairSourceControls(photoId);
      }
      if (!pairWasKnown && _vireoPairKnownByPhoto[String(photoId)]) {
        var pairImg = document.getElementById('lightboxImg');
        var pairLoad = function() {
          pairImg.removeEventListener('load', pairLoad);
          pairImg.removeEventListener('error', pairError);
          _vireoPairSourceImageLoaded(photoId, 'jpeg', pairImg);
        };
        var pairError = function() {
          pairImg.removeEventListener('load', pairLoad);
          pairImg.removeEventListener('error', pairError);
        };
        pairImg.addEventListener('load', pairLoad);
        pairImg.addEventListener('error', pairError);
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
        _lbScheduleAdjacentPhoto(_lbCurrentSrcKey);
        if (_lbCurrentSrcKey === 'full') _lbScheduleOriginalPreload(photoId);
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
      if (_lightboxCurrentId !== photoId || _lbOpenSeq !== openSeq) return;
      _lbSetAdjustmentStatus('Could not load recipe', true);
    });

  // Give up on ever displaying this photo, without leaving the lightbox frozen:
  // while the transition is pending the metadata callback skips layout updates
  // and _lbSaveViewportState treats the incoming photo as mid-flight. The
  // identity commit comes first on purpose -- releasing the controls while the
  // filename, counter and _lightboxCommittedId still name the outgoing photo
  // would let the user act on a photo the UI is not showing. The outgoing
  // bitmap is dropped for the same reason: leaving it painted under the
  // incoming filename would let a flag or delete land on a photo the user
  // cannot see.
  function abandonInitialLoad() {
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
    if (_lbPendingInitialLoadCommit === handleInitialImageLoad) _lbClearPendingInitialLoad();
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
    img.onload = null;
    img.onerror = null;
    if (_lightboxCurrentId !== photoId || _lbOpenSeq !== openSeq) return;
    commitVisiblePhotoIdentity();
    // Only record the /full tier size if the currently loaded source is still /full.
    // A quick user zoom can trigger a debounced swap to a higher tier before /full
    // finishes loading, which would otherwise make us record the swapped source's
    // dimensions as the /full threshold.
    if (_lightboxCurrentId === photoId && _lbCurrentSrcKey === 'full' && img.naturalWidth) {
      _lbFullLongEdge = Math.max(img.naturalWidth, img.naturalHeight);
    }
    if (_lightboxCurrentId === photoId && _lbCurrentSrcKey === 'original' && !_lbPhotoW && img.naturalWidth) {
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
      // everything this view can show. (_lbScheduleOriginalPreload only warms a
      // background copy for a later 1:1 -- it does not change these pixels.)
      // A user zoom during the initial /full load (e.g. clicking 1:1) can leave
      // _lbDesiredSrcKey/_lbPreviewLoading describing an upgrade in flight even
      // though _lbProgressiveTargetKey is null. Settling here would arm the fade
      // timer and race the pending upgrade -- confirming a load whose pixels
      // have not arrived yet. The upgrade's own preloader will settle when it
      // lands, or clear the chip when it fails. Match the phase predicate so
      // the chip stays on 'Sharpening…' until then.
      if (!_lbPreviewLoading && !_lbDetailSharpeningPending()) _lbMarkDetailSettled();
      _lbScheduleAdjacentPhoto(_lbCurrentSrcKey);
      if (_lbCurrentSrcKey === 'full') _lbScheduleOriginalPreload(photoId);
    }
  }
  function handleInitialImageError() {
    if (_lightboxCurrentId !== photoId || _lbOpenSeq !== openSeq) return;
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
      if (_lbPendingInitialLoadCommit === handleInitialImageLoad) _lbClearPendingInitialLoad();
      // No further fallback tier is available.
      abandonInitialLoad();
      return;
    }
    _lbOriginalUnavailable = true;
    _lbCurrentSrcKey = 'full';
    _lbDesiredSrcKey = null;
    img.onload = handleInitialImageLoad;
    img.onerror = handleInitialImageError;
    img.src = _lbSrcUrl(photoId, 'full');
  }
  img.onload = handleInitialImageLoad;
  img.onerror = handleInitialImageError;
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
    _lbPendingInitialLoadCommit = handleInitialImageLoad;
    _lbPendingInitialLoadAbandon = abandonInitialLoad;
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
  var idx = _lightboxPhotoList.findIndex(function(p) { return p.id === _lightboxCurrentId; });
  if (idx === -1) return;
  var newIdx = idx + delta;
  if (newIdx < 0 || newIdx >= _lightboxPhotoList.length) {
    try {
      document.dispatchEvent(new CustomEvent('lightbox:navigationboundary', {
        detail: {
          delta: delta,
          photoId: _lightboxCurrentId,
          index: idx,
          photoCount: _lightboxPhotoList.length
        }
      }));
    } catch (_) {}
    return;
  }
  var currentViewportState = _lbSaveViewportState(_lightboxCurrentId);
  var eyeTrackAnchor = _lbCaptureEyeTrackingAnchor();
  var next = _lightboxPhotoList[newIdx];
  _lbLastNavDelta = delta < 0 ? -1 : 1;
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

function _lbMaxZoom() {
  return _lbNativeZoom ? _lbNativeZoom * 4 : 4;
}

function _lbZoomSliderPosition(zoom) {
  var maxZoom = _lbMaxZoom();
  if (maxZoom <= 1.001) return 0;
  return Math.max(0, Math.min(1000, Math.log(Math.max(1, zoom)) / Math.log(maxZoom) * 1000));
}

function _lbZoomFromSliderPosition(position) {
  var maxZoom = _lbMaxZoom();
  if (maxZoom <= 1.001) return 1;
  var ratio = Math.max(0, Math.min(1, Number(position) / 1000));
  return Math.exp(Math.log(maxZoom) * ratio);
}

function _lbZoomDisplayText() {
  if (_lbPending1To1) return 'Loading 1:1';
  if (_lbZoom <= 1.001) return 'Fit';
  if (_lbNativeZoom) return Math.round((_lbZoom / _lbNativeZoom) * 100) + '%';
  var fitMultiple = Math.round(_lbZoom * 10) / 10;
  return fitMultiple + '\u00d7 Fit';
}

function _lbNativeSliderPercent() {
  return _lbNativeZoom ? _lbZoomSliderPosition(_lbNativeZoom) / 10 : 0;
}

function _lbNativeCoincidesWithFit() {
  return !!_lbNativeZoom && _lbNativeZoom <= 1.001;
}

function _lbUpdateZoomControl() {
  var badge = document.getElementById('lightboxZoomBadge');
  var slider = document.getElementById('lightboxZoomSlider');
  var nativeStop = document.getElementById('lightboxZoomNativeStop');
  var fitStop = document.querySelector('.lb-zoom-stop-fit');
  var maxLabel = document.getElementById('lightboxZoomMaxLabel');
  var zoomOut = document.getElementById('lightboxZoomOut');
  var zoomIn = document.getElementById('lightboxZoomIn');
  var text = _lbZoomDisplayText();

  if (badge) {
    badge.textContent = text;
    badge.title = 'Zoom: ' + text + '. Open zoom controls';
  }
  if (slider) {
    slider.value = String(Math.round(_lbZoomSliderPosition(_lbZoom)));
    slider.setAttribute('aria-valuetext', text);
    slider.disabled = !!_lbVisualTransitionPending;
  }
  if (zoomOut) zoomOut.disabled = _lbVisualTransitionPending || _lbZoom <= 1.001;
  if (zoomIn) zoomIn.disabled = _lbVisualTransitionPending || _lbZoom >= _lbMaxZoom() - 0.001;

  var combinedFitNative = _lbNativeCoincidesWithFit();
  if (fitStop) {
    fitStop.textContent = combinedFitNative ? 'Fit \u00b7 1:1' : 'Fit';
    fitStop.disabled = !!_lbVisualTransitionPending;
  }
  if (nativeStop) {
    var nativePosition = _lbNativeSliderPercent();
    nativeStop.disabled = !!_lbVisualTransitionPending;
    // When fit and 1:1 are visually indistinguishable, combine the labels at the
    // left edge. Otherwise keep 1:1 at least 8% along the track so a near-fit
    // native stop remains separate and clickable without overlapping Fit.
    nativeStop.style.display = (_lbNativeZoom && !combinedFitNative) ? 'inline-block' : 'none';
    nativeStop.style.left = Math.max(8, nativePosition) + '%';
  }
  if (maxLabel) {
    maxLabel.textContent = _lbNativeZoom ? '400%' : '4\u00d7 Fit';
    maxLabel.disabled = !!_lbVisualTransitionPending;
  }
}

function _lbSetZoomPopoverOpen(open) {
  var popover = document.getElementById('lightboxZoomPopover');
  var badge = document.getElementById('lightboxZoomBadge');
  if (popover) popover.classList.toggle('open', !!open);
  if (badge) badge.setAttribute('aria-expanded', open ? 'true' : 'false');
}

function toggleLightboxZoomPopover() {
  var popover = document.getElementById('lightboxZoomPopover');
  _lbSetZoomPopoverOpen(!(popover && popover.classList.contains('open')));
}

function setLightboxZoomFromSlider(position) {
  if (_lbVisualTransitionPending) return;
  _lbClearPendingViewportRestore();
  _lbSetZoom(_lbZoomFromSliderPosition(position), null, null);
}

function stepLightboxZoom(direction) {
  if (_lbVisualTransitionPending) return;
  _lbClearPendingViewportRestore();
  _lbSetZoom(_lbZoom * (direction > 0 ? 1.25 : 0.8), null, null);
}

function setLightboxZoomToFit() {
  if (_lbVisualTransitionPending) return;
  _lbClearPendingViewportRestore();
  _lbSetZoom(1.0, null, null);
}

function setLightboxZoomToOneToOne(e) {
  if (_lbVisualTransitionPending) return;
  var img = document.getElementById('lightboxImg');
  if (!img) return;
  _lbClearPendingViewportRestore();

  // If we don't know native zoom yet (e.g. dimensions fetch still pending),
  // fall back to old two-tier behavior: load original, no continuous control.
  if (!_lbNativeZoom) {
    _lbRecomputeNativeZoom();
  }

  // Anchor zoom on click position (or center for the slider's 1:1 stop).
  var rect = img.getBoundingClientRect();
  var anchorX = e && e.clientX ? e.clientX : rect.left + rect.width / 2;
  var anchorY = e && e.clientY ? e.clientY : rect.top + rect.height / 2;
  if (_lbNativeZoom) {
    var desiredSource = _lbPickSourceKey(_lbNativeZoom);
    // Compare source ranks (not exact key equality) so an already-sharper
    // current tier — e.g. /original loaded during a previous 1:1 view when
    // the picked tier for _lbNativeZoom is /2560 — satisfies 1:1 immediately.
    // The exact-key check would otherwise enter the deferred path, leave the
    // badge at 'Loading 1:1' while a lower tier is fetched, and could strand
    // _lbPending1To1 if that fetch failed.
    if (_lbSrcRank(_lbCurrentSrcKey) < _lbSrcRank(desiredSource)) {
      _lbPending1To1 = true;
      _lbPending1To1Anchor = { x: anchorX, y: anchorY };
      _lbScheduleSourceSwap(_lbNativeZoom);
      _lbApplyTransform();
    } else {
      _lbSetZoom(_lbNativeZoom, anchorX, anchorY, true);
      _lbPending1To1 = false;
      _lbPending1To1Anchor = null;
    }
  } else {
    // Dimensions unknown. Load the high-resolution source first, then apply
    // 1:1 after the browser has decoded it; otherwise the user sees an
    // enlarged preview and reads that as a soft 1:1 view.
    _lbPending1To1 = true;
    _lbPending1To1Anchor = { x: anchorX, y: anchorY };
    _lbScheduleSourceSwap(4);
    _lbApplyTransform();
  }
}

function toggleLightboxZoom(e) {
  // Fit to 1:1 toggle. Preserved for the existing 'z' shortcut and image click.
  // A deferred 1:1 leaves _lbZoom at fit while the sharp source loads, so treat
  // that pending state as zoomed and allow a second toggle to cancel it.
  if (_lbZoom > 1.001 || _lbPending1To1) {
    setLightboxZoomToFit();
  } else {
    setLightboxZoomToOneToOne(e);
  }
}

// Scroll wheel and trackpad pinch zoom — cursor-anchored
(function() {
  document.addEventListener('wheel', function(e) {
    var overlay = document.getElementById('lightboxOverlay');
    if (!overlay || !overlay.classList.contains('active')) return;
    var wrap = document.getElementById('lightboxWrap');
    if (!wrap) return;
    // Only react to wheel events over the image wrap
    if (!wrap.contains(e.target) && e.target !== wrap) return;
    if (e.target.closest('.lightbox-zoom-control')) return;

    e.preventDefault();
    if (_lbVisualTransitionPending) return;
    // Trackpad pinch: e.ctrlKey === true; zoom factor more sensitive.
    var scale = e.ctrlKey ? 0.02 : 0.0015;
    var zoomFactor = Math.exp(-e.deltaY * scale);
    var newZoom = _lbZoom * zoomFactor;
    _lbClearPendingViewportRestore();
    _lbSetZoom(newZoom, e.clientX, e.clientY);
  }, { passive: false });
})();

// Viewport and chrome resize: recompute nativeZoom while lightbox is open so
// the fit ratio stays accurate after window/devicePixelRatio changes or the
// bottom bar wraps, grows, or is hidden.
(function() {
  var resizeTimer = null;
  var refreshSourceTier = false;
  var deferredRefreshPending = false;
  function scheduleLightboxLayoutRefresh(updateSourceTier) {
    var overlay = document.getElementById('lightboxOverlay');
    if (!overlay || !overlay.classList.contains('active')) {
      // Discard any refresh that was deferred while the lightbox was open
      // so a stale intent cannot leak into the next session.
      refreshSourceTier = false;
      deferredRefreshPending = false;
      if (resizeTimer) { clearTimeout(resizeTimer); resizeTimer = null; }
      return;
    }
    refreshSourceTier = refreshSourceTier || !!updateSourceTier;
    if (resizeTimer) clearTimeout(resizeTimer);
    resizeTimer = setTimeout(function() {
      resizeTimer = null;
      var shouldUpdateSourceTier = refreshSourceTier;
      refreshSourceTier = false;
      // Arrow navigation deliberately keeps the outgoing bitmap and its
      // transform frozen until the incoming image decodes. A resize queued
      // just before navigation may fire after the incoming metadata has
      // replaced the layout dimensions; applying it here would move the old
      // bitmap using the new photo's geometry. The image load/error handlers
      // recompute and apply the current layout when the transition finishes,
      // but they do not re-select the source tier — so preserve the pending
      // update and let _lbFlushDeferredLightboxLayoutRefresh reschedule it
      // once the transition clears.
      if (_lbVisualTransitionPending) {
        refreshSourceTier = shouldUpdateSourceTier || refreshSourceTier;
        deferredRefreshPending = true;
        return;
      }
      _lbRecomputeNativeZoom();
      // A deferred 1:1 keeps _lbZoom at fit while the high-res source loads.
      // _lbSetZoom clears _lbPending1To1 (and its trailing reschedule retargets
      // the source swap back to /full), so a resize during "loading 1:1" would
      // silently drop the user's zoom request. Capture the deferred intent,
      // re-clamp the actual zoom, then restore it and re-arm the source swap so
      // the snap still completes once the high-res tier is current.
      var pending = _lbPending1To1;
      var pendingAnchor = _lbPending1To1Anchor;
      var pendingDesiredSource = _lbDesiredSrcKey;
      if (pending) {
        _lbPending1To1 = true;
        _lbPending1To1Anchor = pendingAnchor;
        _lbClampPan();
        _lbApplyTransform();
        if (shouldUpdateSourceTier) {
          if (!pendingDesiredSource || pendingDesiredSource === _lbCurrentSrcKey) {
            _lbScheduleSourceSwap(_lbNativeZoom || 4);
          } else {
            _lbDesiredSrcKey = pendingDesiredSource;
          }
          if (_lbDesiredSrcKey === _lbCurrentSrcKey) {
            _lbApplyPendingOneToOneZoom();
          }
        }
        return;
      }
      if (shouldUpdateSourceTier) {
        // Current zoom may now violate clamps; re-run through _lbSetZoom to
        // re-clamp, apply, and select the best source for a new viewport size.
        _lbSetZoom(_lbZoom, null, null);
      } else {
        // A chrome-only resize changes the fit box but must not cancel a
        // source swap or preload that is already in flight.
        _lbClampPan();
        _lbApplyTransform();
        _lbSaveViewportState(_lightboxCurrentId);
      }
    }, 100);
  }

  window.addEventListener('resize', function() {
    scheduleLightboxLayoutRefresh(true);
  });
  if (window.ResizeObserver) {
    var wrap = document.getElementById('lightboxWrap');
    if (wrap) {
      var lightboxLayoutObserver = new ResizeObserver(function() {
        scheduleLightboxLayoutRefresh(false);
      });
      lightboxLayoutObserver.observe(wrap);
    }
  }
  // Called by the image load/error handlers once _lbVisualTransitionPending
  // clears. Reschedules any resize refresh that was skipped during the
  // transition so the incoming photo can pick up a viewport/DPR change we
  // deferred (e.g. selecting a sharper source tier after a window resize).
  window._lbFlushDeferredLightboxLayoutRefresh = function() {
    if (!deferredRefreshPending) return;
    deferredRefreshPending = false;
    scheduleLightboxLayoutRefresh(false);
  };
})();

// Drag to pan when zoomed; single click (no drag) zooms out
(function() {
  var dragging = false, didDrag = false, startX, startY, panStartX, panStartY;
  var DRAG_THRESHOLD = 5;

  document.addEventListener('mousedown', function(e) {
    // Right-click (button 2) opens the context menu — never start a pan.
    if (e.button !== 0) return;
    if (_lbVisualTransitionPending) return;
    var wrap = document.getElementById('lightboxWrap');
    if (!wrap || _lbZoom <= 1.001) return;
    if (e.target.tagName === 'BUTTON' || e.target.closest('button')) return;
    if (e.target.closest('.lightbox-zoom-control')) return;
    if (!wrap.contains(e.target) && e.target !== wrap) return;
    dragging = true;
    didDrag = false;
    startX = e.clientX;
    startY = e.clientY;
    panStartX = _lbPanX;
    panStartY = _lbPanY;
    e.preventDefault();
  });

  document.addEventListener('mousemove', function(e) {
    if (!dragging) return;
    var dx = e.clientX - startX;
    var dy = e.clientY - startY;
    if (!didDrag && (Math.abs(dx) > DRAG_THRESHOLD || Math.abs(dy) > DRAG_THRESHOLD)) {
      didDrag = true;
      // First real pan movement cancels a pending restore so an async
      // image-load retry cannot snap the viewport back mid-drag.
      _lbClearPendingViewportRestore();
    }
    _lbPanX = panStartX + dx;
    _lbPanY = panStartY + dy;
    _lbClampPan();
    _lbApplyTransform();
  });

  document.addEventListener('mouseup', function(e) {
    if (!dragging) return;
    dragging = false;
    if (!didDrag && _lbZoom > 1.001) {
      // Single click without dragging = zoom out to fit
      _lbClearPendingViewportRestore();
      _lbSetZoom(1.0, null, null);
      window._lightboxZoomHandled = true;
    } else if (didDrag) {
      _lbSaveViewportState(_lightboxCurrentId);
    }
  });
})();

function closeLightbox(e) {
  if (e && e.target && e.target.tagName === 'IMG') return;
  // Use the last committed (visible) identity, not `_lightboxCurrentId` —
  // that advances immediately at the start of navigation while the outgoing
  // bitmap is still on screen, so it names a photo the user never actually
  // viewed if the incoming /full is still loading when the user closes.
  var closedPhotoId = _lightboxCommittedId != null
    ? _lightboxCommittedId
    : _lightboxCurrentId;
  _lbFlushPendingAdjustmentSave();
  if (_lightboxCurrentId != null) _lbSaveViewportState(_lightboxCurrentId);
  var wasOpen = !!window._lbEscToken;
  if (wasOpen) { Keymap.popEsc(window._lbEscToken); window._lbEscToken = null; }
  var wrap = document.getElementById('lightboxWrap');
  if (wrap) { wrap.classList.remove('zoomed'); }
  _lbZoom = 1.0;
  _lbPanX = 0;
  _lbPanY = 0;
  _lbVisualTransitionPending = false;
  _lbSetPhotoTransitionPending(false);
  _lbDeferredOverlayApply = null;
  _lbClearPendingViewportRestore();
  _lbPendingEyeTrack = null;
  _lbEyeTrackScreenAnchor = null;
  if (_lbSwapTimer) { clearTimeout(_lbSwapTimer); _lbSwapTimer = null; }
  _lbDesiredSrcKey = null;
  _lbInitialDecodePending = false;
  _lbClearPendingInitialLoad();
  _lbResetDetailStatus();
  _lbCancelOriginalPreload();
  _lbClearAdjacentPreloads();
  _lbClearAdjustmentPreview();
  var adjustPanel = document.getElementById('lightboxAdjustPanel');
  if (adjustPanel) adjustPanel.classList.remove('open');
  var adjustBtn = document.getElementById('lightboxAdjustBtn');
  if (adjustBtn) adjustBtn.setAttribute('aria-expanded', 'false');
  toggleLightboxViewMenu(false);
  _lbSetZoomPopoverOpen(false);
  _lbApplyTransform();
  document.getElementById('lightboxOverlay').classList.remove('active');
  document.getElementById('lightboxImg').src = '';
  _lightboxCurrentId = null;
  _lbReadOnly = false;
  _lbReadOnlyMessage = 'This lightbox is read-only';
  _lbApplyReadOnlyState();
  _lightboxCommittedId = null;
  try {
    document.dispatchEvent(new CustomEvent('lightbox:closed', {
      detail: { photoId: closedPhotoId }
    }));
  } catch(_) {}
  _lbSetFlagStatus(undefined);
  var detContainer = document.getElementById('lightboxDetections');
  if (detContainer) detContainer.innerHTML = '';
  // Drop the mask overlay too — otherwise reopening on a photo with
  // no masks would leave the previous photo's mask painted across it.
  _lbResetMaskOverlay();
  if (wasOpen) Keymap.unlockBodyScroll();
}

/* ---------- Lightbox right-click context menu ----------
 * The lightbox is shared across pages (browse, review, etc.), so the menu
 * sits in _navbar.html alongside the overlay. Rating / color / flag / reveal
 * helpers (`setRatingFor`, `setColorLabelFor`, `setFlagFor`, `findSimilar`,
 * `openInEditor`, `openPhotoExportModal`, `revealPhoto`, `copyPhotoPaths`) are
 * defined on pages that carry a photo grid (browse.html). On /review, there
 * is no selection model
 * but per-photo `setReviewRating` / `setReviewFlag` helpers exist and the
 * rating/flag chips fall through to them. Pages without a page-local flag
 * helper use the shared photo flag endpoint. Color labels are not part of the
 * review workflow, so the color row is omitted when `setColorLabelFor` is
 * absent.
 */
function buildLightboxContextMenu(pid) {
  var has = function(name) { return typeof window[name] === 'function'; };
  var readOnlyHint = _lbReadOnly ? _lbReadOnlyMessage : null;

  var rateChip = function(n) {
    return {
      label: n === 0 ? '\u2606' : String(n),
      title: n === 0 ? 'No rating' : 'Rate ' + n,
      disabled: !!readOnlyHint,
      disabledHint: readOnlyHint || undefined,
      onClick: function() {
        if (has('setRatingFor')) window.setRatingFor(pid, n);
        else if (has('setReviewRating')) window.setReviewRating(pid, n);
      },
    };
  };
  var colorChip = function(c, icon, title) {
    return {
      label: icon,
      title: c && window.VireoColorLabels
        ? window.VireoColorLabels.title(c, title)
        : title,
      color: c,
      colorBaseTitle: title,
      disabled: !!readOnlyHint,
      disabledHint: readOnlyHint || undefined,
      onClick: function() { if (has('setColorLabelFor')) window.setColorLabelFor(pid, c); },
    };
  };
  var flagChip = function(f, icon, title) {
    return {
      label: icon, title: title,
      disabled: !!readOnlyHint,
      disabledHint: readOnlyHint || undefined,
      onClick: function() {
        _lbApplyFlag(pid, f);
      },
    };
  };
  var toggleItem = function(label, visible, onClick, disabledHint) {
    return {
      label: label + ': ' + (visible ? 'On' : 'Off'),
      disabled: !!disabledHint,
      disabledHint: disabledHint || undefined,
      onClick: onClick,
    };
  };

  var items = [
    { chips: [0, 1, 2, 3, 4, 5].map(rateChip) },
  ];
  if (has('setColorLabelFor')) {
    items.push({ chips: [
      colorChip(null, '\u25CB', 'No color'),
      colorChip('red', '\u25CF', 'Red'),
      colorChip('yellow', '\u25CF', 'Yellow'),
      colorChip('green', '\u25CF', 'Green'),
      colorChip('blue', '\u25CF', 'Blue'),
      colorChip('purple', '\u25CF', 'Purple'),
    ] });
  }
  items.push({ chips: [
    flagChip('flagged', '\u2691', 'Flag as pick'),
    flagChip('rejected', '\u2715', 'Reject'),
    flagChip('none', '\u25CB', 'Unflag'),
  ] });
  items.push({ separator: true });
  items.push(toggleItem('Detection boxes', _lbBoxesVisible, toggleLightboxBoxes));
  items.push(toggleItem('Mask overlay', _lbMasksVisible, toggleLightboxMasks));
  items.push(toggleItem('Eye marker', _lbEyeVisible, toggleLightboxEye));
  items.push(toggleItem('Track eye', _lbTrackEyeEnabled, toggleLightboxTrackEye));
  items.push(toggleItem('Filename and counter', _lbInfoVisible, toggleLightboxInfo));
  items.push(toggleItem('Lightbox controls', _lbChromeVisible, toggleLightboxChrome));
  items.push(toggleItem('Wildlife classification', !_lbCurrentWildlifeExcluded,
    lightboxToggleWildlifeExcluded, readOnlyHint));
  items.push({ separator: true });
  var editButton = document.getElementById('lightboxEditPhoto');
  if (editButton) {
    items.push({ label: 'Edit Photo', disabled: editButton.disabled,
      disabledHint: editButton.disabled ? editButton.title : undefined,
      onClick: function() { editButton.click(); } });
  }
  var browseDisabledHint = typeof window.getLightboxBrowseDisabledHint === 'function'
    ? window.getLightboxBrowseDisabledHint(pid)
    : null;
  items.push({ label: 'Open in Browse', disabled: !!browseDisabledHint,
    disabledHint: browseDisabledHint || undefined,
    onClick: function() {
      window.openInBrowse(pid);
    } });
  if (!_lbReadOnly && typeof window.buildSpeciesHighlightMenuItems === 'function') {
    items = items.concat(window.buildSpeciesHighlightMenuItems([pid], {
      photoById: _lbPhotoDataByPhoto,
      showFetchFallback: true,
    }));
  }
  if (!_lbReadOnly && typeof window.buildSpeciesRepresentativeMenuItems === 'function') {
    items = items.concat(window.buildSpeciesRepresentativeMenuItems([pid], {
      photoById: _lbPhotoDataByPhoto,
    }));
  }

  if (has('findSimilar')) {
    items.push({ label: 'Find Similar',
      onClick: function() { window.findSimilar(pid); } });
  }
  if (has('openInEditor')) {
    items = items.concat(window.buildOpenInEditorMenuItems([pid]));
  }
  if (has('openPhotoExportModal')) {
    items.push({ label: 'Export\u2026',
      onClick: function() {
        // The lightbox can navigate while its context menu remains open.
        // Resolve the visible identity now instead of exporting the photo
        // that happened to be current when the menu was built.
        var exportPid = _lightboxCommittedId != null
          ? _lightboxCommittedId
          : _lightboxCurrentId;
        if (exportPid != null) window.openPhotoExportModal([exportPid]);
      } });
  }
  items.push({ label: window.VIREO_REVEAL_LABEL,
    onClick: function() { revealLightboxPhoto(pid); } });
  if (has('copyPhotoPaths')) {
    items.push({ label: 'Copy Path',
      onClick: function() { window.copyPhotoPaths([pid]); } });
  }

  items.push({ separator: true });
  items.push({ label: 'Close Lightbox',
    onClick: function() { closeLightbox(); } });

  return items;
}

function revealLightboxPhoto(pid) {
  // Browse and Review each expose their own reveal helpers. Other pages can
  // also open the shared lightbox, so keep a direct API fallback here rather
  // than hiding the action based on which page happened to launch it.
  if (typeof window.revealPhoto === 'function') {
    window.revealPhoto(pid);
    return;
  }
  if (typeof window.revealReviewPhoto === 'function') {
    window.revealReviewPhoto(pid);
    return;
  }
  safeFetch('/api/files/reveal', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({photo_id: pid}),
  }, { toast: false }).then(function(data) {
    if (typeof showRevealFeedback === 'function') showRevealFeedback(data);
  }).catch(function(err) {
    if (typeof showToast === 'function') {
      showToast('Reveal failed: ' + (err.message || 'request failed'), 'error');
    } else {
      console.error('revealLightboxPhoto failed', err);
    }
  });
}

(function() {
  var img = document.getElementById('lightboxImg');
  if (!img) return;
  img.addEventListener('contextmenu', function(e) {
    // Stop the event before the browser's native menu or the wrap's
    // mousedown pan handler can react. (contextmenu fires after mousedown
    // in Chromium; stopPropagation + preventDefault together keep our
    // handler in full control.)
    e.preventDefault();
    e.stopPropagation();
    if (_lbVisualTransitionPending) return;
    var pid = _lightboxCurrentId;
    if (!pid) return;
    openContextMenu(e, buildLightboxContextMenu(pid));
  });
})();

/* ---------- Delete Confirmation ---------- */
var _deletePhotoIds = [];
var _deleteCallback = null;
var _deleteJobSource = null;
var _deleteProgressStageOrder = [];

function showDeleteDialog(photoIds, companionCount, callback) {
  _deletePhotoIds = photoIds;
  _deleteCallback = callback;
  var count = photoIds.length;
  document.getElementById('deleteModalTitle').textContent =
    count === 1 ? 'Delete photo?' : 'Delete ' + count + ' photos?';
  var compRow = document.getElementById('deleteCompanionRow');
  if (companionCount > 0) {
    compRow.style.display = '';
    document.getElementById('deleteCompanionLabel').textContent =
      'Also delete ' + companionCount + ' companion file' + (companionCount === 1 ? '' : 's');
    document.getElementById('deleteCompanionCheck').checked = true;
  } else {
    compRow.style.display = 'none';
  }
  document.querySelector('input[name="deleteMode"][value="vireo"]').checked = true;
  resetDeleteProgress();
  document.getElementById('deleteModal').classList.add('open');
}

function hideDeleteModal() {
  if (_deleteJobSource) {
    _deleteJobSource.close();
    _deleteJobSource = null;
  }
  setDeleteModalBusy(false);
  resetDeleteProgress();
  document.getElementById('deleteModal').classList.remove('open');
  _deletePhotoIds = [];
  _deleteCallback = null;
}

function resetDeleteProgress() {
  var box = document.getElementById('deleteProgress');
  var detail = document.getElementById('deleteProgressDetail');
  if (box) box.style.display = 'none';
  if (detail) detail.textContent = '';
  _deleteProgressStageOrder = [];
  ['Files', 'Catalog', 'Cache'].forEach(function(name) {
    var row = document.getElementById('deleteProgress' + name + 'Stage');
    var status = document.getElementById('deleteProgress' + name + 'Status');
    if (!row) return;
    row.hidden = false;
    row.classList.add('waiting');
    row.classList.remove('active', 'complete', 'partial');
    delete row.dataset.failed;
    if (status) status.textContent = 'Waiting';
    var bar = row.querySelector('[role="progressbar"]');
    var fill = row.querySelector('.inat-progress-fill');
    if (bar) {
      bar.setAttribute('aria-valuemax', '1');
      bar.setAttribute('aria-valuenow', '0');
      bar.setAttribute('aria-valuetext', 'Waiting');
    }
    if (fill) fill.style.width = '0%';
  });
}

function setDeleteProgressStage(stage, state, current, total, statusText) {
  var name = stage.charAt(0).toUpperCase() + stage.slice(1);
  var row = document.getElementById('deleteProgress' + name + 'Stage');
  if (!row) return;
  // A stage that has already recorded failures must not be silently
  // "completed" by a later phase transition \u2014 that would let the UI report
  // e.g. "Move files to Trash \u2713 Complete" while files were actually
  // retained. Keep the failure state visible until the modal is reset.
  if (state === 'complete' && row.dataset.failed) {
    return;
  }
  var status = document.getElementById('deleteProgress' + name + 'Status');
  var bar = row.querySelector('[role="progressbar"]');
  var fill = row.querySelector('.inat-progress-fill');
  current = Math.max(0, Number(current || 0));
  total = Math.max(0, Number(total || 0));
  var pct = (state === 'complete' || state === 'partial')
    ? 100
    : (total > 0 ? Math.max(0, Math.min(100, Math.round(100 * current / total))) : 0);

  row.classList.toggle('waiting', state === 'waiting');
  row.classList.toggle('active', state === 'active');
  row.classList.toggle('complete', state === 'complete');
  row.classList.toggle('partial', state === 'partial');
  if (fill) fill.style.width = pct + '%';
  if (bar) {
    bar.setAttribute('aria-valuemax', String(total || 1));
    bar.setAttribute(
      'aria-valuenow',
      String((state === 'complete' || state === 'partial') ? (total || 1) : current),
    );
  }

  var displayStatus = statusText;
  if (!displayStatus) {
    if (state === 'waiting') displayStatus = 'Waiting';
    else if (state === 'complete') displayStatus = '\u2713 Complete';
    else if (state === 'partial') displayStatus = 'Processed with errors';
    else if (total > 0) displayStatus = current + '/' + total;
    else displayStatus = 'Working\u2026';
  }
  if (status) status.textContent = displayStatus;
  if (bar) bar.setAttribute('aria-valuetext', displayStatus);
}

function startDeleteProgress(mode, count) {
  var filesRow = document.getElementById('deleteProgressFilesStage');
  var filesLabel = document.getElementById('deleteProgressFilesLabel');
  var diskMode = mode !== 'vireo';
  _deleteProgressStageOrder = diskMode
    ? ['files', 'catalog', 'cache']
    : ['catalog', 'cache'];
  if (filesRow) filesRow.hidden = !diskMode;
  if (filesLabel) {
    filesLabel.textContent = mode === 'disk_permanent'
      ? 'Delete files permanently'
      : 'Move files to Trash';
  }
  _deleteProgressStageOrder.forEach(function(stage) {
    setDeleteProgressStage(stage, 'waiting', 0, count);
  });
  var box = document.getElementById('deleteProgress');
  if (box) box.style.display = '';
}

function deleteProgressStageForPhase(phase) {
  if (phase === 'Moving files to Trash' || phase === 'Deleting files permanently') {
    return 'files';
  }
  if (phase === 'Removing from Vireo' || phase === 'Removed from Vireo') {
    return 'catalog';
  }
  if (phase === 'Pruning pipeline cache' || phase === 'Cleaning cached files') {
    return 'cache';
  }
  if (phase === 'Starting delete') return _deleteProgressStageOrder[0];
  return null;
}

function markDeleteStageFailed(stage, failed, current, total) {
  var name = stage.charAt(0).toUpperCase() + stage.slice(1);
  var row = document.getElementById('deleteProgress' + name + 'Stage');
  if (!row) return;
  row.dataset.failed = String(failed);
  // ``failed`` is a per-photo count (len(failed_ids)) rather than a
  // per-path count. A single retained photo may leave both a companion and
  // its unattempted primary on disk, so labelling the number as "files"
  // would understate what remains -- label as photos to match the count's
  // real semantics.
  var statusText = failed === 1
    ? '1 photo retained'
    : failed + ' photos retained';
  setDeleteProgressStage(stage, 'partial', current, total, statusText);
}

function updateDeleteProgress(data) {
  data = data || {};
  var box = document.getElementById('deleteProgress');
  var detail = document.getElementById('deleteProgressDetail');
  if (box) box.style.display = '';
  var phase = data.phase || 'Working';
  var total = Number(data.total || 0);
  var current = Number(data.current || 0);
  var failed = Number(data.failed || 0);
  var stageFailures = data.stage_failures || {};

  if (phase === 'Finishing') {
    _deleteProgressStageOrder.forEach(function(stage) {
      // Prefer the per-stage failure map so a single Finishing event is
      // enough to render the correct state even if an intermediate
      // per-stage emit never arrived. ``setDeleteProgressStage`` still
      // refuses to overwrite a stage flagged failed, so a stage already
      // marked partial by an earlier emit stays partial.
      var stageFailed = Number(stageFailures[stage] || 0);
      if (stageFailed > 0) {
        markDeleteStageFailed(stage, stageFailed, 0, 0);
      } else {
        setDeleteProgressStage(stage, 'complete');
      }
    });
    if (detail) detail.textContent = '';
    return;
  }

  var stage = deleteProgressStageForPhase(phase);
  var stageIndex = _deleteProgressStageOrder.indexOf(stage);
  if (stageIndex >= 0) {
    _deleteProgressStageOrder.slice(0, stageIndex).forEach(function(previous) {
      setDeleteProgressStage(previous, 'complete');
    });
    var isDiskPhase = (
      phase === 'Moving files to Trash' ||
      phase === 'Deleting files permanently'
    );
    if (isDiskPhase && failed > 0 && total > 0 && current >= total) {
      // Filesystem step finished with retained files -- surface the failure
      // now rather than letting a later phase mark this stage green.
      markDeleteStageFailed(stage, failed, current, total);
    } else if (phase === 'Pruning pipeline cache') {
      // This usually completes too quickly to deserve its own bar. Keep the
      // cleanup bar monotonic at zero until per-photo cache cleanup begins.
      setDeleteProgressStage(stage, 'active', 0, 0, 'Updating review cache\u2026');
    } else if (phase === 'Removed from Vireo') {
      if (failed > 0) {
        // Catalog revalidation preserved rows whose identity changed mid-
        // delete -- keep the stage from turning green and let the user see
        // that the catalog work was only partially applied.
        markDeleteStageFailed(stage, failed, current, total);
      } else {
        setDeleteProgressStage(stage, 'complete', current, total);
      }
    } else {
      setDeleteProgressStage(
        stage, 'active', current, total,
        phase === 'Starting delete' ? 'Starting\u2026' : ''
      );
    }
  }

  var parts = [];
  if (data.current_file) parts.push(data.current_file);
  if (data.detail) parts.push(data.detail);
  if (detail) detail.textContent = parts.join(' · ');
}

async function handleDeleteJobComplete(evt, savedCallback, mode, includeCompanions) {
  var data = evt && evt.result;
  // A delete that retained some photos after file errors ends "failed" but
  // still carries its result: handle it like a completed one so the
  // retained photos stay visible and the permanent-delete fallback is offered.
  var partial = !!(evt && evt.status === 'failed' && data &&
    data.failed_photo_ids && data.failed_photo_ids.length);
  if (!evt || !data || (evt.status !== 'completed' && !partial)) {
    hideDeleteModal();
    var errors = (evt && evt.errors) || [];
    showToast('Delete failed' + (errors.length ? ': ' + errors[0] : ''), 'error');
    return;
  }

  hideDeleteModal();

  var failedIds = (data.failed_photo_ids || []).filter(function(id, idx, all) {
    return all.indexOf(id) === idx;
  });
  if (data.trash_failed && data.trash_failed.length > 0 && failedIds.length) {
    var paths = data.trash_failed.map(function(f) { return f.path; });
    if (confirm('Trash not available for ' + paths.length + ' file(s):\n' +
        paths.slice(0, 5).join('\n') +
        (paths.length > 5 ? '\n... and ' + (paths.length - 5) + ' more' : '') +
        '\n\nPermanently delete instead?')) {
      try {
        // The server retains catalog rows whose Trash operation failed, so
        // retry by photo id and let it remove each row only after its
        // permanent file deletion succeeds. There is no retry by raw path:
        // the catalog row is what vouches for a file.
        var retry = await safeFetch('/api/batch/delete', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            photo_ids: failedIds,
            mode: 'disk_permanent',
            include_companions: !!includeCompanions,
          }),
        });
        data.deleted += Number(retry.deleted || 0);
        data.trashed += Number(retry.trashed || 0);
        data.trash_failed = retry.trash_failed || [];
        data.failed_photo_ids = retry.failed_photo_ids || [];
      } catch (e) {
        // safeFetch already surfaced the actionable error.
      }
    }
  }

  var msg = mode === 'vireo'
    ? data.deleted + ' photo' + (data.deleted === 1 ? '' : 's') + ' removed'
    : data.deleted + ' photo' + (data.deleted === 1 ? '' : 's') + ' moved to Trash';
  if (data.failed_photo_ids && data.failed_photo_ids.length) {
    msg += '; ' + data.failed_photo_ids.length + ' retained after file errors';
  }
  showToast(msg, data.failed_photo_ids && data.failed_photo_ids.length ? 'error' : 'success');

  if (savedCallback) savedCallback(data);
}

// Toggle the delete modal's in-flight state. While busy we keep the modal
// open (rather than closing it instantly) so the user sees that their click
// registered and work is underway — deleting a large batch can take many
// seconds of synchronous backend work with no other feedback.
var _deleteBusy = false;
function setDeleteModalBusy(busy, count, mode) {
  _deleteBusy = busy;
  var confirmBtn = document.getElementById('deleteConfirmBtn');
  var cancelBtn = document.querySelector('#deleteModal .modal-btn-cancel');
  // The mode radios and companion checkbox were already read into the request
  // when the delete started, so changing them mid-flight does nothing — lock
  // them too so the UI stays honest about what's in progress.
  var inputs = document.querySelectorAll(
    '#deleteModal input[name="deleteMode"], #deleteCompanionCheck');
  if (busy) {
    var verb = mode === 'vireo' ? 'Removing' : 'Deleting';
    var noun = count === 1 ? 'photo' : count + ' photos';
    if (confirmBtn) {
      confirmBtn.disabled = true;
      confirmBtn.style.opacity = '0.85';
      confirmBtn.style.cursor = 'default';
      confirmBtn.innerHTML = '<span class="btn-spinner"></span>' + verb + ' ' + noun + '…';
    }
    if (cancelBtn) cancelBtn.disabled = true;
    inputs.forEach(function(el) { el.disabled = true; });
  } else {
    if (confirmBtn) {
      confirmBtn.disabled = false;
      confirmBtn.style.opacity = '';
      confirmBtn.style.cursor = 'pointer';
      confirmBtn.textContent = 'Delete';
    }
    if (cancelBtn) cancelBtn.disabled = false;
    inputs.forEach(function(el) { el.disabled = false; });
  }
}

async function confirmDelete() {
  if (_deleteBusy) return;  // guard against double-submit
  var mode = document.querySelector('input[name="deleteMode"]:checked').value;
  var includeCompanions = document.getElementById('deleteCompanionCheck').checked &&
    document.getElementById('deleteCompanionRow').style.display !== 'none';
  var savedIds = _deletePhotoIds.slice();
  var savedCallback = _deleteCallback;
  setDeleteModalBusy(true, savedIds.length, mode);
  startDeleteProgress(mode, savedIds.length);
  updateDeleteProgress({
    phase: 'Starting delete',
    current: 0,
    total: savedIds.length,
  });
  try {
    var start = await safeFetch('/api/jobs/batch-delete', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        photo_ids: savedIds,
        mode: mode,
        include_companions: includeCompanions,
      }),
    });
  } catch(e) { hideDeleteModal(); return; }
  _deleteJobSource = safeEventSource('/api/jobs/' + start.job_id + '/stream', {
    onProgress: updateDeleteProgress,
    onComplete: function(evt) {
      _deleteJobSource = null;
      handleDeleteJobComplete(evt, savedCallback, mode, includeCompanions);
    },
    onError: function() {
      _deleteJobSource = null;
      hideDeleteModal();
    },
  });
}

async function lightboxToggleWildlifeExcluded() {
  if (!_lightboxCurrentId) return;
  if (_lbGuardReadOnly()) return false;
  var currentId = _lightboxCurrentId;
  var excluded = !_lbCurrentWildlifeExcluded;
  try {
    if (typeof window.setWildlifeExcludedFor === 'function') {
      var updated = await window.setWildlifeExcludedFor(currentId, excluded);
      if (updated === false) return false;
    } else {
      await safeFetch('/api/photos/' + currentId + '/wildlife_excluded', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ excluded: excluded }),
      }, { toast: false });
    }
    _lbCurrentWildlifeExcluded = excluded;
    if (window.toast) {
      window.toast(
        excluded ? 'Excluded from wildlife classification' : 'Included in wildlife classification',
        'success'
      );
    }
  } catch (e) {
    if (window.toast) window.toast('Update failed: ' + (e && e.message ? e.message : 'unknown'), 'error');
  }
}

function lightboxDelete() {
  if (_lbGuardReadOnly()) return false;
  var overlay = document.getElementById('lightboxOverlay');
  if (
    _lbVisualTransitionPending ||
    !_lightboxCurrentId ||
    !overlay ||
    !overlay.classList.contains('active')
  ) return;
  var currentId = _lightboxCurrentId;
  var p = _lightboxPhotoList.find(function(x) { return x.id === currentId; });
  var companionCount = (p && p.companion_path) ? 1 : 0;

  showDeleteDialog([currentId], companionCount, function(data) {
    // If the server retained this photo because its Trash step failed (and
    // the user declined the permanent-delete fallback), leave it in the
    // lightbox and grid. Dropping it here would hide a file still on disk
    // until the next reload — the toast already told the user it was kept.
    var retained = (data && data.failed_photo_ids) || [];
    if (retained.indexOf(currentId) !== -1) {
      return;
    }

    // Capture the lightbox position before Browse updates its grid. Browse's
    // lazy-loaded lightbox deliberately shares the same array object, so a
    // grid removal may also remove the lightbox entry in one splice.
    var idx = _lightboxPhotoList.findIndex(function(x) { return x.id === currentId; });
    var removedFromSharedLightboxList = false;

    // Update the browse grid before touching lightbox state. Otherwise
    // Browse's `lightbox:closed` handler still finds the deleted row in
    // `photos` and re-selects it (loading stale detail state that this
    // block would then null out — leaving the sidebar blank).
    if (typeof photos !== 'undefined' && typeof renderGrid === 'function') {
      var browseIdx = photos.findIndex(function(x) { return x.id === currentId; });
      if (browseIdx >= 0) {
        removedFromSharedLightboxList = photos === _lightboxPhotoList;
        photos.splice(browseIdx, 1);
      }
      selectedPhotos.delete(currentId);
      if (selectedPhotoId === currentId) selectedPhotoId = null;
      renderGrid();
      if (typeof refreshBrowseSidebarCounts === 'function') {
        refreshBrowseSidebarCounts();
      }
    }

    // Remove from a page-specific lightbox list. When Browse shared its live
    // array, the grid splice above already performed this removal.
    if (!removedFromSharedLightboxList && idx >= 0) {
      _lightboxPhotoList.splice(idx, 1);
    }

    if (_lightboxPhotoList.length === 0) {
      closeLightbox(null);
    } else {
      var nextIdx = Math.min(idx, _lightboxPhotoList.length - 1);
      var next = _lightboxPhotoList[nextIdx];
      openLightbox(next.id, next.filename, _lightboxPhotoList);
    }

    try {
      document.dispatchEvent(new CustomEvent('lightbox:photodeleted', {
        detail: { photoId: currentId, result: data || null }
      }));
    } catch (_) {}
  });
}
/* ---------- iNat Submission ---------- */
var inatQueue = [];  // [{photo_id, taxon_name, observed_on, latitude, longitude, description, geoprivacy, filename, already_submitted, existing_url}]
var _inatSubmitOwner = 0;
var _inatSubmitting = false;  // true while inatDoSubmit's loop is running
var _inatCancelled = false;   // set by closeInatModal to stop the loop gracefully
var _inatQuickFailures = [];
var _inatModalGeneration = 0;
var _inatExportStream = null;

function _closeInatExportStream() {
  if (!_inatExportStream) return;
  try { _inatExportStream.close(); } catch(e) {}
  _inatExportStream = null;
}

function _inatQueueItem(photoId, data) {
  return {
    photo_id: photoId,
    taxon_name: data.scientific_name || data.species,
    observed_on: data.timestamp ? data.timestamp.substring(0, 10) : '',
    latitude: data.latitude != null ? data.latitude : '',
    longitude: data.longitude != null ? data.longitude : '',
    description: '',
    geoprivacy: 'open',
    filename: data.filename,
    edit_recipe: data.edit_recipe,
    already_submitted: data.already_submitted,
    existing_url: data.existing_observation_url,
    upload_url: data.upload_url,
  };
}

async function _inatOpenUrl(url) {
  try {
    if (typeof openExternal === 'function') {
      return await openExternal(url);
    }
  } catch(e) {}
  return false;
}

function _inatQuickUploadUrl(idx) {
  var item = inatQueue[idx];
  var base = (item && item.upload_url) || 'https://www.inaturalist.org/observations/upload';
  base = base.split('?')[0];
  var params = [];
  var taxon = document.getElementById('inatIncludeTaxon' + idx);
  var date = document.getElementById('inatIncludeDate' + idx);
  var location = document.getElementById('inatIncludeLocation' + idx);
  if (taxon && taxon.checked && item.taxon_name) {
    params.push('taxon_name=' + encodeURIComponent(item.taxon_name));
  }
  if (date && date.checked && item.observed_on) {
    params.push('observed_on=' + encodeURIComponent(item.observed_on));
  }
  if (location && location.checked && item.latitude !== '' && item.longitude !== '') {
    params.push('lat=' + encodeURIComponent(String(item.latitude)));
    params.push('lng=' + encodeURIComponent(String(item.longitude)));
  }
  return base + (params.length ? '?' + params.join('&') : '');
}

async function _openInatQuickUploadNative(url) {
  if (await _inatOpenUrl(url)) {
    showToast('Opened iNaturalist in your browser.', 'success');
    return;
  }
  if (typeof showExternalOpenFailure === 'function') {
    showExternalOpenFailure(
      url,
      'Vireo could not open iNaturalist. Retry or copy the upload URL below.'
    );
  }
}

function openInatQuickUpload(event, idx) {
  var url = _inatQuickUploadUrl(idx);
  var link = event && event.currentTarget;
  if (link) link.href = url;

  if (typeof isTauri === 'function' && isTauri()) {
    if (event) event.preventDefault();
    _openInatQuickUploadNative(url);
    return false;
  }

  // The loopback page can occasionally load without Tauri's IPC globals.
  // Preserve a real link in that case: a normal browser opens a new tab, and
  // the native shell's on_new_window hook sends the URL to the OS browser.
  // Stop propagation so the delegated external-link handler does not replace
  // this fallback with the unavailable IPC path.
  if (event) event.stopPropagation();
  return true;
}

function copyInatQuickUploadUrl(idx) {
  return copyExternalUrl(_inatQuickUploadUrl(idx));
}

async function submitToInat(photoId) {
  if (_lbGuardReadOnly()) return false;
  try {
    var data = await safeFetch('/api/inat/prepare/' + photoId, {}, { toast: false });
    if (data.error) { alert(data.error); return; }

    if (data.mode === 'quick') {
      await openInatQuickModal([_inatQueueItem(photoId, data)], []);
      return;
    }

    // Direct mode: open modal
    inatQueue = [_inatQueueItem(photoId, data)];
    openInatModal([]);
  } catch(e) {
    alert('Error: ' + e.message);
  }
}

async function submitToInatBatch(photoIds) {
  if (_lbGuardReadOnly()) return false;
  // Prepare all photos
  inatQueue = [];
  var quickQueue = [];
  var failures = [];
  for (var i = 0; i < photoIds.length; i++) {
    try {
      var data = await safeFetch('/api/inat/prepare/' + photoIds[i], {}, { toast: false });
      if (data.error) {
        failures.push({photo_id: photoIds[i], error: data.error});
        continue;
      }
      var item = _inatQueueItem(photoIds[i], data);
      if (data.mode === 'quick') quickQueue.push(item);
      else inatQueue.push(item);
    } catch(e) {
      failures.push({photo_id: photoIds[i], error: e.message || 'Could not prepare iNaturalist upload'});
    }
  }
  if (quickQueue.length) {
    await openInatQuickModal(quickQueue, failures);
    return;
  }
  if (inatQueue.length === 0) {
    var msg = failures.length
      ? 'No photos could be prepared for iNaturalist.'
      : 'No photos to submit.';
    alert(msg);
    return;
  }
  openInatModal(failures);
}

async function openInatQuickModal(items, failures) {
  _inatModalGeneration++;
  _closeInatExportStream();
  if (window._inatEscToken) Keymap.popEsc(window._inatEscToken);
  window._inatEscToken = Keymap.pushEsc(function() { closeInatModal(); });

  inatQueue = items.slice();
  _inatQuickFailures = (failures || []).slice();
  var title = items.length === 1 ? 'Send to iNaturalist' : 'Send ' + items.length + ' photos to iNaturalist';
  document.getElementById('inatModalTitle').textContent = title;
  document.getElementById('inatProgress').style.display = 'none';

  var submitBtn = document.getElementById('inatSubmitBtn');
  submitBtn.style.display = '';
  submitBtn.disabled = true;
  submitBtn.textContent = items.length === 1 ? 'Send to iNaturalist' : 'Send All to iNaturalist';

  var cancelBtn = document.querySelector('#inatActions .modal-btn-cancel');
  if (cancelBtn) cancelBtn.textContent = 'Close';

  function _inatAlreadySubmittedWarning(item) {
    if (!item.already_submitted) return '';
    var linkOrText = item.existing_url
      ? '<a href="' + escapeAttr(item.existing_url) + '" target="_blank" rel="noopener" onclick="return openExternalLink(event, this.href)" style="color:var(--warning);">' + escapeHtml(item.existing_url) + '</a>'
      : 'no observation URL recorded';
    return '<div class="inat-card-status warning" style="margin-top:6px;">&#9888; Already submitted to iNaturalist: ' + linkOrText + '. Opening a new upload will create a duplicate observation.</div>';
  }

  function _inatQuickOptions(item, idx) {
    var hasTaxon = !!item.taxon_name;
    var hasDate = !!item.observed_on;
    var hasLocation = item.latitude !== '' && item.longitude !== '';
    var taxonText = hasTaxon ? item.taxon_name : 'No detected taxon';
    var dateText = hasDate ? item.observed_on : 'No observation date';
    var locationText = hasLocation
      ? String(item.latitude) + ', ' + String(item.longitude)
      : 'No location';
    return '<fieldset style="border:0;padding:0;margin:12px 0 0;display:flex;flex-direction:column;gap:8px;">' +
      '<legend style="font-size:13px;color:var(--text-primary);margin-bottom:7px;">Include with this photo</legend>' +
      '<label style="font-size:13px;color:var(--text-secondary);display:flex;align-items:center;gap:8px;">' +
        '<input id="inatIncludeTaxon' + idx + '" type="checkbox"' + (hasTaxon ? ' checked' : ' disabled') + '> Taxon: ' + escapeHtml(taxonText) +
      '</label>' +
      '<label style="font-size:13px;color:var(--text-secondary);display:flex;align-items:center;gap:8px;">' +
        '<input id="inatIncludeDate' + idx + '" type="checkbox"' + (hasDate ? ' checked' : ' disabled') + '> Date: ' + escapeHtml(dateText) +
      '</label>' +
      '<label style="font-size:13px;color:var(--text-secondary);display:flex;align-items:center;gap:8px;">' +
        '<input id="inatIncludeLocation' + idx + '" type="checkbox"' + (hasLocation ? ' checked' : ' disabled') + '> Location: ' + escapeHtml(locationText) +
      '</label>' +
      '<label style="font-size:13px;color:var(--text-secondary);display:flex;align-items:center;gap:8px;">' +
        '<input id="inatIncludeDescription' + idx + '" type="checkbox"> Description' +
      '</label>' +
      '<textarea id="inatQuickDescription' + idx + '" aria-label="Description" placeholder="Description (optional)" style="width:100%;min-height:48px;box-sizing:border-box;background:var(--bg-secondary);color:var(--text-primary);border:1px solid var(--border-secondary);border-radius:3px;padding:6px 8px;font-size:12px;resize:vertical;"></textarea>' +
    '</fieldset>';
  }

  var html = '<div class="inat-card" id="inatTokenSetup">' +
    '<div style="font-size:13px;color:var(--text-primary);font-weight:600;margin-bottom:5px;">Direct submission needs an iNaturalist token</div>' +
    '<div style="font-size:12px;line-height:1.5;color:var(--text-secondary);margin-bottom:9px;">' +
      'Paste a token below and Vireo will validate it before saving. iNaturalist tokens expire after 24 hours. ' +
      '<a href="https://www.inaturalist.org/users/api_token" target="_blank" rel="noopener" onclick="return openExternalLink(event, this.href)" style="color:var(--accent);">Get a token</a>' +
    '</div>' +
    '<div style="display:flex;gap:8px;align-items:center;">' +
      '<input id="inatQuickToken" type="password" autocomplete="off" placeholder="Paste iNaturalist token" style="flex:1;min-width:0;background:var(--bg-secondary);color:var(--text-primary);border:1px solid var(--border-secondary);border-radius:3px;padding:7px 8px;font-size:12px;font-family:monospace;">' +
      '<button type="button" class="modal-btn modal-btn-primary" id="inatQuickTokenBtn" onclick="validateAndSaveInatToken()">Validate &amp; Save</button>' +
    '</div>' +
    '<div class="inat-card-status" id="inatQuickTokenStatus">The Send button will be enabled after validation.</div>' +
  '</div>';
  if (items.length === 1) {
    html += '<div class="inat-card">' +
      '<div class="inat-card-header">' +
        '<img class="inat-card-thumb" src="/thumbnails/' + items[0].photo_id + '.jpg" alt="">' +
        '<div style="flex:1;min-width:0;">' +
          '<div style="font-size:13px;color:var(--text-primary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + escapeHtml(items[0].filename) + '</div>' +
          _inatAlreadySubmittedWarning(items[0]) +
        '</div>' +
      '</div>' +
      '<div style="font-size:13px;line-height:1.5;color:var(--text-secondary);">' +
        'Choose which details Vireo should add to the exported JPEG and upload link.' +
      '</div>' +
      _inatQuickOptions(items[0], 0) +
      '<div style="margin-top:12px;">' +
        '<a class="modal-btn modal-btn-primary" href="' + escapeAttr(items[0].upload_url || 'https://www.inaturalist.org/observations/upload') + '" target="_blank" rel="noopener" onclick="return openInatQuickUpload(event, 0)" style="display:inline-block;text-decoration:none;">Open Upload Page</a>' +
        '<button type="button" class="modal-btn" onclick="copyInatQuickUploadUrl(0)" style="margin-left:8px;">Copy URL</button>' +
      '</div>' +
    '</div>';
  } else {
    html += '<div class="inat-card">' +
      '<div style="font-size:13px;line-height:1.5;color:var(--text-secondary);margin-bottom:10px;">' +
        'Use the token field above for direct submission, or export the JPEGs and open an upload page for each photo below.' +
      '</div>';
    items.forEach(function(item, idx) {
      html += '<div style="display:flex;flex-direction:column;gap:4px;padding:8px 0;border-top:1px solid var(--border-primary);">' +
        '<div style="display:flex;align-items:center;justify-content:space-between;gap:12px;">' +
          '<span style="font-size:13px;color:var(--text-primary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + escapeHtml(item.filename) + '</span>' +
          '<span style="display:flex;align-items:center;gap:8px;white-space:nowrap;">' +
            '<a href="' + escapeAttr(item.upload_url || 'https://www.inaturalist.org/observations/upload') + '" target="_blank" rel="noopener" onclick="return openInatQuickUpload(event, ' + idx + ')" style="color:var(--accent);font-size:12px;">Open Upload</a>' +
            '<button type="button" onclick="copyInatQuickUploadUrl(' + idx + ')" style="border:0;background:none;color:var(--text-secondary);font-size:12px;cursor:pointer;padding:0;">Copy URL</button>' +
          '</span>' +
        '</div>' +
        _inatQuickOptions(item, idx) +
        _inatAlreadySubmittedWarning(item) +
      '</div>';
    });
    html += '</div>';
  }

  if (failures && failures.length) {
    html += '<div class="inat-card-status error" style="margin-top:10px;">' +
      escapeHtml(failures.length + ' photo' + (failures.length === 1 ? '' : 's') + ' could not be prepared.') +
      '</div>';
  }

  var exportLabel = items.length === 1 ? 'Export JPEG\u2026' : 'Export ' + items.length + ' JPEGs\u2026';
  html += '<div class="inat-card">' +
    '<div style="font-size:13px;color:var(--text-primary);font-weight:600;margin-bottom:5px;">Upload through your browser</div>' +
    '<div style="font-size:12px;line-height:1.5;color:var(--text-secondary);margin-bottom:9px;">' +
      'Export edited JPEG' + (items.length === 1 ? '' : 's') + ' with only the checked metadata, then add ' + (items.length === 1 ? 'it' : 'them') + ' on the iNaturalist upload page.' +
    '</div>' +
    '<button type="button" class="modal-btn modal-btn-primary" id="inatQuickExportBtn" onclick="exportInatQuickPhotos()">' + escapeHtml(exportLabel) + '</button>' +
    '<div class="inat-card-status" id="inatQuickExportStatus"></div>' +
  '</div>';

  document.getElementById('inatCards').innerHTML = html;
  document.getElementById('inatModal').classList.add('open');
}

async function validateAndSaveInatToken() {
  var input = document.getElementById('inatQuickToken');
  var status = document.getElementById('inatQuickTokenStatus');
  var button = document.getElementById('inatQuickTokenBtn');
  var generation = _inatModalGeneration;
  var token = input ? input.value.trim() : '';
  if (!token) {
    status.className = 'inat-card-status error';
    status.textContent = 'Paste a token first.';
    return;
  }
  button.disabled = true;
  button.textContent = 'Validating\u2026';
  status.className = 'inat-card-status';
  status.textContent = 'Checking with iNaturalist\u2026';
  try {
    var data = await safeFetch('/api/inat/token', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({token: token}),
    }, { toast: false });
    if (
      generation !== _inatModalGeneration ||
      !document.getElementById('inatModal').classList.contains('open') ||
      document.getElementById('inatQuickToken') !== input
    ) return;
    status.className = 'inat-card-status success';
    status.textContent = '\u2713 Valid token' + (data.login ? ' for ' + data.login : '') + '. Opening direct submission\u2026';
    _inatApplyQuickChoicesToQueue();
    openInatModal(_inatQuickFailures);
  } catch(e) {
    if (generation !== _inatModalGeneration) return;
    status.className = 'inat-card-status error';
    status.textContent = '\u2717 ' + e.message;
    button.disabled = false;
    button.textContent = 'Validate & Save';
  }
}

function _inatQuickExportSubmissions() {
  return inatQueue.map(function(item, idx) {
    var description = document.getElementById('inatQuickDescription' + idx);
    return {
      photo_id: item.photo_id,
      taxon_name: item.taxon_name || '',
      observed_on: item.observed_on || '',
      latitude: item.latitude,
      longitude: item.longitude,
      include_taxon: !!(document.getElementById('inatIncludeTaxon' + idx) || {}).checked,
      include_date: !!(document.getElementById('inatIncludeDate' + idx) || {}).checked,
      include_location: !!(document.getElementById('inatIncludeLocation' + idx) || {}).checked,
      include_description: !!(document.getElementById('inatIncludeDescription' + idx) || {}).checked,
      description: description ? description.value.trim() : '',
    };
  });
}

function _inatApplyQuickChoicesToQueue() {
  var choices = _inatQuickExportSubmissions();
  choices.forEach(function(choice, idx) {
    var item = inatQueue[idx];
    if (!item) return;
    if (!choice.include_taxon) item.taxon_name = '';
    if (!choice.include_date) item.observed_on = '';
    if (!choice.include_location) {
      item.latitude = '';
      item.longitude = '';
    }
    item.description = choice.include_description ? choice.description : '';
  });
}

async function exportInatQuickPhotos() {
  var status = document.getElementById('inatQuickExportStatus');
  var button = document.getElementById('inatQuickExportBtn');
  if (!button || button.disabled) return;
  var generation = _inatModalGeneration;
  var queue = inatQueue;
  var itemCount = queue.length;
  var submissions = _inatQuickExportSubmissions();
  var exportLabel = itemCount === 1 ? 'Export JPEG\u2026' : 'Export ' + itemCount + ' JPEGs\u2026';
  var destination = null;
  button.disabled = true;
  button.textContent = 'Choosing folder\u2026';
  try {
    if (typeof pickDirectory === 'function' && typeof isTauri === 'function' && isTauri()) {
      destination = await pickDirectory('Export for iNaturalist');
    } else {
      destination = window.prompt('Export folder path:');
    }
  } catch(e) {
    if (generation !== _inatModalGeneration) return;
    status.className = 'inat-card-status error';
    status.textContent = '\u2717 Could not open the folder picker: ' + e.message;
    button.disabled = false;
    button.textContent = exportLabel;
    return;
  }
  if (!destination) {
    if (generation === _inatModalGeneration && inatQueue === queue) {
      button.disabled = false;
      button.textContent = exportLabel;
    }
    return;
  }
  if (
    generation !== _inatModalGeneration ||
    inatQueue !== queue ||
    !document.getElementById('inatModal').classList.contains('open')
  ) return;
  button.textContent = 'Exporting\u2026';
  status.className = 'inat-card-status';
  status.textContent = 'Rendering edited JPEG' + (itemCount === 1 ? '' : 's') + ' and writing metadata\u2026';
  try {
    var started = await safeFetch('/api/inat/export', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        destination: destination,
        submissions: submissions,
        reveal: typeof isTauri === 'function' && isTauri(),
      }),
    }, { toast: false });
    if (generation !== _inatModalGeneration || inatQueue !== queue) return;
    _closeInatExportStream();
    var exportStream = safeEventSource('/api/jobs/' + started.job_id + '/stream', {
      onProgress: function(progress) {
        if (
          generation !== _inatModalGeneration ||
          _inatExportStream !== exportStream
        ) return;
        var current = progress.current || 0;
        var total = progress.total || itemCount;
        status.textContent = 'Exporting ' + current + ' of ' + total + '\u2026';
      },
      onComplete: function(done) {
        var isCurrentStream = _inatExportStream === exportStream;
        if (!isCurrentStream || generation !== _inatModalGeneration) return;
        try { exportStream.close(); } catch(e) {}
        _inatExportStream = null;
        var result = done.result || {};
        var count = (result.exported || []).length;
        var failed = (result.errors || []).length;
        if (done.status === 'cancelled') {
          status.className = 'inat-card-status warning';
          status.textContent = 'Export cancelled after ' + count + ' JPEG' + (count === 1 ? '' : 's') + '.';
        } else if (!count) {
          status.className = 'inat-card-status error';
          status.textContent = '\u2717 ' + (
            failed ? result.errors[0].error : 'No photos were exported.'
          );
        } else {
          status.className = failed ? 'inat-card-status warning' : 'inat-card-status success';
          status.textContent = '\u2713 Exported ' + count + ' JPEG' + (count === 1 ? '' : 's') +
            (failed ? '; ' + failed + ' failed.' : '.') +
            (result.revealed ? ' Revealed in the file manager.' : '');
        }
        button.disabled = false;
        button.textContent = exportLabel;
      },
      onError: function() {
        var isCurrentStream = _inatExportStream === exportStream;
        if (!isCurrentStream || generation !== _inatModalGeneration) return;
        try { exportStream.close(); } catch(e) {}
        _inatExportStream = null;
        status.className = 'inat-card-status error';
        status.textContent = '\u2717 Lost the export progress connection.';
        button.disabled = false;
        button.textContent = exportLabel;
      },
    });
    _inatExportStream = exportStream;
  } catch(e) {
    if (generation !== _inatModalGeneration) return;
    status.className = 'inat-card-status error';
    status.textContent = '\u2717 ' + e.message;
    button.disabled = false;
    button.textContent = exportLabel;
  }
}

function openInatModal(failures) {
  _inatModalGeneration++;
  _closeInatExportStream();
  if (window._inatEscToken) Keymap.popEsc(window._inatEscToken);
  window._inatEscToken = Keymap.pushEsc(function() { closeInatModal(); });

  var title = inatQueue.length === 1 ? 'Submit to iNaturalist' : 'Submit ' + inatQueue.length + ' observations to iNaturalist';
  document.getElementById('inatModalTitle').textContent = title;
  document.getElementById('inatProgress').style.display = 'none';
  var submitBtn = document.getElementById('inatSubmitBtn');
  submitBtn.style.display = '';
  submitBtn.disabled = false;
  submitBtn.textContent = inatQueue.length === 1 ? 'Send to iNaturalist' : 'Send All to iNaturalist';
  var cancelBtn = document.querySelector('#inatActions .modal-btn-cancel');
  if (cancelBtn) cancelBtn.textContent = 'Cancel';

  var html = '';
  inatQueue.forEach(function(item, idx) {
    var warn = '';
    if (item.already_submitted) {
      warn = '<div class="inat-card-status warning">&#9888; Already submitted: <a href="' + escapeAttr(item.existing_url) + '" target="_blank" onclick="return openExternalLink(event, this.href)" style="color:var(--warning);">' + escapeHtml(item.existing_url) + '</a></div>';
    }
    var thumbUrl = window.vireoThumbnailUrl ? window.vireoThumbnailUrl(item) : '/thumbnails/' + item.photo_id + '.jpg';
    html += '<div class="inat-card" id="inatCard' + idx + '">' +
      '<div class="inat-card-header">' +
        '<img class="inat-card-thumb" src="' + escapeAttr(thumbUrl) + '" alt="">' +
        '<div style="flex:1;min-width:0;">' +
          '<div style="font-size:13px;color:var(--text-primary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + escapeHtml(item.filename) + '</div>' +
          warn +
        '</div>' +
      '</div>' +
      '<div class="inat-card-fields">' +
        '<div><label>Species / Taxon</label><input id="inatTaxon' + idx + '" value="' + escapeAttr(item.taxon_name) + '"></div>' +
        '<div><label>Date observed</label><input id="inatDate' + idx + '" type="date" value="' + escapeAttr(item.observed_on) + '"></div>' +
        '<div><label>Latitude</label><input id="inatLat' + idx + '" value="' + escapeAttr(String(item.latitude)) + '"></div>' +
        '<div><label>Longitude</label><input id="inatLng' + idx + '" value="' + escapeAttr(String(item.longitude)) + '"></div>' +
        '<div><label>Geoprivacy</label><select id="inatGeo' + idx + '"><option value="open">Open</option><option value="obscured">Obscured</option><option value="private">Private</option></select></div>' +
        '<div></div>' +
        '<textarea id="inatDesc' + idx + '" placeholder="Notes (optional)">' + escapeHtml(item.description || '') + '</textarea>' +
      '</div>' +
      '<div class="inat-card-status" id="inatStatus' + idx + '"></div>' +
    '</div>';
  });
  if (failures && failures.length) {
    html += '<div class="inat-card-status error" style="margin-top:10px;">' +
      escapeHtml(failures.length + ' photo' + (failures.length === 1 ? '' : 's') + ' could not be prepared.') +
      '</div>';
  }
  document.getElementById('inatCards').innerHTML = html;
  document.getElementById('inatModal').classList.add('open');
}

function closeInatModal() {
  _inatModalGeneration++;
  _closeInatExportStream();
  if (window._inatEscToken) { Keymap.popEsc(window._inatEscToken); window._inatEscToken = null; }
  document.getElementById('inatModal').classList.remove('open');
  if (_inatSubmitting) {
    // Cancel/Esc during an in-flight submit: don't clear the queue out from
    // under the loop — flag it so it stops after the current item and
    // reports a partial result.
    _inatCancelled = true;
  } else {
    inatQueue = [];
    _inatQuickFailures = [];
  }
}

async function inatDoSubmit() {
  if (_lbGuardReadOnly()) return false;
  var btn = document.getElementById('inatSubmitBtn');
  btn.disabled = true;
  btn.textContent = 'Submitting...';

  var progress = document.getElementById('inatProgress');
  var fill = document.getElementById('inatProgressFill');
  var text = document.getElementById('inatProgressText');
  progress.style.display = 'block';

  var queue = inatQueue;
  var generation = _inatModalGeneration;
  var owner = ++_inatSubmitOwner;
  var total = queue.length;
  var done = 0;
  var succeeded = 0;
  _inatSubmitting = true;
  _inatCancelled = false;

  for (var i = 0; i < total; i++) {
    if (_inatCancelled || generation !== _inatModalGeneration) break;
    var item = queue[i];
    var statusEl = document.getElementById('inatStatus' + i);
    var latValue = document.getElementById('inatLat' + i).value.trim();
    var lngValue = document.getElementById('inatLng' + i).value.trim();

    // Read possibly-edited fields from the form
    var submission = {
      photo_id: item.photo_id,
      taxon_name: document.getElementById('inatTaxon' + i).value.trim(),
      observed_on: document.getElementById('inatDate' + i).value,
      latitude: latValue === '' ? null : parseFloat(latValue),
      longitude: lngValue === '' ? null : parseFloat(lngValue),
      description: document.getElementById('inatDesc' + i).value.trim(),
      geoprivacy: document.getElementById('inatGeo' + i).value,
    };

    statusEl.className = 'inat-card-status';
    statusEl.textContent = 'Submitting...';

    try {
      var result = await safeFetch('/api/inat/submit', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(submission),
      }, { toast: false });

      if (generation !== _inatModalGeneration) {
        if (!result.error) succeeded++;
        done++;
        break;
      }
      if (result.error) {
        statusEl.className = 'inat-card-status error';
        statusEl.textContent = '✗ ' + result.error;
      } else {
        statusEl.className = 'inat-card-status success';
        statusEl.innerHTML = '&#10003; Submitted — <a href="' + escapeAttr(result.observation_url) + '" target="_blank" onclick="return openExternalLink(event, this.href)" style="color:var(--accent);">View on iNaturalist</a>';
        if (total === 1 && result.observation_url && isTauri()) {
          var opened = await openExternal(result.observation_url);
          if (!opened) showToast('Submitted, but could not open iNaturalist: ' + result.observation_url, 'error');
        }
        succeeded++;
      }
    } catch(e) {
      if (generation !== _inatModalGeneration) break;
      statusEl.className = 'inat-card-status error';
      if (e.body && e.body.partial && e.body.observation_url) {
        statusEl.innerHTML = '&#10007; ' + escapeHtml(e.message) +
          ' <a href="' + escapeAttr(e.body.observation_url) + '" target="_blank" rel="noopener" onclick="return openExternalLink(event, this.href)" style="color:var(--danger);text-decoration:underline;">View created observation</a>';
      } else {
        statusEl.textContent = '✗ ' + e.message;
      }
    }

    done++;
    if (generation !== _inatModalGeneration) break;
    fill.style.width = Math.round((done / total) * 100) + '%';
    text.textContent = done + ' / ' + total + ' processed (' + succeeded + ' succeeded)';
  }

  if (owner !== _inatSubmitOwner) return;
  _inatSubmitting = false;
  if (_inatCancelled || generation !== _inatModalGeneration) {
    // Modal is already closed (closeInatModal deferred the queue cleanup
    // to us) — surface the partial result where the user can see it.
    _inatCancelled = false;
    if (inatQueue === queue) inatQueue = [];
    showToast('iNaturalist: submitted ' + succeeded + ' of ' + total + ', cancelled', 'info');
    return;
  }
  btn.textContent = 'Done';
  // Change cancel to Close
  document.querySelector('#inatActions .modal-btn-cancel').textContent = 'Close';
}

// NOTE: Page navigation shortcuts (Cmd+1..0, Cmd+Shift+D/W/L, Cmd+,) are
// handled by the native menu bar when running inside the Tauri desktop shell.
// See src-tauri/src/menu.rs for the menu definition.
// If adding JS-based navigation shortcuts, guard with:
//   if (window.__TAURI_INTERNALS__) return;
// True when the key event's target is a field where typing/arrows must edit
// the field instead of driving lightbox shortcuts. Checkboxes and radios are
// deliberately NOT editable: the View menu's toggles keep focus after a
// click, and they consume no text or arrow keys, so hotkeys (b, h, arrows)
// must keep working while one is focused. Text inputs, selects, and range
// sliders (the Adjust panel) genuinely consume keys, so they stay guarded.
function _lbKeyTargetEditable(t) {
  if (!t) return false;
  if (t.tagName === 'INPUT') return !(t.type === 'checkbox' || t.type === 'radio');
  return t.tagName === 'TEXTAREA' || t.tagName === 'SELECT' || !!t.isContentEditable;
}
document.addEventListener('keydown', function(e) {
  // Esc handling for the lightbox + overlay cascade is owned by the Keymap.pushEsc
  // stack now: each open*() pushes its close*() and each close*() pops it. The
  // listener below still handles arrow keys / +/-/0 / B / Z / F / G while the
  // lightbox is open.
  if (!document.getElementById('lightboxOverlay').classList.contains('active')) return;
  // .grm-overlay.open is deliberately NOT in this suppression list: the burst
  // modal can only sit *underneath* the lightbox (review's "Open in Lightbox"),
  // so the lightbox keeps keyboard precedence; the burst modal's own keydown
  // handler bails while the lightbox is open.
  if (document.querySelector('.pipeline-overlay.active, .similar-overlay.active, .modal-overlay.open, .inspect-overlay.open, .shortcuts-overlay.open, .help-overlay.active, .report-overlay.active')) return;
  // The command palette (Cmd+K) opens over the lightbox and toggles `hidden`
  // rather than a class; while it is open it owns the keyboard.
  var cmdPalette = document.getElementById('commandPalette');
  if (cmdPalette && !cmdPalette.hidden) return;
  var editable0 = _lbKeyTargetEditable(e.target);
  // Editable check must come before arrow handling so arrows inside form
  // fields (e.g. the mask-variant <select>) change the value, not the photo.
  if (!editable0 && e.key === 'ArrowRight') {
    lightboxNav(1);
    e.preventDefault();
    e.stopImmediatePropagation();
    return;
  }
  if (!editable0 && e.key === 'ArrowLeft') {
    lightboxNav(-1);
    e.preventDefault();
    e.stopImmediatePropagation();
    return;
  }
  // The outgoing photo is still visible while the incoming source decodes.
  // Suppress photo-targeted shortcuts until the visible identity commits so
  // ratings, edits, and zoom operations cannot affect a hidden photo.
  if (_lbVisualTransitionPending && e.key !== 'Escape') {
    e.preventDefault();
    e.stopImmediatePropagation();
    return;
  }
  if (!editable0 && !e.ctrlKey && !e.metaKey && !e.altKey && !e.shiftKey && !lightboxKeyMatchesConfiguredBrowseShortcut(e)) {
    if (e.key.toLowerCase() === 'f') {
      requestLightboxFullscreen();
      e.preventDefault();
      e.stopImmediatePropagation();
      return;
    }
    if (e.key.toLowerCase() === 'g') {
      exitLightboxFullscreen();
      closeLightbox();
      e.preventDefault();
      e.stopImmediatePropagation();
      return;
    }
  }
  var boxesKey = (window._vireoShortcuts && window._vireoShortcuts.browse && window._vireoShortcuts.browse.toggle_boxes) || 'b';
  if (matchesShortcut(e, boxesKey)) {
    if (!_lbKeyTargetEditable(e.target)) {
      toggleLightboxBoxes();
      e.preventDefault();
      e.stopImmediatePropagation();
      return;
    }
  }
  // Presence check: `''` means the migration blanked this to avoid stealing
  // an existing user binding on 'h'. Only fall back to the 'h' default when
  // the key is genuinely absent from config (shortcuts not yet loaded).
  var browseSc = window._vireoShortcuts && window._vireoShortcuts.browse;
  var uiKey = browseSc && 'toggle_ui' in browseSc ? browseSc.toggle_ui : 'h';
  if (matchesShortcut(e, uiKey)) {
    if (!_lbKeyTargetEditable(e.target)) {
      toggleLightboxChrome();
      e.preventDefault();
      e.stopImmediatePropagation();
      return;
    }
  }
  var zoomKey = (window._vireoShortcuts && window._vireoShortcuts.browse) ? window._vireoShortcuts.browse.zoom : 'z';
  if (matchesShortcut(e, zoomKey) && !_lbKeyTargetEditable(e.target)) {
    // Zoom toggle — simulate a click in the center
    var wrap = document.getElementById('lightboxWrap');
    var rect = wrap.getBoundingClientRect();
    toggleLightboxZoom({clientX: rect.left + rect.width/2, clientY: rect.top + rect.height/2, stopPropagation: function(){}});
    e.preventDefault();
    e.stopImmediatePropagation();
    return;  // Stop here so a user-remapped zoomKey (e.g. '+') doesn't also trigger step-zoom below.
  }
  // Skip when focus is in a form field — modals opened from the lightbox (e.g. iNaturalist)
  // need '-' / '0' as literal input in lat/lon fields.
  var editable = _lbKeyTargetEditable(e.target);
  // Flag / Reject / Unflag — operate on the photo currently displayed in the
  // lightbox. The lightbox is shared across pages; setFlagFor (browse) and
  // setReviewFlag (review) update their page's local model, so prefer them
  // when available so badges/grids stay in sync without a refetch. The bare
  // POST fallback covers pages that open the lightbox without a flag helper
  // (misses, pipeline-review). Lives outside the no-modifier guard below so
  // user rebindings to combos like Ctrl+P still match — matchesShortcut
  // already enforces the exact modifier set the binding declares.
  if (!editable) {
    var flagKey = (window._vireoShortcuts && window._vireoShortcuts.browse && window._vireoShortcuts.browse.flag) || 'p';
    var rejectKey = (window._vireoShortcuts && window._vireoShortcuts.browse && window._vireoShortcuts.browse.reject) || 'x';
    var unflagKey = (window._vireoShortcuts && window._vireoShortcuts.browse && window._vireoShortcuts.browse.unflag) || 'u';
    var lbFlag = null;
    if (matchesShortcut(e, flagKey)) lbFlag = 'flagged';
    else if (matchesShortcut(e, rejectKey)) lbFlag = 'rejected';
    else if (matchesShortcut(e, unflagKey)) lbFlag = 'none';
    if (lbFlag !== null && _lightboxCurrentId != null) {
      var pid = _lightboxCurrentId;
      if (
        typeof window.handleMissesLightboxFlagShortcut === 'function' &&
        window.handleMissesLightboxFlagShortcut(pid, lbFlag) === true
      ) {
        e.preventDefault();
        e.stopImmediatePropagation();
        return;
      }
      _lbApplyFlag(pid, lbFlag);
      e.preventDefault();
      e.stopImmediatePropagation();
      return;
    }
  }
  // Continuous zoom keyboard shortcuts. stopImmediatePropagation prevents these keys
  // from reaching the browse-page rating handler — otherwise pressing '0' to zoom-to-fit
  // would also fire rate_0 and silently clear the photo's rating.
  if (!editable && !e.ctrlKey && !e.metaKey && !e.altKey) {
    if (e.key === '+' || e.key === '=') {
      _lbClearPendingViewportRestore();
      _lbSetZoom(_lbZoom * 1.25, null, null);
      e.preventDefault();
      e.stopImmediatePropagation();
    } else if (e.key === '-' || e.key === '_') {
      _lbClearPendingViewportRestore();
      _lbSetZoom(_lbZoom * 0.8, null, null);
      e.preventDefault();
      e.stopImmediatePropagation();
    } else if (e.key === '0') {
      _lbClearPendingViewportRestore();
      _lbSetZoom(1.0, null, null);
      e.preventDefault();
      e.stopImmediatePropagation();
    }
  }
});

/* ---------- Ctrl+Tab / Ctrl+Shift+Tab — cycle navbar tabs ---------- */
document.addEventListener('keydown', function(e) {
  if (e.key !== 'Tab' || !e.ctrlKey || e.altKey || e.metaKey) return;
  e.preventDefault();
  var navLinks = Array.prototype.slice.call(
    document.querySelectorAll('#navTabStrip .nav-tab[href]')
  );
  if (!navLinks.length) return;
  var activeIdx = -1;
  for (var i = 0; i < navLinks.length; i++) {
    if (navLinks[i].classList.contains('active')) { activeIdx = i; break; }
  }
  var dir = e.shiftKey ? -1 : 1;
  var next = (activeIdx + dir + navLinks.length) % navLinks.length;
  window.location.href = navLinks[next].getAttribute('href');
});

/* ---------- Keyboard Shortcut Helpers (global) ---------- */
function parseShortcut(str) {
  var parts = str.toLowerCase().split('+');
  var key = parts.pop();
  var mods = {ctrl: false, meta: false, shift: false, alt: false};
  parts.forEach(function(m) { if (m in mods) mods[m] = true; });
  return {key: key, ctrl: mods.ctrl, meta: mods.meta, shift: mods.shift, alt: mods.alt};
}

function matchesShortcut(e, shortcutStr) {
  if (!shortcutStr) return false;
  var sc = parseShortcut(shortcutStr);
  if (e.key.toLowerCase() !== sc.key) return false;
  var wantCtrl = sc.ctrl || sc.meta;
  var hasCtrl = e.ctrlKey || e.metaKey;
  if (wantCtrl !== hasCtrl) return false;
  if (sc.shift !== e.shiftKey) return false;
  if (sc.alt !== e.altKey) return false;
  return true;
}

function formatShortcut(str) {
  if (!str) return 'Unassigned';
  return str.split('+').map(function(p) {
    if (p === ' ') return 'Space';
    return p.charAt(0).toUpperCase() + p.slice(1);
  }).join('+');
}

function _lbApplyTransform() {
  var t = document.getElementById('lightboxTransform');
  if (!t) return;
  var metrics = _lbUpdateLayoutMetrics();
  if (!metrics) {
    _lbUpdateZoomControl();
    return;
  }
  var renderedW = metrics.w * metrics.scale;
  var renderedH = metrics.h * metrics.scale;
  var tx = (metrics.wrapW - renderedW) / 2 + _lbPanX;
  var ty = (metrics.wrapH - renderedH) / 2 + _lbPanY;
  t.style.transform = 'translate(' + tx + 'px, ' + ty + 'px) scale(' + metrics.scale + ')';
  _lbUpdateZoomControl();
}

function _lbOrientedPhotoDims(img) {
  if (!_lbPhotoW || !_lbPhotoH) return null;
  var w = _lbPhotoW;
  var h = _lbPhotoH;
  if (_lbOrientationSwapsAxes(_lbPhotoOrientation)) {
    w = _lbPhotoH;
    h = _lbPhotoW;
  } else if (img && img.naturalWidth && img.naturalHeight) {
    var storedAspect = w / h;
    var imgAspect = img.naturalWidth / img.naturalHeight;
    if (Math.abs(storedAspect - imgAspect) > Math.abs((1 / storedAspect) - imgAspect)) {
      w = _lbPhotoH;
      h = _lbPhotoW;
    }
  }
  return { w: w, h: h };
}

function _lbDisplayDimsForRecipe(width, height, recipe) {
  var w = Number(width) || 0;
  var h = Number(height) || 0;
  if (!w || !h) return { w: null, h: null };
  recipe = recipe || {};
  var rotation = Number(recipe.rotation || 0);
  if (rotation === 90 || rotation === 270) {
    var tmp = w; w = h; h = tmp;
  }
  var crop = recipe.crop;
  if (crop && Number(crop.w) > 0 && Number(crop.h) > 0) {
    w = Math.max(1, Math.round(w * Number(crop.w)));
    h = Math.max(1, Math.round(h * Number(crop.h)));
  }
  return { w: w, h: h };
}

function _lbLayoutDims() {
  var img = document.getElementById('lightboxImg');
  if (!img) return null;
  // A developed companion may have been cropped or resized independently of
  // its RAW primary. Use the pixels that actually loaded so the lightbox never
  // stretches the JPEG into the RAW row's catalog dimensions.
  if (
    _lightboxCurrentId != null &&
    _vireoPairKnownByPhoto[String(_lightboxCurrentId)] &&
    _vireoPairSource(_lightboxCurrentId) === 'jpeg' &&
    img.naturalWidth && img.naturalHeight
  ) {
    return { w: img.naturalWidth, h: img.naturalHeight };
  }
  // While /original is available, keep the transform layer in original photo
  // coordinates even if the currently displayed tier is /full. That matches the
  // sharp natural-layout path and avoids recalibrating pan/zoom when the source swaps.
  var originalDims = !_lbOriginalUnavailable ? _lbOrientedPhotoDims(img) : null;
  if (originalDims) {
    return _lbDisplayDimsForRecipe(
      originalDims.w,
      originalDims.h,
      _lbCurrentEditRecipe
    );
  }
  if (img.naturalWidth && img.naturalHeight) {
    return { w: img.naturalWidth, h: img.naturalHeight };
  }
  return null;
}

function _lbUpdateLayoutMetrics() {
  var wrap = document.getElementById('lightboxWrap');
  var t = document.getElementById('lightboxTransform');
  if (!wrap || !t) return null;
  var dims = _lbLayoutDims();
  if (!dims || !dims.w || !dims.h) return null;
  var wrapW = wrap.clientWidth;
  var wrapH = wrap.clientHeight;
  if (!wrapW || !wrapH) return null;
  _lbFitScale = Math.min(1.0, wrapW / dims.w, wrapH / dims.h);
  if (!_lbFitScale || _lbFitScale <= 0) _lbFitScale = 1.0;
  t.style.width = dims.w + 'px';
  t.style.height = dims.h + 'px';
  var scale = _lbFitScale * _lbZoom;
  return { w: dims.w, h: dims.h, wrapW: wrapW, wrapH: wrapH, scale: scale };
}

function _lbRecomputeNativeZoom() {
  // Compute zoom value that corresponds to 1:1: one decoded source pixel per
  // physical display pixel. On Retina/HiDPI displays this is intentionally
  // smaller than CSS 1:1 by devicePixelRatio.
  var img = document.getElementById('lightboxImg');
  if (!img || !img.complete || !img.naturalWidth || !img.naturalHeight) {
    _lbNativeZoom = null;
    return;
  }
  if (!_lbPhotoW && !_lbPhotoH && !_lbOriginalUnavailable && _lbCurrentSrcKey !== 'original') {
    // A preview/full tier can load before the metadata request returns. Do not
    // treat that tier's decoded size as true 1:1; keep z/click in pending mode
    // so the later original dimensions can upgrade the zoom accurately.
    _lbNativeZoom = null;
    return;
  }
  var metrics = _lbUpdateLayoutMetrics();
  if (!metrics || !_lbFitScale) {
    _lbNativeZoom = null;
    return;
  }
  var dpr = window.devicePixelRatio || 1;
  // Guard: very small images (nativeZoom < 1) clamp to 1.0 so fit == 1:1
  _lbNativeZoom = Math.max(1.0, (1 / dpr) / _lbFitScale);
  _lbApplyTransform();
}

function _lbSyncSourceForZoom(targetZoom, preserveSharperSource) {
  var desiredSource = _lbPickSourceKey(targetZoom);
  if (preserveSharperSource && _lbSrcRank(_lbCurrentSrcKey) > _lbSrcRank(desiredSource)) {
    // The current pixels already exceed this zoom's requirement. Cancel any
    // queued downgrade and retain them; this is especially important for the
    // explicit 1:1 stop, where replacing /original with a lower tier adds work
    // and can leave a failed non-original swap stranded.
    if (_lbSwapTimer) {
      clearTimeout(_lbSwapTimer);
      _lbSwapTimer = null;
    }
    _lbDesiredSrcKey = _lbCurrentSrcKey;
    _lbScheduleAdjacentPhoto(_lbCurrentSrcKey);
    return;
  }
  _lbScheduleSourceSwap(targetZoom);
}

function _lbSetZoom(newZoom, anchorClientX, anchorClientY, preserveSharperSource) {
  // Any zoom mutation cancels a pending "upgrade to 1:1 on original-load" intent.
  // Callers that still want it (toggleLightboxZoom's fallback path) re-set the flag
  // after calling us.
  _lbPending1To1 = false;
  _lbPending1To1Anchor = null;
  // Clamp to valid range. Max is 4x native (or 4x fit if no nativeZoom).
  var maxZoom = _lbMaxZoom();
  newZoom = Math.max(1.0, Math.min(maxZoom, newZoom));
  var wrap = document.getElementById('lightboxWrap');
  var t = document.getElementById('lightboxTransform');
  if (!wrap || !t) return;

  var metrics = _lbUpdateLayoutMetrics();
  if (!metrics) {
    _lbZoom = newZoom;
    if (_lbZoom <= 1.001) {
      _lbPanX = 0;
      _lbPanY = 0;
    }
    wrap.classList.toggle('zoomed', _lbZoom > 1.001);
    _lbApplyTransform();
    _lbSyncSourceForZoom(newZoom, preserveSharperSource);
    _lbSaveViewportState(_lightboxCurrentId);
    return;
  }
  // Cursor at clientX has image-local coord (clientX - rect.left) / scale.
  // Anchor invariant: that image coord must still land under the cursor after zoom.
  var rect = t.getBoundingClientRect();
  var anchorX, anchorY;
  if (anchorClientX != null && anchorClientY != null) {
    anchorX = anchorClientX;
    anchorY = anchorClientY;
  } else {
    // Center anchor: use wrap's visual center
    var wrapRect = wrap.getBoundingClientRect();
    anchorX = wrapRect.left + wrapRect.width / 2;
    anchorY = wrapRect.top + wrapRect.height / 2;
  }
  var currentScale = metrics.scale;
  var imgX = (anchorX - rect.left) / currentScale;
  var imgY = (anchorY - rect.top) / currentScale;
  var newScale = _lbFitScale * newZoom;
  var newBaseX = anchorX - imgX * newScale;
  var newBaseY = anchorY - imgY * newScale;
  var wrapRectForBase = wrap.getBoundingClientRect();
  _lbPanX = newBaseX - (wrapRectForBase.left + (wrapRectForBase.width - metrics.w * newScale) / 2);
  _lbPanY = newBaseY - (wrapRectForBase.top + (wrapRectForBase.height - metrics.h * newScale) / 2);
  _lbZoom = newZoom;

  if (_lbZoom <= 1.001) {
    _lbPanX = 0;
    _lbPanY = 0;
  }

  _lbClampPan();
  wrap.classList.toggle('zoomed', _lbZoom > 1.001);
  _lbApplyTransform();
  _lbSyncSourceForZoom(newZoom, preserveSharperSource);
  _lbSaveViewportState(_lightboxCurrentId);
}

function _lbClampPan() {
  var metrics = _lbUpdateLayoutMetrics();
  if (!metrics) return;
  // Effective rendered size after scale
  var scaledW = metrics.w * metrics.scale;
  var scaledH = metrics.h * metrics.scale;
  var baseLeft = (metrics.wrapW - scaledW) / 2;
  var baseTop = (metrics.wrapH - scaledH) / 2;
  // After applying transform, rendered top-left is base + pan.
  // Clamp so at least minVisible px of image stays inside the wrap, per axis.
  var minVisible = 80;
  // X: rendered right >= minVisible; rendered left <= wrapW - minVisible
  //   panX >= minVisible - baseLeft - scaledW
  //   panX <= wrapW - minVisible - baseLeft
  var minPanX = minVisible - baseLeft - scaledW;
  var maxPanX = metrics.wrapW - minVisible - baseLeft;
  if (scaledW <= metrics.wrapW) {
    // Image fits horizontally; center it
    _lbPanX = 0;
  } else {
    _lbPanX = Math.max(minPanX, Math.min(maxPanX, _lbPanX));
  }
  var minPanY = minVisible - baseTop - scaledH;
  var maxPanY = metrics.wrapH - minVisible - baseTop;
  if (scaledH <= metrics.wrapH) {
    _lbPanY = 0;
  } else {
    _lbPanY = Math.max(minPanY, Math.min(maxPanY, _lbPanY));
  }
}

var _lbSwapTimer = null;
var _lbDesiredSrcKey = null;

function _lbSetPreviewLoading(loading) {
  _lbPreviewLoading = !!loading;
  _lbRenderDetailStatus();
}

// ---- Detail-load status chip ------------------------------------------------
// The lightbox paints a lower-resolution tier first and swaps a sharper one in
// behind it, so "this photo is soft" and "this photo is still arriving" look
// identical on screen. The chip names which one it is: Loading (the incoming
// photo has not decoded yet), Sharpening (a sharper tier is in flight), then a
// brief Full detail once the tier the current zoom needs is the one displayed.
// Loads that finish inside LB_DETAIL_SHOW_DELAY_MS never show anything, so
// arrowing through cached photos stays quiet instead of strobing.
var LB_DETAIL_SHOW_DELAY_MS = 250;
var LB_DETAIL_SETTLED_MS = 900;
var LB_DETAIL_FADE_MS = 250;
var _lbInitialDecodePending = false; // this photo's first bitmap has not decoded
// openLightbox publishes its completion here so anything that takes the image
// loader over mid-load can still end that load properly. Replacing
// img.onload/img.onerror without this leaves the navigation transition frozen:
// the action bar stays inert, deferred overlays never paint, and the status chip
// never clears, all long after the replacement bitmap has rendered.
var _lbPendingInitialLoadCommit = null;
var _lbPendingInitialLoadAbandon = null;

function _lbClearPendingInitialLoad() {
  _lbPendingInitialLoadCommit = null;
  _lbPendingInitialLoadAbandon = null;
}

function _lbFinishInitialLoad() {
  var finish = _lbPendingInitialLoadCommit;
  if (!finish) return false;
  _lbClearPendingInitialLoad();
  finish();
  return true;
}

// The failure counterpart. A displaced load that will never produce a bitmap
// still has to hand over: the incoming photo's identity must be committed
// before its controls come back, or the user can act on a photo the filename,
// counter and _lightboxCommittedId are not showing.
function _lbAbandonInitialLoad() {
  var abandon = _lbPendingInitialLoadAbandon;
  if (!abandon) return false;
  _lbClearPendingInitialLoad();
  abandon();
  return true;
}
var _lbDetailStatusShown = false;    // chip is on screen (past the show delay)
var _lbDetailStatusSettled = false;  // showing the Full detail confirmation
var _lbDetailStatusHold = 0;         // >0 suppresses paints mid-transition
var _lbDetailShowTimer = null;
var _lbDetailSettleTimer = null;
var _lbDetailFadeTimer = null;

function _lbDetailSharpeningPending() {
  // Derived rather than read off _lbPreviewLoading: the first drag of a pan
  // calls _lbClearPendingViewportRestore, which drops that flag even though the
  // queued swap keeps running. Panning around while the original loads at 1:1
  // is the pixel-peeping path, so the chip has to survive it.
  if (_lbProgressiveTargetKey) return true;
  if (!_lbDesiredSrcKey || _lbDesiredSrcKey === _lbCurrentSrcKey) return false;
  // A known-missing original is never going to arrive; claiming otherwise would
  // leave the chip spinning forever.
  if (_lbOriginalUnavailable && _lbDesiredSrcKey === 'original') return false;
  return _lbSrcRank(_lbDesiredSrcKey) > _lbSrcRank(_lbCurrentSrcKey);
}

function _lbDetailStatusPhase() {
  // _lbVisualTransitionPending is navigation-only -- it stays false when the
  // lightbox opens from closed, which is the case with the emptiest overlay and
  // the most need for a signal. _lbInitialDecodePending covers both.
  if (_lbVisualTransitionPending || _lbInitialDecodePending) return 'loading';
  if (_lbPreviewLoading || _lbDetailSharpeningPending()) return 'sharpening';
  if (_lbDetailStatusSettled) return 'full';
  return 'off';
}

function _lbDetailStatusLabel(phase) {
  if (phase === 'loading') return 'Loading\u2026';
  if (phase === 'sharpening') return 'Sharpening\u2026';
  return 'Full detail';
}

function _lbPaintDetailStatus(phase) {
  var el = document.getElementById('lightboxPreviewStatus');
  if (!el) return;
  var text = document.getElementById('lightboxPreviewStatusText');
  if (phase === 'off') {
    el.style.display = 'none';
    el.classList.remove('is-full');
    el.classList.remove('is-fading');
    el.removeAttribute('data-phase');
    if (text) text.textContent = '';
    return;
  }
  el.style.display = 'inline-flex';
  el.setAttribute('data-phase', phase);
  el.classList.toggle('is-full', phase === 'full');
  if (phase !== 'full') el.classList.remove('is-fading');
  if (text) text.textContent = _lbDetailStatusLabel(phase);
}

function _lbCancelDetailConfirmation() {
  if (_lbDetailSettleTimer) { clearTimeout(_lbDetailSettleTimer); _lbDetailSettleTimer = null; }
  if (_lbDetailFadeTimer) { clearTimeout(_lbDetailFadeTimer); _lbDetailFadeTimer = null; }
  _lbDetailStatusSettled = false;
}

function _lbResetDetailStatus() {
  if (_lbDetailShowTimer) { clearTimeout(_lbDetailShowTimer); _lbDetailShowTimer = null; }
  _lbCancelDetailConfirmation();
  _lbDetailStatusShown = false;
  _lbPaintDetailStatus('off');
}

function _lbRenderDetailStatus() {
  if (_lbDetailStatusHold > 0) return;
  var phase = _lbDetailStatusPhase();
  if (phase === 'off') {
    _lbResetDetailStatus();
    return;
  }
  if (phase === 'full') {
    // Only confirm a load the user actually watched happen.
    if (!_lbDetailStatusShown) { _lbResetDetailStatus(); return; }
    _lbPaintDetailStatus('full');
    return;
  }
  // A fresh load started: the previous confirmation is stale.
  _lbCancelDetailConfirmation();
  if (_lbDetailStatusShown) { _lbPaintDetailStatus(phase); return; }
  if (_lbDetailShowTimer) return;  // delay already armed; do not restart it
  _lbDetailShowTimer = setTimeout(function() {
    _lbDetailShowTimer = null;
    var next = _lbDetailStatusPhase();
    if (next !== 'loading' && next !== 'sharpening') { _lbResetDetailStatus(); return; }
    _lbDetailStatusShown = true;
    _lbPaintDetailStatus(next);
  }, LB_DETAIL_SHOW_DELAY_MS);
}

function _lbMarkDetailSettled() {
  // Nothing was announced, so there is nothing to confirm.
  if (!_lbDetailStatusShown) return;
  // /original failed for this photo, so the displayed tier may hold fewer pixels
  // than the file does -- and when it does, _lbLayoutDims rebases onto the
  // fallback, making 1:1 mean 1:1 of the preview while the zoom badge still
  // reads 100%. Rather than work out whether this particular view loses detail
  // (crop, rotation, JPEG companion, viewport and DPR all change the answer),
  // withhold the claim whenever the original is gone. Absence asserts nothing;
  // "Full detail" would assert something we cannot vouch for.
  if (_lbOriginalUnavailable) return;
  _lbCancelDetailConfirmation();
  _lbDetailStatusSettled = true;
  _lbDetailFadeTimer = setTimeout(function() {
    _lbDetailFadeTimer = null;
    var el = document.getElementById('lightboxPreviewStatus');
    if (el && _lbDetailStatusSettled) el.classList.add('is-fading');
  }, Math.max(0, LB_DETAIL_SETTLED_MS - LB_DETAIL_FADE_MS));
  _lbDetailSettleTimer = setTimeout(function() {
    _lbDetailSettleTimer = null;
    _lbDetailStatusSettled = false;
    _lbDetailStatusShown = false;
    _lbRenderDetailStatus();
  }, LB_DETAIL_SETTLED_MS);
}

function _lbFullPreviewLimit() {
  if (_lbPreviewMaxSize === 0 || _lbFullUsesOriginal === true || _lbSessionFullUsesOriginal === true) return Infinity;
  return _lbPreviewMaxSize || 1920;
}

function _lbInitialSourceKey(photo, zoom, oneToOne) {
  var w = Number(photo.width), h = Number(photo.height);
  if (!w || !h) return oneToOne || zoom > 1.001 ? 'original' : 'full';
  if (_vireoPairKnownByPhoto[String(photo.id)] && _vireoPairSource(photo.id) === 'jpeg') {
    // A paired JPEG can have different geometry from its RAW catalog row.
    return oneToOne || zoom > 1.001 ? 'original' : 'full';
  }
  if (_lbOrientationSwapsAxes(_lbMetadataOrientation(photo.metadata))) {
    var tmp = w; w = h; h = tmp;
  }
  var dims = _lbDisplayDimsForRecipe(w, h, photo.edit_recipe);
  var wrap = document.getElementById('lightboxWrap');
  if (!wrap || !wrap.clientWidth || !wrap.clientHeight) return oneToOne || zoom > 1.001 ? 'original' : 'full';
  var fit = Math.min(1, wrap.clientWidth / dims.w, wrap.clientHeight / dims.h);
  var needed = oneToOne ? Math.max(dims.w, dims.h)
    : Math.max(dims.w, dims.h) * fit * zoom * (window.devicePixelRatio || 1);
  var fullEdge = _lbFullPreviewLimit();
  if (needed <= fullEdge) return 'full';
  if (needed <= 2560) return '2560';
  if (needed <= 3840) return '3840';
  return 'original';
}

function _lbDecodedInitialPreview(photoId, desired) {
  var entries = Object.values(_lbAdjacentPreloads);
  if (_lbOriginalPreload) entries.push(_lbOriginalPreload);
  var ready = entries.filter(function(entry) {
    return entry.photoId === photoId && entry.status === 'decoded' &&
      entry.url === _lbSrcUrl(photoId, entry.sourceKey, true);
  });
  var exact = ready.find(function(entry) { return entry.sourceKey === desired; });
  if (exact) return exact;
  // A ready smaller preview is safe only for this exact render/source version.
  // Do not flash an old crop, an unedited thumbnail, or the other RAW/JPEG pair.
  return ready.filter(function(entry) {
    return _lbSrcRank(entry.sourceKey) <= _lbSrcRank(desired);
  }).sort(function(a, b) { return _lbSrcRank(b.sourceKey) - _lbSrcRank(a.sourceKey); })[0] || null;
}

function _lbPickSourceKey(targetZoom) {
  // Returns 'full' | '2560' | '3840' | 'original' based on current display needs.
  // Fall back to /full at fit and /original when zoomed whenever we can't compute
  // a proper displayed-long-edge (photo dims missing, or nativeZoom not yet
  // established — the latter happens briefly when API dims arrive before the
  // initial image load completes).
  var dims = _lbLayoutDims();
  var zoom = targetZoom != null ? targetZoom : _lbZoom;
  if (!dims || !_lbNativeZoom) {
    return (zoom > 1.001 && !_lbOriginalUnavailable) ? 'original' : 'full';
  }
  var longEdge = Math.max(dims.w, dims.h);
  var displayedLong = longEdge * _lbFitScale * zoom;
  // DPR consideration: high-DPI screens need more pixels for a crisp display.
  var dpr = window.devicePixelRatio || 1;
  var needed = displayedLong * dpr;
  // /full's actual long edge depends on the server's preview_max_size config (default 1920).
  // Prefer this photo's decoded size. Before it loads, use the configured cap;
  // a previous small photo may never have reached that cap.
  var fullLongEdge = _lbFullLongEdge || _lbFullPreviewLimit();
  if (needed <= fullLongEdge) return 'full';
  if (needed <= 2560) return '2560';
  if (needed <= 3840) return '3840';
  return _lbOriginalUnavailable ? '3840' : 'original';
}

function _lbSrcUrl(photoId, key, speculative) {
  var url;
  if (key === 'original') url = '/photos/' + photoId + '/original';
  else if (key === 'full') url = '/photos/' + photoId + '/full';
  else url = '/photos/' + photoId + '/preview?size=' + key;
  url = window.vireoRenderedUrl
    ? window.vireoRenderedUrl(url, photoId)
    : _vireoUrlWithRenderVersion(url, photoId);
  var version = _lbEditVersionByPhoto[String(photoId)];
  if (version) url = _vireoUrlWithQueryParam(url, 'editv', version);
  var speculativeUrl = _vireoUrlWithQueryParam(url, 'prefetch', '1');
  if (speculative) return speculativeUrl;

  // Keep the exact URL of a successfully decoded warmup. Browser caches are
  // URL-keyed, so dropping the prefetch marker here would force a second
  // transfer/decode and throw away the retained Image object's main benefit.
  var decodedWarmup = Object.values(_lbAdjacentPreloads).some(function(entry) {
    return entry.photoId === photoId && entry.sourceKey === key &&
      entry.status === 'decoded' && entry.url === speculativeUrl;
  });
  if (
    !decodedWarmup && _lbOriginalPreload && key === 'original' &&
    _lbOriginalPreload.photoId === photoId &&
    _lbOriginalPreload.status === 'decoded' &&
    _lbOriginalPreload.url === speculativeUrl
  ) decodedWarmup = true;
  if (decodedWarmup) return speculativeUrl;
  return url;
}

function _lbCancelAdjacentPreloadTimer() {
  if (_lbAdjacentPreloadTimer) {
    clearTimeout(_lbAdjacentPreloadTimer);
    _lbAdjacentPreloadTimer = null;
  }
}

function _lbClearAdjacentPreloads() {
  _lbCancelAdjacentPreloadTimer();
  Object.keys(_lbAdjacentPreloads).forEach(function(cacheKey) {
    var entry = _lbAdjacentPreloads[cacheKey];
    _lbReleasePreloadImage(entry);
  });
  _lbAdjacentPreloads = {};
  _lbAdjacentPreloadRetry = {};
}

function _lbCancelOriginalPreload() {
  if (_lbOriginalPreloadTimer) {
    clearTimeout(_lbOriginalPreloadTimer);
    _lbOriginalPreloadTimer = null;
  }
  _lbReleasePreloadImage(_lbOriginalPreload);
  _lbOriginalPreload = null;
  _lbOriginalPreloadWaiting = null;
}

function _lbShouldPreloadOriginal(photoId) {
  if (
    photoId == null ||
    _lightboxCurrentId !== photoId ||
    _lbCurrentSrcKey !== 'full' ||
    _lbFullUsesOriginal !== false ||
    _lbOriginalUnavailable ||
    _lbZoom > 1.001 ||
    (_lbDesiredSrcKey && _lbDesiredSrcKey !== 'full') ||
    !_lbNativeZoom ||
    _lbPickSourceKey(_lbNativeZoom) !== 'original'
  ) return false;
  var img = document.getElementById('lightboxImg');
  return !!(img && img.complete && img.naturalWidth && img.naturalHeight);
}

function _lbReleasePreloadImage(entry) {
  // Clearing src does not cancel synchronous server-side generation. Keep the
  // outstanding requests observable even if their decoded-cache entries go.
  if (!entry || !entry.img || _lbSpeculativeLoads.has(entry)) return;
  entry.img.onload = null;
  entry.img.onerror = null;
  entry.img.removeAttribute('src');
}

function _lbSmallPreviewKey() {
  // /full is configurable, including an original-resolution mode. Never use
  // that mode (or a large custom preview) for the wide navigation window.
  return _lbFullPreviewLimit() > 1920 ? '1920' : 'full';
}

function _lbEstimatePreloadBytes(photo, key) {
  var cached = _lbPhotoDataByPhoto[String(photo.id)] || photo;
  var w = Number(cached.width || photo.width) || _lbPhotoW || 6000;
  var h = Number(cached.height || photo.height) || _lbPhotoH || 4000;
  var size = key === 'original' ? Math.max(w, h)
    : key === 'full' ? _lbFullPreviewLimit() : Number(key);
  var scale = Math.min(1, size / Math.max(w, h));
  return Math.ceil(w * scale) * Math.ceil(h * scale) * 4;
}

function _lbPreloadBytes() {
  var entries = new Set(Object.values(_lbAdjacentPreloads));
  _lbSpeculativeLoads.forEach(function(entry) { entries.add(entry); });
  if (_lbOriginalPreload) entries.add(_lbOriginalPreload);
  var bytes = 0;
  entries.forEach(function(entry) { bytes += entry.bytes || 0; });
  return bytes;
}

function _lbTrimPreloadBudget() {
  // Estimates reserve space before starting each decode. Correct them from
  // actual dimensions afterward, dropping distant bitmaps first. This bounds
  // retained pixel buffers, not the WebView's independent graphics/HTTP cache.
  // A larger-than-estimated original should not evict the whole small-preview
  // window before being discarded itself.
  if (_lbPreloadBytes() > _lbPreloadBudgetBytes && _lbOriginalPreload &&
      _lbOriginalPreload.status === 'decoded') _lbCancelOriginalPreload();
  var currentIdx = _lightboxPhotoList.findIndex(function(p) { return p.id === _lightboxCurrentId; });
  var entries = Object.entries(_lbAdjacentPreloads).filter(function(item) {
    return item[1].status === 'decoded';
  });
  entries.sort(function(a, b) {
    function distance(entry) {
      return Math.abs(_lightboxPhotoList.findIndex(function(p) { return p.id === entry.photoId; }) - currentIdx);
    }
    return Number(b[1].bytes > _lbPreloadBudgetBytes) - Number(a[1].bytes > _lbPreloadBudgetBytes) ||
      distance(b[1]) - distance(a[1]) || b[1].bytes - a[1].bytes;
  });
  while (_lbPreloadBytes() > _lbPreloadBudgetBytes && entries.length) {
    var item = entries.shift();
    _lbReleasePreloadImage(item[1]);
    delete _lbAdjacentPreloads[item[0]];
    // Do not immediately re-request an underestimated image we just evicted.
    _lbAdjacentPreloadRetry[item[0]] = { openSeq: _lbOpenSeq, count: 2, at: 0 };
  }
}

function _lbRunSpeculativePreload(entry, onSettled) {
  _lbSpeculativeLoads.add(entry);
  _lbSpeculativeInFlight = _lbSpeculativeLoads.values().next().value || null;
  var preload = entry.img;
  preload.fetchPriority = 'low';
  function settle(decoded) {
    if (entry.status !== 'loading') return;
    entry.status = decoded ? 'decoded' : 'failed';
    _lbSpeculativeLoads.delete(entry);
    _lbSpeculativeInFlight = _lbSpeculativeLoads.values().next().value || null;
    if (decoded) entry.bytes = preload.naturalWidth * preload.naturalHeight * 4;
    onSettled(decoded);
    _lbTrimPreloadBudget();
    // A navigation/close may have retired this request while it was decoding.
    if (!Object.values(_lbAdjacentPreloads).includes(entry) && _lbOriginalPreload !== entry) {
      _lbReleasePreloadImage(entry);
    }
    var overlay = document.getElementById('lightboxOverlay');
    if (overlay && overlay.classList.contains('active')) {
      _lbScheduleAdjacentPhoto(_lbCurrentSrcKey);
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

function _lbScheduleOriginalPreload(photoId, retryCount) {
  _lbCancelOriginalPreload();
  if (!_lbShouldPreloadOriginal(photoId)) return;
  // Dwell prevents fast arrowing from preparing every full-resolution RAW.
  // Once eligible, wait behind ready neighbors without spending a retry on
  // our own occupied server slot.
  _lbOriginalPreloadTimer = setTimeout(function() {
    _lbOriginalPreloadTimer = null;
    if (!_lbShouldPreloadOriginal(photoId)) return;
    _lbOriginalPreloadWaiting = { photoId: photoId, retryCount: retryCount || 0 };
    _lbScheduleAdjacentPhoto(_lbCurrentSrcKey);
  }, 1400);
}

function _lbStartPendingOriginalPreload() {
  if (_lbSpeculativeInFlight || !_lbOriginalPreloadWaiting) return;
  var pending = _lbOriginalPreloadWaiting;
  _lbOriginalPreloadWaiting = null;
  if (!_lbShouldPreloadOriginal(pending.photoId)) return;
  var entry = {
    img: new Image(),
    photoId: pending.photoId,
    sourceKey: 'original',
    url: _lbSrcUrl(pending.photoId, 'original', true),
    status: 'loading',
    bytes: _lbEstimatePreloadBytes({ id: pending.photoId, width: _lbPhotoW, height: _lbPhotoH }, 'original')
  };
  if (_lbPreloadBytes() + entry.bytes > _lbPreloadBudgetBytes) return;
  _lbOriginalPreload = entry;
  _lbRunSpeculativePreload(entry, function(decoded) {
    if (_lbOriginalPreload !== entry) return;
    if (!decoded) {
      // A failed image retains no pixels and must not reserve its estimate.
      _lbOriginalPreload = null;
      if (pending.retryCount < 1 && _lbShouldPreloadOriginal(pending.photoId)) {
        _lbScheduleOriginalPreload(pending.photoId, pending.retryCount + 1);
      }
    }
  });
}

function _lbPrimeAdjacentPhotos(sourceKey) {
  var overlay = document.getElementById('lightboxOverlay');
  var visibleImg = document.getElementById('lightboxImg');
  if (!overlay || !overlay.classList.contains('active') ||
      _lbVisualTransitionPending || !visibleImg ||
      !visibleImg.complete || !visibleImg.naturalWidth) return;
  // The old bitmap remains visible during a zoom source upgrade. Give that
  // interactive request priority over queued neighbors of the outgoing tier.
  // The source-load handler resumes warmups once the new pixels are ready.
  if (_lbDesiredSrcKey && _lbDesiredSrcKey !== _lbCurrentSrcKey) return;
  if (_lightboxPhotoList.length < 2 || _lightboxCurrentId == null) {
    _lbClearAdjacentPreloads();
    _lbStartPendingOriginalPreload();
    return;
  }
  var currentIdx = _lightboxPhotoList.findIndex(function(photo) {
    return photo.id === _lightboxCurrentId;
  });
  if (currentIdx === -1) {
    _lbStartPendingOriginalPreload();
    return;
  }

  _lbCancelAdjacentPreloadTimer();
  var key = sourceKey || 'full';
  var baseKey = _lbSmallPreviewKey();
  var direction = _lbLastNavDelta < 0 ? -1 : 1;
  var candidates = [];
  var keep = {};
  function addCandidate(offset, tier) {
    var photo = _lightboxPhotoList[currentIdx + offset];
    if (!photo) return;
    if (Object.prototype.hasOwnProperty.call(photo, 'edit_recipe')) {
      _lbRememberEditRecipe(photo.id, photo.edit_recipe);
      window.vireoRememberPhotoRenderKey(photo.id, photo.render_key);
    }
    var url = _lbSrcUrl(photo.id, tier, true);
    var cacheKey = String(photo.id) + '|' + url;
    if (keep[cacheKey]) return;
    keep[cacheKey] = true;
    candidates.push({
      photoId: photo.id, sourceKey: tier, url: url, cacheKey: cacheKey,
      bytes: _lbEstimatePreloadBytes(photo, tier)
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
  Object.keys(_lbAdjacentPreloads).forEach(function(cacheKey) {
    var entry = _lbAdjacentPreloads[cacheKey];
    // Retaining the just-displayed warmup makes an immediate reversal cheap.
    if (keep[cacheKey] || (entry.photoId === _lightboxCurrentId && entry.status === 'decoded')) return;
    _lbReleasePreloadImage(entry);
    delete _lbAdjacentPreloads[cacheKey];
  });
  Object.keys(_lbAdjacentPreloadRetry).forEach(function(cacheKey) {
    if (!keep[cacheKey]) delete _lbAdjacentPreloadRetry[cacheKey];
  });
  _lbTrimPreloadBudget();

  // Cache hits can transfer/decode concurrently. The server independently
  // admits just one speculative cache-miss producer; declined work backs off.
  // Count retired in-flight requests too: dropping an Image does not cancel
  // synchronous server generation or immediately reclaim its decode memory.
  var now = Date.now();
  var retryAt = null;
  candidates.forEach(function(candidate) {
    if (_lbSpeculativeLoads.size >= _lbPreloadConcurrency) return;
    if (_lbAdjacentPreloads[candidate.cacheKey]) return;
    // Once a miss has been declined, retry it alone after active transfers
    // finish. Parallel retries would simply collide with the same server slot.
    if (Array.from(_lbSpeculativeLoads).some(function(entry) { return entry.retrying; })) return;
    var retry = _lbAdjacentPreloadRetry[candidate.cacheKey];
    if (retry && retry.openSeq === _lbOpenSeq) {
      if (retry.count > 1) return;
      if (_lbSpeculativeLoads.size) return;
      if (retry.at > now) {
        retryAt = retryAt == null ? retry.at : Math.min(retryAt, retry.at);
        return;
      }
    }
    if (_lbPreloadBytes() + candidate.bytes > _lbPreloadBudgetBytes) return;
    var entry = {
      img: new Image(), photoId: candidate.photoId, sourceKey: candidate.sourceKey,
      url: candidate.url, status: 'loading', bytes: candidate.bytes, retrying: !!retry
    };
    _lbAdjacentPreloads[candidate.cacheKey] = entry;
    _lbRunSpeculativePreload(entry, function(decoded) {
      if (_lbAdjacentPreloads[candidate.cacheKey] !== entry) return;
      if (!decoded) {
        delete _lbAdjacentPreloads[candidate.cacheKey];
        var retry = _lbAdjacentPreloadRetry[candidate.cacheKey];
        if (!retry || retry.openSeq !== _lbOpenSeq) retry = { openSeq: _lbOpenSeq, count: 0 };
        retry.count += 1;
        retry.at = Date.now() + 750;
        _lbAdjacentPreloadRetry[candidate.cacheKey] = retry;
      }
    });
  });
  if (retryAt != null) _lbScheduleAdjacentPhoto(key, retryAt - now);
  else if (!_lbSpeculativeLoads.size) _lbStartPendingOriginalPreload();
}

function _lbScheduleAdjacentPhoto(sourceKey, delay) {
  _lbCancelAdjacentPreloadTimer();
  var openSeq = _lbOpenSeq;
  _lbAdjacentPreloadTimer = setTimeout(function() {
    _lbAdjacentPreloadTimer = null;
    if (_lbOpenSeq !== openSeq || _lbVisualTransitionPending) return;
    _lbPrimeAdjacentPhotos(sourceKey);
  }, delay || 0);
}

function _lbScheduleSourceSwap(targetZoom, immediate) {
  if (!_lightboxCurrentId) return;
  // Was a sharper tier already requested for the displayed bitmap? Read this
  // before retargeting: if the new target is the tier already on screen, that
  // request is being abandoned rather than fulfilled. _lbProgressiveTargetKey
  // deliberately does not count -- it is the initial pick being evaluated for
  // the first time, not a request in flight.
  var abandoningRequest = !!_lbDesiredSrcKey
    && _lbDesiredSrcKey !== _lbCurrentSrcKey
    && _lbSrcRank(_lbDesiredSrcKey) > _lbSrcRank(_lbCurrentSrcKey);
  var desired = _lbPickSourceKey(targetZoom);
  // Metadata may still be in flight when a warm preview first becomes visible.
  // Keep the initial conservative tier until geometry can refine it.
  if (_lbProgressiveTargetKey && !_lbNativeZoom) desired = _lbProgressiveTargetKey;
  _lbDesiredSrcKey = desired;
  if (_lbZoom > 1.001 && desired !== 'original') {
    _lbCancelOriginalPreload();
  }
  if (desired === _lbCurrentSrcKey) {
    _lbProgressiveTargetKey = null;
    // Zooming back to a level the current tier already satisfies cancels the
    // sharper request. Nothing loaded, so go quiet rather than claim the pixels
    // the user was waiting on arrived.
    if (!abandoningRequest) _lbMarkDetailSettled();
    _lbSetPreviewLoading(false);
    _lbScheduleAdjacentPhoto(desired);
    return;
  }

  _lbSetPreviewLoading(_lbSrcRank(desired) > _lbSrcRank(_lbCurrentSrcKey) || !!_lbProgressiveTargetKey);
  if (_lbSwapTimer) clearTimeout(_lbSwapTimer);
  _lbSwapTimer = setTimeout(function() {
    _lbSwapTimer = null;
    if (_lbDesiredSrcKey !== desired) return;          // newer request won
    if (_lbDesiredSrcKey === _lbCurrentSrcKey) return; // already there
    var photoId = _lightboxCurrentId;
    var swapOpenSeq = _lbOpenSeq;
    var key = _lbDesiredSrcKey;
    var url = _lbSrcUrl(photoId, key);

    var preloader = new Image();
    preloader.onload = function() {
      // Ignore stale: different photo, or desired key changed to something else
      if (_lightboxCurrentId !== photoId) return;
      if (_lbOpenSeq !== swapOpenSeq) return;
      if (_lbDesiredSrcKey !== key) return;
      var img = document.getElementById('lightboxImg');
      if (!img) return;
      // After the new source loads, recompute nativeZoom — the aspect ratio is
      // nominally identical across source tiers but subpixel rounding can shift
      // fit dimensions slightly, and if API dims were missing the original's
      // naturalWidth is now authoritative.
      img.addEventListener('load', function onSwapLoad() {
        img.removeEventListener('load', onSwapLoad);
        if (_lightboxCurrentId !== photoId) return;
        if (_lbOpenSeq !== swapOpenSeq) return;
        // A newer swap may have preempted us before this load fired — in that
        // case img.naturalWidth belongs to a DIFFERENT source than our closure's
        // `key`, so bail rather than record wrong dims or recalibrate stale state.
        if (_lbCurrentSrcKey !== key) return;
        _lbProgressiveTargetKey = null;
        _lbMarkDetailSettled();
        _lbSetPreviewLoading(false);
        // Keep the decoded background original alive until the visible image
        // has adopted it; releasing it earlier can make some WebViews decode
        // the same full-resolution bytes a second time on the first 100% click.
        if (key === 'original') _lbCancelOriginalPreload();
        if (!_lbPhotoW && key === 'original' && img.naturalWidth) {
          _lbPhotoW = img.naturalWidth;
          _lbPhotoH = img.naturalHeight;
        }
        _lbRecomputeNativeZoom();
        // If the user pressed z/click when nativeZoom was unknown, upgrade to
        // true 1:1 now. Route through the shared helper so the stored anchor is
        // honored — completing with a recentered zoom would make a deferred 1:1
        // jump away from the point the user clicked.
        if (!_lbTryApplyPendingViewportState()) _lbApplyPendingOneToOneZoom();
        // Track Eye alignment is deferred while _lbPending1To1 is armed so it
        // cannot cancel the deferred sharp-source fallback. Re-apply now that
        // the source has landed and any pending 1:1 has snapped.
        _lbTryApplyPendingEyeTrack();
        _lbApplyTransform();
        _lbScheduleAdjacentPhoto(key);
      });
      img.src = url;
      _lbCurrentSrcKey = key;
    };
    preloader.onerror = function() {
      if (_lightboxCurrentId !== photoId) return;
      if (_lbOpenSeq !== swapOpenSeq) return;
      if (_lbDesiredSrcKey !== key) return;
      _lbProgressiveTargetKey = null;
      // Record the failure BEFORE clearing the loading flag. The chip's
      // sharpening state is derived from the desired/current tiers, so a tier
      // that is never going to arrive has to stop counting as in flight first
      // -- clearing the flag while _lbDesiredSrcKey still names the failed tier
      // leaves the spinner up with no request behind it.
      if (key === 'original') {
        var had1To1Pending = _lbPending1To1;
        _lbOriginalUnavailable = true;
        _lbSetPreviewLoading(false);
        _lbRecomputeNativeZoom();
        if (had1To1Pending) {
          _lbDeferPendingOneToOneToPreviewFallback();
        } else {
          _lbApplyPendingOneToOneZoom();
          _lbApplyTransform();
          // /original is gone, so re-pick the best remaining tier rather than
          // staying stuck on the lower current source.
          _lbScheduleSourceSwap();
        }
      } else {
        // The failed request is no longer pending. Keep the usable bitmap
        // and resume its neighbors without automatically retrying this tier.
        _lbDesiredSrcKey = _lbCurrentSrcKey;
        _lbSetPreviewLoading(false);
        if (_lbPending1To1) {
          _lbPending1To1 = false;
          _lbPending1To1Anchor = null;
          _lbPendingViewportState = null;
          _lbUpdateZoomControl();
          _lbSaveViewportState(photoId);
        }
        _lbScheduleAdjacentPhoto(_lbCurrentSrcKey);
      }
      // Keep current source on failure
    };
    preloader.src = url;
  }, immediate ? 0 : 150);
}

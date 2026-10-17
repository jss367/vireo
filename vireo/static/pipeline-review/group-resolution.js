// Group Review image resolution and loupe zoom levels.
// Classic page script; shared globals are initialized before boot.js runs.

// GRM resolution slider — index into GRM_RES_STOPS. Persists across photos within a session.
var GRM_RES_STOPS = [
  {size: 1920, label: '1920', kind: 'preview'},
  {size: 2560, label: '2560', kind: 'preview'},
  {size: 3840, label: '4K',   kind: 'preview'},
  {size: 0,    label: 'Original', kind: 'original'}
];
// Start with bounded previews so opening a burst does not decode an original
// for every comparison tile. The resolution slider and 1:1 action still
// provide originals when the user needs to inspect fine detail.
var grmResolutionIdx = 1;

/** @param {number} photoId */
function grmPhotoUrl(photoId) {
  var stop = GRM_RES_STOPS[grmResolutionIdx];
  if (requireReviewValue(stop).kind === 'original') return '/photos/' + photoId + '/original';
  return '/photos/' + photoId + '/preview?size=' + requireReviewValue(stop).size;
}

function grmUpdateResLabel() {
  var el = /** @type {HTMLElement} */ (document.getElementById('grmResLabel'));
  if (!el) return;
  var stop = GRM_RES_STOPS[grmResolutionIdx];
  var dims = '';
  var mpStr = '';
  var photo = grmState && grmState.selected
    ? grmState.items.find(function(p) { return p.id === grmState.selected; })
    : (grmState && grmState.items[0]);
  if (photo && photo.width && photo.height) {
    if (requireReviewValue(stop).kind === 'original') {
      dims = photo.width + '×' + photo.height;
      // Show megapixels alongside the dimensions so users can tell at a
      // glance why two shots in the same library zoom to different
      // "Original" sizes — e.g. Nikon DX-crop ~19 MP vs FX ~45 MP.
      var mp = (photo.width * photo.height) / 1e6;
      mpStr = (mp >= 10 ? mp.toFixed(0) : mp.toFixed(1)) + ' MP';
    } else {
      var longest = Math.max(photo.width, photo.height);
      var scale = longest > requireReviewValue(stop).size ? requireReviewValue(stop).size / longest : 1;
      var w = Math.round(photo.width * scale);
      var h = Math.round(photo.height * scale);
      dims = w + '×' + h;
    }
  }
  var prefix = dims ? dims : requireReviewValue(stop).label;
  el.textContent = prefix + ' · ' + (mpStr ? mpStr + ' · ' : '') + requireReviewValue(stop).kind;
}

/** @param {number} idx */
function grmSetResolution(idx) {
  grmResolutionIdx = Number(idx) || 0;
  var wasOneToOne = _grmLoupeOneToOne || _grmLoupePendingOneToOne;
  _grmLoupePendingOneToOne = false;
  _grmLoupeOneToOne = false;
  if (wasOneToOne) {
    // Resolution change invalidates the prior 1:1 calculation; mirror the
    // toggle-off path so the zoom label stops showing "1:1".
    _grmSetLoupeZoomLevel(1);
  }
  // Update card image sources AND natural-size layout in place. Preserving
  // the zoom transforms keeps the loupe locked on the current region while
  // the slider drags. Each card's <img> is laid out at the new served
  // resolution so the rasterized layer matches what's actually being served.
  /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('#grmOverlay .grm-card')).forEach(function(card) {
    var pid = Number(card.getAttribute('data-photo-id'));
    if (!pid) return;
    var img = /** @type {HTMLImageElement} */ (card.querySelector('img'));
    if (!img) return;
    var photo = grmState && grmState.items
      ? grmState.items.find(function(p) { return p.id === pid; })
      : null;
    var nat = grmNaturalDims(photo);
    card.dataset.natW = String(nat.w);
    card.dataset.natH = String(nat.h);
    img.style.width = nat.w + 'px';
    img.style.height = nat.h + 'px';
    img.src = grmPhotoUrl(pid);
  });
  if (grmState && grmState.selected) {
    var loupe = /** @type {HTMLImageElement} */ (document.getElementById('grmLoupePhoto'));
    if (loupe) {
      loupe.src = grmPhotoUrl(grmState.selected);
      grmApplyLoupeZoom();
    }
  }
  // Re-apply card transforms so cover-fit snaps to the new natural size.
  grmApplyCardTransforms();
  grmUpdateResLabel();
}

/** @param {number | string} value */
function grmSetLoupeZoom(value) {
  var pct = Number(value);
  if (!pct) pct = 100;
  pct = Math.max(100, Math.min(_grmLoupeSliderMax(), pct));
  _grmLoupePendingOneToOne = false;
  _grmLoupeOneToOne = false;
  _grmLoupeZoomLevel = pct / 100;
  var slider = /** @type {HTMLInputElement} */ (document.getElementById('grmLoupeZoomSlider'));
  if (slider) slider.value = String(pct);
  var label = /** @type {HTMLElement} */ (document.getElementById('grmLoupeZoomVal'));
  if (label) label.textContent = _grmLoupeZoomLevel.toFixed(_grmLoupeZoomLevel >= 2 ? 1 : 2).replace(/\.0$/, '') + '×';
  grmApplyLoupeZoom();
}

function grmApplyLoupeZoom() {
  var img = /** @type {HTMLImageElement} */ (document.getElementById('grmLoupePhoto'));
  if (!img) return;
  var x = _grmLastHoverX == null ? 50 : _grmLastHoverX;
  var y = _grmLastHoverY == null ? 50 : _grmLastHoverY;
  img.style.transformOrigin = x + '% ' + y + '%';
  img.style.transform = 'scale(' + _grmLoupeZoomLevel + ')';
  grmUpdateSelectedEyeCrosshair();
}

function _grmAfterLoupeSourceChange() {
  if (_grmLoupeOneToOne || _grmLoupePendingOneToOne) {
    _grmLoupeOneToOne = false;
    _grmLoupePendingOneToOne = true;
    _grmFinishLoupeOneToOne();
  } else {
    grmApplyLoupeZoom();
  }
}

function _grmLoupeSliderMax() {
  var slider = /** @type {HTMLInputElement} */ (document.getElementById('grmLoupeZoomSlider'));
  var fromDom = slider ? parseInt(slider.max, 10) : 500;
  return Math.max(500, fromDom || 500);
}

/** @param {HTMLImageElement} img */
function _grmLoupeFitScale(img) {
  var box = /** @type {HTMLElement} */ (document.getElementById('grmLoupeImg'));
  if (!box || !img || !img.naturalWidth || !img.naturalHeight) return null;
  var rect = box.getBoundingClientRect();
  if (!rect.width || !rect.height) return null;
  return Math.min(rect.width / img.naturalWidth, rect.height / img.naturalHeight);
}

/** @param {HTMLImageElement} img */
function _grmLoupeNativeZoom(img) {
  var fitScale = _grmLoupeFitScale(img);
  if (!fitScale) return null;
  return Math.max(1, 1 / (fitScale * (window.devicePixelRatio || 1)));
}

/** @param {number} zoom @param {string} [labelText] */
function _grmSetLoupeZoomLevel(zoom, labelText) {
  _grmLoupeZoomLevel = Math.max(1, zoom || 1);
  var pct = Math.round(_grmLoupeZoomLevel * 100);
  var slider = /** @type {HTMLInputElement} */ (document.getElementById('grmLoupeZoomSlider'));
  if (slider) {
    slider.max = String(Math.max(500, pct));
    slider.value = String(pct);
  }
  var label = /** @type {HTMLElement} */ (document.getElementById('grmLoupeZoomVal'));
  if (label) {
    label.textContent = labelText || _grmLoupeZoomLevel.toFixed(_grmLoupeZoomLevel >= 2 ? 1 : 2).replace(/\.0$/, '') + '×';
  }
  grmApplyLoupeZoom();
}

function _grmSetOriginalResolutionForLoupe() {
  var lastIdx = GRM_RES_STOPS.length - 1;
  if ((GRM_RES_STOPS[grmResolutionIdx] || {}).kind === 'original') return;
  grmResolutionIdx = lastIdx;
  var slider = /** @type {HTMLInputElement} */ (document.getElementById('grmResSlider'));
  if (slider) slider.value = String(grmResolutionIdx);
  /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('#grmOverlay .grm-card')).forEach(function(card) {
    var pid = Number(card.getAttribute('data-photo-id'));
    if (!pid) return;
    var img = /** @type {HTMLImageElement} */ (card.querySelector('img'));
    if (!img) return;
    var photo = grmState && grmState.items
      ? grmState.items.find(function(p) { return p.id === pid; })
      : null;
    var nat = grmNaturalDims(photo);
    card.dataset.natW = String(nat.w);
    card.dataset.natH = String(nat.h);
    img.style.width = nat.w + 'px';
    img.style.height = nat.h + 'px';
    img.src = grmPhotoUrl(pid);
  });
  if (grmState && grmState.selected) {
    var loupe = /** @type {HTMLImageElement} */ (document.getElementById('grmLoupePhoto'));
    if (loupe) loupe.src = grmPhotoUrl(grmState.selected);
  }
  grmApplyCardTransforms();
  grmUpdateResLabel();
}

/** @param {HTMLImageElement} img */
function grmLoupeImgLoaded(img) {
  grmUpdateSelectedEyeCrosshair();
  if (_grmLoupePendingOneToOne) {
    _grmFinishLoupeOneToOne();
  }
}

function grmToggleLoupeOneToOne() {
  if (_grmLoupeOneToOne || _grmLoupePendingOneToOne) {
    _grmLoupePendingOneToOne = false;
    _grmLoupeOneToOne = false;
    _grmSetLoupeZoomLevel(1);
    return;
  }
  _grmLoupePendingOneToOne = true;
  _grmSetOriginalResolutionForLoupe();
  _grmFinishLoupeOneToOne();
}

function _grmFinishLoupeOneToOne() {
  var img = /** @type {HTMLImageElement} */ (document.getElementById('grmLoupePhoto'));
  if (!img || !img.complete || !img.naturalWidth || !img.naturalHeight) return;
  var zoom = _grmLoupeNativeZoom(img);
  if (zoom == null) return;
  _grmLoupePendingOneToOne = false;
  _grmLoupeOneToOne = true;
  _grmSetLoupeZoomLevel(zoom, '1:1');
}

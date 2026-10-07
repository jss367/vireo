// Group Review view state, thumbnail size, image resolution, and loupe zoom and 1:1.
// Classic page script; load boot.js after all definitions.

// Multiplier above each card's cover-fit scale. 1 = cover-fit (no zoom);
// values past 1 zoom in towards CSS 1:1 and beyond to device 1:1 on Retina.
var _grmZoomLevel = 1;
// Card thumbnails default to 180px wide x 120px tall (3:2). The image inside
// is laid out at the served photo's natural pixel dimensions; transform-scale
// shrinks it to fit the current viewport.
var GRM_CARD_W = 180;
var GRM_CARD_H = 120;
var _grmLoupeLocked = false;
// Per-photo pan offsets, in pre-scale CSS pixels (i.e. image-coordinate
// pixels). Because we apply `translate(tx, ty) scale(s)` with offsets baked
// into the translate as `off * s`, pans stay locked to the same pixel of
// the underlying image as the shared zoom changes.
var _grmOffsets = {};
var _grmDragging = null;
var _grmLoupeAlignDragging = null;
var _grmSuppressNextClick = false;
var _grmSuppressLoupeClick = false;
// Latest cursor position over the loupe image (in %), used as the transform
// origin when zoom is applied outside a mouse event (e.g. wheel after drag,
// 1:1 snap, drag-induced re-apply). Initialized to centre so pre-hover zoom
// actions still render sensibly.
var _grmLoupeLastX = 50;
var _grmLoupeLastY = 50;
// Whether the loupe has received a real hover (vs. just the centre default).
// Used to decide between cover-fit (no-hover) and zoom (hover) per card.
var _grmLoupeHovering = false;
// True when grmSnapOneToOne is awaiting the selected card's natural dims
// (the original image is still loading and may be EXIF-rotated relative to
// stored item.width/height). grmCardImgLoaded finishes the snap once it
// reconciles dims so the 1:1 zoom is computed against the axes the browser
// actually paints.
var _grmPendingSnap = false;
var GRM_THUMB_STORAGE_KEY = 'vireo.burstReviewThumbSize';
var GRM_RES_STOPS = [
  { size: 400,  label: '400',      kind: 'thumb' },
  { size: 1920, label: '1920',     kind: 'preview' },
  { size: 3840, label: '4K',       kind: 'preview' },
  { size: 0,    label: 'Original', kind: 'original' }
];
var grmResolutionIdx = 1;
var _grmLoupeZoomLevel = 1;
var _grmLoupeOneToOne = false;
var _grmLoupePendingOneToOne = false;

function _grmInitialThumbSize() {
  try {
    var stored = parseInt(window.localStorage.getItem(GRM_THUMB_STORAGE_KEY), 10);
    if (stored) return Math.max(100, Math.min(320, stored));
  } catch(e) {}
  return 180;
}

function grmSetThumbSize(value, persist) {
  var size = parseInt(value, 10);
  if (!size) size = 180;
  size = Math.max(100, Math.min(320, size));
  GRM_CARD_W = size;
  GRM_CARD_H = Math.round(size * 2 / 3);
  var overlay = document.getElementById('grmOverlay');
  if (overlay) {
    overlay.style.setProperty('--grm-card-w', GRM_CARD_W + 'px');
    overlay.style.setProperty('--grm-card-h', GRM_CARD_H + 'px');
  }
  var slider = document.getElementById('grmThumbSizeSlider');
  if (slider) slider.value = GRM_CARD_W;
  var label = document.getElementById('grmThumbSizeVal');
  if (label) label.textContent = GRM_CARD_W + 'px';
  if (persist !== false) {
    try { window.localStorage.setItem(GRM_THUMB_STORAGE_KEY, String(GRM_CARD_W)); } catch(e) {}
  }
  _grmApplyAllCardTransforms(_grmLoupeLastX, _grmLoupeLastY);
}

GRM_CARD_W = _grmInitialThumbSize();
GRM_CARD_H = Math.round(GRM_CARD_W * 2 / 3);

function grmPhotoUrl(photoOrId) {
  var photo = photoOrId && typeof photoOrId === 'object' ? photoOrId : null;
  var photoId = photo ? (photo.photo_id != null ? photo.photo_id : photo.id) : photoOrId;
  var stop = GRM_RES_STOPS[grmResolutionIdx] || GRM_RES_STOPS[1];
  if (stop.kind === 'thumb') {
    if (!photo && grmState && grmState.items) {
      photo = grmState.items.find(function(it) {
        return String(it.photo_id) === String(photoId);
      }) || null;
    }
    return window.vireoThumbnailUrl
      ? window.vireoThumbnailUrl(photo || photoId)
      : '/thumbnails/' + photoId + '.jpg';
  }
  if (stop.kind === 'original') return '/photos/' + photoId + '/original';
  return vireoPreviewUrl(photoId, stop.size);
}

function grmLoupePhotoUrl(photoId) {
  return grmPhotoUrl(photoId);
}

function grmEditedSourceDims(item) {
  var W = (item && item.width) ? Number(item.width) : 1800;
  var H = (item && item.height) ? Number(item.height) : 1200;
  var recipe = (item && item.edit_recipe) || {};
  var rotation = Number(recipe.rotation || 0);
  if (rotation === 90 || rotation === 270) {
    var tmp = W;
    W = H;
    H = tmp;
  }
  if (recipe.crop && typeof recipe.crop === 'object') {
    var cropW = Number(recipe.crop.w);
    var cropH = Number(recipe.crop.h);
    if (Number.isFinite(cropW) && cropW > 0) W = Math.max(1, Math.round(W * cropW));
    if (Number.isFinite(cropH) && cropH > 0) H = Math.max(1, Math.round(H * cropH));
  }
  return { w: W, h: H };
}

function grmNaturalDims(item) {
  var stop = GRM_RES_STOPS[grmResolutionIdx] || GRM_RES_STOPS[1];
  var dims = grmEditedSourceDims(item);
  var W = dims.w;
  var H = dims.h;
  if (stop.kind === 'original') {
    return { w: W, h: H };
  }
  var maxSide = stop.size || 400;
  var longest = Math.max(W, H);
  if (longest > maxSide) {
    var r = maxSide / longest;
    W = Math.round(W * r);
    H = Math.round(H * r);
  }
  return { w: W, h: H };
}

function grmUpdateResLabel() {
  var el = document.getElementById('grmResLabel');
  if (!el) return;
  var stop = GRM_RES_STOPS[grmResolutionIdx] || GRM_RES_STOPS[1];
  var item = grmState && grmState.selected
    ? grmState.items.find(function(it) { return it.photo_id === grmState.selected; })
    : (grmState && grmState.items[0]);
  var label = stop.label;
  if (item && item.width && item.height) {
    var dims = grmNaturalDims(item);
    label = dims.w + '×' + dims.h;
  }
  el.textContent = label + ' · ' + stop.kind;
}

function grmSetResolution(idx) {
  grmResolutionIdx = parseInt(idx, 10) || 0;
  grmState.upgraded = (GRM_RES_STOPS[grmResolutionIdx] || {}).kind === 'original';
  var wasOneToOne = _grmLoupeOneToOne || _grmLoupePendingOneToOne;
  _grmLoupePendingOneToOne = false;
  _grmLoupeOneToOne = false;
  if (wasOneToOne) {
    // Resolution change invalidates the prior 1:1 calculation; mirror the
    // toggle-off path so the zoom label stops showing "1:1".
    _grmSetLoupeZoomLevel(1);
  }
  var slider = document.getElementById('grmResSlider');
  if (slider) slider.value = grmResolutionIdx;
  document.querySelectorAll('.grm-card').forEach(function(card) {
    var pid = parseInt(card.dataset.photoId, 10);
    var img = card.querySelector('img');
    if (!pid || !img) return;
    var item = grmState.items.find(function(it) { return it.photo_id === pid; });
    var nat = grmNaturalDims(item);
    card.dataset.natW = nat.w;
    card.dataset.natH = nat.h;
    img.style.width = nat.w + 'px';
    img.style.height = nat.h + 'px';
    img.src = grmPhotoUrl(item || pid);
  });
  if (grmState && grmState.selected) {
    var loupe = document.getElementById('grmLoupePhoto');
    if (loupe) loupe.src = grmLoupePhotoUrl(grmState.selected);
  }
  _grmApplyAllCardTransforms(_grmLoupeLastX, _grmLoupeLastY);
  grmUpdateResLabel();
}

function grmSetLoupeZoom(value, persist) {
  var pct = parseInt(value, 10);
  if (!pct) pct = 100;
  pct = Math.max(100, Math.min(_grmLoupeSliderMax(), pct));
  _grmLoupePendingOneToOne = false;
  _grmLoupeOneToOne = false;
  _grmLoupeZoomLevel = pct / 100;
  var slider = document.getElementById('grmLoupeZoomSlider');
  if (slider) slider.value = pct;
  var label = document.getElementById('grmLoupeZoomVal');
  if (label) label.textContent = _grmLoupeZoomLevel.toFixed(_grmLoupeZoomLevel >= 2 ? 1 : 2).replace(/\.0$/, '') + '×';
  grmApplyLoupeZoom();
}

function grmApplyLoupeZoom() {
  var img = document.getElementById('grmLoupePhoto');
  if (!img) return;
  img.style.transformOrigin = _grmLoupeLastX + '% ' + _grmLoupeLastY + '%';
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
  var slider = document.getElementById('grmLoupeZoomSlider');
  var fromDom = slider ? parseInt(slider.max, 10) : 500;
  return Math.max(500, fromDom || 500);
}

function _grmLoupeFitScale(img) {
  var box = document.getElementById('grmLoupeImg');
  if (!box || !img || !img.naturalWidth || !img.naturalHeight) return null;
  var rect = box.getBoundingClientRect();
  if (!rect.width || !rect.height) return null;
  return Math.min(rect.width / img.naturalWidth, rect.height / img.naturalHeight);
}

function _grmLoupeNativeZoom(img) {
  var fitScale = _grmLoupeFitScale(img);
  if (!fitScale) return null;
  return Math.max(1, 1 / (fitScale * (window.devicePixelRatio || 1)));
}

function _grmSetLoupeZoomLevel(zoom, labelText) {
  _grmLoupeZoomLevel = Math.max(1, zoom || 1);
  var pct = Math.round(_grmLoupeZoomLevel * 100);
  var slider = document.getElementById('grmLoupeZoomSlider');
  if (slider) {
    slider.max = String(Math.max(500, pct));
    slider.value = String(pct);
  }
  var label = document.getElementById('grmLoupeZoomVal');
  if (label) {
    label.textContent = labelText || _grmLoupeZoomLevel.toFixed(_grmLoupeZoomLevel >= 2 ? 1 : 2).replace(/\.0$/, '') + '×';
  }
  grmApplyLoupeZoom();
}

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
  _grmUpgradeStripToOriginal();
  _grmFinishLoupeOneToOne();
}

function _grmFinishLoupeOneToOne() {
  var img = document.getElementById('grmLoupePhoto');
  if (!img || !img.complete || !img.naturalWidth || !img.naturalHeight) return;
  var zoom = _grmLoupeNativeZoom(img);
  if (zoom == null) return;
  _grmLoupePendingOneToOne = false;
  _grmLoupeOneToOne = true;
  _grmSetLoupeZoomLevel(zoom, '1:1');
}

function _grmUpgradeStripToOriginal() {
  // Swap every strip card from the 400px thumbnail to the full-resolution
  // original so zoom actually reveals sensor detail. Idempotent per modal open.
  // Also relayout each <img> at the original's natural pixel size so the
  // browser rasterizes the layer at full resolution instead of bilinearly
  // upsampling the 400px thumbnail bitmap (the WKWebView quality bug).
  if ((GRM_RES_STOPS[grmResolutionIdx] || {}).kind === 'original') return;
  grmResolutionIdx = GRM_RES_STOPS.length - 1;
  grmState.upgraded = true;
  var slider = document.getElementById('grmResSlider');
  if (slider) slider.value = grmResolutionIdx;
  document.querySelectorAll('.grm-card').forEach(function(card) {
    var pid = card.getAttribute('data-photo-id');
    var img = card.querySelector('img');
    if (!pid || !img) return;
    var item = grmState.items.find(function(it) { return String(it.photo_id) === String(pid); });
    var nat = grmNaturalDims(item);
    card.dataset.natW = nat.w;
    card.dataset.natH = nat.h;
    img.style.width = nat.w + 'px';
    img.style.height = nat.h + 'px';
    img.src = grmPhotoUrl(item || pid);
  });
  if (grmState && grmState.selected) {
    var loupe = document.getElementById('grmLoupePhoto');
    if (loupe) loupe.src = grmLoupePhotoUrl(grmState.selected);
  }
  _grmApplyAllCardTransforms(_grmLoupeLastX, _grmLoupeLastY);
  grmUpdateResLabel();
}

function grmCardImgLoaded(img) {
  // Reconcile declared natural dims against what the browser actually decoded
  // (NEFs may fall back to an embedded JPEG of a different size; EXIF-rotated
  // JPEGs render with swapped axes vs. our stored pre-orientation dims).
  if (!img || !img.naturalWidth || !img.naturalHeight) return;
  var card = img.closest('.grm-card');
  if (!card) return;
  var declaredW = parseFloat(card.dataset.natW) || 0;
  var declaredH = parseFloat(card.dataset.natH) || 0;
  if (img.naturalWidth !== declaredW || img.naturalHeight !== declaredH) {
    card.dataset.natW = img.naturalWidth;
    card.dataset.natH = img.naturalHeight;
    img.style.width = img.naturalWidth + 'px';
    img.style.height = img.naturalHeight + 'px';
    _grmApplyCardTransform(card);
  }
  // If the user pressed `1` before this card's natural dims were known,
  // finish the snap now that we have the axes the browser actually paints.
  // Without this, EXIF-rotated photos snap to a multiplier computed from
  // pre-orientation (swapped) dims and land at the wrong zoom level.
  if (_grmPendingSnap && grmState && grmState.selected != null
      && String(card.dataset.photoId) === String(grmState.selected)) {
    _grmPendingSnap = false;
    _grmFinishSnap();
  }
}

// Multiplier that, applied on top of cover-fit, yields a displayed scale of
// 1.0 (i.e. one source pixel = one CSS pixel) for the selected card. With
// the natural-size img layout, displayed scale = coverFit * multiplier, so
// CSS 1:1 is reached at multiplier = 1 / coverFit.
function _grmOneToOneMultiplier() {
  if (!grmState || !grmState.selected) return null;
  var card = document.querySelector('.grm-card[data-photo-id="' + grmState.selected + '"]');
  if (!card) return null;
  var coverFit = _grmCardCoverFit(card);
  if (!coverFit) return null;
  return 1 / coverFit;
}

function _grmMaxZoom() {
  // Allow zooming past CSS 1:1 by × DPR so the user can reach one source
  // pixel per device pixel on Retina displays — the actual pixel-peep target.
  // Falls back to 12 when dims are unknown so we never zoom less than before.
  var oneToOne = _grmOneToOneMultiplier();
  var dpr = window.devicePixelRatio || 1;
  if (oneToOne == null) return 12;
  return Math.max(12, Math.ceil(oneToOne * dpr));
}

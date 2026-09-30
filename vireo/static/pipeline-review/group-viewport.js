// Group Review thumbnail sizing, transforms, pan, and loupe interaction.
// Classic page script; shared globals are initialized before boot.js runs.

/* --- Loupe zoom/pan ---
 *
 * The resolution slider drives the zoom: each stop scales cards so one
 * source pixel maps to one display pixel (1:1) at that resolution.
 * Mouse wheel is a fine-tune multiplier on top.
 */

// Card thumbnails default to 180px wide × 120px tall (3:2). Used to compute
// the cover-fit ratio when deriving a 1:1 scale.
var GRM_CARD_W = 180;
var GRM_CARD_H = 120;
var GRM_THUMB_STORAGE_KEY = 'vireo.burstReviewThumbSize';

var _grmZoomMultiplier = 1;   // wheel fine-tune on top of the 1:1 base scale
var _grmLoupeZoomLevel = 1;
var _grmLoupeOneToOne = false;
var _grmLoupePendingOneToOne = false;
var _grmLoupeLocked = false;
var _grmLastHoverX = null;    // last cursor x% on the loupe (null = no hover yet)
var _grmLastHoverY = null;
// Per-photo pan offsets, in pre-scale CSS pixels. Because the transform is
// `scale(s) translate(tx, ty)`, the translate lives in the unscaled coordinate
// space — so offsets stay correct as the shared zoom changes between frames.
var _grmOffsets = {};
var _grmDragging = null;
var _grmLoupeAlignDragging = null;
var _grmSuppressNextClick = false;
var _grmSuppressLoupeClick = false;

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
  grmApplyCardTransforms();
}

// Compute the served image's natural dimensions for the current resolution
// slider stop. The browser will lay the <img> out at exactly these CSS
// pixels, so the transform-scale layer is rasterized at full source size
// instead of upscaled from a tiny tile (the WKWebView quality bug).
//
// Returns expected dims; on image load `grmCardImgLoaded` reconciles against
// the actual `naturalWidth/Height` (which can differ for NEFs that fall back
// to an embedded JPEG of a different size).
function grmNaturalDims(photo) {
  var stop = GRM_RES_STOPS[grmResolutionIdx];
  // Sensible defaults when photo dims are unknown — match the historical
  // 3:2 cover-fit so the card still looks right before metadata loads.
  var W = (photo && photo.width) ? photo.width : 1800;
  var H = (photo && photo.height) ? photo.height : 1200;
  if (stop.kind !== 'original') {
    var longest = Math.max(W, H);
    if (longest > stop.size) {
      var r = stop.size / longest;
      W = Math.round(W * r);
      H = Math.round(H * r);
    }
  }
  return { w: W, h: H };
}

// Cover-fit scale for one card: the smallest scale at which the image still
// covers the current thumbnail viewport. Matches the prior "no-zoom" visible state.
function grmCardCoverFit(card) {
  var W = parseFloat(card.dataset.natW) || GRM_CARD_W;
  var H = parseFloat(card.dataset.natH) || GRM_CARD_H;
  return Math.max(GRM_CARD_W / W, GRM_CARD_H / H);
}

function _grmCardDisplayedScale(card) {
  var coverFit = grmCardCoverFit(card);
  var hovering = _grmLastHoverX != null;
  return hovering ? Math.max(coverFit, _grmZoomMultiplier) : coverFit;
}

// Effective max for `_grmZoomMultiplier`. Since the <img> is laid out at
// natural source size, the multiplier IS the source-to-CSS pixel ratio:
// multiplier=1 → 1:1 source-to-CSS, multiplier=DPR → 1:1 source-to-DEVICE
// (true pixel-peep on a Retina display). Allow a little headroom past that.
function grmMaxZoomMultiplier() {
  var dpr = window.devicePixelRatio || 1;
  return Math.max(3, dpr * 2);
}

// Effective floor for `_grmZoomMultiplier`: the smallest coverFit across all
// currently-visible burst cards. The hover formula floors per-card at each
// card's own coverFit, so once the multiplier reaches the minimum coverFit
// every card is already at cover-fit and further wheel-down would be a
// dead no-op. A high-resolution photo can have a very small coverFit,
// well below the legacy 0.33 floor.
function grmMinZoomMultiplier() {
  var cards = document.querySelectorAll('#grmOverlay .grm-card');
  var minCover = Infinity;
  for (var i = 0; i < cards.length; i++) {
    var cf = grmCardCoverFit(cards[i]);
    if (cf > 0 && cf < minCover) minCover = cf;
  }
  return isFinite(minCover) ? minCover : 0.33;
}

// Compute the transform for one card. Returns the CSS transform string and
// flags whether the user is in the zoomed (hover) state.
function _grmComputeCardTransform(card) {
  var W = parseFloat(card.dataset.natW) || GRM_CARD_W;
  var H = parseFloat(card.dataset.natH) || GRM_CARD_H;
  var coverFit = Math.max(GRM_CARD_W / W, GRM_CARD_H / H);
  var hovering = _grmLastHoverX != null;
  // The <img> is laid out at natural source-pixel size, so transform `scale`
  // IS the source-to-CSS pixel ratio. Cover-fit when not hovering; multiplier
  // (1 = true 1:1) when hovering, floored at coverFit so a sub-1 multiplier
  // never reveals card background.
  var s = coverFit;
  if (hovering) {
    s = Math.max(coverFit, _grmZoomMultiplier);
  }
  // Hover-anchor: cursor at fractional position f of the loupe maps to the
  // same fractional position in the source image. Place pixel (f*W, f*H) of
  // the image at card pixel (f*180, f*120). Default (no hover) = centre.
  var hx = hovering ? (_grmLastHoverX / 100) : 0.5;
  var hy = hovering ? (_grmLastHoverY / 100) : 0.5;
  var baseTx = hx * (GRM_CARD_W - W * s);
  var baseTy = hy * (GRM_CARD_H - H * s);
  var off = _grmOffsets[card.dataset.photoId] || { tx: 0, ty: 0 };
  // User pan offsets are stored in pre-scale (image-coordinate) CSS pixels,
  // so they translate by `off * s` in card-coordinate pixels after the scale.
  var tx = baseTx + off.tx * s;
  var ty = baseTy + off.ty * s;
  return {
    transform: 'translate(' + tx + 'px, ' + ty + 'px) scale(' + s + ')',
    scale: s,
    tx: tx,
    ty: ty,
    w: W,
    h: H,
    zoomed: hovering,
  };
}

function grmApplyCardTransforms() {
  document.querySelectorAll('#grmOverlay .grm-card').forEach(function(card) {
    var img = card.querySelector('img');
    if (!img) return;
    var t = _grmComputeCardTransform(card);
    img.style.transform = t.transform;
    var box = img.parentElement;
    if (box) box.classList.toggle('zoomed', t.zoomed);
  });
  grmUpdateSelectedEyeCrosshair();
}

function _grmApplyCardTransform(card) {
  var img = card.querySelector('img');
  if (!img) return;
  var t = _grmComputeCardTransform(card);
  img.style.transform = t.transform;
  var box = img.parentElement;
  if (box) box.classList.toggle('zoomed', t.zoomed);
  if (grmState && String(card.dataset.photoId) === String(grmState.selected)) {
    grmUpdateSelectedEyeCrosshair();
  }
}

// `grmBaseScale()` retained for any external callers / tests. Reports the
// effective display scale for the currently selected card.
function grmBaseScale() {
  var card = grmState && grmState.selected
    ? document.querySelector('#grmOverlay .grm-card[data-photo-id="' + grmState.selected + '"]')
    : document.querySelector('#grmOverlay .grm-card');
  if (!card) return 1;
  return grmCardCoverFit(card);
}

// Reconcile an `<img>`'s declared size against the dimensions the browser
// actually decoded. Some NEFs fall back to an embedded JPEG that differs
// from the stored width/height; the natural-size layout must use the truth
// or the rasterized layer will be the wrong resolution.
function grmCardImgLoaded(img) {
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
}

function grmPositionCrosshair(x, y) {
  var ch = document.getElementById('grmCrosshairH');
  var cv = document.getElementById('grmCrosshairV');
  if (ch) ch.style.top = y + '%';
  if (cv) cv.style.left = x + '%';
}

function grmHideSelectedEyeCrosshair() {
  var marker = document.getElementById('grmSelectedEyeCrosshair');
  if (marker) marker.style.display = 'none';
}

function grmUpdateSelectedEyeCrosshair() {
  var marker = document.getElementById('grmSelectedEyeCrosshair');
  var box = document.getElementById('grmLoupeImg');
  var img = document.getElementById('grmLoupePhoto');
  if (!marker || !box || !img || !grmState || !grmState.selected) return;

  var photo = grmState.items.find(function(item) {
    return item.id === grmState.selected;
  });
  if (photo && _lbRecipeHasGeometricEdit(photo.edit_recipe)) {
    grmHideSelectedEyeCrosshair();
    return;
  }
  var eyeX = photo && Number(photo.eye_x);
  var eyeY = photo && Number(photo.eye_y);
  if (!photo || photo.eye_x == null || photo.eye_y == null ||
      !isFinite(eyeX) || !isFinite(eyeY) ||
      eyeX < 0 || eyeX > 1 || eyeY < 0 || eyeY > 1 ||
      !img.complete || !img.naturalWidth || !img.naturalHeight) {
    grmHideSelectedEyeCrosshair();
    return;
  }

  // The loupe image uses object-fit: contain. Place the detected-eye marker
  // inside the actual rendered photo, not in the surrounding letterbox. The
  // marker is then moved through the same zoom transform as the image (while
  // keeping its stroke a constant screen size). Include the selected photo's
  // comparison-strip pan so blue shows where that eye will land after the
  // adjustment while the yellow alignment target stays fixed.
  var boxW = box.clientWidth;
  var boxH = box.clientHeight;
  if (!boxW || !boxH) {
    grmHideSelectedEyeCrosshair();
    return;
  }
  var containScale = Math.min(
    boxW / img.naturalWidth,
    boxH / img.naturalHeight
  );
  var renderedW = img.naturalWidth * containScale;
  var renderedH = img.naturalHeight * containScale;
  var unzoomedX = (boxW - renderedW) / 2 + eyeX * renderedW;
  var unzoomedY = (boxH - renderedH) / 2 + eyeY * renderedH;
  var originX = ((_grmLastHoverX == null ? 50 : _grmLastHoverX) / 100) * boxW;
  var originY = ((_grmLastHoverY == null ? 50 : _grmLastHoverY) / 100) * boxH;
  var card = document.querySelector(
    '#grmOverlay .grm-card[data-photo-id="' + grmState.selected + '"]'
  );
  var offset = _grmOffsets[String(grmState.selected)] ||
    _grmOffsets[grmState.selected] || { tx: 0, ty: 0 };
  var cardScale = card ? _grmCardDisplayedScale(card) : 0;
  marker.style.left = (
    originX + (unzoomedX - originX) * _grmLoupeZoomLevel + offset.tx * cardScale
  ) + 'px';
  marker.style.top = (
    originY + (unzoomedY - originY) * _grmLoupeZoomLevel + offset.ty * cardScale
  ) + 'px';
  marker.style.display = 'block';
}

function grmLoupeToggleLock(e) {
  if (_grmSuppressLoupeClick) {
    _grmSuppressLoupeClick = false;
    return;
  }
  _grmLoupeLocked = !_grmLoupeLocked;
  var ch = document.getElementById('grmCrosshairH');
  var cv = document.getElementById('grmCrosshairV');
  if (_grmLoupeLocked) {
    ch.style.background = 'rgba(255, 180, 50, 0.7)';
    cv.style.background = 'rgba(255, 180, 50, 0.7)';
  } else {
    ch.style.background = '';
    cv.style.background = '';
    grmLoupeMove(e);
  }
}

var _grmHoverFrame = null;

function grmCancelHoverFrame() {
  if (_grmHoverFrame !== null) cancelAnimationFrame(_grmHoverFrame);
  _grmHoverFrame = null;
}

function grmLoupeMove(e) {
  if (_grmLoupeAlignDragging) return;
  if (_grmLoupeLocked) return;

  var rect = e.currentTarget.getBoundingClientRect();
  var x = ((e.clientX - rect.left) / rect.width) * 100;
  var y = ((e.clientY - rect.top) / rect.height) * 100;
  _grmLastHoverX = x;
  _grmLastHoverY = y;

  // Pointer events can outpace the display. Transform the whole comparison
  // strip once per frame, using the latest cursor position.
  if (_grmHoverFrame !== null) return;
  _grmHoverFrame = requestAnimationFrame(function() {
    _grmHoverFrame = null;
    grmPositionCrosshair(_grmLastHoverX, _grmLastHoverY);
    grmApplyLoupeZoom();
    grmApplyCardTransforms();
  });
}

function _grmSelectedOffsetTargets() {
  var ids = [];
  if (grmState && grmState.selectedIds && grmState.selectedIds.size) {
    ids = Array.from(grmState.selectedIds);
  } else if (grmState && grmState.selected) {
    ids = [grmState.selected];
  }
  var seen = {};
  return ids.map(function(pid) {
    var key = String(pid);
    if (seen[key]) return null;
    seen[key] = true;
    var targetCard = document.querySelector('#grmOverlay .grm-card[data-photo-id="' + key + '"]');
    if (!targetCard) return null;
    var cur = _grmOffsets[key] || _grmOffsets[pid] || { tx: 0, ty: 0 };
    return { card: targetCard, photoId: key, origTx: cur.tx, origTy: cur.ty };
  }).filter(Boolean);
}

function grmLoupeMouseDown(e) {
  if (e.button !== 0) return;
  var targets = _grmSelectedOffsetTargets();
  if (!targets.length) return;
  _grmLoupeAlignDragging = {
    targets: targets,
    startX: e.clientX,
    startY: e.clientY,
    moved: false,
  };
  document.addEventListener('mousemove', grmLoupeMouseMove);
  document.addEventListener('mouseup', grmLoupeMouseUp);
  e.preventDefault();
}

function grmLoupeMouseMove(e) {
  if (!_grmLoupeAlignDragging) return;
  if ((e.buttons & 1) === 0) {
    grmLoupeMouseUp(e);
    return;
  }
  var dx = e.clientX - _grmLoupeAlignDragging.startX;
  var dy = e.clientY - _grmLoupeAlignDragging.startY;
  if (!_grmLoupeAlignDragging.moved && (Math.abs(dx) > 3 || Math.abs(dy) > 3)) {
    _grmLoupeAlignDragging.moved = true;
    var loupe = document.getElementById('grmLoupeImg');
    if (loupe) loupe.classList.add('align-dragging');
    _grmLoupeAlignDragging.targets.forEach(function(target) {
      target.card.classList.add('dragging');
    });
  }
  if (!_grmLoupeAlignDragging.moved) return;
  _grmLoupeAlignDragging.targets.forEach(function(target) {
    var scale = _grmCardDisplayedScale(target.card);
    var z = scale > 0.01 ? scale : 1;
    var newTx = target.origTx + dx / z;
    var newTy = target.origTy + dy / z;
    _grmOffsets[target.photoId] = { tx: newTx, ty: newTy };
    _grmApplyCardTransform(target.card);
    _grmUpdateIndicator(target.card);
  });
  e.preventDefault();
}

function grmLoupeMouseUp(e) {
  if (!_grmLoupeAlignDragging) return;
  var moved = _grmLoupeAlignDragging.moved;
  _grmLoupeAlignDragging.targets.forEach(function(target) {
    target.card.classList.remove('dragging');
  });
  var loupe = document.getElementById('grmLoupeImg');
  if (loupe) loupe.classList.remove('align-dragging');
  document.removeEventListener('mousemove', grmLoupeMouseMove);
  document.removeEventListener('mouseup', grmLoupeMouseUp);
  _grmLoupeAlignDragging = null;
  if (moved) _grmSuppressLoupeClick = true;
  grmRefreshResetAllVisibility();
}

function grmLoupeZoom(e) {
  e.preventDefault();
  // Multiplier is the source-to-CSS pixel ratio: 1 = true 1:1, DPR = 1:1
  // source-to-DEVICE on Retina (the pixel-peep target). Floor at the
  // smallest visible coverFit so wheel-down can always return every card
  // to cover-fit even for high-resolution photos.
  var max = grmMaxZoomMultiplier();
  var min = grmMinZoomMultiplier();
  if (e.deltaY < 0) {
    _grmZoomMultiplier = Math.min(max, _grmZoomMultiplier * 1.25);
  } else {
    _grmZoomMultiplier = Math.max(min, _grmZoomMultiplier / 1.25);
  }
  grmApplyLoupeZoom();
  grmApplyCardTransforms();
}

function grmLoupeReset() {
  if (_grmLoupeLocked) return;
  if (_grmDragging) return;
  if (_grmLoupeAlignDragging) return;
  grmCancelHoverFrame();
  _grmLastHoverX = null;
  _grmLastHoverY = null;
  // Re-apply transforms in their default (non-hovered, cover-fit, centred)
  // state. We can't just clear `style.transform` because the natural-size
  // <img> needs a scale-down transform at all times to fit the thumbnail box.
  grmApplyCardTransforms();
}

/* --- Per-photo pan (drag a thumbnail to nudge it within its panel) --- */

function grmCardMouseDown(e) {
  if (e.button !== 0) return;
  var card = e.currentTarget;
  var photoId = card.dataset.photoId;
  var selectedIds = _grmEnsureSelectedIds();
  var numericPhotoId = parseInt(photoId, 10);
  var targetIds = selectedIds.has(numericPhotoId) || selectedIds.has(photoId)
    ? Array.from(selectedIds)
    : [photoId];
  var targets = targetIds.map(function(pid) {
    var targetCard = document.querySelector('#grmOverlay .grm-card[data-photo-id="' + pid + '"]');
    if (!targetCard) return null;
    var cur = _grmOffsets[pid] || { tx: 0, ty: 0 };
    return { card: targetCard, photoId: String(pid), origTx: cur.tx, origTy: cur.ty };
  }).filter(Boolean);
  if (!targets.length) {
    var cur = _grmOffsets[photoId] || { tx: 0, ty: 0 };
    targets = [{ card: card, photoId: String(photoId), origTx: cur.tx, origTy: cur.ty }];
  }
  _grmDragging = {
    card: card,
    photoId: photoId,
    targets: targets,
    startX: e.clientX,
    startY: e.clientY,
    moved: false,
  };
  document.addEventListener('mousemove', grmCardMouseMove);
  document.addEventListener('mouseup', grmCardMouseUp);
  e.preventDefault();
}

function grmCardMouseMove(e) {
  if (!_grmDragging) return;
  // If the primary button is no longer held (e.g. mouseup fired outside the
  // window and was missed), end the drag instead of continuing to mutate
  // offsets from passive cursor motion.
  if ((e.buttons & 1) === 0) {
    grmCardMouseUp(e);
    return;
  }
  var dx = e.clientX - _grmDragging.startX;
  var dy = e.clientY - _grmDragging.startY;
  if (!_grmDragging.moved && (Math.abs(dx) > 3 || Math.abs(dy) > 3)) {
    _grmDragging.moved = true;
    _grmDragging.targets.forEach(function(target) { target.card.classList.add('dragging'); });
  }
  if (!_grmDragging.moved) return;
  // Use each card's actual displayed scale so the same screen drag produces
  // the same visible pan on every selected photo, even across mixed sizes.
  _grmDragging.targets.forEach(function(target) {
    var scale = _grmCardDisplayedScale(target.card);
    var z = scale > 0.01 ? scale : 1;
    var newTx = target.origTx + dx / z;
    var newTy = target.origTy + dy / z;
    _grmOffsets[target.photoId] = { tx: newTx, ty: newTy };
    _grmApplyCardTransform(target.card);
    _grmUpdateIndicator(target.card);
  });
}

function grmCardMouseUp(e) {
  if (!_grmDragging) return;
  var moved = _grmDragging.moved;
  var card = _grmDragging.card;
  _grmDragging.targets.forEach(function(target) { target.card.classList.remove('dragging'); });
  document.removeEventListener('mousemove', grmCardMouseMove);
  document.removeEventListener('mouseup', grmCardMouseUp);
  _grmDragging = null;
  // Only suppress the trailing click when the release is inside the dragged
  // card — that's the only case where a `click` fires on the card and needs
  // swallowing. Off-card releases produce no card click, so the flag would
  // otherwise eat the user's next real selection.
  if (moved && e && e.target && card.contains(e.target)) {
    _grmSuppressNextClick = true;
  }
  grmRefreshResetAllVisibility();
}

function grmCardClick(e, photoId) {
  if (_grmSuppressNextClick) {
    _grmSuppressNextClick = false;
    return;
  }
  var mode = e && (e.metaKey || e.ctrlKey) ? 'toggle' : (e && e.shiftKey ? 'range' : 'single');
  grmSelect(photoId, mode);
}

function grmCardDblClick(e) {
  var card = e.currentTarget;
  var photoId = card.dataset.photoId;
  if (!_grmOffsets[photoId]) return;
  delete _grmOffsets[photoId];
  _grmUpdateIndicator(card);
  _grmApplyCardTransform(card);
  grmRefreshResetAllVisibility();
  e.stopPropagation();
}

function grmResetAllOffsets() {
  _grmOffsets = {};
  document.querySelectorAll('#grmOverlay .grm-card').forEach(function(card) {
    _grmUpdateIndicator(card);
    _grmApplyCardTransform(card);
  });
  grmRefreshResetAllVisibility();
}

function grmVisibleRegionForCard(card) {
  var t = _grmComputeCardTransform(card);
  var scale = t.scale || 1;
  var W = t.w || (parseFloat(card.dataset.natW) || GRM_CARD_W);
  var H = t.h || (parseFloat(card.dataset.natH) || GRM_CARD_H);
  var left = -t.tx / scale;
  var top = -t.ty / scale;
  var right = left + (GRM_CARD_W / scale);
  var bottom = top + (GRM_CARD_H / scale);
  var x = Math.max(0, left);
  var y = Math.max(0, top);
  right = Math.min(W, right);
  bottom = Math.min(H, bottom);
  var w = right - x;
  var h = bottom - y;
  return {
    photo_id: parseInt(card.dataset.photoId, 10),
    x: x,
    y: y,
    w: Math.max(1, w),
    h: Math.max(1, h),
    source_w: W,
    source_h: H,
  };
}

async function grmCalculateBoxSharpness() {
  var cards = Array.from(document.querySelectorAll('#grmOverlay .grm-card[data-photo-id]'));
  if (!cards.length) return;
  var btn = document.getElementById('grmBoxSharpnessBtn');
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Scoring...';
  }
  try {
    var data = await safeFetch('/api/photos/sharpness/regions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ regions: cards.map(grmVisibleRegionForCard) }),
    }, { toast: false });
    var byPhoto = {};
    (data.results || []).forEach(function(r) { byPhoto[r.photo_id] = r; });
    grmState.items.forEach(function(photo) {
      var r = byPhoto[photo.id];
      photo.box_sharpness = (r && r.sharpness != null) ? r.sharpness : null;
    });
    renderGroupModal();
    grmRefreshSelectedLoupe();
  } catch(e) {
    console.error('Box sharpness failed:', e);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = 'Score box sharpness';
    }
  }
}

function _grmUpdateIndicator(card) {
  var off = _grmOffsets[card.dataset.photoId];
  if (off && (off.tx !== 0 || off.ty !== 0)) {
    card.classList.add('has-offset');
  } else {
    card.classList.remove('has-offset');
  }
}

function grmRefreshResetAllVisibility() {
  var hasAny = false;
  for (var k in _grmOffsets) {
    var o = _grmOffsets[k];
    if (o && (o.tx !== 0 || o.ty !== 0)) { hasAny = true; break; }
  }
  var btn = document.getElementById('grmResetAllOffsets');
  if (btn) btn.classList.toggle('visible', hasAny);
}

function restoreGroupReviewThumbSize() {
    GRM_CARD_W = _grmInitialThumbSize();
    GRM_CARD_H = Math.round(GRM_CARD_W * 2 / 3);
}

function bindGroupReviewResize() {
    window.addEventListener('resize', grmUpdateSelectedEyeCrosshair);
}

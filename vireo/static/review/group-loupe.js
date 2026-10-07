// The Group Review loupe: lock, eye crosshair, hover zoom, align drags, card transforms, and strip 1:1.
// Classic page script; load boot.js after all definitions.

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

function grmHideSelectedEyeCrosshair() {
  var marker = document.getElementById('grmSelectedEyeCrosshair');
  if (marker) marker.style.display = 'none';
}

function grmUpdateSelectedEyeCrosshair() {
  var marker = document.getElementById('grmSelectedEyeCrosshair');
  var box = document.getElementById('grmLoupeImg');
  var img = document.getElementById('grmLoupePhoto');
  if (!marker || !box || !img || !grmState || !grmState.selected) return;

  var item = grmState.items.find(function(entry) {
    return entry.photo_id === grmState.selected;
  });
  if (item && _lbRecipeHasGeometricEdit(item.edit_recipe)) {
    grmHideSelectedEyeCrosshair();
    return;
  }
  var eyeX = item && Number(item.eye_x);
  var eyeY = item && Number(item.eye_y);
  if (!item || item.eye_x == null || item.eye_y == null ||
      !isFinite(eyeX) || !isFinite(eyeY) ||
      eyeX < 0 || eyeX > 1 || eyeY < 0 || eyeY > 1 ||
      !img.complete || !img.naturalWidth || !img.naturalHeight) {
    grmHideSelectedEyeCrosshair();
    return;
  }

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
  var originX = (_grmLoupeLastX / 100) * boxW;
  var originY = (_grmLoupeLastY / 100) * boxH;
  var card = document.querySelector(
    '.grm-card[data-photo-id="' + grmState.selected + '"]'
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


function grmLoupeMove(e) {
  if (_grmLoupeAlignDragging) return;
  if (_grmLoupeLocked) return;

  var rect = e.currentTarget.getBoundingClientRect();
  var x = ((e.clientX - rect.left) / rect.width) * 100;
  var y = ((e.clientY - rect.top) / rect.height) * 100;
  _grmLoupeLastX = x;
  _grmLoupeLastY = y;
  _grmLoupeHovering = true;

  // Update crosshairs
  document.getElementById('grmCrosshairH').style.top = y + '%';
  document.getElementById('grmCrosshairV').style.left = x + '%';
  grmApplyLoupeZoom();

  // No upgrade here: plain hover shouldn't trigger a burst of full-res fetches
  // for every strip card. Upgrade only when the user expresses zoom intent via
  // the wheel or the 1:1 snap key.
  _grmApplyAllCardTransforms(x, y);
}

function _grmCardDisplayedScale(card) {
  var coverFit = _grmCardCoverFit(card);
  var hovering = _grmLoupeHovering || _grmLoupeLocked;
  return hovering ? Math.max(coverFit, coverFit * _grmZoomLevel) : coverFit;
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
    var targetCard = document.querySelector('.grm-card[data-photo-id="' + key + '"]');
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
    var z = scale > 0.001 ? scale : 1;
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

function _grmCardCoverFit(card) {
  var W = parseFloat(card.dataset.natW) || GRM_CARD_W;
  var H = parseFloat(card.dataset.natH) || GRM_CARD_H;
  return Math.max(GRM_CARD_W / W, GRM_CARD_H / H);
}

function _grmComputeCardTransform(card, hoverX, hoverY, hovering) {
  var W = parseFloat(card.dataset.natW) || GRM_CARD_W;
  var H = parseFloat(card.dataset.natH) || GRM_CARD_H;
  var coverFit = Math.max(GRM_CARD_W / W, GRM_CARD_H / H);
  var s = coverFit;
  if (hovering) {
    s = Math.max(coverFit, coverFit * _grmZoomLevel);
  }
  var hx = hovering ? (hoverX / 100) : 0.5;
  var hy = hovering ? (hoverY / 100) : 0.5;
  var baseTx = hx * (GRM_CARD_W - W * s);
  var baseTy = hy * (GRM_CARD_H - H * s);
  var off = _grmOffsets[card.dataset.photoId] || { tx: 0, ty: 0 };
  // Pan offsets are in pre-scale (image-coordinate) CSS pixels; convert to
  // card-coordinate pixels by multiplying by the displayed scale.
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

function _grmApplyAllCardTransforms(x, y) {
  var hovering = _grmLoupeHovering || _grmLoupeLocked;
  document.querySelectorAll('.grm-card').forEach(function(card) {
    var img = card.querySelector('img');
    if (!img) return;
    var t = _grmComputeCardTransform(card, x, y, hovering);
    img.style.transform = t.transform;
    if (hovering) card.classList.add('zoomed');
    else card.classList.remove('zoomed');
  });
  grmUpdateSelectedEyeCrosshair();
}

function _grmApplyCardTransform(card) {
  var img = card.querySelector('img');
  if (!img) return;
  var hovering = card.classList.contains('zoomed');
  var t = _grmComputeCardTransform(card, _grmLoupeLastX, _grmLoupeLastY, hovering);
  img.style.transform = t.transform;
  if (grmState && String(card.dataset.photoId) === String(grmState.selected)) {
    grmUpdateSelectedEyeCrosshair();
  }
}

function grmLoupeZoom(e) {
  e.preventDefault();
  // Wheel input is an explicit override of any pending 1:1 snap.
  _grmPendingSnap = false;
  var max = _grmMaxZoom();
  // Multiplicative zoom: each tick scales by 1.25, so reaching 30x (a
  // typical CSS 1:1 multiplier for a 5400px image) takes ~16 ticks.
  if (e.deltaY < 0) {
    _grmZoomLevel = Math.min(max, _grmZoomLevel * 1.25);
  } else {
    _grmZoomLevel = Math.max(1, _grmZoomLevel / 1.25);
  }
  _grmUpgradeStripToOriginal();
  _grmLoupeHovering = true;
  // Update origin from the wheel event even when locked so zoom follows cursor.
  var rect = e.currentTarget.getBoundingClientRect();
  _grmLoupeLastX = ((e.clientX - rect.left) / rect.width) * 100;
  _grmLoupeLastY = ((e.clientY - rect.top) / rect.height) * 100;
  document.getElementById('grmCrosshairH').style.top = _grmLoupeLastY + '%';
  document.getElementById('grmCrosshairV').style.left = _grmLoupeLastX + '%';
  grmApplyLoupeZoom();
  _grmApplyAllCardTransforms(_grmLoupeLastX, _grmLoupeLastY);
}

function grmSnapOneToOne() {
  // Lightroom-style 1:1: zoom factor that makes one source pixel == one CSS pixel.
  // Upgrade to original-resolution dims first so the multiplier is computed
  // against full natW/natH, not the 400px thumbnail layout.
  _grmUpgradeStripToOriginal();
  // For EXIF-rotated photos, item.width/height are pre-orientation while the
  // browser paints with swapped axes. Defer the multiplier until the original
  // <img> has loaded and grmCardImgLoaded has reconciled data-nat-w/h with
  // naturalWidth/naturalHeight, so the snap lands at true 1:1.
  var card = grmState.selected
    ? document.querySelector('.grm-card[data-photo-id="' + grmState.selected + '"]')
    : null;
  var img = card ? card.querySelector('img') : null;
  if (img && img.complete && img.naturalWidth && img.naturalHeight) {
    var dw = parseFloat(card.dataset.natW) || 0;
    var dh = parseFloat(card.dataset.natH) || 0;
    if (img.naturalWidth === dw && img.naturalHeight === dh) {
      _grmPendingSnap = false;
      _grmFinishSnap();
      return;
    }
  }
  _grmPendingSnap = true;
}

function _grmFinishSnap() {
  var mult = _grmOneToOneMultiplier();
  if (mult == null) return;
  _grmZoomLevel = Math.max(1, mult);
  _grmLoupeHovering = true;
  _grmApplyAllCardTransforms(_grmLoupeLastX, _grmLoupeLastY);
}

function grmLoupeReset() {
  if (_grmLoupeLocked) return;
  if (_grmDragging) return;
  if (_grmLoupeAlignDragging) return;
  // Re-apply transforms in their default (cover-fit, centred) state. We
  // can't just clear `style.transform` because the natural-size <img> needs
  // a scale-down transform at all times to fit the thumbnail viewport.
  _grmLoupeHovering = false;
  _grmApplyAllCardTransforms(_grmLoupeLastX, _grmLoupeLastY);
}

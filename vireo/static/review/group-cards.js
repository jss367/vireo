// Group Review card pointer handling: pan drags, selection clicks, offset resets, and box sharpness.
// Classic page script; load boot.js after all definitions.

function grmCardMouseDown(e) {
  if (e.button !== 0) return;
  var card = e.currentTarget;
  var photoId = card.dataset.photoId;
  var selectedIds = _grmEnsureSelectedIds();
  var targetIds = selectedIds.has(parseInt(photoId, 10)) || selectedIds.has(photoId)
    ? Array.from(selectedIds)
    : [photoId];
  var targets = targetIds.map(function(pid) {
    var targetCard = document.querySelector('.grm-card[data-photo-id="' + pid + '"]');
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
  // Convert screen-pixel drag delta to each image's pre-scale coordinate
  // space. That keeps the visible pan delta identical across selected cards
  // even when their displayed scales differ.
  _grmDragging.targets.forEach(function(target) {
    var s = _grmCardDisplayedScale(target.card);
    var z = s > 0.001 ? s : 1;
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
  // card — that's the only case where a `click` will fire on the card and
  // need to be swallowed. Off-card releases produce no card click, so
  // setting the flag would eat the user's next real selection.
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
  document.querySelectorAll('.grm-card').forEach(function(card) {
    _grmUpdateIndicator(card);
    _grmApplyCardTransform(card);
  });
  grmRefreshResetAllVisibility();
}

function grmVisibleRegionForCard(card) {
  var t = _grmComputeCardTransform(card, _grmLoupeLastX, _grmLoupeLastY, _grmLoupeHovering || _grmLoupeLocked);
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
    var payload = { regions: cards.map(grmVisibleRegionForCard) };
    var data = await safeFetch('/api/photos/sharpness/regions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    }, { toast: false });
    var byPhoto = {};
    (data.results || []).forEach(function(r) { byPhoto[r.photo_id] = r; });
    grmState.items.forEach(function(item) {
      var r = byPhoto[item.photo_id];
      item.box_sharpness = (r && r.sharpness != null) ? r.sharpness : null;
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

// Group Review zones and cards, selection, and the loupe's photo and predictions.
// Classic page script; load boot.js after all definitions.

function renderGroupModal() {
  var items = grmState.items.filter(function(it) { return !grmState.removed.has(it.id); });

  // Find AI best
  var bestId = null;
  var bestScore = -1;
  items.forEach(function(it) {
    if ((it.quality_score || 0) > bestScore) { bestScore = it.quality_score || 0; bestId = it.photo_id; }
  });

  function renderCard(it) {
    var isBest = it.photo_id === bestId;
    var isSelected = grmState.selectedIds && grmState.selectedIds.has(it.photo_id);
    var cls = 'grm-card' + (isSelected ? ' selected' : '') + (isBest ? ' ai-best' : '');
    var qScore = it.quality_score != null ? Math.round(it.quality_score * 100) : '-';
    var sharp = it.subject_sharpness != null ? Math.round(it.subject_sharpness) : (it.sharpness != null ? Math.round(it.sharpness) : '-');
    var boxSharp = it.box_sharpness != null ? Math.round(it.box_sharpness) : null;
    var confPct = Math.round((it.confidence || 0) * 100);

    var src = grmPhotoUrl(it);
    var nat = grmNaturalDims(it);
    var imgStyle = 'width:' + nat.w + 'px;height:' + nat.h + 'px;';
    var scoresHtml = 'Q: <span class="score-val">' + qScore + '</span> S: <span class="score-val">' + sharp + '</span>';
    if (boxSharp != null) scoresHtml += ' Box: <span class="score-val">' + boxSharp + '</span>';
    return '<div class="' + cls + '" data-photo-id="' + it.photo_id + '" data-pred-id="' + it.id + '"' +
      ' data-nat-w="' + nat.w + '" data-nat-h="' + nat.h + '"' +
      ' onmousedown="grmCardMouseDown(event)"' +
      ' onclick="grmCardClick(event, ' + it.photo_id + ')"' +
      ' ondblclick="grmCardDblClick(event)">' +
      '<div class="grm-card-img-box">' +
      '<img src="' + src + '" style="' + imgStyle + '"' +
        ' onload="grmCardImgLoaded(this)" loading="lazy" draggable="false">' +
      '</div>' +
      '<div class="grm-card-info">' +
        '<div class="grm-card-species">' + escapeHtml(it.species || '') + ' <span style="font-weight:400;color:var(--text-dim,#888);">' + confPct + '%</span></div>' +
        '<div class="grm-card-scores">' + scoresHtml + '</div>' +
        (isBest ? '<div class="grm-card-ai">AI BEST</div>' : '') +
      '</div>' +
    '</div>';
  }

  // Picks
  var pickItems = items.filter(function(it) { return grmState.picks.has(it.photo_id); });
  var candidateItems = items.filter(function(it) { return !grmState.picks.has(it.photo_id) && !grmState.rejects.has(it.photo_id); });
  var rejectItems = items.filter(function(it) { return grmState.rejects.has(it.photo_id); });

  document.getElementById('grmPicks').innerHTML = pickItems.map(renderCard).join('') || '<span style="font-size:12px;color:var(--text-ghost,#555);">Drag or press ↑ to add picks</span>';
  document.getElementById('grmCandidates').innerHTML = candidateItems.map(renderCard).join('') || '<span style="font-size:12px;color:var(--text-ghost,#555);">All sorted</span>';
  document.getElementById('grmRejects').innerHTML = rejectItems.map(renderCard).join('') || '<span style="font-size:12px;color:var(--text-ghost,#555);">Drag or press ↓ to reject</span>';

  // Species input — prefill with the group consensus. Matches the backend
  // consensus_prediction algorithm: pick the species with the highest sum of
  // per-frame confidence (== count × avg_confidence), tie-broken by vote count.
  if (!document.getElementById('grmSpecies').value && items.length > 0) {
    var votes = {};
    items.forEach(function(it) {
      if (!it.species) return;
      if (!votes[it.species]) votes[it.species] = { count: 0, confSum: 0 };
      votes[it.species].count += 1;
      votes[it.species].confSum += (it.confidence || 0);
    });
    var consensus = '';
    var bestConf = -1, bestCount = -1;
    Object.keys(votes).forEach(function(sp) {
      var v = votes[sp];
      if (v.confSum > bestConf || (v.confSum === bestConf && v.count > bestCount)) {
        bestConf = v.confSum;
        bestCount = v.count;
        consensus = sp;
      }
    });
    document.getElementById('grmSpecies').value = consensus;
  }

  document.getElementById('grmCount').textContent = pickItems.length + ' picks, ' + rejectItems.length + ' rejects, ' + candidateItems.length + ' unsorted';

  grmUpdateApplyLabel();

  // Re-apply offset indicators and the per-card transforms — cover-fit
  // always needs an explicit transform now that <img>s are laid out at
  // natural source-pixel size. innerHTML wipes inline styles each render.
  document.querySelectorAll('.grm-card').forEach(function(card) {
    _grmUpdateIndicator(card);
  });
  _grmApplyAllCardTransforms(_grmLoupeLastX, _grmLoupeLastY);
  grmRefreshResetAllVisibility();
}

function _grmVisibleItems() {
  return grmState.items.filter(function(it) { return !grmState.removed.has(it.id); });
}

function _grmEnsureSelectedIds() {
  if (!grmState.selectedIds) grmState.selectedIds = new Set();
  return grmState.selectedIds;
}

function grmSyncSelectionClasses() {
  var selectedIds = grmState && grmState.selectedIds ? grmState.selectedIds : new Set();
  document.querySelectorAll('#grmOverlay .grm-card[data-photo-id]').forEach(function(card) {
    var id = card.getAttribute('data-photo-id');
    card.classList.toggle(
      'selected',
      selectedIds.has(parseInt(id, 10)) || selectedIds.has(id)
    );
  });
}

function grmZonePlaceholder(text) {
  var span = document.createElement('span');
  span.style.cssText = 'font-size:12px;color:var(--text-ghost,#555);';
  span.textContent = text;
  return span;
}

function grmSyncZoneCards() {
  if (!grmState) return;
  var zones = { picks: [], candidates: [], rejects: [] };
  var visible = {};
  _grmVisibleItems().forEach(function(it) {
    var id = String(it.photo_id);
    visible[id] = true;
    if (grmState.picks.has(it.photo_id)) zones.picks.push(id);
    else if (grmState.rejects.has(it.photo_id)) zones.rejects.push(id);
    else zones.candidates.push(id);
  });

  var cardById = {};
  document.querySelectorAll('#grmOverlay .grm-card[data-photo-id]').forEach(function(card) {
    var id = card.getAttribute('data-photo-id');
    if (visible[id]) cardById[id] = card;
    else card.remove();
  });

  function syncStrip(stripId, ids, emptyText) {
    var strip = document.getElementById(stripId);
    if (!strip) return;
    Array.from(strip.children).forEach(function(child) {
      if (!child.classList || !child.classList.contains('grm-card')) child.remove();
    });
    ids.forEach(function(id) {
      var card = cardById[id];
      if (card) strip.appendChild(card);
    });
    if (!ids.length) strip.appendChild(grmZonePlaceholder(emptyText));
  }

  syncStrip('grmPicks', zones.picks, 'Drag or press ↑ to add picks');
  syncStrip('grmCandidates', zones.candidates, 'All sorted');
  syncStrip('grmRejects', zones.rejects, 'Drag or press ↓ to reject');

  document.getElementById('grmCount').textContent =
    zones.picks.length + ' picks, ' + zones.rejects.length + ' rejects, ' +
    zones.candidates.length + ' unsorted';
  grmSyncSelectionClasses();
  grmRefreshResetAllVisibility();
  grmUpdateApplyLabel();
}

function _grmSelectRange(anchorId, photoId) {
  var selectedIds = _grmEnsureSelectedIds();
  var items = _grmVisibleItems();
  var a = items.findIndex(function(it) { return it.photo_id === anchorId; });
  var b = items.findIndex(function(it) { return it.photo_id === photoId; });
  if (a < 0 || b < 0) {
    selectedIds.add(photoId);
    return;
  }
  var start = Math.min(a, b);
  var end = Math.max(a, b);
  for (var i = start; i <= end; i++) {
    selectedIds.add(items[i].photo_id);
  }
}

function grmSelect(photoId, mode) {
  var selectedIds = _grmEnsureSelectedIds();
  if (mode === 'toggle') {
    if (selectedIds.has(photoId)) {
      selectedIds.delete(photoId);
      if (grmState.selected === photoId) {
        var remaining = Array.from(selectedIds);
        grmState.selected = remaining.length ? remaining[remaining.length - 1] : null;
      }
    } else {
      selectedIds.add(photoId);
      grmState.selected = photoId;
      grmState.selectionAnchor = photoId;
    }
  } else if (mode === 'range') {
    if (!grmState.selectionAnchor) grmState.selectionAnchor = grmState.selected || photoId;
    _grmSelectRange(grmState.selectionAnchor, photoId);
    grmState.selected = photoId;
  } else {
    if (grmState.selected === photoId && selectedIds.size === 1 && selectedIds.has(photoId)) {
      selectedIds.clear();
      grmState.selected = null;
      grmState.selectionAnchor = null;
    } else {
      selectedIds.clear();
      selectedIds.add(photoId);
      grmState.selected = photoId;
      grmState.selectionAnchor = photoId;
    }
  }
  grmSyncSelectionClasses();
  grmRefreshSelectedLoupe();
}

function grmRefreshSelectedLoupe() {
  var loupeImg = document.getElementById('grmLoupePhoto');
  var loupeInfo = document.getElementById('grmLoupeInfo');
  var loupePreds = document.getElementById('grmLoupePreds');
  if (!loupeImg || !loupeInfo || !loupePreds) return;
  if (grmState.selected) {
    var nextSrc = grmLoupePhotoUrl(grmState.selected);
    if (loupeImg.getAttribute('src') !== nextSrc) {
      grmHideSelectedEyeCrosshair();
      loupeImg.src = nextSrc;
      _grmAfterLoupeSourceChange();
    } else {
      grmUpdateSelectedEyeCrosshair();
    }
    var item = grmState.items.find(function(it) { return it.photo_id === grmState.selected; });
    if (item) {
      var sharp = item.subject_sharpness != null ? Math.round(item.subject_sharpness) : (item.sharpness != null ? Math.round(item.sharpness) : '?');
      var boxSharp = item.box_sharpness != null ? ' — box sharpness: ' + Math.round(item.box_sharpness) : '';
      loupeInfo.textContent = (item.filename || '') + ' — sharpness: ' + sharp + boxSharp + ' — move cursor to compare';
      loupePreds.innerHTML = renderLoupePreds(item);
    }
    grmUpdateResLabel();
  } else {
    grmHideSelectedEyeCrosshair();
    loupeImg.src = '';
    loupeInfo.textContent = 'Select a photo to preview. Hover to compare sharpness across all frames.';
    loupePreds.innerHTML = '';
    grmLoupeReset();
  }
}

function renderLoupePreds(item) {
  var preds = [];
  if (item.species) {
    preds.push({ species: item.species, confidence: item.confidence || 0, top: true });
  }
  (item.alternatives || []).forEach(function(a) {
    preds.push({ species: a.species, confidence: a.confidence || 0, top: false });
  });
  preds = preds.slice(0, 5);
  var header = '<div class="grm-loupe-preds-title">This photo\u2019s top predictions</div>';
  if (!preds.length) {
    return header + '<div class="grm-loupe-preds-empty">No predictions available.</div>';
  }
  return header + preds.map(function(p) {
    return '<div class="grm-loupe-preds-row' + (p.top ? ' top' : '') + '">' +
      '<span>' + escapeHtml(p.species) + '</span>' +
      '<span class="conf">' + Math.round((p.confidence || 0) * 100) + '%</span>' +
    '</div>';
  }).join('');
}

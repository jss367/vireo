// Group Review cards, subject selection, and loupe selection.
// Classic page script; shared globals are initialized before boot.js runs.

function grmHasSpeciesKeyword(photoId, species) {
  if (!species || !grmState || grmState.keywordStateSpecies !== species) return false;
  var st = (grmState.dbState && grmState.dbState[photoId]) || {};
  return !!st.has_species_keyword;
}

function grmKeywordStats(items) {
  var speciesEl = document.getElementById('grmSpecies');
  var species = speciesEl ? (speciesEl.value || '').trim() : '';
  var stats = {
    species: species,
    total: species ? items.length : 0,
    applied: 0,
    missing: 0
  };
  if (!species) return stats;

  items.forEach(function(p) {
    if (grmHasSpeciesKeyword(p.id, species)) stats.applied++;
    else stats.missing++;
  });
  return stats;
}

function grmUpdateKeywordSummary(stats) {
  var summary = document.getElementById('grmSpeciesKeywordSummary');
  if (!summary) return;
  summary.className = 'grm-keyword-summary';
  if (!grmState || !grmState.seeded) {
    summary.textContent = 'Loading keyword state';
    return;
  }
  if (!stats.species) {
    summary.textContent = 'No species keyword';
    return;
  }
  summary.textContent = 'Species keyword: ' + stats.applied + '/' + stats.total + ' applied';
  if (stats.total > 0 && stats.missing === 0) summary.classList.add('ok');
  else if (stats.missing > 0) summary.classList.add('warn');
}

function grmKeywordBadgeHtml(p, stats) {
  if (!stats.species || !grmState || !grmState.seeded || stats.total === 0) return '';
  var hasKw = grmHasSpeciesKeyword(p.id, stats.species);
  if (stats.missing === 0) return '';
  if (stats.applied === 0) return '';
  if (hasKw) {
    return '<span class="grm-card-kw-dot" title="Species keyword already applied"></span>';
  }
  return '<span class="grm-card-needs-tag" title="Missing species keyword; Apply will add it if Confirm species is checked">Needs tag</span>';
}

function grmPhotoSubjects(photo) {
  return photo && Array.isArray(photo.subjects) ? photo.subjects : [];
}

function grmSubjectTopPrediction(subject) {
  var predictions = subject && Array.isArray(subject.predictions)
    ? subject.predictions : [];
  if (!predictions.length) return null;
  return predictions.reduce(function(best, entry) {
    return !best || Number(entry[1] || 0) > Number(best[1] || 0) ? entry : best;
  }, null);
}

function grmSubjectLabel(subject, index) {
  var top = grmSubjectTopPrediction(subject);
  return 'Subject ' + (index + 1) + ' · ' + (top ? top[0] : 'Unclassified');
}

function grmSubjectBadgeHtml(photo) {
  var subjects = grmPhotoSubjects(photo);
  if (subjects.length < 2) return '';
  return '<span class="grm-card-subject-count" data-testid="multi-subject-badge" ' +
    'title="Multiple detected subjects">' + subjects.length + ' subjects</span>';
}

function grmSelectedSubject(photo) {
  var subjects = grmPhotoSubjects(photo);
  if (!subjects.length) return null;
  if (!grmState.selectedSubjectByPhoto) grmState.selectedSubjectByPhoto = {};
  var selectedId = grmState.selectedSubjectByPhoto[photo.id];
  var selected = subjects.find(function(subject) {
    return String(subject.detection_id) === String(selectedId);
  });
  if (!selected) {
    selected = subjects[0];
    grmState.selectedSubjectByPhoto[photo.id] = selected.detection_id;
  }
  return selected;
}

function grmSelectSubject(photoId, detectionId) {
  if (!grmState || !grmState.items) return;
  var photo = grmState.items.find(function(item) { return item.id === photoId; });
  if (!photo) return;
  var subject = grmPhotoSubjects(photo).find(function(item) {
    return String(item.detection_id) === String(detectionId);
  });
  if (!subject) return;
  if (!grmState.selectedSubjectByPhoto) grmState.selectedSubjectByPhoto = {};
  grmState.selectedSubjectByPhoto[photoId] = subject.detection_id;
  grmState.selected = photoId;
  grmRefreshSelectedLoupe();
}

function renderGroupModal() {
  var items = grmState.items.filter(function(p) { return !grmState.removed.has(p.id); });
  // Per-card badges are keyed off the *visible* items — a removed frame has
  // no card to decorate, so its keyword state can't drive per-card markers
  // without confusing the user. The header summary, in contrast, must count
  // every frame Apply would still tag (removed frames included when flags
  // are unchecked), which is what _grmKeywordSummaryMembers resolves.
  var keywordStats = grmKeywordStats(items);
  var summaryStats = grmKeywordStats(_grmKeywordSummaryMembers());

  function renderCard(p) {
    var isSelected = grmState.selectedIds && grmState.selectedIds.has(p.id);
    var cls = 'grm-card' + (isSelected ? ' selected' : '');
    var qScore = p.quality_composite != null ? Math.round(p.quality_composite * 100) : '-';
    var sharp = p.subject_tenengrad != null ? Math.round(p.subject_tenengrad) : '-';
    var boxSharp = p.box_sharpness != null ? Math.round(p.box_sharpness) : null;

    // Read flag/keyword state from the live DB snapshot the modal opened
    // with, not the (potentially stale) cached p.flag — so a photo flagged
    // earlier in this session shows the "already in DB" marker even if the
    // pipeline cache hasn't been refreshed.
    var dbSt = (grmState.dbState && grmState.dbState[p.id]) || {};
    var dbFlag = dbSt.flag || 'none';

    var flagHtml = '';
    if (dbFlag === 'flagged') flagHtml = '<span class="photo-flag-badge flag-flagged" title="Already flagged in library" style="bottom:auto;top:4px;left:4px;">P</span>';
    else if (dbFlag === 'rejected') flagHtml = '<span class="photo-flag-badge flag-rejected" title="Already rejected in library" style="bottom:auto;top:4px;left:4px;">X</span>';

    var kwHtml = grmKeywordBadgeHtml(p, keywordStats);
    var subjectHtml = grmSubjectBadgeHtml(p);
    var repHtml = dbSt.is_species_representative
      ? '<span class="grm-card-representative">Representative</span>'
      : '';

    var nat = grmNaturalDims(p);
    var imgStyle = 'width:' + nat.w + 'px;height:' + nat.h + 'px;';
    var scoresHtml = 'Q: <span class="score-val">' + qScore + '</span> S: <span class="score-val">' + sharp + '</span>';
    if (boxSharp != null) scoresHtml += ' Box: <span class="score-val">' + boxSharp + '</span>';
    return '<div class="' + cls + '" data-photo-id="' + p.id + '"' +
        ' data-nat-w="' + nat.w + '" data-nat-h="' + nat.h + '"' +
        ' onmousedown="grmCardMouseDown(event)"' +
        ' onclick="grmCardClick(event, ' + p.id + ')"' +
        ' ondblclick="grmCardDblClick(event)">' +
      '<div class="grm-card-img-box">' +
        '<img src="' + grmPhotoUrl(p.id) + '" style="' + imgStyle + '"' +
          ' onload="grmCardImgLoaded(this)" loading="lazy" draggable="false">' +
        flagHtml +
        kwHtml +
        subjectHtml +
        repHtml +
      '</div>' +
      '<div class="grm-card-info">' +
        '<div class="grm-card-species">' + escapeHtml(p.filename || '') + '</div>' +
        '<div class="grm-card-scores">' + scoresHtml + '</div>' +
      '</div>' +
    '</div>';
  }

  var pickItems = items.filter(function(p) { return grmState.picks.has(p.id); });
  var candidateItems = items.filter(function(p) { return !grmState.picks.has(p.id) && !grmState.rejects.has(p.id); });
  var rejectItems = items.filter(function(p) { return grmState.rejects.has(p.id); });

  document.getElementById('grmPicks').innerHTML = pickItems.map(renderCard).join('') || '<span style="font-size:12px;color:var(--text-ghost);">Press ↑ to add picks</span>';
  document.getElementById('grmCandidates').innerHTML = candidateItems.map(renderCard).join('') || '<span style="font-size:12px;color:var(--text-ghost);">All sorted</span>';
  document.getElementById('grmRejects').innerHTML = rejectItems.map(renderCard).join('') || '<span style="font-size:12px;color:var(--text-ghost);">Press ↓ to reject</span>';

  // Species input — pre-fill from the encounter on the *initial* render only.
  // Once the user has typed in (or cleared) the field, honor that: repopulating
  // here would make it impossible to leave the species blank, and would also
  // resurrect the smart default so an unconfirmed predicted burst could apply
  // an unwanted species without the user noticing.
  if (!document.getElementById('grmSpecies').value && items.length > 0 && !grmState.speciesFieldTouched) {
    var enc = pipelineResults.encounters[grmState.encIdx];
    // A burst carrying the explicit-empty override ({confirmed:false,
    // species_list:[]}) is confirmed as nothing; an empty field is its
    // honest state, not a gap to fill from the encounter (see
    // openGroupReview's initialSpecies).
    var renderBurst = enc && enc.bursts && enc.bursts[grmState.burstIdx];
    var renderOvr = renderBurst && renderBurst.species_override;
    var renderExplicitEmpty = !!renderOvr
      && Array.isArray(renderOvr.species_list)
      && renderOvr.species_list.length === 0;
    if (!renderExplicitEmpty) {
      document.getElementById('grmSpecies').value = enc.confirmed_species || (enc.species ? enc.species[0] : '') || '';
    }
  }

  document.getElementById('grmCount').textContent = pickItems.length + ' picks, ' + rejectItems.length + ' rejects, ' + candidateItems.length + ' unsorted';
  grmUpdateKeywordSummary(summaryStats);

  // Re-apply offset indicators and the per-card transforms (cover-fit always
  // needs an explicit transform now that <img>s are laid out at natural
  // source-pixel size). innerHTML wipes inline styles on every re-render.
  document.querySelectorAll('#grmOverlay .grm-card').forEach(function(card) {
    _grmUpdateIndicator(card);
  });
  grmApplyCardTransforms();
  grmRefreshResetAllVisibility();
  grmUpdateApplyLabel();
}

function _grmVisibleItems() {
  return grmState.items.filter(function(p) { return !grmState.removed.has(p.id); });
}

// Members whose keyword state the header summary reports on. Mirrors the
// effective post-apply burst membership (grmSpeciesMemberItems): a removed
// photo is excluded only when its removal actually commits (checks.flags);
// otherwise the removal is discarded on close and Apply still tags it, so
// the summary must count it too. Falls back to visible items before the
// modal is seeded (no diff/checks yet).
function _grmKeywordSummaryMembers() {
  if (!grmState || !grmState.seeded) return _grmVisibleItems();
  var checks = grmResolveChecks(grmComputeDiff());
  return grmSpeciesMemberItems(checks);
}

function _grmEnsureSelectedIds() {
  if (!grmState.selectedIds) grmState.selectedIds = new Set();
  return grmState.selectedIds;
}

function _grmSelectRange(anchorId, photoId) {
  var selectedIds = _grmEnsureSelectedIds();
  var items = _grmVisibleItems();
  var a = items.findIndex(function(p) { return p.id === anchorId; });
  var b = items.findIndex(function(p) { return p.id === photoId; });
  if (a < 0 || b < 0) {
    selectedIds.add(photoId);
    return;
  }
  var start = Math.min(a, b);
  var end = Math.max(a, b);
  for (var i = start; i <= end; i++) {
    selectedIds.add(items[i].id);
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
  var detailEl = document.getElementById('grmLoupeDetail');
  if (!loupeImg || !loupeInfo || !detailEl) return;

  if (grmState.selected) {
    var nextSrc = grmPhotoUrl(grmState.selected);
    if (loupeImg.getAttribute('src') !== nextSrc) {
      grmHideSelectedEyeCrosshair();
      loupeImg.src = nextSrc;
      _grmAfterLoupeSourceChange();
    } else {
      grmUpdateSelectedEyeCrosshair();
    }
    var photo = grmState.items.find(function(p) { return p.id === grmState.selected; });
    if (photo) {
      var sharp = photo.subject_tenengrad != null ? Math.round(photo.subject_tenengrad) : '?';
      var boxSharp = photo.box_sharpness != null ? ' - box sharpness: ' + Math.round(photo.box_sharpness) : '';
      loupeInfo.textContent = (photo.filename || '') + ' - sharpness: ' + sharp + boxSharp + ' - move cursor to compare';
      detailEl.innerHTML = buildPipelineMetadataHtml(photo);
    }
    grmUpdateResLabel();
  } else {
    grmHideSelectedEyeCrosshair();
    loupeImg.removeAttribute('src');
    loupeInfo.textContent = 'Select a photo to preview. Hover to compare sharpness across all frames.';
    detailEl.innerHTML = '';
    grmLoupeReset();
  }
}

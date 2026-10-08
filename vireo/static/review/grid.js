// The prediction grid: filtering, sorting, cards, burst-group buttons, and grid clicks.
// Classic page script; load boot.js after all definitions.

/* ---------- Grid Rendering ---------- */
function getVisibleItems() {
  var filtered = predictions.slice(); // copy for sorting

  // Filter by minimum confidence
  if (minConfidence > 0) {
    filtered = filtered.filter(function(p) { return p.confidence >= minConfidence; });
  }

  // Filter by model
  if (currentModel !== 'all') {
    filtered = filtered.filter(function(p) { return p.model === currentModel; });
  }

  // Filter by label-set fingerprint (from dashboard inventory's Inspect link)
  if (currentLabelsFingerprint) {
    filtered = filtered.filter(function(p) {
      return p.labels_fingerprint === currentLabelsFingerprint;
    });
  }

  // Filter by status tab
  if (currentTab !== 'all') {
    filtered = filtered.filter(function(p) { return p.status === currentTab; });
  }

  // Sort
  filtered.sort(function(a, b) {
    switch (currentSort) {
      case 'confidence_desc': return (b.confidence || 0) - (a.confidence || 0);
      case 'confidence_asc': return (a.confidence || 0) - (b.confidence || 0);
      case 'species': return (a.species || '').localeCompare(b.species || '');
      case 'filename': return (a.filename || '').localeCompare(b.filename || '');
      case 'date': return (a.timestamp || '').localeCompare(b.timestamp || '');
      case 'date_desc': return (b.timestamp || '').localeCompare(a.timestamp || '');
      default: return 0;
    }
  });

  // Deduplicate groups — show only the first photo per group
  var seenGroups = {};
  return filtered.filter(function(p) {
    if (!p.group_id) return true;
    if (seenGroups[p.group_id]) return false;
    seenGroups[p.group_id] = true;
    return true;
  });
}

function renderGrid() {
  var items = getVisibleItems();
  var grid = document.getElementById('grid');
  var empty = document.getElementById('empty');

  if (items.length === 0) {
    grid.innerHTML = '';
    if (predictions.length === 0) {
      empty.textContent = 'No predictions to review. Run classification on the Classify page first.';
    } else {
      empty.textContent = 'No predictions in this category.';
    }
    empty.style.display = 'block';
    return;
  }
  empty.style.display = 'none';
  grid.style.setProperty('--card-width', thumbSize + 'px');

  grid.innerHTML = items.map(function(p) { return renderPredictionCard(p); }).join('');
}

/* Delegate grid clicks for lightbox and group review (XSS-safe) */
function bindReviewGridClicks() {
  document.getElementById('grid').addEventListener('click', function(e) {
    var img = e.target.closest('img[data-photo-id]');
    if (img) {
      var seen = {};
      var photoList = [];
      var thumbs = document.getElementById('grid').querySelectorAll('img[data-photo-id]');
      for (var i = 0; i < thumbs.length; i++) {
        var pid = parseInt(thumbs[i].dataset.photoId, 10);
        if (seen[pid]) continue;
        seen[pid] = true;
        photoList.push({ id: pid, filename: thumbs[i].dataset.filename || '' });
      }
      openLightbox(parseInt(img.dataset.photoId, 10), img.dataset.filename || '', photoList);
      return;
    }
    var groupBtn = e.target.closest('button[data-group-id]');
    if (groupBtn) {
      openGroupReview(groupBtn.dataset.groupId, groupBtn.dataset.model || '');
    }
  });
}

function getConsensusSpecies(pred) {
  // For grouped predictions, derive the consensus winner from the individual votes
  if (pred.individual) {
    try {
      var votes = typeof pred.individual === 'string' ? JSON.parse(pred.individual) : pred.individual;
      var best = null;
      var bestCount = 0;
      for (var sp in votes) {
        if (votes[sp] > bestCount) { bestCount = votes[sp]; best = sp; }
      }
      if (best) return best;
    } catch(e) {}
  }
  return pred.species;
}

function renderPredictionCard(pred) {
  if (
    typeof window.vireoRememberPhotoEditRecipe === 'function' &&
    Object.prototype.hasOwnProperty.call(pred, 'edit_recipe')
  ) {
    window.vireoRememberPhotoEditRecipe(pred.photo_id, pred.edit_recipe, {
      skipIfLocallyWritten: true,
    });
  }
  var thumbUrl = window.vireoThumbnailUrl
    ? window.vireoThumbnailUrl(pred)
    : '/thumbnails/' + pred.photo_id + '.jpg';
  var displaySpecies = pred.group_id ? getConsensusSpecies(pred) : pred.species;
  var confPct = Math.round(pred.confidence * 100);
  var confColor = confPct >= 70 ? 'var(--accent)' : confPct >= 50 ? 'var(--warning)' : 'var(--danger)';
  var cardClass = 'card';
  if (pred.status === 'accepted') cardClass += ' accepted';
  if (pred.status === 'rejected') cardClass += ' skipped';

  // Category badge
  var badgeHtml = '';
  if (pred.category && pred.category !== 'match') {
    var badgeClass = pred.category === 'new' ? 'badge-new' : pred.category === 'refinement' ? 'badge-refinement' : 'badge-disagreement';
    var badgeText = pred.category;
    if (pred.category === 'disagreement' && pred.existing_species && pred.existing_species.length > 0) {
      badgeText = 'disagrees with "' + pred.existing_species.join(', ') + '"';
    } else if (pred.category === 'refinement' && pred.existing_species && pred.existing_species.length > 0) {
      badgeText = 'refines "' + pred.existing_species.join(', ') + '"';
    }
    badgeHtml = '<span class="badge ' + badgeClass + '">' + escapeHtml(badgeText) + '</span>';
  }

  // Group badge
  var groupHtml = '';
  if (pred.group_id && pred.total_votes > 1) {
    groupHtml = '<span class="badge-group">' + pred.vote_count + '/' + pred.total_votes + ' votes</span>';
  }

  // Individual vote breakdown
  var voteHtml = '';
  if (pred.individual) {
    try {
      var votes = typeof pred.individual === 'string' ? JSON.parse(pred.individual) : pred.individual;
      var parts = [];
      var keys = Object.keys(votes).sort(function(a, b) { return votes[b] - votes[a]; });
      keys.forEach(function(k) {
        parts.push('<span>' + escapeHtml(k) + ': ' + votes[k] + '</span>');
      });
      if (parts.length > 1) {
        voteHtml = '<div class="individual-preds">' + parts.join('') + '</div>';
      }
    } catch(e) {}
  }

  // Action buttons
  var actionsHtml;
  var species = escapeHtml(displaySpecies || '');
  if (pred.status === 'accepted') {
    actionsHtml = '<div class="card-actions"><button class="btn-done">Accepted as ' + species + '</button></div>';
  } else if (pred.status === 'rejected') {
    actionsHtml = '<div class="card-actions"><button class="btn-done skipped-btn">Rejected</button></div>';
  } else {
    var acceptLabel = pred.group_id
      ? 'Tag ' + pred.total_votes + ' photos as "' + species + '"'
      : 'Tag as "' + species + '"';
    actionsHtml =
      '<div class="card-actions">' +
        '<button class="btn-accept" onclick="acceptPrediction(' + pred.id + ')">' + acceptLabel + '</button>' +
        '<button class="btn-skip" onclick="rejectPrediction(' + pred.id + ')">Not ' + species + '</button>' +
      '</div>';
  }

  // Model badge — always show
  var modelHtml = '<span style="font-size:10px;background:var(--bg-tertiary);color:var(--info);padding:1px 6px;border-radius:3px;margin-left:4px;">' + escapeHtml(pred.model || '') + '</span>';

  // Other models' predictions for the same photo
  var otherModelsHtml = '';
  if (availableModels.length > 1) {
    var others = predictions.filter(function(p) {
      return p.photo_id === pred.photo_id && p.model !== pred.model;
    });
    if (others.length > 0) {
      parts = [];
      others.forEach(function(o) {
        var oPct = Math.round(o.confidence * 100);
        var oColor = o.species === pred.species ? 'var(--accent)' : 'var(--warning)';
        parts.push('<span style="color:' + oColor + '">' + escapeHtml(o.model) + ': ' + escapeHtml(o.species) + ' (' + oPct + '%)</span>');
      });
      otherModelsHtml = '<div class="individual-preds" style="margin-top:4px;border-top:1px solid var(--border-primary);padding-top:4px;">' + parts.join('') + '</div>';
    }
  }

  // Alternative predictions
  var alternativesHtml = '';
  if (pred.alternatives && pred.alternatives.length > 0 && pred.status === 'pending') {
    var altItems = '';
    pred.alternatives.forEach(function(alt) {
      var altPct = Math.round(alt.confidence * 100);
      var altColor = altPct >= 70 ? 'var(--accent)' : altPct >= 50 ? 'var(--warning)' : 'var(--danger)';
      altItems +=
        '<div style="display:flex;align-items:center;gap:8px;padding:4px 0;">' +
          '<span style="flex:1;font-size:13px;">' + escapeHtml(alt.species) + '</span>' +
          '<div style="flex:0 0 60px;height:6px;background:var(--bg-tertiary);border-radius:3px;overflow:hidden;">' +
            '<div style="width:' + altPct + '%;height:100%;background:' + altColor + ';border-radius:3px;"></div>' +
          '</div>' +
          '<span style="flex:0 0 35px;font-size:11px;color:var(--text-secondary);text-align:right;">' + altPct + '%</span>' +
          '<button onclick="acceptAlternative(' + alt.id + ',' + pred.id + ')" ' +
            'style="flex:0 0 auto;background:var(--bg-tertiary);color:var(--text-primary);border:1px solid var(--border-primary);border-radius:4px;padding:2px 8px;font-size:11px;cursor:pointer;">Accept</button>' +
        '</div>';
    });
    alternativesHtml =
      '<details style="margin-top:6px;border-top:1px solid var(--border-primary);padding-top:4px;">' +
        '<summary style="font-size:11px;color:var(--text-secondary);cursor:pointer;user-select:none;">Alternatives (' + pred.alternatives.length + ')</summary>' +
        '<div style="padding:4px 0;">' + altItems + '</div>' +
      '</details>';
  }

  // Build image with optional bounding box overlay
  var hasBox = pred.box_x != null && pred.box_y != null && pred.box_w != null && pred.box_h != null;
  var hideDetectionOverlay = (
    typeof window.vireoPhotoHasOrientationEdit === 'function' &&
    window.vireoPhotoHasOrientationEdit(pred.photo_id)
  );
  var imgStyle = hasBox ? 'cursor:pointer;object-fit:contain;' : 'cursor:pointer;';
  var imgTag = '<img src="' + thumbUrl + '" loading="lazy" alt="' + escapeAttr(pred.filename || '') +
    '" style="' + imgStyle + '" data-photo-id="' + pred.photo_id + '" data-filename="' + escapeAttr(pred.filename || '') + '">';
  var representativeBadge = pred.is_species_representative
    ? '<span class="representative-badge">Representative</span>'
    : '';

  var imgHtml;
  if (hasBox) {
    var color = getSpeciesColor(displaySpecies);
    var bLeft = (pred.box_x * 100).toFixed(2) + '%';
    var bTop = (pred.box_y * 100).toFixed(2) + '%';
    var bWidth = (pred.box_w * 100).toFixed(2) + '%';
    var bHeight = (pred.box_h * 100).toFixed(2) + '%';
    imgHtml = '<div class="card-img-wrap">' + imgTag +
      representativeBadge +
      '<div class="detection-box" data-photo-id="' + pred.photo_id + '" style="' + ((reviewDetectionBoxesVisible && !hideDetectionOverlay) ? '' : 'display:none;') + 'left:' + bLeft + ';top:' + bTop +
      ';width:' + bWidth + ';height:' + bHeight + ';border-color:' + color + ';">' +
      '<span class="detection-label" style="background:' + color +
      ';color:#0A1F2E;">' + escapeHtml(displaySpecies) + ' ' + confPct + '%</span>' +
      '</div></div>';
  } else {
    imgHtml = '<div class="card-img-wrap">' + imgTag + representativeBadge + '</div>';
  }

  return '<div class="' + cardClass + '" data-pred-id="' + pred.id + '">' +
    imgHtml +
    '<div class="card-body">' +
      '<div class="card-filename">' + escapeHtml(pred.filename || '') + modelHtml + '</div>' +
      badgeHtml + groupHtml +
      '<div class="card-prediction">' + escapeHtml(displaySpecies) + '</div>' +
      voteHtml +
      '<div class="confidence-bar"><div class="confidence-fill" style="width:' + confPct + '%;background:' + confColor + '"></div></div>' +
      '<div class="card-confidence">' + confPct + '% confidence' + (pred.group_id ? ' (consensus)' : '') + '</div>' +
      otherModelsHtml +
      alternativesHtml +
      actionsHtml +
      groupMembersHtml(pred) +
    '</div>' +
  '</div>';
}

function groupMembersHtml(pred) {
  if (!pred.group_id) return '';

  var members = predictions.filter(function(p) {
    return p.group_id === pred.group_id && p.model === pred.model;
  });
  if (members.length <= 1) return '';

  return '<button data-group-id="' + escapeAttr(pred.group_id) + '" data-model="' + escapeAttr(pred.model || '') + '" ' +
    'style="background:var(--accent,#24E5CA);color:var(--accent-text,#0A1F2E);border:none;border-radius:6px;padding:8px 12px;font-size:13px;font-weight:600;cursor:pointer;margin-top:8px;width:100%;display:flex;align-items:center;justify-content:center;gap:6px;">' +
    '<span style="font-size:16px;">&#9638;</span> Review Burst Group (' + members.length + ' photos)</button>';
}

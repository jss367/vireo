// The prediction grid: filtering, sorting, cards, burst-group buttons, and grid clicks.
// Classic page script; load boot.js after all definitions.

/* ---------- Grid Rendering ---------- */
// Fresh comparison categories (`effective_category`) mapped to the badge the
// stored classify-time `category` would have drawn.
/** @type {Record<string, string | null>} */
var REVIEW_EFFECTIVE_BADGE_KIND = {
  match: null, new: 'new', refinement: 'refinement', broader: 'broader', conflict: 'disagreement',
};

/**
 * The category badge for a card, or null. The server's `effective_category`
 * compares the prediction with the photo's current keywords, leaving out a
 * species another detection on the photo accounts for, so a second bird is
 * not shown as disagreeing with the first. The stored `category` is a
 * classify-time snapshot against every keyword on the photo, used only when
 * no fresh comparison was possible (the same precedence as Browse).
 * `existing_species` names the keywords the comparison used.
 * @param {ReviewPrediction} pred
 * @returns {{cls: string, text: string} | null}
 */
function reviewCategoryBadge(pred) {
  var eff = pred.effective_category;
  var kind = eff ? REVIEW_EFFECTIVE_BADGE_KIND[eff] || null : pred.category || null;
  if (!kind || kind === 'match') return null;
  var existing = (pred.existing_species || []).join(', ');
  if (kind === 'disagreement') {
    return {cls: 'badge-disagreement', text: existing ? 'disagrees with "' + existing + '"' : 'disagreement'};
  }
  if (kind === 'refinement') {
    return {cls: 'badge-refinement', text: existing ? 'refines "' + existing + '"' : 'refinement'};
  }
  if (kind === 'broader') {
    return {cls: 'badge-refinement', text: existing ? 'broader than "' + existing + '"' : 'broader'};
  }
  return {cls: kind === 'new' ? 'badge-new' : 'badge-disagreement', text: kind};
}

/** @param {boolean} [includeGroupMembers] */
function getVisibleItems(includeGroupMembers) {
  var filtered = predictions.slice(); // copy for sorting

  if (reviewEvidenceFilter !== 'all') {
    filtered = filtered.filter(function(p) {
      if (reviewEvidenceFilter === 'deferred') return reviewEvidenceDeferred(p);
      return Boolean(p.evidence?.reasons.length);
    });
  }

  // Filter by minimum relative score
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

  if (includeGroupMembers) return filtered;

  // Deduplicate groups — show only the first photo per group
  /** @type {Record<string,boolean>} */ var seenGroups = {};
  return filtered.filter(function(p) {
    if (reviewEvidenceFilter !== 'all') {
      var subject = 'detection:' + p.detection_id;
      if (seenGroups[subject]) return false;
      seenGroups[subject] = true;
      return true;
    }
    if (!p.group_id) return true;
    if (seenGroups[p.group_id]) return false;
    seenGroups[p.group_id] = true;
    return true;
  });
}

function renderGrid() {
  var items = getVisibleItems();
  var grid = /** @type {HTMLElement} */ (document.getElementById('grid'));
  var empty = /** @type {HTMLElement} */ (document.getElementById('empty'));

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
  /** @type {HTMLElement} */ (document.getElementById('grid')).addEventListener('click', function(e) {
    if (!(e.target instanceof Element)) return;
    var img = e.target.closest(/** @type {'img'} */ ('img[data-photo-id]'));
    if (img) {
      /** @type {Record<number,boolean>} */ var seen = {};
      var photoList = [];
      var thumbs = /** @type {HTMLElement} */ (document.getElementById('grid')).querySelectorAll(/** @type {'img'} */ ('img[data-photo-id]'));
      for (var i = 0; i < thumbs.length; i++) {
        var thumb = thumbs[i]; if (!thumb) continue; var pid = parseInt(thumb.dataset.photoId || '', 10);
        if (seen[pid]) continue;
        seen[pid] = true;
        photoList.push({ id: pid, filename: thumb.dataset.filename || '' });
      }
      openLightbox(parseInt(img.dataset.photoId || '', 10), img.dataset.filename || '', photoList);
      return;
    }
    var groupBtn = e.target.closest(/** @type {'button'} */ ('button[data-group-id]'));
    if (groupBtn) {
      openGroupReview(groupBtn.dataset.groupId || '', groupBtn.dataset.model || '');
    }
  });
}

/** @param {ReviewPrediction} pred */ function getConsensusSpecies(pred) {
  // For grouped predictions, derive the consensus winner from the individual votes
  if (pred.individual) {
    try {
      var votes = reviewVoteCounts(pred.individual);
      var best = null;
      var bestCount = 0;
      for (var sp in votes) {
        if ((votes[sp] || 0) > bestCount) { bestCount = votes[sp] || 0; best = sp; }
      }
      if (best) return best;
    } catch(e) {}
  }
  return pred.species;
}

/** @param {ReviewPrediction} pred */ function renderPredictionCard(pred) {
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
  var confPct = reviewRelativeScore(pred.confidence);
  var confColor = 'var(--info)';
  var cardClass = 'card';
  if (pred.status === 'accepted') cardClass += ' accepted';
  if (pred.status === 'rejected') cardClass += ' skipped';

  // Category badge
  var badge = reviewCategoryBadge(pred);
  var badgeHtml = badge
    ? '<span class="badge ' + badge.cls + '">' + escapeHtml(badge.text) + '</span>'
    : '';

  // Group badge
  var groupHtml = '';
  if (pred.group_id && pred.total_votes > 1) {
    groupHtml = '<span class="badge-group">' + pred.vote_count + '/' + pred.total_votes + ' votes</span>';
  }

  // Individual vote breakdown
  var voteHtml = '';
  if (pred.individual) {
    try {
      var votes = reviewVoteCounts(pred.individual);
      /** @type {string[]} */ var parts = [];
      var keys = Object.keys(votes).sort(function(a, b) { return (votes[b] || 0) - (votes[a] || 0); });
      keys.forEach(function(k) {
        parts.push('<span>' + escapeHtml(k) + ': ' + votes[k] + '</span>');
      });
      if (parts.length > 1) {
        voteHtml = '<div class="individual-preds">' + parts.join('') + '</div>';
      }
    } catch(e) {}
  }

  var actionsHtml = reviewPredictionActions(pred, displaySpecies);

  // Model badge — always show
  var modelHtml = '<span style="font-size:10px;background:var(--bg-tertiary);color:var(--info);padding:1px 6px;border-radius:3px;margin-left:4px;">' + escapeHtml(pred.model || '') + '</span>';

  var otherModelsHtml = reviewModelEvidenceHtml(pred);

  // Alternative predictions
  var alternativesHtml = '';
  if (pred.alternatives && pred.alternatives.length > 0 && pred.status === 'pending' && !reviewEvidenceBlocksAccept(pred.id)) {
    var altItems = '';
    pred.alternatives.forEach(function(alt) {
      var altPct = reviewRelativeScore(alt.confidence);
      var altColor = 'var(--info)';
      altItems +=
        '<div style="display:flex;align-items:center;gap:8px;padding:4px 0;">' +
          '<span style="flex:1;font-size:13px;">' + escapeHtml(alt.species) + '</span>' +
          '<div style="flex:0 0 60px;height:6px;background:var(--bg-tertiary);border-radius:3px;overflow:hidden;">' +
            '<div style="width:' + altPct + '%;height:100%;background:' + altColor + ';border-radius:3px;"></div>' +
          '</div>' +
          '<span style="flex:0 0 35px;font-size:11px;color:var(--text-secondary);text-align:right;">' + altPct + '% relative</span>' +
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
  var box = reviewPredictionBox(pred); var hasBox = box !== null;
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
  if (box) {
    var color = getSpeciesColor(displaySpecies);
    var bLeft = (box.x * 100).toFixed(2) + '%';
    var bTop = (box.y * 100).toFixed(2) + '%';
    var bWidth = (box.w * 100).toFixed(2) + '%';
    var bHeight = (box.h * 100).toFixed(2) + '%';
    imgHtml = '<div class="card-img-wrap">' + imgTag +
      representativeBadge +
      '<div class="detection-box" data-photo-id="' + pred.photo_id + '" style="' + ((reviewDetectionBoxesVisible && !hideDetectionOverlay) ? '' : 'display:none;') + 'left:' + bLeft + ';top:' + bTop +
      ';width:' + bWidth + ';height:' + bHeight + ';border-color:' + color + ';">' +
      '<span class="detection-label" style="background:' + color +
      ';color:#0A1F2E;">' + escapeHtml(displaySpecies) + ' · ' + confPct + '% relative</span>' +
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
      '<div class="card-confidence">' + confPct + '% relative score for ' + escapeHtml(pred.species) + '</div>' +
      reviewEvidenceHtml(pred) +
      otherModelsHtml +
      alternativesHtml +
      actionsHtml +
      groupMembersHtml(pred) +
    '</div>' +
  '</div>';
}

/** @param {ReviewPrediction} pred */ function groupMembersHtml(pred) {
  if (!pred.group_id) return '';

  var members = predictions.filter(function(p) {
    return p.group_id === pred.group_id && p.model === pred.model;
  });
  if (members.length <= 1) return '';

  return '<button data-group-id="' + escapeAttr(pred.group_id) + '" data-model="' + escapeAttr(pred.model || '') + '" ' +
    'style="background:var(--accent,#24E5CA);color:var(--accent-text,#0A1F2E);border:none;border-radius:6px;padding:8px 12px;font-size:13px;font-weight:600;cursor:pointer;margin-top:8px;width:100%;display:flex;align-items:center;justify-content:center;gap:6px;">' +
    '<span style="font-size:16px;">&#9638;</span> Review Burst Group (' + members.length + ' photos)</button>';
}

/** @param {ReviewPrediction['individual']} raw @returns {Record<string,number>} */
function reviewVoteCounts(raw) {
  /** @type {unknown} */ var parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};
  return Object.fromEntries(Object.entries(parsed).filter(function(entry) { return typeof entry[1] === 'number'; }));
}

/** @param {ReviewPrediction} prediction */
function reviewPredictionBox(prediction) {
  if (prediction.box_x == null || prediction.box_y == null || prediction.box_w == null || prediction.box_h == null) return null;
  return {x:prediction.box_x,y:prediction.box_y,w:prediction.box_w,h:prediction.box_h};
}

/** @param {ReviewPrediction} pred @param {string} displaySpecies */
function reviewPredictionActions(pred, displaySpecies) {
  var actionsHtml;
  var species = escapeHtml(displaySpecies || '');
  if (pred.status === 'accepted') {
    actionsHtml = '<div class="card-actions"><button class="btn-done">Accepted as ' + species + '</button></div>';
  } else if (pred.status === 'rejected') {
    actionsHtml = '<div class="card-actions"><button class="btn-done skipped-btn">Rejected</button></div>';
  } else if (reviewEvidenceDeferred(pred)) {
    actionsHtml = '<p>Identification deferred. Clear the subject assessment above to review species again.</p>';
  } else if (reviewEvidenceBlocksAccept(pred.id)) {
    actionsHtml = '<p>Species confirmation is on hold. Revisit the subject assessments for this region and its burst.</p>';
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

  return actionsHtml;
}

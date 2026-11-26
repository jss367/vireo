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

// Session-only ordering keeps skipped cards available at the end of the grid.
/** @type {Map<number, number>} */ var skippedPredictionOrder = new Map();
var predictionSkipSequence = 0;

/**
 * Filtered and sorted rows with their card index, shared by the calls of one
 * renderAll(). The heading, the tabs, Accept All and the grid each ask for
 * the visible rows; a whole-workspace queue takes about a second to filter,
 * sort and index, so one render does it once per status view.
 * @type {Map<string, {rows: ReviewPrediction[], index: Map<number, ReviewPrediction[]>}>|null}
 */
var reviewVisibleRowsCache = null;

/** @param {boolean} [includeAllStatuses] */
function reviewVisibleRows(includeAllStatuses) {
  var statusView = includeAllStatuses ? 'all' : currentTab;
  var cached = reviewVisibleRowsCache?.get(statusView);
  if (cached) {
    // Card lookups read the index of the latest view asked for, so a
    // status view brings back the index that was built with its rows.
    reviewCardMemberIndex = cached.index;
    reviewCardIndexRows = predictions;
    reviewCardIndexScope = reviewCardScopeKey();
    return cached.rows;
  }
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
  if (statusView !== 'all') {
    filtered = filtered.filter(function(p) { return p.status === statusView; });
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

  indexVisibleReviewCards(filtered);
  reviewVisibleRowsCache?.set(statusView, {rows: filtered, index: reviewCardMemberIndex});
  return filtered;
}

/** @param {boolean} [includeGroupMembers] @param {boolean} [includeAllStatuses] */
function getVisibleItems(includeGroupMembers, includeAllStatuses) {
  var filtered = reviewVisibleRows(includeAllStatuses);
  if (includeGroupMembers) return filtered;

  // Deduplicate groups — show only the first photo per group
  /** @type {Record<string,boolean>} */ var seenGroups = {};
  const cards = filtered.filter(function(p) {
    if (reviewEvidenceFilter !== 'all') {
      var subject = 'detection:' + p.detection_id;
      if (seenGroups[subject]) return false;
      seenGroups[subject] = true;
      return true;
    }
    var key = reviewCardKey(p);
    if (seenGroups[key]) return false;
    seenGroups[key] = true;
    return true;
  });
  if (skippedPredictionOrder.size) {
    cards.sort((a, b) => (skippedPredictionOrder.get(a.id) || 0) - (skippedPredictionOrder.get(b.id) || 0));
  }
  return cards;
}

// The grid builds its cards in batches. A whole-workspace queue is about
// 100,000 cards; building them all keeps the page blank for minutes and
// repeats on every accept, because a decision re-renders the grid.
var REVIEW_GRID_BATCH = 60;
/** Every card of the current view, in display order. @type {ReviewPrediction[]} */ var reviewGridItems = [];
/** How many of `reviewGridItems` are in the DOM. */ var reviewGridRendered = 0;
var reviewGridViewKey = '';
/** @type {IntersectionObserver|null} */ var reviewGridMoreObserver = null;

function renderGrid() {
  var items = getVisibleItems();
  var grid = /** @type {HTMLElement} */ (document.getElementById('grid'));
  var empty = /** @type {HTMLElement} */ (document.getElementById('empty'));
  // A decision re-renders the same view: keep the cards already built so the
  // page does not jump back to the top. A different view starts over.
  var viewKey = JSON.stringify([reviewCardScopeKey(), currentSort, currentCollection, currentPhotoIdFilter]);
  var keep = viewKey === reviewGridViewKey ? reviewGridRendered : 0;
  reviewGridViewKey = viewKey;
  reviewGridItems = items;
  reviewGridRendered = Math.min(items.length, Math.max(REVIEW_GRID_BATCH, keep));

  if (items.length === 0) {
    grid.innerHTML = '';
    if (predictions.length === 0) {
      empty.textContent = 'No predictions to review. Run classification on the Classify page first.';
    } else {
      empty.textContent = 'No predictions in this category.';
    }
    empty.style.display = 'block';
    renderReviewGridMore();
    return;
  }
  empty.style.display = 'none';
  grid.style.setProperty('--card-width', thumbSize + 'px');

  grid.innerHTML = items.slice(0, reviewGridRendered).map(function(p) { return renderPredictionCard(p); }).join('');
  renderReviewGridMore();
}

/** Append the next batch of cards below the ones already built. */
function showMoreReviewCards() {
  var next = reviewGridItems.slice(reviewGridRendered, reviewGridRendered + REVIEW_GRID_BATCH);
  if (!next.length) return;
  /** @type {HTMLElement} */ (document.getElementById('grid')).insertAdjacentHTML(
    'beforeend', next.map(function(p) { return renderPredictionCard(p); }).join(''));
  reviewGridRendered += next.length;
  renderReviewGridMore();
}

/** Say how many cards are built, and build more when the reader nears the end. */
function renderReviewGridMore() {
  var more = /** @type {HTMLElement} */ (document.getElementById('gridMore'));
  if (!more) return;
  var remaining = reviewGridItems.length - reviewGridRendered;
  more.hidden = remaining <= 0;
  if (remaining <= 0) return;
  /** @type {HTMLElement} */ (document.getElementById('gridMoreStatus')).textContent =
    'Showing ' + reviewGridRendered.toLocaleString() + ' of ' + reviewGridItems.length.toLocaleString() +
    ' cards. More appear as you scroll.';
  /** @type {HTMLElement} */ (document.getElementById('gridMoreBtn')).textContent =
    'Show ' + Math.min(REVIEW_GRID_BATCH, remaining).toLocaleString() + ' more';
  if (reviewGridMoreObserver) {
    // An observer reports only a change. Observing again makes it report the
    // current position, so a batch that still leaves the end in view builds
    // the next one.
    reviewGridMoreObserver.unobserve(more);
    reviewGridMoreObserver.observe(more);
  }
}

function bindReviewGridMore() {
  var more = /** @type {HTMLElement} */ (document.getElementById('gridMore'));
  /** @type {HTMLElement} */ (document.getElementById('gridMoreBtn')).addEventListener('click', showMoreReviewCards);
  if (typeof IntersectionObserver !== 'function') return;
  reviewGridMoreObserver = new IntersectionObserver(function(entries) {
    if (entries.some(function(entry) { return entry.isIntersecting; })) showMoreReviewCards();
  }, {rootMargin: '0px 0px 1500px 0px'});
  reviewGridMoreObserver.observe(more);
}

/** The photos of the current view in display order, one entry per photo, for the lightbox's arrow keys. */
function reviewGridPhotoList() {
  /** @type {Set<number>} */ var seen = new Set();
  /** @type {{id:number;filename:string}[]} */ var photoList = [];
  reviewGridItems.forEach(function(p) {
    if (seen.has(p.photo_id)) return;
    seen.add(p.photo_id);
    photoList.push({id: p.photo_id, filename: p.filename || ''});
  });
  return photoList;
}

/* Delegate grid clicks for lightbox and group review (XSS-safe) */
function bindReviewGridClicks() {
  /** @type {HTMLElement} */ (document.getElementById('grid')).addEventListener('click', function(e) {
    if (!(e.target instanceof Element)) return;
    var img = e.target.closest(/** @type {'img'} */ ('img[data-photo-id]'));
    if (img) {
      openLightbox(parseInt(img.dataset.photoId || '', 10), img.dataset.filename || '', reviewGridPhotoList());
      return;
    }
    var groupBtn = e.target.closest(/** @type {'button'} */ ('button[data-group-id]'));
    if (groupBtn) {
      openGroupReview(groupBtn.dataset.groupId || '', groupBtn.dataset.model || '');
    }
  });
}

/** @param {ReviewPrediction} pred */ function getConsensusSpecies(pred) {
  if (pred.consensus_species) return pred.consensus_species;
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
  var displaySpecies = getConsensusSpecies(pred);
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
  var modelHtml = reviewCardModelsHtml(pred);

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
    ? '<span class="representative-badge">Top highlight</span>'
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
      ';color:#0A1F2E;">' + escapeHtml(pred.species) + ' · ' + confPct + '% relative</span>' +
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
  /** @type {Map<string, ReviewPrediction[]>} */ var groups = new Map();
  reviewCardMembers(pred).forEach(function(p) {
    if (!p.group_id) return;
    var key = JSON.stringify([p.model, p.group_id]);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key)?.push(p);
  });
  return Array.from(groups.values(), function(members) {
    var group = members[0];
    var photoCount = new Set(members.map(function(p) { return p.photo_id; })).size;
    if (!group || photoCount <= 1) return '';
    return '<button data-group-id="' + escapeAttr(group.group_id) + '" data-model="' + escapeAttr(group.model || '') + '" ' +
    'style="background:var(--accent,#24E5CA);color:var(--accent-text,#0A1F2E);border:none;border-radius:6px;padding:8px 12px;font-size:13px;font-weight:600;cursor:pointer;margin-top:8px;width:100%;display:flex;align-items:center;justify-content:center;gap:6px;">' +
    '<span style="font-size:16px;">&#9638;</span> Review Burst Group (' + photoCount + ' photos) · ' + escapeHtml(group.model) + '</button>';
  }).join('');
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
    var photoCount = reviewCardPhotoCount(pred);
    var acceptLabel = photoCount > 1
      ? 'Tag ' + photoCount + ' photos as "' + species + '"'
      : 'Tag as "' + species + '"';
    actionsHtml =
      '<div class="card-actions">' +
        '<button class="btn-accept" onclick="acceptPrediction(' + pred.id + ')">' + acceptLabel + '</button>' +
        '<button class="btn-skip" onclick="rejectPrediction(' + pred.id + ')">Not ' + species + '</button>' +
      '</div>';
  }

  return actionsHtml;
}

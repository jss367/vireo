// Rendering the analysis: the summary's decision stats and actions, and
// each species with its scene groups and photo cards.
// Classic page script.

function cullDecisionStatsHtml() {
  var data = /** @type {CullData} */ (cullData);
  var html = '';
  var pinned = data.pinned_count || 0;
  if (pinned) {
    html += '<div class="summary-stat" title="Decisions you made by hand. ' +
      'Recomputing keeps them exactly as they are; Apply writes them to the photos.">' +
      '<div class="num" style="color:var(--text-primary);">' + pinned + '</div>' +
      '<div class="label">Pinned by you</div></div>';
  }
  var confirmed = data.confirmed_count || 0;
  if (confirmed) {
    html += '<div class="summary-stat" title="Photos already flagged by a previous Apply. ' +
      'Their saved flag is shown instead of the fresh suggestion.">' +
      '<div class="num" style="color:var(--text-secondary);">' + confirmed + '</div>' +
      '<div class="label">Already applied</div></div>';
  }
  return html;
}

function cullDecisionActionsHtml() {
  var buttons = [];
  var pinnedTotal = manualDecisionCount();
  if (pinnedTotal) {
    var offscreen = /** @type {CullData} */ (cullData).pinned_offscreen || 0;
    var label = 'Clear ' + pinnedTotal + ' pinned';
    var title = 'Drop your manual decisions and go back to what the analysis suggests.';
    if (offscreen) {
      title += ' ' + offscreen + ' of them are on photos outside this view.';
    }
    buttons.push('<button class="summary-link" title="' + escapeAttr(title) +
      '" onclick="clearPinnedDecisions()">' + escapeHtml(label) + '</button>');
  }
  if (!buttons.length) return '';
  return '<div class="summary-actions">' + buttons.join('') + '</div>';
}

function renderCulling() {
  if (!cullData || !cullData.species_groups || cullData.species_groups.length === 0) {
    /** @type {HTMLElement} */ (document.getElementById('cullSummary')).style.display = 'none';
    /** @type {HTMLButtonElement} */ (document.getElementById('applyBtn')).style.display = 'none';
    /** @type {HTMLElement} */ (document.getElementById('cullContent')).innerHTML =
      '<div class="cull-empty">' + (cullSourceResults && hasCullFilters() ? 'No photos match these species and dates. Try changing the filters.' : 'No pipeline results for this view. Run Analyze for Culling after extracting pipeline features.') + '</div>';
    renderCullScope();
    focusedCullGroup = { speciesIdx: null, groupIdx: null };
    return;
  }
  if (Object.keys(expandedSpecies).length === 0 && cullData.species_groups.length > 0) {
    expandedSpecies[0] = true;
  }
  normalizeFocusedCullGroup();
  renderCullScope();
  /** @type {Record<number, CullSourcePhoto>} */
  var rawPhotoMap = {};
  (pipelineResults && pipelineResults.photos || []).forEach(function(p) { rawPhotoMap[p.id] = p; });

  // Summary bar
  var summaryHtml = '<div class="summary-bar">' +
    '<div class="summary-stat"><div class="num">' + cullData.total_photos + '</div><div class="label">Photos</div></div>' +
    '<div class="summary-stat"><div class="num" style="color:var(--accent);">' + cullData.suggested_keepers + '</div><div class="label">Favorites</div></div>' +
    '<div class="summary-stat"><div class="num" style="color:var(--warning);">' + (cullData.suggested_reviews || 0) + '</div><div class="label">Undecided</div></div>' +
    '<div class="summary-stat"><div class="num" style="color:var(--danger);">' + cullData.suggested_rejects + '</div><div class="label">Reject</div></div>' +
    '<div class="summary-stat"><div class="num">' + cullData.species_groups.length + '</div><div class="label">Species</div></div>' +
    cullDecisionStatsHtml() +
    cullDecisionActionsHtml() +
    '<button class="summary-link" id="cullReviewRejected" onclick="reviewCullRejected()"' +
      (cullData.suggested_rejects ? '' : ' disabled') + '>Review rejected (' + cullData.suggested_rejects + ')</button>' +
  '</div>';
  /** @type {HTMLElement} */ (document.getElementById('cullSummary')).innerHTML = summaryHtml;
  /** @type {HTMLElement} */ (document.getElementById('cullSummary')).style.display = '';
  /** @type {HTMLButtonElement} */ (document.getElementById('applyBtn')).style.display = '';

  // Species groups
  var html = '';
  cullData.species_groups.forEach(function(sg, si) {
    var keepCount = sg.keepers;
    var reviewCount = sg.reviews || 0;
    var rejectCount = sg.rejects;

    html += '<div class="species-section">';
    html += '<div class="species-header" onclick="toggleSpecies(' + si + ')">';
    html += '<span class="species-name">' + escapeHtml(sg.species) + '</span>';
    html += '<span class="species-stats">' + sg.photo_count + ' photos &middot; ';
    html += '<span class="species-keep">' + keepCount + ' favorites</span> &middot; ';
    html += '<span style="color:var(--warning);">' + reviewCount + ' undecided</span> &middot; ';
    html += '<span class="species-reject">' + rejectCount + ' reject</span>';
    html += '</span></div>';

    var isExpanded = expandedSpecies[si];
    html += '<div class="species-body" id="speciesBody' + si + '" style="display:' + (isExpanded ? '' : 'none') + ';">';

    var groups = cullGroupsForSpecies(sg);
    html += cullFavoritesHtml(sg, si);
    html += '<div class="cull-remaining-heading">Remaining photos <span class="cull-hint">Add a favorite, keep undecided, or reject.</span></div>';
    groups.forEach(function(pg, /** @type {number} */ pi) {
      var groupFocused = focusedCullGroup.speciesIdx === si && focusedCullGroup.groupIdx === pi;
      html += '<div class="pose-group' + (groupFocused ? ' focused' : '') + '" data-species-index="' + si + '" data-group-index="' + pi + '">';
      var groupLabel = pg.label || ('Scene ' + (pi + 1) + ' (' + pg.photos.length + ' photos)');
      html += '<div class="pose-label">' + escapeHtml(groupLabel) + '</div>';

      // Store photo list for lightbox navigation
      var poseKey = 'cullPose_' + si + '_' + pi;
      /** @type {Record<string, unknown>} */ (/** @type {unknown} */ (window))[poseKey] = pg.photos.map(function(p) { return rawPhotoMap[p.photo_id] || {id: p.photo_id, filename: p.filename || ''}; });

      html += '<div class="pose-strip">';
      var remaining = pg.photos.filter(function(p) { return !p.favorite; });
      remaining.forEach(function(photo) { html += cullCardHtml(photo, si, pi, poseKey, false); });
      if (!remaining.length) html += '<p class="cull-hint">All photos in this encounter are in Favorites above.</p>';
      html += '</div></div>';
    });

    html += '</div></div>';
  });

  /** @type {HTMLElement} */ (document.getElementById('cullContent')).innerHTML = html;

}

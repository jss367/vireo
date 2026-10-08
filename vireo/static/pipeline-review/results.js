// Encounter grid reconciliation and photo card rendering.
// Classic page script; shared globals are initialized before boot.js runs.

// Compare the generated markup rather than object identity: review actions
// update photos and encounters in place. Retaining unchanged cards also keeps
// their decoded thumbnails and horizontal burst scroll positions intact.
var pipelineReviewRenderedCards = new Map();

function reconcilePipelineReviewCards(container, cards) {
  var nextCards = new Map();
  var nextNodes = new Set();
  cards.forEach(function(entry) {
    var previous = pipelineReviewRenderedCards.get(entry.index);
    var card;
    if (previous && previous.html === entry.html && previous.node.parentNode === container) {
      card = previous.node;
    } else {
      var template = document.createElement('template');
      template.innerHTML = entry.html;
      card = template.content.firstElementChild;
    }
    nextCards.set(entry.index, {html: entry.html, node: card});
    nextNodes.add(card);
  });
  Array.from(container.children).forEach(function(card) {
    if (!nextNodes.has(card)) card.remove();
  });
  var cursor = container.firstElementChild;
  nextCards.forEach(function(entry) {
    if (entry.node === cursor) cursor = cursor.nextElementSibling;
    else container.insertBefore(entry.node, cursor);
  });
  pipelineReviewRenderedCards = nextCards;
}

function renderResults() {
  clearTimeout(_speciesFilterRenderTimer);
  _speciesFilterRenderTimer = null;
  if (!pipelineResults) return;
  var container = document.getElementById('encountersContainer');
  if (!container) return;
  // Build photo lookup
  var photoMap = {};
  pipelineResults.photos.forEach(function(p) { photoMap[p.id] = p; });
  var speciesConflictEvidence = buildSpeciesConflictEvidence(photoMap);

  // Build the photo set for encounters matching the species/filename search.
  var speciesVisibleIds = null;
  var speciesVisibleEncounters = null;
  if (speciesFilter) {
    speciesVisibleIds = new Set();
    speciesVisibleEncounters = new Set();
    pipelineResults.encounters.forEach(function(enc, ei) {
      if (encounterMatchesSearch(enc, photoMap)) {
        speciesVisibleEncounters.add(ei);
        (enc.photo_ids || []).forEach(function(pid) { speciesVisibleIds.add(pid); });
      }
    });
  }

  // Build set of photos hidden because their burst/encounter is confirmed
  var confirmedHiddenIds = null;
  if (hideConfirmed) {
    confirmedHiddenIds = new Set();
    pipelineResults.encounters.forEach(function(enc) {
      if (enc.bursts && enc.bursts.length > 0) {
        enc.bursts.forEach(function(burst) {
          if (isBurstConfirmed(enc, burst)) {
            var ids = burst.photo_ids || burst || [];
            ids.forEach(function(pid) { confirmedHiddenIds.add(pid); });
          }
        });
      } else if (enc.species_confirmed) {
        (enc.photo_ids || []).forEach(function(pid) { confirmedHiddenIds.add(pid); });
      }
    });
  }

  // This toggle mirrors the encounter-level species widget: if the header
  // would render "Add species", hide the entire encounter and exclude its
  // photos from the toolbar counts.
  var withoutSuggestionHiddenIds = null;
  if (hideWithoutSuggestions) {
    withoutSuggestionHiddenIds = new Set();
    pipelineResults.encounters.forEach(function(enc) {
      if (encounterHasSpeciesSuggestion(enc)) return;
      (enc.photo_ids || []).forEach(function(pid) { withoutSuggestionHiddenIds.add(pid); });
    });
  }

  // Update filter counts (respecting search and both hide toggles)
  var counts = {all: 0, KEEP: 0, REVIEW: 0, REJECT: 0, SPECIES_CONFLICT: 0};
  pipelineResults.photos.forEach(function(p) {
    if (speciesVisibleIds && !speciesVisibleIds.has(p.id)) return;
    if (confirmedHiddenIds && confirmedHiddenIds.has(p.id)) return;
    if (withoutSuggestionHiddenIds && withoutSuggestionHiddenIds.has(p.id)) return;
    counts.all++;
    if (p.label) counts[p.label] = (counts[p.label] || 0) + 1;
    if (speciesConflictEvidence[p.id] && speciesConflictEvidence[p.id].severity) {
      counts.SPECIES_CONFLICT++;
    }
  });
  var ce = document.getElementById('countAll'); if (ce) ce.textContent = ' (' + counts.all + ')';
  ce = document.getElementById('countKeep'); if (ce) ce.textContent = ' (' + (counts.KEEP||0) + ')';
  ce = document.getElementById('countReview'); if (ce) ce.textContent = ' (' + (counts.REVIEW||0) + ')';
  ce = document.getElementById('countReject'); if (ce) ce.textContent = ' (' + (counts.REJECT||0) + ')';
  ce = document.getElementById('countSpeciesConflict'); if (ce) ce.textContent = ' (' + counts.SPECIES_CONFLICT + ')';

  var cards = [];
  // Track which encounter indices actually produced a card so focus
  // management below can pick a *visible* encounter (not one that was
  // filtered out by species filter, hide-confirmed, or label filter).
  var renderedIndices = [];
  // Reset the click-time target cache; it's rebuilt below to mirror the exact
  // set of photos each Reject/Clear button represents on screen.
  pipelineReviewVisibleTargets = {};
  encounterSortOrder().forEach(function(ei) {
    var enc = pipelineResults.encounters[ei];
    var timeRange = formatEncounterTimeRange(enc.time_range);

    // Check if the encounter passes the species/filename search.
    if (speciesVisibleEncounters && !speciesVisibleEncounters.has(ei)) return;
    if (hideWithoutSuggestions && !encounterHasSpeciesSuggestion(enc)) return;

    // Build the set of photos this encounter will actually draw: skip
    // hide-confirmed photos and photos filtered out by the active label /
    // species-conflict filter. The encounter card is rendered only if this
    // set is non-empty, and the encounter-level Reject/Clear button targets
    // exactly this set so it can never flip flags on photos the user can't
    // see.
    var encPhotoIds = enc.photo_ids || [];
    var visibleEncIds = encPhotoIds.filter(function(pid) {
      var p = photoMap[pid];
      if (!p) return false;
      if (confirmedHiddenIds && confirmedHiddenIds.has(pid)) return false;
      return photoMatchesPipelineFilter(p, speciesConflictEvidence[pid]);
    });
    if (!visibleEncIds.length) return;
    pipelineReviewVisibleTargets['encounter:' + ei] = visibleEncIds;

    renderedIndices.push(ei);

    var encKey = encounterKey(enc);
    var isCollapsed = encKey != null && collapsedEncounters.has(encKey);
    var visibleBurstCount = enc.bursts && enc.bursts.length
      ? enc.bursts.filter(function(burst) { return !hideConfirmed || !isBurstConfirmed(enc, burst); }).length
      : visibleEncIds.length;
    // Until an offscreen card is first laid out, estimate its height from the
    // visible rows and thumbnail size. The browser remembers its real height.
    var estimatedHeight = isCollapsed ? '48px'
      : 'calc(100px + ' + visibleBurstCount + ' * (var(--photo-card-size, 160px) * 2 / 3 + 60px))';
    var html = '<div class="encounter-card" data-encounter-index="' + ei +
      '" style="--encounter-estimated-height:' + estimatedHeight + '">';
    html += '<div class="encounter-header" onclick="setFocusedEncounter(' + ei + ')">';
    html += '<span class="encounter-chevron' + (isCollapsed ? ' collapsed' : '') + '" id="encChev' + ei + '" onclick="toggleEncounter(' + ei + ')">&#9660;</span>';
    html += renderSpeciesWidget(enc, ei, 'encounter', null);
    html += '<span class="encounter-meta" onclick="toggleEncounter(' + ei + ')" style="cursor:pointer;flex:1;">';
    html += '<span>' + enc.photo_count + ' photos</span>';
    if (enc.burst_count) html += '<span>' + enc.burst_count + ' bursts</span>';
    if (timeRange) html += '<span>' + escapeHtml(timeRange) + '</span>';
    html += missingTimestampBadge(enc);
    html += renderEncounterSpeciesConflict(enc, speciesConflictEvidence, confirmedHiddenIds);
    html += '</span>';
    html += renderGroupRejectButton('encounter', ei, null, visibleEncIds, photoMap);
    html += '</div>';
    html += '<div class="encounter-body" id="encBody' + ei + '"' + (isCollapsed ? ' style="display:none;"' : '') + '>';

    if (enc.bursts && enc.bursts.length > 0) {
      enc.bursts.forEach(function(burst, bi) {
        if (hideConfirmed && isBurstConfirmed(enc, burst)) return;
        var burstIds = burst.photo_ids || burst;
        // Narrow the burst's Reject/Clear target to photos that actually
        // render — the active label / species-conflict filter can hide
        // frames inside a burst even though the burst strip itself is drawn.
        var visibleBurstIds = burstIds.filter(function(pid) {
          var p = photoMap[pid];
          if (!p) return false;
          return photoMatchesPipelineFilter(p, speciesConflictEvidence[pid]);
        });
        pipelineReviewVisibleTargets['burst:' + ei + ':' + bi] = visibleBurstIds;
        html += '<div class="burst-strip">';
        html += '<div class="burst-controls">';
        if (enc.bursts.length > 1) {
          html += '<span class="burst-label">B' + (bi+1) + '</span>';
        }
        // Burst-level species widget (only show if overridden or multiple bursts)
        if (enc.bursts.length > 1 || (burst.species_override)) {
          html += renderSpeciesWidget(enc, ei, 'burst', bi);
        }
        if (!burst.species_override && enc.bursts.length > 1) {
          html += '<span class="burst-species-edit" onclick="toggleSpeciesDropdown(event,' + ei + ',' + bi + ',\'top\')">&#9998;</span>';
        }
        html += renderBurstSpeciesConflict(enc, ei, burst, bi, speciesConflictEvidence);
        html += renderGroupRejectButton('burst', ei, bi, visibleBurstIds, photoMap);
        html += '<button class="detach-btn" onclick="detachBurst(' + ei + ',' + bi + ')" title="Detach burst">&times;</button>';
        html += '</div>';
        burstIds.forEach(function(pid) {
          var p = photoMap[pid];
          if (!p) return;
          if (!photoMatchesPipelineFilter(p, speciesConflictEvidence[pid])) return;
          html += renderPhotoCard(p, ei, bi, speciesConflictEvidence[pid]);
        });
        html += '</div>';
      });
    } else {
      // No burst data — render flat
      encPhotoIds.forEach(function(pid) {
        var p = photoMap[pid];
        if (!p) return;
        if (!photoMatchesPipelineFilter(p, speciesConflictEvidence[pid])) return;
        html += renderPhotoCard(p, null, null, speciesConflictEvidence[pid]);
      });
    }

    // Bottom bar — same controls as the top header so confirming after
    // scrolling through a long encounter doesn't require scrolling back up.
    html += '<div class="encounter-header encounter-footer">';
    html += renderSpeciesWidget(enc, ei, 'encounter', null, 'bot');
    html += '<span class="encounter-meta" style="flex:1;">';
    html += '<span>' + enc.photo_count + ' photos</span>';
    if (enc.burst_count) html += '<span>' + enc.burst_count + ' bursts</span>';
    if (timeRange) html += '<span>' + escapeHtml(timeRange) + '</span>';
    html += missingTimestampBadge(enc);
    html += renderEncounterSpeciesConflict(enc, speciesConflictEvidence, confirmedHiddenIds);
    html += '</span></div>';

    html += '</div></div>';
    cards.push({index: ei, html: html});
  });

  closeAllDropdowns();
  reconcilePipelineReviewCards(container, cards);

  // Focus management: only focus encounters that are actually rendered.
  // If we focused an index that got filtered out (species/hide-confirmed/
  // label filter), the trace panel would show data for a card the user
  // can't see, with no .focused highlight visible anywhere — confusing.
  if (renderedIndices.length === 0) {
    _focusedEncounterIdx = null;
    renderAlgorithmTrace();
  } else if (_focusedEncounterIdx === null || renderedIndices.indexOf(_focusedEncounterIdx) === -1) {
    // No focus yet, or current focus is hidden — fall back to the first
    // visible encounter.
    setFocusedEncounter(renderedIndices[0]);
  } else {
    // Re-apply focus highlight (DOM was rebuilt) and refresh trace panel.
    setFocusedEncounter(_focusedEncounterIdx);
  }
}

function renderPhotoCard(p, encIdx, burstIdx, speciesConflict) {
  var label = (p.label || '').toLowerCase();
  var manualLabel = '';
  var visibleLabel;
  var visibleLabelClass = label;
  var hasQuality = hasQualityScore(p);
  var q = hasQuality ? Number(p.quality_composite) : null;
  var qPct = hasQuality ? Math.round(q * 100) : 0;
  if (p.flag === 'flagged') {
    manualLabel = 'KEEP';
    visibleLabelClass = 'keep';
  } else if (p.flag === 'rejected') {
    manualLabel = 'REJECT';
    visibleLabelClass = 'reject';
  }
  visibleLabel = manualLabel || (showPhotoLabels ? (p.label || '') : '');
  var cardLabelClass = manualLabel ? visibleLabelClass : (showPhotoLabels ? label : '');
  var thumbUrl = window.vireoThumbnailUrl ? window.vireoThumbnailUrl(p) : '/thumbnails/' + p.id + '.jpg';
  var html = '<div class="photo-card ' + cardLabelClass + '" data-photo-id="' + p.id + '">';
  html += '<img src="' + escapeAttr(thumbUrl) + '" loading="lazy" alt="" onclick="openInspect(' + p.id + ')">';
  if (visibleLabel) html += '<span class="photo-label ' + visibleLabelClass + '">' + visibleLabel + '</span>';
  if (p.rarity_protected) html += '<span class="photo-rarity">protected</span>';
  if (p.is_species_representative) html += '<span class="photo-representative">Representative</span>';
  if (p.flag === 'flagged') html += '<span class="photo-flag-badge flag-flagged">P</span>';
  else if (p.flag === 'rejected') html += '<span class="photo-flag-badge flag-rejected">X</span>';
  if (encIdx != null && burstIdx != null) {
    html += '<button class="detach-btn" onclick="event.stopPropagation();detachPhoto(' + encIdx + ',' + burstIdx + ',' + p.id + ')" title="Detach photo" style="position:absolute;top:2px;right:2px;">&times;</button>';
  }
  if (hasQuality) {
    html += '<div class="photo-score-bar"><div class="photo-score-fill" style="width:' + qPct + '%"></div></div>';
    html += '<div class="photo-info">' + escapeHtml(p.filename || '') + ' &middot; Q=' + q.toFixed(2) + '</div>';
  } else {
    html += '<div class="photo-info">' + escapeHtml(p.filename || '') + '</div>';
  }
  if (speciesConflict && speciesConflict.severity) {
    var conflictTitle = escapeAttr(speciesConflictTitle(speciesConflict));
    html += '<button class="species-conflict-badge photo-species-conflict ' + speciesConflict.severity +
      '" data-species-conflict="' + speciesConflict.severity + '" onclick="event.stopPropagation();openInspect(' +
      p.id + ')" title="' + conflictTitle + '" aria-label="' + conflictTitle + '">';
    html += '<span aria-hidden="true">&#9888;</span><span class="species-conflict-label">' +
      escapeHtml(speciesConflict.alternativeSpecies) + ' ' +
      formatSpeciesConfidence(speciesConflict.alternativeSupport) + '</span></button>';
  }
  html += '</div>';
  return html;
}

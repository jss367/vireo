/* Cull's scope, favorites and comparison presentation. Decisions are pinned
 * by cull.html and saved through its existing undoable Apply operation. */
var cullSourceResults = null;
var cullSourceCollectionId = null;
var cullSelectedSpecies = new Set();
var cullScopeFailed = false;
var cullFavoriteReferences = Object.create(null);
var cullCompareState = null;
var cullComparisonSeq = 0;
var browseCompare = null;

function hasCullFilters() {
  return cullSelectedSpecies.size > 0 || !!document.getElementById('cullDateFrom').value ||
    !!document.getElementById('cullDateTo').value;
}

function cullPhotoSpecies(photo, encounter, photoMap) {
  if (photo.confirmed_species) return speciesName(photo.confirmed_species);
  if (photo.species_top5 && photo.species_top5.length) return topSpeciesForPhoto(photo);
  return speciesForEncounter(encounter || {}, photoMap || {});
}

function cullPhotoDay(photo) {
  // EXIF dates are camera-local dates; UTC conversion would move photos
  // around midnight into a different day. Missing dates never pass a range.
  return /^\d{4}-\d{2}-\d{2}/.test(photo.timestamp || '') ? photo.timestamp.substring(0, 10) : '';
}

function cullPhotoMatches(photo, species) {
  if (cullSelectedSpecies.size && !cullSelectedSpecies.has(species)) return false;
  return cullPhotoMatchesDate(photo);
}

function cullPhotoMatchesDate(photo) {
  var from = document.getElementById('cullDateFrom').value;
  var to = document.getElementById('cullDateTo').value;
  var day = cullPhotoDay(photo);
  if ((from || to) && !day) return false;
  return (!from || day >= from) && (!to || day <= to);
}

function cullSourcePhotos() {
  var results = cullSourceResults;
  if (!results) return [];
  var map = {};
  (results.photos || []).forEach(function(p) { map[p.id] = p; });
  var seen = new Set();
  var photos = [];
  (results.encounters || []).forEach(function(enc) {
    (enc.photo_ids || []).forEach(function(id) {
      if (!map[id] || seen.has(id)) return;
      seen.add(id);
      photos.push({photo: map[id], species: cullPhotoSpecies(map[id], enc, map)});
    });
  });
  return photos;
}

function cullScopePhotoIds() {
  return cullSourcePhotos().filter(function(entry) {
    return cullPhotoMatches(entry.photo, entry.species);
  }).map(function(entry) { return entry.photo.id; });
}

function renderCullSpeciesOptions() {
  var counts = Object.create(null);
  cullSourcePhotos().forEach(function(entry) {
    if (cullPhotoMatchesDate(entry.photo)) counts[entry.species] = (counts[entry.species] || 0) + 1;
  });
  cullSelectedSpecies.forEach(function(name) { if (!counts[name]) counts[name] = 0; });
  var search = document.getElementById('cullSpeciesSearch').value.toLocaleLowerCase();
  var list = document.getElementById('cullSpeciesOptions');
  list.replaceChildren();
  Object.keys(counts).sort(function(a, b) { return a.localeCompare(b); }).forEach(function(name) {
    if (search && name.toLocaleLowerCase().indexOf(search) === -1) return;
    var label = document.createElement('label');
    var checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.checked = cullSelectedSpecies.has(name);
    checkbox.addEventListener('change', function() {
      if (checkbox.checked) cullSelectedSpecies.add(name);
      else cullSelectedSpecies.delete(name);
      onCullScopeChange();
    });
    label.append(checkbox, document.createTextNode(name + ' (' + counts[name] + ')'));
    list.appendChild(label);
  });
  if (!list.childNodes.length) list.textContent = cullSourceResults ? 'No matching species.' : 'Analyze photos to load species.';
  document.getElementById('cullSpeciesSummary').textContent = cullSelectedSpecies.size
    ? Array.from(cullSelectedSpecies).join(', ') : 'All species';
}

function renderCullScope() {
  var names = cullSelectedSpecies.size ? Array.from(cullSelectedSpecies).join(', ') : 'All species';
  var from = document.getElementById('cullDateFrom').value;
  var to = document.getElementById('cullDateTo').value;
  var dates = from && to ? from + ' through ' + to : from ? 'From ' + from : to ? 'Through ' + to : 'All dates';
  document.getElementById('cullScope').textContent = names + ' · ' + dates + ' · ' +
    (cullData ? cullData.total_photos : 0) + ' photos with pipeline results';
}

function clearCullSpecies() {
  cullSelectedSpecies.clear();
  onCullScopeChange();
}

function clearCullFilters() {
  cullSelectedSpecies.clear();
  document.getElementById('cullDateFrom').value = '';
  document.getElementById('cullDateTo').value = '';
  onCullScopeChange();
}

function onCullScopeChange() {
  // Invalidate an older request before any new request starts, including an
  // empty scope (which needs no server request at all).
  ++cullAnalysisSeq;
  clearTimeout(_reflowTimer); _reflowTimer = null;
  clearTimeout(_regroupTimer); _regroupTimer = null;
  if (browseCompare) browseCompare.close();
  closeCullRejected();
  expandedSpecies = {};
  renderCullSpeciesOptions();
  pipelineResults = cullSourceResults;
  rebuildCullDataFromPipeline();
  var from = document.getElementById('cullDateFrom').value;
  var to = document.getElementById('cullDateTo').value;
  if (from && to && from > to) {
    cullScopeFailed = true;
    document.getElementById('cullStatus').textContent = 'Choose an end date on or after the start date.';
    updateCullApplyButton();
    return;
  }
  cullScopeFailed = false;
  if (cullSourceCollectionId !== selectedCollectionId || (cullSourceResults && cullScopePhotoIds().length)) { cullScopeFailed = true; runCulling(); }
  else updateCullApplyButton();
}

function cullBurstId(enc, id, encounterIndex) {
  var burst = (enc.bursts || []).findIndex(function(b) { return (b.photo_ids || b || []).indexOf(id) !== -1; });
  return encounterIndex + ':' + burst;
}

function assignCullFavorites(sg) {
  var days = Object.create(null);
  sg.scene_groups.forEach(function(group, gi) {
    group.photos.forEach(function(photo) {
      photo.group_index = gi;
      photo.favorite = photo.action === 'keep' && photo.decision_source !== 'auto';
      (days[photo.day] || (days[photo.day] = [])).push(photo);
    });
  });
  Object.keys(days).forEach(function(day) {
    var candidates = days[day].filter(function(p) {
      return p.decision_source === 'auto' && p.suggested_action === 'keep';
    }).sort(function(a, b) { return b.quality - a.quality || a.photo_id - b.photo_id; });
    // Give different encounters, then different bursts, a chance before
    // filling the shortlist with neighboring frames of the same moment.
    var encounters = new Set();
    var bursts = new Set();
    days[day].filter(function(p) { return p.favorite; }).forEach(function(p) {
      encounters.add(p.group_index); bursts.add(p.burst_id);
    });
    var chosen = [];
    for (var pass = 0; pass < 3 && chosen.length < 3; pass++) {
      candidates.forEach(function(p) {
        if (chosen.length >= 3 || chosen.indexOf(p) !== -1) return;
        if (pass === 0 && encounters.has(p.group_index)) return;
        if (pass === 1 && bursts.has(p.burst_id)) return;
        p.favorite = true;
        p.action = 'keep';
        chosen.push(p);
        encounters.add(p.group_index); bursts.add(p.burst_id);
      });
    }
  });
  sg.keepers = 0; sg.reviews = 0;
  sg.scene_groups.forEach(function(group) {
    group.photos.forEach(function(p) {
      if (p.action === 'keep') sg.keepers++;
      else if (p.action === 'review') sg.reviews++;
    });
  });
}

function cullFavoritesForSpecies(sg) {
  return cullGroupsForSpecies(sg).flatMap(function(g) { return g.photos; }).filter(function(p) { return p.favorite; });
}

function cullFavoritesHtml(sg, si) {
  var favorites = cullFavoritesForSpecies(sg);
  var ref = cullFavoriteReferences[sg.species];
  if (!favorites.some(function(p) { return p.photo_id === ref; })) {
    var pick = favorites.find(function(p) { return p.decision_source !== 'auto'; }) || favorites[0];
    cullFavoriteReferences[sg.species] = pick ? pick.photo_id : null;
  }
  var html = '<div class="cull-favorites"><div class="cull-favorites-heading">Favorites <span>' +
    favorites.length + '</span></div><p class="cull-hint">All your flagged picks, plus up to three suggestions per day. Select a favorite to use as your comparison reference.</p>';
  var days = Object.create(null);
  favorites.forEach(function(p) { (days[p.day] || (days[p.day] = [])).push(p); });
  Object.keys(days).sort().forEach(function(day) {
    html += '<div class="cull-favorite-day"><div class="cull-day-label">' + escapeHtml(day || 'Date unknown') + '</div><div class="pose-strip">';
    days[day].sort(function(a, b) {
      return (a.decision_source === 'auto') - (b.decision_source === 'auto') || b.quality - a.quality;
    }).forEach(function(p) {
      html += cullCardHtml(p, si, p.group_index, 'cullPose_' + si + '_' + p.group_index, true);
    });
    html += '</div></div>';
  });
  if (!favorites.length) html += '<p class="cull-hint">No favorites yet. Add your own picks from the photos below.</p>';
  return html + '</div>';
}

function cullCardHtml(photo, si, pi, poseKey, favorite) {
  var sg = cullData.species_groups[si];
  var id = photo.photo_id;
  var reference = favorite && cullFavoriteReferences[sg.species] === id;
  var cardClass = 'cull-card ' + photo.action + (favorite ? ' cull-favorite-card' : '') + (reference ? ' reference' : '');
  var icon = photo.action === 'keep' ? '&#10003;' : photo.action === 'reject' ? '&#10005;' : '?';
  var badge = '<button class="cull-card-action ' + photo.action + '-badge" aria-label="Change decision" title="Cycle favorite, undecided, rejected" onclick="event.stopPropagation(); toggleCullAction(' + si + ',' + pi + ',' + id + ')">' + icon + '</button>';
  var source = '';
  if (photo.decision_source === 'manual') {
    source = '<button class="cull-card-source manual" title="Your decision is preserved when suggestions change. Click to release it." onclick="event.stopPropagation(); releaseCullDecision(' + id + ')">Pinned</button>';
  } else if (photo.decision_source === 'confirmed') {
    source = '<span class="cull-card-source confirmed" title="Saved photo flag">Applied</span>';
  }
  var thumb = window.vireoThumbnailUrl ? window.vireoThumbnailUrl(photo) : '/thumbnails/' + id + '.jpg';
  var status = favorite ? (photo.decision_source === 'auto' ? 'Suggested' : 'Picked by you') : photo.action === 'reject' ? 'Rejected' : 'Undecided';
  var click = favorite ? 'selectCullReference(' + si + ',' + id + ')' : 'openCullComparison(' + si + ',' + id + ')';
  var html = '<div class="' + cardClass + '" data-photo-id="' + id + '" data-filename="' + escapeAttr(photo.filename) + '">' +
    '<div class="cull-card-image">' + badge + source + '<button class="cull-image-button" onclick="' + click + '" aria-label="' + escapeAttr((favorite ? 'Select reference: ' : 'Compare: ') + photo.filename) + '">' +
    '<img src="' + escapeAttr(thumb) + '" alt="' + escapeAttr(photo.filename) + '" loading="lazy"></button></div>' +
    '<div class="cull-card-info"><div class="cull-quality">' + status + (reference ? ' · Reference' : '') + '</div>' +
    '<div class="cull-filename">' + escapeHtml(photo.filename) + '</div><div class="cull-card-controls">';
  if (!favorite) html += '<button onclick="setCullAction(' + id + ',\'keep\')">Add favorite</button>';
  else html += '<button onclick="setCullAction(' + id + ',\'review\')">Remove favorite</button>';
  html += photo.action === 'reject' ? '<button onclick="setCullAction(' + id + ',\'review\')">Undecided</button>' : '<button onclick="setCullAction(' + id + ',\'reject\')">Reject</button>';
  html += '<button title="Open photo" onclick="openLightbox(' + id + ', this.closest(\'.cull-card\').dataset.filename, window.' + poseKey + ')">View</button>';
  return html + '</div></div></div>';
}

function selectCullReference(si, id) {
  cullFavoriteReferences[cullData.species_groups[si].species] = id;
  renderCulling();
}

function openCullComparison(si, id) {
  var sg = cullData.species_groups[si];
  var refs = cullFavoritesForSpecies(sg);
  if (!refs.length) {
    var p = cullGroupsForSpecies(sg).flatMap(function(g) { return g.photos; }).find(function(p) { return p.photo_id === id; });
    openLightbox(id, p.filename, cullGroupsForSpecies(sg).flatMap(function(g) {
      return g.photos.map(function(p) { return {id: p.photo_id, filename: p.filename}; });
    }));
    return;
  }
  cullCompareState = {species: sg.species, photoId: id};
  renderCullComparison();
}

function cullComparePhotos() {
  if (!cullCompareState || !cullData) return [];
  var sg = cullData.species_groups.find(function(g) { return g.species === cullCompareState.species; });
  return sg ? cullGroupsForSpecies(sg).flatMap(function(g) { return g.photos; }).filter(function(p) { return !p.favorite; }) : [];
}

async function renderCullComparison() {
  var seq = ++cullComparisonSeq;
  var state = cullCompareState;
  var sg = cullData && cullData.species_groups.find(function(g) { return state && g.species === state.species; });
  if (!sg) return browseCompare.close();
  var favorites = cullFavoritesForSpecies(sg);
  var candidates = cullComparePhotos();
  var index = candidates.findIndex(function(p) { return p.photo_id === state.photoId; });
  if (!favorites.length || index < 0) return browseCompare.close();
  var ref = cullFavoriteReferences[sg.species];
  if (!favorites.some(function(p) { return p.photo_id === ref; })) ref = favorites[0].photo_id;
  cullFavoriteReferences[sg.species] = ref;
  var select = document.getElementById('cullCompareReference');
  select.replaceChildren();
  favorites.forEach(function(p) {
    var option = document.createElement('option');
    option.value = p.photo_id;
    option.textContent = (p.decision_source === 'auto' ? 'Suggested: ' : 'Your pick: ') + p.filename;
    option.selected = p.photo_id === ref;
    select.appendChild(option);
  });
  await browseCompare.controller.open([ref, state.photoId]);
  if (seq !== cullComparisonSeq || cullCompareState !== state || !browseCompare.isOpen()) return;
  document.getElementById('browseCompareCount').textContent = (index + 1) + ' of ' + candidates.length + ' remaining';
  document.getElementById('browseComparePrev').disabled = index <= 0;
  document.getElementById('browseCompareNext').disabled = index >= candidates.length - 1;
}

function decideCullComparison(action) {
  if (!cullCompareState || cullOperationPending()) return;
  var candidates = cullComparePhotos();
  var index = candidates.findIndex(function(p) { return p.photo_id === cullCompareState.photoId; });
  var id = cullCompareState.photoId;
  setCullAction(id, action);
  candidates = cullComparePhotos();
  if (!candidates.length) return browseCompare.close();
  // Advance after each decision while preserving the reference photo.
  var nextIndex = action === 'keep' ? Math.min(index, candidates.length - 1) : Math.min(index + 1, candidates.length - 1);
  cullCompareState.photoId = candidates[nextIndex].photo_id;
  renderCullComparison();
}

function syncCullSourceFlags(results) {
  if (!cullSourceResults || !results) return;
  var flags = {};
  (results.photos || []).forEach(function(p) { flags[p.id] = p.flag; });
  (cullSourceResults.photos || []).forEach(function(p) {
    if (Object.prototype.hasOwnProperty.call(flags, p.id)) p.flag = flags[p.id];
  });
}

function removeCullPhotos(ids) {
  var removed = new Set(ids);
  [pipelineResults, cullSourceResults].forEach(function(results) {
    if (!results) return;
    results.photos = results.photos.filter(function(p) { return !removed.has(p.id); });
    var photoMap = {};
    results.photos.forEach(function(p) { photoMap[p.id] = p; });
    results.encounters.forEach(function(enc) {
      enc.photo_ids = enc.photo_ids.filter(function(id) { return !removed.has(id); });
      enc.photo_count = enc.photo_ids.length;
      enc.bursts = (enc.bursts || []).map(function(burst) {
        var ids = (burst.photo_ids || burst || []).filter(function(id) { return !removed.has(id); });
        if (Array.isArray(burst)) return ids;
        return Object.assign({}, burst, {photo_ids: ids});
      }).filter(function(burst) { return (burst.photo_ids || burst).length > 0; });
      enc.burst_count = enc.bursts.length;
      var times = enc.photo_ids.map(function(id) { return photoMap[id] && photoMap[id].timestamp; }).filter(Boolean).sort();
      enc.time_range = times.length ? [times[0], times[times.length - 1]] : [null, null];
    });
  });
  ids.forEach(function(id) { delete cullManualDecisions[id]; delete cullAppliedDecisions[id]; });
  cullDirty = unappliedPinCount() > 0;
  renderCullSpeciesOptions();
  rebuildCullDataFromPipeline();
}

function closeCullRejected() {
  var dialog = document.getElementById('cullRejectedDialog');
  if (dialog && dialog.open) dialog.close();
}

function reviewCullRejected() {
  if (cullOperationPending()) return;
  var rejected = cullData.species_groups.flatMap(function(sg) { return cullGroupsForSpecies(sg); }).flatMap(function(g) { return g.photos; }).filter(function(p) { return p.action === 'reject'; });
  var list = document.getElementById('cullRejectedPhotos');
  list.replaceChildren();
  rejected.forEach(function(p) {
    var card = document.createElement('div');
    var img = document.createElement('img');
    img.src = '/thumbnails/' + p.photo_id + '.jpg'; img.alt = p.filename;
    var name = document.createElement('div'); name.textContent = p.filename;
    var restore = document.createElement('button'); restore.textContent = 'Keep undecided';
    restore.addEventListener('click', function() { setCullAction(p.photo_id, 'review'); reviewCullRejected(); });
    card.append(img, name, restore); list.appendChild(card);
  });
  document.getElementById('cullDeleteRejected').disabled = !rejected.length;
  document.getElementById('cullRejectedCount').textContent = rejected.length + ' rejected photos in this selection';
  if (!document.getElementById('cullRejectedDialog').open) document.getElementById('cullRejectedDialog').showModal();
}

async function deleteCullRejected() {
  if (cullOperationPending()) return;
  var ids = cullData.species_groups.flatMap(function(sg) { return cullGroupsForSpecies(sg); }).flatMap(function(g) { return g.photos; }).filter(function(p) { return p.action === 'reject'; }).map(function(p) { return p.photo_id; });
  if (!ids.length) return;
  cullBusy = true; updateCullApplyButton();
  try {
    // Count companions before offering the existing file-deletion dialog.
    var counts = await safeFetch('/api/photos/companion-count', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({photo_ids: ids}),
    }, {toast: false});
    if (document.querySelector('#deleteModal.open')) {
      showToast('Close the existing delete dialog before deleting these photos.', 'info');
      return;
    }
    closeCullRejected();
    showDeleteDialog(ids, counts.count, function(data) {
      var failed = new Set(data.failed_photo_ids || []);
      removeCullPhotos(ids.filter(function(id) { return !failed.has(id); }));
    });
  } catch (error) {
    showToast('Could not prepare deletion: ' + error.message, 'error');
  } finally {
    cullBusy = false; updateCullApplyButton();
  }
}

function initializeCullFavorites() {
  var overlay = document.getElementById('browseCompareOverlay');
  overlay.querySelector('.browse-compare-title').textContent = 'Compare with favorite';
  overlay.querySelector('.browse-compare-hint').textContent += ' · linked zoom';
  var referenceSelect = document.createElement('select');
  referenceSelect.id = 'cullCompareReference';
  referenceSelect.setAttribute('aria-label', 'Favorite reference');
  overlay.querySelector('.browse-compare-title').after(referenceSelect);
  var actions = document.createElement('div');
  actions.className = 'cull-compare-actions';
  [['keep', 'Add to favorites'], ['review', 'Keep undecided'], ['reject', 'Reject']].forEach(function(entry) {
    var button = document.createElement('button');
    button.className = 'browse-compare-btn';
    button.textContent = entry[1];
    button.addEventListener('click', function() { decideCullComparison(entry[0]); });
    actions.appendChild(button);
  });
  overlay.querySelectorAll('.browse-compare-pane')[1].appendChild(actions);
  var controller = VireoBrowseCompare.create({
    findPhoto: function(id) { return (pipelineResults && pipelineResults.photos || []).find(function(p) { return p.id === id; }); },
    fetch: safeFetch, showToast: showToast, syncZoom: true,
  });
  browseCompare = {
    controller: controller,
    isOpen: controller.isOpen,
    resetViews: controller.resetViews,
    close: function(e) { controller.close(e); if (!controller.isOpen()) cullCompareState = null; },
    step: function(delta) {
      if (!cullCompareState) return;
      var photos = cullComparePhotos();
      var index = photos.findIndex(function(p) { return p.photo_id === cullCompareState.photoId; });
      var next = Math.max(0, Math.min(photos.length - 1, index + delta));
      if (photos[next]) { cullCompareState.photoId = photos[next].photo_id; renderCullComparison(); }
    },
  };
  document.getElementById('cullCompareReference').addEventListener('change', function(e) {
    if (!cullCompareState) return;
    cullFavoriteReferences[cullCompareState.species] = Number(e.target.value);
    renderCullComparison();
  });
  document.addEventListener('keydown', function(e) {
    if (!controller.isOpen() || (e.target.closest && e.target.closest('input, textarea, select')) || e.target.isContentEditable || e.altKey || e.ctrlKey || e.metaKey || e.shiftKey) return;
    if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
      e.preventDefault(); browseCompare.step(e.key === 'ArrowRight' ? 1 : -1);
    }
  });
  document.addEventListener('vireo:edit-history-busy', updateCullApplyButton);
  new MutationObserver(updateCullApplyButton).observe(document.getElementById('deleteModal'), {attributes: true, attributeFilter: ['class']});
  document.addEventListener('lightbox:flagchanged', function(e) {
    var id = e.detail.photoId;
    [pipelineResults, cullSourceResults].forEach(function(results) {
      var p = results && results.photos.find(function(p) { return p.id === id; });
      if (p) p.flag = e.detail.flag;
    });
    delete cullManualDecisions[id]; delete cullAppliedDecisions[id];
    cullDirty = unappliedPinCount() > 0;
    rebuildCullDataFromPipeline();
  });
  document.addEventListener('lightbox:photodeleted', function(e) { removeCullPhotos([e.detail.photoId]); });
}

async function recomputeCulling(endpoint, statusText, sourceOnly) {
  if (cullBusy || (window.vireoHistoryBusy && window.vireoHistoryBusy())) {
    showToast('Wait for the current edit to finish before recomputing culling.', 'info');
    return;
  }
  sourceOnly = sourceOnly || cullSourceCollectionId !== selectedCollectionId;
  var scoped = !sourceOnly && hasCullFilters();
  if (scoped && !cullScopePhotoIds().length) {
    document.getElementById('cullStatus').textContent = 'No photos match the selected species and dates.';
    return;
  }
  var seq = ++cullAnalysisSeq;
  if (sourceOnly && hasCullFilters()) cullScopeFailed = true;
  cullAnalysisPending++;
  updateCullApplyButton();
  try {
    var config = Object.assign({}, getGroupingConfig(), getScoringConfig());
    var data = await safeFetch(endpoint, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(pipelineBody(config, sourceOnly)),
    }, {toast: false});
    if (seq === cullAnalysisSeq) {
      setPipelineResults(data, statusText, scoped);
      if (!scoped) cullSourceCollectionId = selectedCollectionId;
      if (sourceOnly && hasCullFilters()) await recomputeCulling(endpoint, statusText);
      else cullScopeFailed = false;
    }
  } catch (error) {
    if (seq === cullAnalysisSeq) {
      var status = document.getElementById('cullStatus');
      status.textContent = 'Error: ' + error.message;
      status.style.color = 'var(--danger)';
    }
  } finally {
    cullAnalysisPending--;
    updateCullApplyButton();
    if (!cullAnalysisPending) {
      var indicator = document.getElementById('reflowIndicator');
      if (indicator) indicator.style.display = 'none';
    }
  }
}

/* Cull's scope, favorites and comparison presentation. Decisions are pinned
 * by cull.html and saved through its existing undoable Apply operation. */
/** @type {CullSourceResults | null} */
var cullSourceResults = null;
/** @type {number | null} */
var cullSourceCollectionId = null;
/** @type {Set<string>} */
var cullSelectedSpecies = new Set();
var cullScopeFailed = false;
/** @type {Record<string, number | null>} */
var cullFavoriteReferences = Object.create(null);
/** @type {{species: string, photoId: number} | null} */
var cullCompareState = null;
var cullComparisonSeq = 0;
/** @type {CullCompare | null} */
var browseCompare = null;
VireoBrowseCompare.bindControls(document, function() { return browseCompare; });

function hasCullFilters() {
  return cullSelectedSpecies.size > 0 || !!/** @type {HTMLInputElement} */ (document.getElementById('cullDateFrom')).value ||
    !!/** @type {HTMLInputElement} */ (document.getElementById('cullDateTo')).value;
}

/** @param {CullSourcePhoto} photo @param {CullEncounter | null} [encounter] @param {Record<number, CullSourcePhoto>} [photoMap] */
function cullPhotoSpecies(photo, encounter, photoMap) {
  if (photo.confirmed_species) return speciesName(photo.confirmed_species);
  if (photo.species_top5 && photo.species_top5.length) return topSpeciesForPhoto(photo);
  return speciesForEncounter(encounter || {}, photoMap || {});
}

/** @param {CullSourcePhoto} photo */
function cullPhotoDay(photo) {
  // EXIF dates are camera-local dates; UTC conversion would move photos
  // around midnight into a different day. Missing dates never pass a range.
  return /^\d{4}-\d{2}-\d{2}/.test(photo.timestamp || '') ? (photo.timestamp || '').substring(0, 10) : '';
}

/** @param {CullSourcePhoto} photo @param {string} species */
function cullPhotoMatches(photo, species) {
  if (cullSelectedSpecies.size && !cullSelectedSpecies.has(species)) return false;
  return cullPhotoMatchesDate(photo);
}

/** @param {CullSourcePhoto} photo */
function cullPhotoMatchesDate(photo) {
  var from = /** @type {HTMLInputElement} */ (document.getElementById('cullDateFrom')).value;
  var to = /** @type {HTMLInputElement} */ (document.getElementById('cullDateTo')).value;
  var day = cullPhotoDay(photo);
  if ((from || to) && !day) return false;
  return (!from || day >= from) && (!to || day <= to);
}

function cullSourcePhotos() {
  var results = cullSourceResults;
  if (!results) return [];
  /** @type {Record<number, CullSourcePhoto>} */
  var map = {};
  (results.photos || []).forEach(function(p) { map[p.id] = p; });
  var seen = new Set();
  /** @type {{photo: CullSourcePhoto, species: string}[]} */
  var photos = [];
  (results.encounters || []).forEach(function(enc) {
    (enc.photo_ids || []).forEach(function(id) {
      var photo = map[id];
      if (!photo || seen.has(id)) return;
      seen.add(id);
      photos.push({photo: photo, species: cullPhotoSpecies(photo, enc, map)});
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
  /** @type {Record<string, number>} */
  var counts = Object.create(null);
  cullSourcePhotos().forEach(function(entry) {
    if (cullPhotoMatchesDate(entry.photo)) counts[entry.species] = (counts[entry.species] || 0) + 1;
  });
  cullSelectedSpecies.forEach(function(name) { if (!counts[name]) counts[name] = 0; });
  var search = /** @type {HTMLInputElement} */ (document.getElementById('cullSpeciesSearch')).value.toLocaleLowerCase();
  var list = /** @type {HTMLElement} */ (document.getElementById('cullSpeciesOptions'));
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
  /** @type {HTMLElement} */ (document.getElementById('cullSpeciesSummary')).textContent = cullSelectedSpecies.size
    ? Array.from(cullSelectedSpecies).join(', ') : 'All species';
}

function renderCullScope() {
  var names = cullSelectedSpecies.size ? Array.from(cullSelectedSpecies).join(', ') : 'All species';
  var from = /** @type {HTMLInputElement} */ (document.getElementById('cullDateFrom')).value;
  var to = /** @type {HTMLInputElement} */ (document.getElementById('cullDateTo')).value;
  var dates = from && to ? from + ' through ' + to : from ? 'From ' + from : to ? 'Through ' + to : 'All dates';
  /** @type {HTMLElement} */ (document.getElementById('cullScope')).textContent = names + ' · ' + dates + ' · ' +
    (cullData ? cullData.total_photos : 0) + ' photos with pipeline results';
}

function clearCullSpecies() {
  cullSelectedSpecies.clear();
  onCullScopeChange();
}

function clearCullFilters() {
  cullSelectedSpecies.clear();
  /** @type {HTMLInputElement} */ (document.getElementById('cullDateFrom')).value = '';
  /** @type {HTMLInputElement} */ (document.getElementById('cullDateTo')).value = '';
  onCullScopeChange();
}

function onCullScopeChange() {
  // Invalidate an older request before any new request starts, including an
  // empty scope (which needs no server request at all).
  ++cullAnalysisSeq;
  if (_reflowTimer) clearTimeout(_reflowTimer); _reflowTimer = null;
  if (_regroupTimer) clearTimeout(_regroupTimer); _regroupTimer = null;
  if (browseCompare) browseCompare.close();
  closeCullRejected();
  expandedSpecies = {};
  renderCullSpeciesOptions();
  pipelineResults = cullSourceResults;
  rebuildCullDataFromPipeline();
  var from = /** @type {HTMLInputElement} */ (document.getElementById('cullDateFrom')).value;
  var to = /** @type {HTMLInputElement} */ (document.getElementById('cullDateTo')).value;
  if (from && to && from > to) {
    cullScopeFailed = true;
    /** @type {HTMLElement} */ (document.getElementById('cullStatus')).textContent = 'Choose an end date on or after the start date.';
    updateCullApplyButton();
    return;
  }
  cullScopeFailed = false;
  if (cullSourceCollectionId !== selectedCollectionId || (cullSourceResults && cullScopePhotoIds().length)) { cullScopeFailed = true; runCulling(); }
  else updateCullApplyButton();
}

/** @param {CullEncounter} enc @param {number} id @param {number} encounterIndex */
function cullBurstId(enc, id, encounterIndex) {
  var burst = (enc.bursts || []).findIndex(function(b) { return (Array.isArray(b) ? b : b.photo_ids).indexOf(id) !== -1; });
  return encounterIndex + ':' + burst;
}

/** @param {CullSpeciesGroup} sg */
function assignCullFavorites(sg) {
  /** @type {Record<string, CullDisplayPhoto[]>} */
  var days = Object.create(null);
  sg.scene_groups.forEach(function(group, gi) {
    group.photos.forEach(function(photo) {
      photo.group_index = gi;
      photo.favorite = photo.action === 'keep' && photo.decision_source !== 'auto';
      (days[photo.day] || (days[photo.day] = [])).push(photo);
    });
  });
  Object.keys(days).forEach(function(day) {
    var dayPhotos = days[day] || [];
    var candidates = dayPhotos.filter(function(p) {
      return p.decision_source === 'auto' && p.suggested_action === 'keep';
    }).sort(function(a, b) { return b.quality - a.quality || a.photo_id - b.photo_id; });
    // Give different encounters, then different bursts, a chance before
    // filling the shortlist with neighboring frames of the same moment.
    var encounters = new Set();
    var bursts = new Set();
    dayPhotos.filter(function(p) { return p.favorite; }).forEach(function(p) {
      encounters.add(p.group_index); bursts.add(p.burst_id);
    });
    /** @type {CullDisplayPhoto[]} */
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

/** @param {CullSpeciesGroup} sg */
function cullFavoritesForSpecies(sg) {
  return cullGroupsForSpecies(sg).flatMap(function(g) { return g.photos; }).filter(function(p) { return p.favorite; });
}

/** @param {CullSpeciesGroup} sg @param {number} si */
function cullFavoritesHtml(sg, si) {
  var favorites = cullFavoritesForSpecies(sg);
  var ref = cullFavoriteReferences[sg.species];
  if (!favorites.some(function(p) { return p.photo_id === ref; })) {
    var pick = favorites.find(function(p) { return p.decision_source !== 'auto'; }) || favorites[0];
    cullFavoriteReferences[sg.species] = pick ? pick.photo_id : null;
  }
  var html = '<div class="cull-favorites"><div class="cull-favorites-heading">Favorites <span>' +
    favorites.length + '</span></div><p class="cull-hint">All your flagged picks, plus up to three suggestions per day. Select a favorite to use as your comparison reference.</p>';
  /** @type {Record<string, CullDisplayPhoto[]>} */
  var days = Object.create(null);
  favorites.forEach(function(p) { (days[p.day] || (days[p.day] = [])).push(p); });
  Object.keys(days).sort().forEach(function(day) {
    html += '<div class="cull-favorite-day"><div class="cull-day-label">' + escapeHtml(day || 'Date unknown') + '</div><div class="pose-strip">';
    (days[day] || []).sort(function(a, b) {
      return Number(a.decision_source === 'auto') - Number(b.decision_source === 'auto') || b.quality - a.quality;
    }).forEach(function(p) {
      html += cullCardHtml(p, si, p.group_index, 'cullPose_' + si + '_' + p.group_index, true);
    });
    html += '</div></div>';
  });
  if (!favorites.length) html += '<p class="cull-hint">No favorites yet. Add your own picks from the photos below.</p>';
  return html + '</div>';
}

/** @param {CullDisplayPhoto} photo @param {number} si @param {number} pi @param {string} poseKey @param {boolean} favorite */
function cullCardHtml(photo, si, pi, poseKey, favorite) {
  var sg = cullData && cullData.species_groups[si];
  if (!sg) return '';
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

/** @param {number} si @param {number} id */
function selectCullReference(si, id) {
  var sg = cullData && cullData.species_groups[si];
  if (!sg) return;
  cullFavoriteReferences[sg.species] = id;
  renderCulling();
}

/** @param {number} si @param {number} id */
function openCullComparison(si, id) {
  var sg = cullData && cullData.species_groups[si];
  if (!sg) return;
  var refs = cullFavoritesForSpecies(sg);
  if (!refs.length) {
    var p = cullGroupsForSpecies(sg).flatMap(function(g) { return g.photos; }).find(function(p) { return p.photo_id === id; });
    if (!p) return;
    openLightbox(id, p.filename, cullGroupsForSpecies(sg).flatMap(function(g) {
      return g.photos.map(function(p) { return {id: p.photo_id, filename: p.filename}; });
    }));
    return;
  }
  cullCompareState = {species: sg.species, photoId: id};
  renderCullComparison();
}

function cullComparePhotos() {
  const state = cullCompareState;
  if (!state || !cullData) return [];
  var sg = cullData.species_groups.find(function(g) { return g.species === state.species; });
  return sg ? cullGroupsForSpecies(sg).flatMap(function(g) { return g.photos; }).filter(function(p) { return !p.favorite; }) : [];
}

async function renderCullComparison() {
  var seq = ++cullComparisonSeq;
  const state = cullCompareState;
  const compare = browseCompare;
  if (!state || !compare) return;
  const sg = cullData && cullData.species_groups.find(function(g) { return state && g.species === state.species; });
  if (!sg) return compare.close();
  var favorites = cullFavoritesForSpecies(sg);
  var candidates = cullComparePhotos();
  var index = candidates.findIndex(function(p) { return p.photo_id === state.photoId; });
  if (!favorites.length || index < 0) return compare.close();
  var favorite = favorites.find(function(p) { return p.photo_id === cullFavoriteReferences[sg.species]; }) || favorites[0];
  if (!favorite) return compare.close();
  var ref = favorite.photo_id;
  cullFavoriteReferences[sg.species] = ref;
  var select = /** @type {HTMLSelectElement} */ (document.getElementById('cullCompareReference'));
  select.replaceChildren();
  favorites.forEach(function(p) {
    var option = document.createElement('option');
    option.value = String(p.photo_id);
    option.textContent = (p.decision_source === 'auto' ? 'Suggested: ' : 'Your pick: ') + p.filename;
    option.selected = p.photo_id === ref;
    select.appendChild(option);
  });
  await compare.controller.open([ref, state.photoId]);
  if (seq !== cullComparisonSeq || cullCompareState !== state || !compare.isOpen()) return;
  /** @type {HTMLElement} */ (document.getElementById('browseCompareCount')).textContent = (index + 1) + ' of ' + candidates.length + ' remaining';
  /** @type {HTMLButtonElement} */ (document.getElementById('browseComparePrev')).disabled = index <= 0;
  /** @type {HTMLButtonElement} */ (document.getElementById('browseCompareNext')).disabled = index >= candidates.length - 1;
}

/** @param {CullAction} action */
function decideCullComparison(action) {
  const state = cullCompareState;
  if (!state || !browseCompare || cullOperationPending()) return;
  var candidates = cullComparePhotos();
  var index = candidates.findIndex(function(p) { return p.photo_id === state.photoId; });
  var id = state.photoId;
  setCullAction(id, action);
  candidates = cullComparePhotos();
  if (!candidates.length) return browseCompare.close();
  // Advance after each decision while preserving the reference photo.
  var nextIndex = action === 'keep' ? Math.min(index, candidates.length - 1) : Math.min(index + 1, candidates.length - 1);
  var nextPhoto = candidates[nextIndex];
  if (nextPhoto) state.photoId = nextPhoto.photo_id;
  renderCullComparison();
}

/** @param {CullSourceResults | null} results */
function syncCullSourceFlags(results) {
  if (!cullSourceResults || !results) return;
  /** @type {Record<number, string | undefined>} */
  var flags = {};
  (results.photos || []).forEach(function(p) { flags[p.id] = p.flag; });
  (cullSourceResults.photos || []).forEach(function(p) {
    if (Object.prototype.hasOwnProperty.call(flags, p.id)) p.flag = flags[p.id];
  });
}

/** @param {number[]} ids */
function removeCullPhotos(ids) {
  var removed = new Set(ids);
  [pipelineResults, cullSourceResults].forEach(function(results) {
    if (!results) return;
    results.photos = results.photos.filter(function(p) { return !removed.has(p.id); });
    /** @type {Record<number, CullSourcePhoto>} */
    var photoMap = {};
    results.photos.forEach(function(p) { photoMap[p.id] = p; });
    results.encounters.forEach(function(enc) {
      enc.photo_ids = enc.photo_ids.filter(function(id) { return !removed.has(id); });
      enc.photo_count = enc.photo_ids.length;
      enc.bursts = (enc.bursts || []).map(function(burst) {
        var ids = (Array.isArray(burst) ? burst : burst.photo_ids).filter(function(id) { return !removed.has(id); });
        if (Array.isArray(burst)) return ids;
        return Object.assign({}, burst, {photo_ids: ids});
      }).filter(function(burst) { return (Array.isArray(burst) ? burst : burst.photo_ids).length > 0; });
      enc.burst_count = enc.bursts.length;
      var times = enc.photo_ids.map(function(id) { return photoMap[id]?.timestamp; }).filter(function(time) { return typeof time === 'string'; }).sort();
      enc.time_range = times.length ? [times[0] || null, times[times.length - 1] || null] : [null, null];
    });
  });
  ids.forEach(function(id) { delete cullManualDecisions[id]; delete cullAppliedDecisions[id]; });
  cullDirty = unappliedPinCount() > 0;
  renderCullSpeciesOptions();
  rebuildCullDataFromPipeline();
}

function closeCullRejected() {
  var dialog = /** @type {HTMLDialogElement} */ (document.getElementById('cullRejectedDialog'));
  if (dialog && dialog.open) dialog.close();
}

function reviewCullRejected() {
  if (cullOperationPending()) return;
  var rejected = (cullData ? cullData.species_groups : []).flatMap(function(sg) { return cullGroupsForSpecies(sg); }).flatMap(function(g) { return g.photos; }).filter(function(p) { return p.action === 'reject'; });
  var list = /** @type {HTMLElement} */ (document.getElementById('cullRejectedPhotos'));
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
  /** @type {HTMLButtonElement} */ (document.getElementById('cullDeleteRejected')).disabled = !rejected.length;
  /** @type {HTMLElement} */ (document.getElementById('cullRejectedCount')).textContent = rejected.length + ' rejected photos in this selection';
  if (!/** @type {HTMLDialogElement} */ (document.getElementById('cullRejectedDialog')).open) /** @type {HTMLDialogElement} */ (document.getElementById('cullRejectedDialog')).showModal();
}

async function deleteCullRejected() {
  if (cullOperationPending()) return;
  var ids = (cullData ? cullData.species_groups : []).flatMap(function(sg) { return cullGroupsForSpecies(sg); }).flatMap(function(g) { return g.photos; }).filter(function(p) { return p.action === 'reject'; }).map(function(p) { return p.photo_id; });
  if (!ids.length) return;
  cullBusy = true; updateCullApplyButton();
  try {
    // Count companions before offering the existing file-deletion dialog.
    /** @type {{count: number}} */
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
    showToast('Could not prepare deletion: ' + (error instanceof Error ? error.message : String(error)), 'error');
  } finally {
    cullBusy = false; updateCullApplyButton();
  }
}

function initializeCullFavorites() {
  var overlay = /** @type {HTMLElement} */ (document.getElementById('browseCompareOverlay'));
  /** @type {HTMLElement} */ (overlay.querySelector('.browse-compare-title')).textContent = 'Compare with favorite';
  /** @type {HTMLElement} */ (overlay.querySelector('.browse-compare-hint')).textContent += ' · linked zoom';
  var referenceSelect = document.createElement('select');
  referenceSelect.id = 'cullCompareReference';
  referenceSelect.setAttribute('aria-label', 'Favorite reference');
  /** @type {HTMLElement} */ (overlay.querySelector('.browse-compare-title')).after(referenceSelect);
  var actions = document.createElement('div');
  actions.className = 'cull-compare-actions';
  [['keep', 'Add to favorites'], ['review', 'Keep undecided'], ['reject', 'Reject']].forEach(function(entry) {
    var button = document.createElement('button');
    button.className = 'browse-compare-btn';
    button.textContent = entry[1] || '';
    button.addEventListener('click', function() { if (entry[0] === 'keep' || entry[0] === 'review' || entry[0] === 'reject') decideCullComparison(entry[0]); });
    actions.appendChild(button);
  });
  overlay.querySelectorAll('.browse-compare-pane')[1]?.appendChild(actions);
  var controller = VireoBrowseCompare.create({
    findPhoto: function(id) { return (pipelineResults && pipelineResults.photos || []).find(function(p) { return p.id === id; }); },
    fetch: safeFetch, showToast: showToast, syncZoom: true,
  });
  const compare = browseCompare = {
    controller: controller,
    isOpen: controller.isOpen,
    resetViews: controller.resetViews,
    /** @param {Event} [e] */
    close: function(e) { controller.close(e); if (!controller.isOpen()) cullCompareState = null; },
    /** @param {number} delta */
    step: function(delta) {
      const state = cullCompareState;
      if (!state) return;
      var photos = cullComparePhotos();
      var index = photos.findIndex(function(p) { return p.photo_id === state.photoId; });
      var next = Math.max(0, Math.min(photos.length - 1, index + delta));
      var nextPhoto = photos[next];
      if (nextPhoto) { state.photoId = nextPhoto.photo_id; renderCullComparison(); }
    },
  };
  /** @type {HTMLSelectElement} */ (document.getElementById('cullCompareReference')).addEventListener('change', function(e) {
    if (!cullCompareState) return;
    cullFavoriteReferences[cullCompareState.species] = Number(/** @type {HTMLSelectElement} */ (e.target).value);
    renderCullComparison();
  });
  document.addEventListener('keydown', function(e) {
    if (!controller.isOpen() || (e.target instanceof Element && e.target.closest('input, textarea, select')) || (e.target instanceof HTMLElement && e.target.isContentEditable) || e.altKey || e.ctrlKey || e.metaKey || e.shiftKey) return;
    if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
      e.preventDefault(); compare.step(e.key === 'ArrowRight' ? 1 : -1);
    }
  });
  document.addEventListener('vireo:edit-history-busy', updateCullApplyButton);
  new MutationObserver(updateCullApplyButton).observe(/** @type {HTMLElement} */ (document.getElementById('deleteModal')), {attributes: true, attributeFilter: ['class']});
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

/** @param {string} endpoint @param {string} statusText @param {boolean} [sourceOnly] */
async function recomputeCulling(endpoint, statusText, sourceOnly) {
  if (cullBusy || (window.vireoHistoryBusy && window.vireoHistoryBusy())) {
    showToast('Wait for the current edit to finish before recomputing culling.', 'info');
    return;
  }
  sourceOnly = sourceOnly || cullSourceCollectionId !== selectedCollectionId;
  var scoped = !sourceOnly && hasCullFilters();
  if (scoped && !cullScopePhotoIds().length) {
    /** @type {HTMLElement} */ (document.getElementById('cullStatus')).textContent = 'No photos match the selected species and dates.';
    return;
  }
  var seq = ++cullAnalysisSeq;
  if (sourceOnly && hasCullFilters()) cullScopeFailed = true;
  cullAnalysisPending++;
  updateCullApplyButton();
  try {
    var config = Object.assign({}, getGroupingConfig(), getScoringConfig());
    /** @type {CullSourceResults} */
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
      var status = /** @type {HTMLElement} */ (document.getElementById('cullStatus'));
      status.textContent = 'Error: ' + (error instanceof Error ? error.message : String(error));
      status.style.color = 'var(--danger)';
    }
  } finally {
    cullAnalysisPending--;
    updateCullApplyButton();
    if (!cullAnalysisPending) {
      var indicator = /** @type {HTMLElement} */ (document.getElementById('reflowIndicator'));
      if (indicator) indicator.style.display = 'none';
    }
  }
}

/* Browse: multi-selection panel (edits paste, compare, keyword and wildlife state).
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

function updateSelectionPanel(ids) {
  var panel = document.getElementById('selectionPanel');
  if (!panel) return;
  if (ids.length <= 1) {
    var dc = document.getElementById('detailContent');
    if (dc) dc.classList.remove('batch-mode');
    // Clear batch-only Mixed markers immediately so they can't linger while the
    // single-photo detail fetch is in flight.
    _batchToggleMixed('ratingMixed', false);
    _batchToggleMixed('flagMixed', false);
    _batchToggleMixed('colorMixed', false);
    var wasVisible = !panel.classList.contains('hidden');
    panel.classList.add('hidden');
    Vireo.browse.panelRequests.keywords.invalidate();
    selectionKeywordMissingById = {};
    selectionKeywordPresentById = {};
    selectionKeywordNameById = {};
    var list = document.getElementById('selectionKeywordSuggestions');
    if (list) list.innerHTML = '';
    // Same teardown for the prediction rows — invalidation drops any
    // in-flight selection response so it can't paint into the panel the user
    // has just collapsed back to a single photo.
    Vireo.browse.panelRequests.predictions.invalidate();
    selectionPredictionAcceptableById = {};
    selectionPredictionSpeciesByIdx = {};
    selectionPredictionPhotoIdsByIdx = {};
    var predList = document.getElementById('selectionPredictions');
    if (predList) predList.innerHTML = '';
    renderSelectionWildlifeState([]);
    if (wasVisible && selectedPhotoId) {
      loadDetail(selectedPhotoId);
    } else if (wasVisible) {
      var detail = document.getElementById('detailContent');
      var summary = document.getElementById('summaryPanel');
      if (detail) detail.classList.remove('visible');
      if (summary) summary.classList.remove('hidden');
      loadSummary();
    }
    return;
  }

  Vireo.browse.panelRequests.detailPredictions.invalidate();
  var detail = document.getElementById('detailContent');
  var summary = document.getElementById('summaryPanel');
  if (summary) summary.classList.add('hidden');
  // Reuse the detail panel as a batch inspector for the whole selection:
  // Rating/Flag/Color/Location act on all selected photos, single-photo-only
  // sections are hidden via .batch-mode CSS.
  if (detail) {
    detail.classList.add('visible');
    detail.classList.add('batch-mode');
  }
  panel.classList.remove('hidden');
  document.getElementById('selectionCount').textContent =
    ids.length.toLocaleString() + ' photos selected' + browseSelectionStackNote(ids);
  updatePasteEditSection(ids);
  renderSelectionWildlifeState(ids);
  // Keep any EXIF suggestion fetched for the anchor photo: Accept is
  // batch-aware (_locationApplyPhotoIds), so growing the selection is the
  // designed way to apply one suggestion to many photos.
  renderBatchInspector(ids, { preserveExifSuggestion: true });
  if (ids.length > 1000) {
    Vireo.browse.panelRequests.keywords.invalidate();
    selectionKeywordMissingById = {};
    selectionKeywordPresentById = {};
    selectionKeywordNameById = {};
    var list = document.getElementById('selectionKeywordSuggestions');
    if (list) {
      list.innerHTML = '<div class="selection-empty">Keyword suggestions are available for selections of 1,000 photos or fewer.</div>';
    }
    // Say the cap out loud here too, rather than leaving an empty
    // Predictions box that reads as "nothing predicted".
    Vireo.browse.panelRequests.predictions.invalidate();
    selectionPredictionAcceptableById = {};
    selectionPredictionSpeciesByIdx = {};
    selectionPredictionPhotoIdsByIdx = {};
    var predList = document.getElementById('selectionPredictions');
    if (predList) {
      predList.innerHTML = '<div class="selection-empty">Predictions are available for selections of 1,000 photos or fewer.</div>';
    }
    return;
  }
  loadSelectionKeywordSuggestions(ids);
  loadSelectionPredictions(ids);
}

function updatePasteEditSection(ids) {
  var section = document.getElementById('pasteEditSection');
  var hint = document.getElementById('pasteEditHint');
  if (!section) return;
  var copied = window.vireoEditNav ? window.vireoEditNav.getCopiedRecipe() : null;
  if (!copied || !copied.recipe) {
    section.hidden = true;
    return;
  }
  section.hidden = false;
  if (hint) {
    var n = ids.length.toLocaleString();
    hint.textContent = copied.source
      ? 'Copied from ' + copied.source + ' — applies to ' + n + ' selected photos.'
      : 'Applies the copied development settings to ' + n + ' selected photos.';
  }
}

function _browseHasDevelopmentSettings(recipe) {
  return !!(recipe && typeof recipe === 'object' && Object.keys(recipe).some(function(key) {
    return key !== 'version';
  }));
}

var _browseDevelopmentCopySeq = 0;

async function copyDevelopmentSettingsFromPhoto(photoId) {
  var photo = photos.find(function(candidate) { return candidate.id === Number(photoId); });
  if (!photoId || !window.vireoEditNav) return;
  var copySeq = ++_browseDevelopmentCopySeq;
  var copyToken = typeof window.vireoEditNav.beginCopiedRecipe === 'function'
    ? await window.vireoEditNav.beginCopiedRecipe()
    : null;
  try {
    // Fetch the authoritative recipe so a long-lived Browse tab cannot copy
    // settings that were changed in another surface since its last refresh.
    var data = await safeFetch('/api/photos/' + photoId + '/edit-recipe', {}, {toast: false});
    if (copySeq !== _browseDevelopmentCopySeq) return;
    if (
      copyToken &&
      typeof window.vireoEditNav.isCopiedRecipeCurrent === 'function' &&
      !window.vireoEditNav.isCopiedRecipeCurrent(copyToken)
    ) return;
    var recipe = data && data.recipe;
    if (!_browseHasDevelopmentSettings(recipe)) {
      if (typeof window.vireoEditNav.cancelCopiedRecipe === 'function') {
        if (!await window.vireoEditNav.cancelCopiedRecipe(copyToken)) return;
      }
      showToast('This photo has no development settings to copy', 'error');
      return;
    }
    var meta = {
      source: (photo && photo.filename) || null,
      at: Date.now(),
    };
    if (
      copyToken &&
      typeof window.vireoEditNav.setCopiedRecipeIfCurrent === 'function'
    ) {
      if (!await window.vireoEditNav.setCopiedRecipeIfCurrent(recipe, meta, copyToken)) return;
    } else {
      await window.vireoEditNav.setCopiedRecipe(recipe, meta);
    }
    showToast('Development settings copied', 'success');
    updatePasteEditSection(getActiveSelection());
  } catch (e) {
    if (copySeq !== _browseDevelopmentCopySeq) return;
    if (
      copyToken &&
      typeof window.vireoEditNav.isCopiedRecipeCurrent === 'function' &&
      !window.vireoEditNav.isCopiedRecipeCurrent(copyToken)
    ) return;
    if (typeof window.vireoEditNav.cancelCopiedRecipe === 'function') {
      if (!await window.vireoEditNav.cancelCopiedRecipe(copyToken)) return;
    }
    showToast(e.message || 'Could not copy development settings', 'error');
  }
}

async function openBatchDevelopmentEditor() {
  try { await VireoBatchEdits.open(getActiveSelection().slice()); }
  catch (error) { showToast(error.message || 'Could not open batch editor', 'error'); }
}

var _browseDevelopmentPasteInFlight = false;

async function pasteEditSettingsToSelection() {
  if (_browseDevelopmentPasteInFlight) {
    showToast('A development settings paste is already running.', 'warning');
    return;
  }
  var ids = getActiveSelection();
  if (ids.length < 1) { showToast('Select photos first.', 'error'); return; }
  var copied = window.vireoEditNav ? window.vireoEditNav.getCopiedRecipe() : null;
  if (!copied || !copied.recipe) {
    showToast('Copy development settings from a photo first.', 'error');
    return;
  }
  var btn = document.getElementById('pasteEditBtn');
  _browseDevelopmentPasteInFlight = true;
  if (btn) btn.disabled = true;
  try {
    var data = await VireoBatchEdits.paste(ids.slice());
    if (data) showToast(VireoBatchEdits.resultMessage(data), data.skipped.length ? 'warning' : 'success');
  } catch (e) {
    showToast(e.message || 'Could not paste development settings', 'error');
  } finally {
    _browseDevelopmentPasteInFlight = false;
    if (btn) btn.disabled = false;
  }
}

const browseCompare = VireoBrowseCompare.create({
  findPhoto: findBrowsePhoto,
  fetch: safeFetch,
  showToast: showToast
});

function openBrowseCompare() {
  return browseCompare.open(getActiveSelection());
}

function findBrowsePhoto(id) {
  var topLevel = photos.find(function(p) { return p.id === id; });
  if (topLevel) return topLevel;
  var coverIds = Object.keys(browseStackMembers);
  for (var i = 0; i < coverIds.length; i++) {
    var member = (browseStackMembers[coverIds[i]] || []).find(function(p) {
      return p.id === id;
    });
    if (member) return member;
  }
  return null;
}

async function loadSelectionKeywordSuggestions(ids) {
  if (ids.length > 1000) return;
  var request = Vireo.browse.panelRequests.keywords.begin(selectionIdsKey(ids));
  if (!request) return;
  var list = document.getElementById('selectionKeywordSuggestions');
  if (list) list.innerHTML = '<div class="selection-empty">Checking selected keywords...</div>';

  try {
    var data = await safeFetch('/api/selection/keyword-suggestions', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids}),
    }, { toast: false });
    if (!request.isCurrent()) return;
    renderSelectionKeywordSuggestions(data.keywords || [], data.selected_count || ids.length);
  } catch(e) {
    if (request.fail() && list) {
      list.innerHTML = '<div class="selection-empty">Could not load keyword suggestions.</div>';
    }
  }
}

function renderSelectionKeywordSuggestions(keywords, selectedCount) {
  var list = document.getElementById('selectionKeywordSuggestions');
  if (!list) return;
  if (!keywords.length) {
    selectionKeywordMissingById = {};
    selectionKeywordPresentById = {};
    selectionKeywordNameById = {};
    list.innerHTML = '<div class="selection-empty">No keywords on selected photos.</div>';
    return;
  }

  var html = '';
  selectionKeywordMissingById = {};
  selectionKeywordPresentById = {};
  selectionKeywordNameById = {};
  var groupDefinitions = [
    { type: 'taxonomy', label: 'Species' },
    { type: 'location', label: 'Locations' },
    { type: 'individual', label: 'Individuals' },
    { type: 'genre', label: 'Genres' },
    { type: 'general', label: 'Keywords' }
  ];
  var keywordsByType = {};
  keywords.forEach(function(k) {
    var type = k.type || 'general';
    if (!keywordsByType[type]) keywordsByType[type] = [];
    keywordsByType[type].push(k);
  });
  function renderKeywordRow(k) {
    var missing = k.missing_count || 0;
    var count = k.count || 0;
    selectionKeywordMissingById[String(k.id)] = k.missing_photo_ids || [];
    selectionKeywordPresentById[String(k.id)] = k.present_photo_ids || [];
    selectionKeywordNameById[String(k.id)] = k.name;
    var actions = '';
    if (missing > 0) {
      actions += '<button class="selection-keyword-add" onclick="applySelectionKeyword(' + k.id + ')" title="Add this keyword to the selected photos missing it">Add to ' + missing + '</button>';
    }
    if (count > 0) {
      actions += '<button class="selection-keyword-remove" onclick="removeSelectionKeyword(' + k.id + ')" title="Remove this keyword from selected photos that have it">Remove from ' + count + '</button>';
    }
    return '<div class="selection-keyword-row">' +
      '<div style="min-width:0;">' +
        '<div class="selection-keyword-name">' + escapeHtml(k.name) + '</div>' +
        '<div class="selection-keyword-meta">On ' + count + ' of ' + selectedCount + (missing ? ', missing from ' + missing : '') + '</div>' +
      '</div>' +
      '<div class="selection-keyword-actions">' + actions + '</div>' +
      '</div>';
  }
  groupDefinitions.forEach(function(group) {
    var rows = keywordsByType[group.type] || [];
    if (!rows.length) return;
    html += '<div class="selection-keyword-group" data-keyword-type="' + group.type + '">' +
      '<div class="selection-keyword-group-title">' + group.label + '</div>' +
      '<div class="selection-keyword-group-rows">' + rows.map(renderKeywordRow).join('') + '</div>' +
      '</div>';
  });
  list.innerHTML = html;
}

async function renderSelectionWildlifeState(ids) {
  var status = document.getElementById('selectionWildlifeStatus');
  var actions = document.getElementById('selectionWildlifeActions');
  if (!status || !actions) return;
  if (!ids || !ids.length) {
    Vireo.browse.panelRequests.wildlife.invalidate();
    status.textContent = '';
    actions.innerHTML = '';
    return;
  }
  // Derive counts from the full selection, not from the loaded grid:
  // "Select all matching" can include off-page photos that are absent from
  // ``photos``, and filtering client-side would omit them from the counts
  // and hide batch controls that should still be available.
  var request = Vireo.browse.panelRequests.wildlife.begin();
  var data;
  try {
    data = await safeFetch('/api/selection/wildlife-state', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids}),
    }, { toast: false });
  } catch (e) {
    if (request.fail()) {
      status.textContent = '';
      actions.innerHTML = '';
    }
    return;
  }
  if (!request.isCurrent()) return;
  var includedCount = (data && data.included_count) || 0;
  var excludedCount = (data && data.excluded_count) || 0;
  var selectedCount = (data && data.selected_count) || 0;
  var missingCount = (data && data.missing_count) || 0;
  if (!selectedCount && !missingCount) {
    status.textContent = '';
    actions.innerHTML = '';
    return;
  }
  var statusText;
  if (!selectedCount) {
    statusText = '';
  } else if (excludedCount === 0) {
    statusText = 'All accessible photos are included in wildlife processing.';
  } else if (includedCount === 0) {
    statusText = 'All accessible photos are excluded from wildlife processing.';
  } else {
    statusText = includedCount + ' included · ' + excludedCount + ' excluded';
  }
  // Surface missing IDs the same way the state endpoint reports them so
  // the panel never implies its counts cover the full user selection when
  // some photos are unavailable in the active workspace.
  if (missingCount > 0) {
    var missingText = missingCount + ' unavailable in this workspace';
    statusText = statusText ? statusText + ' · ' + missingText : missingText;
  }
  status.textContent = statusText;
  var html = '';
  if (includedCount > 0) {
    html += '<button class="selection-keyword-remove" onclick="setSelectionWildlifeExcluded(true)">Exclude ' + includedCount + '</button>';
  }
  if (excludedCount > 0) {
    html += '<button class="selection-keyword-add" onclick="setSelectionWildlifeExcluded(false)">Include ' + excludedCount + '</button>';
  }
  actions.innerHTML = html;
}

async function setSelectionWildlifeExcluded(excluded) {
  var ids = getActiveSelection();
  if (!ids.length) return;
  var data;
  try {
    data = await safeFetch('/api/batch/wildlife-excluded', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids, excluded: excluded}),
    });
  } catch (e) { return; }
  var changed = new Set((data && data.photo_ids) || ids);
  changed.forEach(function(photoId) {
    var photo = findBrowsePhoto(photoId);
    if (photo) photo.wildlife_excluded = excluded ? 1 : 0;
  });
  var changedIds = Array.from(changed);
  refreshGridCards(changedIds);
  refreshExpandedBrowseStackMembers(changedIds);
  // The selection may have changed while the batch request was in flight.
  // Refresh the current selection so this completion cannot supersede a
  // newer state request with counts and actions for the old photo ids.
  renderSelectionWildlifeState(getActiveSelection());
  scheduleCollectionCountsRefresh();
  refreshActiveCollectionAfterMembershipChange([MUTATION_WILDLIFE]);
  // The batch endpoint skips missing/out-of-workspace IDs instead of
  // rejecting the whole selection; tell the user how many were skipped
  // so a partial apply is never invisible.
  var skipped = (data && data.skipped_count) || 0;
  if (skipped > 0 && typeof showToast === 'function') {
    showToast('Skipped ' + skipped + ' photo' + (skipped === 1 ? '' : 's') +
              ' not accessible in this workspace.', 'warning');
  }
  showUndoToast();
}

async function applySelectionKeyword(keywordId) {
  var ids = selectionKeywordMissingById[String(keywordId)] || [];
  if (!ids.length) return;
  try {
    await safeFetch('/api/batch/keyword', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids, keyword_id: keywordId}),
    });
  } catch(e) { return; }
  await _refreshBrowseKeywordState(ids);
  Vireo.browse.panelRequests.keywords.invalidate();
  loadSelectionKeywordSuggestions(getActiveSelection());
  loadKeywords();
  scheduleCollectionCountsRefresh();
  refreshActiveCollectionAfterMembershipChange([MUTATION_KEYWORD]);
  refreshPendingSyncBanner();
  showUndoToast();
}

async function removeSelectionKeyword(keywordId) {
  var ids = selectionKeywordPresentById[String(keywordId)] || [];
  if (!ids.length) return;
  var removedName = selectionKeywordNameById[String(keywordId)] || null;
  try {
    await safeFetch('/api/batch/keyword-remove', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids, keyword_id: keywordId}),
    });
  } catch(e) { return; }
  await _refreshBrowseKeywordState(ids);
  _clearRepresentativeStateAfterKeywordRemoval(ids, removedName);
  Vireo.browse.panelRequests.keywords.invalidate();
  loadSelectionKeywordSuggestions(getActiveSelection());
  loadKeywords();
  scheduleCollectionCountsRefresh();
  // One reload only. ``refreshActiveCollectionAfterMembershipChange`` runs
  // ``resetAndLoad({preserveScroll: true})`` when any active rule reads a
  // keyword-derived field (``dependsOnMutation([MUTATION_KEYWORD])``), and
  // the sidebar's ``activeKeyword`` path installs the same ``keyword``
  // rule into VireoFilter — so the earlier follow-up call to
  // ``refreshActiveKeywordAfterRemoval`` fired a *second* reset that
  // arrived after the first had already cleared ``photos`` and grid
  // cards, leaving ``captureBrowseViewportAnchor`` nothing to anchor on
  // and dropping the scroll position (Codex review r4013311737).
  refreshActiveCollectionAfterMembershipChange([MUTATION_KEYWORD]);
  refreshPendingSyncBanner();
  showUndoToast();
}

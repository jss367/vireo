/* Browse: single-photo detail panel, metadata, summary panel, undo/redo.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Undo / Redo ---------- */
async function undoLast() {
  return window.doUndo();
}
async function redoLast() {
  return window.doRedo();
}

async function showUndoToast() {
  try {
    var data = await safeFetch('/api/undo/status', {}, { toast: false });
    if (data.available) showToast(data.description + ' — Ctrl+Z to undo', 'success');
  } catch(e) {}
}

/* showToast is now provided globally by _navbar.html */

async function loadDetail(id) {
  // If a multi-selection is already active, skip this single-photo load — it
  // would strip .batch-mode and rewire the sidebar (rating stars → setRating(id))
  // to the anchor while the user still has N photos selected. Common flow:
  // click A, then Cmd/Ctrl-click B before A's fetch returns. selectedPhotoId
  // stays A, so both the sync class removal below and the post-await
  // renderDetail(A) would overwrite the batch inspector the Cmd-click just set up.
  if (selectedPhotos.size > 1) return;
  Vireo.browse.panelRequests.detailPredictions.invalidate();
  document.getElementById('summaryPanel').classList.add('hidden');
  var detail = document.getElementById('detailContent');
  // Loading a single photo's detail always exits batch mode.
  if (detail) detail.classList.remove('batch-mode');
  if (detail) detail.classList.toggle('visible', window._detailPhotoId === id);

  // Kill any stale EXIF suggestion tagged for a *different* photo before the
  // fetch returns. Otherwise: open A (suggestion shown, data-photo-id=A) →
  // click B → Select All lands before B's /api/photos response resolves.
  // renderBatchInspector's preserveExifSuggestion check finds A's stale
  // data-photo-id, sees A is in the Select All ids, and resurrects A's
  // Accept line for the whole batch — clicking it applies A's GPS place to
  // every selected photo. Also null _detailPhotoId so any in-flight
  // reverse-geocode for A can't repaint through maybeShowExifSuggestion's
  // post-await guard once the batch inspector is on screen. Codex P2 on
  // PR #1097 (17:23Z).
  if (window._detailPhotoId !== id) {
    clearExifSuggestion();
    window._detailPhotoId = null;
  }

  try {
    var photo = await safeFetch('/api/photos/' + id, {}, { toast: false });
    // Re-check after the fetch: a Cmd/Ctrl-click landing during the await can
    // promote the selection to multi. The batch inspector is already rendered;
    // don't clobber it with this stale single-photo response.
    if (selectedPhotoId === id && selectedPhotos.size <= 1) {
      renderDetail(photo);
      if (detail) detail.classList.add('visible');
      // Fetched separately from /api/photos so the panel renders the same
      // enriched rows Review does (nested alternatives, disagreement context)
      // instead of a second, thinner idea of what a prediction is.
      loadDetailPredictions(id);
    }
  } catch(e) {}
}

function renderDetail(photo) {
  window._detailPhotoId = photo.id;
  // Single-photo view: clear any Mixed markers left over from batch mode.
  _batchToggleMixed('ratingMixed', false);
  _batchToggleMixed('flagMixed', false);
  _batchToggleMixed('colorMixed', false);
  if (
    typeof window.vireoRememberPhotoEditRecipe === 'function' &&
    Object.prototype.hasOwnProperty.call(photo, 'edit_recipe')
  ) {
    window.vireoRememberPhotoEditRecipe(photo.id, photo.edit_recipe, {
      skipIfLocallyWritten: true,
    });
  }
  if (typeof window.vireoRememberPhotoPair === 'function') {
    window.vireoRememberPhotoPair(photo);
  }
  if (typeof window.vireoUpdatePairSourceControls === 'function') {
    window.vireoUpdatePairSourceControls(photo.id);
  }
  var detailThumb = window.vireoThumbnailUrl ? window.vireoThumbnailUrl(photo) : '/thumbnails/' + photo.id + '.jpg';
  document.getElementById('detailImg').src = detailThumb;
  document.getElementById('detailFilename').textContent = photo.filename;
  renderCoordinateStatus(photo);

  // Rating stars
  var ratingHtml = '';
  for (var i = 1; i <= 5; i++) {
    var cls = i <= photo.rating ? 'detail-star active' : 'detail-star';
    ratingHtml += '<span class="' + cls + '" onclick="setRating(' + photo.id + ',' + i + ')">&#9733;</span>';
  }
  document.getElementById('detailRating').innerHTML = ratingHtml;

  // Flags
  updateDetailFlagButtons(photo.flag);

  // Color labels
  updateDetailColors();

  // Wildlife classification exclusion
  updateDetailWildlifeExcluded(photo);

  // Keywords
  var kwTypeIcons = {general:'●', taxonomy:'🌿', individual:'👤', location:'📍', genre:'🎭'};
  var kwTypeLabels = {general:'General', taxonomy:'Taxonomy', individual:'Individual', location:'Location', genre:'Genre'};
  _rememberKeywordNames(photo.keywords || []);
  var kwHtml = '';
  (photo.keywords || []).forEach(function(k) {
    var ktype = k.type || 'general';
    var icon = kwTypeIcons[ktype] || '●';
    var dropdownHtml = '<div class="keyword-type-dropdown" data-kw-id="' + k.id + '">';
    Object.keys(kwTypeIcons).forEach(function(t) {
      var activeClass = t === ktype ? ' active' : '';
      dropdownHtml += '<span class="keyword-type-option' + activeClass + '" onclick="setKeywordType(' + k.id + ',\'' + t + '\',this)">' + kwTypeIcons[t] + ' ' + kwTypeLabels[t] + '</span>';
    });
    dropdownHtml += '</div>';
    kwHtml += '<span class="keyword-tag">' +
      '<span class="keyword-type-indicator" onclick="toggleTypeDropdown(this,' + k.id + ')" title="Type: ' + kwTypeLabels[ktype] + '">' + icon + dropdownHtml + '</span>' +
      escapeHtml(k.name) +
      '<span class="remove-kw" onclick="removeKeyword(' + photo.id + ',' + k.id + ')">&times;</span></span>';
  });
  document.getElementById('detailKeywords').innerHTML = kwHtml;

  // Location section
  if (photo.location) {
    renderLocationFilled(photo.location);
  } else {
    renderLocationEmpty();
    maybeShowExifSuggestion(photo);
  }

  // XMP sidecar
  var xmpEl = document.getElementById('detailXmp');
  if (photo.xmp_exists === false) {
    xmpEl.innerHTML = '<span style="color:var(--text-ghost);">No .xmp sidecar file</span>';
  } else if (photo.xmp_keywords && photo.xmp_keywords.length > 0) {
    var dbKwNames = (photo.keywords || []).map(function(k) { return k.name.toLowerCase(); });
    var xmpHtml = '';
    photo.xmp_keywords.forEach(function(kw) {
      var inDb = dbKwNames.indexOf(kw.toLowerCase()) !== -1;
      var color = inDb ? 'var(--accent)' : 'var(--warning)';
      var title = inDb ? 'In sync with database' : 'In XMP but not in database';
      xmpHtml += '<span style="display:inline-block;background:var(--bg-tertiary);padding:2px 6px;border-radius:3px;margin:2px;font-size:11px;border:1px solid ' + color + ';color:' + color + ';" title="' + title + '">' + escapeHtml(kw) + '</span>';
    });
    // Check for DB keywords not in XMP
    var xmpLower = photo.xmp_keywords.map(function(k) { return k.toLowerCase(); });
    (photo.keywords || []).forEach(function(k) {
      if (xmpLower.indexOf(k.name.toLowerCase()) === -1) {
        xmpHtml += '<span style="display:inline-block;background:var(--bg-tertiary);padding:2px 6px;border-radius:3px;margin:2px;font-size:11px;border:1px solid var(--info);color:var(--info);" title="In database but not in XMP (pending sync)">+ ' + escapeHtml(k.name) + '</span>';
      }
    });
    xmpHtml += '<div style="margin-top:4px;font-size:10px;color:var(--text-ghost);">' +
      '<span style="color:var(--accent);">&#9632;</span> synced ' +
      '<span style="color:var(--warning);">&#9632;</span> XMP only ' +
      '<span style="color:var(--info);">&#9632;</span> DB only (pending sync)</div>';
    xmpEl.innerHTML = xmpHtml;
  } else if (photo.xmp_exists) {
    xmpEl.innerHTML = '<span style="color:var(--text-ghost);">XMP file exists but has no keywords</span>';
  }

  // Quick summary
  var summary = '';
  var meta = photo.metadata;
  if (meta) {
    var exifTags = meta.EXIF || {};
    var composite = meta.Composite || {};

    // Camera (remove make from model if model already starts with it)
    var make = exifTags.Make || '';
    var model = exifTags.Model || '';
    var camera = model.startsWith(make) ? model : (make + ' ' + model).trim();
    if (camera) summary += '<div class="summary-camera">' + escapeHtml(camera) + '</div>';

    // Lens
    var lens = composite.LensID || exifTags.LensModel || composite.Lens || '';
    if (lens) summary += '<div class="summary-lens">' + escapeHtml(String(lens)) + '</div>';

    // Exposure line: focal | aperture | shutter | ISO
    var parts = [];
    if (exifTags.FocalLength) parts.push(Math.round(exifTags.FocalLength) + 'mm');
    if (exifTags.FNumber) parts.push('f/' + exifTags.FNumber);
    if (exifTags.ExposureTime) {
      if (exifTags.ExposureTime < 1) {
        parts.push('1/' + Math.round(1 / exifTags.ExposureTime) + 's');
      } else {
        parts.push(exifTags.ExposureTime + 's');
      }
    }
    if (exifTags.ISO) parts.push('ISO ' + exifTags.ISO);
    if (parts.length) summary += '<div class="summary-exposure">' + parts.join('&nbsp;&nbsp;') + '</div>';
  }

  // Date, dimensions, file size (always from photo fields)
  if (photo.timestamp) summary += 'Date: ' + photo.timestamp + '<br>';
  if (photo.width && photo.height) summary += 'Size: ' + photo.width + ' x ' + photo.height + '<br>';
  if (photo.file_size) {
    var sz = photo.file_size;
    summary += 'File: ' + (sz >= 1048576 ? (sz / 1048576).toFixed(1) + ' MB' : Math.round(sz / 1024) + ' KB') + '<br>';
  }
  document.getElementById('detailSummary').innerHTML = summary;

  // Path
  document.getElementById('detailPath').textContent = photo.filename;

  // Clear search and render metadata groups
  var searchInput = document.getElementById('metadataSearch');
  if (searchInput) searchInput.value = '';
  renderMetadataGroups(photo.metadata);
}

function updateDetailFlagButtons(flag) {
  document.querySelectorAll('.detail-flag-btn').forEach(function(btn) {
    btn.className = 'detail-flag-btn';
  });
  var flagBtns = document.querySelectorAll('.detail-flag-btn');
  if (flag === 'flagged') flagBtns[1].classList.add('active-flag');
  else if (flag === 'rejected') flagBtns[2].classList.add('active-reject');
}

function renderMetadataGroups(metadata) {
  var container = document.getElementById('metadataGroups');
  var section = document.getElementById('detailMetadataSection');
  container.innerHTML = '';

  if (!metadata) {
    section.style.display = 'none';
    return;
  }

  // Sort groups: EXIF first, then alphabetical, _meta last
  var groups = Object.keys(metadata).sort(function(a, b) {
    if (a === 'EXIF') return -1;
    if (b === 'EXIF') return 1;
    if (a === '_meta') return 1;
    if (b === '_meta') return -1;
    return a.localeCompare(b);
  });

  // Skip groups with 0 tags
  groups = groups.filter(function(g) {
    return Object.keys(metadata[g]).length > 0;
  });

  if (groups.length === 0) {
    section.style.display = 'none';
    return;
  }

  section.style.display = '';

  groups.forEach(function(group) {
    var tags = metadata[group];
    var tagNames = Object.keys(tags).sort();

    var div = document.createElement('div');
    div.className = 'meta-group';
    div.setAttribute('data-group', group);

    var header = document.createElement('div');
    header.className = 'meta-group-header';
    header.innerHTML = '<span>' + escapeHtml(group) + '</span><span class="meta-group-count">' + tagNames.length + '</span>';
    header.onclick = function() { div.classList.toggle('open'); };

    var body = document.createElement('div');
    body.className = 'meta-group-body';

    tagNames.forEach(function(tag) {
      var val = tags[tag];
      if (typeof val === 'object' && val !== null) val = JSON.stringify(val);
      var row = document.createElement('div');
      row.className = 'meta-tag-row';
      row.setAttribute('data-tag', tag.toLowerCase());
      row.setAttribute('data-value', String(val).toLowerCase());
      row.innerHTML = '<span class="meta-tag-name">' + escapeHtml(tag) + '</span><span class="meta-tag-value">' + escapeHtml(String(val)) + '</span>';
      body.appendChild(row);
    });

    div.appendChild(header);
    div.appendChild(body);
    container.appendChild(div);
  });
}

function filterMetadata(query) {
  var q = query.toLowerCase().trim();
  var groups = document.querySelectorAll('#metadataGroups .meta-group');

  groups.forEach(function(group) {
    if (!q) {
      // Reset: show all groups, collapse all
      group.style.display = '';
      group.classList.remove('open');
      group.querySelectorAll('.meta-tag-row').forEach(function(row) {
        row.style.display = '';
      });
      return;
    }

    var rows = group.querySelectorAll('.meta-tag-row');
    var hasMatch = false;

    rows.forEach(function(row) {
      var tag = row.getAttribute('data-tag') || '';
      var val = row.getAttribute('data-value') || '';
      var matches = tag.indexOf(q) !== -1 || val.indexOf(q) !== -1;
      row.style.display = matches ? '' : 'none';
      if (matches) hasMatch = true;
    });

    // Also check if group name matches
    var groupName = (group.getAttribute('data-group') || '').toLowerCase();
    if (groupName.indexOf(q) !== -1) {
      hasMatch = true;
      rows.forEach(function(row) { row.style.display = ''; });
    }

    group.style.display = hasMatch ? '' : 'none';
    if (hasMatch && q) {
      group.classList.add('open');
    }
  });
}

function hideDetailPanel() {
  document.getElementById('detailContent').classList.remove('visible');
  document.getElementById('summaryPanel').classList.remove('hidden');
}

// Trading a single-photo detail focus for a batch. The panel is not the only
// thing that has to go: the EXIF suggestion element keeps its data-photo-id
// and its Accept button, and an in-flight reverse-geocode keeps
// window._detailPhotoId as its owner check — so a later selection that still
// contains the abandoned photo would resurrect its Accept line for the whole
// batch, and one click would apply that one photo's place to every selected
// photo. closeDetail() and clearSelection() have retired both since Codex P2
// on PR #1097; the paths that enter a stack selection need it for the same
// reason. Codex P1 on PR #1672.
function abandonDetailFocusForBatch() {
  hideDetailPanel();
  clearExifSuggestion();
  window._detailPhotoId = null;
}

function closeDetail() {
  anchorRestoreEpoch++;
  hideDetailPanel();
  selectedPhotoId = null;
  selectedIndex = -1;
  // Drop any lingering EXIF suggestion attached to the closed detail photo.
  // Without this, the element keeps its data-photo-id and Accept button; a
  // later Ctrl+A (or any batch that still contains that photo) would satisfy
  // renderLocationEmpty's owner-in-selection check and resurrect the anchor's
  // Accept line for the whole batch — clicking it would apply the anchor's
  // GPS-derived place to every selected photo. Codex P2 on PR #1097.
  clearExifSuggestion();
  // Null the ambient detail-photo pointer for the same reason. maybeShowExif
  // Suggestion's post-await guard uses window._detailPhotoId as its owner
  // check: if a reverse-geocode was in flight when we closed the panel, the
  // fetch keeps running and later resolves. With the pointer still set, a
  // Ctrl+A that includes the departed photo would satisfy both async guards
  // and repaint A's Accept line into the batch inspector. Codex P2 on
  // PR #1097 (17:04Z follow-up).
  window._detailPhotoId = null;
  // Re-highlight any cmd/shift-click selections that survive detail close.
  // A blanket .remove('selected') would leave selectedPhotos armed for batch
  // actions with no visible indicator — e.g. click A -> cmd-click B -> cmd-click
  // B drops the set to {A}, and closing detail would otherwise show "1 selected"
  // in the bar against an unhighlighted photo.
  refreshCardSelectionVisuals();
  updateBatchBar();
  loadSummary();
}

function buildSummaryParams() {
  var params = new URLSearchParams();
  if (activeFolderId) params.set('folder_id', activeFolderId);
  var rules = getBrowseRules();
  if (rules) params.set('rules', JSON.stringify(rules));
  appendVisualScopeParams(params);
  if (activeCollectionId) params.set('collection_id', activeCollectionId);
  return params;
}

function reconcileSummaryLoadRenders() {
  var gen = summaryLoadGen;
  while (gen > summaryRenderDecisionGen) {
    var state = summaryLoadStates[gen];
    if (!state || state.status === 'pending') return;
    if (state.status === 'success') {
      var currentKey = buildSummaryParams().toString();
      summaryRenderDecisionGen = gen;
      if (state.key === currentKey) renderSummary(state.data);
      Object.keys(summaryLoadStates).forEach(function(key) {
        if (Number(key) <= summaryLoadGen) delete summaryLoadStates[key];
      });
      return;
    }
    gen--;
  }
}

async function loadSummary() {
  var gen = ++summaryLoadGen;
  var params = buildSummaryParams();
  summaryLoadStates[gen] = {
    status: 'pending',
    key: params.toString()
  };

  try {
    var data = await safeFetch('/api/browse/summary?' + params.toString(), {
      headers: Vireo.api.searchLaneHeaders(
        'summary', searchLaneSeq(summaryLane, params.toString())),
    }, { toast: false });
    var state = summaryLoadStates[gen];
    if (!state) return data;
    state.status = 'success';
    state.data = data;
    reconcileSummaryLoadRenders();
    return data;
  } catch(e) {
    var failedState = summaryLoadStates[gen];
    if (failedState) {
      failedState.status = 'failure';
      reconcileSummaryLoadRenders();
    }
    return null;
  }
}

function renderSummary(data) {
  var isFiltered = data.filtered_total !== data.total;

  document.getElementById('summaryPhotoCount').textContent = data.filtered_total.toLocaleString();
  document.getElementById('summaryFilterNote').textContent =
    isFiltered ? 'of ' + data.total.toLocaleString() + ' total' : '';

  document.getElementById('summaryClassified').textContent = data.classified.toLocaleString() + ' classified';
  document.getElementById('summaryUnclassified').textContent = data.unclassified.toLocaleString() + ' unclassified';

  // Top species
  var speciesSection = document.getElementById('summarySpeciesSection');
  var speciesList = document.getElementById('summarySpeciesList');
  speciesList.replaceChildren();
  var species = data.top_species || [];
  speciesSection.style.display = species.length ? '' : 'none';
  species.forEach(function(s) {
    var row = document.createElement('button');
    row.type = 'button';
    row.className = 'summary-species-row';
    row.title = 'Filter by top predicted species: ' + s.species;
    var name = document.createElement('span');
    name.className = 'summary-species-name';
    name.textContent = s.species;
    var count = document.createElement('span');
    count.className = 'summary-species-count';
    count.textContent = s.count;
    row.append(name, count);
    row.addEventListener('click', function() { filterByTopSpecies(s.species); });
    row.addEventListener('keydown', function(event) {
      // Let the native button activate without triggering grid shortcuts.
      if (event.key === 'Enter' || event.key === ' ') event.stopPropagation();
    });
    speciesList.appendChild(row);
  });
}

async function filterByTopSpecies(species) {
  var scopeGen = ++browseScopeGen;
  if (!VireoFilter.isReady()) {
    if (!browseFilterInitPromise) return;
    try {
      await browseFilterInitPromise;
    } catch (e) {
      return;
    }
    if (!VireoFilter.isReady() || scopeGen !== browseScopeGen) return;
  }
  // Compose with legacy collection deep links instead of dropping their scope.
  if (activeCollectionId) dashboardCollectionScope = true;
  var rules = VireoFilter.getUserRules();
  if (rules.mode && rules.mode !== 'all' && rules.rules.length) {
    // Narrow an OR collection as a whole, rather than adding another OR arm.
    VireoFilter.loadExpression({mode: 'all', rules: [
      rules, {field: 'top_predicted_species', op: 'is', value: species}
    ]}, VireoFilter.getVisual(), {reason: 'filterAdded'});
  } else {
    VireoFilter.addRule('top_predicted_species', 'is', species);
  }
}

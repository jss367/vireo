// Species menus, search, confirmation, and history hooks.
// Classic page script; shared globals are initialized before boot.js runs.

// -- Species confirmation --

/** @type {HTMLElement | null} */ var activeDropdown = null;
/** @type {Record<string,boolean>} */ var speciesConfirmInFlight = {};

/** @param {number[]} photoIds */
function speciesConfirmPhotoKey(photoIds) {
  return 'photos:' + (photoIds || []).slice().sort(function(a, b) { return a - b; }).join(',');
}

/** @param {ReviewEncounter} enc @param {number | null} [burstIdx] */
function speciesConfirmPendingFor(enc, burstIdx) {
  if (!enc) return false;
  var photoIds;
  if (burstIdx != null && enc.bursts && enc.bursts[burstIdx ?? -1]) {
    photoIds = requireReviewValue(requireReviewValue(enc.bursts)[burstIdx ?? -1]).photo_ids;
  } else {
    photoIds = enc.photo_ids;
  }
  return !!speciesConfirmInFlight[speciesConfirmPhotoKey(photoIds)];
}

/** @param {ReviewEncounter} enc @param {number} encIdx @param {string} level @param {number | null} [burstIdx] @param {string} [pos] */
function renderSpeciesWidget(enc, encIdx, level, burstIdx, pos) {
  // level: 'encounter' or 'burst'
  // pos:   'top' (default) or 'bot' — disambiguates the duplicated bottom widget
  pos = pos || 'top';
  var isConfirmed, displayName, candidateSpecies;
  if (level === 'burst') {
    var burst = enc.bursts[burstIdx ?? -1];
    var ovr = requireReviewValue(burst).species_override;
    if (ovr) {
      isConfirmed = !!ovr.confirmed;
      candidateSpecies = ovr.species || enc.confirmed_species || (enc.species ? enc.species[0] : null);
    } else {
      // No per-burst override: inherit from the encounter so confirming the
      // encounter visibly cascades to every burst.
      isConfirmed = !!enc.species_confirmed;
      candidateSpecies = enc.confirmed_species || (enc.species ? enc.species[0] : null);
    }
    displayName = candidateSpecies || 'Add species';
  } else {
    isConfirmed = enc.species_confirmed;
    candidateSpecies = enc.confirmed_species || (enc.species ? enc.species[0] : null);
    displayName = candidateSpecies || 'Add species';
  }

  var hasCandidate = !!candidateSpecies;
  var identity = pipelineResults && currentReviewResults().species_identities && requireReviewValue(currentReviewResults().species_identities)[candidateSpecies || ''];
  if (identity) displayName = identity.display_name;
  var isPending = speciesConfirmPendingFor(enc, burstIdx);
  var cls = isConfirmed ? 'confirmed' : (hasCandidate ? 'unconfirmed' : 'missing');
  var badge = isConfirmed ? '&#10003;' : (hasCandidate ? '?' : '+');
  var widgetCls = level === 'burst' ? 'burst-species-widget' : 'species-widget';

  var idKey = (burstIdx != null ? burstIdx : 'enc') + '_' + pos;
  var html = '<span class="' + widgetCls + '" data-species-widget="1" data-level="' + level + '" data-enc="' + encIdx + '" data-burst="' + (burstIdx != null ? burstIdx : '') + '" data-pos="' + pos + '">';
  html += '<span class="species-name ' + cls + '" title="' + (isPending ? 'Confirming species...' : (hasCandidate ? 'Change species' : 'Add species')) + '"'
    + (isPending ? '' : ' onclick="toggleSpeciesDropdown(event, ' + encIdx + ',' + (burstIdx != null ? burstIdx : 'null') + ',\'' + pos + '\')"')
    + '>' + escapeHtml(displayName) + '</span>';
  html += '<span class="species-badge ' + cls + '">' + badge + '</span>';
  if (!isConfirmed && hasCandidate) {
    html += '<button class="species-confirm-btn" onclick="confirmSpecies(event, ' + encIdx + ',' + (burstIdx != null ? burstIdx : 'null') + ',null)" title="' + (isPending ? 'Confirming species...' : 'Confirm as ' + escapeHtml(displayName)) + '"' + (isPending ? ' disabled aria-busy="true"' : '') + '>&#10003;</button>';
  }

  // Dropdown — bottom-bar widget opens upward to stay inside the card
  var dropdownCls = 'species-dropdown' + (pos === 'bot' ? ' up' : '');
  html += '<div class="' + dropdownCls + '" id="spDrop_' + encIdx + '_' + idKey + '"></div>';
  html += '</span>';
  return html;
}

// A large review can have thousands of species widgets. Build the search
// controls and prediction rows only for the menu the user actually opens.
/** @param {ReviewEncounter} enc @param {number} encIdx @param {number | null} [burstIdx] @param {string} [pos] */
function renderSpeciesDropdownContents(enc, encIdx, burstIdx, pos) {
  var level = burstIdx != null ? 'burst' : 'encounter';
  var predictions = level === 'burst'
    ? (requireReviewValue(requireReviewValue(enc.bursts)[burstIdx ?? -1]).species_predictions || []) : (enc.species_predictions || []);
  var idKey = (burstIdx != null ? burstIdx : 'enc') + '_' + pos;
  var html = '';
  html += '<div class="species-dropdown-search"><div class="search-control"><input type="text" placeholder="Search or type a new name..." oninput="searchSpecies(event, ' + encIdx + ',' + (burstIdx != null ? burstIdx : 'null') + ',\'' + pos + '\')" onkeydown="speciesInputKey(event, ' + encIdx + ',' + (burstIdx != null ? burstIdx : 'null') + ')">';
  html += '<button class="search-option-btn species-dropdown-match-case' + (speciesDropdownSearchOptions.matchCase ? ' active' : '') + '" type="button" aria-pressed="' + (speciesDropdownSearchOptions.matchCase ? 'true' : 'false') + '" onclick="toggleSpeciesDropdownSearchOption(event, \'match_case\')" title="Match case">Aa</button>';
  html += '<button class="search-option-btn species-dropdown-whole-word' + (speciesDropdownSearchOptions.wholeWord ? ' active' : '') + '" type="button" aria-pressed="' + (speciesDropdownSearchOptions.wholeWord ? 'true' : 'false') + '" onclick="toggleSpeciesDropdownSearchOption(event, \'whole_word\')" title="Match whole word">ab</button></div></div>';

  // "Use encounter label" option for burst level
  var encounterLabel = enc.confirmed_species || (enc.species ? enc.species[0] : null);
  if (level === 'burst' && encounterLabel) {
    html += '<div class="species-dropdown-item" onclick="clearBurstOverride(event, ' + encIdx + ',' + burstIdx + ')">';
    html += '<span class="sp-name" style="color:var(--text-dim);">Use encounter label (' + escapeHtml(encounterLabel) + ')</span>';
    html += '</div>';
  }

  // Prediction rows
  predictions.forEach(function(sp) {
    var modelParts = (sp.models || []).map(function(m) {
      return escapeHtml(m.model) + ' ' + (m.confidence * 100).toFixed(0) + '%';
    });
    html += '<div class="species-dropdown-item" onclick="confirmSpecies(event, ' + encIdx + ',' + (burstIdx != null ? burstIdx : 'null') + ',' + speciesNameArg(sp.species) + ')">';
    html += '<span class="sp-name">' + escapeHtml(sp.species) + '</span>';
    html += '<span class="sp-models">' + modelParts.join(', ') + '</span>';
    html += '</div>';
  });

  // Search results container
  html += '<div id="spSearch_' + encIdx + '_' + idKey + '"></div>';
  return html;
}

/** @param {Event} event @param {number} encIdx @param {number | null} [burstIdx] @param {string} [pos] */
function toggleSpeciesDropdown(event, encIdx, burstIdx, pos) {
  event.stopPropagation();
  if (pipelineResults && currentReviewResults().encounters && speciesConfirmPendingFor(requireReviewValue(currentReviewResults().encounters[encIdx]), burstIdx)) return;
  pos = pos || 'top';
  var id = 'spDrop_' + encIdx + '_' + (burstIdx != null ? burstIdx : 'enc') + '_' + pos;
  var el = /** @type {HTMLElement} */ (document.getElementById(id));
  if (!el) return;
  var isOpen = el.classList.contains('open');
  // Close any open dropdown
  closeAllDropdowns();
  if (!isOpen) {
    el.innerHTML = renderSpeciesDropdownContents(requireReviewValue(currentReviewResults().encounters[encIdx]), encIdx, burstIdx, pos);
    el.classList.add('open');
    activeDropdown = el;
    var input = /** @type {HTMLInputElement} */ (el.querySelector('input'));
    if (input) setTimeout(function() { input.focus(); }, 50);
  }
}

function closeAllDropdowns() {
  clearTimeout(_searchTimer || undefined);
  _searchTimer = null;
  _speciesSearchSeq++;
  /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('.species-dropdown.open')).forEach(function(d) {
    d.classList.remove('open');
    d.replaceChildren();
  });
  activeDropdown = null;
}

/** @type {ReturnType<typeof setTimeout> | null} */ var _searchTimer = null;
// Bumped on every keystroke and on close, so a search already in flight can
// tell it is stale and not fill the list with results for an older query.
var _speciesSearchSeq = 0;
function renderSpeciesDropdownSearchOptions() {
  /** @type {NodeListOf<HTMLButtonElement>} */ (document.querySelectorAll('.species-dropdown-match-case')).forEach(function(btn) {
    btn.classList.toggle('active', speciesDropdownSearchOptions.matchCase);
    btn.setAttribute('aria-pressed', speciesDropdownSearchOptions.matchCase ? 'true' : 'false');
  });
  /** @type {NodeListOf<HTMLButtonElement>} */ (document.querySelectorAll('.species-dropdown-whole-word')).forEach(function(btn) {
    btn.classList.toggle('active', speciesDropdownSearchOptions.wholeWord);
    btn.setAttribute('aria-pressed', speciesDropdownSearchOptions.wholeWord ? 'true' : 'false');
  });
}

/** @param {Event} event @param {'match_case' | 'whole_word'} option */
function toggleSpeciesDropdownSearchOption(event, option) {
  if (event) event.stopPropagation();
  if (option === 'match_case') {
    speciesDropdownSearchOptions.matchCase = !speciesDropdownSearchOptions.matchCase;
  }
  if (option === 'whole_word') {
    speciesDropdownSearchOptions.wholeWord = !speciesDropdownSearchOptions.wholeWord;
  }
  renderSpeciesDropdownSearchOptions();
  var dropdown = event && event.target ? /** @type {HTMLElement} */ (event.target).closest('.species-dropdown') : null;
  var input = dropdown ? /** @type {HTMLInputElement} */ (dropdown.querySelector('.species-dropdown-search input')) : null;
  if (input) input.dispatchEvent(new Event('input', {bubbles: true}));
}

/** @param {Event} event @param {number} encIdx @param {number | null} [burstIdx] @param {string} [pos] */
function searchSpecies(event, encIdx, burstIdx, pos) {
  pos = pos || 'top';
  var q = /** @type {HTMLInputElement} */ (event.target).value.trim();
  var targetId = 'spSearch_' + encIdx + '_' + (burstIdx != null ? burstIdx : 'enc') + '_' + pos;
  var container = /** @type {HTMLElement} */ (document.getElementById(targetId));
  if (!container) return;
  // Cancel the pending search before the early returns below: typing "ro"
  // then clearing the field must not let the "ro" search fire and fill the
  // list afterwards.
  clearTimeout(_searchTimer || undefined);
  _searchTimer = null;
  var seq = ++_speciesSearchSeq;
  if (!q) { container.innerHTML = ''; return; }

  function renderInto(/** @type {HTMLElement} */ target, /** @type {string} */ typed, /** @type {string[]} */ results) {
    var hasExact = results.some(function(name) {
      return speciesDropdownSearchOptions.matchCase
        ? name === typed
        : name.toLowerCase() === typed.toLowerCase();
    });
    var html = '';
    if (!hasExact) {
      html += '<div class="species-dropdown-item" data-freeform="1">'
        + '<span class="sp-name">Use &ldquo;' + escapeHtml(typed) + '&rdquo;</span>'
        + '<span class="sp-models" style="color:var(--text-dim);">new name</span>'
        + '</div>';
    }
    results.forEach(function(name) {
      html += '<div class="species-dropdown-item" onclick="confirmSpecies(event, ' + encIdx + ',' + (burstIdx != null ? burstIdx : 'null') + ',' + speciesNameArg(name) + ')">';
      html += '<span class="sp-name">' + escapeHtml(name) + '</span>';
      html += '<span class="sp-models" style="color:var(--text-dim);">search</span>';
      html += '</div>';
    });
    target.innerHTML = html;
    var freeRow = /** @type {HTMLElement} */ (target.querySelector('[data-freeform="1"]'));
    if (freeRow) {
      freeRow.addEventListener('click', function(e) {
        confirmSpecies(e, encIdx, burstIdx, typed);
      });
    }
  }

  if (q.length < 2) {
    renderInto(container, q, []);
    return;
  }

  _searchTimer = setTimeout(async function() {
    var params = new URLSearchParams({q: q});
    VireoTextSearch.appendParams(params, speciesDropdownSearchOptions);
    var results;
    try {
      results = await safeFetch('/api/species/search?' + params.toString(), {}, { toast: false });
    } catch (e) {
      results = [];
    }
    if (seq !== _speciesSearchSeq || !container.isConnected) return;
    if (!results || !Array.isArray(results)) results = [];
    renderInto(container, q, results);
  }, 250);
}

/** @param {KeyboardEvent} event @param {number} encIdx @param {number | null} [burstIdx] */
function speciesInputKey(event, encIdx, burstIdx) {
  if (event.key !== 'Enter') return;
  // Skip while an IME composition is active — Enter is used to finalize
  // character conversion in East Asian input methods.
  if (event.isComposing || event.keyCode === 229) return;
  event.preventDefault();
  var typed = (/** @type {HTMLInputElement} */ (event.target).value || '').trim();
  if (!typed) return;
  confirmSpecies(event, encIdx, burstIdx, typed);
}

/** @param {ReviewEncounter[]} encounters */
function encounterStructureSignature(encounters) {
  return JSON.stringify((encounters || []).map(function(enc) {
    return {
      photo_ids: enc.photo_ids || [],
      bursts: (enc.bursts || []).map(function(burst) {
        return (burst && burst.photo_ids) || burst || [];
      }),
    };
  }));
}

/** @param {HTMLElement} node @param {number} encIdx @param {number | null} [burstIdx] */
function replaceSpeciesWidgetNode(node, encIdx, burstIdx) {
  if (!node || !pipelineResults || !currentReviewResults().encounters) return;
  const enc = requireReviewValue(currentReviewResults().encounters[encIdx]);
  if (!enc) return;
  var level = node.getAttribute('data-level') || (burstIdx != null ? 'burst' : 'encounter');
  var pos = node.getAttribute('data-pos') || 'top';
  var nodeBurst = node.getAttribute('data-burst');
  var targetBurstIdx = nodeBurst === '' || nodeBurst == null ? null : parseInt(nodeBurst, 10);
  node.outerHTML = renderSpeciesWidget(enc, encIdx, level, targetBurstIdx, pos);
}

/** @param {number} encIdx @param {number | null} [burstIdx] */
function rerenderSpeciesWidgets(encIdx, burstIdx) {
  // Partial widget updates change the live card without regenerating its
  // cached markup. Invalidate it so Undo can restore a former state even
  // when that state happens to match the markup from the last full render.
  var rendered = pipelineReviewRenderedCards.get(encIdx);
  if (rendered) rendered.html = null;
  var selector = '[data-species-widget="1"][data-enc="' + encIdx + '"]';
  if (burstIdx != null) selector += '[data-burst="' + burstIdx + '"]';
  document.querySelectorAll(selector).forEach(function(node) {
    replaceSpeciesWidgetNode(/** @type {HTMLElement} */ (node), encIdx, burstIdx);
  });
}

function speciesConfirmNeedsFullRender() {
  return hideConfirmed || !!speciesFilter;
}

/** @param {number} encIdx @param {number | null} [burstIdx] @param {boolean} [forceFullRender] */
function refreshAfterSpeciesConfirm(encIdx, burstIdx, forceFullRender) {
  updateSummaryBar(refreshLocalSummaryCounts());
  if (forceFullRender || speciesConfirmNeedsFullRender()) {
    renderResults();
  } else {
    rerenderSpeciesWidgets(encIdx, burstIdx);
  }
}

/** @param {Event} event @param {number} encIdx @param {number | null | undefined} burstIdx @param {string | null | undefined} speciesName */
async function confirmSpecies(event, encIdx, burstIdx, speciesName) {
  if (event && event.stopPropagation) event.stopPropagation();
  closeAllDropdowns();

  // /api/encounters/species reloads and rewrites the saved cache on the
  // server, so calling it from a regrouped scope view would
  // overwrite the persisted review with only the scoped photo set. Block
  // it here — the inline species chip/dropdown buttons that call us have
  // no scope awareness of their own.
  if (isScopedReviewView() || (pipelineResults && currentReviewResults().source === 'browse-selection')) {
    notifyReadOnlyScopedView();
    return;
  }

  const enc = requireReviewValue(currentReviewResults().encounters[encIdx]);
  if (!enc) return;

  // Determine which species to confirm
  if (!speciesName) {
    if (burstIdx != null && enc.bursts && enc.bursts[burstIdx ?? -1]) {
      var ovr = requireReviewValue(requireReviewValue(enc.bursts)[burstIdx ?? -1]).species_override;
      speciesName = (ovr && ovr.species) || enc.confirmed_species || (enc.species ? enc.species[0] : null);
    } else {
      speciesName = enc.confirmed_species || (enc.species ? enc.species[0] : null);
    }
  }
  if (!speciesName) return;

  // Determine photo IDs
  var photoIds;
  if (burstIdx != null && enc.bursts && enc.bursts[burstIdx ?? -1]) {
    photoIds = requireReviewValue(requireReviewValue(enc.bursts)[burstIdx ?? -1]).photo_ids;
  } else {
    photoIds = enc.photo_ids;
  }
  var pendingKey = speciesConfirmPhotoKey(photoIds);
  if (speciesConfirmInFlight[pendingKey]) return;
  speciesConfirmInFlight[pendingKey] = true;
  rerenderSpeciesWidgets(encIdx, burstIdx);

  /** @type {{species: string; photo_ids: number[]; burst_index?: number}} */ var body = { species: speciesName, photo_ids: photoIds };
  if (burstIdx != null) body.burst_index = burstIdx;

  var previousStructure = encounterStructureSignature(currentReviewResults().encounters);

  var resp;
  try {
    resp = await safeFetch('/api/encounters/species', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  } catch (e) {
    delete speciesConfirmInFlight[pendingKey];
    rerenderSpeciesWidgets(encIdx, burstIdx);
    return;
  }
  delete speciesConfirmInFlight[pendingKey];
  if (!resp || !resp.ok) {
    rerenderSpeciesWidgets(encIdx, burstIdx);
    return;
  }

  // Prefer the server's encounter list — the endpoint may auto-detach a burst
  // into a new or adjacent encounter when its confirmed species differs from
  // the encounter's, and a stale local list would lose that change on the
  // next save-cache POST.
  var forceFullRender = false;
  if (resp.encounters) {
    forceFullRender = encounterStructureSignature(resp.encounters) !== previousStructure;
    currentReviewResults().encounters = resp.encounters;
    if (resp.summary) {
      currentReviewResults().summary = resp.summary;
      updateSummaryBar(currentReviewResults().summary);
    }
  } else if (burstIdx != null && enc.bursts && enc.bursts[burstIdx ?? -1]) {
    requireReviewValue(requireReviewValue(enc.bursts)[burstIdx ?? -1]).species_override = { species: speciesName, confirmed: true };
    updateSummaryBar(refreshLocalSummaryCounts());
  } else {
    enc.species_confirmed = true;
    enc.confirmed_species = speciesName;
    updateSummaryBar(refreshLocalSummaryCounts());
  }
  if (resp.low_confidence_photo_ids && resp.low_confidence_photo_ids.length) {
    showToast(
      'Tagged ' + resp.low_confidence_photo_ids.length + ' low-confidence detector ' +
      (resp.low_confidence_photo_ids.length === 1 ? 'photo' : 'photos'),
      'info'
    );
  }
  refreshAfterSpeciesConfirm(encIdx, burstIdx, forceFullRender);
  checkPendingSync();
  refreshLatestScopeSnapshotIfCurrent();
}

/** @param {Event} event @param {number} encIdx @param {number | null} [burstIdx] */
async function clearBurstOverride(event, encIdx, burstIdx) {
  event.stopPropagation();
  closeAllDropdowns();
  if (isScopedReviewView() || (pipelineResults && currentReviewResults().source === 'browse-selection')) {
    notifyReadOnlyScopedView();
    return;
  }
  const enc = requireReviewValue(currentReviewResults().encounters[encIdx]);
  if (!enc || !enc.bursts || !enc.bursts[burstIdx ?? -1]) return;
  var burst = enc.bursts[burstIdx ?? -1];
  try {
    var response = await safeFetch('/api/pipeline/save-cache', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({clear_override: {
        encounter_photo_ids: enc.photo_ids,
        burst_photo_ids: requireReviewValue(burst).photo_ids,
        expected_override: requireReviewValue(burst).species_override || null,
      }}),
    });
    if (response.encounters) currentReviewResults().encounters = response.encounters;
    if (response.summary) currentReviewResults().summary = response.summary;
  } catch (_) { return; }

  renderResults();
  updateSummaryBar(refreshLocalSummaryCounts());
  refreshLatestScopeSnapshotIfCurrent();
}

function bindPipelineReviewSpeciesDropdown() {
    // Close dropdown on outside click
    document.addEventListener('click', function(e) {
      if (activeDropdown && !activeDropdown.contains(/** @type {Node | null} */ (e.target)) &&
          !/** @type {HTMLElement} */ (e.target).classList.contains('species-name')) {
        closeAllDropdowns();
      }
    });
}

function registerPipelineReviewHistoryHooks() {
  // Grouping, flags and other photo edits share the durable workspace history.
  Vireo.registerPageHooks({
    beforeHistoryChange: function() {
      if (Object.keys(pipelineReviewGroupFlagInFlight).length) {
        showToast('A photo edit is still finishing — try again in a moment', 'info');
        return false;
      }
      if (/** @type {HTMLElement | null} */ (document.querySelector('.grm-overlay.open, .inspect-overlay.open'))) {
        showToast('Finish or close Group Review before undoing saved edits', 'info');
        return false;
      }
      return true;
    },

    /** @param {unknown} change */
    afterHistoryChange: async function(change) {
      renderHistoryCacheStaleBanner(/** @type {{pipeline_cache_stale?: boolean}} */ (change));
      var seq = ++reviewScopeRequestSeq;
      var data = await safeFetch('/api/pipeline/page-init', {}, {toast: false});
      if (seq !== reviewScopeRequestSeq || !data || !data.results) return;
      var results = data.results;
      cachedPipelineResults = cloneReviewData(results);
      cachedResultsCacheInfo = cloneReviewData(data.results_cache_info || null);
      if (!isScopedReviewView()) applyReviewResults(results, data.results_cache_info || null);
      else loadReviewScopeResults();
    },
  });
}

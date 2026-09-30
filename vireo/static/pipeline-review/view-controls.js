// Sidebar, sorting, searching, filters, and display preferences.
// Classic page script; shared globals are initialized before boot.js runs.

function setPipelineSidebarCollapsed(collapsed, persist) {
  var layout = document.getElementById('pipelineLayout');
  var toggle = document.getElementById('pipelineSidebarToggle');
  if (!layout || !toggle) return;
  layout.classList.toggle('sidebar-collapsed', !!collapsed);
  toggle.textContent = collapsed ? '\u203a' : '\u2039';
  toggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
  toggle.setAttribute('aria-label', collapsed ? 'Expand sidebar' : 'Collapse sidebar');
  toggle.title = collapsed ? 'Expand sidebar' : 'Collapse sidebar';
  if (persist !== false) {
    try {
      window.localStorage.setItem(PIPELINE_SIDEBAR_STORAGE_KEY, collapsed ? '1' : '0');
    } catch (e) {}
  }
}

function togglePipelineSidebar() {
  var layout = document.getElementById('pipelineLayout');
  setPipelineSidebarCollapsed(!(layout && layout.classList.contains('sidebar-collapsed')));
}

function initPipelineSidebarState() {
  var collapsed = false;
  try {
    collapsed = window.localStorage.getItem(PIPELINE_SIDEBAR_STORAGE_KEY) === '1';
  } catch (e) {}
  setPipelineSidebarCollapsed(collapsed, false);
}

function persistPipelineReviewViewState() {
  var speciesInput = document.getElementById('speciesFilterInput');
  try {
    window.localStorage.setItem(PIPELINE_VIEW_STATE_STORAGE_KEY, JSON.stringify({
      activeFilter: activeFilter,
      speciesFilter: speciesInput ? speciesInput.value : speciesFilterText,
      speciesFilterSearchOptions: speciesFilterSearchOptions,
      hideConfirmed: !!hideConfirmed,
      hideWithoutSuggestions: !!hideWithoutSuggestions,
      encounterSort: encounterSort,
      showPhotoLabels: !!showPhotoLabels,
      thumbSize: document.getElementById('thumbSizeSlider')
        ? document.getElementById('thumbSizeSlider').value
        : null,
    }));
  } catch (e) {}
}

function applyPipelineReviewToolbarState() {
  document.querySelectorAll('.filter-btn[data-filter]').forEach(function(btn) {
    btn.classList.toggle('active', btn.getAttribute('data-filter') === activeFilter);
  });

  var hideBtn = document.getElementById('hideConfirmedBtn');
  if (hideBtn) hideBtn.classList.toggle('active', hideConfirmed);

  var hideWithoutSuggestionsBtn = document.getElementById('hideWithoutSuggestionsBtn');
  if (hideWithoutSuggestionsBtn) {
    hideWithoutSuggestionsBtn.classList.toggle('active', hideWithoutSuggestions);
  }

  var input = document.getElementById('speciesFilterInput');
  if (input) input.value = speciesFilterText || '';
  var clearBtn = document.getElementById('speciesFilterClear');
  if (clearBtn) clearBtn.style.display = speciesFilter ? 'block' : 'none';
  VireoTextSearch.renderOptions('speciesFilter', speciesFilterSearchOptions);

  var sortSel = document.getElementById('encounterSortSelect');
  if (sortSel) sortSel.value = encounterSort;

  var labelsChk = document.getElementById('showPhotoLabelsChk');
  if (labelsChk) labelsChk.checked = !!showPhotoLabels;
}

function restorePipelineReviewViewState() {
  var state = null;
  try {
    state = JSON.parse(window.localStorage.getItem(PIPELINE_VIEW_STATE_STORAGE_KEY) || 'null');
  } catch (e) {}
  if (!state || typeof state !== 'object') {
    applyPipelineReviewToolbarState();
    return;
  }

  if (PIPELINE_REVIEW_FILTERS.indexOf(state.activeFilter) !== -1) {
    activeFilter = state.activeFilter;
  }
  speciesFilterText = typeof state.speciesFilter === 'string' ? state.speciesFilter : '';
  speciesFilter = speciesFilterText;
  if (state.speciesFilterSearchOptions && typeof state.speciesFilterSearchOptions === 'object') {
    speciesFilterSearchOptions = {
      matchCase: !!state.speciesFilterSearchOptions.matchCase,
      wholeWord: !!state.speciesFilterSearchOptions.wholeWord,
    };
  }
  hideConfirmed = !!state.hideConfirmed;
  hideWithoutSuggestions = !!state.hideWithoutSuggestions;
  showPhotoLabels = !!state.showPhotoLabels;

  if (PIPELINE_ENCOUNTER_SORTS.indexOf(state.encounterSort) !== -1) {
    encounterSort = state.encounterSort;
  }

  var thumbSize = parseInt(state.thumbSize, 10);
  if (!isNaN(thumbSize)) {
    thumbSize = Math.max(100, Math.min(320, thumbSize));
    var slider = document.getElementById('thumbSizeSlider');
    if (slider) slider.value = String(thumbSize);
    updateThumbSize(thumbSize, false);
  }

  applyPipelineReviewToolbarState();
}

function setEncounterSort(val) {
  if (PIPELINE_ENCOUNTER_SORTS.indexOf(val) === -1) return;
  encounterSort = val;
  persistPipelineReviewViewState();
  renderResults();
}

// Returns the order in which encounters should be displayed, as a list of
// ORIGINAL indices into pipelineResults.encounters. Never mutates the array,
// so every encounter's canonical index stays stable. Falls back to natural
// (chronological) order for 'default'. Ties break on original index so the
// result is deterministic regardless of Array.sort stability.
function encounterSortOrder() {
  var encs = (pipelineResults && pipelineResults.encounters) || [];
  var order = encs.map(function(_, i) { return i; });
  if (encounterSort === 'default') return order;
  var tsByPhoto = null;
  function photoTimestamps() {
    if (tsByPhoto) return tsByPhoto;
    tsByPhoto = {};
    ((pipelineResults && pipelineResults.photos) || []).forEach(function(p) {
      if (p && p.timestamp) tsByPhoto[p.id] = p.timestamp;
    });
    return tsByPhoto;
  }
  function startTime(e) {
    if (e && e.time_range && e.time_range[0]) return e.time_range[0];
    // No stored range (e.g. a freshly detached burst): fall back to the
    // earliest contained photo timestamp so time sorts place it
    // chronologically instead of dumping it at an extreme.
    var map = photoTimestamps(), best = '';
    ((e && e.photo_ids) || []).forEach(function(pid) {
      var t = map[pid];
      if (t && (best === '' || t < best)) best = t;
    });
    return best;
  }
  function cmpStr(a, b) { return a < b ? -1 : (a > b ? 1 : 0); }
  order.sort(function(ia, ib) {
    var a = encs[ia], b = encs[ib], d = 0;
    switch (encounterSort) {
      case 'photos_desc': d = (b.photo_count || 0) - (a.photo_count || 0); break;
      case 'photos_asc':  d = (a.photo_count || 0) - (b.photo_count || 0); break;
      case 'bursts_desc': d = (b.burst_count || 0) - (a.burst_count || 0); break;
      case 'time_desc':   d = cmpStr(startTime(b), startTime(a)); break;
      case 'time_asc':    d = cmpStr(startTime(a), startTime(b)); break;
    }
    return d !== 0 ? d : ia - ib;
  });
  return order;
}

function encounterKey(enc) {
  if (!enc || !enc.photo_ids || !enc.photo_ids.length) return null;
  return enc.photo_ids.slice().sort(function(a, b) { return a - b; }).join(',');
}

function escapeHtml(s) {
  if (s == null) return '';
  var d = document.createElement('div');
  d.textContent = String(s);
  return d.innerHTML.replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function formatEncounterTimeRange(timeRange) {
  if (!timeRange || !timeRange[0]) return '';

  var monthNames = [
    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
  ];
  function timestampParts(value) {
    var match = String(value || '').match(
      /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}:\d{2}(?::\d{2})?)/
    );
    if (!match) return null;
    var monthIndex = parseInt(match[2], 10) - 1;
    if (monthIndex < 0 || monthIndex >= monthNames.length) return null;
    return {
      dayKey: match[1] + '-' + match[2] + '-' + match[3],
      date: monthNames[monthIndex] + ' ' + parseInt(match[3], 10) + ', ' + match[1],
      time: match[4],
    };
  }

  var start = timestampParts(timeRange[0]);
  var end = timestampParts(timeRange[1]);
  // Keep the old time-only fallback for unexpected legacy timestamp shapes.
  if (!start) {
    var fallbackStart = String(timeRange[0]).split('T')[1] || String(timeRange[0]);
    var fallbackEnd = timeRange[1]
      ? (String(timeRange[1]).split('T')[1] || String(timeRange[1]))
      : '';
    return fallbackStart.substring(0, 8)
      + (fallbackEnd ? ' – ' + fallbackEnd.substring(0, 8) : '');
  }
  if (!end || (start.dayKey === end.dayKey && start.time === end.time)) {
    return start.date + ' · ' + start.time;
  }
  if (start.dayKey === end.dayKey) {
    return start.date + ' · ' + start.time + '–' + end.time;
  }
  return start.date + ' ' + start.time + ' – ' + end.date + ' ' + end.time;
}

function speciesNameArg(name) {
  if (name == null) return "''";
  return "'" + escapeHtml(String(name).replace(/\\/g, '\\\\').replace(/'/g, "\\'")) + "'";
}

var _speciesFilterRenderTimer = null;

function setSpeciesFilter(val) {
  speciesFilterText = val;
  speciesFilter = val;
  var clearBtn = document.getElementById('speciesFilterClear');
  if (clearBtn) clearBtn.style.display = val ? 'block' : 'none';
  persistPipelineReviewViewState();
  clearTimeout(_speciesFilterRenderTimer);
  _speciesFilterRenderTimer = setTimeout(renderResults, 120);
}

function toggleSpeciesFilterSearchOption(option) {
  VireoTextSearch.toggle('speciesFilter', option, function(options) {
    speciesFilterSearchOptions = options;
    persistPipelineReviewViewState();
    renderResults();
  });
}

function clearSpeciesFilter() {
  speciesFilter = '';
  speciesFilterText = '';
  var input = document.getElementById('speciesFilterInput');
  if (input) input.value = '';
  var clearBtn = document.getElementById('speciesFilterClear');
  if (clearBtn) clearBtn.style.display = 'none';
  persistPipelineReviewViewState();
  renderResults();
}

function encounterMatchesSearch(enc, photoMap) {
  if (!speciesFilter) return true;
  var threshold = minConfidence / 100;
  var fields = [];

  // When Hide confirmed is on, the render loop below skips confirmed bursts
  // (and their photos). Search must scope its burst-level fields — per-burst
  // confirmed overrides, per-burst predictions, and per-photo filenames — to
  // only the bursts that will actually render. Otherwise a query matching a
  // hidden confirmed burst's species or filename surfaces the encounter and
  // shows only the unrelated unconfirmed burst's content.
  var bursts = enc.bursts || [];
  function burstVisible(burst) {
    return !hideConfirmed || !isBurstConfirmed(enc, burst);
  }

  // Manually assigned encounter label. The encounter widget still renders
  // (even when some bursts hide), so it stays searchable — unless every burst
  // that carries the label is hidden. For mixed/partial encounters the
  // serializer keeps `confirmed_species` even when `species_confirmed` is
  // false (for replacement flows), so the label may only be backed by a
  // per-burst confirmed override; if that override is on a hidden burst the
  // label must not surface the encounter.
  var confirmedLabelHasVisibleBurst = true;
  if (hideConfirmed && enc.confirmed_species && bursts.length > 0) {
    confirmedLabelHasVisibleBurst = bursts.some(function(b) {
      if (!burstVisible(b)) return false;
      if (b && b.species_override && b.species_override.confirmed === true) {
        return b.species_override.species === enc.confirmed_species;
      }
      // Bursts without an override inherit and display `enc.confirmed_species`
      // whenever it is set (see renderSpeciesWidget's burst branch), including
      // mixed encounters where the encounter itself is unconfirmed. Requiring
      // `species_confirmed` here would hide the label from search even though
      // the visible burst is still showing it as its own label.
      return !(b && b.species_override);
    });
  }
  if (confirmedLabelHasVisibleBurst) fields.push(enc.confirmed_species);

  // Confirmed per-burst overrides — manual labels that always bypass the
  // slider, but only from bursts that will render.
  bursts.forEach(function(burst) {
    if (!burstVisible(burst)) return;
    if (burst.species_override && burst.species_override.confirmed === true) {
      fields.push(burst.species_override.species);
    }
  });

  // Classifier-derived species — including the encounter consensus label in
  // enc.species and burst-level predictions that back unconfirmed overrides —
  // flow through species_predictions with per-model confidence, so gate them
  // behind the min-confidence slider. Adding enc.species[0] separately would
  // bypass that gate and let low-confidence matches survive even after the
  // user raised the threshold.
  //
  // The encounter-level aggregate is rolled up from every photo in the
  // encounter, so under Hide confirmed it can carry a species contributed
  // only by a hidden confirmed burst. Restrict aggregate species to those
  // that at least one visible burst also predicts, so searching a species
  // whose only evidence is inside a hidden burst can't surface an unrelated
  // visible burst.
  // Support means: a visible burst has evidence the search should follow. A
  // confirmed burst override qualifies (manual label always counts), and so
  // does a burst-level prediction whose confidence meets the slider — matching
  // the gate applied to the aggregate below. Recording every visible burst
  // species regardless of confidence would let a hidden burst's high-confidence
  // prediction push the aggregate over the threshold while a below-threshold
  // sighting on the visible burst quietly keeps the encounter searchable.
  // Legacy raw bursts store `bursts` as arrays of photo IDs with no
  // per-burst prediction data. Gating the encounter aggregate against a
  // visible-burst set built from that shape would drop every
  // encounter-level prediction whenever Hide confirmed is on — even
  // though nothing is actually hidden. The strict gate is only sound
  // when we can fully account for what every visible burst contributes
  // to the aggregate, so require every VISIBLE burst to carry per-burst
  // predictions before enabling it. Otherwise (all-raw, or a mix where
  // a visible raw burst sits alongside an object burst — the GRM detach
  // flow leaves the source burst as a raw array while appending a new
  // object burst) at least one visible burst is opaque and could be the
  // source of an aggregate species we would otherwise drop.
  var visibleBursts = bursts.filter(burstVisible);
  var visibleBurstsAllCarryPredictions = visibleBursts.length > 0
    && visibleBursts.every(function(burst) {
      return burst && Array.isArray(burst.species_predictions);
    });
  var visibleBurstSpecies = null;
  if (hideConfirmed && visibleBurstsAllCarryPredictions) {
    visibleBurstSpecies = new Set();
    visibleBursts.forEach(function(burst) {
      if (burst && burst.species_override && burst.species_override.confirmed === true
          && burst.species_override.species) {
        visibleBurstSpecies.add(burst.species_override.species);
      }
      (burst && burst.species_predictions || []).forEach(function(sp) {
        if (!sp || !sp.species) return;
        if ((sp.models || []).some(function(m) { return m.confidence >= threshold; })) {
          visibleBurstSpecies.add(sp.species);
        }
      });
    });
  }
  // In the fallback path — mixed shapes where at least one visible burst is a
  // legacy raw photo-id array — we can't enumerate that burst's species. Do
  // not drop aggregate species that only hidden object bursts back either:
  // the GRM detach flow can leave the source burst raw while the detached
  // object burst is confirmed to a species that the raw burst still carries
  // (unconfirmed) in its own photos, so filtering that species out would
  // silence a legitimate raw-burst match. The tradeoff is a small false-
  // positive risk when the raw burst genuinely does not share the hidden
  // burst's species — the encounter still renders only the visible raw
  // burst, which the user can inspect.
  (enc.species_predictions || []).forEach(function(sp) {
    if (visibleBurstSpecies && !visibleBurstSpecies.has(sp.species)) return;
    if ((sp.models || []).some(function(m) { return m.confidence >= threshold; })) {
      fields.push(sp.species);
    }
  });
  bursts.forEach(function(burst) {
    if (!burstVisible(burst)) return;
    (burst.species_predictions || []).forEach(function(sp) {
      if ((sp.models || []).some(function(m) { return m.confidence >= threshold; })) {
        fields.push(sp.species);
      }
    });
  });

  // Filename search remains encounter-level: finding one photo keeps its
  // whole encounter visible so the surrounding burst context is not lost.
  // Under Hide confirmed, restrict to photos in bursts that will render.
  var filenamePids;
  if (bursts.length > 0) {
    filenamePids = [];
    bursts.forEach(function(burst) {
      if (!burstVisible(burst)) return;
      (burst.photo_ids || burst || []).forEach(function(pid) {
        filenamePids.push(pid);
      });
    });
  } else if (hideConfirmed && enc.species_confirmed) {
    filenamePids = [];
  } else {
    filenamePids = enc.photo_ids || [];
  }
  filenamePids.forEach(function(pid) {
    var photo = photoMap && photoMap[pid];
    if (photo) fields.push(photo.filename);
  });

  // Match the full query against each candidate field independently so a
  // multi-token query like "Mute Swan" can't be satisfied by pairing "Mute"
  // from one field (e.g. "Mute Grouse") with "Swan" from another (e.g.
  // "Trumpeter Swan"). A single field must contain every token to match.
  return fields.some(function(field) {
    return VireoTextSearch.matchesFields(field, speciesFilter, speciesFilterSearchOptions);
  });
}

function hasQualityScore(photo) {
  return photo && photo.quality_composite != null && isFinite(Number(photo.quality_composite));
}

function hasQualityResults() {
  if (!pipelineResults || pipelineResults.review_mode === 'species') return false;
  return (pipelineResults.photos || []).some(hasQualityScore);
}

function updatePipelineReviewSidebarVisibility() {
  var ss = document.getElementById('sidebarScoring');
  if (ss) ss.style.display = hasQualityResults() ? '' : 'none';
  var sg = document.getElementById('sidebarGrouping');
  // Species-only review has no burst/quality/keep/reject output, and
  // /api/pipeline/regroup-live always runs the full pipeline — nudging a
  // grouping slider here would clobber the all-REVIEW cache with quality
  // triage results, reintroducing the culling pipeline that identify mode
  // is meant to skip. Hide the grouping sidebar in that mode; users who
  // want to change grouping can switch to advanced mode and rerun.
  if (sg) sg.style.display = (pipelineResults && pipelineResults.review_mode === 'species') ? 'none' : '';
}

function toggleEncounter(idx) {
  var body = document.getElementById('encBody' + idx);
  if (!body) return;
  // Collapsing changes the retained node directly. A later results reload
  // resets collapse state, so it must not reuse the pre-toggle markup.
  var rendered = pipelineReviewRenderedCards.get(idx);
  if (rendered) rendered.html = null;
  var collapsed = body.style.display !== 'none';
  body.style.display = collapsed ? 'none' : '';
  var chev = document.getElementById('encChev' + idx);
  if (chev) chev.classList.toggle('collapsed', collapsed);
  var enc = pipelineResults && pipelineResults.encounters && pipelineResults.encounters[idx];
  var key = encounterKey(enc);
  if (key != null) {
    if (collapsed) collapsedEncounters.add(key);
    else collapsedEncounters.delete(key);
  }
}

function setFilter(f) {
  activeFilter = f;
  applyPipelineReviewToolbarState();
  persistPipelineReviewViewState();
  renderResults();
}

function isBurstConfirmed(enc, burst) {
  var ovr = burst && burst.species_override;
  if (ovr) return !!ovr.confirmed;
  return !!(enc && enc.species_confirmed);
}

// Photo IDs the user currently sees inside this encounter's body. When
// "Hide confirmed" is off, this is every photo in the encounter. When on,
// photos belonging to confirmed bursts (or a confirmed no-burst encounter)
// are excluded so encounter-level actions only touch what's on screen.
function visibleEncounterPhotoIds(enc) {
  var ids = (enc && enc.photo_ids) || [];
  if (!hideConfirmed) return ids.slice();
  if (enc.bursts && enc.bursts.length > 0) {
    var hidden = new Set();
    enc.bursts.forEach(function(burst) {
      if (isBurstConfirmed(enc, burst)) {
        (burst.photo_ids || burst || []).forEach(function(pid) { hidden.add(pid); });
      }
    });
    return ids.filter(function(pid) { return !hidden.has(pid); });
  }
  return enc.species_confirmed ? [] : ids.slice();
}

function toggleHideConfirmed() {
  hideConfirmed = !hideConfirmed;
  applyPipelineReviewToolbarState();
  persistPipelineReviewViewState();
  renderResults();
}

function toggleHideWithoutSuggestions() {
  hideWithoutSuggestions = !hideWithoutSuggestions;
  applyPipelineReviewToolbarState();
  persistPipelineReviewViewState();
  renderResults();
}

function togglePhotoLabels(show) {
  showPhotoLabels = !!show;
  applyPipelineReviewToolbarState();
  persistPipelineReviewViewState();
  renderResults();
}

var _confTimer = null;

function updateThumbSize(val, persist) {
  document.getElementById('encountersContainer').style.setProperty('--photo-card-size', val + 'px');
  if (persist !== false) persistPipelineReviewViewState();
}

function setMinConfidence(val) {
  minConfidence = parseInt(val, 10);
  document.getElementById('confSliderVal').textContent = minConfidence + '%';
  if (speciesFilter) renderResults();
  clearTimeout(_confTimer);
  _confTimer = setTimeout(function() {
    fetch('/api/workspaces/active/config')
      .then(function(r) { return r.json(); })
      .then(function(existing) {
        existing.review_min_confidence = minConfidence;
        return fetch('/api/workspaces/active/config', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(existing)
        });
      })
      .catch(function() {});
  }, 300);
}

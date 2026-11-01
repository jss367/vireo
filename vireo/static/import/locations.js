// Location suggestions: once the preview settles, read the files' GPS, show
// one suggested place per spot, and turn the user's choices into the
// request's location_choices. The import applies exactly what is on screen.
// Classic page script; load boot.js after all definitions.

/** @type {ImportLocationState} */
let importLocations = {
  signature: null, pending: false, error: '', reading: 0, naming: 0, nearby: 0, total: 0,
  data: null, choices: new Map(), abort: null,
};

function importLocationsEnabled() {
  const chk = /** @type {HTMLInputElement | null} */ (document.getElementById('chkLocationFromGps'));
  return !!chk && chk.checked;
}

// The preview on screen is final: discovery and the duplicate check are done
// and the controls still match it. Suggestions are only read for that list.
function importLocationPreviewReady() {
  return importPreviewCapturedSignature !== null && !importPreviewInFlight
    && !importDupStreamPending && !importPreviewFailed
    && importPreviewedPaths.length > 0
    && !importPreviewSignatureChanged(importPreviewCapturedSignature);
}

// Start waits for suggestions: what the panel shows is what the import does.
function importLocationsPending() {
  return importLocationsEnabled() && importLocationPreviewReady()
    && (importLocations.pending || importLocations.data === null) && !importLocations.error;
}

// The files Start would import: the checked cards in copy mode, every
// previewed file otherwise.
function importLocationCandidatePaths() {
  if (!importSelectionEnabled()) return new Set(importPreviewedPaths);
  return new Set(importSelectableCardPaths(importGridCards())
    .filter(p => !importDeselected.has(p)));
}

function cancelImportLocations() {
  if (importLocations.abort) importLocations.abort.abort();
  importLocations = {
    signature: null, pending: false, error: '', reading: 0, naming: 0, nearby: 0, total: 0,
    data: null, choices: new Map(), abort: null,
  };
}

// Called whenever the preview, the selection, or the checkbox changes.
function refreshImportLocations() {
  const panel = /** @type {HTMLElement | null} */ (document.getElementById('importLocationPanel'));
  if (!panel) return;
  if (!importLocationsEnabled()) {
    cancelImportLocations();
    panel.style.display = 'none';
    panel.innerHTML = '';
    return;
  }
  panel.style.display = '';
  if (!importLocationPreviewReady()) {
    if (importLocations.signature !== null) cancelImportLocations();
    panel.innerHTML = '';
    const note = document.createElement('div');
    note.className = 'hint';
    note.textContent = 'Location suggestions appear once the preview finishes.';
    panel.appendChild(note);
    return;
  }
  const signature = String(importPreviewCapturedSignature) + '|' + importPreviewedPaths.length;
  if (importLocations.signature !== signature) {
    cancelImportLocations();
    importLocations.signature = signature;
    void fetchImportLocations(signature);
    renderImportLocations();
    return;
  }
  updateImportLocationCounts();
}

/** @param {string} signature */
async function fetchImportLocations(signature) {
  const abort = new AbortController();
  importLocations.abort = abort;
  importLocations.pending = true;
  importLocations.total = new Set(importPreviewedPaths).size;
  try {
    const resp = await fetch('/api/import/location-suggestions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      signal: abort.signal,
      body: JSON.stringify({ paths: Array.from(new Set(importPreviewedPaths)) }),
    });
    if (!resp.ok) throw new Error((await resp.json()).error || 'location suggestions failed');
    const reader = /** @type {ReadableStream<Uint8Array>} */ (resp.body).getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    for (;;) {
      const r = await reader.read();
      if (r.done) break;
      buffer += decoder.decode(r.value, { stream: true });
      const parts = buffer.split('\n\n');
      buffer = parts.pop() || '';
      for (const part of parts) {
        const m = part.match(/^data: (.+)$/m);
        if (!m || importLocations.signature !== signature) continue;
        /** @type {ImportLocationFrame} */ const frame = JSON.parse(m[1] || '');
        if (frame.done) {
          if (frame.error) importLocations.error = frame.error;
          else importLocations.data = /** @type {ImportLocationSuggestions} */ (frame);
        } else {
          // GPS reading, then naming one GPS cell at a time, then looking
          // up named places near spots Google could only name broadly.
          if (frame.nearby != null) importLocations.nearby = Number(frame.nearby) || 0;
          else if (frame.naming != null) importLocations.naming = Number(frame.naming) || 0;
          else importLocations.reading = Number(frame.reading) || 0;
          importLocations.total = Number(frame.total) || importLocations.total;
          renderImportLocationStatus();
        }
      }
    }
    if (importLocations.signature !== signature) return;
    if (!importLocations.data && !importLocations.error) {
      importLocations.error = 'Location suggestions ended unexpectedly. Preview again to retry.';
    }
  } catch (e) {
    if (e instanceof Error && e.name === 'AbortError') return;
    if (importLocations.signature !== signature) return;
    importLocations.error = String(e instanceof Error ? e.message : e);
  } finally {
    if (importLocations.signature === signature) {
      importLocations.pending = false;
      importLocations.abort = null;
      renderImportLocations();
      updateStartGate();
    }
  }
}

/** @param {Set<string>} candidates @returns {Map<string, number>} */
function importLocationCellCounts(candidates) {
  /** @type {Map<string, number>} */ const counts = new Map();
  const data = importLocations.data;
  if (!data) return counts;
  candidates.forEach(path => {
    const cell = data.file_cells[path];
    if (cell !== undefined) counts.set(cell, (counts.get(cell) || 0) + 1);
  });
  return counts;
}

/** @param {ImportLocationGroup} group @param {Map<string, number>} counts */
function importLocationGroupCount(group, counts) {
  return group.cells.reduce((total, cell) => total + (counts.get(cell) || 0), 0);
}

// The text a group's input starts with: the suggestion, written as the
// picker writes the same place, so leaving it alone keeps the suggestion.
/** @param {ImportLocationGroup} group */
function importLocationSuggestedValue(group) {
  const suggestion = group.suggestion;
  if (!suggestion) return '';
  if (suggestion.kind === 'keyword') {
    const option = (importLocations.data?.location_options || [])
      .find(o => o.id === suggestion.keyword_id);
    return option ? option.label : suggestion.name;
  }
  return suggestion.name;
}

/** @param {ImportLocationGroup} group @returns {ImportLocationTarget} */
function importLocationTarget(group) {
  const state = importLocations.choices.get(group.key);
  const value = (state ? state.value : importLocationSuggestedValue(group)).trim();
  if (state && !state.enabled) return { kind: 'skip', reason: 'Skipped: these photos won’t get a location.' };
  if (!value) return { kind: 'skip', reason: 'Type a name to tag these photos, or leave it blank to skip them.' };
  const suggestion = group.suggestion;
  const folded = value.toLocaleLowerCase();
  // A nearby place the user clicked, for as long as the box still says it.
  // Checked before the suggestion-name branch: a nearby park can share its
  // normalized name with the broad suggestion's county but have a different
  // place_id, so matching on name alone would submit the wrong place.
  const picked = (group.nearby_places || []).find(p => p.place_id === state?.pick);
  if (picked && folded === picked.name.toLocaleLowerCase()) {
    return { kind: 'google', place_id: picked.place_id, name: picked.name };
  }
  if (suggestion && folded === importLocationSuggestedValue(group).toLocaleLowerCase()) {
    if (suggestion.kind === 'keyword') {
      return { kind: 'keyword', keyword_id: suggestion.keyword_id, name: suggestion.name };
    }
    return { kind: 'google', place_id: suggestion.place_id, name: suggestion.name };
  }
  const options = importLocations.data?.location_options || [];
  // Labels are unique; a bare name may be shared by several saved places.
  const byLabel = options.find(o => o.label.toLocaleLowerCase() === folded);
  const byName = options.filter(o => o.name.toLocaleLowerCase() === folded);
  const option = byLabel || (byName.length === 1 ? byName[0] : null);
  if (option) return { kind: 'keyword', keyword_id: option.id, name: option.name };
  if (byName.length > 1) {
    return { kind: 'skip', reason: byName.length + ' saved locations are named “' + value
      + '”; pick one from the list to tag these photos.' };
  }
  if (value.includes('|')) {
    return { kind: 'skip', reason: 'Location names can’t contain “|”; these photos will be skipped.' };
  }
  return { kind: 'new', name: value, latitude: group.latitude, longitude: group.longitude };
}

/** @param {ImportLocationTarget} target */
function importLocationOutcomeText(target) {
  if (target.kind === 'skip') return target.reason;
  if (target.kind === 'keyword') return 'Will add your location “' + target.name + '”.';
  // Not "new": Vireo reuses a keyword that already names this Google place
  // or area (Mission Beach as a parent, say).
  if (target.kind === 'google') return 'Will add “' + target.name + '” from Google Maps.';
  return 'Will create a new location “' + target.name + '”.';
}

/** @param {ImportLocationGroup} group */
function importLocationSourceText(group) {
  const s = group.suggestion;
  if (!s) {
    const reasons = group.unresolved_reasons || {};
    if (reasons.no_api_key) {
      return 'No suggestion: you haven’t tagged photos here, and Google Maps lookups need an API key in Settings.';
    }
    if (reasons.transient_error) {
      return 'No suggestion: Google Maps didn’t answer for these spots. Preview again to retry.';
    }
    return 'No suggestion: you haven’t tagged photos here, and Google Maps has no name for it.';
  }
  if (s.source === 'your_photos') {
    const n = Number(s.photo_count) || 0;
    return n.toLocaleString() + ' of your photo' + (n === 1 ? '' : 's')
      + ' taken here ' + (n === 1 ? 'has' : 'have') + ' this location.';
  }
  if (s.source === 'your_place') {
    return 'One of your saved locations' + (s.distance_m != null ? ', ' + s.distance_m + ' m away.' : '.');
  }
  const where = s.summary && s.summary !== s.name ? ' (' + s.summary + ')' : '';
  return (s.source === 'google_park' ? 'Nearest well-known park on Google Maps' : 'Area name from Google Maps') + where + '.';
}

function renderImportLocationStatus() {
  const status = /** @type {HTMLElement | null} */ (document.getElementById('importLocationStatus'));
  if (!status) return;
  if (importLocations.error) {
    status.textContent = 'Couldn’t suggest locations: ' + importLocations.error
      + ' Photos will import without locations.';
    return;
  }
  const data = importLocations.data;
  if (!data) {
    const n = importLocations.nearby || importLocations.naming;
    status.textContent = importLocations.nearby
      ? 'Finding named places near spots Google Maps only names broadly… '
        + n.toLocaleString() + ' of ' + importLocations.total.toLocaleString()
      : importLocations.naming
        ? 'Naming spots… ' + n.toLocaleString() + ' of ' + importLocations.total.toLocaleString()
        : 'Reading GPS… ' + importLocations.reading.toLocaleString()
          + ' of ' + importLocations.total.toLocaleString() + ' files';
    return;
  }
  const problem = importLocationChoicesProblem();
  if (problem) {
    status.textContent = problem;
    return;
  }
  const candidates = importLocationCandidatePaths();
  const counts = importLocationCellCounts(candidates);
  const noGps = new Set(data.no_gps_paths);
  let withGps = 0;
  let missing = 0;
  candidates.forEach(path => {
    if (data.file_cells[path] !== undefined) withGps += 1;
    else if (noGps.has(path)) missing += 1;
  });
  const spots = data.groups.filter(g => importLocationGroupCount(g, counts) > 0).length;
  if (!withGps) {
    status.textContent = 'None of the photos to import have GPS, so none will get a location.';
    return;
  }
  status.textContent = withGps.toLocaleString() + ' photo' + (withGps === 1 ? '' : 's')
    + ' to import ' + (withGps === 1 ? 'has' : 'have') + ' GPS, in ' + spots + ' group'
    + (spots === 1 ? '' : 's') + '. Each checked group gets the location shown when you import;'
    + ' change a name or uncheck a group to skip it.'
    + (missing ? ' ' + missing.toLocaleString() + ' without GPS won’t get one.' : '')
    + ' Photos that already have a location keep it.';
}

/** @param {number} meters */
function importLocationDistanceText(meters) {
  return meters < 1000 ? meters + ' m' : (meters / 1000).toFixed(1) + ' km';
}

// Named places near spots Google Maps could only name broadly (a county or
// state). Offered, not chosen: a pin is one point, so the distance is all
// Vireo knows. 5 km is NEARBY_RADIUS_M in services/import_locations.py.
/** @param {ImportLocationGroup} group @param {(place: ImportLocationNearbyPlace) => void} onPick */
function renderImportLocationNearby(group, onPick) {
  const places = group.nearby_places;
  if (!places) return null;
  const box = document.createElement('div');
  box.className = 'import-location-nearby';
  const note = document.createElement('div');
  note.className = 'hint';
  if (!places.length) {
    note.textContent = group.nearby_unresolved === 'no_api_key'
      ? 'Google Maps only has a broad area name for these spots. Add a Google Maps API key in Settings to see named places nearby.'
      : group.nearby_unresolved === 'transient_error'
        ? 'Google Maps only has a broad area name for these spots, and didn’t answer when asked for named places nearby. Preview again to retry.'
        : 'Google Maps only has a broad area name for these spots, and no visited park or natural feature within 5 km of them.';
    box.appendChild(note);
    return box;
  }
  note.textContent = 'Google Maps only has a broad area name for these spots. Named places nearby,'
    + ' with the distance from the closest of these photos to the place’s map pin'
    + ' (a pin is one point, so Vireo can’t tell whether the photos were taken inside the place):';
  const list = document.createElement('div');
  list.className = 'import-location-nearby-list';
  places.forEach(place => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'btn import-location-nearby-place';
    btn.textContent = place.name + ' · ' + importLocationDistanceText(place.distance_m);
    btn.title = (place.type === 'natural_feature' ? 'Natural feature' : 'Park or recreation area')
      + ' on Google Maps. The photos keep their own GPS.';
    btn.addEventListener('click', () => onPick(place));
    list.appendChild(btn);
  });
  box.append(note, list);
  return box;
}

/** @param {ImportLocationGroup} group */
function renderImportLocationGroup(group) {
  const row = document.createElement('div');
  row.className = 'import-location-group';
  row.dataset.key = group.key;
  const toggle = document.createElement('input');
  toggle.type = 'checkbox';
  toggle.className = 'import-location-toggle';
  toggle.setAttribute('aria-label', 'Tag this group');
  const thumbs = document.createElement('div');
  thumbs.className = 'import-location-thumbs';
  group.sample_paths.slice(0, 3).forEach(path => {
    const img = document.createElement('img');
    img.loading = 'lazy';
    img.alt = '';
    img.src = '/api/import/folder-preview/thumbnail?path=' + encodeURIComponent(path);
    thumbs.appendChild(img);
  });
  const body = document.createElement('div');
  body.className = 'import-location-body';
  const count = document.createElement('div');
  count.className = 'import-location-count';
  const input = document.createElement('input');
  input.type = 'text';
  input.className = 'import-location-input';
  input.setAttribute('list', 'importLocationOptions');
  input.setAttribute('aria-label', 'Location for this group');
  input.placeholder = 'Location name';
  const state = importLocations.choices.get(group.key);
  input.value = state ? state.value : importLocationSuggestedValue(group);
  toggle.checked = state ? state.enabled : !!group.suggestion;
  const source = document.createElement('div');
  source.className = 'hint';
  source.textContent = importLocationSourceText(group);
  const outcome = document.createElement('div');
  outcome.className = 'hint import-location-outcome';
  /** @type {string | undefined} */ let pick = state ? state.pick : undefined;
  const remember = () => {
    importLocations.choices.set(group.key, { enabled: toggle.checked, value: input.value, pick });
    outcome.textContent = importLocationOutcomeText(importLocationTarget(group));
    renderImportLocationStatus();
    updateStartGate();
  };
  input.addEventListener('input', () => {
    if (input.value.trim()) toggle.checked = true;
    remember();
  });
  toggle.addEventListener('change', remember);
  outcome.textContent = importLocationOutcomeText(importLocationTarget(group));
  body.append(count, input, source, outcome);
  const nearby = renderImportLocationNearby(group, place => {
    pick = place.place_id;
    input.value = place.name;
    toggle.checked = true;
    remember();
  });
  if (nearby) body.appendChild(nearby);
  row.append(toggle, thumbs, body);
  return row;
}

function renderImportLocations() {
  const panel = /** @type {HTMLElement | null} */ (document.getElementById('importLocationPanel'));
  if (!panel) return;
  panel.innerHTML = '';
  const status = document.createElement('div');
  status.id = 'importLocationStatus';
  status.className = 'import-location-status';
  panel.appendChild(status);
  const data = importLocations.data;
  if (data) {
    const list = document.createElement('datalist');
    list.id = 'importLocationOptions';
    data.location_options.forEach(o => {
      const opt = document.createElement('option');
      opt.value = o.label;
      list.appendChild(opt);
    });
    panel.appendChild(list);
    data.groups.forEach(group => panel.appendChild(renderImportLocationGroup(group)));
  }
  updateImportLocationCounts();
}

// Selection changes only move counts; the rows (and any typing) stay put.
function updateImportLocationCounts() {
  renderImportLocationStatus();
  const data = importLocations.data;
  if (!data) return;
  const counts = importLocationCellCounts(importLocationCandidatePaths());
  data.groups.forEach(group => {
    const row = /** @type {HTMLElement | null} */ (document.querySelector(
      '.import-location-group[data-key="' + CSS.escape(group.key) + '"]'));
    if (!row) return;
    const n = importLocationGroupCount(group, counts);
    row.style.display = n ? '' : 'none';
    const count = /** @type {HTMLElement} */ (row.querySelector('.import-location-count'));
    count.textContent = n.toLocaleString() + ' photo' + (n === 1 ? '' : 's');
  });
}

// The request's location_choices: one entry per group the user is tagging.
/** @returns {ImportLocationChoice[]} */
function importLocationChoices() {
  const data = importLocations.data;
  if (!importLocationsEnabled() || !data) return [];
  const counts = importLocationCellCounts(importLocationCandidatePaths());
  /** @type {ImportLocationChoice[]} */ const choices = [];
  data.groups.forEach(group => {
    if (!importLocationGroupCount(group, counts)) return;
    const target = importLocationTarget(group);
    if (target.kind === 'skip') return;
    choices.push({ ...target, cells: group.cells.slice() });
  });
  return choices;
}

// Keep these admission limits aligned with services/import_locations.py.
/** @returns {string | null} */
function importLocationChoicesProblem() {
  if (importLocationsEnabled() && importLocations.data?.work_limit_error) {
    return importLocations.data.work_limit_error;
  }
  const choices = importLocationChoices();
  if (choices.length > 1000) {
    return 'Select at most 1,000 location groups. Uncheck groups, turn off GPS location suggestions, or import fewer files.';
  }
  const cells = new Set(choices.flatMap(choice => choice.cells));
  if (cells.size > 100000) {
    return 'Select at most 100,000 GPS cells. Uncheck groups, turn off GPS location suggestions, or import fewer files.';
  }
  return null;
}

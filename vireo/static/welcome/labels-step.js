// Step 2: pick a region and wildlife groups and download the species list.
// DOM is built with createElement/textContent (not innerHTML) so place names
// returned by the iNaturalist search can't inject markup.
// ES module.

import { byId, state } from './state.js';
import { errorText, safeEventSource, safeFetch } from './shared.js';
import { finishSetup } from './model-step.js';

export function showLabelsStep() {
  byId('stepModel').style.display = 'none';
  byId('stepLabels').style.display = 'block';
  loadTaxonGroups();
}

async function loadTaxonGroups() {
  var container = byId('taxonCheckboxes');
  container.textContent = '';
  try {
    var groups = /** @type {{key: string, name: string}[] | null} */ (await safeFetch('/api/labels/taxon-groups', {}));
    (groups || []).forEach(function(g) {
      var label = document.createElement('label');
      var cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.className = 'taxon-cb';
      cb.value = g.key;
      if (g.key === 'birds') cb.checked = true;
      label.appendChild(cb);
      label.appendChild(document.createTextNode(' ' + g.name));
      container.appendChild(label);
    });
  } catch(e) {
    var span = document.createElement('span');
    span.style.cssText = 'font-size:12px;color:var(--text-dim);';
    span.textContent = 'Could not load groups.';
    container.appendChild(span);
  }
}

function placeSearchInput() {
  return /** @type {HTMLInputElement} */ (byId('placeSearch'));
}

function fetchLabelsButton() {
  return /** @type {HTMLButtonElement} */ (byId('fetchLabelsBtn'));
}

export function searchPlacesDebounced() {
  if (state.selectedPlaceId) {
    state.selectedPlaceId = null;
    state.selectedPlaceName = '';
    fetchLabelsButton().disabled = true;
    placeSearchInput().style.borderColor = 'var(--border-secondary)';
  }
  if (state.placeSearchTimer) clearTimeout(state.placeSearchTimer);
  state.placeSearchTimer = setTimeout(searchPlaces, 300);
}

/** @param {string} cls @param {string} text */
function setDropdownMessage(cls, text) {
  var dropdown = byId('placeDropdown');
  dropdown.textContent = '';
  var div = document.createElement('div');
  div.className = cls;
  div.textContent = text;
  dropdown.appendChild(div);
}

async function searchPlaces() {
  var q = placeSearchInput().value.trim();
  var dropdown = byId('placeDropdown');
  if (q.length < 2) { dropdown.textContent = ''; return; }
  try {
    var places = /** @type {WelcomePlace[] | null} */ (await safeFetch('/api/labels/search-places?q=' + encodeURIComponent(q), {}));
    if (!places || places.length === 0) {
      setDropdownMessage('place-empty', 'No places found');
      return;
    }
    dropdown.textContent = '';
    places.slice(0, 8).forEach(function(p) {
      var dn = p.display_name || p.name || '';
      var opt = document.createElement('div');
      opt.className = 'place-option';
      opt.textContent = dn;
      opt.addEventListener('click', function() { selectPlace(p.id, dn); });
      dropdown.appendChild(opt);
    });
  } catch(e) {
    setDropdownMessage('place-error', 'Error: ' + errorText(e));
  }
}

/** @param {number | string} id @param {string} name */
function selectPlace(id, name) {
  state.selectedPlaceId = parseInt(String(id));
  state.selectedPlaceName = name;
  byId('placeDropdown').textContent = '';
  placeSearchInput().value = name;
  placeSearchInput().style.borderColor = 'var(--accent)';
  fetchLabelsButton().disabled = false;
}

/** @param {{model_id?: string, labels_file?: string} | null | undefined} precompute */
async function startWelcomeEmbeddingPrecompute(precompute) {
  if (!precompute || !precompute.model_id || !precompute.labels_file) return;
  try {
    await safeFetch('/api/jobs/precompute-embeddings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model_id: precompute.model_id,
        labels_file: precompute.labels_file,
      }),
    });
  } catch(e) {
    /* Non-fatal: the first classification run can still compute embeddings. */
  }
}

/** Follow the species-list job; on success warm embeddings, then finish setup. @param {string} jobId */
function followLabelsJob(jobId) {
  var btn = fetchLabelsButton();
  var status = byId('labelsStatus');
  var bar = byId('labelsBar');
  safeEventSource('/api/jobs/' + jobId + '/stream', {
    onProgress: function(p) {
      if (p.current_file) status.textContent = p.current_file;
      if (p.total > 0 && p.current > 0) {
        bar.style.width = Math.round((p.current / p.total) * 100) + '%';
      }
    },
    onComplete: function(result) {
      if (result.status === 'completed' && result.result) {
        bar.style.width = '100%';
        status.style.color = 'var(--accent)';
        var precompute = result.result.embedding_precompute;
        status.textContent = 'Done! ' + result.result.species_count +
          ' species downloaded.' + (precompute ? ' Pre-computing embeddings in background…' : ' Finishing setup…');
        Promise.resolve(startWelcomeEmbeddingPrecompute(precompute)).then(function() {
          setTimeout(finishSetup, 500);
        });
      } else {
        status.style.color = 'var(--danger)';
        status.textContent = 'Failed: ' + ((result.errors || []).join(', ') || 'unknown error');
        btn.disabled = false;
      }
    },
    onError: function() {
      status.style.color = 'var(--danger)';
      status.textContent = 'Connection lost during download. Please try again.';
      btn.disabled = false;
    }
  });
}

export async function fetchLabels() {
  if (!state.selectedPlaceId) return;
  /** @type {string[]} */
  var groups = [];
  document.querySelectorAll('.taxon-cb:checked').forEach(function(cb) { groups.push(/** @type {HTMLInputElement} */ (cb).value); });
  var status = byId('labelsStatus');
  if (groups.length === 0) {
    status.style.color = 'var(--text-dim)';
    status.textContent = 'Select at least one wildlife group.';
    return;
  }

  var btn = fetchLabelsButton();
  btn.disabled = true;
  status.style.color = 'var(--text-dim)';
  status.textContent = 'Starting download…';

  try {
    var data = /** @type {{job_id?: string} | null} */ (await safeFetch('/api/jobs/fetch-labels', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        place_id: state.selectedPlaceId,
        place_name: state.selectedPlaceName,
        taxon_groups: groups,
        observation_filter: 'research',
      }),
    }));

    if (!data || !data.job_id) {
      status.textContent = 'Could not start the download. Please try again.';
      btn.disabled = false;
      return;
    }

    byId('labelsProgress').style.display = 'block';
    byId('labelsBar').style.width = '0%';
    status.textContent = 'Fetching species…';
    followLabelsJob(data.job_id);
  } catch(e) {
    status.style.color = 'var(--danger)';
    status.textContent = 'Error: ' + errorText(e);
    btn.disabled = false;
  }
}

// The species-label download modal: place search, filters, and fetch.
// Classic page script; load boot.js after all definitions.

var pipelineSelectedPlaceId = null;
var pipelineSelectedPlaceName = '';
var pipelineLabelsSearchTimer = null;
var pipelineLabelsSearchRequestId = 0;
var pipelineLabelsFormLoaded = false;
var pipelineLabelsEscHandler = null;

function openPipelineLabelsModal() {
  var modal = document.getElementById('pipelineLabelsModal');
  if (!modal) return;
  modal.classList.add('open');
  resetPipelineLabelsStatus();
  if (!pipelineLabelsFormLoaded) {
    pipelineLabelsFormLoaded = true;
    loadPipelineTaxonGroups();
    loadPipelineObservationFilters();
  }
  pipelineLabelsEscHandler = function(e) {
    if (e.key === 'Escape') closePipelineLabelsModal();
  };
  document.addEventListener('keydown', pipelineLabelsEscHandler);
  setTimeout(function() {
    var input = document.getElementById('pipelinePlaceSearch');
    if (input) input.focus();
  }, 0);
}

function closePipelineLabelsModal() {
  var modal = document.getElementById('pipelineLabelsModal');
  if (modal) modal.classList.remove('open');
  if (pipelineLabelsEscHandler) {
    document.removeEventListener('keydown', pipelineLabelsEscHandler);
    pipelineLabelsEscHandler = null;
  }
}

function resetPipelineLabelsStatus() {
  var status = document.getElementById('pipelineFetchLabelsStatus');
  var progress = document.getElementById('pipelineFetchLabelsProgress');
  var fill = document.getElementById('pipelineFetchLabelsFill');
  var btn = document.getElementById('pipelineFetchLabelsBtn');
  if (status) {
    status.textContent = '';
    status.style.color = 'var(--text-dim)';
  }
  if (progress) progress.style.display = 'none';
  if (fill) fill.style.width = '0%';
  if (btn) btn.disabled = !pipelineSelectedPlaceId;
}

async function loadPipelineTaxonGroups() {
  var box = document.getElementById('pipelineTaxonCheckboxes');
  if (!box) return;
  box.textContent = 'Loading...';
  try {
    var groups = await safeFetch('/api/labels/taxon-groups', {}, { toast: false });
    box.textContent = '';
    groups.forEach(function(g) {
      var label = document.createElement('label');
      label.style.cssText = 'font-size:12px;color:var(--text-secondary);display:flex;align-items:center;gap:4px;cursor:pointer;';
      var cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.className = 'pipeline-taxon-cb';
      cb.value = g.key;
      cb.checked = g.key === 'birds';
      cb.style.cssText = 'accent-color:var(--accent);';
      label.appendChild(cb);
      label.appendChild(document.createTextNode(g.name));
      box.appendChild(label);
    });
  } catch(e) {
    box.innerHTML = '<span style="color:var(--danger);font-size:12px;">Failed to load taxa</span>';
  }
}

async function loadPipelineObservationFilters() {
  var box = document.getElementById('pipelineObservationFilterRadios');
  if (!box) return;
  box.textContent = 'Loading...';
  try {
    var filters = await safeFetch('/api/labels/observation-filters', {}, { toast: false });
    box.textContent = '';
    filters.forEach(function(f) {
      var label = document.createElement('label');
      label.style.cssText = 'font-size:12px;color:var(--text-secondary);display:flex;align-items:center;gap:4px;cursor:pointer;line-height:1.35;';
      var radio = document.createElement('input');
      radio.type = 'radio';
      radio.name = 'pipeline-obs-filter';
      radio.value = f.key;
      radio.checked = f.key === 'research';
      radio.style.cssText = 'accent-color:var(--accent);';
      label.appendChild(radio);
      label.appendChild(document.createTextNode(f.name + ' '));
      var desc = document.createElement('span');
      desc.style.cssText = 'color:var(--text-faint);';
      desc.textContent = '- ' + f.description;
      label.appendChild(desc);
      box.appendChild(label);
    });
  } catch(e) {
    box.innerHTML = '<span style="color:var(--danger);font-size:12px;">Failed to load observation filters</span>';
  }
}

function searchPipelinePlacesDebounced() {
  if (pipelineSelectedPlaceId) {
    pipelineSelectedPlaceId = null;
    pipelineSelectedPlaceName = '';
    var btn = document.getElementById('pipelineFetchLabelsBtn');
    var input = document.getElementById('pipelinePlaceSearch');
    if (btn) btn.disabled = true;
    if (input) input.style.borderColor = 'var(--border-secondary)';
  }
  resetPipelineLabelsStatus();
  if (pipelineLabelsSearchTimer) clearTimeout(pipelineLabelsSearchTimer);
  pipelineLabelsSearchTimer = setTimeout(searchPipelinePlaces, 300);
}

async function searchPipelinePlaces() {
  var input = document.getElementById('pipelinePlaceSearch');
  var dropdown = document.getElementById('pipelinePlaceDropdown');
  if (!input || !dropdown) return;
  var q = input.value.trim();
  if (q.length < 2) {
    dropdown.textContent = '';
    return;
  }
  var requestId = ++pipelineLabelsSearchRequestId;
  try {
    var places = await safeFetch('/api/labels/search-places?q=' + encodeURIComponent(q), {}, { toast: false });
    if (
      requestId !== pipelineLabelsSearchRequestId ||
      input.value.trim() !== q
    ) {
      return;
    }
    dropdown.textContent = '';
    if (places.length === 0) {
      var empty = document.createElement('div');
      empty.style.cssText = 'padding:8px 10px;font-size:12px;color:var(--text-dim);';
      empty.textContent = 'No places found';
      dropdown.appendChild(empty);
      return;
    }
    places.slice(0, 8).forEach(function(p) {
      var dn = p.display_name || p.name || '';
      var option = document.createElement('div');
      option.className = 'pipeline-place-option';
      option.textContent = dn;
      option.addEventListener('click', function() {
        selectPipelinePlace(p.id, dn);
      });
      dropdown.appendChild(option);
    });
  } catch(e) {
    if (
      requestId !== pipelineLabelsSearchRequestId ||
      input.value.trim() !== q
    ) {
      return;
    }
    dropdown.innerHTML = '<div style="padding:8px 10px;font-size:12px;color:var(--danger);">Search failed</div>';
  }
}

function selectPipelinePlace(id, name) {
  pipelineSelectedPlaceId = parseInt(id, 10);
  pipelineSelectedPlaceName = name;
  var dropdown = document.getElementById('pipelinePlaceDropdown');
  var input = document.getElementById('pipelinePlaceSearch');
  var btn = document.getElementById('pipelineFetchLabelsBtn');
  if (dropdown) dropdown.textContent = '';
  if (input) {
    input.value = name;
    input.style.borderColor = 'var(--accent)';
  }
  if (btn) btn.disabled = false;
  resetPipelineLabelsStatus();
}

async function startPipelineLabelEmbeddingPrecompute(precompute) {
  if (!precompute || !precompute.model_id || !precompute.labels_file) return;
  var modelName = precompute.model_name || 'active model';
  try {
    var data = await safeFetch('/api/jobs/precompute-embeddings', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        model_id: precompute.model_id,
        labels_file: precompute.labels_file
      }),
    }, { toast: false });
    showToast('Pre-computing embeddings for ' + modelName + ' in background', 'info');
    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onComplete: function(result) {
        if (result.status === 'completed') {
          showToast('Embeddings cached for ' + modelName, 'success');
          updateReadiness();
          schedulePlanRefresh();
        } else {
          showToast('Embedding precompute failed; classification can compute them later', 'warning');
        }
      },
      onError: function() {
        updateReadiness();
      }
    });
  } catch(e) {
    showToast('Species list downloaded; embedding precompute did not start', 'warning');
  }
}

async function fetchPipelineLabels() {
  if (!pipelineSelectedPlaceId) return;
  var groups = [];
  document.querySelectorAll('.pipeline-taxon-cb:checked').forEach(function(cb) {
    groups.push(cb.value);
  });
  if (groups.length === 0) {
    var noGroupsStatus = document.getElementById('pipelineFetchLabelsStatus');
    if (noGroupsStatus) {
      noGroupsStatus.textContent = 'Select at least one taxon group.';
      noGroupsStatus.style.color = 'var(--danger)';
    }
    return;
  }

  var btn = document.getElementById('pipelineFetchLabelsBtn');
  var status = document.getElementById('pipelineFetchLabelsStatus');
  var progress = document.getElementById('pipelineFetchLabelsProgress');
  var fill = document.getElementById('pipelineFetchLabelsFill');
  if (btn) btn.disabled = true;
  if (status) {
    status.textContent = 'Starting download...';
    status.style.color = 'var(--text-dim)';
  }
  if (progress) progress.style.display = '';
  if (fill) fill.style.width = '0%';

  try {
    var data = await safeFetch('/api/jobs/fetch-labels', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        place_id: pipelineSelectedPlaceId,
        place_name: pipelineSelectedPlaceName,
        taxon_groups: groups,
        observation_filter: (document.querySelector('input[name="pipeline-obs-filter"]:checked') || {}).value || 'research',
      }),
    }, { toast: false });
    if (status) status.textContent = 'Fetching species...';

    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(p) {
        if (status && p.current_file) status.textContent = p.current_file;
        if (fill && p.total > 0 && p.current > 0) {
          fill.style.width = Math.round((p.current / p.total) * 100) + '%';
        }
      },
      onComplete: function(result) {
        var newFile = result && result.result ? result.result.labels_file : '';
        if (result && result.status === 'completed' && result.result) {
          if (status) {
            status.textContent = 'Downloaded ' + (result.result.species_count || 0).toLocaleString() + ' species.';
            status.style.color = 'var(--accent)';
          }
          if (fill) fill.style.width = '100%';
          var precompute = result.result.embedding_precompute;
          var selectFiles = getSelectedLabelFiles();
          if (newFile && selectFiles.indexOf(newFile) === -1) selectFiles.push(newFile);
          var saveSelectionFailed = false;
          safeFetch('/api/labels/active', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({labels_files: selectFiles}),
          }, { toast: false }).catch(function(e) {
            saveSelectionFailed = true;
            if (status) {
              status.textContent = 'Downloaded, but could not save the active label selection: ' + (e.message || e);
              status.style.color = 'var(--warning)';
            }
          }).then(function() {
            return loadLabels({ selectFiles: selectFiles });
          }).then(function() {
            updateLabelsPickerState();
            updateReadiness();
            schedulePlanRefresh();
            if (precompute) startPipelineLabelEmbeddingPrecompute(precompute);
            if (!saveSelectionFailed) setTimeout(closePipelineLabelsModal, 900);
          });
        } else {
          if (status) {
            status.textContent = 'Failed: ' + ((result.errors || []).join(', ') || 'download did not complete');
            status.style.color = 'var(--danger)';
          }
        }
        if (btn) btn.disabled = !pipelineSelectedPlaceId;
      },
      onError: function() {
        if (status) {
          status.textContent = 'Connection lost while downloading labels.';
          status.style.color = 'var(--danger)';
        }
        if (btn) btn.disabled = !pipelineSelectedPlaceId;
      }
    });
  } catch(e) {
    if (status) {
      status.textContent = 'Error: ' + (e.message || e);
      status.style.color = 'var(--danger)';
    }
    if (btn) btn.disabled = !pipelineSelectedPlaceId;
  }
}

async function loadLabels(options) {
  try {
    options = options || {};
    var data = await safeFetch('/api/labels', {}, { toast: false });
    var div = document.getElementById('labelsPicker');
    var active = data.active || [];
    var activePaths = new Set(
      options.selectFiles || active.map(function(a) { return a.labels_file; })
    );

    div.textContent = '';
    if (!data.labels || data.labels.length === 0) {
      var empty = document.createElement('span');
      empty.style.cssText = 'color:var(--text-dim);font-size:12px;';
      empty.textContent = '(Tree of Life — all species)';
      div.appendChild(empty);
    } else {
      data.labels.forEach(function(l) {
        var isActive = activePaths.has(l.labels_file);
        var label = document.createElement('label');
        label.style.cssText = 'display:flex;align-items:center;gap:4px;color:var(--text-secondary);cursor:pointer;padding:2px 0;';
        var cb = document.createElement('input');
        cb.type = 'checkbox';
        cb.className = 'labels-run-cb';
        cb.value = l.labels_file;
        cb.checked = isActive;
        cb.onchange = function() { updateReadiness(); schedulePlanRefresh(); };
        cb.style.cssText = 'accent-color:var(--accent);';
        label.appendChild(cb);
        var usable = (l.usable_count !== undefined && l.usable_count !== null)
          ? l.usable_count : (l.species_count || 0);
        label.appendChild(document.createTextNode(' ' + l.name + ' (' + usable + ')'));
        div.appendChild(label);
      });
    }

    updateLabelsPickerState();
  } catch(e) {}
}

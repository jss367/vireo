/* ---------- Labels ---------- */
var selectedPlaceId = null;
var selectedPlaceName = '';
var searchTimer = null;

async function loadLabels() {
  try {
    var data = await safeFetch('/api/labels', {}, { toast: false });
    var labels = data.labels || [];
    var active = data.active || [];
    var activePaths = new Set(active.map(function(a) { return a.labels_file; }));

    if (labels.length === 0) {
      document.getElementById('labelsContent').innerHTML = '<span style="color:var(--text-faint);font-size:13px;">No species lists downloaded yet. Use the form below to download one.</span>';
      return;
    }

    var html = '';
    labels.forEach(function(l) {
      var isActive = activePaths.has(l.labels_file);
      var groups = (l.taxon_groups || []).map(function(g) { return g.charAt(0).toUpperCase() + g.slice(1); }).join(', ');

      html += '<div style="padding:8px 0;border-bottom:1px solid var(--border-subtle);display:flex;align-items:center;gap:8px;">';
      html += '<label style="display:flex;align-items:center;gap:8px;flex:1;cursor:pointer;">';
      html += '<input type="checkbox" class="label-active-cb" value="' + escapeAttr(l.labels_file) + '"' +
              (isActive ? ' checked' : '') + ' onchange="updateActiveLabels()" style="accent-color:var(--accent);">';
      html += '<div>';
      html += '<span style="font-size:13px;color:var(--text-primary);font-weight:600;">' + escapeHtml(l.name) + '</span>';
      // The count a run receives, not the count on disk: they differ when
      // names are shared between species, and the warning below says so.
      var usable = (l.usable_count !== undefined && l.usable_count !== null)
        ? l.usable_count : (l.species_count || 0);
      html += '<div style="font-size:11px;color:var(--text-dim);">' + usable + ' species &middot; ' + groups + '</div>';
      if (l.ambiguous_count) {
        html += '<div style="font-size:11px;color:var(--warning,#d08700);margin-top:2px;">' +
                l.ambiguous_count + ' name' + (l.ambiguous_count > 1 ? 's' : '') +
                ' shared by several species and skipped when classifying &middot; ' +
                'download this list again to split them by scientific name</div>';
      }
      html += '</div>';
      html += '</label>';
      html += '<button data-label-path="' + escapeAttr(l.labels_file) + '" data-label-name="' + escapeAttr(l.name) + '" onclick="deleteLabelSet(this.dataset.labelPath,this.dataset.labelName)" ' +
              'style="background:none;color:var(--danger);border:1px solid var(--danger);border-radius:4px;padding:3px 10px;font-size:11px;cursor:pointer;white-space:nowrap;" ' +
              'title="Delete this label set">Delete</button>';
      html += '</div>';
    });

    // Summary line
    var totalSpecies = active.reduce(function(sum, a) {
      return sum + ((a.usable_count !== undefined && a.usable_count !== null)
        ? a.usable_count : (a.species_count || 0));
    }, 0);
    if (active.length > 0) {
      html += '<div style="padding:8px 0;font-size:12px;color:var(--accent);">' +
              active.length + ' label set' + (active.length > 1 ? 's' : '') + ' active &middot; ~' +
              totalSpecies.toLocaleString() + ' species</div>';
    }

    document.getElementById('labelsContent').innerHTML = html;
  } catch(e) {
    document.getElementById('labelsContent').innerHTML = '<span style="color:var(--danger);font-size:13px;">Failed to load</span>';
  }
}

async function updateActiveLabels() {
  var paths = [];
  document.querySelectorAll('.label-active-cb:checked').forEach(function(cb) {
    paths.push(cb.value);
  });
  try {
    await safeFetch('/api/labels/active', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({labels_files: paths}),
    });
  } catch(e) {}
  loadLabels();
}

async function deleteLabelSet(labelsFile, name) {
  if (!confirm('Delete label set "' + name + '"?')) return;
  try {
    await safeFetch('/api/labels', {
      method: 'DELETE',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({labels_file: labelsFile}),
    });
  } catch(e) {}
  loadLabels();
}

async function loadTaxonGroups() {
  try {
    var groups = await safeFetch('/api/labels/taxon-groups', {}, { toast: false });
    var html = '';
    groups.forEach(function(g, i) {
      var checked = (g.key === 'birds') ? ' checked' : '';
      html += '<label style="font-size:12px;color:var(--text-secondary);display:flex;align-items:center;gap:4px;cursor:pointer;">' +
        '<input type="checkbox" class="taxon-cb" value="' + g.key + '"' + checked + ' style="accent-color:var(--accent);"> ' +
        g.name + '</label>';
    });
    document.getElementById('taxonCheckboxes').innerHTML = html;
  } catch(e) {}
}

async function loadObservationFilters() {
  try {
    var filters = await safeFetch('/api/labels/observation-filters', {}, { toast: false });
    var html = '';
    filters.forEach(function(f) {
      var checked = (f.key === 'research') ? ' checked' : '';
      html += '<label style="font-size:12px;color:var(--text-secondary);display:flex;align-items:center;gap:4px;cursor:pointer;">' +
        '<input type="radio" name="obs-filter" value="' + f.key + '"' + checked + ' style="accent-color:var(--accent);"> ' +
        f.name + ' <span style="color:var(--text-faint);">— ' + f.description + '</span></label>';
    });
    document.getElementById('observationFilterRadios').innerHTML = html;
  } catch(e) {}
}

function searchPlacesDebounced() {
  // Clear selection if user is typing something new
  if (selectedPlaceId) {
    selectedPlaceId = null;
    selectedPlaceName = '';
    document.getElementById('fetchLabelsBtn').disabled = true;
    document.getElementById('placeSearch').style.borderColor = 'var(--border-secondary)';
  }
  if (searchTimer) clearTimeout(searchTimer);
  searchTimer = setTimeout(searchPlaces, 300);
}

async function searchPlaces() {
  var q = document.getElementById('placeSearch').value.trim();
  var dropdown = document.getElementById('placeDropdown');
  if (q.length < 2) {
    dropdown.innerHTML = '';
    return;
  }
  try {
    var places = await safeFetch('/api/labels/search-places?q=' + encodeURIComponent(q), {}, { toast: false });
    if (places.length === 0) {
      dropdown.innerHTML = '<div style="padding:8px 10px;font-size:12px;color:var(--text-dim);">No places found</div>';
      return;
    }
    var html = '';
    places.slice(0, 8).forEach(function(p) {
      var dn = p.display_name || p.name || '';
      html += '<div style="padding:8px 10px;font-size:13px;color:var(--text-secondary);cursor:pointer;border-bottom:1px solid var(--border-secondary);" ' +
        'onmouseover="this.style.background=\'var(--bg-tertiary)\'" onmouseout="this.style.background=\'\'" ' +
        'data-place-id="' + p.id + '" data-place-name="' + escapeAttr(dn) + '" ' +
        'onclick="selectPlace(this.dataset.placeId, this.dataset.placeName)">' +
        escapeHtml(dn) + '</div>';
    });
    dropdown.innerHTML = html;
  } catch(e) {
    dropdown.innerHTML = '<div style="padding:8px 10px;font-size:12px;color:var(--danger);">Error: ' + escapeHtml(String(e.message || e)) + '</div>';
  }
}

function selectPlace(id, name) {
  selectedPlaceId = parseInt(id);
  selectedPlaceName = name;
  document.getElementById('placeDropdown').innerHTML = '';
  document.getElementById('placeSearch').value = name;
  document.getElementById('placeSearch').style.borderColor = 'var(--accent)';
  document.getElementById('fetchLabelsBtn').disabled = false;
}

async function fetchLabels() {
  if (!selectedPlaceId) return;
  var groups = [];
  document.querySelectorAll('.taxon-cb:checked').forEach(function(cb) {
    groups.push(cb.value);
  });
  if (groups.length === 0) { alert('Select at least one taxon group'); return; }

  var btn = document.getElementById('fetchLabelsBtn');
  var status = document.getElementById('fetchLabelsStatus');
  btn.disabled = true;
  status.textContent = 'Starting download...';

  try {
    var data = await safeFetch('/api/jobs/fetch-labels', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        place_id: selectedPlaceId,
        place_name: selectedPlaceName,
        taxon_groups: groups,
        observation_filter: (document.querySelector('input[name="obs-filter"]:checked') || {}).value || 'research',
      }),
    }, { toast: false });
    status.textContent = 'Fetching species...';

    var progressDiv = document.getElementById('fetchLabelsProgress');
    var progressFill = document.getElementById('fetchLabelsFill');
    progressDiv.style.display = '';

    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(p) {
        if (p.current_file) status.textContent = p.current_file;
        if (p.total > 0 && p.current > 0) {
          progressFill.style.width = Math.round((p.current / p.total) * 100) + '%';
        }
      },
      onComplete: function(result) {
        if (result.status === 'completed' && result.result) {
          status.textContent = 'Done! ' + result.result.species_count + ' species downloaded.';
          status.style.color = 'var(--accent)';
          progressFill.style.width = '100%';
          var precompute = result.result.embedding_precompute;
          if (precompute) {
            startBackgroundEmbeddingPrecompute(precompute, {
              status: status,
              progressDiv: progressDiv,
              progressFill: progressFill
            });
          }
        } else {
          status.textContent = 'Failed: ' + (result.errors || []).join(', ');
          status.style.color = 'var(--danger)';
        }
        btn.disabled = false;
        loadLabels();
        loadEmbeddingMatrix();
        if (!(result.status === 'completed' && result.result && result.result.embedding_precompute)) {
          setTimeout(function() { progressDiv.style.display = 'none'; }, 3000);
        }
      },
      onError: function() {
        status.textContent = 'Connection lost';
        btn.disabled = false;
      }
    });
  } catch(e) {
    status.textContent = 'Error: ' + e.message;
    btn.disabled = false;
  }
}

async function startBackgroundEmbeddingPrecompute(precompute, opts) {
  if (!precompute || !precompute.model_id || !precompute.labels_file) return;
  opts = opts || {};
  var status = opts.status;
  var progressDiv = opts.progressDiv;
  var progressFill = opts.progressFill;
  var modelName = precompute.model_name || 'active model';
  if (status) {
    status.textContent = 'Pre-computing embeddings for ' + modelName + ' in background...';
    status.style.color = 'var(--text-dim)';
  }
  if (progressDiv) progressDiv.style.display = '';
  if (progressFill) progressFill.style.width = '0%';

  try {
    var data = await safeFetch('/api/jobs/precompute-embeddings', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        model_id: precompute.model_id,
        labels_file: precompute.labels_file
      }),
    }, { toast: false });

    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(p) {
        if (status && p.current_file) {
          status.textContent = p.current_file.replace(/^Computing embeddings/, 'Pre-computing embeddings');
        }
        if (progressFill && p.total > 0) {
          progressFill.style.width = Math.round((p.current / p.total) * 100) + '%';
        }
      },
      onComplete: function(result) {
        if (result.status === 'completed') {
          if (status) {
            status.textContent = 'Embeddings cached for ' + modelName + '.';
            status.style.color = 'var(--accent)';
          }
          if (progressFill) progressFill.style.width = '100%';
        } else if (status) {
          var detail = (
            (result.errors && result.errors[0]) ||
            (result.failure && result.failure.message) ||
            'Unknown error'
          );
          // Keep the actionable explanation while hiding the usually long
          // ONNXRuntime diagnostic that follows it. The complete exception
          // remains available in Jobs and Logs.
          detail = detail.split(' Underlying error:')[0];
          status.textContent = 'Species list downloaded, but embedding precompute failed: ' + detail;
          status.style.color = 'var(--warning)';
        }
        loadEmbeddingMatrix();
        if (progressDiv) setTimeout(function() { progressDiv.style.display = 'none'; }, 3000);
      },
      onError: function() {
        if (status) {
          status.textContent = 'Species list downloaded; embedding precompute is still listed in Jobs.';
          status.style.color = 'var(--warning)';
        }
      }
    });
  } catch(e) {
    if (status) {
      status.textContent = 'Species list downloaded; embedding precompute did not start: ' + e.message;
      status.style.color = 'var(--warning)';
    }
    if (progressDiv) setTimeout(function() { progressDiv.style.display = 'none'; }, 3000);
  }
}

/* ---------- Taxonomy ---------- */
async function loadTaxonomy() {
  try {
    var d = await safeFetch('/api/taxonomy/info', {}, { toast: false });
    var html = '';
    if (d.available) {
      html += '<div style="display:flex;align-items:center;gap:12px;">';
      html += '<div>';
      html += '<div style="font-size:14px;color:var(--text-primary);">iNaturalist Taxonomy</div>';
      html += '<div style="font-size:12px;color:var(--text-dim);">~' + (d.taxa_count || 0).toLocaleString() + ' taxa';
      if (d.last_updated) html += ' &middot; Updated ' + d.last_updated;
      if (d.file_size) html += ' &middot; ' + formatBytes(d.file_size);
      html += '</div></div>';
      html += '<button onclick="downloadTaxonomy()" style="margin-left:auto;background:var(--bg-tertiary);color:var(--text-secondary);border:none;border-radius:4px;padding:6px 14px;font-size:12px;cursor:pointer;">Re-download</button>';
      html += '</div>';
    } else {
      html += '<div style="margin-bottom:8px;">';
      html += '<div style="font-size:13px;color:var(--text-dim);">No taxonomy downloaded.</div>';
      html += '<div style="font-size:12px;color:var(--text-dim);margin-top:4px;">The taxonomy adds taxonomic hierarchy to predictions (order, family, genus), enables filtering by group (e.g. Raptors, Waterfowl), and auto-types existing and newly-synced keywords as species. Classification works without it — if you don\'t need these features, you can skip this.</div>';
      html += '</div>';
      html += '<button onclick="downloadTaxonomy()" style="background:var(--accent);color:var(--accent-text);border:none;border-radius:4px;padding:8px 20px;font-size:13px;cursor:pointer;">Download iNaturalist Taxonomy</button>';
    }
    document.getElementById('taxonomyContent').innerHTML = html;
  } catch(e) {
    document.getElementById('taxonomyContent').innerHTML = '<span style="color:var(--danger);font-size:13px;">Failed to load</span>';
  }
}

async function downloadTaxonomy() {
  try {
    var data = await safeFetch('/api/jobs/download-taxonomy', { method: 'POST' }, { toast: false });
    document.getElementById('taxonomyContent').innerHTML = '<span style="color:var(--text-dim);font-size:13px;">Downloading... (check Jobs panel for progress)</span>';
    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onComplete: function() {
        loadTaxonomy();
      }
    });
  } catch(e) {}
}

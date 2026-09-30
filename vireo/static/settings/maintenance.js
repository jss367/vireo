/* ---------- Dashboard ---------- */

function formatBytes(b) {
  if (b == null || b === 0) return '0';
  if (b < 1024) return b + ' B';
  if (b < 1024 * 1024) return Math.round(b / 1024) + ' KB';
  if (b < 1024 * 1024 * 1024) return (b / (1024 * 1024)).toFixed(1) + ' MB';
  return (b / (1024 * 1024 * 1024)).toFixed(1) + ' GB';
}

/* ---------- Embedding Matrix ---------- */
async function loadEmbeddingMatrix() {
  var matrixEl = document.getElementById('matrixContent');
  if (!matrixEl) return;
  try {
    var data = await safeFetch('/api/embedding-matrix', {}, { toast: false });
    var models = data.models || [];
    var matrix = data.matrix || [];

    if (models.length === 0 || matrix.length === 0) {
      matrixEl.innerHTML =
        '<span style="color:var(--text-faint);font-size:13px;">Download a model and a species list to see the embedding matrix.</span>';
      return;
    }

    var html = '<table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:8px;">';
    html += '<thead><tr><th style="text-align:left;padding:6px 10px;color:var(--text-faint);font-weight:500;border-bottom:1px solid var(--border-primary);">Labels</th>';
    models.forEach(function(m) {
      html += '<th style="text-align:center;padding:6px 10px;color:var(--text-faint);font-weight:500;border-bottom:1px solid var(--border-primary);">' + escapeHtml(m.name) + '</th>';
    });
    html += '</tr></thead><tbody>';

    matrix.forEach(function(row) {
      html += '<tr>';
      html += '<td style="padding:6px 10px;border-bottom:1px solid var(--border-subtle);color:var(--text-secondary);">';
      html += escapeHtml(row.labels_name) + ' <span style="color:var(--text-ghost);">(' + row.species_count + ')</span>';
      if (row.unusable) {
        html += '<div style="font-size:11px;color:var(--warning,#d08700);">no usable species' +
                (row.skipped ? ' — all ' + row.skipped + ' names are shared by several species' : '') +
                '; download the list again above</div>';
      }
      html += '</td>';
      models.forEach(function(m) {
        var cell = row.models[m.id];
        html += '<td style="text-align:center;padding:6px 10px;border-bottom:1px solid var(--border-subtle);">';
        if (row.unusable) {
          html += '<span style="color:var(--text-faint);">—</span>';
        } else if (cell && cell.cached) {
          html += '<span style="color:var(--accent);">cached</span>';
        } else {
          html += '<button data-model-id="' + escapeAttr(m.id) + '" data-label-path="' + escapeAttr(row.labels_file) + '" onclick="precomputeEmbeddings(this.dataset.modelId,this.dataset.labelPath)" ' +
            'style="background:var(--bg-tertiary);color:var(--text-secondary);border:none;border-radius:3px;padding:3px 10px;font-size:11px;cursor:pointer;">Compute</button>';
        }
        html += '</td>';
      });
      html += '</tr>';
    });
    html += '</tbody></table>';
    matrixEl.innerHTML = html;
  } catch(e) {
    matrixEl.innerHTML = '<span style="color:var(--danger);font-size:13px;">Failed to load</span>';
  }
}

async function precomputeEmbeddings(modelId, labelsFile) {
  try {
    var data = await safeFetch('/api/jobs/precompute-embeddings', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({model_id: modelId, labels_file: labelsFile}),
    });
    // Watch for completion and refresh matrix
    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onComplete: function() {
        loadEmbeddingMatrix();
      }
    });
  } catch(e) {}
}

/* ---------- Preview Cache ---------- */
async function loadPreviewCacheStatus() {
  var statusEl = document.getElementById('previewCacheStatus');
  var warnEl = document.getElementById('previewCacheWarning');
  if (!statusEl) return;
  try {
    var d = await safeFetch('/api/preview-cache', {}, { toast: false });
    var usedMb = (d.total_size / 1024 / 1024).toFixed(1);
    var quotaMb = Math.round(d.quota_bytes / 1024 / 1024);
    var fileWord = d.count === 1 ? 'file' : 'files';
    statusEl.textContent = 'Current: ' + usedMb + ' / ' + quotaMb + ' MB (' + d.count + ' ' + fileWord + ')';
    if (warnEl) {
      var rec = d.recommended_mb || 0;
      // Quota=0 means "disabled" — that's a deliberate user choice, not a warning case.
      if (rec > 0 && quotaMb > 0 && quotaMb < rec) {
        warnEl.textContent = 'Cache is smaller than your library — previews will regenerate from RAW on every pipeline run. Recommended: ' + rec.toLocaleString() + ' MB.';
        warnEl.style.display = 'block';
      } else {
        warnEl.style.display = 'none';
      }
    }
  } catch(e) {
    statusEl.textContent = 'Failed to load';
    if (warnEl) warnEl.style.display = 'none';
  }
}

async function clearPreviewCache() {
  if (!confirm('Clear all cached preview images? They will regenerate on demand from the originals.')) return;
  var btn = document.getElementById('clearPreviewCacheBtn');
  if (btn) btn.disabled = true;
  try {
    await safeFetch('/api/preview-cache/clear', { method: 'POST' });
  } catch(e) {}
  if (btn) btn.disabled = false;
  loadPreviewCacheStatus();
}

/* ---------- Detection Cache ---------- */
async function loadDetectionCacheStats() {
  var el = document.getElementById('detectionCacheStats');
  if (!el) return;
  try {
    var d = await safeFetch('/api/detection-cache/stats', {}, { toast: false });
    var pc = d.photo_count || 0;
    var mc = d.model_count || 0;
    var photoWord = pc === 1 ? 'photo' : 'photos';
    var modelWord = mc === 1 ? 'model' : 'models';
    el.textContent = pc + ' ' + photoWord + ' \u00D7 ' + mc + ' ' + modelWord + ' cached';
  } catch(e) {
    el.textContent = 'Failed to load';
  }
}

/* ---------- Portable Computation Cache ---------- */
async function loadComputationCacheStatus() {
  var el = document.getElementById('computationCacheStatus');
  if (!el) return;
  try {
    var data = await safeFetch('/api/computation-cache', {}, { toast: false });
    var exportable = data.exportable || {};
    var runs = (exportable.detector_runs || 0) + (exportable.classifier_runs || 0);
    var stored = data.object_count || 0;
    el.textContent = runs.toLocaleString() + ' exportable runs; ' +
      stored.toLocaleString() + ' imported/local objects (' + formatBytes(data.total_bytes || 0) + ')';
  } catch (e) {
    el.textContent = 'Status unavailable';
  }
}

function selectedComputationCacheTypes() {
  var types = [];
  if (document.getElementById('cacheExportDetections').checked) types.push('detection');
  if (document.getElementById('cacheExportClassifications').checked) types.push('classification');
  return types;
}

function exportComputationCache() {
  var types = selectedComputationCacheTypes();
  if (!types.length) {
    alert('Select at least one result type to export.');
    return;
  }
  var action = document.getElementById('computationCacheAction');
  action.textContent = 'Preparing download…';
  window.location.href = '/api/computation-cache/export?types=' + encodeURIComponent(types.join(','));
  window.setTimeout(function() {
    action.textContent = 'Download requested. Legacy results without a portable runtime identity are skipped.';
  }, 500);
}

async function importComputationCache(file) {
  if (!file) return;
  var button = document.getElementById('computationCacheImportBtn');
  var action = document.getElementById('computationCacheAction');
  button.disabled = true;
  action.style.color = 'var(--text-dim)';
  action.textContent = 'Validating and applying ' + file.name + '…';
  var form = new FormData();
  form.append('file', file, file.name);
  try {
    var response = await fetch('/api/computation-cache/import', {
      method: 'POST',
      body: form,
    });
    var data = await response.json().catch(function() { return {}; });
    if (!response.ok) throw new Error(data.error || response.statusText || 'Import failed');
    action.style.color = 'var(--accent)';
    action.textContent = 'Imported ' + (data.added || 0).toLocaleString() +
      ' new objects; applied ' + (data.detector_runs_applied || 0).toLocaleString() +
      ' detector and ' + (data.classifier_runs_applied || 0).toLocaleString() +
      ' classifier runs to ' + (data.matched_photos || 0).toLocaleString() + ' matching photos.';
    if (data.pinned_older_runtime) {
      action.textContent += ' ' + data.pinned_older_runtime.toLocaleString() +
        ' reviewed older results were preserved.';
    }
    if (data.unknown_runtime) {
      action.textContent += ' ' + data.unknown_runtime.toLocaleString() +
        ' detection object(s) came from a runtime this install does not' +
        ' recognize and were kept in the local store until matching' +
        ' weights are installed.';
    }
    if (data.classifier_deferred_pending_detection) {
      action.textContent += ' ' +
        data.classifier_deferred_pending_detection.toLocaleString() +
        ' classification object(s) are waiting for a matching detector run' +
        ' — re-run classification after detection to apply them.';
    }
    await loadComputationCacheStatus();
  } catch (error) {
    action.style.color = 'var(--danger)';
    action.textContent = error && error.message ? error.message : 'Import failed';
  } finally {
    button.disabled = false;
  }
}

/* ---------- Embedding Cache ---------- */
async function loadEmbeddingCache() {
  try {
    var d = await safeFetch('/api/embedding-cache', {}, { toast: false });
    document.getElementById('embeddingCacheSize').textContent = formatBytes(d.total_size);
    var entries = d.entries || [];
    var count = entries.filter(entry => !entry.label_count).length;
    var labels = entries.reduce((sum, entry) => sum + (entry.label_count || 0), 0);
    var parts = [];
    if (count) parts.push(count + ' cached label ' + (count === 1 ? 'set' : 'sets'));
    if (labels) parts.push(labels.toLocaleString() + ' reusable species labels');
    document.getElementById('embeddingCacheCount').textContent = parts.join(' · ') || 'empty';
    document.getElementById('clearCacheBtn').style.display = entries.length > 0 ? '' : 'none';
  } catch(e) {}
}

async function clearEmbeddingCache() {
  if (!confirm('Clear all cached label embeddings? The next classification run will recompute them (slow on CPU).')) return;
  try {
    await safeFetch('/api/embedding-cache', { method: 'DELETE' });
  } catch(e) {}
  loadEmbeddingCache();
}

/* ---------- Scan Roots ---------- */
async function loadScanRoots() {
  try {
    var cfg = await safeFetch('/api/config', {}, { toast: false });
    var roots = cfg.scan_roots || [];
    if (roots.length === 0) {
      document.getElementById('scanRootsContent').innerHTML = '<span style="color:var(--text-faint);font-size:13px;">No directories scanned yet. Scan a folder on the Import page to save it here.</span>';
      return;
    }
    var html = '';
    roots.forEach(function(r, i) {
      html += '<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid var(--border-subtle);">';
      html += '<span style="font-size:13px;color:var(--text-secondary);flex:1;font-family:monospace;">' + escapeHtml(r) + '</span>';
      html += '<button onclick="removeScanRoot(' + i + ')" style="background:none;border:none;color:var(--text-faint);font-size:14px;cursor:pointer;padding:2px 6px;" title="Remove">&times;</button>';
      html += '</div>';
    });
    document.getElementById('scanRootsContent').innerHTML = html;
  } catch(e) {
    document.getElementById('scanRootsContent').innerHTML = '<span style="color:var(--danger);font-size:13px;">Failed to load</span>';
  }
}

async function removeScanRoot(index) {
  try {
    var cfg = await safeFetch('/api/config', {}, { toast: false });
    var roots = cfg.scan_roots || [];
    roots.splice(index, 1);
    await safeFetch('/api/config', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ scan_roots: roots }),
    });
  } catch(e) {}
  loadScanRoots();
}

async function cleanKeywords() {
  var status = document.getElementById('cleanKeywordsStatus');
  status.textContent = 'Checking...';
  status.style.color = '';
  try {
    var dupes = await safeFetch('/api/keywords/duplicates', {}, { toast: false });
    if (dupes.length === 0) {
      status.textContent = 'No duplicates found';
      status.style.color = 'var(--text-dim)';
      return;
    }

    // Build confirmation message
    var msg = dupes.length + ' duplicate group(s) found:\n\n';
    dupes.forEach(function(d) {
      var variants = d.variants.map(function(v) {
        return '"' + v.name + '" (' + v.photo_count + ' photos)';
      });
      msg += '  ' + variants.join(' + ') + '  \u2192  keep "' + d.keep + '"\n';
    });
    msg += '\nMerge all duplicates?';

    if (!confirm(msg)) {
      status.textContent = 'Cancelled';
      status.style.color = 'var(--text-dim)';
      return;
    }

    var data = await safeFetch('/api/keywords/clean', {method: 'POST'}, { toast: false });
    status.textContent = 'Merged ' + data.merged + ' duplicate(s)';
    status.style.color = 'var(--accent)';
  } catch(e) {
    status.textContent = 'Error: ' + e.message;
    status.style.color = 'var(--danger)';
  }
}

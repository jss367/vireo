// The preview, detection and label-embedding caches: status lines, clears,
// and the embedding matrix with its Compute buttons.

import { storageEscapeAttr, storageEscapeHtml, storageFormatBytes } from './format.js';
import { loadStoragePanel } from './panel.js';
import { safeEventSource, safeFetch } from './shared.js';

export async function loadPreviewCacheStatus() {
  var statusEl = /** @type {HTMLElement} */ (document.getElementById('previewCacheStatus'));
  var warnEl = /** @type {HTMLElement} */ (document.getElementById('previewCacheWarning'));
  try {
    var d = /** @type {{total_size: number, quota_bytes: number, count: number, recommended_mb?: number}} */ (
      await safeFetch('/api/preview-cache', {}, { toast: false }));
    var usedMb = (d.total_size / 1024 / 1024).toFixed(1);
    var quotaMb = Math.round(d.quota_bytes / 1024 / 1024);
    var fileWord = d.count === 1 ? 'file' : 'files';
    statusEl.textContent = 'Current: ' + usedMb + ' / ' + quotaMb + ' MB (' + d.count + ' ' + fileWord + ')';
    var rec = d.recommended_mb || 0;
    if (rec > 0 && quotaMb > 0 && quotaMb < rec) {
      warnEl.textContent = 'Recommended: ' + rec.toLocaleString() + ' MB for one preview per photo.';
      warnEl.style.display = 'block';
    } else {
      warnEl.style.display = 'none';
    }
  } catch(e) {
    statusEl.textContent = 'Failed to load';
    warnEl.style.display = 'none';
  }
}

export async function clearPreviewCache() {
  if (!confirm('Clear all cached preview images? They will regenerate on demand from the originals.')) return;
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('clearPreviewCacheBtn'));
  if (btn) btn.disabled = true;
  try {
    await safeFetch('/api/preview-cache/clear', { method: 'POST' });
  } catch(e) {}
  if (btn) btn.disabled = false;
  loadStoragePanel();
  loadPreviewCacheStatus();
}

export async function loadDetectionCacheStats() {
  var el = /** @type {HTMLElement} */ (document.getElementById('detectionCacheStats'));
  try {
    var d = /** @type {{photo_count?: number, model_count?: number}} */ (
      await safeFetch('/api/detection-cache/stats', {}, { toast: false }));
    var pc = d.photo_count || 0;
    var mc = d.model_count || 0;
    el.textContent = pc + ' ' + (pc === 1 ? 'photo' : 'photos') +
      ' x ' + mc + ' ' + (mc === 1 ? 'model' : 'models') + ' cached';
  } catch(e) {
    el.textContent = 'Failed to load';
  }
}

export async function loadEmbeddingCache() {
  try {
    var d = /** @type {{total_size: number, entries?: unknown[]}} */ (
      await safeFetch('/api/embedding-cache', {}, { toast: false }));
    /** @type {HTMLElement} */ (document.getElementById('embeddingCacheSize')).textContent = storageFormatBytes(d.total_size);
    var count = (d.entries || []).length;
    /** @type {HTMLElement} */ (document.getElementById('embeddingCacheCount')).textContent =
      count > 0 ? count + ' cached label ' + (count === 1 ? 'set' : 'sets') : 'empty';
    /** @type {HTMLButtonElement} */ (document.getElementById('clearCacheBtn')).style.display = count > 0 ? '' : 'none';
  } catch(e) {}
}

export async function clearEmbeddingCache() {
  if (!confirm('Clear all cached label embeddings? The next classification run will recompute them.')) return;
  try {
    await safeFetch('/api/embedding-cache', { method: 'DELETE' });
  } catch(e) {}
  loadStoragePanel();
  loadEmbeddingCache();
  loadEmbeddingMatrix();
}

export async function loadEmbeddingMatrix() {
  try {
    var data = /** @type {StorageEmbeddingMatrix} */ (await safeFetch('/api/embedding-matrix', {}, { toast: false }));
    var models = data.models || [];
    var matrix = data.matrix || [];

    if (models.length === 0 || matrix.length === 0) {
      /** @type {HTMLElement} */ (document.getElementById('matrixContent')).innerHTML =
        '<span style="color:var(--text-faint);font-size:13px;">Download a model and a species list to see the embedding matrix.</span>';
      return;
    }

    var html = '<table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:8px;">';
    html += '<thead><tr><th style="text-align:left;padding:6px 10px;color:var(--text-faint);font-weight:500;border-bottom:1px solid var(--border-primary);">Labels</th>';
    models.forEach(function(m) {
      html += '<th style="text-align:center;padding:6px 10px;color:var(--text-faint);font-weight:500;border-bottom:1px solid var(--border-primary);">' + storageEscapeHtml(m.name) + '</th>';
    });
    html += '</tr></thead><tbody>';

    matrix.forEach(function(row) {
      html += '<tr>';
      html += '<td style="padding:6px 10px;border-bottom:1px solid var(--border-subtle);color:var(--text-secondary);">';
      // species_count here is already the count a run receives (the
      // endpoint normalizes), so it only needs the unusable case — same
      // rule as the Settings matrix, which reads this one endpoint too.
      html += storageEscapeHtml(row.labels_name) + ' <span style="color:var(--text-ghost);">(' + row.species_count + ')</span>';
      if (row.unusable) {
        html += '<div style="font-size:11px;color:var(--warning,#d08700);">no usable species' +
                (row.skipped ? ' — all ' + row.skipped + ' names are shared by several species' : '') +
                '; download the list again in Settings &rarr; Labels</div>';
      }
      html += '</td>';
      models.forEach(function(m) {
        var cell = row.models[m.id];
        html += '<td style="text-align:center;padding:6px 10px;border-bottom:1px solid var(--border-subtle);">';
        if (row.unusable) {
          html += '<span style="color:var(--text-faint);">&mdash;</span>';
        } else if (cell && cell.cached) {
          html += '<span style="color:var(--accent);">cached</span>';
        } else {
          html += '<button class="embedding-compute-btn" data-model-id="' + storageEscapeAttr(m.id) +
            '" data-label-file="' + storageEscapeAttr(row.labels_file) + '" ' +
            'style="background:var(--bg-tertiary);color:var(--text-secondary);border:none;border-radius:3px;padding:3px 10px;font-size:11px;cursor:pointer;">Compute</button>';
        }
        html += '</td>';
      });
      html += '</tr>';
    });
    html += '</tbody></table>';
    /** @type {HTMLElement} */ (document.getElementById('matrixContent')).innerHTML = html;
    document.querySelectorAll('.embedding-compute-btn').forEach(function(btn) {
      btn.addEventListener('click', function() {
        var el = /** @type {HTMLElement} */ (btn);
        precomputeEmbeddings(el.dataset.modelId || '', el.dataset.labelFile || '');
      });
    });
  } catch(e) {
    /** @type {HTMLElement} */ (document.getElementById('matrixContent')).innerHTML = '<span style="color:var(--danger);font-size:13px;">Failed to load</span>';
  }
}

/** @param {string} modelId @param {string} labelsFile */
export async function precomputeEmbeddings(modelId, labelsFile) {
  try {
    var data = /** @type {{job_id: string}} */ (await safeFetch('/api/jobs/precompute-embeddings', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({model_id: modelId, labels_file: labelsFile}),
    }));
    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onComplete: function() {
        loadEmbeddingMatrix();
        loadEmbeddingCache();
        loadStoragePanel();
      },
    });
  } catch(e) {}
}

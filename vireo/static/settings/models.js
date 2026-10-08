async function loadPipelineModels() {
  var container = document.getElementById('pipelineModelsContainer');
  if (!container) return;
  try {
    var data = await safeFetch('/api/models/pipeline', {}, { toast: false });
    var html = '<table style="width:100%;border-collapse:collapse;">';
    html += '<tr style="border-bottom:1px solid var(--border-primary);font-size:11px;color:var(--text-dim);">';
    html += '<td style="padding:6px 8px;">Model</td><td>Role</td><td>Status</td><td>Size</td><td></td></tr>';
    data.models.forEach(function(m) {
      var statusColor;
      var statusText;
      if (m.status === 'downloaded') { statusColor = 'var(--accent)'; statusText = 'Downloaded'; }
      else if (m.status === 'corrupt' || m.status === 'incomplete') { statusColor = 'var(--danger)'; statusText = m.status.charAt(0).toUpperCase() + m.status.slice(1); }
      else if (m.status === 'repo cached') { statusColor = 'var(--warning)'; statusText = 'Repo only'; }
      else { statusColor = 'var(--warning)'; statusText = 'Not downloaded'; }

      html += '<tr style="border-bottom:1px solid var(--border-primary);">';
      html += '<td style="padding:8px;"><strong>' + m.name + '</strong><br><span style="color:var(--text-dim);font-size:11px;">' + m.description + '</span></td>';
      html += '<td style="padding:8px;color:var(--text-dim);">' + m.role + '</td>';
      html += '<td style="padding:8px;color:' + statusColor + ';">' + statusText + '</td>';
      html += '<td style="padding:8px;color:var(--text-dim);">' + (m.size || m.size_estimate) + '</td>';
      html += '<td style="padding:8px;text-align:right;">';
      if (m.status === 'downloaded') {
        html += '<button class="btn-sm btn-danger" onclick="deletePipelineModel(\'' + m.id + '\',\'' + m.name + '\')">Delete</button>';
      } else if (m.status === 'corrupt' || m.status === 'incomplete') {
        html += '<button class="btn-sm btn-danger" onclick="deletePipelineModel(\'' + m.id + '\',\'' + m.name + '\')">Delete</button> ';
        html += '<button class="btn-sm" onclick="downloadPipelineModel(\'' + m.id + '\',this)">Re-download</button>';
      } else {
        html += '<button class="btn-sm" onclick="downloadPipelineModel(\'' + m.id + '\',this)">Download</button>';
      }
      html += '</td></tr>';
    });
    html += '</table>';
    container.innerHTML = html;
  } catch(e) {
    container.textContent = 'Error loading models: ' + e;
  }
}

async function downloadPipelineModel(modelId, btn) {
  if (btn) { btn.disabled = true; btn.textContent = 'Downloading...'; }
  try {
    var data = await safeFetch('/api/models/pipeline/download', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({model_id: modelId}),
    }, { toast: false });
    if (data.job_id) {
      safeEventSource('/api/jobs/' + data.job_id + '/stream', {
        onProgress: function(p) {
          if (btn) btn.textContent = p.phase || 'Downloading...';
        },
        onComplete: function(result) {
          if (result.status === 'completed') {
            if (btn) { btn.textContent = 'Done!'; btn.style.color = 'var(--accent)'; }
          } else {
            if (btn) { btn.textContent = 'Failed'; btn.style.color = 'var(--danger)'; btn.disabled = false; }
          }
          setTimeout(loadPipelineModels, 500);
        },
        onError: function() {
          if (btn) { btn.textContent = 'Error'; btn.style.color = 'var(--danger)'; btn.disabled = false; }
        }
      });
    }
  } catch(e) {
    if (btn) { btn.textContent = 'Error'; btn.style.color = 'var(--danger)'; btn.disabled = false; }
  }
}

async function deletePipelineModel(modelId, modelName) {
  if (!confirm('Delete ' + modelName + ' weights?')) return;
  try {
    await safeFetch('/api/models/pipeline/delete', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({model_id: modelId}),
    });
    loadPipelineModels();
  } catch(e) {}
}

/* ---------- Models ---------- */
var _modelsById = {};
async function loadModels() {
  try {
    var data = await safeFetch('/api/models', {}, { toast: false });
    var models = data.models || [];
    var activeId = data.active_id;

    _modelsById = {};
    models.forEach(function(m) { _modelsById[m.id] = m; });

    if (models.length === 0) {
      document.getElementById('modelsContent').innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">No models available.</span>';
      return;
    }

    var html = '';
    models.forEach(function(m) {
      var isActive = m.id === activeId;
      var state = m.state || (m.downloaded ? 'ok' : 'missing');
      var missingOptional = m.missing_optional_files || [];
      // Unverified installs also expose missing_optional_files in
      // get_models() because they count as "downloaded", so gate on both
      // 'ok' and 'unverified' — otherwise the Retry-verification button
      // is the only action offered and it can't fetch optional artifacts
      // (see downloadModel vs verifyAllModels below).
      var hasMissingOptional = (state === 'ok' || state === 'unverified') && missingOptional.length > 0;
      var statusColor, statusText;
      if (state === 'ok' && hasMissingOptional) {
        statusColor = 'var(--warning)'; statusText = 'Downloaded — optional files available';
      } else if (state === 'ok') {
        statusColor = 'var(--accent)'; statusText = 'Downloaded';
      } else if (state === 'incomplete') {
        statusColor = 'var(--warning)'; statusText = 'Incomplete — repair available';
      } else if (state === 'unverified' && hasMissingOptional) {
        statusColor = 'var(--warning)'; statusText = 'Unverified — optional files available';
      } else if (state === 'unverified') {
        statusColor = 'var(--warning)'; statusText = 'Unverified — could not reach HuggingFace';
      } else {
        statusColor = 'var(--text-faint)'; statusText = 'Not downloaded';
      }
      var activeTag = isActive ? ' <span style="color:var(--accent);font-size:10px;font-weight:600;">ACTIVE</span>' : '';

      html += '<div style="padding:10px 0;border-bottom:1px solid var(--border-subtle);">';
      html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">';
      html += '<span style="font-size:14px;color:var(--text-primary);font-weight:600;">' + escapeHtml(m.name) + '</span>';
      html += '<span style="font-size:11px;color:var(--text-faint);">' + escapeHtml(m.architecture || m.model_str) + '</span>';
      if (m.parameters) html += '<span style="font-size:11px;color:var(--text-faint);">' + escapeHtml(m.parameters) + ' params</span>';
      if (m.size_mb) html += '<span style="font-size:11px;color:var(--text-faint);">' + (m.size_mb >= 1000 ? (m.size_mb / 1000).toFixed(1) + ' GB' : m.size_mb + ' MB') + '</span>';
      html += activeTag;
      html += '</div>';
      html += '<div style="font-size:12px;color:var(--text-dim);margin-bottom:6px;">' + escapeHtml(m.description || '') + '</div>';
      if (state === 'incomplete') {
        html += '<div style="font-size:11px;color:var(--warning);margin-bottom:6px;">Model files are missing or truncated. Click Repair to finish the download — already-downloaded files will not be re-fetched.</div>';
      } else if (state === 'unverified' && hasMissingOptional) {
        var reason = m.verify_skipped_reason ? ' (' + escapeHtml(m.verify_skipped_reason) + ')' : '';
        var missingList = missingOptional.map(escapeHtml).join(', ');
        html += '<div style="font-size:11px;color:var(--warning);margin-bottom:6px;">Files are present but SHA256 could not be checked against HuggingFace' + reason + ', and optional files are not installed: ' + missingList + '. Click Repair to fetch them and re-verify — Retry verification alone only re-checks hashes and cannot download the optional artifacts.</div>';
      } else if (state === 'unverified') {
        reason = m.verify_skipped_reason ? ' (' + escapeHtml(m.verify_skipped_reason) + ')' : '';
        html += '<div style="font-size:11px;color:var(--warning);margin-bottom:6px;">Files are present but SHA256 could not be checked against HuggingFace' + reason + '. The model should work; click Retry verification once the network is reachable.</div>';
      } else if (hasMissingOptional) {
        missingList = missingOptional.map(escapeHtml).join(', ');
        // Be specific about what Repair can actually do for each artifact.
        // label_descriptions.json has a second source (the upstream model
        // config), so Repair fixes it now even when our ONNX repo doesn't
        // carry it yet. The others really do have to land on HuggingFace
        // first, and saying otherwise would promise a no-op.
        var optionalHint = missingOptional.indexOf('label_descriptions.json') !== -1
          ? 'Common names fall back to taxonomy lookups, so some species show their scientific name; click Repair to fetch the mapping — Vireo derives it from the upstream model config if HuggingFace does not carry it yet.'
          : 'The model works for label-list classification without them; click Repair to fetch them once available on HuggingFace (e.g. to enable Tree of Life mode).';
        html += '<div style="font-size:11px;color:var(--warning);margin-bottom:6px;">Optional files not installed: ' + missingList + '. ' + optionalHint + '</div>';
      }
      html += '<div style="display:flex;align-items:center;gap:8px;">';
      html += '<span style="font-size:11px;color:' + statusColor + ';">' + statusText + '</span>';

      if ((state === 'ok' || state === 'unverified') && m.weights_path) {
        html += '<span style="font-size:11px;color:var(--text-invisible);">' + escapeHtml(m.weights_path) + '</span>';
      }

      html += '<span style="margin-left:auto;display:flex;gap:6px;">';
      if (state === 'incomplete' && m.source !== 'custom') {
        html += '<button data-model-id="' + escapeAttr(m.id) + '" onclick="downloadModel(this.dataset.modelId)" style="background:var(--warning);color:var(--accent-text);border:none;border-radius:4px;padding:4px 12px;font-size:11px;cursor:pointer;font-weight:600;">Repair</button>';
      } else if (state === 'unverified' && hasMissingOptional && m.source !== 'custom') {
        // Missing optionals need download_model (Repair); retry-verification
        // alone would leave them absent. Offer both so the user can pick a
        // lighter re-check once they've handled the optional artifacts.
        html += '<button data-model-id="' + escapeAttr(m.id) + '" onclick="downloadModel(this.dataset.modelId)" style="background:var(--warning);color:var(--accent-text);border:none;border-radius:4px;padding:4px 12px;font-size:11px;cursor:pointer;font-weight:600;">Repair</button>';
        html += '<button onclick="verifyAllModels()" style="background:var(--bg-tertiary);color:var(--text-secondary);border:none;border-radius:4px;padding:4px 12px;font-size:11px;cursor:pointer;">Retry verification</button>';
      } else if (state === 'unverified' && m.source !== 'custom') {
        html += '<button onclick="verifyAllModels()" style="background:var(--warning);color:var(--accent-text);border:none;border-radius:4px;padding:4px 12px;font-size:11px;cursor:pointer;font-weight:600;">Retry verification</button>';
      } else if (hasMissingOptional && m.source !== 'custom') {
        html += '<button data-model-id="' + escapeAttr(m.id) + '" onclick="downloadModel(this.dataset.modelId)" style="background:var(--warning);color:var(--accent-text);border:none;border-radius:4px;padding:4px 12px;font-size:11px;cursor:pointer;font-weight:600;">Repair</button>';
      } else if (state === 'missing' && m.source !== 'custom') {
        html += '<button data-model-id="' + escapeAttr(m.id) + '" onclick="downloadModel(this.dataset.modelId)" style="background:var(--accent);color:var(--accent-text);border:none;border-radius:4px;padding:4px 12px;font-size:11px;cursor:pointer;">Download</button>';
      }
      if (!isActive && (state === 'ok' || state === 'unverified')) {
        html += '<button data-model-id="' + escapeAttr(m.id) + '" onclick="setActiveModel(this.dataset.modelId)" style="background:var(--bg-tertiary);color:var(--text-secondary);border:none;border-radius:4px;padding:4px 12px;font-size:11px;cursor:pointer;">Use This</button>';
      }
      if (state !== 'missing' || m.source === 'custom') {
        html += '<button data-model-id="' + escapeAttr(m.id) + '" onclick="removeModel(this.dataset.modelId)" style="background:none;color:var(--danger);border:1px solid var(--danger);border-radius:4px;padding:4px 12px;font-size:11px;cursor:pointer;">Remove</button>';
      }
      html += '</span>';
      html += '</div>';
      // Progress bar (hidden until download starts)
      html += '<div id="modelProgress-' + escapeAttr(m.id) + '" style="display:none;margin-top:6px;">';
      html += '<div style="height:4px;background:var(--bg-tertiary);border-radius:2px;overflow:hidden;margin-bottom:4px;"><div id="modelProgressFill-' + escapeAttr(m.id) + '" style="height:100%;background:var(--accent);border-radius:2px;width:0%;transition:width 0.3s;"></div></div>';
      html += '<div id="modelProgressText-' + escapeAttr(m.id) + '" style="font-size:11px;color:var(--text-dim);"></div>';
      html += '</div>';
      html += '</div>';
    });
    document.getElementById('modelsContent').innerHTML = html;
  } catch(e) {
    document.getElementById('modelsContent').innerHTML = '<span style="color:var(--danger);font-size:13px;">Failed to load models</span>';
  }
}

async function downloadModel(modelId) {
  try {
    var data = await safeFetch('/api/jobs/download-model', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({model_id: modelId}),
    }, { toast: false });

    // Show inline progress
    var progressDiv = document.getElementById('modelProgress-' + modelId);
    var progressFill = document.getElementById('modelProgressFill-' + modelId);
    var progressText = document.getElementById('modelProgressText-' + modelId);
    if (progressDiv) progressDiv.style.display = '';

    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(p) {
        if (progressFill && p.total > 0 && p.current > 0) {
          var pct = Math.round((p.current / p.total) * 100);
          progressFill.style.width = pct + '%';
        }
        if (progressText && p.current_file) {
          progressText.textContent = p.current_file;
        }
      },
      onComplete: function(result) {
        if (result.status === 'completed') {
          if (progressText) progressText.textContent = 'Download complete!';
          if (progressFill) progressFill.style.width = '100%';
        } else {
          if (progressText) {
            progressText.textContent = 'Failed: ' + (result.errors || []).join(', ');
            progressText.style.color = 'var(--danger)';
          }
        }
        setTimeout(loadModels, 1000);
      },
      onError: function() {
        if (progressText) {
          progressText.textContent = 'Connection lost';
          progressText.style.color = 'var(--danger)';
        }
      }
    });
  } catch(e) {}
}

async function verifyAllModels() {
  var btn = document.getElementById('verifyAllBtn');
  var status = document.getElementById('verifyAllStatus');
  btn.disabled = true;
  status.textContent = 'Starting...';
  status.style.color = 'var(--text-dim)';
  try {
    var data = await safeFetch('/api/jobs/verify-all-models', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({}),
    }, { toast: false });
    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(p) {
        if (p.current_file) status.textContent = p.current_file;
      },
      onComplete: function(result) {
        btn.disabled = false;
        // A run where a model failed verification ends "failed" but still
        // carries the per-model result; show it rather than a bare failure.
        if ((result.status === 'completed' || result.status === 'failed') && result.result) {
          var ok = (result.result.ok || []).length;
          var failed = (result.result.failed || []);
          if (failed.length > 0) {
            status.textContent = ok + ' ok, ' + failed.length + ' failed: ' + failed.join(', ');
            status.style.color = 'var(--danger)';
          } else {
            status.textContent = 'All ' + ok + ' models verified';
            status.style.color = 'var(--accent)';
          }
        } else {
          status.textContent = 'Verification failed';
          status.style.color = 'var(--danger)';
        }
        loadModels();
      },
      onError: function() {
        btn.disabled = false;
        status.textContent = 'Connection lost';
        status.style.color = 'var(--danger)';
      }
    });
  } catch(e) {
    btn.disabled = false;
    status.textContent = 'Error: ' + e.message;
    status.style.color = 'var(--danger)';
  }
}

async function removeModel(modelId) {
  if (!confirm('Remove this model? Weights Vireo downloaded are deleted from disk; weights in your own folders are left in place.')) return;
  try {
    var result = await safeFetch('/api/models/' + encodeURIComponent(modelId), {
      method: 'DELETE',
    });
    if (result && result.kept_path) {
      showToast('Model removed from Vireo. Its weights were left in place at ' + result.kept_path, 'success');
    }
  } catch(e) {}
  loadModels();
}

async function setActiveModel(modelId) {
  var m = _modelsById[modelId];
  if (m && !m.downloaded) {
    var sizeStr = m.size_mb ? (m.size_mb >= 1000 ? (m.size_mb / 1000).toFixed(1) + ' GB' : m.size_mb + ' MB') : '';
    showToast('Model not downloaded — downloading' + (sizeStr ? ' (~' + sizeStr + ')' : '') + '...', 'success');
    try {
      var data = await safeFetch('/api/jobs/download-model', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({model_id: modelId}),
      }, { toast: false });

      var progressDiv = document.getElementById('modelProgress-' + modelId);
      var progressFill = document.getElementById('modelProgressFill-' + modelId);
      var progressText = document.getElementById('modelProgressText-' + modelId);
      if (progressDiv) progressDiv.style.display = '';

      safeEventSource('/api/jobs/' + data.job_id + '/stream', {
        onProgress: function(p) {
          if (progressFill && p.total > 0 && p.current > 0) {
            var pct = Math.round((p.current / p.total) * 100);
            progressFill.style.width = pct + '%';
          }
          if (progressText && p.current_file) {
            progressText.textContent = p.current_file;
          }
        },
        onComplete: function(result) {
          if (result.status === 'completed') {
            if (progressText) progressText.textContent = 'Download complete!';
            if (progressFill) progressFill.style.width = '100%';
            // Now set as active
            safeFetch('/api/models/active', {
              method: 'POST',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({model_id: modelId}),
            }).catch(function(){});
            setTimeout(loadModels, 1000);
          } else {
            if (progressText) {
              progressText.textContent = 'Failed: ' + (result.errors || []).join(', ');
              progressText.style.color = 'var(--danger)';
            }
            setTimeout(loadModels, 1000);
          }
        },
        onError: function() {
          if (progressText) {
            progressText.textContent = 'Connection lost';
            progressText.style.color = 'var(--danger)';
          }
        }
      });
    } catch(e) {}
    return;
  }
  try {
    await safeFetch('/api/models/active', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({model_id: modelId}),
    });
  } catch(e) {}
  loadModels();
}

async function addHfModel() {
  var repoId = document.getElementById('hfRepoId').value.trim();
  if (!repoId) return;
  // Clean up input — handle full URLs or repo IDs
  repoId = repoId.replace('https://huggingface.co/', '').replace(/\/$/, '');

  var status = document.getElementById('modelActionStatus');
  status.textContent = 'Starting download from HuggingFace...';
  status.style.color = 'var(--text-dim)';

  try {
    var data = await safeFetch('/api/jobs/download-hf-model', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({repo_id: repoId}),
    }, { toast: false });
    status.textContent = 'Downloading...';

    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(p) {
        if (p.current_file) status.textContent = p.current_file;
      },
      onComplete: function(result) {
        if (result.status === 'completed') {
          status.textContent = 'Downloaded! Model ready to use.';
          status.style.color = 'var(--accent)';
          document.getElementById('hfRepoId').value = '';
        } else {
          status.textContent = 'Failed: ' + (result.errors || []).join(', ');
          status.style.color = 'var(--danger)';
        }
        loadModels();
      },
      onError: function() {
        status.textContent = 'Connection lost';
      }
    });
  } catch(e) {
    status.textContent = 'Error: ' + e.message;
    status.style.color = 'var(--danger)';
  }
}

async function addCustomModel() {
  var name = document.getElementById('customModelName').value.trim();
  var path = document.getElementById('customModelPath').value.trim();
  if (!name || !path) return;
  try {
    await safeFetch('/api/models/custom', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name: name, weights_path: path}),
    });
    document.getElementById('customModelName').value = '';
    document.getElementById('customModelPath').value = '';
  } catch(e) {}
  loadModels();
}

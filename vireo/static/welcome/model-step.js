// Step 1: download the classification model, then finish setup or go on to
// the region step when classification still needs a species list.
// ES module.

import { MODEL_ID, byId, state } from './state.js';
import { safeEventSource, safeFetch } from './shared.js';
import { showLabelsStep } from './labels-step.js';

export async function checkModelStatus() {
  try {
    var data = /** @type {WelcomeModelStatus} */ (await safeFetch('/api/models/status', {}));
    var match = (data.available_models || []).find(function(m) { return m.id === MODEL_ID; });
    if (match && match.downloaded) {
      state.modelAlreadyDownloaded = true;
    }
    state.classificationReady = !!(data && data.classification && data.classification.labels_ready);
  } catch(e) {
    /* Ignore — we'll attempt the download when the user clicks */
  }
}

export async function getStarted() {
  var btn = /** @type {HTMLButtonElement} */ (byId('downloadBtn'));
  byId('errorContainer').style.display = 'none';

  if (state.modelAlreadyDownloaded) {
    btn.disabled = true;
    btn.textContent = 'Loading…';
    await proceedAfterModel();
    return;
  }

  btn.disabled = true;
  btn.textContent = 'Downloading...';

  var progressBar = byId('progressBar');
  var progressLabel = byId('progressLabel');
  byId('progressContainer').style.display = 'block';
  progressBar.style.width = '0%';
  progressLabel.textContent = 'Starting download...';

  try {
    var data = /** @type {{job_id?: string}} */ (await safeFetch('/api/jobs/download-model', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model_id: MODEL_ID }),
    }));

    if (!data.job_id) {
      showError('Failed to start download. Please try again.');
      return;
    }

    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(p) {
        if (p.total > 0) {
          var pct = Math.round((p.current / p.total) * 100);
          progressBar.style.width = pct + '%';
        }
        progressLabel.textContent = p.phase || p.current_file || 'Downloading model...';
      },
      onComplete: function(result) {
        if (result.status === 'completed') {
          progressBar.style.width = '100%';
          progressLabel.textContent = 'Download complete!';
          state.modelAlreadyDownloaded = true;
          proceedAfterModel();
        } else {
          var msg = (result.errors && result.errors[0]) || 'Download failed';
          showError(msg);
        }
      },
      onError: function() {
        showError('Connection lost during download. Please check your network and try again.');
      }
    });
  } catch(e) {
    showError((e instanceof Error && e.message) || 'Download failed. Please try again.');
  }
}

// An existing active BioCLIP model may still need a regional species list.
async function proceedAfterModel() {
  var ready;
  try {
    var status = /** @type {WelcomeModelStatus} */ (await safeFetch('/api/models/status', {}));
    ready = !!(status && status.classification && status.classification.labels_ready);
  } catch(e) {
    showError('Could not check model readiness. Please retry.');
    return;
  }
  if (ready) {
    await finishSetup();
    return;
  }
  showLabelsStep();
}

export async function finishSetup() {
  try {
    await safeFetch('/api/setup/complete', { method: 'POST' });
  } catch(e) {}
  window.location.href = '/browse';
}

/** @param {string} message */
function showError(message) {
  var btn = byId('downloadBtn');
  /** @type {HTMLButtonElement} */ (btn).disabled = false;
  btn.textContent = 'Retry model setup';
  byId('progressContainer').style.display = 'none';
  byId('errorMessage').textContent = message;
  byId('errorContainer').style.display = 'block';
}


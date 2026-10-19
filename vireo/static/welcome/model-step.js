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

/* The model is ready. The default model (BioCLIP-2.5) ships Tree of Life
   embeddings, so it classifies label-free and setup can finish straight away.
   The status endpoint reports classification usable when the active model is
   label-free (Tree of Life or timm) OR a non-empty label list is present;
   otherwise route the user to the labels step so the pipeline won't fail at
   the classify stage. */
async function proceedAfterModel() {
  var ready = false;
  try {
    var status = /** @type {WelcomeModelStatus} */ (await safeFetch('/api/models/status', {}));
    ready = !!(status && status.classification && status.classification.labels_ready);
  } catch(e) {
    /* On error, fall through to the labels step — better to offer it than
       to silently land the user in a configuration that can't classify. */
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
  btn.textContent = 'Get Started';
  byId('progressContainer').style.display = 'none';
  byId('errorMessage').textContent = message;
  byId('errorContainer').style.display = 'block';
  byId('skipLink').style.display = 'inline';
}


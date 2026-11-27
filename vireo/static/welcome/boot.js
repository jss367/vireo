// Page startup: the module the template loads. Binds the page controls, runs
// the ExifTool check, and enables Get Started
// once the model status is known (or resumes at the region step).
// ES module.

import { STARTER, byId, state } from './state.js';
import { checkExiftool } from './system-check.js';
import { checkModelStatus, finishSetup, getStarted } from './model-step.js';
import { fetchLabels, searchPlacesDebounced, showLabelsStep } from './labels-step.js';

// Starter details come from ``window.VIREO_WELCOME_STARTER`` (see state.js);
// the button renders with a placeholder label until this fills it in.
var downloadBtn = /** @type {HTMLButtonElement} */ (byId('downloadBtn'));
downloadBtn.dataset.modelId = STARTER.id;
// ``%g`` of ``size_mb / 1000`` in the previous Jinja template: strip trailing
// zeros from the GB value so 1200 MB reads "1.2 GB" and 1000 MB reads "1 GB".
var sizeGb = Number((STARTER.size_mb / 1000).toPrecision(6)).toString();
downloadBtn.textContent = 'Download ' + STARTER.name + ' · about ' + sizeGb + ' GB';

/** A link that runs `action` instead of navigating. @param {string} id @param {() => unknown} action */
function onLinkClick(id, action) {
  byId(id).addEventListener('click', function(event) {
    event.preventDefault();
    action();
  });
}

// Welcome has no navbar and so no showToast: the config-recovery notice from
// /config-defaults.js (window.VIREO_CONFIG_RECOVERY) renders into the static
// alert container below instead.
var recovery = /** @type {{id: string, message: string} | null | undefined} */ (
  window.VIREO_CONFIG_RECOVERY
);
if (recovery && recovery.message) {
  var noticeEl = byId('configRecoveryNotice');
  noticeEl.textContent = recovery.message;
  noticeEl.hidden = false;
}

byId('downloadBtn').addEventListener('click', function() { getStarted(); });
byId('retryBtn').addEventListener('click', function() { getStarted(); });
onLinkClick('skipLink', finishSetup);
byId('placeSearch').addEventListener('input', searchPlacesDebounced);
byId('fetchLabelsBtn').addEventListener('click', function() { fetchLabels(); });
onLinkClick('labelsSkip', finishSetup);

checkExiftool();

checkModelStatus().then(function() {
  // Resume mid-onboarding: if the model is already on disk but classification
  // isn't usable yet (no labels), jump straight to the labels step rather than
  // making the user re-click "Get Started".
  if (state.modelAlreadyDownloaded && !state.classificationReady) {
    showLabelsStep();
    return;
  }
  var button = /** @type {HTMLButtonElement} */ (byId('downloadBtn'));
  if (state.modelAlreadyDownloaded) button.textContent = 'Continue';
  button.disabled = false;
});

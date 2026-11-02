// Page startup: the module the template loads. Binds the page controls, runs
// the ExifTool check, and enables Get Started
// once the model status is known (or resumes at the region step).
// ES module.

import { byId, state } from './state.js';
import { checkExiftool } from './system-check.js';
import { checkModelStatus, finishSetup, getStarted } from './model-step.js';
import { fetchLabels, searchPlacesDebounced, showLabelsStep } from './labels-step.js';

/** A link that runs `action` instead of navigating. @param {string} id @param {() => unknown} action */
function onLinkClick(id, action) {
  byId(id).addEventListener('click', function(event) {
    event.preventDefault();
    action();
  });
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
  /** @type {HTMLButtonElement} */ (byId('downloadBtn')).disabled = false;
});

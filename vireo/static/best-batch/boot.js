// Page startup: the module the template loads. Binds the page controls and
// the delegated card clicks, then loads the batch.
// ES module.

import { bindFocusControls } from './focus.js';
import { byId, state } from './state.js';
import { analyzeSubjects, changeSubject, startComparison } from './subjects.js';
import { bindCardClicks } from './render.js';
import {
  flagBest, flagBestRejectRest, loadBatch, openReviewBurst,
} from './actions.js';

/** @param {string} id @param {() => unknown} action */
function onClick(id, action) {
  byId(id).addEventListener('click', function() { action(); });
}

onClick('backToBrowseBtn', function() { window.location.href = '/browse'; });
onClick('reviewBtn', openReviewBurst);
onClick('flagBtn', flagBest);
onClick('rejectBtn', flagBestRejectRest);
bindFocusControls();
onClick('compareSubjectsBtn', startComparison);
onClick('analyzeSubjectsBtn', analyzeSubjects);
byId('subjectChoices').addEventListener('change', changeSubject);
byId('requestedSpecies').addEventListener('input', function() {
  state.requestSequence++;
  state.subjectOverrides = {};
  state.bestBatchData = null;
  ['flagBtn', 'rejectBtn', 'reviewBtn'].forEach(function(id) {
    /** @type {HTMLButtonElement} */ (byId(id)).disabled = true;
  });
  byId('comparisonStatus').textContent = 'Species changed. Compare subjects to refresh the ranking.';
});
bindCardClicks();

loadBatch();

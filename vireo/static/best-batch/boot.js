// Page startup: the module the template loads. Binds the controls that used
// to carry inline handlers and the delegated card clicks, then loads the batch.
// ES module.

import { byId } from './state.js';
import { bindCardClicks } from './render.js';
import {
  flagBest, flagBestRejectRest, loadBatch, openInspectedLightbox, openReviewBurst,
} from './actions.js';

/** @param {string} id @param {() => unknown} action */
function onClick(id, action) {
  byId(id).addEventListener('click', function() { action(); });
}

onClick('backToBrowseBtn', function() { window.location.href = '/browse'; });
onClick('reviewBtn', openReviewBurst);
onClick('flagBtn', flagBest);
onClick('rejectBtn', flagBestRejectRest);
onClick('inspectImg', openInspectedLightbox);
bindCardClicks();

loadBatch();

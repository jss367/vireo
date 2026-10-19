// Page startup: the module the template loads. Binds the controls that used
// to carry inline handlers, fills the source from the import page's link,
// and loads the volume suggestions.
// ES module.

import { cardCleanupCancelJob, cardCleanupCloseConfirm, cardCleanupConfirmDelete, cardCleanupOpenConfirm } from './delete.js';
import { cardCleanupBrowse } from './folder-browser.js';
import { cardCleanupStartScan } from './jobs.js';
import { cardCleanupApplyQueryParams, loadVolumeSuggestions } from './source.js';
import { cardCleanupStartVerification } from './verification.js';

/** @param {string} id @param {() => unknown} action */
function onCardCleanupClick(id, action) {
  /** @type {HTMLElement} */ (document.getElementById(id)).addEventListener('click', function() { action(); });
}

onCardCleanupClick('card-cleanup-browse-btn', cardCleanupBrowse);
onCardCleanupClick('card-cleanup-scan-btn', cardCleanupStartScan);
onCardCleanupClick('card-cleanup-cancel-btn', cardCleanupCancelJob);
onCardCleanupClick('card-cleanup-verify-btn', cardCleanupStartVerification);
onCardCleanupClick('card-cleanup-delete-btn', cardCleanupOpenConfirm);
onCardCleanupClick('card-cleanup-confirm-close', cardCleanupCloseConfirm);
onCardCleanupClick('card-cleanup-confirm-cancel', cardCleanupCloseConfirm);
onCardCleanupClick('card-cleanup-confirm-btn', cardCleanupConfirmDelete);

// A click on the dimmed backdrop (not the dialog) closes the confirmation.
var confirmOverlay = /** @type {HTMLElement} */ (document.getElementById('card-cleanup-confirm'));
confirmOverlay.addEventListener('click', function(event) {
  if (event.target === confirmOverlay && confirmOverlay.classList.contains('open')) cardCleanupCloseConfirm();
});

cardCleanupApplyQueryParams();
loadVolumeSuggestions();

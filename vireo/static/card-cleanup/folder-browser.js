// Choosing the card folder: the native picker in the desktop app, the
// shared in-page folder browser otherwise.
// ES module.

import { VireoFolderBrowser, hasPickDirectory, isTauri, pickDirectory } from './shared.js';
import { cardCleanupSetError } from './state.js';

/* ---------- Shared single-select folder browser ---------- */

export const cardFolderBrowser = new VireoFolderBrowser({
  overlayId: 'folderBrowser',
  defaultMode: 'card',
  onError: (message) => cardCleanupSetError(message),
  modes: {
    card: {
      title: 'Select Card Folder',
      multiple: false,
      showCounts: true,
      // Cleanup considers every photo format Vireo imports, independent of
      // the Import page's last RAW/JPEG filter selection.
      fileTypes: 'both',
      startPath: () =>
        (/** @type {HTMLInputElement} */ (document.getElementById('card-cleanup-source')).value || '').trim(),
      onSelect: (path) => {
        /** @type {HTMLInputElement} */ (document.getElementById('card-cleanup-source')).value = path;
      },
    },
  },
});

export async function cardCleanupBrowse() {
  // Same shape as the import page's browseForSource(): native picker
  // when the desktop shell offers one, in-page browser otherwise.
  if (hasPickDirectory()) {
    const seqBeforePicker = cardFolderBrowser.sequence;
    const result = await pickDirectory('Select the card folder');
    if (result) {
      const path = Array.isArray(result) ? result[0] : result;
      if (path) /** @type {HTMLInputElement} */ (document.getElementById('card-cleanup-source')).value = path;
      return;
    }
    if (isTauri()) return;
    if (cardFolderBrowser.sequence !== seqBeforePicker) {
      openCardFolderBrowser({ skipInitialBrowse: true });
      return;
    }
  }
  openCardFolderBrowser();
}

/** @param {{skipInitialBrowse?: boolean}} [opts] */
function openCardFolderBrowser(opts = {}) {
  cardFolderBrowser.open('card', opts);
}

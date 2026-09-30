// Native folder pickers and the shared folder-browser integration.
// Classic page script; load boot.js after all definitions.

async function browseForSource() {
  if (typeof pickDirectory === 'function') {
    const seqBeforePicker = importFolderBrowser.sequence;
    const result = await pickDirectory('Select source folders', { multiple: true });
    if (result) {
      const paths = Array.isArray(result) ? result : [result];
      paths.forEach(addSourcePath);
      return;
    }
    if (typeof isTauri === 'function' && isTauri()) return;
    if (importFolderBrowser.sequence !== seqBeforePicker) {
      openImportFolderBrowser('source', { skipInitialBrowse: true });
      return;
    }
  }
  openImportFolderBrowser('source');
}

async function browseForDestination() {
  if (typeof pickDirectory === 'function') {
    const seqBeforePicker = importFolderBrowser.sequence;
    const result = await pickDirectory('Select archive folder');
    if (result) {
      const path = Array.isArray(result) ? result[0] : result;
      if (path) {
        const destination = document.getElementById('destInput');
        destination.value = path;
        // Notify destInput listeners (dest-structure invalidation, NAS-move
        // row) exactly like a manual edit would — matches
        // selectRecentDestination().
        destination.dispatchEvent(new Event('input', { bubbles: true }));
      }
      return;
    }
    if (typeof isTauri === 'function' && isTauri()) return;
    if (importFolderBrowser.sequence !== seqBeforePicker) {
      openImportFolderBrowser('destination', { skipInitialBrowse: true });
      return;
    }
  }
  openImportFolderBrowser('destination');
}

function createImportFolderBrowser() {
  return new VireoFolderBrowser({
    overlayId: 'folderBrowser',
    defaultMode: 'source',
    onError: (message) => showError(message),
    modes: {
      source: {
        title: 'Select Source Folders',
        multiple: true,
        showCounts: true,
        fileTypes: () => selectedSourceCountOptions().file_types,
        startPath: '',
        onSelect: (paths) => paths.forEach(addSourcePath),
      },
      destination: {
        title: 'Select Destination Folder',
        multiple: false,
        showCounts: false,
        startPath: () => (document.getElementById('destInput').value || '').trim(),
        onSelect: (path) => {
          const destination = document.getElementById('destInput');
          destination.value = path;
          // Notify destination-preview listeners exactly like a manual edit.
          destination.dispatchEvent(new Event('input', { bubbles: true }));
        },
      },
    },
  });
}

// Compatibility wrappers keep existing inline callers, deep-link behavior,
// and browser-level tests stable while all behavior lives in the shared module.
function openImportFolderBrowser(mode, opts = {}) {
  importFolderBrowser.open(mode === 'destination' ? 'destination' : 'source', opts);
}

function closeImportFolderBrowser() {
  importFolderBrowser.close();
}

function browseImportFolderTo(path) {
  return importFolderBrowser.browse(path);
}

function selectImportBrowserFolder() {
  importFolderBrowser.confirm();
}

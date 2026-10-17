// Captured new-image lists and their import deep links.
// Classic page script; load boot.js after all definitions.

/** @param {number} ms */
function sleepNewImagesImport(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/** @param {(progress: {files_checked?: number; new_count_so_far?: number}) => void} onPending */
async function createNewImagesImportSnapshot(onPending) {
  for (;;) {
    const resp = await fetch(
      '/api/workspaces/active/new-images/snapshot', { method: 'POST' });
    if (resp.status === 202) {
      /** @type {{files_checked?: number; new_count_so_far?: number}} */ const progress = await resp.json().catch(() => ({}));
      if (onPending) onPending(progress || {});
      await sleepNewImagesImport(3000);
      continue;
    }
    /** @type {{snapshot_id: number; error?: string; file_count?: number}} */ const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.error || 'snapshot failed');
    if (data.snapshot_id == null) throw new Error('snapshot missing');
    return data.snapshot_id;
  }
}

/** @param {number | string} snapshotId */
async function activateNewImagesImport(snapshotId) {
  const sourceNote = /** @type {HTMLElement} */ (document.getElementById('newImagesImportSource'));
  newImagesStartBlocked = true;
  updateStartGate();
  sourceNote.style.display = '';
  sourceNote.textContent = 'Loading the captured list of newly detected images…';

  const resp = await fetch(
    '/api/workspaces/active/new-images/snapshot/' +
    encodeURIComponent(snapshotId));
  /** @type {{snapshot_id: number; error?: string; folder_paths?: string[]; folder_counts?: Record<string, number>; file_count?: number}} */ const data = await resp.json().catch(() => ({}));
  if (!resp.ok) throw new Error(data.error || 'snapshot expired');

  newImagesSnapshotId = Number(snapshotId);
  newImagesFolderCounts = data.folder_counts || {};
  newImagesFileCount = Number(data.file_count || 0);
  newImagesExcludedFolders = new Set();
  sources = data.folder_paths || [];
  sourceCounts = {};
  sources.forEach(path => {
    sourceCounts[path] = newImagesSourceCount(path);
  });

  /** @type {HTMLInputElement} */ (document.getElementById('modeInPlace')).checked = true;
  /** @type {HTMLInputElement} */ (document.getElementById('modeInPlace')).disabled = true;
  /** @type {HTMLInputElement} */ (document.getElementById('modeCopy')).disabled = true;
  /** @type {HTMLElement} */ (document.getElementById('sourcePickerRow')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('sourceRecursiveRow')).style.display = 'none';
  /** @type {HTMLInputElement} */ (document.getElementById('workspaceCurrent')).checked = true;
  /** @type {HTMLInputElement} */ (document.getElementById('workspaceCurrent')).disabled = true;
  /** @type {HTMLInputElement} */ (document.getElementById('workspaceNew')).disabled = true;
  sourceNote.textContent = newImagesSourceNoteText();
  renderSources();
  updateImportMode();
  /** @type {HTMLButtonElement} */ (document.getElementById('btnPreview')).textContent = 'Refresh preview';
  newImagesStartBlocked = Number(data.file_count || 0) === 0;
  updateStartGate();
  await previewImport({ automatic: true });
}

/** @param {number} n */
function formatNewImageCount(n) {
  return Number(n || 0).toLocaleString() + ' new image' + (n === 1 ? '' : 's');
}

// The folders left out, in the order the source list shows them.
function newImagesExcludedFolderList() {
  return sources.filter(path => newImagesExcludedFolders.has(path));
}

function newImagesExcludedFileCount() {
  return newImagesExcludedFolderList().reduce(
    (sum, path) => sum + Number(newImagesFolderCounts[path] || 0), 0);
}

// What Start will add: the capture minus the folders left out.
function newImagesIncludedFileCount() {
  return Math.max(0, newImagesFileCount - newImagesExcludedFileCount());
}

/** @param {string} path */
function newImagesSourceCount(path) {
  const count = formatNewImageCount(Number(newImagesFolderCounts[path] || 0));
  return {
    status: 'loaded',
    text: newImagesExcludedFolders.has(path) ? 'Left out · ' + count : count,
  };
}

function newImagesSourceNoteText() {
  const captured = newImagesFileCount.toLocaleString() +
    ' newly detected image' + (newImagesFileCount === 1 ? '' : 's') +
    ' captured from registered folders.';
  const folders = newImagesExcludedFolderList().length;
  if (!folders) return captured + ' Import will add exactly this list in place.';
  const leftOut = folders.toLocaleString() + ' folder' + (folders === 1 ? '' : 's') +
    ' (' + formatNewImageCount(newImagesExcludedFileCount()) + ') left out.';
  const included = newImagesIncludedFileCount();
  if (!included) {
    return captured + ' ' + leftOut + ' Include at least one folder to import.';
  }
  return captured + ' ' + leftOut + ' Import will add the other ' +
    included.toLocaleString() + ' in place.';
}

/** @returns {ImportRequest} */
function newImagesImportBody() {
  return {
    source_snapshot_id: Number(newImagesSnapshotId),
    exclude_folders: newImagesExcludedFolderList(),
  };
}

function newImagesPreviewBody() {
  return {
    snapshot_id: newImagesSnapshotId,
    exclude_folders: newImagesExcludedFolderList(),
  };
}

// Leave a captured folder out of this import, or bring it back. Only that
// folder's own files: its captured subfolders are separate rows.
/** @param {string} path */
function toggleNewImagesFolder(path) {
  if (newImagesExcludedFolders.has(path)) newImagesExcludedFolders.delete(path);
  else newImagesExcludedFolders.add(path);
  sourceCounts[path] = newImagesSourceCount(path);
  /** @type {HTMLElement} */ (document.getElementById('newImagesImportSource')).textContent =
    newImagesSourceNoteText();
  renderSources();
  scheduleImportPreview();
}

// A captured folder's row can't be removed (the capture is frozen); its
// button leaves the folder out of this import instead, or brings it back.
/**
 * @param {HTMLElement} row
 * @param {HTMLButtonElement} btn
 * @param {string} path
 */
function configureNewImagesFolderRow(row, btn, path) {
  const excluded = newImagesExcludedFolders.has(path);
  row.classList.toggle('source-item-excluded', excluded);
  btn.textContent = excluded ? 'Include' : '×';
  btn.title = excluded
    ? 'Include this folder in the import'
    : 'Leave this folder out of the import';
  btn.onclick = () => toggleNewImagesFolder(path);
}

/** @param {string} deepLinkId */
async function initNewImagesImport(deepLinkId) {
  const sourceNote = /** @type {HTMLElement} */ (document.getElementById('newImagesImportSource'));
  newImagesStartBlocked = true;
  updateStartGate();
  sourceNote.style.display = '';
  try {
    /** @type {string | number} */ let snapshotId = deepLinkId;
    if (deepLinkId === 'preparing') {
      sourceNote.textContent = 'Scanning registered folders for new images…';
      snapshotId = await createNewImagesImportSnapshot(progress => {
        let text = 'Scanning registered folders for new images…';
        if ((progress.files_checked || 0) > 0) {
          text = Number(progress.files_checked).toLocaleString() +
            ' files checked · ' +
            Number(progress.new_count_so_far || 0).toLocaleString() +
            ' new so far…';
        }
        sourceNote.textContent = text;
      });
      history.replaceState(
        null, '', '/import?new_images=' + encodeURIComponent(snapshotId));
    } else if (!/^\d+$/.test(String(deepLinkId || ''))) {
      throw new Error('The detected-image list is invalid.');
    }
    await activateNewImagesImport(snapshotId);
  } catch (e) {
    sourceNote.textContent = 'The list of newly detected images could not be loaded.';
    showError(String(e instanceof Error ? e.message : e));
    newImagesStartBlocked = true;
    updateStartGate();
  }
}

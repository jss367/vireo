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
    /** @type {{snapshot_id: number; error?: string; folder_paths?: string[]; file_count?: number}} */ const data = await resp.json().catch(() => ({}));
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
  /** @type {{snapshot_id: number; error?: string; folder_paths?: string[]; file_count?: number}} */ const data = await resp.json().catch(() => ({}));
  if (!resp.ok) throw new Error(data.error || 'snapshot expired');

  newImagesSnapshotId = Number(snapshotId);
  sources = data.folder_paths || [];
  sourceCounts = {};
  sources.forEach(path => {
    sourceCounts[path] = {
      status: 'loaded',
      text: 'Captured source folder',
    };
  });

  /** @type {HTMLInputElement} */ (document.getElementById('modeInPlace')).checked = true;
  /** @type {HTMLInputElement} */ (document.getElementById('modeInPlace')).disabled = true;
  /** @type {HTMLInputElement} */ (document.getElementById('modeCopy')).disabled = true;
  /** @type {HTMLElement} */ (document.getElementById('sourcePickerRow')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('sourceRecursiveRow')).style.display = 'none';
  /** @type {HTMLInputElement} */ (document.getElementById('workspaceCurrent')).checked = true;
  /** @type {HTMLInputElement} */ (document.getElementById('workspaceCurrent')).disabled = true;
  /** @type {HTMLInputElement} */ (document.getElementById('workspaceNew')).disabled = true;
  sourceNote.textContent = Number(data.file_count || 0).toLocaleString() +
    ' newly detected image' + (Number(data.file_count || 0) === 1 ? '' : 's') +
    ' captured from registered folders. Import will add exactly this list in place.';
  renderSources();
  updateImportMode();
  /** @type {HTMLButtonElement} */ (document.getElementById('btnPreview')).textContent = 'Refresh preview';
  newImagesStartBlocked = Number(data.file_count || 0) === 0;
  updateStartGate();
  await previewImport({ automatic: true });
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

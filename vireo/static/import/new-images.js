// Captured new-image lists and their import deep links.
// Classic page script; load boot.js after all definitions.

function sleepNewImagesImport(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function createNewImagesImportSnapshot(onPending) {
  for (;;) {
    const resp = await fetch(
      '/api/workspaces/active/new-images/snapshot', { method: 'POST' });
    if (resp.status === 202) {
      const progress = await resp.json().catch(() => ({}));
      if (onPending) onPending(progress || {});
      await sleepNewImagesImport(3000);
      continue;
    }
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.error || 'snapshot failed');
    if (data.snapshot_id == null) throw new Error('snapshot missing');
    return data.snapshot_id;
  }
}

async function activateNewImagesImport(snapshotId) {
  const sourceNote = document.getElementById('newImagesImportSource');
  newImagesStartBlocked = true;
  updateStartGate();
  sourceNote.style.display = '';
  sourceNote.textContent = 'Loading the captured list of newly detected images…';

  const resp = await fetch(
    '/api/workspaces/active/new-images/snapshot/' +
    encodeURIComponent(snapshotId));
  const data = await resp.json().catch(() => ({}));
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

  document.getElementById('modeInPlace').checked = true;
  document.getElementById('modeInPlace').disabled = true;
  document.getElementById('modeCopy').disabled = true;
  document.getElementById('sourcePickerRow').style.display = 'none';
  document.getElementById('sourceRecursiveRow').style.display = 'none';
  document.getElementById('workspaceCurrent').checked = true;
  document.getElementById('workspaceCurrent').disabled = true;
  document.getElementById('workspaceNew').disabled = true;
  sourceNote.textContent = Number(data.file_count || 0).toLocaleString() +
    ' newly detected image' + (Number(data.file_count || 0) === 1 ? '' : 's') +
    ' captured from registered folders. Import will add exactly this list in place.';
  renderSources();
  updateImportMode();
  document.getElementById('btnPreview').textContent = 'Refresh preview';
  newImagesStartBlocked = Number(data.file_count || 0) === 0;
  updateStartGate();
  await previewImport({ automatic: true });
}

async function initNewImagesImport(deepLinkId) {
  const sourceNote = document.getElementById('newImagesImportSource');
  newImagesStartBlocked = true;
  updateStartGate();
  sourceNote.style.display = '';
  try {
    let snapshotId = deepLinkId;
    if (deepLinkId === 'preparing') {
      sourceNote.textContent = 'Scanning registered folders for new images…';
      snapshotId = await createNewImagesImportSnapshot(progress => {
        let text = 'Scanning registered folders for new images…';
        if (progress.files_checked > 0) {
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
    showError(String(e.message || e));
    newImagesStartBlocked = true;
    updateStartGate();
  }
}

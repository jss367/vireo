// Orphaned staging comparison, cleanup, and paused-job polling.
// Classic page script; load boot.js after all definitions.

// What the last cleanup did, shown above the folder list until the next one.
let stagingOutcomeNote = '';

/**
 * @param {number} jobId
 * @param {(progress: {current?: number; total?: number}) => void} [onProgress]
 */
async function pollJobResult(jobId, onProgress) {
  for (let i = 0; i < 180; i++) {
    const resp = await fetch('/api/jobs/' + jobId);
    if (resp.ok) {
      /** @type {{status: string; errors?: string[]; result?: ImportStaging; progress?: {current?: number; total?: number}}} */ const job = await resp.json();
      if (['queued', 'running', 'pausing', 'paused'].includes(job.status)) {
        i = -1;
        if (onProgress && job.progress) onProgress(job.progress);
      }
      if (['completed', 'failed', 'cancelled', 'expired'].includes(job.status)) {
        if (job.status === 'failed') {
          const msg = (job.errors && job.errors[0]) || 'job failed';
          throw new Error(msg);
        }
        if (job.status === 'cancelled') throw new Error('The job was cancelled before it finished.');
        if (!job.result) throw new Error('job result missing'); return job.result;
      }
    }
    await new Promise(r => setTimeout(r, 1000));
  }
  throw new Error('job did not finish in time');
}

/** @param {number} count @param {string} singular @param {string} plural */
function stagingCount(count, singular, plural) {
  return count + ' ' + (count === 1 ? singular : plural);
}

async function loadOrphanedStaging() {
  try {
    const resp = await fetch('/api/import/orphaned-staging');
    if (!resp.ok) return;
    /** @type {{items?: ImportStaging[]; error?: string; job_id: number}} */ const data = await resp.json();
    renderOrphanedStaging(data.items || []);
  } catch (e) {
    // Recovery card is best-effort; import itself should remain usable.
  }
}

/** @param {ImportStaging[]} items */
function renderOrphanedStaging(items) {
  const card = /** @type {HTMLElement} */ (document.getElementById('orphanedStagingCard'));
  const box = /** @type {HTMLElement} */ (document.getElementById('orphanedStagingItems'));
  box.innerHTML = '';
  if (!items.length && !stagingOutcomeNote) {
    card.style.display = 'none';
    return;
  }
  card.style.display = '';
  if (stagingOutcomeNote) {
    const note = document.createElement('div');
    note.className = 'staging-outcome';
    note.textContent = stagingOutcomeNote;
    box.appendChild(note);
  }
  items.forEach((item) => {
    const row = document.createElement('div');
    row.className = 'staging-item';
    row.dataset.path = item.path;

    const title = document.createElement('div');
    title.className = 'staging-title';
    title.textContent = item.name + ' · ' + item.source_root;
    const meta = document.createElement('div');
    meta.className = 'staging-meta';
    meta.textContent = stagingCount(item.file_count, 'file', 'files') + ' · ' + formatBytes(item.bytes) +
      ' · not compared with the archive yet';

    const actions = document.createElement('div');
    actions.className = 'staging-actions';
    const verify = document.createElement('button');
    verify.className = 'btn';
    verify.textContent = 'Compare with the archive';
    verify.onclick = () => verifyStaging(item.path, verify);
    actions.appendChild(verify);

    row.append(title, meta, actions);
    box.appendChild(row);
    // A folder compared earlier on this page keeps its result when another
    // folder's cleanup redraws the list.
    const compared = stagingResultsByPath[item.path];
    if (compared) renderStagingVerification(compared);
  });
}

/** @param {string} path */
function stagingRow(path) {
  return Array.from(/** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('.staging-item')))
    .find(el => el.dataset.path === path);
}

/**
 * List the entries that are not deletable, with the reason for each.
 * @param {HTMLElement} row @param {ImportStaging} result @param {string} heading
 */
function appendStagingDetail(row, result, heading) {
  const old = /** @type {HTMLElement} */ (row.querySelector('.staging-details'));
  if (old) old.remove();
  const bad = (result.details || []).filter(d => d.status !== 'verified');
  if (!bad.length) return;
  const total = Math.max(result.details_total || 0, bad.length);
  const list = document.createElement('ul');
  list.className = 'staging-details';
  const head = document.createElement('li');
  head.className = 'staging-details-heading';
  head.textContent = heading;
  list.appendChild(head);
  bad.slice(0, 20).forEach((d) => {
    const li = document.createElement('li');
    li.textContent = d.rel_path + ' — ' + d.reason;
    list.appendChild(li);
  });
  if (total > 20) {
    const li = document.createElement('li');
    li.textContent = (total - 20) + ' more not shown';
    list.appendChild(li);
  }
  row.appendChild(list);
}

/** @param {ImportStaging} result */
function stagingProblemParts(result) {
  const parts = [];
  if (result.unaccounted) parts.push(result.unaccounted + ' not in archive');
  if (result.unreachable) parts.push(result.unreachable + ' archive unreachable');
  if (result.unchecked) parts.push(result.unchecked + ' could not be checked');
  if (result.delete_failed) parts.push(result.delete_failed + ' could not be deleted');
  return parts;
}

/** @param {ImportStaging} result */
function stagingDeleteLabel(result) {
  const identical = result.verified || 0;
  if (!result.can_delete) return 'Delete the ' + stagingCount(identical, 'identical file', 'identical files');
  return identical ? 'Delete staging copy' : 'Remove empty staging folder';
}

/** @param {ImportStaging} result */
function renderStagingVerification(result) {
  stagingResultsByPath[result.path] = result;
  const row = stagingRow(result.path);
  if (!row) return;
  const identical = result.verified || 0;
  const meta = /** @type {HTMLElement} */ (row.querySelector('.staging-meta'));
  const parts = [
    stagingCount(result.file_count, 'file', 'files'),
    formatBytes(result.bytes),
    identical + ' identical to the archive copy',
  ].concat(stagingProblemParts(result));
  if (result.inferred_destination) parts.push('archive: ' + result.inferred_destination);
  meta.textContent = parts.join(' · ');

  const actions = /** @type {HTMLElement} */ (row.querySelector('.staging-actions'));
  actions.innerHTML = '';
  if (result.can_delete || identical) {
    const del = document.createElement('button');
    del.className = result.can_delete ? 'btn btn-primary' : 'btn';
    del.textContent = stagingDeleteLabel(result);
    del.onclick = () => deleteStaging(result.path, del);
    actions.appendChild(del);
  }
  if (!result.can_delete) {
    const use = document.createElement('button');
    use.className = 'btn';
    use.textContent = 'Use as import source';
    use.onclick = () => useStagingAsImportSource(result);
    actions.appendChild(use);
    const again = document.createElement('button');
    again.className = 'btn';
    again.textContent = 'Compare again';
    again.onclick = () => verifyStaging(result.path, again);
    actions.appendChild(again);
  }
  appendStagingDetail(row, result, 'Not deletable, because no identical archive copy was found:');
}

/** @param {string} path @param {HTMLButtonElement} btn */
async function verifyStaging(path, btn) {
  const oldText = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Comparing...';
  try {
    const resp = await fetch('/api/import/orphaned-staging/verify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path }),
    });
    /** @type {{items?: ImportStaging[]; error?: string; job_id: number}} */ const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || 'comparison failed');
    const result = await pollJobResult(data.job_id, (progress) => {
      if (!progress.total) return;
      btn.textContent = 'Comparing ' + (progress.current || 0) + ' of ' + stagingCount(progress.total, 'file', 'files') + '...';
    });
    renderStagingVerification(result);
  } catch (e) {
    showError(String(e instanceof Error ? e.message : e));
    btn.disabled = false;
    btn.textContent = oldText;
  }
}

/**
 * Ask before a cleanup. The delete removes files from disk, not to the
 * Trash, so the question says what the comparison proved, what the delete
 * checks again, and what stays.
 * @param {string} path @param {ImportStaging | undefined} compared
 */
function confirmStagingDelete(path, compared) {
  // Without a comparison on screen there is nothing truthful to ask about.
  if (!compared) return Promise.resolve(false);
  const identical = compared.verified || 0;
  const others = compared.details_total || 0;
  if (!identical) {
    return Vireo.confirm({
      title: 'Remove this empty staging folder?',
      message: 'Vireo found no files in this folder. It removes the folder only if it is still empty.',
      details: [path],
      confirmLabel: 'Remove folder',
      danger: true,
    });
  }
  const size = formatBytes(compared.verified_bytes || 0);
  let message = 'Vireo deletes ' + stagingCount(identical, 'file', 'files') + ' (' + size + ') from disk. ' +
    'Deleted files are not moved to the Trash and cannot be recovered.\n\n' +
    (identical === 1 ? 'The file' : 'Each file') + ' matched its archive copy byte for byte. ' +
    'Before deleting a file, Vireo compares it with the archive copy again and deletes it only if the two are still identical.';
  if (others) {
    message += '\n\n' + stagingCount(others, 'other entry has', 'other entries have') +
      ' no identical archive copy and ' + (others === 1 ? 'stays' : 'stay') + ' in the folder.';
  }
  return Vireo.confirm({
    title: 'Delete ' + stagingCount(identical, 'staged file', 'staged files') + ' permanently?',
    message,
    details: [path],
    confirmLabel: 'Delete permanently',
    danger: true,
  });
}

/**
 * Show what a cleanup job did: what it deleted, and what it kept and why.
 * @param {ImportStaging} result
 */
async function renderStagingCleanup(result) {
  const deleted = result.deleted || 0;
  const freed = deleted ? ' (' + formatBytes(result.deleted_bytes || 0) + ')' : '';
  const did = 'Deleted ' + stagingCount(deleted, 'file', 'files') + freed + ' after matching ' +
    (deleted === 1 ? 'it' : 'each') + ' with the archive copy';
  delete stagingResultsByPath[result.path];
  if (result.folder_removed) {
    stagingOutcomeNote = (result.name || result.path) + ': ' + did + '. The folder was removed.';
    await loadOrphanedStaging();
    return;
  }
  stagingOutcomeNote = '';
  const row = stagingRow(result.path);
  if (!row) { await loadOrphanedStaging(); return; }
  const meta = /** @type {HTMLElement} */ (row.querySelector('.staging-meta'));
  meta.textContent = [did, stagingCount(result.kept || 0, 'entry', 'entries') + ' kept']
    .concat(stagingProblemParts(result)).join(' · ');
  const actions = /** @type {HTMLElement} */ (row.querySelector('.staging-actions'));
  actions.innerHTML = '';
  const again = document.createElement('button');
  again.className = 'btn';
  again.textContent = 'Compare again';
  again.onclick = () => verifyStaging(result.path, again);
  actions.appendChild(again);
  appendStagingDetail(row, result, 'Kept in the folder:');
}

/** @param {string} path @param {HTMLButtonElement} btn */
async function deleteStaging(path, btn) {
  if (btn.disabled) return;
  const accepted = await confirmStagingDelete(path, stagingResultsByPath[path]);
  if (!accepted || btn.disabled) return;
  const oldText = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Comparing again...';
  try {
    const resp = await fetch('/api/import/orphaned-staging', {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path, confirm_permanent: true }),
    });
    /** @type {{items?: ImportStaging[]; error?: string; job_id: number}} */ const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || 'cleanup failed');
    const result = await pollJobResult(data.job_id, (progress) => {
      if (!progress.total) return;
      btn.textContent = 'Comparing and deleting ' + (progress.current || 0) + ' of ' +
        stagingCount(progress.total, 'file', 'files') + '...';
    });
    await renderStagingCleanup(result);
  } catch (e) {
    // The job may have deleted files before it stopped, so the row's earlier
    // comparison no longer describes the folder: list the folders afresh.
    showError(String(e instanceof Error ? e.message : e));
    btn.disabled = false;
    btn.textContent = oldText;
    delete stagingResultsByPath[path];
    stagingOutcomeNote = '';
    await loadOrphanedStaging();
  }
}

/** @param {ImportStaging} result */
function useStagingAsImportSource(result) {
  sources = [result.source_root];
  hideDestStructure();
  renderSources();
  // Staging recovery must copy the temporary staging tree into the archive.
  // In-place would catalog paths that disappear once staging is cleaned,
  // leaving missing-folder rows, so force Copy to archive here.
  const copyRadio = /** @type {HTMLInputElement} */ (document.getElementById('modeCopy'));
  if (copyRadio) {
    copyRadio.checked = true;
    updateImportMode();
  }
  if (result.inferred_destination) {
    const destination = /** @type {HTMLInputElement} */ (document.getElementById('destInput'));
    destination.value = result.inferred_destination;
    // Dispatch AFTER the value is set so every destInput listener
    // (dest-structure invalidation, NAS-move row) re-evaluates against the
    // inferred destination — updateImportMode() above ran while destInput
    // still held its previous value.
    destination.dispatchEvent(new Event('input', { bubbles: true }));
    hideDestStructure();
  }
  scheduleImportPreview();
  /** @type {HTMLElement} */ (document.getElementById('sourceCard')).scrollIntoView({ behavior: preferredScrollBehavior(), block: 'start' });
}

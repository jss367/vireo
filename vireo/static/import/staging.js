// Orphaned staging verification, cleanup, and paused-job polling.
// Classic page script; load boot.js after all definitions.

async function pollJobResult(jobId) {
  for (let i = 0; i < 180; i++) {
    const resp = await fetch('/api/jobs/' + jobId);
    if (resp.ok) {
      const job = await resp.json();
      if (['queued', 'running', 'pausing', 'paused'].includes(job.status)) i = -1;
      if (['completed', 'failed', 'cancelled', 'expired'].includes(job.status)) {
        if (job.status === 'failed') {
          const msg = (job.errors && job.errors[0]) || 'job failed';
          throw new Error(msg);
        }
        return job.result || {};
      }
    }
    await new Promise(r => setTimeout(r, 1000));
  }
  throw new Error('job did not finish in time');
}

async function loadOrphanedStaging() {
  try {
    const resp = await fetch('/api/import/orphaned-staging');
    if (!resp.ok) return;
    const data = await resp.json();
    renderOrphanedStaging(data.items || []);
  } catch (e) {
    // Recovery card is best-effort; import itself should remain usable.
  }
}

function renderOrphanedStaging(items) {
  const card = document.getElementById('orphanedStagingCard');
  const box = document.getElementById('orphanedStagingItems');
  box.innerHTML = '';
  if (!items.length) {
    card.style.display = 'none';
    return;
  }
  card.style.display = '';
  items.forEach((item) => {
    const row = document.createElement('div');
    row.className = 'staging-item';
    row.dataset.path = item.path;

    const title = document.createElement('div');
    title.className = 'staging-title';
    title.textContent = item.name + ' · ' + item.source_root;
    const meta = document.createElement('div');
    meta.className = 'staging-meta';
    meta.textContent = item.file_count + ' files · ' + formatBytes(item.bytes);

    const actions = document.createElement('div');
    actions.className = 'staging-actions';
    const verify = document.createElement('button');
    verify.className = 'btn';
    verify.textContent = 'Verify before cleanup';
    verify.onclick = () => verifyStaging(item.path, verify);
    actions.appendChild(verify);

    row.append(title, meta, actions);
    box.appendChild(row);
  });
}

function appendStagingDetail(row, result) {
  const old = row.querySelector('.staging-details');
  if (old) old.remove();
  const bad = (result.details || []).filter(d => d.status !== 'verified');
  if (!bad.length) return;
  const list = document.createElement('ul');
  list.className = 'staging-details';
  bad.slice(0, 20).forEach((d) => {
    const li = document.createElement('li');
    li.textContent = d.rel_path + ' — ' + d.reason;
    list.appendChild(li);
  });
  if (bad.length > 20) {
    const li = document.createElement('li');
    li.textContent = (bad.length - 20) + ' more files not shown';
    list.appendChild(li);
  }
  row.appendChild(list);
}

function renderStagingVerification(result) {
  stagingResultsByPath[result.path] = result;
  const row = Array.from(document.querySelectorAll('.staging-item'))
    .find(el => el.dataset.path === result.path);
  if (!row) return;
  const meta = row.querySelector('.staging-meta');
  const parts = [
    result.file_count + ' files',
    formatBytes(result.bytes),
    result.verified + ' verified',
  ];
  if (result.unaccounted) parts.push(result.unaccounted + ' not in archive');
  if (result.unreachable) parts.push(result.unreachable + ' unreachable');
  if (result.inferred_destination) parts.push('archive: ' + result.inferred_destination);
  meta.textContent = parts.join(' · ');

  const actions = row.querySelector('.staging-actions');
  actions.innerHTML = '';
  if (result.can_delete) {
    const del = document.createElement('button');
    del.className = 'btn btn-primary';
    del.textContent = 'Delete verified staging copy';
    del.onclick = () => deleteStaging(result.path, del);
    actions.appendChild(del);
  } else {
    const use = document.createElement('button');
    use.className = 'btn';
    use.textContent = 'Use as import source';
    use.onclick = () => useStagingAsImportSource(result);
    actions.appendChild(use);
    const again = document.createElement('button');
    again.className = 'btn';
    again.textContent = 'Verify again';
    again.onclick = () => verifyStaging(result.path, again);
    actions.appendChild(again);
  }
  appendStagingDetail(row, result);
}

async function verifyStaging(path, btn) {
  const oldText = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Verifying...';
  try {
    const resp = await fetch('/api/import/orphaned-staging/verify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || 'verification failed');
    const result = await pollJobResult(data.job_id);
    renderStagingVerification(result);
  } catch (e) {
    showError(String(e.message || e));
    btn.disabled = false;
    btn.textContent = oldText;
  }
}

async function deleteStaging(path, btn) {
  btn.disabled = true;
  btn.textContent = 'Re-verifying...';
  try {
    const resp = await fetch('/api/import/orphaned-staging', {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || 'cleanup failed');
    await loadOrphanedStaging();
  } catch (e) {
    showError(String(e.message || e));
    btn.disabled = false;
    btn.textContent = 'Delete verified staging copy';
  }
}

function useStagingAsImportSource(result) {
  sources = [result.source_root];
  hideDestStructure();
  renderSources();
  // Staging recovery must copy the temporary staging tree into the archive.
  // In-place would catalog paths that disappear once staging is cleaned,
  // leaving missing-folder rows, so force Copy to archive here.
  const copyRadio = document.getElementById('modeCopy');
  if (copyRadio) {
    copyRadio.checked = true;
    updateImportMode();
  }
  if (result.inferred_destination) {
    const destination = document.getElementById('destInput');
    destination.value = result.inferred_destination;
    // Dispatch AFTER the value is set so every destInput listener
    // (dest-structure invalidation, NAS-move row) re-evaluates against the
    // inferred destination — updateImportMode() above ran while destInput
    // still held its previous value.
    destination.dispatchEvent(new Event('input', { bubbles: true }));
    hideDestStructure();
  }
  scheduleImportPreview();
  document.getElementById('sourceCard').scrollIntoView({ behavior: preferredScrollBehavior(), block: 'start' });
}

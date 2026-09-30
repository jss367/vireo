// Source folders, streamed scan counts, and stalled-scan progress.
// Classic page script; load boot.js after all definitions.

function renderSources() {
  const list = document.getElementById('sourceList');
  list.innerHTML = '';
  sources.forEach((s, i) => {
    const div = document.createElement('div');
    div.className = 'source-item';
    const span = document.createElement('span');
    span.textContent = s;
    span.style.flex = '1';
    const meta = document.createElement('span');
    meta.className = 'source-meta';
    meta.textContent = sourceCountRowText(sourceCounts[s]);
    const btn = document.createElement('button');
    btn.textContent = '×';
    btn.title = 'Remove';
    btn.onclick = () => {
      sources.splice(i, 1);
      delete sourceCounts[s];
      hideDestStructure();
      if (!sources.length) {
        clearImportPreviewGrid();
        // Emptying the source list ends this card's session, so the
        // filter retires with it — the row is already hidden here, and
        // carrying the choice into whatever card is added next would
        // filter that one on arrival without the user opting in. Note
        // this is deliberately NOT in clearImportPreviewGrid(): ordinary
        // re-previews clear the grid too and must keep the opt-in.
        retireHideDuplicatesOptIn();
        document.getElementById('previewSummary').textContent = '';
      }
      renderSources();
      scheduleImportPreview();
    };
    if (newImagesSnapshotId !== null) btn.style.display = 'none';
    div.appendChild(span);
    div.appendChild(meta);
    div.appendChild(btn);
    list.appendChild(div);
  });
  renderSourceCountProgress();
  updateStartGate();
}

// No frame for this long from a folder that is being walked means the walk
// is probably blocked on the storage itself (dead NAS mount, sleeping
// removable drive) — the walker emits heartbeats while it makes progress.
let SOURCE_SCAN_STALL_MS = 15000;

function sourceScanLooksStalled(state) {
  return state.status === 'loading' &&
    state.storage !== 'local' &&
    Number.isFinite(state.lastEventAt) &&
    Date.now() - state.lastEventAt > SOURCE_SCAN_STALL_MS;
}

function sourceCountRowText(state) {
  if (!state) return 'Waiting to scan';
  if (state.status === 'queued') {
    return 'Waiting · ' + state.position + ' of ' + state.total;
  }
  if (state.status === 'loading') {
    const elapsed = Math.max(0, Math.floor((Date.now() - state.startedAt) / 1000));
    let text = 'Scanning…' + (elapsed ? ' ' + elapsed + 's' : '');
    if (['metadata', 'capture_dates'].includes(state.stage) && state.found) {
      text = (state.stage === 'capture_dates' ? 'Reading capture dates…' : 'Reading file info…') +
        (elapsed ? ' ' + elapsed + 's' : '') +
        ' · ' + Number(state.checked).toLocaleString() + ' of ' +
        Number(state.found).toLocaleString();
    } else if (Number.isFinite(state.checked) && state.checked > 0) {
      text += ' · ' + Number(state.checked).toLocaleString() + ' checked' +
        (state.found
          ? ' (' + Number(state.found).toLocaleString() + ' photos)'
          : '');
    }
    if (sourceScanLooksStalled(state)) text += ' · no response';
    return text;
  }
  return state.text || 'Count unavailable';
}

function renderSourceCountProgress() {
  const el = document.getElementById('sourceCountProgress');
  if (!el) return;
  // Snapshot mode never runs folder-count scans — the per-source rows show
  // "Captured source folder" text with no `count`, so summing them would
  // render a bogus "N folders counted · 0 photos found" underneath the
  // snapshot's own photo-count message.
  if (newImagesSnapshotId !== null) {
    el.textContent = '';
    el.classList.remove('visible');
    return;
  }
  const states = sources.map(path => sourceCounts[path]).filter(Boolean);
  const active = states.filter(state => state.status === 'loading');
  const queued = states.filter(state => state.status === 'queued').length;
  const loaded = states.filter(state => state.status === 'loaded');
  const errors = states.filter(state => state.status === 'error').length;
  const photos = loaded.reduce((sum, state) => sum + Number(state.count || 0), 0);

  if (active.length) {
    const oldestStart = Math.min(...active.map(state => state.startedAt));
    const elapsed = Math.max(0, Math.floor((Date.now() - oldestStart) / 1000));
    const counted = loaded.length + errors;
    const countedText = counted
      ? counted.toLocaleString() + ' folder' + (counted === 1 ? '' : 's') +
        ' counted (' + formatPhotoCount(photos) + ')'
      : 'no folders finished yet';
    const positions = active.map(state => state.position).join(' and ');
    const storageKinds = new Set(active.map(state => state.storage).filter(Boolean));
    const storageLabels = {
      local: ' on local storage',
      network: ' on network storage',
      removable: ' on removable storage',
    };
    const storageLabel = storageKinds.size === 1
      ? (storageLabels[[...storageKinds][0]] || '')
      : '';
    const stalled = active.some(sourceScanLooksStalled);
    el.textContent = 'Scanning folder' + (active.length === 1 ? ' ' : 's ') +
      positions + ' of ' + active[0].total + storageLabel +
      ' · ' + countedText + (queued ? ' · ' + queued + ' waiting' : '') +
      (elapsed ? ' · ' + elapsed + 's elapsed' : '') +
      (stalled
        ? ' · not responding — the storage may be disconnected or slow'
        : '');
    el.classList.add('visible');
    return;
  }
  if (queued) {
    el.textContent = 'Preparing to scan ' + queued + ' folder' +
      (queued === 1 ? '' : 's') + '…';
    el.classList.add('visible');
    return;
  }
  if (states.length && loaded.length + errors === states.length) {
    el.textContent = loaded.length.toLocaleString() + ' folder' +
      (loaded.length === 1 ? '' : 's') + ' counted · ' +
      formatPhotoCount(photos) + ' found' +
      (errors ? ' · ' + errors + ' unavailable' : '');
    el.classList.add('visible');
    return;
  }
  el.textContent = '';
  el.classList.remove('visible');
}

function selectedSourceCountOptions() {
  const copyMode = newImagesSnapshotId === null && selectedImportMode() === 'copy';
  return {
    file_types: copyMode ? selectedFileTypes() : 'both',
    recursive: document.getElementById('chkRecursive').checked,
  };
}

function formatPhotoCount(n) {
  const count = Number(n || 0);
  return count.toLocaleString() + ' photo' + (count === 1 ? '' : 's');
}

function refreshSourceCounts() {
  // Counts ride the shared preview traversal now — one walk feeds the
  // per-folder rows and the grid. Kept as a named alias because every
  // option control's inline handler calls it.
  scheduleImportPreview();
}

function ensureSourceCountClock() {
  if (!sourceCountTickTimer) {
    sourceCountTickTimer = setInterval(updateSourceCountClock, 1000);
  }
}

function stopSourceCountClock() {
  if (sourceCountTickTimer) {
    clearInterval(sourceCountTickTimer);
    sourceCountTickTimer = null;
  }
}

function updateSourceCountClock() {
  const metas = document.querySelectorAll('#sourceList .source-meta');
  sources.forEach((sourcePath, index) => {
    if (metas[index]) metas[index].textContent =
      sourceCountRowText(sourceCounts[sourcePath]);
  });
  renderSourceCountProgress();
  // Self-stopping: an aborted stream with no successor leaves no active
  // states, so the next tick retires the timer instead of leaking it.
  const anyActive = sources.some(path => {
    const state = sourceCounts[path];
    return state && (state.status === 'loading' || state.status === 'queued');
  });
  if (!anyActive) stopSourceCountClock();
}

// -- Shared preview traversal --------------------------------------------
// The stream endpoint walks every source folder once, with storage-aware
// concurrency decided server-side, and narrates the walk as frames. These
// appliers translate frames into the per-folder row states.

function applyFolderPreviewStreamFrame(frame) {
  if (frame.type === 'policy') {
    (frame.sources || []).forEach(src => {
      if (!sources.includes(src.path)) return;
      sourceCounts[src.path] = {
        status: 'queued',
        position: Number(src.position),
        total: Number(src.total),
        storage: src.storage,
      };
    });
    ensureSourceCountClock();
    renderSources();
    return;
  }
  const state = sourceCounts[frame.path];
  if (frame.type === 'folder_started') {
    if (!state) return;
    state.status = 'loading';
    state.startedAt = Date.now();
    state.lastEventAt = Date.now();
    if (frame.storage) state.storage = frame.storage;
    renderSources();
    return;
  }
  if (frame.type === 'folder_progress') {
    if (!state || state.status !== 'loading') return;
    state.checked = Number(frame.checked);
    state.found = Number(frame.found);
    state.stage = frame.stage;
    state.lastEventAt = Date.now();
    updateSourceCountClock();
    return;
  }
  if (frame.type === 'folder_done') {
    if (!state) return;
    sourceCounts[frame.path] = {
      status: frame.error ? 'error' : 'loaded',
      count: Number(frame.count || 0),
      text: frame.error ? 'Count unavailable' : formatPhotoCount(frame.count),
    };
    renderSources();
  }
}

function totalDiscoveredSoFar() {
  return sources.reduce((sum, path) => {
    const state = sourceCounts[path];
    if (!state) return sum;
    if (state.status === 'loaded') return sum + Number(state.count || 0);
    if (state.status === 'loading' && Number.isFinite(state.found)) {
      return sum + state.found;
    }
    return sum;
  }, 0);
}

// Reads the folder-preview stream, applying scan-progress frames as they
// arrive, and returns the final done payload (the old synchronous
// folder-preview response shape) — or null if the stream ended without one.
async function readFolderPreviewStream(resp, requestSeq) {
  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  const summary = document.getElementById('previewSummary');
  let buffer = '';
  let done = null;
  for (;;) {
    const r = await reader.read();
    if (r.done) break;
    buffer += decoder.decode(r.value, { stream: true });
    const parts = buffer.split('\n\n');
    buffer = parts.pop();
    for (const part of parts) {
      const m = part.match(/^data: (.+)$/m);
      if (!m) continue;
      let frame;
      try {
        frame = JSON.parse(m[1]);
      } catch (e) {
        continue; // partial frame
      }
      if (frame.type === 'done') {
        done = frame;
        continue;
      }
      // A newer preview owns the rows now; keep draining so the final
      // done frame (needed for nothing, but cheap) doesn't wedge parsing.
      if (requestSeq !== importPreviewSeq) continue;
      applyFolderPreviewStreamFrame(frame);
      if (frame.type === 'folder_progress' || frame.type === 'folder_done') {
        const found = totalDiscoveredSoFar();
        summary.textContent = 'Discovering files…' +
          (found ? ' ' + found.toLocaleString() + ' found so far' : '');
      }
    }
  }
  if (requestSeq === importPreviewSeq) stopSourceCountClock();
  return done;
}

// A preview run that failed outright would otherwise strand rows on
// "Scanning…" with no walker behind them.
function failActiveSourceScans() {
  let changed = false;
  sources.forEach(path => {
    const state = sourceCounts[path];
    if (state && (state.status === 'loading' || state.status === 'queued')) {
      sourceCounts[path] = { status: 'error', count: 0, text: 'Count unavailable' };
      changed = true;
    }
  });
  stopSourceCountClock();
  if (changed) renderSources();
}

// A validation failure can supersede and abort a real traversal before the
// replacement run reaches discovery. Those old loading/queued states no
// longer have a walker behind them, but they are not scan failures either.
// Return every row to the neutral pre-scan state until the options are valid.
// Completed/error rows describe the old filter just as much as active rows
// do, so retaining them would show stale counts after validation aborts.
function resetActiveSourceScans() {
  let changed = false;
  sources.forEach(path => {
    const state = sourceCounts[path];
    if (state) {
      delete sourceCounts[path];
      changed = true;
    }
  });
  stopSourceCountClock();
  if (changed) renderSources();
}

// Belt-and-suspenders against a stream whose per-folder frames were missed
// (e.g. a stub or proxy that only delivers the final frame): the done
// payload's per-source totals settle any row still waiting or scanning.
function applyDoneFrameSourceCounts(counts) {
  if (!counts || typeof counts !== 'object' || Array.isArray(counts)) return;
  let changed = false;
  sources.forEach(path => {
    if (!Object.prototype.hasOwnProperty.call(counts, path)) return;
    const state = sourceCounts[path];
    if (state && (state.status === 'loaded' || state.status === 'error')) return;
    sourceCounts[path] = {
      status: 'loaded',
      count: Number(counts[path] || 0),
      text: formatPhotoCount(counts[path]),
    };
    changed = true;
  });
  stopSourceCountClock();
  if (changed) renderSources();
}

function addSourcePath(path) {
  const v = (path || '').trim();
  if (!v) return false;
  if (!sources.includes(v)) {
    sources.push(v);
    hideDestStructure();
    renderSources();
    scheduleImportPreview();
    return true;
  }
  return false;
}

function addSource() {
  const input = document.getElementById('sourceInput');
  const v = (input.value || '').trim();
  if (!v) return;
  addSourcePath(v);
  input.value = '';
}

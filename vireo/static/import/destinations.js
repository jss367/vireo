// Archive targets, folder templates, recent destinations, and structure preview.
// Classic page script; load boot.js after all definitions.

function selectedFolderTemplate() {
  const preset = document.getElementById('folderTemplatePreset');
  if (preset.value !== '__custom__') return preset.value;
  return (document.getElementById('folderTemplate').value || '').trim();
}

function updateFolderTemplateControls() {
  const preset = document.getElementById('folderTemplatePreset');
  const custom = document.getElementById('folderTemplate');
  if (!preset || !custom) return;
  custom.style.display = preset.value === '__custom__' ? '' : 'none';
  if (preset.value === '__custom__') custom.focus();
}

// Label each folder-template preset with a folder this import would really
// create. The examples come from the destination preview, rendered by the
// same build_destination_path() the copy uses against the earliest capture
// time among the source files — so the label agrees with the resulting-folders
// table instead of contradicting it. Passing null resets to bare patterns,
// which is what we show whenever the real dates aren't known yet: a stale or
// invented example is worse than none.
function applyFolderTemplateSamples(templateSamples) {
  const preset = document.getElementById('folderTemplatePreset');
  if (!preset) return;
  const samples = (templateSamples && templateSamples.samples) || {};
  Array.from(preset.options).forEach((option) => {
    if (option.value === '__custom__') return;
    const sample = samples[option.value];
    const label = option.dataset.label || option.value;
    option.textContent = sample ? label + ' — ' + sample : label;
  });
}

function setFolderTemplate(template) {
  const preset = document.getElementById('folderTemplatePreset');
  const custom = document.getElementById('folderTemplate');
  const commonOption = Array.from(preset.options)
    .some((option) => option.value === template && option.value !== '__custom__');
  preset.value = commonOption ? template : '__custom__';
  custom.value = commonOption ? '' : template;
  updateFolderTemplateControls();
}

function recentDestinationLabel(path) {
  const trimmed = path.replace(/[\\/]+$/, '');
  const parts = trimmed.split(/[\\/]/).filter(Boolean);
  return parts[parts.length - 1] || path;
}

function selectRecentDestination(path) {
  const destination = document.getElementById('destInput');
  destination.value = path;
  // Match a manually entered destination so an already-rendered preview
  // cannot remain visible for the previous folder.
  destination.dispatchEvent(new Event('input', { bubbles: true }));
  destination.focus();
}

function renderRecentDestinations(recents) {
  const section = document.getElementById('recentDestinations');
  const list = document.getElementById('recentDestinationList');
  if (!section || !list) return;
  list.replaceChildren();
  recents.forEach((path) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'btn';
    button.textContent = recentDestinationLabel(path);
    button.title = path;
    button.setAttribute('aria-label', 'Use ' + path);
    button.addEventListener('click', () => selectRecentDestination(path));
    list.appendChild(button);
  });
  section.style.display = recents.length ? '' : 'none';
}

function wireDestStructureInvalidation() {
  [
    'chkRecursive', 'fileTypePreset', 'destMode', 'remoteSubpath',
    'chkSkipDuplicates', 'chkTrustLikelyDuplicates', 'chkVerifyByHash', 'destInput',
    'folderTemplatePreset', 'folderTemplate', 'newWorkspaceName',
    'chkAllowMissingExiftool',
  ].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    const onPreviewOptionChange = () => {
      hideDestStructure();
      scheduleImportPreview();
      updateAfterMoveUI();
      if (id === 'destInput' || id === 'destMode') {
        updateDestinationModeHint();
      }
      // Immediately, not after the 350ms debounce: for the 350ms between the
      // toggle and the re-preview the rendered grid describes the OLD
      // options, and Start would happily send the selection made against it.
      updateStartGate();
    };
    el.addEventListener('change', onPreviewOptionChange);
    el.addEventListener('input', onPreviewOptionChange);
  });
  document.querySelectorAll('.file-ext').forEach((el) => {
    el.addEventListener('change', () => {
      hideDestStructure();
      scheduleImportPreview();
      updateStartGate();
    });
  });
}

function importRemoteTargetId() {
  const el = document.getElementById('destMode');
  const v = el ? el.value : 'local';
  return v.indexOf('remote:') === 0 ? v.slice(7) : '';
}

function importRemoteTarget() {
  const id = importRemoteTargetId();
  if (!id) return null;
  return importRemoteTargets.find(t => t.id === id) || null;
}

function normalizeRemoteSubpath(raw) {
  const trimmed = (raw || '').trim();
  if (!trimmed) return {value: '', error: ''};
  const s = trimmed.replace(/\\/g, '/');
  if (s.charAt(0) === '/' || /^[A-Za-z]:/.test(s)) {
    return {value: '', error: 'Subpath must be relative.'};
  }
  const parts = [];
  for (const seg of s.split('/')) {
    if (seg === '' || seg === '.') continue;
    if (seg === '..') return {value: '', error: "Subpath may not contain '..'."};
    parts.push(seg);
  }
  return {value: parts.join('/'), error: ''};
}

function importIsAbsolutePath(p) {
  if (!p) return false;
  return p.charAt(0) === '/' || /^[A-Za-z]:[\\/]/.test(p) || /^\\\\/.test(p);
}

function rsyncDestSpec(user, host, path) {
  const hostToken = host && host.indexOf(':') !== -1 ? '[' + host + ']' : host;
  return user + '@' + hostToken + ':' + path;
}

function remotePreviewWarn(text) {
  const d = document.createElement('div');
  d.style.color = 'var(--warning,#bf8700)';
  d.textContent = text;
  return d;
}

function updateRemoteDestPreview() {
  const el = document.getElementById('remoteDestPreview');
  if (!el) return;
  el.textContent = '';
  const input = document.getElementById('remoteSubpath');
  const errorEl = document.getElementById('remoteSubpathError');
  const t = importRemoteTarget();
  if (!t) {
    if (errorEl) {
      errorEl.textContent = 'Choose a configured NAS target.';
      errorEl.classList.add('visible');
    }
    if (input) input.classList.add('input-invalid');
    updateStartGate();
    return;
  }
  const subResult = normalizeRemoteSubpath(input.value);
  let fieldError = '';
  if (subResult.error) {
    fieldError = subResult.error + ' Enter a folder such as Raw Files/USA.';
  } else if (!subResult.value) {
    fieldError = 'Enter the folder inside ' + t.remote_path +
      ', for example Raw Files/USA.';
  } else {
    const line = document.createElement('div');
    line.textContent = 'Vireo will send files directly over SSH to: ';
    const span = document.createElement('span');
    span.style.fontFamily = 'monospace';
    span.textContent = rsyncDestSpec(
      t.user, t.host, t.remote_path.replace(/\/+$/, '') + '/' + subResult.value);
    line.appendChild(span);
    el.appendChild(line);
    if (t.mount_path) {
      const catalogLine = document.createElement('div');
      catalogLine.textContent = 'The same folder is cataloged at: ';
      const catalogPath = document.createElement('span');
      catalogPath.style.fontFamily = 'monospace';
      catalogPath.textContent =
        t.mount_path.replace(/\/+$/, '') + '/' + subResult.value;
      catalogLine.appendChild(catalogPath);
      el.appendChild(catalogLine);
    }
  }
  if (errorEl) {
    errorEl.textContent = fieldError;
    errorEl.classList.toggle('visible', !!fieldError);
  }
  if (input) input.classList.toggle('input-invalid', !!fieldError);
  if (!importRsyncAvailable) {
    el.appendChild(remotePreviewWarn('Warning: no GNU rsync found. ' + rsyncInstallHint()));
  }
  if (!t.mount_path) {
    el.appendChild(remotePreviewWarn('Warning: this target needs a local mount path for cataloging. Add one under Settings > Remote targets.'));
  } else if (!importIsAbsolutePath(t.mount_path)) {
    el.appendChild(remotePreviewWarn('Warning: this target local mount path must be absolute.'));
  }
  updateStartGate();
}

function remoteArchiveSelectionReady() {
  const t = importRemoteTarget();
  if (!t || !importRsyncAvailable) return false;
  if (!t.mount_path || !importIsAbsolutePath(t.mount_path)) return false;
  const subResult = normalizeRemoteSubpath(document.getElementById('remoteSubpath').value);
  return !subResult.error && !!subResult.value;
}

function updateDestinationModeHint() {
  const hint = document.getElementById('destModeHint');
  if (!hint) return;
  if (localProcessingRequest()) {
    hint.textContent = deferNasTransferRequest()
      ? 'This is the final NAS destination. Photos stay on this computer for processing and review until you choose Send to NAS.'
      : 'This is the final NAS destination. Photos are imported and processed in temporary storage on this computer, then transferred here.';
    return;
  }
  const target = importRemoteTarget();
  if (target) {
    hint.textContent = 'Direct SSH transfer to ' + target.remote_path +
      '. The folder inside that root is required below.';
    return;
  }
  const destination =
    (document.getElementById('destInput')?.value || '').trim();
  const mountedTarget = importRemoteTargets.find((item) => {
    const mount = String(item.mount_path || '').replace(/\/+$/, '');
    return mount && (
      destination === mount || destination.indexOf(mount + '/') === 0
    );
  });
  hint.textContent = mountedTarget
    ? 'Using ' + mountedTarget.name + ' through its Finder-mounted path ' +
      mountedTarget.mount_path + '. Files are copied through the mounted volume.'
    : 'Use this for folders on this Mac and NAS volumes mounted in Finder.';
}

function onImportDestModeChange() {
  const remoteId = importRemoteTargetId();
  const localRows = document.getElementById('destLocalRows');
  const remoteRows = document.getElementById('destRemoteRows');
  if (localRows) localRows.style.display = remoteId ? 'none' : '';
  if (remoteRows) remoteRows.style.display = remoteId ? '' : 'none';
  updateDestinationModeHint();
  updateRemoteDestPreview();
  updateAfterMoveUI();
  updateStartGate();
}

// Bounded fetch of /api/remote-targets, shared by the initial load and the
// after-move refresh. A HUNG endpoint (e.g. the server blocked on a stale
// SMB mount) must settle either way: an unbounded background fetch would
// leave its in-flight flag set forever and every later trigger would be
// swallowed until a reload.
// Requests are numbered so a response can never overwrite state applied
// from a newer request: the initial load and a background refresh can be
// in flight together (a slow first load while the user fills in enough of
// the form to show the after-move hint), and if Settings changes a target
// in between, the older snapshot must lose regardless of arrival order.
let _remoteTargetsRequestSeq = 0;
let _remoteTargetsAppliedSeq = 0;
async function fetchImportRemoteTargets() {
  const seq = ++_remoteTargetsRequestSeq;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 10000);
  let resp;
  try {
    resp = await fetch('/api/remote-targets', { signal: ctrl.signal });
  } finally {
    clearTimeout(timer);
  }
  if (!resp.ok) throw new Error('remote targets unavailable');
  return { seq, data: await resp.json() };
}

function remoteTargetsResponseIsStale(res) {
  return !res || res.seq < _remoteTargetsAppliedSeq;
}

// Apply a successful response: the target list AND the tool availability
// it reports. Whichever path fetched it, a success is a full recovery —
// leaving importRsyncAvailable/importSshAvailable at their initial false
// while repopulating the dropdown would block the very SSH destinations it
// just restored.
let _previousRemoteTargets = [];
function applyImportRemoteTargets(res) {
  const data = res.data;
  _remoteTargetsAppliedSeq = Math.max(_remoteTargetsAppliedSeq, res.seq);
  _previousRemoteTargets = importRemoteTargets;
  importRemoteTargets = (data && data.targets) || [];
  importRsyncAvailable = !!(data && data.rsync_available);
  importSshAvailable = !!(data && data.ssh_available);
  _afterMoveTargetsCheckedAt = Date.now();
}

function renderRemoteTargetsError(failed) {
  const errEl = document.getElementById('remoteTargetsError');
  if (!errEl) return;
  errEl.textContent = '';
  errEl.dataset.kind = failed ? 'load-failed' : '';
  errEl.classList.toggle('visible', failed);
  if (!failed) return;
  errEl.append(
    'Couldn’t load the SSH archive destinations — the ' +
    'server didn’t answer. ' +
    (importRemoteTargets.length
      ? 'Showing the last known list. '
      : 'Only local or mounted destinations are shown. '));
  const retry = document.createElement('button');
  retry.type = 'button';
  retry.className = 'retry-link';
  retry.textContent = 'Retry';
  retry.addEventListener('click', () => loadImportRemoteTargets());
  errEl.appendChild(retry);
}

function remoteTargetsLoadFailed() {
  const errEl = document.getElementById('remoteTargetsError');
  return !!(errEl && errEl.classList.contains('visible')
    && errEl.dataset.kind === 'load-failed');
}

async function loadImportRemoteTargets() {
  // A failure here used to silently wipe the SSH options and leave a
  // local-only dropdown with no explanation — and a HUNG endpoint never
  // settled this promise at all, so the dropdown just never grew its SSH
  // entries. Bound the wait, say what happened, offer a retry, and keep
  // the last known target list rather than discarding a working option
  // over a transient failure.
  let failed = false;
  try {
    const res = await fetchImportRemoteTargets();
    // A newer refresh already applied a later snapshot: this one is
    // history, and applying it would show a configuration Start won't use.
    if (remoteTargetsResponseIsStale(res)) return;
    applyImportRemoteTargets(res);
  } catch (e) {
    failed = true;
  }
  renderRemoteTargetsError(failed);
  renderImportDestModes();
}

// Rebuild the Archive Destination dropdown from importRemoteTargets.
// Rebuilding removes the selected option, which silently flips the form
// back to "Local" — put the selection back if its target survived (matters
// for the Retry link and for the after-move refresh: a filled-in remote
// form must not reset itself when the list is re-fetched).
function renderImportDestModes() {
  const sel = document.getElementById('destMode');
  if (!sel) return;
  const prevValue = sel.value;
  const prevTarget = _previousRemoteTargets.find(
    (t) => 'remote:' + t.id === prevValue) || null;
  while (sel.options.length > 1) sel.remove(1);
  importRemoteTargets.forEach((t) => {
    const opt = document.createElement('option');
    opt.value = 'remote:' + t.id;
    opt.textContent = t.name + ' — direct transfer over SSH';
    sel.appendChild(opt);
  });
  let restored = Array.prototype.some.call(sel.options, (o) => o.value === prevValue);
  if (restored) {
    sel.value = prevValue;
  } else if (prevTarget) {
    // A legacy target saved without an id is served under a synthesized
    // id and re-saved by Settings under a generated one. Follow it across
    // the id change only when the match is unambiguous: same user, host
    // AND remote path. Anything looser (e.g. "the one remaining target on
    // this host") could turn a deleted target into a different NAS path
    // while keeping remote mode selected, so that case falls through to
    // the visible selection-lost fallback instead.
    const same = migratedRemoteTarget(prevTarget, importRemoteTargets, _previousRemoteTargets);
    if (same) {
      sel.value = 'remote:' + same.id;
      restored = true;
    }
  }
  if (String(prevValue).startsWith('remote:') && !restored) {
    // The selected SSH destination is gone from the saved targets. The
    // dropdown falls back to Local; say so, because the local path field
    // may still hold a usable path and a silent switch would let Start
    // import to a destination the user never chose.
    noteRemoteSelectionLost(prevTarget ? prevTarget.name : '');
  }
  onImportDestModeChange();
  updateAfterMoveUI();
}

// The refreshed entry that is unambiguously the same target as ``prev``
// under a different id, or null. Shared by the direct-SSH destination
// dropdown and the chained-move target select.
// A migration is: the old id vanished and exactly one *new* id appeared
// with the same connection tuple. A tuple twin that already existed
// before the refresh (same NAS path under a different port/key/mount) is
// a different target, not a migration — treating it as one would keep a
// selection pointing at a configuration the user never chose.
function migratedRemoteTarget(prev, targets, previousTargets) {
  if (!prev) return null;
  const known = new Set((previousTargets || []).map((t) => t.id));
  const matches = targets.filter(
    (t) => t.id !== prev.id && !known.has(t.id)
      && t.user === prev.user && t.host === prev.host
      && t.remote_path === prev.remote_path);
  return matches.length === 1 ? matches[0] : null;
}

function noteRemoteSelectionLost(name) {
  const errEl = document.getElementById('remoteTargetsError');
  if (!errEl) return;
  errEl.textContent = (name ? '"' + name + '"' : 'The selected SSH destination')
    + ' is no longer configured under Settings › Remote targets. '
    + 'Switched to a local or mounted destination — check it before starting.';
  errEl.dataset.kind = 'selection-lost';
  errEl.classList.add('visible');
}

function hideDestStructure() {
  destStructureSeq += 1;
  // The folder-template examples describe the same file set as the structure
  // table, so they retire with it. This is the shared invalidation path —
  // several callers (removing the last source, a preview that finds no
  // importable files) hide the table and never reach renderDestStructure(),
  // and resetting only there would leave the dropdown advertising dates from
  // a source that is no longer selected.
  applyFolderTemplateSamples(null);
  const el = document.getElementById('destStructure');
  if (!el) return;
  el.style.display = 'none';
  el.innerHTML = '';
}

// Resolve the absolute local path files will be cataloged at, for the
// destination-structure preview. Local mode: the destination field. Remote
// (SSH) mode: the target's local mount path joined with the subpath — the
// same path the import job catalogs at (mount_final). Returns '' when the
// path isn't known/absolute yet, in which case the structure preview is
// skipped (the duplicate summary still stands).
function resolvedCopyDestination() {
  if (importRemoteTargetId()) {
    const t = importRemoteTarget();
    if (!t || !t.mount_path || !importIsAbsolutePath(t.mount_path)) return '';
    const sub = normalizeRemoteSubpath(document.getElementById('remoteSubpath').value);
    // The mount root by itself is not a valid remote selection: the SSH
    // importer requires a folder below the configured remote root. Rendering
    // a destination preview for the bare mount made an invalid form look
    // complete and could surface an unrelated managed archive nested below
    // that mount.
    if (sub.error || !sub.value) return '';
    const base = t.mount_path.replace(/\/+$/, '');
    return base + '/' + sub.value;
  }
  const local = (document.getElementById('destInput').value || '').trim();
  return importIsAbsolutePath(local) ? local : '';
}

function destStructureSignature(destination, fileTypes, recursive, excludePaths) {
  return JSON.stringify({
    sources: sources.slice(),
    destination: destination,
    folder_template: selectedFolderTemplate(),
    file_types: fileTypes,
    recursive: recursive,
    skip_duplicates: document.getElementById('chkSkipDuplicates').checked,
    verify_by_hash: document.getElementById('chkVerifyByHash').checked,
    trust_likely_duplicates: document.getElementById('chkTrustLikelyDuplicates').checked,
    exclude_paths: (excludePaths || []).slice().sort(),
  });
}

// Preview the destination folder structure (new vs existing folders) and,
// when the destination sits in a folder Vireo already manages, a callout
// naming that archive and how many photos it already holds. Best-effort:
// any failure leaves the destination unknown and simply shows nothing, so
// the duplicate summary above is never clobbered. ``excludePaths`` are the
// source files that will be skipped as duplicates — excluding them keeps the
// new/existing folder counts matching the files that actually land.
async function renderDestStructure(excludePaths, signal) {
  // hideDestStructure() also drops the previous run's example folder names.
  // Unlike the move page — where the examples depend only on the selected
  // folder — an import's examples come from the set of source files that will
  // actually land, which every re-render can change (sources added or
  // removed, or more files excluded as duplicates). Bare patterns for a
  // moment beat an example derived from files that are no longer importing.
  hideDestStructure();
  const destination = resolvedCopyDestination();
  if (!destination) return null;
  const fileTypes = selectedFileTypes();
  const recursive = document.getElementById('chkRecursive').checked;
  const requestSeq = destStructureSeq;
  const requestSignature = destStructureSignature(
    destination, fileTypes, recursive, excludePaths);
  let data;
  try {
    const resp = await fetch('/api/import/destination-preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      signal: signal,
      body: JSON.stringify({
        sources: sources,
        destination: destination,
        folder_template: selectedFolderTemplate(),
        file_types: fileTypes,
        recursive: recursive,
        exclude_paths: excludePaths || [],
      }),
    });
    if (!resp.ok) return null;
    data = await resp.json();
  } catch (e) {
    return null;  // structure preview is optional; keep the summary intact
  }
  const el = document.getElementById('destStructure');
  if (
    requestSeq !== destStructureSeq ||
    requestSignature !== destStructureSignature(
      resolvedCopyDestination(), selectedFileTypes(),
      document.getElementById('chkRecursive').checked, excludePaths)
  ) {
    return;
  }
  // Before the early return below: the dropdown's examples are useful even
  // when there is no folder table to draw.
  applyFolderTemplateSamples(data.template_samples);
  const folders = data.folders || [];
  const archives = Array.isArray(data.managed_archives)
    ? data.managed_archives
    : (data.managed_archive
        ? [{
            path: data.managed_archive.path,
            photo_count: data.managed_archive.photo_count,
            coverage: 'full',
          }]
        : []);
  if (!folders.length && !archives.length) return data;

  const headline = document.createElement('div');
  headline.className = 'structure-headline';
  headline.textContent = 'Resulting folders: ' + data.total_photos +
    ' file' + (data.total_photos === 1 ? '' : 's') + ' split into ' +
    data.total_folders + ' folder' + (data.total_folders === 1 ? '' : 's') +
    ' (' + data.new_folders + ' new, ' + data.existing_folders + ' existing)';
  el.appendChild(headline);

  // Phrase each callout in terms of where the generated folders land,
  // not the selected destination — the backend flags an archive whenever
  // *some* generated folders sit inside it (including when the
  // destination is above the archive but the template maps files back
  // into it). Coverage 'full' means every generated folder lands inside
  // this archive; 'partial' means only some do — either because part of
  // the source lands outside every tracked archive, or because multiple
  // archives split the generated folders between them. Say so, so the
  // caller does not read "lands inside X" as "all of it lands inside X".
  const multipleArchives = archives.length > 1;
  archives.forEach((arch) => {
    const callout = document.createElement('div');
    callout.className = 'managed-archive-callout';
    const label = document.createElement('span');
    const partial = arch.coverage === 'partial' || multipleArchives;
    label.textContent = partial
      ? 'Some files imported here will land inside the managed archive rooted at '
      : 'Files imported here land inside the managed archive rooted at ';
    const path = document.createElement('span');
    path.style.fontFamily = 'monospace';
    path.textContent = arch.path;
    const tail = document.createElement('span');
    tail.textContent = ' — ' + arch.photo_count +
      ' photo' + (arch.photo_count === 1 ? '' : 's') +
      ' already cataloged there.';
    callout.append(label, path, tail);
    el.appendChild(callout);
  });

  const table = document.createElement('table');
  table.className = 'structure-table';
  const header = document.createElement('tr');
  const folderHeading = document.createElement('th');
  folderHeading.scope = 'col';
  folderHeading.textContent = 'Exact folder';
  const countHeading = document.createElement('th');
  countHeading.scope = 'col';
  countHeading.className = 'num';
  countHeading.textContent = 'Files';
  const statusHeading = document.createElement('th');
  statusHeading.scope = 'col';
  statusHeading.className = 'tag';
  statusHeading.textContent = 'Status';
  header.append(folderHeading, countHeading, statusHeading);
  table.appendChild(header);
  folders.forEach((f) => {
    const tr = document.createElement('tr');
    const name = document.createElement('td');
    name.className = 'path';
    name.textContent = f.full_path || (f.path === '.' ? '(archive root)' : f.path);
    const count = document.createElement('td');
    count.className = 'num';
    count.textContent = String(f.count);
    const tag = document.createElement('td');
    tag.className = 'tag ' + (f.exists ? 'tag-existing' : 'tag-new');
    tag.textContent = f.exists ? 'existing' : 'new';
    tr.append(name, count, tag);
    table.appendChild(tr);
  });
  el.appendChild(table);
  el.style.display = '';
  return data;
}

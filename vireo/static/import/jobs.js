// Import submission, job progress, completion, and failed-file retries.
// Classic page script; load boot.js after all definitions.

function renderFolderTable(el, folders) {
  el.innerHTML = '';
  Object.keys(folders).sort().forEach((rel) => {
    const c = folders[rel];
    const tr = document.createElement('tr');
    const name = document.createElement('td');
    name.textContent = rel;
    const copied = document.createElement('td');
    copied.className = 'num';
    copied.textContent = c.copied + ' copied';
    const skipped = document.createElement('td');
    skipped.className = 'num';
    skipped.textContent = c.skipped_duplicate + ' duplicate';
    const failed = document.createElement('td');
    failed.className = 'num';
    failed.textContent = c.failed ? (c.failed + ' failed') : '';
    if (c.failed) failed.style.color = 'var(--danger)';
    tr.append(name, copied, skipped, failed);
    el.appendChild(tr);
  });
}

async function startImport() {
  showError('');
  addImportTagFromInput();
  const problem = importFormProblem();
  if (problem) {
    showError(problem.message, problem.target);
    updateStartGate();
    return;
  }
  const allowMissingExiftool = !!document.getElementById('chkAllowMissingExiftool')?.checked;
  const copyMode = newImagesSnapshotId === null && selectedImportMode() === 'copy';
  const body = newImagesSnapshotId !== null ? {
    source_snapshot_id: newImagesSnapshotId,
  } : {
      sources: sources,
      recursive: document.getElementById('chkRecursive').checked,
    };
  if (allowMissingExiftool) body.allow_missing_exiftool = true;
  if (importTags.length) body.tags = importTags.slice();
  if (document.getElementById('chkLocationFromGps').checked) {
    body.location_from_gps = true;
  }
  if (document.getElementById('workspaceNew').checked) {
    const name = (document.getElementById('newWorkspaceName').value || '').trim();
    if (!name) { showError('Name the new workspace.'); return; }
    body.new_workspace_name = name;
  }
  if (copyMode) {
    const fileTypes = selectedFileTypes();
    if (Array.isArray(fileTypes) && !fileTypes.length) {
      showError('Choose at least one file extension.');
      return;
    }
    const remoteId = importRemoteTargetId();
    if (remoteId) {
      if (!remoteArchiveSelectionReady()) {
        showError('Choose a usable remote target and relative subpath.');
        return;
      }
      body.remote_target_id = remoteId;
      body.remote_subpath = normalizeRemoteSubpath(
        document.getElementById('remoteSubpath').value).value;
    } else {
      const destination = (document.getElementById('destInput').value || '').trim();
      if (!destination) { showError('Choose a destination folder.'); return; }
      body.destination = destination;
    }
    body.folder_template = selectedFolderTemplate();
    body.file_types = fileTypes;
    body.skip_duplicates = document.getElementById('chkSkipDuplicates').checked;
    body.verify_by_hash = document.getElementById('chkVerifyByHash').checked;
    body.trust_likely_duplicates =
      document.getElementById('chkTrustLikelyDuplicates').checked;
    // Only a COMPLETED preview describes a file list; with none the import
    // sends no selection at all and copies everything, which is what the
    // screen says. This branch is copy-mode only for a second reason:
    // previewImport() captures the signature BEFORE its `if (!copyMode)`
    // return, so an in-place preview leaves it non-null and the guard alone
    // would post a file list to a route that scans whatever is on disk.
    if (importPreviewCapturedSignature !== null) {
      // include_paths is NOT the checked boxes. A deselection only counts if
      // the file was eligible in the first place, so a click landing on a
      // duplicate before its verdict arrived is discarded here — nothing
      // else ever removes it from importDeselected. Duplicates must reach
      // the job or they land in no ledger bucket: skipped_duplicate is only
      // incremented inside the copy loop over the FILTERED list, so a
      // duplicate dropped here makes copied + skipped_duplicate fall short
      // of discovered and a fully-archived card is reported unsafe to
      // format, blaming the user for deselecting files they never touched.
      //
      // Eligibility is read off the rendered cards, like every other
      // selection readout on this page — see importSelectableCardPaths().
      // importGridCards() and not importVisibleCards() to match
      // importCheckedCount(), which walks the same set: checked_count and
      // include_paths are compared to each other by the route, so they must
      // be derived from the same cards or the relation between them stops
      // being a property of the code and becomes a coincidence of which
      // cards the hide-duplicates checkbox happens to be hiding.
      const selectable = new Set(importSelectableCardPaths(importGridCards()));
      const eligibleDeselections = new Set(
        Array.from(importDeselected).filter(p => selectable.has(p)));
      body.include_paths = importPreviewedPaths.filter(
        p => !eligibleDeselections.has(p));
      // UNIQUE, for the nested-sources reason on importEligibleCount():
      // folder-preview appends per source, so /card and /card/DCIM emit the
      // same file twice and previewed_count would over-count the discovery
      // the job is asked to compare itself against.
      body.previewed_count = new Set(importPreviewedPaths).size;
      body.checked_count = importCheckedCount();
    }
  }
  // For new-workspace imports where the user hasn't touched the After
  // Import dropdown, the visible selection was prefilled from the
  // currently-active workspace's default — posting it would make the
  // server apply that old default instead of the new workspace's (or
  // global) one. Omit the key so the server resolves it against the
  // freshly-created workspace after the switch.
  const newWorkspaceUntouched =
    !!body.new_workspace_name && !_afterImportUserTouched;
  if (
    _afterImportConfigLoaded
    && !_afterImportHidingDefault
    && !newWorkspaceUntouched
  ) {
    body.after_import = selectedAfterImport();
  }
  // The chained NAS move is only offered while its row is visible (local
  // archive copy + chained process + eligible target) — a hidden row means
  // a stale checkbox must not send anything.
  body.after_process_move = afterMoveRequest();
  body.local_processing = localProcessingRequest();
  body.defer_nas_transfer = deferNasTransferRequest();
  importStartPending = true;
  updateStartGate();
  try {
    const resp = await fetch(copyMode ? '/api/jobs/import-photos' : '/api/jobs/import-in-place', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || 'import failed to start');
    if (data.workspace && data.workspace.name) {
      const activeName = document.getElementById('activeWorkspaceName');
      if (activeName) activeName.textContent = '(' + data.workspace.name + ')';
    }
    activeJobId = data.job_id;
    importStartPending = false;
    updateStartGate();
    document.getElementById('progressCard').style.display = '';
    document.getElementById('resultCard').style.display = 'none';
    watchJob(activeJobId);
  } catch (e) {
    importStartPending = false;
    showError(String(e.message || e), document.getElementById('btnStart'));
    updateStartGate();
  }
}

function watchJob(jobId) {
  if (es) { es.close(); es = null; }
  es = new EventSource('/api/jobs/' + jobId + '/stream');
  es.addEventListener('progress', (evt) => {
    try { renderProgress(JSON.parse(evt.data)); } catch (e) { /* ignore */ }
  });
  es.addEventListener('complete', () => { es.close(); es = null; finishJob(jobId); });
  es.addEventListener('error', () => {
    // Stream dropped (server restart, network) — fall back to polling the
    // job once rather than pretending progress continues.
    if (es) { es.close(); es = null; }
    finishJob(jobId);
  });
}

function renderProgress(data) {
  let phaseText =
    (data.phase || '') + (data.current_file ? ' · ' + data.current_file : '');
  // Remote imports report the current batch's actual network transfer as
  // sub-phase progress. Show it next to the prepared-files counter — the
  // main bar advances while files are merely inspected and queued, so
  // without this line "200 / 5,523" reads as 200 files safely copied
  // while the batch is still crossing the network.
  if (data.phase_total > 0 && data.phase_label) {
    phaseText += ' · ' + data.phase_label + ' ' +
      (data.phase_current || 0) + ' / ' + data.phase_total;
  }
  document.getElementById('progressPhase').textContent = phaseText;
  // Once indexing finishes, working-copy generation becomes the active phase.
  // Drive the visible bar from that phase instead of leaving it pinned at the
  // indexing counter's 100% while thousands of files are still being rendered.
  // Other sub-phases, notably remote transfer batches, are intentionally local
  // to one batch and must not replace the import's overall progress bar.
  const workingCopyPhase =
    data.phase_label === 'Generating working copies' && data.phase_total > 0;
  const visibleCurrent = workingCopyPhase ? (data.phase_current || 0) : data.current;
  const visibleTotal = workingCopyPhase ? data.phase_total : data.total;
  const pct = visibleTotal ? Math.round(100 * visibleCurrent / visibleTotal) : 0;
  document.getElementById('progressFill').style.width = pct + '%';
  if (data.folders && Object.keys(data.folders).length) {
    renderFolderTable(document.getElementById('folderProgress'), data.folders);
  }
}

async function finishJob(jobId) {
  let job = null;
  for (let i = 0; i < 30; i++) {
    const resp = await fetch('/api/jobs/' + jobId);
    if (resp.ok) {
      job = await resp.json();
      if (['queued', 'running', 'pausing', 'paused'].includes(job.status)) i = -1;
      if (['completed', 'failed', 'cancelled', 'expired'].includes(job.status)) break;
    }
    await new Promise(r => setTimeout(r, 1000));
  }
  // The job is over, so drop the id the gate reads as "Importing…". Nothing
  // else consumes activeJobId once watchJob() has its stream, and leaving it
  // set would keep Start disabled for the rest of the page's life.
  activeJobId = null;
  importStartPending = false;
  updateStartGate();
  if (!job || !job.result) {
    showError('Import ended but its result could not be loaded — check the Jobs page.');
    return;
  }
  lastFinishedImportJob = job;
  renderResult(job.result, job.status);
}

function retryBodyFromFinishedJob(job) {
  const cfg = (job && job.config) || {};
  // Recovery retry always POSTs to /api/jobs/import-photos, which rejects
  // requests without a destination. An in-place import finishes with a
  // non-empty sources list but cfg.destination === null / cfg.mode ===
  // "in_place", so exposing the retry action there would offer a run that
  // cannot start. Gate on the persisted job type/mode the same way the
  // Jobs page does.
  if (!job || job.type !== 'import' || cfg.mode === 'in_place') return null;
  if (!Array.isArray(cfg.sources) || !cfg.sources.length) return null;
  // The server binds the retry's carry list to this parent (below) and
  // — when the parent used a remote target — verifies the current
  // resolution still matches the parent's snapshot. Skip when the
  // parent has no id (defensive; production always has one).
  if (!job.id) return null;
  // Preserve `""` when the parent's config explicitly stored an empty
  // folder_template — that means "copy directly into the archive root",
  // and `||` here would silently redirect the retry to a date-tree the
  // user didn't ask for. Fall back to the default only when the key is
  // truly absent/nullish. Same treatment on the Jobs page.
  const folderTemplate = cfg.folder_template != null
    ? cfg.folder_template : '%Y/%Y-%m-%d';
  // Preserve the parent's duplicate-skip semantics. Forcing skip_duplicates
  // on for every retry means a failed file that happens to match some
  // existing catalog entry — including from an unrelated card — is
  // silently skipped again instead of getting the copy the user asked
  // for; that hits imports the user deliberately configured with
  // duplicate skipping OFF particularly hard. The copy layer treats
  // byte-identical prior successes already sitting at the destination
  // path as adoptable skips regardless of this flag, so preserving the
  // parent value still avoids re-copying files that landed successfully.
  const parentSkipDuplicates = cfg.skip_duplicates != null
    ? !!cfg.skip_duplicates : true;
  const body = {
    sources: cfg.sources.slice(),
    destination: cfg.destination,
    recursive: cfg.recursive !== false,
    folder_template: folderTemplate,
    file_types: cfg.file_types || 'both',
    skip_duplicates: parentSkipDuplicates,
    verify_by_hash: !!cfg.verify_by_hash,
    trust_likely_duplicates: !!cfg.trust_likely_duplicates,
    after_import: cfg.after_import == null ? null : cfg.after_import,
    tags: Array.isArray(cfg.tags) ? cfg.tags.slice() : [],
    location_from_gps: !!cfg.location_from_gps,
    allow_missing_exiftool: !!cfg.allow_missing_exiftool,
    parent_import_job_id: job.id,
  };
  // Carry forward the complete original import scope so the after-import
  // process runs on ALL files the user meant to import, not only the
  // ones this retry newly landed. The parent's failed run skipped
  // chaining entirely (its result.ok was false), so those photos are
  // still unprocessed even though they're on disk. On a retry-of-retry
  // the parent's own carry list must persist too, otherwise the second
  // retry would forget everything the first retry inherited from the
  // original attempt.
  const parentCarry = Array.isArray(cfg.carry_photo_ids)
    ? cfg.carry_photo_ids : [];
  // Everything the parent carried (its own carry list plus photos a
  // resume recovered from an interrupted run) stays in scope too.
  const parentImported = ['photo_ids', 'carried_photo_ids', 'recovered_photo_ids']
    .flatMap((key) => (Array.isArray(job.result && job.result[key]) ? job.result[key] : []));
  const carry = [];
  const carrySeen = new Set();
  parentCarry.concat(parentImported).forEach((pid) => {
    if (typeof pid !== 'number' || !Number.isInteger(pid) || pid <= 0) return;
    if (carrySeen.has(pid)) return;
    carrySeen.add(pid);
    carry.push(pid);
  });
  if (carry.length) body.carry_photo_ids = carry;
  // Recover the parent's per-file selection so the retry stays scoped to
  // the same files the user originally chose. Without this the retry
  // would either be rejected by the source-signature drift check (the
  // parent's ``source_snapshots`` are captured pre-filter, so the
  // signatures agree, but the parent's ``include_paths`` is the real
  // ground truth for what should get copied) or — worse — silently
  // re-import the files the user deliberately deselected on the parent
  // run. All three fields travel together (see the server-side
  // ``include_paths, previewed_count and checked_count must be sent
  // together`` gate); sending a subset would 400. Parents from before
  // ``include_paths`` was persisted omit it and simply retry as before.
  if (Array.isArray(cfg.include_paths) && cfg.include_paths.length
      && typeof cfg.previewed_count === 'number'
      && typeof cfg.checked_count === 'number') {
    body.include_paths = cfg.include_paths.slice();
    body.previewed_count = cfg.previewed_count;
    body.checked_count = cfg.checked_count;
  }
  if (cfg.remote_target_id) {
    delete body.destination;
    body.remote_target_id = cfg.remote_target_id;
    body.remote_subpath = cfg.remote_subpath;
  }
  if (cfg.after_process_move && cfg.after_process_move.remote_target_id) {
    body.after_process_move = {
      remote_target_id: cfg.after_process_move.remote_target_id,
    };
  }
  body.local_processing = !!cfg.local_processing;
  body.defer_nas_transfer = !!cfg.defer_nas_transfer;
  return body;
}

async function retryFailedImport() {
  const button = document.getElementById('btnRetryImport');
  const hint = document.getElementById('retryImportHint');
  const body = retryBodyFromFinishedJob(lastFinishedImportJob);
  if (!body) {
    hint.textContent = 'The original import settings are no longer available. Start a new import from the source card.';
    return;
  }
  button.disabled = true;
  button.textContent = 'Starting retry…';
  hint.textContent = 'Already imported files will be skipped.';
  // Flip the in-flight flag BEFORE the retry request goes out — not after
  // its response arrives. Retry validation synchronously re-enumerates
  // sources and hashes every carried destination file to guard against
  // stealth overwrites, which pushes the fetch to a materially non-trivial
  // wall-clock. If the main Start button remains enabled during that
  // window, a click launches an ordinary import (no ``parent_import_job_id``
  // in its body) that the server-side retry exclusivity gate can't catch,
  // and the two runs race over the same card and destination. Mirror what
  // startImport() does: flag first, updateStartGate(), then fetch;
  // clear the flag on failure so the button re-enables. See PR #1387
  // Codex review.
  importStartPending = true;
  updateStartGate();
  try {
    const resp = await fetch('/api/jobs/import-photos', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.error || 'retry failed to start');
    activeJobId = data.job_id;
    document.getElementById('progressCard').style.display = '';
    document.getElementById('resultCard').style.display = 'none';
    document.getElementById('progressCard').scrollIntoView({
      behavior: preferredScrollBehavior(), block: 'center',
    });
    watchJob(activeJobId);
  } catch (e) {
    importStartPending = false;
    updateStartGate();
    button.disabled = false;
    button.textContent = 'Retry failed files';
    hint.textContent = String(e.message || e);
  }
}

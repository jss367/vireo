// Import mode, file types, tags, validation, and the Start button gate.
// Classic page script; load boot.js after all definitions.

function normalizedImportTag(name) {
  return String(name || '').trim().replace(/\s+/g, ' ');
}

function renderImportTags() {
  const list = document.getElementById('importTagList');
  list.innerHTML = '';
  importTags.forEach((name, index) => {
    const chip = document.createElement('span');
    chip.className = 'import-tag-chip';
    const label = document.createElement('span');
    label.textContent = name;
    const remove = document.createElement('button');
    remove.type = 'button';
    remove.setAttribute('aria-label', 'Remove tag ' + name);
    remove.textContent = '×';
    remove.onclick = () => {
      importTags.splice(index, 1);
      renderImportTags();
    };
    chip.append(label, remove);
    list.appendChild(chip);
  });
}

function addImportTag(name) {
  const clean = normalizedImportTag(name);
  if (!clean) return false;
  if (!importTags.some(tag => tag.toLocaleLowerCase() === clean.toLocaleLowerCase())) {
    importTags.push(clean);
    renderImportTags();
  }
  return true;
}

function addImportTagFromInput() {
  const input = document.getElementById('importTagInput');
  if (addImportTag(input.value)) input.value = '';
}

function handleImportTagKeydown(event) {
  if (event.key !== 'Enter' && event.key !== ',') return;
  event.preventDefault();
  addImportTagFromInput();
}

function selectedImportMode() {
  const checked = document.querySelector('input[name="importMode"]:checked');
  return checked ? checked.value : 'in_place';
}

// SINGLE DEFINITION of "per-file selection applies here". Read by the
// renderer (which draws the boxes), updateImportSelectionUI() (which shows
// the select-all row and the explanatory note) and updateStartGate() (which
// counts the selection into the button label). Three copies of this
// expression would be three chances for the boxes and the readouts about
// them to disagree.
//
// Copy mode only. The in-place path runs through do_scan(restrict_files=…),
// which vireo/scanner.py honours only alongside restrict_dirs, and filling
// restrict_dirs would change which directories get registered as workspace
// roots — a core scanner change, deliberately deferred. The snapshot
// (new-images) flow is a third state: it imports exactly one frozen list.
function importSelectionEnabled() {
  return newImagesSnapshotId === null && selectedImportMode() === 'copy';
}

// Why the boxes aren't there. An unexplained absence is the same class of
// failure as an unexplained disabled control: the user cannot tell whether
// selection is missing, broken, or hidden behind a setting they haven't
// found. Both strings therefore carry two halves — where selection DOES
// live, and what this mode will do instead — and both halves are pinned by
// tests.
//
// "Copy to archive" is the radio's own label, quoted exactly: a user
// scanning the screen for the words the note used cannot find "when copying
// files". Plural "source folders" because Add in place walks every source
// added, recursively when Include subfolders is on.
//
// The two strings must stay distinct. In snapshot mode both mode radios are
// DISABLED, so naming Copy to archive would point at a control the user
// cannot reach, and that import adds exactly the captured list rather than
// everything the source folders hold.
function selectionUnavailableText() {
  if (newImagesSnapshotId !== null) {
    return 'File selection is not available for this import. It adds exactly'
      + ' the captured list of newly detected images, leaving the originals'
      + ' where they are.';
  }
  return 'File selection is available in Copy to archive mode. Add in place'
    + ' catalogs every file it finds in your source folders.';
}

function updateImportMode() {
  if (newImagesSnapshotId !== null) {
    document.getElementById('modeInPlace').checked = true;
  }
  activeImportMode = selectedImportMode();
  const copyMode = activeImportMode === 'copy';
  document.getElementById('destCard').style.display = copyMode ? '' : 'none';
  document.getElementById('fileTypesWrap').style.display = copyMode ? '' : 'none';
  document.getElementById('btnPreview').textContent =
    copyMode ? 'Check for duplicates' : 'Preview import';
  document.getElementById('previewSummary').textContent = '';
  hideDestStructure();
  clearImportPreviewGrid();
  // A mode switch throws the rendered preview away, so the lifecycle has to
  // hear about it. Staleness alone can't: copy -> in place -> copy inside the
  // 350ms re-preview debounce restores the EXACT signature the completed
  // preview captured, so the check reports "current" over a grid that no
  // longer exists — live Start, invisible grid, and a select-all still
  // reading "3 of 3 selected", all at once.
  //
  // Here and not in clearImportPreviewGrid(): the zero-file branch of
  // previewImport() calls that AFTER capturing the signature, so resetting
  // there would push a completed empty preview back into "no preview run"
  // and re-enable Start over it.
  importPreviewCapturedSignature = null;
  importPreviewedPaths = [];
  updateImportSelectionUI();
  showError('');
  updateFileTypeControls();
  updateWorkspaceMode();
  onImportDestModeChange();
  scheduleImportPreview();
  updateAfterMoveUI();
  updateStartGate();
}

function selectedFileTypes() {
  const preset = document.getElementById('fileTypePreset').value;
  if (preset !== 'custom') return preset;
  return Array.from(document.querySelectorAll('.file-ext:checked'))
    .map(el => el.value);
}

function updateFileTypeControls() {
  const preset = document.getElementById('fileTypePreset');
  const custom = document.getElementById('customFileTypes');
  if (!preset || !custom) return;
  custom.style.display = preset.value === 'custom' ? '' : 'none';
}

function updateWorkspaceMode() {
  const newMode = !!document.getElementById('workspaceNew')?.checked;
  const row = document.getElementById('newWorkspaceRow');
  if (row) row.style.display = newMode ? '' : 'none';
  // Flipping between "current workspace" and "new workspace" changes
  // which default (workspace-scoped vs global) the server will resolve
  // after_import against, so the After Import display must resync.
  updateAdvancedImportOptions();
  // ...and the resync can change whether a process chains at all, which
  // gates the "Then move to NAS" row.
  updateAfterMoveUI();
  updateStartGate();
}

function importFormProblem() {
  if (!sources.length && newImagesSnapshotId === null) {
    return {
      message: 'Add at least one source folder.',
      target: document.getElementById('sourceCard'),
    };
  }
  const allowMissingExiftool =
    !!document.getElementById('chkAllowMissingExiftool')?.checked;
  if (
    importExiftoolRequired && importExiftoolReady === false
    && !allowMissingExiftool
  ) {
    return {
      message: 'Repair ExifTool before importing, or use Advanced → Import without metadata.',
      target: document.getElementById('exiftoolCard'),
    };
  }
  if (document.getElementById('workspaceNew')?.checked) {
    const name =
      (document.getElementById('newWorkspaceName')?.value || '').trim();
    if (!name) {
      return {
        message: 'Enter a name for the new workspace.',
        target: document.getElementById('newWorkspaceName'),
      };
    }
  }
  const copyMode =
    newImagesSnapshotId === null && selectedImportMode() === 'copy';
  if (!copyMode) return null;
  const fileTypes = selectedFileTypes();
  if (Array.isArray(fileTypes) && !fileTypes.length) {
    return {
      message: 'Choose at least one file extension.',
      target: document.getElementById('fileTypePreset'),
    };
  }
  const remoteId = importRemoteTargetId();
  if (remoteId) {
    const target = importRemoteTarget();
    const subResult = normalizeRemoteSubpath(
      document.getElementById('remoteSubpath').value);
    if (!target) {
      return {
        message: 'Choose a configured NAS target.',
        target: document.getElementById('destMode'),
      };
    }
    if (!importRsyncAvailable) {
      return {
        message: 'Direct NAS transfer needs GNU rsync. ' + rsyncInstallHint(),
        target: document.getElementById('destMode'),
      };
    }
    if (!target.mount_path || !importIsAbsolutePath(target.mount_path)) {
      return {
        message: 'This NAS target needs an absolute mounted path in Settings before Vireo can catalog imported files.',
        target: document.getElementById('destMode'),
      };
    }
    if (subResult.error || !subResult.value) {
      return {
        message: subResult.error
          ? subResult.error + ' Enter a folder such as Raw Files/USA.'
          : 'Enter the folder inside ' + target.remote_path +
            ', for example Raw Files/USA.',
        target: document.getElementById('remoteSubpath'),
      };
    }
  } else {
    const destination =
      (document.getElementById('destInput').value || '').trim();
    if (!destination) {
      return {
        message: 'Choose a destination folder.',
        target: document.getElementById('destInput'),
      };
    }
  }
  return null;
}

function showError(msg, target) {
  const el = document.getElementById('importError');
  el.textContent = msg;
  el.style.display = msg ? 'block' : 'none';
  if (msg && target) {
    const focusTarget = target.focus ? target : null;
    target.scrollIntoView({ behavior: preferredScrollBehavior(), block: 'center' });
    if (focusTarget) focusTarget.focus({ preventScroll: true });
  }
}

function formatBytes(n) {
  n = Number(n || 0);
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i += 1; }
  return (i === 0 ? String(n) : n.toFixed(n >= 10 ? 1 : 2)) + ' ' + units[i];
}

// SINGLE OWNER of #btnStart.disabled and its label. Every other site that
// touches the button must call this instead of assigning directly, or the
// two will race — finishJob() re-enabling the button after an import would
// otherwise undo a gate that wants it disabled.
//
// Preview state is FOUR-valued and the values must not collapse:
//   no preview run ....... enabled; the import sends no include_paths and
//                          copies everything, which is what the screen says.
//   preview current ...... enabled.
//   preview stale ........ disabled; the controls moved on and the grid no
//                          longer describes what would be copied.
//   preview in flight .... disabled; the grid is cleared before the disk
//                          walk starts, and on a 5,000-file card that walk
//                          is long enough to click through.
function updateStartGate() {
  const btn = document.getElementById('btnStart');
  if (!btn) return;
  // Same predicate as the boxes themselves — see importSelectionEnabled().
  // The label counts the selection, so it must not outlive the controls
  // that produce one: "Start import (3 files)" in in-place mode would be a
  // promise the request never makes, since the scan catalogues whatever is
  // on disk when it runs rather than the three cards on screen.
  const copyMode = importSelectionEnabled();
  let reason = null;
  if (activeJobId !== null || importStartPending) reason = 'Importing…';
  else if (copyMode && importPreviewInFlight) reason = 'Previewing…';
  else if (copyMode && importPreviewCapturedSignature !== null
           && importPreviewSignatureChanged(importPreviewCapturedSignature)) {
    reason = 'Preview again before importing';
  } else if (copyMode && importDupStreamPending) reason = 'Checking duplicates…';
  else if (copyMode && importPreviewFailed) {
    // The failed run already cleared the grid, so the screen shows nothing
    // while the signature still matches — "not stale, nothing eligible"
    // would read as safe and re-enable Start over an empty preview.
    reason = 'Preview again before importing';
  } else if (copyMode && importPreviewCapturedSignature !== null
             && importPreviewedPaths.length === 0) {
    // A completed preview that found nothing is NOT "no preview run". Leaving
    // Start live here would import whatever lands on the card later, unseen.
    reason = 'No files to import';
  } else if (copyMode && importEligibleCount() > 0
             && importCheckedCount() === 0) {
    // The `importEligibleCount() > 0` qualifier is load-bearing: an
    // already-archived card renders zero checked through no choice of the
    // user's, and blocking Start there would remove the only way to get the
    // safe-to-format verdict on it.
    reason = 'No files selected';
  }
  // Form validity, from #1387's importFormProblem(). That PR landed its own
  // single owner of this button (updateStartAvailability()) on main while
  // this branch was landing updateStartGate() here; both assigned
  // btn.disabled, so keeping both would have made the button read whichever
  // ran last. They answer different questions and both have to be able to
  // hold it shut — "is the FORM complete" (no destination, no file types,
  // unrepaired ExifTool, an unusable NAS target) and "does the PREVIEW still
  // describe what would be copied". So this function owns both, and
  // updateStartAvailability() is gone.
  //
  // The two report through different channels on purpose: a lifecycle
  // `reason` replaces the LABEL, because it describes something the page is
  // doing and the user only has to wait; a form `problem` names a field the
  // user must go fix, and belongs in the tooltip next to the inline error
  // #1387 already renders beside that field. A form problem must not
  // overwrite "Importing…".
  const problem = importFormProblem();
  btn.disabled = reason !== null || newImagesStartBlocked || !!problem;
  btn.title = problem ? problem.message : '';
  btn.setAttribute('aria-disabled', btn.disabled ? 'true' : 'false');
  const checked = importCheckedCount();
  btn.textContent = reason
    || (copyMode && importPreviewCapturedSignature !== null
        ? 'Start import (' + checked.toLocaleString() + ' file'
          + (checked === 1 ? '' : 's') + ')'
        : 'Start import');
}

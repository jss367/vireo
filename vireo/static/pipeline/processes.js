// Saved processes: loading, applying, editing, and the create/rename/delete dialog.
// Classic page script; load boot.js after all definitions.

// ---- Saved processes (the process page IS the editor) --------------------
// A saved process is a named snapshot of the stage toggles below. The picker
// loads/saves them; Run always uses the CURRENT toggle values (a one-off
// tweak never mutates a saved process until you Save).
var _processesById = {};        // id -> process dict (from /api/processes)
var _selectedProcessId = null;  // currently-loaded process, or null (Custom)
var _processDirty = false;      // toggles diverge from the loaded process
var _applyingProcess = false;   // guard: programmatic toggle changes
var _processDialogMode = null;  // "create", "rename", or "delete"
var _processDialogTargetId = null;
var _processDialogBusy = false;
var _processDialogRestoreFocus = null;
var _processDialogEscHandler = null;

async function loadSavedProcesses(selectId) {
  var sel = document.getElementById('strategySelect');
  if (!sel) return;
  try {
    var resp = await fetch('/api/processes');
    if (!resp.ok) return;
    var procs = await resp.json();
    _processesById = {};
    sel.innerHTML = '<option value="__custom__">Custom (unsaved)</option>';
    (procs || []).forEach(function(p) {
      _processesById[p.id] = p;
      var opt = document.createElement('option');
      opt.value = String(p.id);
      opt.textContent = p.name;
      sel.appendChild(opt);
    });
    if (selectId != null && _processesById[selectId]) {
      sel.value = String(selectId);
      onProcessSelect();
    } else {
      sel.value = '__custom__';
      _selectedProcessId = null;
      _processDirty = false;
      updateDerivedProcessControls();
      updateProcessEditorUI();
    }
  } catch (e) { /* leave with Custom only */ }
}

function currentProcessFields() {
  return {
    skip_classify: !document.getElementById('enableClassify').checked,
    skip_extract_masks: !document.getElementById('enableExtract').checked,
    skip_eye_keypoints: !document.getElementById('enableEyeKeypoints').checked,
    skip_regroup: !document.getElementById('enableGroup').checked,
    miss_enabled: !!document.getElementById('enableMisses').checked,
    review_mode:
      document.getElementById('enableSpeciesReview').checked ? 'species' : null,
  };
}

function applyProcessToToggles(proc) {
  _applyingProcess = true;
  try {
    document.getElementById('enableClassify').checked = !proc.skip_classify;
    document.getElementById('enableExtract').checked = !proc.skip_extract_masks;
    document.getElementById('enableEyeKeypoints').checked = !proc.skip_eye_keypoints;
    document.getElementById('enableGroup').checked = !proc.skip_regroup;
    document.getElementById('enableMisses').checked = !!proc.miss_enabled;
    document.getElementById('enableSpeciesReview').checked =
      (proc.review_mode === 'species');
    // Re-run the dependency handling so enabled/disabled state is consistent.
    ['classify', 'extract', 'eyekeypoints', 'group'].forEach(function(st) {
      try { onStageToggle(st); } catch (e) {}
    });
    if (!proc.skip_eye_keypoints) {
      var ek = document.getElementById('enableEyeKeypoints'); ek.checked = true; ek.disabled = false;
    }
  } finally {
    _applyingProcess = false;
  }
  updateDerivedProcessControls();
  updateStartButton();
  refreshPipelineUI();
  schedulePlanRefresh();
}

function onProcessSelect() {
  var sel = document.getElementById('strategySelect');
  if (!sel) return;
  if (sel.value === '__custom__') {
    _selectedProcessId = null;
    _processDirty = false;
    updateProcessEditorUI();
    return;
  }
  var proc = _processesById[parseInt(sel.value, 10)];
  if (!proc) {
    _selectedProcessId = null;
    _processDirty = false;
    updateProcessEditorUI();
    return;
  }
  _selectedProcessId = proc.id;
  _processDirty = false;
  applyProcessToToggles(proc);
  updateProcessEditorUI();
}

// Called by the two post-processing toggles (Find misses / Species review).
function onPostOptToggle() {
  updateDerivedProcessControls();
  if (!_applyingProcess) markProcessModified();
  updateStartButton();
  refreshPipelineUI();
  schedulePlanRefresh();
}

function markProcessModified() {
  _processDirty = true;
  updateProcessEditorUI();
}

function updateProcessEditorUI() {
  var sel = document.getElementById('strategySelect');
  var tag = document.getElementById('processModifiedTag');
  var hasSel = _selectedProcessId != null;
  // Keep the select pointing at the selected process even while modified,
  // so Save knows which process to overwrite.
  if (sel) sel.value = hasSel ? String(_selectedProcessId) : '__custom__';
  if (tag) tag.style.display = _processDirty ? 'inline' : 'none';
  var save = document.getElementById('btnProcessSave');
  var rename = document.getElementById('btnProcessRename');
  var del = document.getElementById('btnProcessDelete');
  if (save) save.disabled = !(hasSel && _processDirty);
  if (rename) rename.disabled = !hasSel;
  if (del) del.disabled = !hasSel;
}

// Keep Find misses / Species review honest about when they actually apply,
// mirroring the server-side stage gates.
function updateDerivedProcessControls() {
  var classifyOn = document.getElementById('enableClassify').checked;
  var groupOn = document.getElementById('enableGroup').checked;
  // Find misses only runs when both Classify and Group ran.
  var missCb = document.getElementById('enableMisses');
  var missLbl = document.getElementById('lblEnableMisses');
  var missOk = classifyOn && groupOn;
  if (missCb) {
    missCb.disabled = !missOk;
    if (!missOk) missCb.checked = false;
  }
  if (missLbl) missLbl.style.opacity = missOk ? '1' : '0.5';
  // Species review only applies when Classify is on and Group is off (the
  // Identify-birds shape). Hidden otherwise, and force-off so it can't leak
  // into a run where it means nothing.
  var srLbl = document.getElementById('lblEnableSpeciesReview');
  var srCb = document.getElementById('enableSpeciesReview');
  var srApplies = classifyOn && !groupOn;
  if (srLbl) srLbl.style.display = srApplies ? 'inline-flex' : 'none';
  if (srCb && !srApplies) srCb.checked = false;
}

async function _postProcessJSON(url, method, body) {
  var opts = { method: method };
  if (body !== undefined) {
    opts.headers = { 'Content-Type': 'application/json' };
    opts.body = JSON.stringify(body);
  }
  var resp = await fetch(url, opts);
  if (!resp.ok) {
    var e = await resp.json().catch(function() { return {}; });
    throw new Error(e.error || ('HTTP ' + resp.status));
  }
  return resp.status === 204 ? null : resp.json();
}

async function saveProcess() {
  if (_selectedProcessId == null) return;
  try {
    var updated = await _postProcessJSON(
      '/api/processes/' + _selectedProcessId, 'PUT', currentProcessFields());
    _processesById[updated.id] = updated;
    _processDirty = false;
    updateProcessEditorUI();
    showProcessEditorStatus('Saved.', false);
  } catch (e) {
    showProcessEditorStatus('Save failed: ' + e.message, true);
  }
}

function showProcessEditorStatus(message, isError) {
  var status = document.getElementById('processEditorStatus');
  if (!status) return;
  status.textContent = message || '';
  status.style.color = isError ? 'var(--danger)' : 'var(--accent)';
  status.style.display = message ? 'inline' : 'none';
}

function showProcessDialogError(message) {
  var error = document.getElementById('processEditorError');
  if (!error) return;
  error.textContent = message || '';
  error.style.display = message ? 'block' : 'none';
}

function setProcessDialogBusy(busy) {
  _processDialogBusy = busy;
  var submit = document.getElementById('processEditorSubmitBtn');
  var cancel = document.getElementById('processEditorCancelBtn');
  var input = document.getElementById('processEditorName');
  if (submit) submit.disabled = busy;
  if (cancel) cancel.disabled = busy;
  if (input) input.disabled = busy;
}

function openProcessDialog(mode) {
  if (mode !== 'create' && _selectedProcessId == null) return;
  var cur = _processesById[_selectedProcessId];
  var modal = document.getElementById('processEditorModal');
  var title = document.getElementById('processEditorModalTitle');
  var description = document.getElementById('processEditorModalDescription');
  var nameField = document.getElementById('processEditorNameField');
  var input = document.getElementById('processEditorName');
  var submit = document.getElementById('processEditorSubmitBtn');
  if (!modal || !title || !description || !nameField || !input || !submit) return;

  _processDialogMode = mode;
  _processDialogTargetId = mode === 'create' ? null : _selectedProcessId;
  _processDialogRestoreFocus = document.activeElement;
  showProcessDialogError('');
  showProcessEditorStatus('', false);
  setProcessDialogBusy(false);

  if (mode === 'create') {
    title.textContent = 'Save process as new';
    description.textContent =
      'Save the current processing-stage choices as a reusable process.';
    nameField.style.display = '';
    input.value = '';
    submit.textContent = 'Save process';
    submit.style.background = '';
  } else if (mode === 'rename') {
    title.textContent = 'Rename process';
    description.textContent = 'Choose a new name for this saved process.';
    nameField.style.display = '';
    input.value = cur ? cur.name : '';
    submit.textContent = 'Rename';
    submit.style.background = '';
  } else {
    title.textContent = 'Delete process';
    description.textContent = 'Delete “' + (cur ? cur.name : '') +
      '”? Any workspace using it as the default will fall back to import only.';
    nameField.style.display = 'none';
    input.value = '';
    submit.textContent = 'Delete';
    submit.style.background = 'var(--danger)';
  }

  modal.classList.add('open');
  _processDialogEscHandler = function(e) {
    if (e.key === 'Escape') closeProcessDialog();
  };
  document.addEventListener('keydown', _processDialogEscHandler);
  setTimeout(function() {
    if (mode === 'delete') submit.focus();
    else {
      input.focus();
      input.select();
    }
  }, 0);
}

function closeProcessDialog() {
  if (_processDialogBusy) return;
  var modal = document.getElementById('processEditorModal');
  if (modal) modal.classList.remove('open');
  if (_processDialogEscHandler) {
    document.removeEventListener('keydown', _processDialogEscHandler);
    _processDialogEscHandler = null;
  }
  _processDialogMode = null;
  _processDialogTargetId = null;
  var restore = _processDialogRestoreFocus;
  _processDialogRestoreFocus = null;
  if (restore && typeof restore.focus === 'function') restore.focus();
}

async function submitProcessDialog(event) {
  event.preventDefault();
  if (_processDialogBusy || !_processDialogMode) return;
  var mode = _processDialogMode;
  var input = document.getElementById('processEditorName');
  var name = input ? input.value.trim() : '';
  if (mode !== 'delete' && !name) {
    showProcessDialogError('Enter a process name.');
    if (input) input.focus();
    return;
  }

  setProcessDialogBusy(true);
  try {
    if (mode === 'create') {
      var body = currentProcessFields();
      body.name = name;
      var created = await _postProcessJSON('/api/processes', 'POST', body);
      await loadSavedProcesses(created.id);
      setProcessDialogBusy(false);
      closeProcessDialog();
      showProcessEditorStatus('Saved “' + created.name + '”.', false);
    } else if (mode === 'rename') {
      var renamed = await _postProcessJSON(
        '/api/processes/' + _processDialogTargetId, 'PUT', { name: name });
      await loadSavedProcesses(_processDialogTargetId);
      setProcessDialogBusy(false);
      closeProcessDialog();
      showProcessEditorStatus('Renamed to “' + renamed.name + '”.', false);
    } else {
      var deleted = _processesById[_processDialogTargetId];
      await _postProcessJSON(
        '/api/processes/' + _processDialogTargetId, 'DELETE');
      _selectedProcessId = null;
      _processDirty = false;
      await loadSavedProcesses(null);
      setProcessDialogBusy(false);
      closeProcessDialog();
      showProcessEditorStatus(
        'Deleted “' + (deleted ? deleted.name : 'process') + '”.', false);
    }
  } catch (e) {
    setProcessDialogBusy(false);
    showProcessDialogError(e.message || 'The process could not be saved.');
  }
}

function saveProcessAsNew() {
  openProcessDialog('create');
}

function renameProcess() {
  openProcessDialog('rename');
}

function deleteProcess() {
  openProcessDialog('delete');
}

// Saved processes have no advanced tier, so there's nothing to hide/show.
// Kept for the advancedmodechange/devmodechange listeners it's wired to.
function updateAdvancedPipelineOptions() {
  updateDerivedProcessControls();
}

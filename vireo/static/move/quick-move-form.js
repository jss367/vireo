// Quick Move form: the source folder, the folder name, and the capture-date folder format.
// Classic page script; load boot.js after all definitions.

/* ---------- Quick Move ---------- */
function onQuickFolderChange() {
  var sel = /** @type {HTMLSelectElement} */ (document.getElementById('quickFolderSelect'));
  var info = /** @type {HTMLElement} */ (document.getElementById('quickFolderInfo'));
  var nameInput = /** @type {HTMLInputElement} */ (document.getElementById('quickFolderName'));
  var resetBtn = /** @type {HTMLButtonElement} */ (document.getElementById('quickResetNameBtn'));
  var fid = parseInt(sel.value);
  // Drop the previous folder's example dates immediately — the next preflight
  // brings this folder's real ones.
  applyQuickDateSamples(null);
  if (!fid) {
    info.textContent = '';
    nameInput.value = '';
    nameInput.disabled = true;
    resetBtn.disabled = true;
    updateQuickMoveBtn();
    return;
  }
  var folder = allFolders.find(function(f) { return f.id === fid; });
  if (folder) {
    var count = folder.photo_count || 0;
    info.textContent = folder.path + ' · ' +
      count.toLocaleString() + ' photo' + (count !== 1 ? 's' : '');
    nameInput.value = sourceFolderName(folder);
    nameInput.disabled = false;
    resetBtn.disabled = false;
  }
  updateQuickMoveBtn();
}

/** @param {MoveFolder | null | undefined} folder */
function sourceFolderName(folder) {
  var srcPath = (folder && folder.path) || '';
  return (folder && folder.name) ||
    srcPath.replace(/[\\/]+$/, '').split(/[\\/]/).pop() || '';
}

function resetQuickFolderName() {
  var fid = parseInt(/** @type {HTMLSelectElement} */ (document.getElementById('quickFolderSelect')).value);
  var folder = allFolders.find(function(f) { return f.id === fid; });
  /** @type {HTMLInputElement} */ (document.getElementById('quickFolderName')).value = sourceFolderName(folder);
  updateQuickMoveBtn();
}

function quickUsesDateFolders() {
  return /** @type {HTMLSelectElement} */ (document.getElementById('quickFolderMode')).value === 'date';
}

function onQuickFolderModeChange() {
  var byDate = quickUsesDateFolders();
  /** @type {HTMLElement} */ (document.getElementById('quickSingleFolderControls')).style.display = byDate ? 'none' : '';
  /** @type {HTMLElement} */ (document.getElementById('quickDateFolderControls')).style.display = byDate ? '' : 'none';
  updateQuickMoveBtn();
}

function selectedQuickFolderTemplate() {
  var preset = /** @type {HTMLSelectElement} */ (document.getElementById('quickFolderTemplatePreset'));
  if (preset.value !== '__custom__') return preset.value;
  return (/** @type {HTMLInputElement} */ (document.getElementById('quickFolderTemplate')).value || '').trim();
}

// Label each date-format preset with a folder this move would really create.
// The examples ride along on the date preflight response, rendered server-side
// by the same build_destination_path() that produced the resulting-folders
// list — one scan, one source of truth, so the label a user reads as "my
// folder will be called this" cannot disagree with the folder list beside it.
//
// Passing null resets the labels to bare patterns — the deliberate state
// before this folder's real dates are known. An example that is stale or
// invented is worse than no example: this control shipped a frozen sample
// date for months that contradicted the panel right below it.
//
// The examples depend on the selected source folder and nothing else, so the
// only thing that invalidates them is changing that folder.
/** @param {MoveDateSamples | null | undefined} templateSamples */
function applyQuickDateSamples(templateSamples) {
  var preset = /** @type {HTMLSelectElement} */ (document.getElementById('quickFolderTemplatePreset'));
  var hint = /** @type {HTMLElement} */ (document.getElementById('quickDateSampleHint'));
  var samples = (templateSamples && templateSamples.samples) || {};
  Array.from(preset.options).forEach(function(option) {
    if (option.value === '__custom__') return;
    var sample = samples[option.value];
    option.textContent = sample ? option.value + ' — ' + sample : option.value;
  });
  if (!hint) return;
  if (!templateSamples) {
    hint.textContent = '';
  } else if (!templateSamples.dated_count) {
    hint.textContent = 'No photo in this folder has a capture date, so every '
      + 'example reads unsorted.';
  } else {
    hint.textContent = 'Examples use the earliest capture date in this folder.';
  }
}

function updateQuickFolderTemplateControls() {
  var preset = /** @type {HTMLSelectElement} */ (document.getElementById('quickFolderTemplatePreset'));
  var custom = /** @type {HTMLInputElement} */ (document.getElementById('quickFolderTemplate'));
  custom.style.display = preset.value === '__custom__' ? '' : 'none';
  if (preset.value === '__custom__') custom.focus();
  updateQuickMoveBtn();
}

function quickFolderTemplateResult() {
  if (!quickUsesDateFolders()) return {value: '', error: ''};
  var value = selectedQuickFolderTemplate();
  if (!value) return {value: '', error: 'Enter a date folder format.'};
  var normalized = value.replace(/\\/g, '/');
  if (normalized.charAt(0) === '/' || /^[A-Za-z]:/.test(normalized) ||
      normalized.split('/').some(function(part) { return part === '..'; })) {
    return {value: '', error: 'Use a relative folder format without “..”.'};
  }
  return {value: value, error: ''};
}

function quickFolderNameResult() {
  if (quickUsesDateFolders()) return {value: '', error: '', rename: false};
  var raw = /** @type {HTMLInputElement} */ (document.getElementById('quickFolderName')).value;
  // "Unchanged from the pre-populated default" is a distinct state from
  // "the user typed something that happens to trim to the source name":
  // if we .trim() a source folder whose real name has leading or trailing
  // whitespace on a no-op submit, we'd rename it to the trimmed variant
  // (landing at /archive/Shoot instead of preserving /archive/ Shoot ).
  // Signal "keep original" with an empty value + rename:false so callers
  // send no destination_name and the backend uses folder.name verbatim.
  var fid = parseInt(/** @type {HTMLSelectElement} */ (document.getElementById('quickFolderSelect')).value);
  var folder = fid ? allFolders.find(function(f) { return f.id === fid; }) : null;
  if (folder && raw === sourceFolderName(folder)) {
    return {value: '', error: '', rename: false};
  }
  var name = raw.trim();
  if (!name) return {value: '', error: 'Enter a name for the folder.', rename: false};
  // Colon rejection matches the backend: on Windows, joining a parent with
  // e.g. "C:shoot" collapses to the drive-relative "C:shoot" and escapes
  // the selected parent, so the leaf name has to stay free of : as well.
  if (name === '.' || name === '..' || /[\\/:]/.test(name)) {
    return {
      value: '',
      error: 'Use a single folder name without slashes or colons.',
      rename: false,
    };
  }
  return {value: name, error: '', rename: true};
}

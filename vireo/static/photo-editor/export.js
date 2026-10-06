// The Export dialog: presets, filename preview, preflight, and starting the job.
// Classic page script; load boot.js after all definitions.

var editorExportRequestGeneration = 0;
var editorExportDismissible = true;

function setExportControlsBusy(busy) {
  document.querySelectorAll('#exportOverlay input, #exportOverlay select, #exportOverlay button').forEach(function(control) {
    if (control.id === 'exportSubmitBtn' || control.hasAttribute('data-export-cancel')) return;
    if (busy) {
      control.setAttribute('data-export-was-disabled', control.disabled ? '1' : '0');
      control.disabled = true;
    } else if (control.hasAttribute('data-export-was-disabled')) {
      control.disabled = control.getAttribute('data-export-was-disabled') === '1';
      control.removeAttribute('data-export-was-disabled');
    }
  });
}

function setExportDismissible(dismissible) {
  editorExportDismissible = dismissible;
  document.querySelectorAll('#exportOverlay [data-export-cancel]').forEach(function(control) {
    control.disabled = !dismissible;
  });
}

function createExportFolderBrowser() {
  return new VireoFolderBrowser({
    overlayId: 'folderBrowser',
    defaultMode: 'destination',
    modes: {
      destination: {
        title: 'Select Export Folder',
        multiple: false,
        showCounts: false,
        startPath: function() {
          return document.getElementById('exportDest').value.trim();
        },
        onSelect: function(path) {
          document.getElementById('exportDest').value = path;
          VireoExportPresets.markCustom();
          VireoExportCollisions.schedule();
        },
      },
    },
  });
}

async function openExportModal() {
  if (!editorState.photoId || !editorState.photo || editorState.loading) return;
  var photoId = editorState.photoId;
  var exportBtn = document.getElementById('exportBtn');
  exportBtn.disabled = true;

  // The export worker renders the persisted recipe. Save first so clicking
  // Export always produces the edit currently visible on the canvas.
  if (isEditorDirty()) {
    var saved = await saveRecipe('Saved before export');
    if (!saved || editorState.photoId !== photoId) {
      exportBtn.disabled = !editorState.photoId || editorState.loading;
      return;
    }
  }

  exportBtn.disabled = false;
  document.getElementById('exportPreset').value = 'original-jpg';
  applyExportPreset('original-jpg');
  document.getElementById('exportTemplate').value = '{original}';
  document.querySelectorAll('#exportOverlay .export-metadata-option input').forEach(function(input) {
    input.checked = false;
  });
  document.getElementById('exportDest').value = '';
  document.getElementById('exportSubfolder').checked = false;
  document.getElementById('exportSubfolderName').value = 'exported';
  document.getElementById('exportRevealAfter').checked = false;
  editorExportRequestGeneration++;
  VireoExportCollisions.reset();
  setExportControlsBusy(false);
  setExportDismissible(true);
  document.getElementById('exportSubmitBtn').disabled = false;
  document.getElementById('exportSubmitBtn').textContent = 'Export Photo';
  document.getElementById('exportOverlay').classList.add('open');
  // Re-applies the last-used preset (saved or built-in) over the defaults
  // set above; async but near-instant locally.
  VireoExportPresets.modalOpened();
  updateExportPreview();
}

function closeExportModal() {
  if (!editorExportDismissible) return;
  editorExportRequestGeneration++;
  VireoExportCollisions.reset();
  setExportControlsBusy(false);
  document.getElementById('exportOverlay').classList.remove('open');
}

async function browseForExportDestination() {
  var destination = document.getElementById('exportDest');
  if (typeof pickDirectory === 'function') {
    var seqBeforePicker = exportFolderBrowser.sequence;
    var result = await pickDirectory('Select export folder', {
      defaultPath: destination.value.trim() || undefined,
    });
    if (result) {
      destination.value = Array.isArray(result) ? result[0] : result;
      VireoExportPresets.markCustom();
      VireoExportCollisions.schedule();
      return;
    }
    if (typeof isTauri === 'function' && isTauri()) return;
    if (exportFolderBrowser.sequence !== seqBeforePicker) {
      exportFolderBrowser.open('destination', {skipInitialBrowse: true});
      return;
    }
  }
  exportFolderBrowser.open('destination');
}

function insertTemplateVar(value) {
  var input = document.getElementById('exportTemplate');
  var start = input.selectionStart;
  var end = input.selectionEnd;
  input.value = input.value.substring(0, start) + value + input.value.substring(end);
  input.selectionStart = input.selectionEnd = start + value.length;
  input.focus();
  VireoExportPresets.markCustom();
  updateExportPreview();
}

function exportExtensionForFormat(format) {
  if (format === 'png') return 'png';
  if (format === 'tiff') return 'tiff';
  return 'jpg';
}

function updateExportFormatControls() {
  var isJpeg = document.getElementById('exportFormat').value === 'jpg';
  document.getElementById('exportQualityLabel').style.display = isJpeg ? 'block' : 'none';
  document.getElementById('exportQualityRow').style.display = isJpeg ? 'flex' : 'none';
}

function applyExportPreset(preset) {
  var resize = '';
  var format = 'jpg';
  var quality = 92;
  if (preset === 'web-jpg') {
    resize = '2048';
    quality = 85;
  } else if (preset === 'small-jpg') {
    resize = '1080';
    quality = 82;
  } else if (preset === 'archive-tiff') {
    format = 'tiff';
  } else if (preset === 'png') {
    format = 'png';
  } else if (preset === 'custom') {
    return;
  }
  document.getElementById('exportFormat').value = format;
  document.getElementById('exportResize').value = resize;
  document.getElementById('exportResizeCustom').value = '';
  document.getElementById('exportResizeCustom').style.display = 'none';
  document.getElementById('exportQuality').value = quality;
  document.getElementById('exportQualityVal').textContent = String(quality);
  updateExportFormatControls();
  updateExportPreview();
}

function markExportCustom() {
  document.getElementById('exportPreset').value = 'custom';
}

function sanitizeExportFilenamePart(value) {
  return String(value || '').replace(/[<>:"/|?*\\]/g, '_');
}

function updateExportPreview() {
  // Programmatic control changes (presets, inserted template variables)
  // repaint through here without firing input events.
  VireoExportCollisions.schedule();
  var template = document.getElementById('exportTemplate').value;
  var photo = editorState.photo;
  if (!template || !photo) {
    document.getElementById('exportPreview').textContent = '';
    return;
  }
  var stem = (photo.filename || ('photo-' + editorState.photoId)).replace(/\.[^.]+$/, '');
  var timestamp = photo.timestamp || '';
  var datePart = timestamp ? timestamp.substring(0, 10) : 'unknown-date';
  var timePart = timestamp.length >= 19 ? timestamp.substring(11, 19).replace(/:/g, '') : '000000';
  var species = Array.isArray(photo.species) && photo.species.length ? photo.species[0] : 'unknown';
  var replacements = {
    original: stem,
    date: datePart,
    datetime: datePart + '_' + timePart,
    species: sanitizeExportFilenamePart(species),
    rating: String(photo.rating || 0),
    seq: '0001',
    folder: sanitizeExportFilenamePart(photo.folder_name),
  };
  var preview = template;
  Object.keys(replacements).forEach(function(key) {
    preview = preview.split('{' + key + '}').join(replacements[key]);
  });
  var extension = exportExtensionForFormat(document.getElementById('exportFormat').value);
  var subPrefix = document.getElementById('exportSubfolder').checked
    ? VireoExportPresets.subfolderName() + '/'
    : '';
  document.getElementById('exportPreview').textContent =
    'Preview: ' + subPrefix + preview + '.' + extension;
}

function selectedExportMetadataFields() {
  return Array.from(document.querySelectorAll('#exportOverlay .export-metadata-option input:checked'))
    .map(function(input) { return input.value; });
}

function validCustomExportSize(value) {
  var size = Number(value);
  return Number.isInteger(size) && size >= 100 && size <= 20000;
}

// The settings that decide where the file lands and what it is called:
// everything the filename-collision preflight needs. Null while the custom
// size is out of range, which startExport reports.
function buildExportPreflightRequest() {
  if (!editorState.photoId) return null;
  var resizeValue = document.getElementById('exportResize').value;
  var maxSize = resizeValue ? parseInt(resizeValue, 10) : null;
  if (resizeValue === 'custom') {
    var customValue = document.getElementById('exportResizeCustom').value;
    if (!validCustomExportSize(customValue)) return null;
    maxSize = Number(customValue);
  }
  return {
    photo_ids: [editorState.photoId],
    destination: document.getElementById('exportDest').value.trim(),
    export_to_subfolder: document.getElementById('exportSubfolder').checked,
    subfolder_name: VireoExportPresets.subfolderName(),
    naming_template: document.getElementById('exportTemplate').value || '{original}',
    max_size: maxSize,
    format: document.getElementById('exportFormat').value || 'jpg',
  };
}

async function startExport() {
  if (!editorState.photoId) return;
  var requestGeneration = ++editorExportRequestGeneration;
  var button = document.getElementById('exportSubmitBtn');
  var buttonLabel = 'Export Photo';
  if (document.getElementById('exportResize').value === 'custom') {
    var customResizeInput = document.getElementById('exportResizeCustom');
    if (!validCustomExportSize(customResizeInput.value)) {
      if (typeof showToast === 'function') {
        showToast('Enter a custom size from 100 to 20000 pixels.', 'error');
      } else {
        setStatus('Enter a custom size from 100 to 20000 pixels.', true);
      }
      customResizeInput.focus();
      return;
    }
  }
  var exportRequest = buildExportPreflightRequest();
  exportRequest.quality = parseInt(document.getElementById('exportQuality').value, 10) || 92;
  exportRequest.metadata_fields = selectedExportMetadataFields();
  exportRequest.reveal_after_export = document.getElementById('exportRevealAfter').checked;
  button.disabled = true;
  button.textContent = 'Checking filename…';
  setExportControlsBusy(true);
  VireoExportCollisions.cancelPending();
  var preflight;
  try {
    preflight = await safeFetch('/api/jobs/export/preflight', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(exportRequest),
    }, {toast: false});
  } catch (error) {
    if (requestGeneration !== editorExportRequestGeneration) return;
    if (typeof showToast === 'function') showToast('Export check failed: ' + error.message, 'error');
    else setStatus('Export check failed: ' + error.message, true);
    setExportControlsBusy(false);
    button.disabled = false;
    button.textContent = buttonLabel;
    return;
  }
  if (requestGeneration !== editorExportRequestGeneration ||
      !document.getElementById('exportOverlay').classList.contains('open')) return;
  if (preflight.error) {
    if (typeof showToast === 'function') showToast('Export check failed: ' + preflight.error, 'error');
    else setStatus('Export check failed: ' + preflight.error, true);
    setExportControlsBusy(false);
    button.disabled = false;
    button.textContent = buttonLabel;
    return;
  }
  // A numbered name the notice did not already show stops the export here
  // so the user reads it first; clicking Export again accepts it.
  if (!VireoExportCollisions.acknowledged(preflight)) {
    setExportControlsBusy(false);
    button.disabled = false;
    button.textContent = buttonLabel;
    return;
  }

  try {
    button.textContent = 'Starting export…';
    setExportDismissible(false);
    var data = await safeFetch('/api/jobs/export', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(exportRequest),
    });
    if (requestGeneration !== editorExportRequestGeneration ||
        !document.getElementById('exportOverlay').classList.contains('open')) return;
    if (data.error) throw new Error(data.error);
    setExportDismissible(true);
    closeExportModal();
    if (typeof showToast === 'function') showToast('Export started', 'info');
    VireoExportJob.watch(data.job_id);
  } catch (error) {
    if (requestGeneration !== editorExportRequestGeneration) return;
    setExportDismissible(true);
    if (typeof showToast === 'function') showToast('Export failed: ' + error.message, 'error');
    else setStatus('Export failed: ' + error.message, true);
    setExportControlsBusy(false);
    button.disabled = false;
    button.textContent = buttonLabel;
  }
}

function bindExportControls() {
  document.getElementById('exportTemplate').addEventListener('input', updateExportPreview);
  // The preset dropdown's change handling (built-in and saved presets) lives
  // in the shared vireo-export-presets.js module.
  document.getElementById('exportFormat').addEventListener('change', function() {
    markExportCustom();
    updateExportFormatControls();
    updateExportPreview();
  });
  document.getElementById('exportResize').addEventListener('change', function() {
    markExportCustom();
    document.getElementById('exportResizeCustom').style.display = this.value === 'custom' ? 'block' : 'none';
    updateExportPreview();
  });
  document.getElementById('exportQuality').addEventListener('input', function() {
    markExportCustom();
    document.getElementById('exportQualityVal').textContent = this.value;
  });
  document.getElementById('exportOverlay').addEventListener('click', function(event) {
    if (event.target === this) closeExportModal();
  });
}

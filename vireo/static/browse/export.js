/* Browse: export modal, offline copies, full-resolution preparation.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Export Modal ---------- */
var exportRequestGeneration = 0;
var exportDismissible = true;
var _exportPhotoIds = null;

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
  exportDismissible = dismissible;
  document.querySelectorAll('#exportOverlay [data-export-cancel]').forEach(function(control) {
    control.disabled = !dismissible;
  });
}

function migrateExportCaptureDateTimePreference() {
  var combinedKey = 'vireo.browse.export.metadata.captureDateTime';
  if (VireoViewPreferences.read(combinedKey) !== null) return;

  var legacyDate = VireoViewPreferences.read('vireo.browse.export.metadata.captureDate');
  var legacyTime = VireoViewPreferences.read('vireo.browse.export.metadata.captureTime');
  if (legacyDate === null && legacyTime === null) return;

  VireoViewPreferences.write(
    combinedKey,
    legacyDate === '1' || legacyTime === '1' ? '1' : '0'
  );
}

function openExportModal(photoIds) {
  var activeIds = Array.isArray(photoIds) ? photoIds.slice() : getActiveSelection();
  if (activeIds.length === 0) return;
  exportRequestGeneration++;
  VireoExportCollisions.reset();
  setExportControlsBusy(false);
  setExportDismissible(true);
  // Snapshot the requested photos for the lifetime of the modal. In
  // particular, the lightbox menu exports the displayed photo even when the
  // Browse grid has a different single- or multi-photo selection behind it.
  _exportPhotoIds = activeIds;
  // Drop on-demand preview hydration from the previous open so a rename (or
  // a photo that has since left the workspace) can't be previewed from a
  // stale cache. Requests still in flight are dropped with it: they carry
  // the superseded generation and are ignored on arrival, so this session
  // refetches instead of waiting on (and trusting) their payload. Reset both
  // maps here — before the control resets below, which fire change handlers
  // that repaint the preview — so a request this session starts is still
  // tracked afterwards and cannot be issued twice.
  _exportPreviewPhotos = {};
  _exportPreviewFetches = {};
  document.getElementById('exportSubmitBtn').textContent = 'Export ' + activeIds.length + ' photo' + (activeIds.length === 1 ? '' : 's');
  document.getElementById('exportSubmitBtn').disabled = false;
  document.getElementById('exportPreset').value = 'original-jpg';
  applyExportPreset('original-jpg');
  document.getElementById('exportTemplate').value = '{original}';
  document.querySelectorAll('.export-metadata-option input').forEach(function(input) {
    input.checked = false;
  });
  document.getElementById('exportDest').value = '';
  document.getElementById('exportSubfolder').checked = false;
  document.getElementById('exportSubfolderName').value = 'exported';
  migrateExportCaptureDateTimePreference();
  VireoViewPreferences.restoreAll(document.getElementById('exportOverlay'));
  document.getElementById('exportOverlay').classList.add('open');
  // Re-applies the last-used preset (saved or built-in) over the defaults
  // and view preferences restored above; async but near-instant locally.
  VireoExportPresets.modalOpened();
  updateExportPreview();
}

// The shared lightbox only offers photo export when its host page exposes
// this capability. Other pages use openExportModal for unrelated exports
// (for example Life List data), so the context menu must not key off that
// generic function name.
function openPhotoExportModal(photoIds) {
  if (document.getElementById('lightboxOverlay').classList.contains('active')) {
    closeLightbox();
  }
  openExportModal(photoIds);
}

function selectedExportMetadataFields() {
  return Array.from(document.querySelectorAll('.export-metadata-option input:checked'))
    .reduce(function(fields, input) {
      if (input.value === 'capture_date_time') {
        // A preset saved in Photo Editor may specify just date or just time.
        // VireoExportPresets.applySettings stashes that split here so we
        // honor it instead of quietly promoting the box to "both". Manual
        // edits clear the stash, at which point the combined box means both.
        if (input.dataset.presetFields) {
          input.dataset.presetFields.split(',').forEach(function(field) {
            if (field) fields.push(field);
          });
        } else {
          fields.push('capture_date', 'capture_time');
        }
      } else {
        fields.push(input.value);
      }
      return fields;
    }, []);
}

function closeExportModal() {
  if (!exportDismissible) return;
  exportRequestGeneration++;
  VireoExportCollisions.reset();
  setExportControlsBusy(false);
  document.getElementById('exportOverlay').classList.remove('open');
  _exportPhotoIds = null;
}

const exportFolderBrowser = new VireoFolderBrowser({
  overlayId: 'folderBrowser',
  defaultMode: 'destination',
  modes: {
    panorama: {
      title: 'Choose Panorama Folder',
      multiple: false,
      showCounts: false,
      startPath: function() { return document.getElementById('panoramaDestination').value.trim(); },
      onSelect: function(path) { document.getElementById('panoramaDestination').value = path; },
    },
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

function insertTemplateVar(v) {
  var input = document.getElementById('exportTemplate');
  var start = input.selectionStart;
  var end = input.selectionEnd;
  var val = input.value;
  input.value = val.substring(0, start) + v + val.substring(end);
  input.selectionStart = input.selectionEnd = start + v.length;
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
  var customResize = '';
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
  document.getElementById('exportResizeCustom').value = customResize;
  document.getElementById('exportResizeCustom').style.display = resize === 'custom' ? 'block' : 'none';
  document.getElementById('exportQuality').value = quality;
  document.getElementById('exportQualityVal').textContent = String(quality);
  updateExportFormatControls();
  updateExportPreview();
}

function markExportCustom() {
  document.getElementById('exportPreset').value = 'custom';
}

// Photos fetched on demand for the export preview, keyed by id (null means
// "asked, and the server could not resolve it"). The preview names the first
// file the export will write, so when that photo is not in the grid, an
// expanded stack tray, or the lightbox cache, it has to be resolved rather
// than substituted. /api/photos/<id> returns the same species-rank keyword
// list the export worker names files with, so the preview matches the file
// on disk. Cleared each time the modal opens so a renamed photo cannot show
// a stale name (Codex P2 on PR #1561).
var _exportPreviewPhotos = {};
var _exportPreviewFetches = {};

function hydrateExportPreviewPhoto(photoId) {
  if (photoId == null) return;
  var key = String(photoId);
  if (_exportPreviewFetches[key]) return;
  // Closing the modal does not cancel a fetch already in flight. Without a
  // generation stamp, that response lands after the next open and writes the
  // cache openExportModal just cleared, repainting the reopened modal with
  // the pre-rename name the clearing existed to drop — and because the
  // pending entry also survives in _exportPreviewFetches, the new session
  // would never issue its own request. exportRequestGeneration is the
  // modal's existing request token (bumped on open, close, and export), so
  // stamp against it rather than adding a second counter.
  var generation = exportRequestGeneration;
  var request = safeFetch('/api/photos/' + photoId, {}, { toast: false })
    .then(function(photo) {
      if (generation !== exportRequestGeneration) return;
      _exportPreviewPhotos[key] = (photo && photo.id != null) ? photo : null;
    })
    .catch(function() {
      if (generation !== exportRequestGeneration) return;
      _exportPreviewPhotos[key] = null;
    })
    .then(function() {
      // Only clear the in-flight marker this call installed: a superseded
      // session must not drop the current session's pending request.
      if (_exportPreviewFetches[key] === request) delete _exportPreviewFetches[key];
      if (generation !== exportRequestGeneration) return;
      // Only repaint while the modal is still open; a closed modal has
      // already dropped _exportPhotoIds and would preview a stale id.
      if (document.getElementById('exportOverlay').classList.contains('open')) {
        updateExportPreview();
      }
    });
  _exportPreviewFetches[key] = request;
  return request;
}

function updateExportPreview() {
  // Programmatic control changes (presets, inserted template variables)
  // repaint through here without firing input events.
  VireoExportCollisions.schedule();
  var template = document.getElementById('exportTemplate').value;
  if (!template) { document.getElementById('exportPreview').textContent = ''; return; }
  // Use first selected photo for preview
  var firstId = (_exportPhotoIds || getActiveSelection())[0];
  if (firstId == null) { document.getElementById('exportPreview').textContent = ''; return; }
  var photo = findBrowsePhoto(firstId);
  // Find Similar and other shared-lightbox entry points can display photos
  // outside Browse's currently loaded page. Their metadata remains available
  // through the lightbox cache/list after the viewer closes for this modal.
  if (!photo && typeof _lbPhotoData === 'function') photo = _lbPhotoData(firstId);
  // Select-all-matching with Stacks enabled puts every underlying member id
  // in selectedPhotos, but browseStackMembers only carries trays the user
  // expanded — and the selection spans pages the grid never loaded. So the
  // first id is often absent from findBrowsePhoto and the lightbox cache.
  // Resolve that photo from the server rather than previewing a stand-in:
  // rendering the stack's loaded cover here would assert a filename the
  // export will never write, which is exactly the kind of plausible-looking
  // proxy CORE_PHILOSOPHY forbids (Codex P2 on PR #1561).
  if (!photo && Object.prototype.hasOwnProperty.call(_exportPreviewPhotos, String(firstId))) {
    photo = _exportPreviewPhotos[String(firstId)];
    if (!photo) {
      document.getElementById('exportPreview').textContent =
        'Preview unavailable: could not load photo ' + firstId + '.';
      return;
    }
  }
  if (!photo) {
    hydrateExportPreviewPhoto(firstId);
    document.getElementById('exportPreview').textContent =
      'Preview: loading photo ' + firstId + '\u2026';
    return;
  }
  if (!photo.filename) { document.getElementById('exportPreview').textContent = ''; return; }
  var stem = photo.filename.replace(/\.[^.]+$/, '');
  var ts = photo.timestamp || '';
  var datePart = ts ? ts.substring(0, 10) : 'unknown-date';
  var timePart = ts && ts.length >= 19 ? ts.substring(11, 19).replace(/:/g, '') : '000000';
  var species = (photo.species && photo.species.length > 0) ? photo.species[0] : 'unknown';
  var preview = template
    .replace('{original}', stem)
    .replace('{date}', datePart)
    .replace('{datetime}', datePart + '_' + timePart)
    .replace('{species}', species)
    .replace('{rating}', String(photo.rating || 0))
    .replace('{seq}', '0001')
    .replace('{folder}', '');
  var ext = exportExtensionForFormat(document.getElementById('exportFormat').value);
  var subPrefix = document.getElementById('exportSubfolder').checked
    ? VireoExportPresets.subfolderName() + '/'
    : '';
  document.getElementById('exportPreview').textContent =
    'Preview: ' + subPrefix + preview + '.' + ext;
}

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
  document.getElementById('exportResizeCustom').style.display =
    this.value === 'custom' ? 'block' : 'none';
  updateExportPreview();
});

document.getElementById('exportQuality').addEventListener('input', function() {
  markExportCustom();
  document.getElementById('exportQualityVal').textContent = this.value;
});

// The settings that decide where each file lands and what it is called:
// everything the filename-collision preflight needs.
function buildExportPreflightRequest() {
  var activeIds = (_exportPhotoIds || getActiveSelection()).slice();
  if (activeIds.length === 0) return null;
  var resizeSelect = document.getElementById('exportResize').value;
  var maxSize = null;
  if (resizeSelect === 'custom') {
    maxSize = parseInt(document.getElementById('exportResizeCustom').value) || null;
  } else if (resizeSelect) {
    maxSize = parseInt(resizeSelect);
  }
  return {
    photo_ids: activeIds,
    destination: document.getElementById('exportDest').value.trim(),
    export_to_subfolder: document.getElementById('exportSubfolder').checked,
    subfolder_name: VireoExportPresets.subfolderName(),
    naming_template: document.getElementById('exportTemplate').value || '{original}',
    max_size: maxSize,
    format: document.getElementById('exportFormat').value || 'jpg',
  };
}

async function startExport() {
  var requestGeneration = ++exportRequestGeneration;
  var exportRequest = buildExportPreflightRequest();
  if (!exportRequest) return;
  exportRequest.quality = parseInt(document.getElementById('exportQuality').value) || 92;
  exportRequest.metadata_fields = selectedExportMetadataFields();
  exportRequest.reveal_after_export = document.getElementById('exportRevealAfter').checked;

  var btn = document.getElementById('exportSubmitBtn');
  var count = exportRequest.photo_ids.length;
  var buttonLabel = 'Export ' + count + ' photo' + (count === 1 ? '' : 's');
  btn.disabled = true;
  btn.textContent = 'Checking filenames…';
  setExportControlsBusy(true);
  VireoExportCollisions.cancelPending();

  var preflight;
  try {
    preflight = await safeFetch('/api/jobs/export/preflight', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(exportRequest),
    }, {toast: false});
  } catch (err) {
    if (requestGeneration !== exportRequestGeneration) return;
    alert('Export check failed: ' + err.message);
    setExportControlsBusy(false);
    btn.disabled = false;
    btn.textContent = buttonLabel;
    return;
  }
  if (requestGeneration !== exportRequestGeneration ||
      !document.getElementById('exportOverlay').classList.contains('open')) return;
  if (preflight.error) {
    alert('Export check failed: ' + preflight.error);
    setExportControlsBusy(false);
    btn.disabled = false;
    btn.textContent = buttonLabel;
    return;
  }
  // Numbered names the notice did not already show stop the export here so
  // the user reads them first; clicking Export again accepts them.
  if (!VireoExportCollisions.acknowledged(preflight)) {
    setExportControlsBusy(false);
    btn.disabled = false;
    btn.textContent = buttonLabel;
    return;
  }

  try {
    btn.textContent = 'Starting export…';
    setExportDismissible(false);
    var data = await safeFetch('/api/jobs/export', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(exportRequest),
    });
    if (requestGeneration !== exportRequestGeneration ||
        !document.getElementById('exportOverlay').classList.contains('open')) return;
    if (data.error) {
      setExportDismissible(true);
      alert('Export error: ' + data.error);
      setExportControlsBusy(false);
      btn.disabled = false;
      btn.textContent = buttonLabel;
      return;
    }
    setExportDismissible(true);
    closeExportModal();
    showToast('Export started (' + count + ' photos)', 'info');
  } catch(err) {
    if (requestGeneration !== exportRequestGeneration) return;
    setExportDismissible(true);
    alert('Export failed: ' + err.message);
    setExportControlsBusy(false);
    btn.disabled = false;
    btn.textContent = buttonLabel;
  }
}

async function makeAvailableOffline() {
  var activeIds = getActiveSelection();
  if (activeIds.length === 0) return;
  try {
    var data = await safeFetch('/api/jobs/offline-cache', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: activeIds}),
    });
    if (data.error) {
      showToast('Offline cache error: ' + data.error, 'error');
      return;
    }
    showToast(
      'Offline caching started (' + activeIds.length + ' photo' +
        (activeIds.length === 1 ? '' : 's') + ')',
      'info'
    );
  } catch(err) {
    showToast('Offline cache failed: ' + err.message, 'error');
  }
}

var _prepareFullResolutionJobId = null;

function _setPrepareFullResolutionButton(running, current, total) {
  var btn = document.getElementById('prepareFullResolutionBtn');
  if (!btn) return;
  btn.disabled = !!running;
  if (running && current != null && total) {
    btn.textContent = 'Preparing ' + current + '/' + total;
  } else if (running) {
    btn.textContent = 'Preparing…';
  } else {
    btn.textContent = 'Prepare Full Resolution';
  }
}

async function prepareFullResolutionSelection(photoIds) {
  var activeIds = Array.isArray(photoIds) ? photoIds.slice() : getActiveSelection();
  if (!activeIds.length || _prepareFullResolutionJobId) return;
  _setPrepareFullResolutionButton(true);
  try {
    var data = await safeFetch('/api/jobs/prepare-full-resolution', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: activeIds}),
    });
    _prepareFullResolutionJobId = data.job_id;
    showToast(
      'Preparing ' + activeIds.length.toLocaleString() +
      ' photo' + (activeIds.length === 1 ? '' : 's') +
      ' for full-resolution inspection…',
      'info'
    );
    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(progress) {
        _setPrepareFullResolutionButton(
          true, progress.current || 0, progress.total || activeIds.length
        );
      },
      onComplete: function(done) {
        _prepareFullResolutionJobId = null;
        _setPrepareFullResolutionButton(false);
        var result = done.result || {};
        if (done.status === 'cancelled') {
          showToast('Full-resolution preparation was stopped.', 'error');
          return;
        }
        if (done.status === 'failed' && !done.result) {
          var errors = done.errors || [];
          var failureMessage = errors.length
            ? errors[0]
            : 'The preparation job ended before producing a result.';
          showToast(
            'Full-resolution preparation failed: ' + failureMessage,
            'error'
          );
          return;
        }
        var ready = result.ready || 0;
        var failed = result.failed || 0;
        var copied = result.copied || 0;
        var summary = ready.toLocaleString() + ' ready';
        if (copied) summary += ', ' + copied.toLocaleString() + ' copied locally';
        var skippedDeleted = result.skipped_deleted || 0;
        if (skippedDeleted) {
          summary += ', ' + skippedDeleted.toLocaleString() + ' skipped (deleted during preparation)';
        }
        if (failed) summary += ', ' + failed.toLocaleString() + ' failed';
        showToast(
          'Full-resolution preparation complete: ' + summary,
          failed ? 'error' : 'success'
        );
        try {
          window.dispatchEvent(new CustomEvent('vireo-job-done', {
            detail: {job_id: data.job_id}
          }));
        } catch (_) {}
      },
      onError: function() {
        _prepareFullResolutionJobId = null;
        _setPrepareFullResolutionButton(false);
      },
    });
  } catch(err) {
    _prepareFullResolutionJobId = null;
    _setPrepareFullResolutionButton(false);
    showToast(
      'Could not start full-resolution preparation: ' + err.message,
      'error'
    );
  }
}

// Close on overlay click
document.getElementById('exportOverlay').addEventListener('click', function(e) {
  if (e.target === this) closeExportModal();
});

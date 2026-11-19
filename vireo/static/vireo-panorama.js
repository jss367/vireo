/* Panorama selection is captured when the dialog opens, before asynchronous work. */
/** @type {number[]} */
var panoramaPhotoIds = [];
var panoramaBusy = false;
var panoramaIsHdr = false;

/** @param {number[]} ids @param {boolean} [hdr] */
function openPanoramaModal(ids, hdr = false) {
  if (!panoramaBusy) {
    panoramaIsHdr = hdr;
    document.querySelectorAll('[data-hdr-only], [data-panorama-only]').forEach(function(el) {
      if (el instanceof HTMLElement) el.hidden = hdr !== el.hasAttribute('data-hdr-only');
    });
    var title = hdr ? 'Merge to HDR' : 'Create Panorama';
    /** @type {HTMLElement} */ (document.getElementById('panoramaTitle')).textContent = title;
    /** @type {HTMLElement} */ (document.getElementById('panoramaSubmit')).textContent = title;
    panoramaPhotoIds = ids.slice();
    /** @type {HTMLElement} */ (document.getElementById('panoramaSelection')).textContent = ids.length + ' photos selected';
    /** @type {HTMLElement} */ (document.getElementById('panoramaStatus')).textContent = '';
  }
  /** @type {HTMLElement} */ (document.getElementById('panoramaOverlay')).classList.add('open');
  /** @type {HTMLElement} */ (document.getElementById('panoramaSubmit')).focus();
}

function closePanoramaModal() {
  /** @type {HTMLElement} */ (document.getElementById('panoramaOverlay')).classList.remove('open');
  window._vireoNativeMenuPhotoIdsOverride = null;
}

/** @param {boolean} busy */
function setPanoramaBusy(busy) {
  panoramaBusy = busy;
  document.querySelectorAll('#panoramaOverlay input, #panoramaOverlay select, #panoramaSubmit, #panoramaBrowse').forEach(function(el) {
    if (el instanceof HTMLInputElement || el instanceof HTMLSelectElement || el instanceof HTMLButtonElement) el.disabled = busy;
  });
}

async function startPanorama() {
  if (panoramaBusy) return;
  setPanoramaBusy(true);
  var status = /** @type {HTMLElement} */ (document.getElementById('panoramaStatus'));
  var name = panoramaIsHdr ? 'HDR merge' : 'Panorama';
  status.textContent = 'Starting ' + name + '…';
  try {
    /** @type {{job_id: string}} */
    var data = await safeFetch(panoramaIsHdr ? '/api/jobs/hdr' : '/api/jobs/panorama', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        photo_ids: panoramaPhotoIds.slice(),
        destination: /** @type {HTMLInputElement} */ (document.getElementById('panoramaDestination')).value.trim(),
        format: /** @type {HTMLSelectElement} */ (document.getElementById(panoramaIsHdr ? 'hdrFormat' : 'panoramaFormat')).value,
        input_size: Number(/** @type {HTMLSelectElement} */ (document.getElementById('panoramaInputSize')).value),
        reveal: /** @type {HTMLInputElement} */ (document.getElementById('panoramaReveal')).checked,
      }),
    }, {toast: false});
    status.textContent = 'Loading photos… You can close this dialog and follow progress or cancel in Jobs. Cancellation waits for the current processing stage.';
    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      /** @param {{phase?: string}} progress */
      onProgress: function(progress) {
        status.textContent = (progress.phase || 'Processing photos') + '… Follow progress or cancel in Jobs.';
      },
      /** @param {PanoramaDone} done */
      onComplete: function(done) {
        setPanoramaBusy(false);
        if (!done.result || !done.result.path) {
          status.textContent = name + ' ' + (done.status || 'failed') + ': ' + ((done.errors || [])[0] || 'No file saved.');
          showToast(status.textContent, 'error');
          return;
        }
        var result = done.result;
        status.textContent = 'Saved ' + result.width + ' × ' + result.height + ' ' + name + ': ' + result.path
          + (done.status === 'cancelled' ? ' (Cancellation arrived after the file was saved.)' : '');
        showToast(name + ' saved: ' + result.path, 'success');
      },
      onError: function() {
        // Losing the progress stream does not cancel the server job.
        status.textContent = 'Progress connection lost. Check Jobs for the result before starting another.';
        setPanoramaBusy(false);
      },
    });
  } catch (error) {
    status.textContent = (error instanceof Error ? error.message : '') || 'Could not start ' + name;
    setPanoramaBusy(false);
  }
}

/** @type {HTMLElement} */ (document.getElementById('panoramaOverlay')).addEventListener('click', function(event) {
  if (event.target === this) closePanoramaModal();
});

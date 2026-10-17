/* Panorama selection is captured when the dialog opens, before asynchronous work. */
/** @type {number[]} */
var panoramaPhotoIds = [];
var panoramaBusy = false;

/** @param {number[]} ids */
function openPanoramaModal(ids) {
  if (!panoramaBusy) {
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
  status.textContent = 'Starting panorama…';
  try {
    /** @type {{job_id: string}} */
    var data = await safeFetch('/api/jobs/panorama', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        photo_ids: panoramaPhotoIds.slice(),
        destination: /** @type {HTMLInputElement} */ (document.getElementById('panoramaDestination')).value.trim(),
        format: /** @type {HTMLSelectElement} */ (document.getElementById('panoramaFormat')).value,
        input_size: Number(/** @type {HTMLSelectElement} */ (document.getElementById('panoramaInputSize')).value),
        reveal: /** @type {HTMLInputElement} */ (document.getElementById('panoramaReveal')).checked,
      }),
    }, {toast: false});
    status.textContent = 'Loading photos… You can close this dialog and follow progress or cancel in Jobs. Cancellation waits for the current stitching stage.';
    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      /** @param {{phase?: string}} progress */
      onProgress: function(progress) {
        status.textContent = (progress.phase || 'Creating panorama') + '… Follow progress or cancel in Jobs.';
      },
      /** @param {PanoramaDone} done */
      onComplete: function(done) {
        setPanoramaBusy(false);
        if (!done.result || !done.result.path) {
          status.textContent = 'Panorama ' + (done.status || 'failed') + ': ' + ((done.errors || [])[0] || 'No file saved.');
          showToast(status.textContent, 'error');
          return;
        }
        var result = done.result;
        status.textContent = 'Saved ' + result.width + ' × ' + result.height + ' panorama: ' + result.path
          + (done.status === 'cancelled' ? ' (Cancellation arrived after the file was saved.)' : '');
        showToast('Panorama saved: ' + result.path, 'success');
      },
      onError: function() {
        // Losing the progress stream does not cancel the server job.
        status.textContent = 'Progress connection lost. Check Jobs for the panorama result before starting another.';
        setPanoramaBusy(false);
      },
    });
  } catch (error) {
    status.textContent = (error instanceof Error ? error.message : '') || 'Could not start panorama';
    setPanoramaBusy(false);
  }
}

/** @type {HTMLElement} */ (document.getElementById('panoramaOverlay')).addEventListener('click', function(event) {
  if (event.target === this) closePanoramaModal();
});

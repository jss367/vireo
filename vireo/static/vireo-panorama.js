/* Panorama selection is captured when the dialog opens, before asynchronous work. */
var panoramaPhotoIds = [];
var panoramaBusy = false;

function openPanoramaModal(ids) {
  if (!panoramaBusy) {
    panoramaPhotoIds = ids.slice();
    document.getElementById('panoramaSelection').textContent = ids.length + ' photos selected';
    document.getElementById('panoramaStatus').textContent = '';
  }
  document.getElementById('panoramaOverlay').classList.add('open');
  document.getElementById('panoramaSubmit').focus();
}

function closePanoramaModal() {
  document.getElementById('panoramaOverlay').classList.remove('open');
  window._vireoNativeMenuPhotoIdsOverride = null;
}

function setPanoramaBusy(busy) {
  panoramaBusy = busy;
  document.querySelectorAll('#panoramaOverlay input, #panoramaOverlay select, #panoramaSubmit, #panoramaBrowse').forEach(function(el) {
    el.disabled = busy;
  });
}

async function startPanorama() {
  if (panoramaBusy) return;
  setPanoramaBusy(true);
  var status = document.getElementById('panoramaStatus');
  status.textContent = 'Starting panorama…';
  try {
    var data = await safeFetch('/api/jobs/panorama', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        photo_ids: panoramaPhotoIds.slice(),
        destination: document.getElementById('panoramaDestination').value.trim(),
        format: document.getElementById('panoramaFormat').value,
        input_size: Number(document.getElementById('panoramaInputSize').value),
        reveal: document.getElementById('panoramaReveal').checked,
      }),
    }, {toast: false});
    status.textContent = 'Loading photos… You can close this dialog and follow progress or cancel in Jobs. Cancellation waits for the current stitching stage.';
    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(progress) {
        status.textContent = (progress.phase || 'Creating panorama') + '… Follow progress or cancel in Jobs.';
      },
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
    status.textContent = error.message || 'Could not start panorama';
    setPanoramaBusy(false);
  }
}

document.getElementById('panoramaOverlay').addEventListener('click', function(event) {
  if (event.target === this) closePanoramaModal();
});

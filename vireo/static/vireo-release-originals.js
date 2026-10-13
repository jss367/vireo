/* Shared by Browse and Storage: managed originals are shared across workspaces. */
window.VireoReleaseOriginals = (function() {
  var running = false;

  function setRunning(value, progress) {
    running = value;
    var button = document.getElementById('releaseLocalOriginalsBtn');
    if (!button) return;
    button.disabled = value;
    button.textContent = progress
      ? 'Checking ' + progress.current + '/' + progress.total
      : value ? 'Releasing…' : 'Release local originals…';
  }

  function formatBytes(bytes) {
    var units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var index = 0;
    while (bytes >= 1024 && index < units.length - 1) { bytes /= 1024; index++; }
    return (index ? bytes.toFixed(1) : bytes) + ' ' + units[index];
  }

  async function release(scope, description) {
    if (running) return;
    if (!window.confirm(
      'Release local originals for ' + description + '?\n\n' +
      'Vireo will remove cached originals and their cached sidecars only when the source files are available. ' +
      'Changed source images will be kept for review.\n\n' +
      'Your source photos, edits, ratings, thumbnails, and processing results will stay intact. ' +
      'Local copies are shared across workspaces; full-resolution access will use source storage again.'
    )) return;
    setRunning(true);
    try {
      var data = await safeFetch('/api/jobs/release-local-originals', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(scope),
      });
      showToast('Checking source files and releasing local originals…', 'info');
      safeEventSource('/api/jobs/' + data.job_id + '/stream', {
        onProgress: function(progress) { setRunning(true, progress); },
        onComplete: function(done) {
          setRunning(false);
          var result = done.result;
          if (!result) {
            showToast('Local originals could not be released. See Jobs for details.', 'error');
            return;
          }
          var summary = (result.released || 0).toLocaleString() + ' local originals released, ' +
            formatBytes(result.bytes || 0) + ' reclaimed';
          if (result.kept) summary += ', ' + result.kept.toLocaleString() + ' kept';
          if (result.not_cached) summary += ', ' + result.not_cached.toLocaleString() + ' already released';
          if (result.failed) summary += ', ' + result.failed.toLocaleString() + ' failed';
          var remaining = result.total - result.processed;
          if (remaining > 0) summary += ', ' + remaining.toLocaleString() + ' not checked';
          if (done.status === 'cancelled') summary = 'Release stopped: ' + summary;
          if (result.kept_reasons && result.kept_reasons.length) {
            summary += '. ' + result.kept_reasons[0] + '. See Jobs for details.';
          }
          showToast(summary, done.status === 'failed' ? 'error' : result.kept || done.status !== 'completed' ? 'info' : 'success');
          if (typeof loadStoragePanel === 'function') loadStoragePanel();
          window.dispatchEvent(new CustomEvent('vireo-job-done', {detail: {job_id: data.job_id}}));
        },
        onError: function() {
          setRunning(false);
          showToast('Connection lost while releasing local originals. Check Jobs for the result.', 'error');
        },
      });
    } catch (error) {
      setRunning(false);
      showToast('Could not start release: ' + error.message, 'error');
    }
  }

  function storageButton() {
    return '<button id="releaseLocalOriginalsBtn" class="btn-manage" ' +
      (running ? 'disabled ' : '') +
      'onclick="VireoReleaseOriginals.release({all_cached: true}, \'all cached photos across every workspace\')">Release local originals…</button>';
  }

  return {release: release, storageButton: storageButton, isRunning: function() { return running; }};
})();

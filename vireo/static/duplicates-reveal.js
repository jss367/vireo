// Duplicates page: feedback for revealing a bucket's folders.
async function revealBucketFolders(bi, button) {
  var bucket = (_lastScanResult && _lastScanResult.buckets || [])[bi];
  if (!bucket || !bucket.folders || bucket.folders.length === 0) return;
  if (button && button.disabled) return;
  var originalLabel = button && button.textContent;
  var manager = window.VIREO_FILE_MANAGER_NAME || 'file manager';
  if (button) {
    button.disabled = true;
    button.textContent = 'Opening ' + manager + '\u2026';
  }
  try {
    var data = await safeFetch('/api/folders/reveal', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ paths: bucket.folders }),
    }, {toast: false});
    var revealed = (data.revealed || []).length;
    var problems = (data.failed || []).concat(data.skipped || []);
    var message = 'Revealed ' + revealed + ' folder' + (revealed === 1 ? '' : 's') + ' in ' + manager;
    if (problems.length > 0) {
      message += '. Could not reveal: ' + problems.map(function(problem) {
        return problem.path + ' (' + problem.reason + ')';
      }).join('; ');
      showToast(message, revealed > 0 ? 'warning' : 'error');
    } else if (revealed === 0) {
      showToast('No folders were revealed in ' + manager, 'error');
    } else {
      showToast(message, 'success');
    }
  } catch (e) {
    showToast('Reveal failed: ' + (e.message || 'error'), 'error');
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = originalLabel;
    }
  }
}

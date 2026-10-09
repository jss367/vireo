// Duplicates page: feedback for revealing a bucket's folders.
var _pendingBucketReveals = new Set();

function bucketRevealKey(bi) {
  var bucket = (_lastScanResult && _lastScanResult.buckets || [])[bi];
  return JSON.stringify((bucket && bucket.folders || []).slice().sort());
}

function bucketRevealButton(bi) {
  var pending = _pendingBucketReveals.has(bucketRevealKey(bi));
  var label = pending
    ? 'Opening ' + (window.VIREO_FILE_MANAGER_NAME || 'file manager') + '\u2026'
    : window.VIREO_REVEAL_LABEL;
  return '<button class="keep-btn reveal-btn" data-reveal-bucket="' + bi + '" ' +
    'onclick="revealBucketFolders(' + bi + ', this)" ' +
    (pending ? 'disabled ' : '') +
    'title="Reveal all bucket folders in your OS file manager">' +
    escapeHtml(label) + '</button>';
}

function refreshBucketRevealButtons() {
  document.querySelectorAll('[data-reveal-bucket]').forEach(function(button) {
    var pending = _pendingBucketReveals.has(bucketRevealKey(Number(button.dataset.revealBucket)));
    button.disabled = pending;
    button.textContent = pending
      ? 'Opening ' + (window.VIREO_FILE_MANAGER_NAME || 'file manager') + '\u2026'
      : window.VIREO_REVEAL_LABEL;
  });
}
async function revealBucketFolders(bi, button) {
  var bucket = (_lastScanResult && _lastScanResult.buckets || [])[bi];
  if (!bucket || !bucket.folders || bucket.folders.length === 0) return;
  var key = bucketRevealKey(bi);
  if (_pendingBucketReveals.has(key) || (button && button.disabled)) return;
  _pendingBucketReveals.add(key);
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
    _pendingBucketReveals.delete(key);
    refreshBucketRevealButtons();
    if (button) {
      button.disabled = false;
      button.textContent = originalLabel;
    }
  }
}

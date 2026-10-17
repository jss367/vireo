// Duplicates page: feedback for revealing a bucket's folders.
/** @type {Set<string>} */ var _pendingBucketReveals = new Set();

/** @param {number} bi @param {Element|null} [button] @returns {string[]} */
function bucketRevealFolders(bi, button) {
  var card = button && button.closest('.bucket-card');
  if (card && card.hasAttribute('data-reveal-paths')) {
    var paths = JSON.parse(card.getAttribute('data-reveal-paths') || '[]'); return duplicateStringList(paths) ? paths : [];
  }
  var bucket = visibleBucket((_lastScanResult && _lastScanResult.buckets || [])[bi]);
  return (bucket && bucket.folders || []).slice();
}

/** @param {number} bi @param {Element|null} [button] */
function bucketRevealKey(bi, button) {
  return JSON.stringify(bucketRevealFolders(bi, button).sort());
}

/** @param {number} bi @param {Element|null} [card] @param {string[]} [folders] */
function bucketRevealButton(bi, card, folders) {
  var key = folders ? JSON.stringify(folders.slice().sort()) : bucketRevealKey(bi, card);
  var pending = _pendingBucketReveals.has(key);
  var label = pending
    ? 'Opening ' + (window.VIREO_FILE_MANAGER_NAME || 'file manager') + '\u2026'
    : window.VIREO_REVEAL_LABEL;
  return '<button class="keep-btn reveal-btn" data-reveal-bucket="' + bi + '" ' +
    'onclick="revealBucketFolders(' + bi + ', this)" ' +
    (pending ? 'disabled ' : '') +
    'title="Reveal all bucket folders in your OS file manager">' +
    escapeHtml(label) + '</button>';
}

/** @param {string} [completedKey] */
function refreshBucketRevealButtons(completedKey) {
  document.querySelectorAll(/** @type {'button'} */ ('[data-reveal-bucket]')).forEach(function(button) {
    var key = bucketRevealKey(Number(button.dataset.revealBucket), button);
    if (completedKey !== undefined && key !== completedKey) return;
    var pending = _pendingBucketReveals.has(key);
    var card = button.closest('.bucket-card');
    button.disabled = pending || !!(card && card.classList.contains('applying'));
    button.textContent = pending
      ? 'Opening ' + (window.VIREO_FILE_MANAGER_NAME || 'file manager') + '\u2026'
      : window.VIREO_REVEAL_LABEL;
  });
}
/** @param {number} bi @param {HTMLButtonElement|null} [button] */
async function revealBucketFolders(bi, button) {
  var folders = bucketRevealFolders(bi, button);
  if (!folders.length) return;
  var key = bucketRevealKey(bi, button);
  if (_pendingBucketReveals.has(key) || (button && button.disabled)) return;
  _pendingBucketReveals.add(key);
  var manager = window.VIREO_FILE_MANAGER_NAME || 'file manager';
  if (button) {
    button.disabled = true;
    button.textContent = 'Opening ' + manager + '\u2026';
  }
  try {
    /** @type {{revealed?:string[];failed?:Array<{path:string;reason:string}>;skipped?:Array<{path:string;reason:string}>}} */ var data = await safeFetch('/api/folders/reveal', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ paths: folders }),
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
  } catch (caught) { const e = duplicateRequestError(caught);
    showToast('Reveal failed: ' + (e.message || 'error'), 'error');
  } finally {
    _pendingBucketReveals.delete(key);
    refreshBucketRevealButtons(key);
  }
}

// Duplicates page: feedback for revealing a bucket's folders.

import { state } from './state.js';
import { duplicateRequestError, duplicateStringList } from './cleanup.js';
import { visibleBucket } from './filter.js';
import { escapeHtml, fileManagerName, revealLabel, safeFetch, showToast } from './shared.js';

/** @param {number} bi @param {Element|null} [button] @returns {string[]} */
export function bucketRevealFolders(bi, button) {
  var card = button && button.closest('.bucket-card');
  if (card && card.hasAttribute('data-reveal-paths')) {
    var paths = JSON.parse(card.getAttribute('data-reveal-paths') || '[]'); return duplicateStringList(paths) ? paths : [];
  }
  var bucket = visibleBucket((state.lastScanResult && state.lastScanResult.buckets || [])[bi]);
  return (bucket && bucket.folders || []).slice();
}

/** @param {number} bi @param {Element|null} [button] */
export function bucketRevealKey(bi, button) {
  return JSON.stringify(bucketRevealFolders(bi, button).sort());
}

/** @param {number} bi @param {Element|null} [card] @param {string[]} [folders] */
export function bucketRevealButton(bi, card, folders) {
  var key = folders ? JSON.stringify(folders.slice().sort()) : bucketRevealKey(bi, card);
  var pending = state.pendingBucketReveals.has(key);
  var label = pending
    ? 'Opening ' + fileManagerName() + '\u2026'
    : revealLabel();
  return '<button class="keep-btn reveal-btn" data-reveal-bucket="' + bi + '" ' +
    (pending ? 'disabled ' : '') +
    'title="Reveal all bucket folders in your OS file manager">' +
    escapeHtml(label) + '</button>';
}

/** @param {string} [completedKey] */
export function refreshBucketRevealButtons(completedKey) {
  document.querySelectorAll(/** @type {'button'} */ ('[data-reveal-bucket]')).forEach(function(button) {
    var key = bucketRevealKey(Number(button.dataset.revealBucket), button);
    if (completedKey !== undefined && key !== completedKey) return;
    var pending = state.pendingBucketReveals.has(key);
    var card = button.closest('.bucket-card');
    button.disabled = pending || !!(card && card.classList.contains('applying'));
    button.textContent = pending
      ? 'Opening ' + fileManagerName() + '\u2026'
      : revealLabel();
  });
}
/** @param {number} bi @param {HTMLButtonElement|null} [button] */
export async function revealBucketFolders(bi, button) {
  var folders = bucketRevealFolders(bi, button);
  if (!folders.length) return;
  var key = bucketRevealKey(bi, button);
  if (state.pendingBucketReveals.has(key) || (button && button.disabled)) return;
  state.pendingBucketReveals.add(key);
  var manager = fileManagerName();
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
    state.pendingBucketReveals.delete(key);
    refreshBucketRevealButtons(key);
  }
}

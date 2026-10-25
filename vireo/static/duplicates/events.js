// Duplicates page: the delegated listeners for markup the page renders. The
// cards, buckets and section headers carry data attributes instead of inline
// handlers, so one listener on the document covers fresh scans, restored
// results, filter re-renders and bucket cards pinned above the results while
// their Keep-for-all runs.

import { state } from './state.js';
import { revealDuplicatePhoto } from './card.js';
import { keepDuplicateCopy, retryBucketTrash, trashAllLoserFiles, trashOneLoserFile } from './cleanup.js';
import { selectDuplicateCategory, updateApplyCount } from './patterns.js';
import { toggleResolvedSection } from './resolved.js';
import { bulkResolveByFolderIdx } from './results.js';
import { revealBucketFolders } from './reveal.js';
import { renderSameShotResults } from './same-shot.js';

/** The photo id of the card a control sits in, or null. @param {Element} control */
function cardPhotoId(control) {
  var card = control.closest(/** @type {'div'} */ ('.dup-card[data-photo-id]'));
  var id = card ? Number(card.dataset.photoId) : NaN;
  return Number.isInteger(id) && id > 0 ? id : null;
}

/** @type {Record<string, (control: HTMLElement) => unknown>} */
var ACTIONS = {
  'keep-copy': function(control) {
    var id = cardPhotoId(control);
    if (id != null) keepDuplicateCopy(id);
  },
  'trash-copy': function(control) {
    var id = cardPhotoId(control);
    if (id != null) trashOneLoserFile(id);
  },
  'toggle-resolved': function() { toggleResolvedSection(); },
  'trash-all': function() { trashAllLoserFiles(); },
};

/** @param {MouseEvent} event */
function onDuplicatesClick(event) {
  if (!(event.target instanceof Element)) return;
  var target = event.target;
  var action = target.closest(/** @type {'button'} */ ('[data-dup-action]'));
  if (action) {
    var handler = ACTIONS[action.dataset.dupAction || ''];
    if (!handler) throw new Error('Unknown Duplicates action: ' + action.dataset.dupAction);
    handler(action);
    return;
  }
  var keepFolder = target.closest(/** @type {'button'} */ ('button[data-keep-folder]'));
  if (keepFolder) {
    var bucket = keepFolder.closest(/** @type {'div'} */ ('.bucket-card[data-bi]'));
    if (bucket) bulkResolveByFolderIdx(Number(bucket.dataset.bi), Number(keepFolder.dataset.keepFolder));
    return;
  }
  var reveal = target.closest(/** @type {'button'} */ ('button[data-reveal-bucket]'));
  if (reveal) {
    revealBucketFolders(Number(reveal.dataset.revealBucket), reveal);
    return;
  }
  var retry = target.closest(/** @type {'button'} */ ('button[data-retry-trash]'));
  if (retry) {
    retryBucketTrash(retry);
    return;
  }
  var category = target.closest(/** @type {'button'} */ ('button[data-duplicate-category], button[data-show-category]'));
  if (category) {
    selectDuplicateCategory(category.dataset.duplicateCategory || category.dataset.showCategory || 'all');
    return;
  }
  if (target.closest('[data-same-shot-more]') && state.lastScanResult && !state.scanInProgress) {
    state.sameShotVisibleLimit += 50;
    renderSameShotResults(state.lastScanResult);
    return;
  }
  var sameShotReveal = target.closest(/** @type {'button'} */ ('[data-same-shot-reveal]'));
  if (sameShotReveal) revealDuplicatePhoto(Number(sameShotReveal.dataset.sameShotReveal));
}

/** @param {Event} event */
function onDuplicatesChange(event) {
  if (event.target instanceof HTMLInputElement && event.target.classList.contains('group-check')) {
    updateApplyCount();
  }
}

// Card thumbnails: a loaded image hides its placeholder; a failed one is
// removed and the placeholder says so. Image load and error events do not
// bubble, so these listen in the capture phase.
/** @param {Event} event */
function onThumbLoad(event) {
  var img = event.target;
  if (img instanceof HTMLImageElement && img.hasAttribute('data-dup-thumb') && img.parentElement) {
    img.parentElement.classList.add('loaded');
  }
}

/** @param {Event} event */
function onThumbError(event) {
  var img = event.target;
  if (!(img instanceof HTMLImageElement) || !img.hasAttribute('data-dup-thumb')) return;
  var placeholder = img.previousElementSibling;
  if (placeholder) placeholder.textContent = 'No thumbnail';
  img.remove();
}

export function bindDuplicateEvents() {
  document.addEventListener('click', onDuplicatesClick);
  document.addEventListener('change', onDuplicatesChange);
  document.addEventListener('load', onThumbLoad, true);
  document.addEventListener('error', onThumbError, true);
}

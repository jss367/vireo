// Page startup: the module the template loads. Binds the template's
// data-compare-on controls and the delegated listeners for rendered markup,
// then loads the threshold control and the collections on DOMContentLoaded.

import { state } from './state.js';
import { goToPage, scheduleSearch, setPerPage } from './controls.js';
import { batchAction } from './decisions.js';
import { bindCompareRenderedMarkup } from './events.js';
import { loadCollections, loadComparison, reloadRows, setVisibleModels } from './loading.js';
import { setSort } from './render.js';
import {
  previewConflictThreshold, setConflictThreshold, updateConflictThresholdControl,
} from './threshold.js';
import { VireoTextSearch } from './shared.js';

/**
 * The template's controls name their handlers in
 * `data-compare-on="event:name"` instead of inline on*= attributes; an
 * unknown name throws so a typo fails loudly. Handlers that read the
 * control's value get the element, as `onchange="setSort(this.value)"` did.
 *
 * @type {Record<string, (element: HTMLElement) => unknown>}
 */
const COMPARE_HANDLERS = {
  loadComparison: function() { return loadComparison(); },
  setVisibleModels: function() { setVisibleModels(); },
  setSort: function(el) { setSort(/** @type {HTMLSelectElement} */ (el).value); },
  previewConflictThreshold: function(el) { previewConflictThreshold(/** @type {HTMLInputElement} */ (el).value); },
  setConflictThreshold: function(el) { setConflictThreshold(/** @type {HTMLInputElement} */ (el).value); },
  scheduleSearch: function() { scheduleSearch(); },
  toggleMatchCase: function() { VireoTextSearch.toggle('compareSearch', 'match_case', reloadRows); },
  toggleWholeWord: function() { VireoTextSearch.toggle('compareSearch', 'whole_word', reloadRows); },
  previousPage: function() { goToPage(state.comparePage - 1); },
  nextPage: function() { goToPage(state.comparePage + 1); },
  setPerPage: function(el) { setPerPage(/** @type {HTMLSelectElement} */ (el).value); },
  batchAccept: function() { return batchAction('accept'); },
  batchReplace: function() { return batchAction('replace'); },
  batchReject: function() { return batchAction('reject'); },
  batchReviewed: function() { return batchAction('reviewed'); },
};

function bindCompareMarkup() {
  document.querySelectorAll('[data-compare-on]').forEach(function(node) {
    var el = /** @type {HTMLElement} */ (node);
    (el.dataset.compareOn || '').split(/\s+/).filter(Boolean).forEach(function(spec) {
      var parts = spec.split(':');
      var found = COMPARE_HANDLERS[parts[1] || ''];
      if (!found) throw new Error('Unknown ID Conflicts handler: ' + spec);
      var handler = found;
      el.addEventListener(parts[0] || '', function() { handler(el); });
    });
  });
}

bindCompareMarkup();
bindCompareRenderedMarkup();
document.addEventListener('DOMContentLoaded', function() { updateConflictThresholdControl(); });
document.addEventListener('DOMContentLoaded', function() { loadCollections(); });

// Page startup: binds the template's data-compare-on controls, then loads
// the threshold control and the collections on DOMContentLoaded.
// Classic page script; loads after every other ID Conflicts script.

/**
 * The template's controls name their handlers in
 * `data-compare-on="event:name"` instead of inline on*= attributes; an
 * unknown name throws so a typo fails loudly. Handlers that read the
 * control's value get the element, as `onchange="setSort(this.value)"` did.
 *
 * @type {Record<string, (element: HTMLElement) => unknown>}
 */
var COMPARE_HANDLERS = {
  loadComparison: function() { return loadComparison(); },
  setVisibleModels: function() { setVisibleModels(); },
  setSort: function(el) { setSort(/** @type {HTMLSelectElement} */ (el).value); },
  previewConflictThreshold: function(el) { previewConflictThreshold(/** @type {HTMLInputElement} */ (el).value); },
  setConflictThreshold: function(el) { setConflictThreshold(/** @type {HTMLInputElement} */ (el).value); },
  scheduleSearch: function() { scheduleSearch(); },
  toggleMatchCase: function() { VireoTextSearch.toggle('compareSearch', 'match_case', reloadRows); },
  toggleWholeWord: function() { VireoTextSearch.toggle('compareSearch', 'whole_word', reloadRows); },
  previousPage: function() { goToPage(comparePage - 1); },
  nextPage: function() { goToPage(comparePage + 1); },
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
document.addEventListener('DOMContentLoaded', function() { updateConflictThresholdControl(); });
document.addEventListener('DOMContentLoaded', function() { loadCollections(); });

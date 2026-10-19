// Page startup: the module the template loads. Registers the page hooks,
// binds the template's data-highlights-on controls and the filter controls,
// the lightbox listeners, restores the saved view preferences, and loads on
// DOMContentLoaded.
// ES module.

import { VireoTextSearch, VireoViewPreferences, registerPageHooks } from './shared.js';
import { onHighlightFlagChanged } from './flag-changes.js';
import { hideHighlightUndo, undoLastHighlightReject, updateHighlightsLightboxControls } from './lightbox.js';
import { debounceLoad, highlightsAfterHistoryChange, loadHighlights, updateAdvancedHighlightsOptions } from './loading.js';
import { render } from './render.js';
import { closeSaveModal, doSave, showSaveModal } from './save.js';
import { closeSpeciesModal, submitSpeciesModal } from './species.js';
import { state } from './state.js';

// The navbar's undo/redo and the lightbox's curation actions call these.
registerPageHooks({
  afterHistoryChange: highlightsAfterHistoryChange,
  loadHighlights: loadHighlights,
  updateHighlightsLightboxControls: updateHighlightsLightboxControls,
});

/**
 * The template's controls name their handlers in
 * `data-highlights-on="event:name"` instead of inline on*= attributes; an
 * unknown name throws so a typo fails loudly.
 *
 * @type {Record<string, (event: Event, element: HTMLElement) => unknown>}
 */
var HIGHLIGHTS_HANDLERS = {
  searchMatchCase: function() { VireoTextSearch.toggle('highlightSearch', 'match_case', debounceLoad); },
  searchWholeWord: function() { VireoTextSearch.toggle('highlightSearch', 'whole_word', debounceLoad); },
  speciesMatchCase: function() { VireoTextSearch.toggle('highlightSpecies', 'match_case', debounceLoad); },
  speciesWholeWord: function() { VireoTextSearch.toggle('highlightSpecies', 'whole_word', debounceLoad); },
  showSaveModal: function() { return showSaveModal(); },
  undoLastHighlightReject: function() { return undoLastHighlightReject(); },
  hideHighlightUndo: function() { hideHighlightUndo(); },
  closeSaveModal: function() { closeSaveModal(); },
  saveReplace: function() { return doSave('replace'); },
  saveNew: function() { return doSave('new'); },
  speciesModalBackdrop: function(event, el) { if (event.target === el) closeSpeciesModal(); },
  closeSpeciesModal: function() { closeSpeciesModal(); },
  submitSpeciesModal: function() { return submitSpeciesModal(); },
};

function bindHighlightsMarkup() {
  document.querySelectorAll('[data-highlights-on]').forEach(function(node) {
    var el = /** @type {HTMLElement} */ (node);
    (el.dataset.highlightsOn || '').split(/\s+/).filter(Boolean).forEach(function(spec) {
      var parts = spec.split(':');
      var found = HIGHLIGHTS_HANDLERS[parts[1] || ''];
      if (!found) throw new Error('Unknown Highlights handler: ' + spec);
      var handler = found;
      el.addEventListener(parts[0] || '', function(event) { handler(event, el); });
    });
  });
}

/** @param {string} id */
function highlightsEl(id) {
  return /** @type {HTMLElement} */ (document.getElementById(id));
}

/** @param {Event} event */
function controlValue(event) {
  return /** @type {HTMLInputElement} */ (event.currentTarget).value;
}

/** A filter that changes which buckets exist: collapse every row, then reload. */
function resetExpansionAndReload() {
  state.expandedBuckets.clear();
  state.unidentifiedExpanded = false;
  loadHighlights();
}

function bindHighlightsControls() {
  highlightsEl('speciesModalInput').addEventListener('keydown', function(event) {
    if (event.key === 'Enter') {
      event.preventDefault();
      submitSpeciesModal();
    } else if (event.key === 'Escape') {
      closeSpeciesModal();
    }
  });
  highlightsEl('qualitySlider').addEventListener('input', function(event) {
    highlightsEl('qualityValue').textContent = (Number(controlValue(event)) / 100).toFixed(2);
    debounceLoad();
  });
  highlightsEl('confidenceSlider').addEventListener('input', function(event) {
    highlightsEl('confidenceValue').textContent = (Number(controlValue(event)) / 100).toFixed(2);
    debounceLoad();
  });
  highlightsEl('highlightSearch').addEventListener('input', function() { debounceLoad(); });
  highlightsEl('speciesSearch').addEventListener('input', function() { debounceLoad(); });
  highlightsEl('confirmationFilter').addEventListener('change', resetExpansionAndReload);
  highlightsEl('highlightSelectionFilter').addEventListener('change', resetExpansionAndReload);
  highlightsEl('representativeFilter').addEventListener('change', resetExpansionAndReload);
  highlightsEl('perRowSlider').addEventListener('input', function(event) {
    highlightsEl('perRowValue').textContent = controlValue(event);
    if (state.currentData) render();
  });
  highlightsEl('sortSelect').addEventListener('change', function() {
    if (state.currentData) render();
  });
  window.addEventListener('advancedmodechange', updateAdvancedHighlightsOptions);
  window.addEventListener('devmodechange', updateAdvancedHighlightsOptions);
  // Reset per-row expand state when the user picks a different folder
  // so the new folder's rows don't inherit the previous folder's state.
  highlightsEl('folderSelect').addEventListener('change', resetExpansionAndReload);
}

bindHighlightsMarkup();
document.addEventListener('lightbox:photochanged', function(event) {
  var detail = /** @type {CustomEvent<{photoId?: number}>} */ (event).detail;
  state.activeLightboxPhotoId = detail ? (detail.photoId ?? null) : null;
  updateHighlightsLightboxControls(state.activeLightboxPhotoId);
});
bindHighlightsControls();
document.addEventListener('lightbox:flagchanged', onHighlightFlagChanged);

VireoViewPreferences.restoreAll(highlightsEl('controlsBar'));
highlightsEl('perRowValue').textContent =
  /** @type {HTMLInputElement} */ (highlightsEl('perRowSlider')).value;
document.addEventListener('DOMContentLoaded', function() { updateAdvancedHighlightsOptions(); });
document.addEventListener('DOMContentLoaded', function() { loadHighlights(); });

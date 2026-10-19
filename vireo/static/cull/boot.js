// Page startup: registers the undo/redo page hooks, binds the template's
// data-cull-on controls and the group-focus arrow keys, and loads the page.
// Classic page script; loads after every other Cull page script.

Vireo.registerPageHooks({
  beforeHistoryChange: cullBeforeHistoryChange,
  afterHistoryChange: cullAfterHistoryChange,
});

/**
 * The template's controls name their handlers in
 * `data-cull-on="event:name"` instead of inline on*= attributes; an unknown
 * name throws so a typo fails loudly. The sliders pass themselves, as
 * `onScoringChange(this)` did.
 *
 * @type {Record<string, (element: HTMLElement) => unknown>}
 */
var CULL_HANDLERS = {
  renderCullSpeciesOptions: function() { renderCullSpeciesOptions(); },
  clearCullSpecies: function() { clearCullSpecies(); },
  onCullScopeChange: function() { onCullScopeChange(); },
  clearCullFilters: function() { clearCullFilters(); },
  runCulling: function() { return runCulling(); },
  toggleCullSettings: function() { toggleCullSettings(); },
  applyCulling: function() { return applyCulling(); },
  onScoringChange: function(el) { onScoringChange(/** @type {HTMLInputElement} */ (el)); },
  onGroupingChange: function(el) { onGroupingChange(/** @type {HTMLInputElement} */ (el)); },
  resetScoringDefaults: function() { resetScoringDefaults(); },
  resetGroupingDefaults: function() { resetGroupingDefaults(); },
  saveGroupingDefaults: function() { saveGroupingDefaults(); },
  closeCullRejected: function() { closeCullRejected(); },
  deleteCullRejected: function() { return deleteCullRejected(); },
};

function bindCullMarkup() {
  document.querySelectorAll('[data-cull-on]').forEach(function(node) {
    var el = /** @type {HTMLElement} */ (node);
    (el.dataset.cullOn || '').split(/\s+/).filter(Boolean).forEach(function(spec) {
      var parts = spec.split(':');
      var found = CULL_HANDLERS[parts[1] || ''];
      if (!found) throw new Error('Unknown Cull handler: ' + spec);
      var handler = found;
      el.addEventListener(parts[0] || '', function() { handler(el); });
    });
  });
}

bindCullMarkup();

// Left/Right move the focused scene group.
document.addEventListener('keydown', function(e) {
  if (e.defaultPrevented) return;
  if (e.key !== 'ArrowRight' && e.key !== 'ArrowLeft') return;
  if (e.altKey || e.ctrlKey || e.metaKey || e.shiftKey) return;
  if (cullKeyTargetIsEditable(e.target)) return;
  if (document.querySelector('.lightbox-overlay.active, .pipeline-overlay.active, .similar-overlay.active, .modal-overlay.open, .grm-overlay.open, .inspect-overlay.open, .shortcuts-overlay.open, .help-overlay.active, .report-overlay.active, .browse-compare-overlay.active, #deleteModal.open, #cullRejectedDialog[open]')) return;
  if (moveFocusedCullGroup(e.key === 'ArrowRight' ? 1 : -1)) {
    e.preventDefault();
  }
});

document.addEventListener('DOMContentLoaded', async function() {
  await loadPipelineSliderDefaults();
  await loadCollections();
  await loadCullingResults();
});
document.addEventListener('DOMContentLoaded', function() { initializeCullFavorites(); });

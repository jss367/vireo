// Page startup: registers the page hook, binds the template's controls and
// the delegated listeners on rendered markup, and loads the page.
// Classic page script; loads after every other Storage page script.

// vireo-release-originals.js reloads the overview after a release job.
Vireo.registerPageHooks({loadStoragePanel: loadStoragePanel});

/**
 * The template's controls name their handlers in
 * `data-storage-on="event:name ..."` instead of inline on*= attributes. Each
 * handler runs with the element it is bound to; an unknown name throws so a
 * typo fails loudly.
 *
 * @type {Record<string, (event: Event, element: HTMLElement) => unknown>}
 */
var STORAGE_HANDLERS = {
  refreshStoragePage: function() { return refreshStoragePage(); },
  clearSafeCaches: function() { return clearSafeCaches(); },
  renderStorageCards: function() { renderStorageCards(); },
  /** @param {Event} _event @param {HTMLElement} el */
  thumbSizeInput: function(_event, el) {
    storageEl('cfgThumbSizeVal').textContent = /** @type {HTMLInputElement} */ (el).value + 'px';
    saveStorageConfig();
  },
  /** @param {Event} _event @param {HTMLElement} el */
  thumbQualityInput: function(_event, el) {
    storageEl('cfgThumbQualityVal').textContent = /** @type {HTMLInputElement} */ (el).value;
    saveStorageConfig();
  },
  /** @param {Event} _event @param {HTMLElement} el */
  previewQualityInput: function(_event, el) {
    storageEl('cfgPreviewQualityVal').textContent = /** @type {HTMLInputElement} */ (el).value;
    saveStorageConfig();
  },
  saveStorageConfig: function() { saveStorageConfig(); },
  clearPreviewCache: function() { return clearPreviewCache(); },
  updateWorkingCopyLimitState: function() { updateWorkingCopyLimitState(); },
  applyWorkingCopyLimit: function() { return applyWorkingCopyLimit(); },
  clearEmbeddingCache: function() { return clearEmbeddingCache(); },
  /** @param {Event} _event @param {HTMLElement} el */
  saveRetainedStoragePolicy: function(_event, el) {
    return saveRetainedStoragePolicy(/** @type {HTMLButtonElement} */ (el));
  },
  /** @param {Event} event */
  cancelWorkingCopyLimitModal: function(event) {
    event.preventDefault();
    closeWorkingCopyLimitModal();
  },
  /** @param {Event} event @param {HTMLElement} el */
  workingCopyLimitBackdrop: function(event, el) { if (event.target === el) closeWorkingCopyLimitModal(); },
  closeWorkingCopyLimitModal: function() { closeWorkingCopyLimitModal(); },
  confirmWorkingCopyLimit: function() { confirmWorkingCopyLimit(); },
  /** @param {Event} event @param {HTMLElement} el */
  masksBackdrop: function(event, el) { if (event.target === el) closeMasksModal(); },
  closeMasksModal: function() { closeMasksModal(); },
  /** @param {Event} event @param {HTMLElement} el */
  storageModalBackdrop: function(event, el) { if (event.target === el) closeStorageModal(); },
  closeStorageModal: function() { closeStorageModal(); },
  selectAllShownFiles: function() { selectAllFiles(true); },
  selectNoFiles: function() { selectAllFiles(false); },
  deleteSelectedFiles: function() { return deleteSelectedFiles(); },
  deleteAllFiles: function() { return deleteAllFiles(); },
  /** @param {Event} event @param {HTMLElement} el */
  hfCacheBackdrop: function(event, el) { if (event.target === el) closeHfCacheModal(); },
  closeHfCacheModal: function() { closeHfCacheModal(); },
  removeUnusedHfCache: function() { return removeUnusedHfCache(); },
};

function bindStorageMarkup() {
  document.querySelectorAll('[data-storage-on]').forEach(function(node) {
    var el = /** @type {HTMLElement} */ (node);
    (el.dataset.storageOn || '').split(/\s+/).filter(Boolean).forEach(function(spec) {
      var parts = spec.split(':');
      var found = STORAGE_HANDLERS[parts[1] || ''];
      if (!found) throw new Error('Unknown Storage handler: ' + spec);
      var handler = found;
      el.addEventListener(parts[0] || '', function(event) { handler(event, el); });
    });
  });
}

/** @param {string} id @param {string} selector @param {(target: HTMLElement, event: Event) => void} action */
function delegateStorageClick(id, selector, action) {
  storageEl(id).addEventListener('click', function(event) {
    var target = event.target instanceof Element ? event.target.closest(selector) : null;
    if (target instanceof HTMLElement && !(target instanceof HTMLButtonElement && target.disabled)) action(target, event);
  });
}

/** Buttons in rendered markup carry data attributes; these delegated listeners survive re-renders. */
function bindStorageRenderedMarkup() {
  delegateStorageClick('storageLocations', '[data-storage-location]', function(button) {
    openStorageFolder(button.dataset.storageLocation || '');
  });
  delegateStorageClick('storageGrid', '[data-storage-manage]', function(button) {
    var target = button.dataset.storageManage || '';
    if (target === 'hf-cache') openHfCacheModal();
    else if (target === 'settings') window.location.href = '/settings';
    else openStorageModal(target);
  });
  delegateStorageClick('masksCard', '[data-masks-action]', function(button) {
    var action = button.dataset.masksAction;
    if (action === 'manage') openMasksModal();
    else if (action === 'delete-inactive') deleteInactiveMasks();
    else if (action === 'delete-stale') deleteStaleMasks();
  });
  storageEl('modalFiles').addEventListener('change', function(event) {
    if (event.target instanceof HTMLInputElement && event.target.type === 'checkbox') updateSelectedInfo();
  });
}

bindStorageMarkup();
bindStorageRenderedMarkup();
document.addEventListener('DOMContentLoaded', function() { refreshStoragePage(); });
document.addEventListener('DOMContentLoaded', function() { loadStorageConfig(); });

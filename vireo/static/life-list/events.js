// The Life List page's event bindings. The template's controls name their
// handlers in `data-life-on="event:name"` (with `data-life-arg` for
// llSwitchTab's tab) instead of inline on*= attributes; the explorer's
// rendered taxonomy-download button and class select are handled by
// delegated listeners on its panel.

import { explorerDownloadTaxonomy, loadExplorer } from './explorer.js';
import { closeExportModal, downloadLifeListExport, openExportModal, updateExportControls } from './export.js';
import { debounceRender } from './filters.js';
import {
  browsePublishDest, closePublishModal, openPublishModal, publishSettingsChanged, startPublishSite,
} from './publish.js';
import {
  browseSiteExportDest, closeSiteExportModal, openSiteExportModal, startSiteExport,
} from './site-export.js';
import { llSwitchTab } from './tabs.js';
import { VireoTextSearch } from './shared.js';

/**
 * Handlers named by `data-life-on`. Each gets the event and the bound
 * element; an unknown name throws so a typo fails loudly.
 *
 * @type {Record<string, (event: Event, element: HTMLElement) => unknown>}
 */
var LIFE_HANDLERS = {
  llSwitchTab: function(_event, el) { llSwitchTab(el.dataset.lifeArg || 'list'); },
  toggleMatchCase: function() { VireoTextSearch.toggle('lifeSpecies', 'match_case', debounceRender); },
  toggleWholeWord: function() { VireoTextSearch.toggle('lifeSpecies', 'whole_word', debounceRender); },
  openExportModal: function() { openExportModal(); },
  openPublishModal: function() { openPublishModal(); },
  openSiteExportModal: function() { openSiteExportModal(); },
  // A click on the overlay itself, not the dialog inside it, closes it.
  exportBackdrop: function(event, el) { if (event.target === el) closeExportModal(); },
  siteExportBackdrop: function(event, el) { if (event.target === el) closeSiteExportModal(); },
  updateExportControls: function() { updateExportControls(); },
  downloadLifeListExport: function() { downloadLifeListExport(); },
  closeExportModal: function() { closeExportModal(); },
  browsePublishDest: function() { return browsePublishDest(); },
  publishSettingsChanged: function() { publishSettingsChanged(); },
  startPublishSite: function() { return startPublishSite(); },
  closePublishModal: function() { closePublishModal(); },
  browseSiteExportDest: function() { return browseSiteExportDest(); },
  startSiteExport: function() { return startSiteExport(); },
  closeSiteExportModal: function() { closeSiteExportModal(); },
};

export function bindLifeListMarkup() {
  document.querySelectorAll('[data-life-on]').forEach(function(node) {
    var el = /** @type {HTMLElement} */ (node);
    (el.dataset.lifeOn || '').split(/\s+/).filter(Boolean).forEach(function(spec) {
      var parts = spec.split(':');
      var found = LIFE_HANDLERS[parts[1] || ''];
      if (!found) throw new Error('Unknown Life List handler: ' + spec);
      var handler = found;
      el.addEventListener(parts[0] || '', function(event) { handler(event, el); });
    });
  });
}

/** The explorer panel's rendered controls; the panel is re-rendered on every load. */
export function bindExplorerControls() {
  var panel = /** @type {HTMLElement} */ (document.getElementById('tab-explorer'));
  panel.addEventListener('click', function(event) {
    var button = event.target instanceof Element && event.target.closest('button[data-explorer-download]');
    if (button) explorerDownloadTaxonomy(/** @type {HTMLButtonElement} */ (button));
  });
  panel.addEventListener('change', function(event) {
    if (event.target instanceof HTMLSelectElement && event.target.id === 'explorerClass') {
      loadExplorer(event.target.value);
    }
  });
}

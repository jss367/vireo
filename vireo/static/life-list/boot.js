// Page startup: the module the template loads. Binds the template's
// data-life-on controls and the explorer's delegated listeners, then keeps the
// old inline script's order: the lightbox and list-control listeners, the
// shared folder browser, the site-export Escape key, the lifelist:changed
// refresh, the page hooks, then the first load and the ?view= tab.

import { state } from './state.js';
import { bindExplorerControls, bindLifeListMarkup } from './events.js';
import { openExportModal } from './export.js';
import { bindLifeListControls } from './filters.js';
import { bindLifeListLightboxEvents } from './lightbox.js';
import { loadLifeList, restoreLifeListViewPreferences } from './list.js';
import { createPublishFolderBrowser } from './publish.js';
import { bindSiteExportEscape } from './site-export.js';
import { llSwitchTab } from './tabs.js';
import { registerPageHooks } from './shared.js';

bindLifeListMarkup();
bindExplorerControls();
bindLifeListLightboxEvents();
bindLifeListControls();
state.publishFolderBrowser = createPublishFolderBrowser();
bindSiteExportEscape();

// The representative lightbox panel is shared (see _navbar.html) and
// works on every page. When it changes a representative photo, it dispatches
// `lifelist:changed`; refresh this page's grid + ribbons in response.
document.addEventListener('lifelist:changed', function() {
  loadLifeList();
});

// The shared navbar's undo/redo and native Export command (vireo-page-hooks.js).
registerPageHooks({
  afterHistoryChange: function() { return loadLifeList(); },
  openExportModal: openExportModal,
});

restoreLifeListViewPreferences();
loadLifeList();

// Initial tab from ?view= (defaults to list).
(function() {
  var view = 'list';
  try { view = new URLSearchParams(window.location.search).get('view') || 'list'; } catch (e) {}
  if (view === 'explorer') llSwitchTab('explorer');
})();

// Page startup.
// Classic page script; loads after every other Life List definition.

// Preserve the inline script's order: the lightbox and list-control
// listeners, the shared folder browser, the site-export Escape key, the
// lifelist:changed refresh, then the first load and the ?view= tab.
bindLifeListLightboxEvents();
bindLifeListControls();
const publishFolderBrowser = createPublishFolderBrowser();
bindSiteExportEscape();

// The representative lightbox panel is shared (see _navbar.html) and
// works on every page. When it changes a representative photo, it dispatches
// `lifelist:changed`; refresh this page's grid + ribbons in response.
document.addEventListener('lifelist:changed', function() {
  loadLifeList();
});

restoreLifeListViewPreferences();
loadLifeList();

// Initial tab from ?view= (defaults to list).
(function() {
  var view = 'list';
  try { view = new URLSearchParams(window.location.search).get('view') || 'list'; } catch (e) {}
  if (view === 'explorer') llSwitchTab('explorer');
})();

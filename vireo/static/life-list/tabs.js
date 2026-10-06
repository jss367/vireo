// List/Explorer tab switching and the explorer's lazy first load.
// Classic page script; load boot.js after all definitions.

// ---------------- Explorer tab ----------------
var explorerLoaded = false;   // lazy-load guard: fetch explorer once on first open

function llSwitchTab(name) {
  document.querySelectorAll('.ll-tab').forEach(function(t) {
    t.classList.toggle('active', t.dataset.tab === name);
  });
  document.querySelectorAll('.ll-tabpanel').forEach(function(p) {
    p.classList.toggle('active', p.id === 'tab-' + name);
  });
  try {
    var url = new URL(window.location.href);
    url.searchParams.set('view', name);
    history.replaceState(null, '', url.toString());
  } catch (e) {}
  if (name === 'explorer' && !explorerLoaded) {
    explorerLoaded = true;
    loadExplorer();
  }
}

window.openCommandPalette = function() {
  const overlay = document.getElementById('commandPalette');
  if (!overlay) return;
  overlay.removeAttribute('hidden');
  const input = document.getElementById('cmdPaletteInput');
  if (input) { input.value = ''; input.focus(); }
  if (window._cmdPaletteRender) window._cmdPaletteRender('');
};
window.closeCommandPalette = function() {
  const overlay = document.getElementById('commandPalette');
  if (overlay) overlay.setAttribute('hidden', '');
};
document.addEventListener('keydown', function(e) {
  const isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0;
  const mod = isMac ? e.metaKey : e.ctrlKey;
  if (mod && e.key.toLowerCase() === 'k') {
    e.preventDefault();
    window.openCommandPalette();
    return;
  }
  if (e.key === 'Escape') {
    const overlay = document.getElementById('commandPalette');
    if (overlay && !overlay.hasAttribute('hidden')) {
      window.closeCommandPalette();
      return;
    }
  }
  // cmd+1..9 → nth pinned tab
  if (mod && /^[1-9]$/.test(e.key)) {
    const tabs = (window._navTabs ? window._navTabs.getTabs() : []);
    const all = (window._navTabs ? window._navTabs.getAllPages() : []);
    const idx = parseInt(e.key, 10) - 1;
    if (idx < tabs.length) {
      e.preventDefault();
      const target = all.find(p => p.id === tabs[idx]);
      if (target) {
        window.location.href = window.vireoResolveNavigationHref(target.href);
      }
    }
  }
  // cmd+W → close current tab (pinned or ephemeral)
  if (mod && e.key.toLowerCase() === 'w') {
    const cur = (function() {
      const p = window.location.pathname;
      if (p.startsWith('/pipeline/rapid-review')) return 'pipeline_rapid_review';
      if (p.startsWith('/pipeline/review')) return 'pipeline_review';
      if (p.startsWith('/locations/review')) return 'location_review';
      if (p === '/' || p.startsWith('/browse')) return 'browse';
      return (p.split('/')[1] || '').replace(/-/g, '_');
    })();
    if (cur) {
      // The ephemeral tab also renders as .nav-tab with a .nav-tab-close
      // that calls clearEphemeral(), so a single selector handles both.
      const tabAnchor = document.querySelector('.nav-tab[data-nav-id="' + cur + '"]');
      if (tabAnchor) {
        e.preventDefault();
        const closeBtn = tabAnchor.querySelector('.nav-tab-close');
        if (closeBtn) closeBtn.click();
      }
    }
  }
});

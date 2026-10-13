// Both the browser handler and the native menu use this ordered destination list.
window.vireoNumberedNavigationMode = 'tab_order';
window.vireoNumberedNavigationPages = function() {
  const fixed = ['import', 'browse', 'pipeline', 'pipeline_review', 'review',
    'cull', 'jobs', 'highlights', 'misses'];
  const nav = window._navTabs;
  const ids = window.vireoNumberedNavigationMode === 'fixed_pages'
    ? fixed : (nav ? nav.getTabs() : []);
  const all = nav ? nav.getAllPages() : [];
  // Keep empty slots in place if a registry entry is unavailable.
  return ids.slice(0, 9).map(id => all.find(page => page.id === id) || null);
};
let nativeNavigationSync = Promise.resolve();
window.vireoSyncNumberedNavigation = function() {
  if (!window._navTabs || !window.__TAURI_INTERNALS__) return;
  const paths = window.vireoNumberedNavigationPages().map(page => page ? page.href : '');
  // Serialize native updates so a slower earlier render cannot restore old bindings.
  nativeNavigationSync = nativeNavigationSync.then(() =>
    window.__TAURI_INTERNALS__.invoke('sync_navigation_shortcuts', {paths})
  ).catch(error => console.warn('[nav] Could not sync native shortcuts', error));
  return nativeNavigationSync;
};
window.vireoSetNumberedNavigationMode = function(mode) {
  window.vireoNumberedNavigationMode = mode === 'fixed_pages' ? 'fixed_pages' : 'tab_order';
  return window.vireoSyncNumberedNavigation();
};

window.openCommandPalette = function() {
  // A managed VireoDialog on the stack has made this overlay's branch inert
  // via updateBackground(), so revealing #commandPalette would present a
  // visible but inoperable palette whose input and results cannot receive
  // focus; the dialog's focus containment would also pull focus back out of
  // the palette, and the first Escape is owned by the dialog's Keymap
  // handler. Treat Ctrl/Cmd+K (and the ⌘K hint click) as a no-op in that
  // state so the user keeps interacting with the dialog; dismissing the
  // dialog clears the inert state and the palette opens normally afterward.
  if (window.VireoDialog && window.VireoDialog.hasActive && window.VireoDialog.hasActive()) return;
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
  // Keep the desktop's existing secondary navigation shortcuts available
  // when a menu item uses its single accelerator slot for a number.
  if (window.__TAURI_INTERNALS__ && mod && e.shiftKey && !e.altKey) {
    const paths = {j: '/jobs', d: '/dashboard', w: '/workspace', l: '/logs'};
    const path = paths[e.key.toLowerCase()];
    if (path) {
      e.preventDefault();
      window.location.href = window.vireoResolveNavigationHref(path);
      return;
    }
  }
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
  // Cmd/Ctrl+1..9 → tab position or fixed page, according to Settings.
  if (mod && !e.shiftKey && !e.altKey && /^[1-9]$/.test(e.key)) {
    const pages = window.vireoNumberedNavigationPages();
    const idx = parseInt(e.key, 10) - 1;
    const target = pages[idx];
    if (target) {
      e.preventDefault();
      window.location.href = window.vireoResolveNavigationHref(target.href);
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
